"""Migration run state: steps, progress events, and the in-process run registry."""

from __future__ import annotations

import contextlib
import queue
import threading
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class StepStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class Step:
    id: str
    title: str
    status: StepStatus = StepStatus.PENDING
    detail: str = ""
    warnings: list[str] = field(default_factory=list)
    started_at: str | None = None
    finished_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "status": self.status.value,
            "detail": self.detail,
            "warnings": list(self.warnings),
            "startedAt": self.started_at,
            "finishedAt": self.finished_at,
        }


class CancelledError(RuntimeError):
    """The operator asked to stop the migration."""


class MigrationRun:
    """Thread-safe state for one migration, plus a fan-out queue for the UI."""

    def __init__(self, *, source_workspace_name: str, capacity_name: str) -> None:
        self.id = uuid.uuid4().hex
        self.source_workspace_name = source_workspace_name
        self.capacity_name = capacity_name
        self.status = RunStatus.PENDING
        self.error: str | None = None
        self.created_at = _now()
        self.finished_at: str | None = None

        # Populated as the migration progresses so the UI can link out and clean up.
        self.target_workspace: dict[str, Any] | None = None
        self.scratch_workspace: dict[str, Any] | None = None
        self.summary: dict[str, Any] = {}
        self.cleanup_done = False

        self._steps: list[Step] = []
        self._steps_by_id: dict[str, Step] = {}
        self._lock = threading.Lock()
        self._cancel = threading.Event()
        self._subscribers: list[queue.Queue[dict[str, Any] | None]] = []

    # ------------------------------------------------------------------- steps

    def _ensure_step(self, step_id: str, title: str) -> Step:
        """Create the step if it is new. Callers publish once they have finished mutating it."""
        step = self._steps_by_id.get(step_id)
        if step:
            return step
        step = Step(id=step_id, title=title)
        self._steps.append(step)
        self._steps_by_id[step_id] = step
        return step

    def add_step(self, step_id: str, title: str) -> Step:
        with self._lock:
            step = self._ensure_step(step_id, title)
        self._publish()
        return step

    def start_step(self, step_id: str, title: str) -> Step:
        with self._lock:
            step = self._ensure_step(step_id, title)
            step.status = StepStatus.RUNNING
            step.started_at = _now()
            step.detail = ""
        self._publish()
        return step

    def update_step(self, step_id: str, detail: str) -> None:
        with self._lock:
            step = self._steps_by_id.get(step_id)
            if not step:
                return
            step.detail = detail
        self._publish()

    def finish_step(
        self,
        step_id: str,
        status: StepStatus,
        detail: str = "",
        warnings: list[str] | None = None,
    ) -> None:
        with self._lock:
            step = self._steps_by_id.get(step_id)
            if not step:
                return
            step.status = status
            step.finished_at = _now()
            if detail:
                step.detail = detail
            if warnings:
                step.warnings.extend(warnings)
        self._publish()

    # ------------------------------------------------------------------ status

    def mark_running(self) -> None:
        with self._lock:
            self.status = RunStatus.RUNNING
        self._publish()

    def mark_finished(self, status: RunStatus, error: str | None = None) -> None:
        with self._lock:
            self.status = status
            self.error = error
            self.finished_at = _now()
        self._publish()
        self._close_subscribers()

    def cancel(self) -> None:
        self._cancel.set()

    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    def raise_if_cancelled(self) -> None:
        if self._cancel.is_set():
            raise CancelledError("Migration cancelled by the operator")

    # ------------------------------------------------------------ serialisation

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "id": self.id,
                "status": self.status.value,
                "error": self.error,
                "createdAt": self.created_at,
                "finishedAt": self.finished_at,
                "sourceWorkspaceName": self.source_workspace_name,
                "capacityName": self.capacity_name,
                "targetWorkspace": self.target_workspace,
                "scratchWorkspace": self.scratch_workspace,
                "cleanupDone": self.cleanup_done,
                "summary": dict(self.summary),
                "steps": [step.as_dict() for step in self._steps],
            }

    # -------------------------------------------------------------- event feed

    def subscribe(self) -> queue.Queue[dict[str, Any] | None]:
        subscriber: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=200)
        with self._lock:
            self._subscribers.append(subscriber)
        subscriber.put(self.snapshot())
        return subscriber

    def unsubscribe(self, subscriber: queue.Queue[dict[str, Any] | None]) -> None:
        with self._lock:
            if subscriber in self._subscribers:
                self._subscribers.remove(subscriber)

    def _publish(self) -> None:
        snapshot = self.snapshot()
        with self._lock:
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            # A slow reader must not stall the migration; it will catch up on the next event.
            with contextlib.suppress(queue.Full):
                subscriber.put_nowait(snapshot)

    def _close_subscribers(self) -> None:
        with self._lock:
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            with contextlib.suppress(queue.Full):
                subscriber.put_nowait(None)


class RunRegistry:
    """Holds the runs for this process. Fab Shuffle is a single-container tool, so memory is fine."""

    def __init__(self) -> None:
        self._runs: dict[str, MigrationRun] = {}
        self._lock = threading.Lock()

    def add(self, run: MigrationRun) -> MigrationRun:
        with self._lock:
            self._runs[run.id] = run
        return run

    def get(self, run_id: str) -> MigrationRun | None:
        with self._lock:
            return self._runs.get(run_id)

    def all(self) -> Iterator[MigrationRun]:
        with self._lock:
            return iter(list(self._runs.values()))


REGISTRY = RunRegistry()


__all__ = [
    "REGISTRY",
    "CancelledError",
    "MigrationRun",
    "RunRegistry",
    "RunStatus",
    "Step",
    "StepStatus",
]
