"""Migration run state: steps, progress events, and the in-process run registry."""

from __future__ import annotations

import contextlib
import queue
import threading
import uuid
from collections.abc import Iterator
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from fabshuffle import journal
from fabshuffle.lifecycle import EvidenceState, Lifecycle


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
        self.lineage_id = self.id
        self.resumed_from = ""
        self.journal_started = False
        self.plan: dict[str, Any] = {}
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
        self.readiness_revision = 0
        self.inventory_complete = False
        self.readiness_attempts: list[dict[str, Any]] = []
        # The latest tenant-wide connection advisory scan (see
        # ``fabshuffle.fabric.connection_advisory``), or ``None`` before the phase that
        # produces it has run this attempt. Advisory only: it never changes ``status``.
        self.connection_advisory: dict[str, Any] | None = None
        self.lifecycle = Lifecycle(attempt_id=self.id, changed=self.readiness_changed)

    def readiness_changed(self) -> None:
        # The next normal progress event carries only a revision, not the item list.
        with self._lock:
            self.readiness_revision += 1

    def set_connection_advisory(self, payload: dict[str, Any]) -> None:
        with self._lock:
            self.connection_advisory = payload
            self.readiness_revision += 1

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
                "lineageId": self.lineage_id,
                "resumedFrom": self.resumed_from,
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
                "readinessRevision": self.readiness_revision,
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


class RunConflict(RuntimeError):
    def __init__(self, run_id: str, reason: str) -> None:
        self.run_id = run_id
        super().__init__(reason)


class RunRegistry:
    """Holds the runs for this process. Fab Shuffle is a single-container tool, so memory is fine."""

    def __init__(self) -> None:
        self._runs: dict[str, MigrationRun] = {}
        self._lock = threading.Lock()
        self._claims: set[str] = set()
        self._cleaning: set[str | tuple[str, ...]] = set()

    @staticmethod
    def _related(
        run: MigrationRun, lineage_id: str, target_id: str, plan: dict[str, Any] | None = None,
        scratch_id: str = "",
    ) -> bool:
        plan = plan or {}
        owned = {
            journal.target_scope(run.plan, workspace["id"])
            for workspace in (run.target_workspace, run.scratch_workspace)
            if workspace and workspace.get("id")
        }
        requested = {
            journal.target_scope(plan, workspace) for workspace in (target_id, scratch_id) if workspace
        }
        return (
            run.lineage_id == lineage_id and journal.binding_scope(run.plan) == journal.binding_scope(plan)
        ) or bool(owned.intersection(requested))

    def _active(
        self, lineage_id: str = "", target_id: str = "", plan: dict[str, Any] | None = None,
        scratch_id: str = "",
    ) -> MigrationRun | None:
        for run in self._runs.values():
            if (
                run.id in self._claims or run.status in (RunStatus.PENDING, RunStatus.RUNNING)
            ) and (not lineage_id or self._related(run, lineage_id, target_id, plan, scratch_id)):
                return run
        return None

    @staticmethod
    def _cleanup_keys(
        plan: dict[str, Any], lineage: str, target: str, scratch: str = "",
    ) -> set[tuple[str, ...]]:
        keys = {("lineage", *journal.binding_scope(plan), lineage)}
        keys.update(
            ("workspace", *journal.target_scope(plan, workspace))
            for workspace in (target, scratch) if workspace
        )
        return keys

    def admit(
        self, run: MigrationRun, *, directory: Path, plan: dict[str, Any],
        cleanup: bool, prior: journal.Replay | None = None,
    ) -> journal.Replay | None:
        """Check, durably record and claim an attempt under one single-writer lock.

        The registry is process scoped, matching the single-container worker architecture.
        Persisted ancestry makes old attempt URLs unusable even after a process restart.
        """
        with self._lock:
            journal.tenant_binding(plan)
            if prior:
                recorded = journal.read(directory / f"{prior.run_id}.jsonl")
                if not recorded.plan:
                    raise journal.TenantBindingError(
                        "The original journal is missing from this tenant pair's directory. "
                        "Select the original source and destination sign-in."
                    )
                journal.validate_resume_plan(plan, prior)
                journal.validate_resume_plan(plan, recorded)
                if (
                    prior.target_workspace_id != recorded.target_workspace_id
                    or prior.scratch_workspace_id != recorded.scratch_workspace_id
                ):
                    raise journal.TenantBindingError(
                        "The requested recovery workspaces differ from the durable journal."
                    )
                prior = recorded
            lineage = (prior.lineage_id or prior.run_id) if prior else run.id
            target = prior.target_workspace_id if prior else ""
            active = self._active(lineage, target, plan, prior.scratch_workspace_id if prior else "")
            if active:
                raise RunConflict(active.id, "This migration already has an active attempt.")
            keys = self._cleanup_keys(
                plan, lineage, target, prior.scratch_workspace_id if prior else "",
            )
            if "*" in self._cleaning or self._cleaning.intersection(keys):
                raise RunConflict(run.id, "Wait for workspace cleanup to finish before starting.")
            if prior:
                for replay in journal.list_runs(directory):
                    if replay.ownership_error:
                        continue
                    if (
                        (
                            journal.binding_scope(replay.plan) == journal.binding_scope(plan)
                            and prior.run_id in replay.ancestors
                        )
                        or (
                            replay.run_id != prior.run_id
                            and ((
                                replay.lineage_id == lineage
                                and journal.binding_scope(replay.plan) == journal.binding_scope(plan)
                            ) or (
                                target and replay.target_workspace_id
                                and journal.target_scope(replay.plan, replay.target_workspace_id)
                                == journal.target_scope(plan, target)
                            ))
                            and replay.created_at > prior.created_at
                        )
                    ):
                        raise RunConflict(
                            replay.run_id, "This attempt was superseded. Resume the latest attempt."
                        )
            journal.Journal(directory / f"{run.id}.jsonl").run_created(
                plan, cleanup=cleanup, prior=prior
            )
            run.journal_started = True
            run.plan = deepcopy(plan)
            run.lineage_id = lineage
            run.resumed_from = prior.run_id if prior else ""
            if target:
                run.target_workspace = {"id": target, "displayName": prior.target_workspace_name}
            if prior and prior.scratch_workspace_id:
                run.scratch_workspace = {"id": prior.scratch_workspace_id}
            self._runs[run.id] = run
            self._claims.add(run.id)
        return prior

    def release(self, run_id: str) -> None:
        with self._lock:
            self._claims.discard(run_id)

    def resumable(self, directory: Path, **expected_identity: str) -> list[journal.Replay]:
        """Offer only runs bound to the caller's ordered tenant/application pair."""
        with self._lock:
            latest: list[journal.Replay] = []
            for replay in journal.latest_runs(directory):
                try:
                    journal.validate_replay_binding(replay, **expected_identity)
                except journal.TenantBindingError:
                    continue
                lineage = replay.lineage_id or replay.run_id
                target = replay.target_workspace_id
                keys = self._cleanup_keys(replay.plan, lineage, target, replay.scratch_workspace_id)
                failed_work = replay.status == RunStatus.SUCCEEDED.value and any(
                    evidence.state is EvidenceState.FAILED
                    for outcome in replay.outcomes.values()
                    for step, evidence in outcome.steps.items()
                    if step in outcome.required
                )
                if (
                    not replay.ignored and replay.restart_state != "complete"
                    and (replay.interrupted or replay.status == RunStatus.CANCELLED.value
                         or failed_work or replay.restart_state == "pending")
                    and not self._active(lineage, target, replay.plan, replay.scratch_workspace_id)
                    and "*" not in self._cleaning
                    and not self._cleaning.intersection(keys)
                ):
                    latest.append(replay)
            return latest

    @contextlib.contextmanager
    def saved_action_claim(
        self, run_id: str, *, directory: Path, destructive: bool = False,
        **expected_identity: str,
    ) -> Iterator[journal.Replay]:
        """Lock a saved attempt and its workspaces against resume, cleanup and other actions."""
        with self._lock:
            replay = journal.read(directory / f"{run_id}.jsonl")
            if not replay.plan:
                raise RunConflict(run_id, "The saved migration journal cannot be read.")
            journal.validate_replay_binding(replay, **expected_identity)
            if replay.damaged_lines:
                raise RunConflict(
                    run_id, "Repair the damaged journal before changing saved migration state.",
                )
            latest = journal.latest_runs(directory)
            if run_id not in {entry.run_id for entry in latest}:
                raise RunConflict(
                    run_id, "This attempt was superseded. Select the latest saved migration.",
                )
            lineage = replay.lineage_id or run_id
            target, scratch = replay.target_workspace_id, replay.scratch_workspace_id
            active = self._active(lineage, target, replay.plan, scratch)
            if active:
                raise RunConflict(
                    active.id, "Wait for the related migration to stop before using this action.",
                )
            keys = self._cleanup_keys(replay.plan, lineage, target, scratch)
            if "*" in self._cleaning or self._cleaning.intersection(keys):
                raise RunConflict(
                    run_id, "Another saved-migration action or cleanup is already in progress.",
                )
            if destructive:
                for entry in journal.list_runs(directory):
                    if entry.ownership_error:
                        raise journal.TenantBindingError(entry.ownership_error)
                    related = self._related_saved(replay, entry)
                    own_ancestor = (
                        entry.run_id in replay.ancestors
                        and journal.binding_scope(entry.plan) == journal.binding_scope(replay.plan)
                    )
                    if related and entry.run_id != run_id and not own_ancestor:
                        raise RunConflict(
                            entry.run_id, "Another saved migration also owns these workspaces. "
                            "Reconcile that migration before deleting anything.",
                        )
                    if related and not own_ancestor and entry.copy_jobs:
                        raise RunConflict(
                            entry.run_id, "Copy Jobs may still be running. Resume and reconcile their "
                            "recorded status before deleting the destination.",
                        )
                for attempt in self._runs.values():
                    own_ancestor = (
                        attempt.id in replay.ancestors
                        and journal.binding_scope(attempt.plan) == journal.binding_scope(replay.plan)
                    )
                    if (
                        not own_ancestor and self._related(attempt, lineage, target, replay.plan, scratch)
                        and attempt.summary.get("unresolvedCopyJobs")
                    ):
                        raise RunConflict(
                            attempt.id, "Reconcile unresolved Copy Jobs before full restart.",
                        )
            self._cleaning.update(keys)
        try:
            yield replay
        finally:
            with self._lock:
                self._cleaning.difference_update(keys)

    @staticmethod
    def _related_saved(left: journal.Replay, right: journal.Replay) -> bool:
        left_workspaces = {
            journal.target_scope(left.plan, value)
            for value in (left.target_workspace_id, left.scratch_workspace_id) if value
        }
        right_workspaces = {
            journal.target_scope(right.plan, value)
            for value in (right.target_workspace_id, right.scratch_workspace_id) if value
        }
        return (
            left.lineage_id == right.lineage_id
            and journal.binding_scope(left.plan) == journal.binding_scope(right.plan)
        ) or bool(left_workspaces.intersection(right_workspaces))

    @contextlib.contextmanager
    def cleanup_claim(
        self, run: MigrationRun | None = None, *, directory: Path | None = None,
        **expected_identity: str,
    ) -> Iterator[None]:
        """Serialize manual cleanup with admission, including cleanup through an ancestor."""
        if run is None and journal.tenant_binding(expected_identity):
            raise journal.TenantBindingError(
                "Paired cleanup must name a recorded run; global name-prefix cleanup is not permitted."
            )
        plan = run.plan if run else {}
        paired = journal.validate_tenant_binding(plan, **expected_identity)
        lineage = run.lineage_id if run else ""
        target = str((run.target_workspace or {}).get("id") or "") if run else ""
        scratch = str((run.scratch_workspace or {}).get("id") or "") if run else ""
        keys = self._cleanup_keys(plan, lineage, target, scratch) if run else {"*"}
        with self._lock:
            if paired:
                if directory is None:
                    raise journal.TenantBindingError(
                        "Paired cleanup requires the recorded journal directory."
                    )
                recorded = journal.read(directory / f"{run.id}.jsonl")
                journal.validate_replay_binding(recorded, **expected_identity)
                journal.validate_resume_plan(plan, recorded)
                if target and target != recorded.target_workspace_id:
                    raise journal.TenantBindingError(
                        "The cleanup destination does not match the recorded workspace."
                    )
                if scratch and scratch != recorded.owned_workspace_id(
                    "scratch", tenant_id=expected_identity["target_tenant_id"],
                    client_id=expected_identity["target_client_id"],
                ):
                    raise journal.TenantBindingError(
                        "The scratch workspace is not owned by this run; refusing cleanup."
                    )
            active = self._active(lineage, target, plan, scratch)
            if active:
                raise RunConflict(active.id, "Wait for the related migration to finish before cleanup.")
            latest = journal.latest_runs(directory) if directory is not None else []
            if not paired and any(replay.tenant_binding for replay in latest):
                raise journal.TenantBindingError(
                    "Paired cleanup must name a recorded run and its original tenant/application pair; "
                    "global name-prefix cleanup is not permitted."
                )
            superseded = {
                (journal.binding_scope(replay.plan), ancestor)
                for replay in latest if not replay.ownership_error for ancestor in replay.ancestors
            }
            for attempt in self._runs.values():
                if (
                    (journal.binding_scope(attempt.plan), attempt.id) not in superseded
                    and attempt.summary.get("unresolvedCopyJobs")
                ) and (
                    not run or self._related(attempt, lineage, target, plan, scratch)
                ):
                    raise RunConflict(
                        attempt.id, "Unfinished Copy Jobs may still be running. Reconcile their "
                        "recorded IDs before deleting the scratch workspace."
                    )
            if directory is not None:
                for replay in latest:
                    if replay.ownership_error:
                        raise journal.TenantBindingError(replay.ownership_error)
                    if replay.copy_jobs and (
                        not run or (
                            replay.lineage_id == lineage
                            and journal.binding_scope(replay.plan) == journal.binding_scope(plan)
                        ) or (
                            target and replay.target_workspace_id
                            and journal.target_scope(replay.plan, replay.target_workspace_id)
                            == journal.target_scope(plan, target)
                        ) or (
                            scratch and replay.scratch_workspace_id
                            and journal.target_scope(replay.plan, replay.scratch_workspace_id)
                            == journal.target_scope(plan, scratch)
                        )
                    ):
                        raise RunConflict(
                            replay.run_id, "Unfinished Copy Jobs may still be running. Resume and "
                            "reconcile that attempt before deleting its scratch workspace."
                        )
            if "*" in self._cleaning or self._cleaning.intersection(keys) or (
                not run and self._cleaning
            ):
                raise RunConflict(run.id if run else "", "Workspace cleanup is already in progress.")
            self._cleaning.update(keys)
        try:
            yield
        finally:
            with self._lock:
                self._cleaning.difference_update(keys)

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
    "RunConflict",
    "RunRegistry",
    "RunStatus",
    "Step",
    "StepStatus",
]
