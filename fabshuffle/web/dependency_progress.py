"""Bounded in-memory progress for a read-only dependency assessment."""

from __future__ import annotations

import secrets
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from fabshuffle.lifecycle import safe_text
from fabshuffle.run import Step, StepStatus

MAX_ASSESSMENTS = 16
MAX_ACTIVE_ASSESSMENTS = 4
ASSESSMENT_TTL_SECONDS = 600


class AssessmentCancelled(Exception):
    """Raised between dependency reads when review progress was cancelled."""


@dataclass
class DependencyAssessment:
    id: str
    session_id: str
    source_workspace_id: str
    rebuild: bool
    created_at: float = field(default_factory=time.monotonic)
    status: str = "pending"
    step: Step = field(default_factory=lambda: Step(
        id="dependencies", title="Checking dependencies (read-only review)",
    ))
    result: dict[str, Any] | None = None
    error: str | None = None
    _cancel: threading.Event = field(default_factory=threading.Event, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _worker_active: bool = True

    def start(self) -> None:
        with self._lock:
            self.status = "running"
            self.step.status = StepStatus.RUNNING
            self.step.started_at = self._now()
            self.step.detail = "Preparing the read-only dependency check."

    def update(self, detail: str) -> None:
        with self._lock:
            self.step.detail = safe_text(str(detail))[:500]

    def check_cancelled(self) -> None:
        if self._cancel.is_set():
            raise AssessmentCancelled

    def on_item(self, item: Mapping[str, Any], action: str) -> None:
        if self._cancel.is_set():
            raise AssessmentCancelled
        item_id = str(item.get("id") or "")
        if not item_id:
            return
        state = (
            "running" if action.startswith("Reading")
            else "failed" if "failed" in action.casefold()
            else "unknown" if "unavailable" in action.casefold()
            else "succeeded"
        )
        with self._lock:
            if self._cancel.is_set():
                raise AssessmentCancelled
            self.step.record_activity(
                item_id,
                str(item.get("displayName") or item_id),
                str(item.get("type") or ""),
                action,
                state,
                "item",
                counted=self.step.activity_operation in (None, "item"),
            )
            self.step.detail = safe_text(action)[:500]

    def set_activity_total(self, total: int) -> None:
        with self._lock:
            self.step.activity_total = max(0, int(total))
            self.step.activity_operation = "item"

    def complete(self, result: dict[str, Any]) -> None:
        with self._lock:
            self.result = deepcopy(result)
            self.status = "succeeded"
            self.step.status = StepStatus.SUCCEEDED
            self.step.finished_at = self._now()
            self.step.detail = "Read-only dependency assessment complete."
            self._worker_active = False

    def fail(self, error: BaseException) -> None:
        with self._lock:
            self.step.close_active_activity()
            self.error = safe_text(str(error))[:1000] or type(error).__name__
            self.status = "failed"
            self.step.status = StepStatus.FAILED
            self.step.finished_at = self._now()
            self.step.detail = self.error
            self.step.warnings.append(self.error)
            self._worker_active = False

    def cancel(self) -> None:
        with self._lock:
            if self._worker_active and self.status in ("pending", "running", "cancelling"):
                self._cancel.set()
                self.status = "cancelling"
                self.step.detail = "Stopping after the current Fabric request."

    def cancelled(self) -> None:
        with self._lock:
            self.step.close_active_activity()
            self.status = "cancelled"
            self.step.status = StepStatus.SKIPPED
            self.step.finished_at = self._now()
            self.step.detail = "Read-only dependency check stopped; no further reads were started."
            self._worker_active = False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "assessmentId": self.id,
                "status": self.status,
                "step": self.step.as_dict(),
                "result": deepcopy(self.result),
                "error": self.error,
            }

    @staticmethod
    def _now() -> str:
        from datetime import UTC, datetime

        return datetime.now(UTC).isoformat()


class DependencyAssessmentRegistry:
    """Session-scoped assessments with bounded active work and terminal-state retention."""

    def __init__(self) -> None:
        self._assessments: OrderedDict[str, DependencyAssessment] = OrderedDict()
        self._lock = threading.RLock()

    def create(
        self, session_id: str, source_workspace_id: str, rebuild: bool,
    ) -> tuple[DependencyAssessment, bool]:
        with self._lock:
            self._prune()
            for entry in self._assessments.values():
                if (
                    entry.session_id == session_id
                    and entry.source_workspace_id == source_workspace_id
                    and entry.rebuild == rebuild
                    and entry._worker_active
                ):
                    return entry, True
            if sum(entry._worker_active for entry in self._assessments.values()) >= MAX_ACTIVE_ASSESSMENTS:
                raise RuntimeError("Too many dependency assessments are already running; wait and retry.")
            if len(self._assessments) >= MAX_ASSESSMENTS:
                removable = next(
                    (key for key, entry in self._assessments.items() if not entry._worker_active),
                    None,
                )
                if removable is None:
                    raise RuntimeError("Dependency assessment capacity is full; wait and retry.")
                del self._assessments[removable]
            assessment = DependencyAssessment(
                id=secrets.token_urlsafe(24),
                session_id=session_id,
                source_workspace_id=source_workspace_id,
                rebuild=rebuild,
            )
            self._assessments[assessment.id] = assessment
            return assessment, False

    def get(self, session_id: str, assessment_id: str) -> DependencyAssessment | None:
        with self._lock:
            self._prune()
            assessment = self._assessments.get(assessment_id)
            if assessment is None or assessment.session_id != session_id:
                return None
            self._assessments.move_to_end(assessment_id)
            return assessment

    def cancel_session(self, session_id: str) -> None:
        with self._lock:
            assessments = tuple(self._assessments.values())
        for assessment in assessments:
            if assessment.session_id == session_id:
                assessment.cancel()

    def _prune(self) -> None:
        now = time.monotonic()
        for key, assessment in list(self._assessments.items()):
            if (
                not assessment._worker_active
                and now - assessment.created_at > ASSESSMENT_TTL_SECONDS
            ):
                del self._assessments[key]
