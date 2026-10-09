"""In-process request observation, not catalog authority or a replacement lease."""

from __future__ import annotations

from collections import deque
from contextvars import ContextVar
from datetime import UTC, datetime
from threading import Lock
from time import monotonic
from uuid import uuid4

from fabshuffle.lifecycle import safe_text

CURRENT_ACTION: ContextVar[str] = ContextVar("bcdr_action", default="operation")
_CURRENT: ContextVar[tuple | None] = ContextVar("bcdr_activity", default=None)


def report_activity(phase: str | None = None, *, detail: str | None = None,
                    owns_deployment: bool | None = None) -> None:
    current = _CURRENT.get()
    if current:
        current[0].update(current[1], phase=phase, detail=detail, owns_deployment=owns_deployment)


class ActivityTracker:
    def __init__(self):
        self._lock = Lock()
        self._active = {}
        self._recent = deque(maxlen=40)

    def run(self, key, action, work):
        identifier = str(uuid4())
        record = {
            "id": identifier, "action": action, "phase": "Starting request", "detail": "",
            "status": "running", "owns_deployment": False,
            "started_at": datetime.now(UTC).isoformat(), "updated_at": datetime.now(UTC).isoformat(),
            "_key": key, "_start": monotonic(),
        }
        with self._lock:
            self._active[identifier] = record
        token = _CURRENT.set((self, identifier))
        try:
            result = work()
        except Exception as error:
            self._finish(identifier, "failed", safe_text(str(error)))
            raise
        else:
            outcome = getattr(result, "outcome", None)
            self._finish(identifier, "needs_attention" if outcome in {"partial", "blocked"} else "completed")
            return result
        finally:
            _CURRENT.reset(token)

    def update(self, identifier, *, phase=None, detail=None, owns_deployment=None):
        with self._lock:
            record = self._active.get(identifier)
            if record is None:
                return
            if phase is not None:
                record["phase"] = safe_text(phase)[:300]
                record["detail"] = ""
            if detail is not None:
                record["detail"] = safe_text(detail)[:500]
            if owns_deployment is not None:
                record["owns_deployment"] = owns_deployment
            record["updated_at"] = datetime.now(UTC).isoformat()

    def _finish(self, identifier, status, error=None):
        with self._lock:
            record = self._active.pop(identifier)
            if record["phase"] == "Starting request":
                record["phase"] = "Request finished"
            record.update(
                status=status, error=error, owns_deployment=False,
                finished_at=datetime.now(UTC).isoformat(),
                elapsed_seconds=round(monotonic() - record["_start"], 1),
            )
            self._recent.append(record)

    def snapshot(self, key):
        def public(record):
            result = {name: value for name, value in record.items() if not name.startswith("_")}
            if record["status"] == "running":
                result["elapsed_seconds"] = round(monotonic() - record["_start"], 1)
            return result

        with self._lock:
            active = [public(record) for record in self._active.values() if record["_key"] == key]
            recent = [public(record) for record in reversed(self._recent) if record["_key"] == key][:8]
        return {
            "coverage": "this_web_process", "active": active, "recent": recent,
            "message": (
                "Observation only: no catalog read, capacity resume or deployment lock is acquired. "
                "Other processes and pre-restart requests are not observed here."
            ),
        }


ACTIVITY = ActivityTracker()
