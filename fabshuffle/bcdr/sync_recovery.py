"""Retain sync intent before capture and recover only under existing ownership guards."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from uuid import uuid4

from fabshuffle.bcdr.activity import report_activity
from fabshuffle.bcdr.backend import RecoveryBlocked, now
from fabshuffle.bcdr.contracts import RecoveryMode
from fabshuffle.bcdr.service import SyncRequest
from fabshuffle.lifecycle import safe_text

logger = logging.getLogger(__name__)
SYNC_ATTEMPT = "sync-attempt"


def sync_recovery_state(coordinator, state=None) -> dict:
    state = state or coordinator.catalog.state()
    pending = coordinator.catalog.pending_operations()
    attempt = coordinator.runtime.get("lifecycle", SYNC_ATTEMPT)
    recoverable_mode = state.mode in {RecoveryMode.STANDBY, RecoveryMode.SYNCING}
    interrupted = recoverable_mode and (
        state.mode == RecoveryMode.SYNCING or bool(attempt and attempt.get("status") == "failed")
        or state.controller_id is not None or bool(pending)
    )
    idle = state.controller_id is None and not pending
    request = SyncRequest.model_validate(attempt["request"]) if attempt and "request" in attempt else None
    can_retry = recoverable_mode and interrupted and idle and request is not None
    legacy = state.mode == RecoveryMode.SYNCING and idle and request is None
    can_release = recoverable_mode and state.controller_id is not None and not pending
    if not recoverable_mode:
        message = f"Recovery is in {state.mode.value}. Continue that workflow before changing standby scope."
    elif pending:
        message = "Reconcile the uncertain or unfinished operation receipt before retrying."
    elif state.controller_id:
        message = (
            "The catalog still records a controller owner. "
            "Stop/fence its worker before reconciling ownership."
        )
    elif can_retry:
        message = (
            "The interrupted sync can retry its recorded selection. "
            "Existing receipts and targets are retained."
        )
    elif legacy:
        message = (
            "This earlier failed run did not retain its request. Choose and review the intended selection "
            "to resume safely; existing owned targets are retained."
        )
    else:
        message = "No safely retryable interrupted sync is recorded."
    return {
        "mode": state.mode.value, "attempt_id": attempt.get("attempt_id") if attempt else None,
        "interrupted": interrupted,
        "phase": attempt.get("phase") if attempt else None,
        "error": attempt.get("error") if attempt else None,
        "scope": attempt.get("scope") if attempt else None,
        "can_retry": can_retry, "requires_selection": legacy,
        "can_reconcile_owner": can_release,
        "controller_id": state.controller_id, "controller_epoch": state.epoch,
        "pending_operation_count": len(pending), "message": message,
    }


@contextmanager
def track_sync_attempt(coordinator, request, *, scheduled, scope_names=()):
    runtime = coordinator.runtime
    previous = runtime.get("lifecycle", SYNC_ATTEMPT)
    document = request.model_dump(mode="json")
    if runtime.mode == RecoveryMode.SYNCING and previous and previous.get("request") != document:
        raise RecoveryBlocked(
            "Use Retry saved sync for the interrupted selection; do not replace its scope."
        )
    same = previous and previous.get("request") == document
    names = list(scope_names) or (previous.get("scope", {}).get("workspace_names", []) if same else [])
    attempt = {
        "attempt_id": (
            previous["attempt_id"] if same and runtime.mode == RecoveryMode.SYNCING else str(uuid4())
        ),
        "request": document, "status": "running", "phase": "Preparing synchronization",
        "started_at": now().isoformat(), "kind": "scheduled" if scheduled else "manual", "error": None,
        "scope": {
            "workspace_names": [safe_text(str(name))[:256] for name in names[:50]],
            "exact_workspace_count": len(
                set(request.include_workspace_ids) | set(request.approved_addition_ids)
            ),
            "name_rules": list(request.keywords),
            "all_in_scope": not request.include_workspace_ids and not request.keywords,
            "exclusion_count": len(request.exclude_workspace_ids),
        },
    }
    runtime.put("lifecycle", SYNC_ATTEMPT, attempt)
    try:
        yield attempt
    except Exception as error:
        message = safe_text(str(error))
        logger.error("BCDR sync attempt %s failed: %s", attempt["attempt_id"], message)
        attempt.update(status="failed", error=message, failed_at=now().isoformat())
        if runtime.mode != RecoveryMode.PARKING:
            try:
                runtime.put("lifecycle", SYNC_ATTEMPT, attempt)
            except Exception as record_error:
                logger.error(
                    "Could not persist sync failure; retain ownership/receipts and inspect activity: %s",
                    safe_text(str(record_error)),
                )
        raise


def sync_phase(coordinator, attempt, phase, *, metadata_complete=False):
    report_activity(phase)
    attempt.update(phase=phase, updated_at=now().isoformat())
    if metadata_complete:
        attempt["status"] = "metadata_completed"
    coordinator.runtime.put("lifecycle", SYNC_ATTEMPT, attempt)


def retry_saved_sync(coordinator, request):
    coordinator._wake()
    recovery = sync_recovery_state(coordinator)
    if not recovery["can_retry"] or recovery["attempt_id"] != request.expected_attempt_id:
        raise RecoveryBlocked("Load the current sync recovery state before retrying its recorded attempt.")
    attempt = coordinator.runtime.get("lifecycle", SYNC_ATTEMPT)
    saved = SyncRequest.model_validate(attempt["request"])
    return coordinator.synchronize(
        saved, scheduled=False, scope_names=attempt.get("scope", {}).get("workspace_names", ()),
    )


def reconcile_sync_owner(coordinator, request):
    from fabshuffle.bcdr.service import ServiceResult

    coordinator._wake()
    if request.previous_controller_stopped is not True:
        raise RecoveryBlocked("Stop and fence the previous worker before reconciling ownership.")
    state = coordinator.catalog.state()
    if (
        state.mode not in {RecoveryMode.STANDBY, RecoveryMode.SYNCING}
        or coordinator.catalog.pending_operations()
    ):
        raise RecoveryBlocked("Only sync ownership without pending mutation receipts can be released here.")
    lease = coordinator.catalog.takeover_controller(
        coordinator.runtime.controller_id,
        expected_controller_id=request.expected_controller_id,
        expected_epoch=request.expected_epoch,
        fencing_evidence=request.fencing_evidence,
        operation_id=str(uuid4()),
    )
    if coordinator.catalog.pending_operations():
        raise RecoveryBlocked(
            "A mutation receipt appeared during ownership reconciliation. "
            "Ownership remains fenced; reconcile the exact operation before retrying."
        )
    coordinator.catalog.release_controller(lease)
    return ServiceResult(
        mode=state.mode,
        details={"sync_recovery": sync_recovery_state(coordinator)},
        warnings=(
            "Interrupted controller ownership was reconciled. No sync was started, "
            "no receipts were cleared and no resources were recreated.",
        ),
    )
