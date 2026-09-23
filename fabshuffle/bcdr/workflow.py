"""Operator workflow facts derived from the authoritative recovery catalog."""

from __future__ import annotations

import os

from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.contracts import RecoveryMode, reject_embedded_secrets
from fabshuffle.bcdr.deployment_lock import LEASE_ENV
from fabshuffle.bcdr.service import ScheduleGuideRequest, ServiceResult, SyncRequest


def workflow_summary(coordinator) -> dict:
    state = coordinator.catalog.state()
    generation_id = state.current_generation_id
    groups = coordinator._groups(generation_id) if generation_id else ()
    pending = coordinator.catalog.pending_operations()
    baseline = bool(groups) and all(group.metadata_applied for group in groups)
    idle = state.mode == RecoveryMode.STANDBY and state.controller_id is None and not pending
    test = coordinator.runtime.get("lifecycle", "dr-test")
    if test and state.mode in {RecoveryMode.TESTING, RecoveryMode.ENDING_TEST}:
        test = {**test, "phase": state.mode.value}
    last_sync = coordinator.runtime.get("lifecycle", "last-sync")
    gaps = [group.group_id for group in groups if not group.data_ready]
    return {
        "catalog_ready": True, "metadata_baseline_ready": baseline,
        "generation_id": generation_id,
        "observed_mode": state.mode.value, "recovery_gap_groups": gaps,
        "pending_operations": len(pending), "controller_busy": state.controller_id is not None,
        "last_sync": last_sync,
        "last_scheduled_sync": coordinator.runtime.get("lifecycle", "last-scheduled-sync"),
        "test": test,
        "schedule_eligible": idle and baseline,
        "test_eligible": idle and baseline,
        "schedule_reason": (
            "Review the scope and prepare scheduled-sync instructions." if idle and baseline else
            "Complete the metadata sync and reconcile active or interrupted work before scheduling."
        ),
        "remote_lease_configured": bool(os.environ.get(LEASE_ENV)),
        "scheduler_deployment": "not_verified",
        "next_step": (
            "test" if state.mode in {RecoveryMode.TESTING, RecoveryMode.ENDING_TEST} else
            "incident" if state.mode in {
                RecoveryMode.ENABLING_RECOVERY, RecoveryMode.ACTIVE_RECOVERY,
                RecoveryMode.FAILING_BACK, RecoveryMode.REARMING,
            } else "reconcile" if pending or state.controller_id else
            "schedule" if baseline else "initial_sync"
        ),
    }


def schedule_guide(coordinator, request: ScheduleGuideRequest) -> ServiceResult:
    from fabshuffle.bcdr.scheduled import MAX_REQUEST_BYTES, validate_request

    coordinator._wake()
    with coordinator.runtime.controller({RecoveryMode.STANDBY}):
        state = coordinator.catalog.state()
        groups = coordinator._groups(state.current_generation_id) if state.current_generation_id else ()
        if (
            request.generation_id != state.current_generation_id
            or not groups or not all(group.metadata_applied for group in groups)
        ):
            raise RecoveryBlocked("Complete metadata sync and review its current generation first.")
        saved = coordinator.runtime.get("plans", request.generation_id)
        if saved is None:
            raise RecoveryBlocked("The completed generation has no reviewed synchronization scope.")
        sync = SyncRequest.model_validate(saved).model_copy(update={
            "capture": True, "park": True, "generation_id": None,
        })
        validate_request(sync)
        document = sync.model_dump_json()
        if len(document.encode("utf-8")) > MAX_REQUEST_BYTES:
            raise RecoveryBlocked("The scheduled request is too large; reduce the approved scope.")
        reject_embedded_secrets(document.encode("utf-8"))
        generation = coordinator.catalog.load_generation(request.generation_id)
        selected_workspaces = {item.workspace_id for group in groups for item in group.items}
        return ServiceResult(
            mode=state.mode, generation_id=request.generation_id,
            warnings=(
                "Instructions prepared only. No scheduled job was created or enabled.",
                "Metadata sync does not establish data readiness; review the remaining workload gaps.",
            ),
            details={"schedule_guide": {
                "request_filename": "scheduled-sync-request.json",
                "workspace_names": [
                    workspace.display_name for workspace in generation.snapshot.workspaces
                    if workspace.identity.workspace_id in selected_workspaces
                ],
                "request": sync.model_dump(mode="json"), "schedule_utc": request.schedule_utc,
                "deployment_status": "not_verified",
                "remote_lease_configured": bool(os.environ.get(LEASE_ENV)),
                "command": (
                    "python -m fabshuffle.bcdr scheduled-sync --bootstrap /app/local/bcdr/bootstrap.json "
                    "--request /app/local/bcdr/scheduled-sync-request.json --confirm scheduled-sync"
                ),
                "template_url": "https://github.com/cbattlegear/fab-shuffle/blob/main/deploy/azuredeploy-sync-job.json",
                "guide_url": "https://github.com/cbattlegear/fab-shuffle/blob/main/docs/bcdr.md",
                "steps": [
                    "Review and download this scope. Recurring runs capture fresh metadata and park safely.",
                    "Place the request beside bootstrap on shared storage; never overwrite bootstrap.",
                    "Use the same recovery identity and exact remote lease URL on controller and scheduler.",
                    "Use the sync-job template from the SAME reviewed release, not an older main/latest.",
                    "Use the web deployment's environment, identity, lease outputs "
                    "and production image digest.",
                    "Review the UTC cron. Prepare storage, identity and lease prerequisites "
                    "before deployment.",
                    "Observe a completed scheduled execution before treating automatic sync as operational.",
                ],
            }},
        )
