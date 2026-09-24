"""Operator workflow facts derived from the authoritative recovery catalog."""

from __future__ import annotations

import logging
import os

from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.contracts import RecoveryMode, canonical_json, digest, reject_embedded_secrets
from fabshuffle.bcdr.deployment_lock import LEASE_ENV
from fabshuffle.bcdr.protection_binding import owns_schema
from fabshuffle.bcdr.service import (
    CapacityRoute,
    ScheduleGuideRequest,
    ServiceResult,
    StandbyDefaultsRequest,
    StandbySelectionRequest,
    SyncRequest,
)
from fabshuffle.fabric import workspaces
from fabshuffle.lifecycle import safe_text

logger = logging.getLogger(__name__)


def _standby_configuration(coordinator):
    coordinator._wake()
    state = coordinator.catalog.state()
    if state.mode != RecoveryMode.STANDBY or state.controller_id or coordinator.catalog.pending_operations():
        raise RecoveryBlocked("Finish active work or reconcile interrupted operations before changing scope.")
    saved = (
        coordinator.runtime.get("plans", state.current_generation_id) if state.current_generation_id else None
    )
    previous = SyncRequest.model_validate(saved) if saved else SyncRequest(capacity_routes=())
    defaults = coordinator.runtime.get("lifecycle", "standby-defaults") or {}
    targets = coordinator.recovery_set.target_capacity_ids
    fallback = defaults.get("target_capacity_id") or (targets[0] if len(targets) == 1 else None)
    return state, previous, fallback


def _scope_workspaces(coordinator):
    if coordinator.source is None:
        raise RecoveryBlocked(
            "Connect source credentials to list workspaces. Incident recovery does not need this."
        )
    allowed = set(coordinator.recovery_set.source_capacity_ids)
    return [
        entry for entry in workspaces.list_workspaces(coordinator.source)
        if entry.get("type") == "Workspace" and entry.get("capacityId") in allowed
        and entry.get("id") != coordinator.recovery_set.control_workspace.workspace_id
    ]


def standby_scope_options(coordinator, *, include_workspaces: bool) -> ServiceResult:
    state, previous, fallback = _standby_configuration(coordinator)
    entries = _scope_workspaces(coordinator) if include_workspaces else []
    allowed_targets = set(coordinator.recovery_set.target_capacity_ids)
    targets = [
        {key: entry.get(key) for key in ("id", "displayName", "region")}
        for entry in workspaces.list_capacities(coordinator.destination) if entry.get("id") in allowed_targets
    ]
    saved_selection = None
    if previous.include_workspace_ids and not previous.keywords and not previous.exclude_workspace_ids:
        saved_selection = {
            "selection_mode": "workspaces",
            "workspace_ids": sorted(
                set(previous.include_workspace_ids) | set(previous.approved_addition_ids)
            ),
        }
    elif (
        len(previous.keywords) == 1 and not previous.include_workspace_ids
        and not previous.exclude_workspace_ids and not previous.approved_addition_ids
    ):
        saved_selection = {"selection_mode": "pattern", "name_pattern": previous.keywords[0]}
    return ServiceResult(mode=state.mode, details={"standby_scope": {
        "workspaces": [{key: entry.get(key) for key in ("id", "displayName", "description", "capacityRegion")}
                       for entry in entries],
        "target_capacities": targets, "default_target_id": fallback, "saved_selection": saved_selection,
        "message": "Choose workspaces in the configured source scope; reuse saved recovery settings.",
    }})


def _selection_recipe(coordinator, request: StandbySelectionRequest):
    state, previous, fallback = _standby_configuration(coordinator)
    entries = _scope_workspaces(coordinator)
    visible = {entry["id"]: entry for entry in entries}
    if request.selection_mode == "workspaces":
        if set(request.workspace_ids) - visible.keys():
            raise RecoveryBlocked(
                "A selected workspace is unavailable or outside the configured source scope. Reload the list."
            )
        selected = [visible[identifier] for identifier in sorted(request.workspace_ids)]
        keywords = ()
        included = tuple(sorted(request.workspace_ids))
        sources = {entry["capacityId"] for entry in selected}
    else:
        pattern = request.name_pattern.strip().casefold()
        selected = [entry for entry in entries if pattern in str(entry.get("displayName") or "").casefold()]
        keywords, included = (pattern,), ()
        sources = set(coordinator.recovery_set.source_capacity_ids)
    if not selected:
        raise RecoveryBlocked("No workspaces match. Choose workspaces or change the name rule.")
    allowed_targets = set(coordinator.recovery_set.target_capacity_ids)
    existing = {route.source_capacity_id: route.target_capacity_id for route in previous.capacity_routes}
    routes = []
    for source in sorted(sources):
        target = existing.get(source) or fallback
        if target not in allowed_targets:
            raise RecoveryBlocked(
                "Choose a default recovery capacity once in Settings, or configure an explicit route. "
                "No destination was guessed."
            )
        routes.append(CapacityRoute(source_capacity_id=source, target_capacity_id=target))
    sync = SyncRequest(
        include_workspace_ids=included, keywords=keywords, capacity_routes=tuple(routes),
        connection_mappings=previous.connection_mappings, suffix=previous.suffix,
        capture=True, park=True,
    )
    fingerprint = digest(canonical_json({
        "recovery_set": coordinator.recovery_set.recovery_set_id, "request": sync.model_dump(mode="json"),
    }))
    return state, sync, selected, fingerprint


def preview_standby_selection(coordinator, request: StandbySelectionRequest) -> ServiceResult:
    state, sync, selected, fingerprint = _selection_recipe(coordinator, request)
    for entry in selected:
        logger.info("BCDR scope selection: workspace=%s name=%r mode=%s",
                    entry["id"], safe_text(str(entry.get("displayName") or "")), request.selection_mode)
    return ServiceResult(mode=state.mode, details={"standby_preview": {
        "workspaces": [{"id": entry["id"], "displayName": entry.get("displayName") or "Unnamed workspace"}
                       for entry in selected],
        "configuration": fingerprint, "selection_mode": request.selection_mode,
        "name_pattern": request.name_pattern.strip(),
        "route_count": len(sync.capacity_routes),
        "target_capacity_ids": sorted({route.target_capacity_id for route in sync.capacity_routes}),
        "message": "Saved settings will be reused. The sync prepares stopped standby resources; no cutover.",
    }})


def create_standby(coordinator, request: StandbySelectionRequest) -> ServiceResult:
    _, sync, _, fingerprint = _selection_recipe(coordinator, request)
    if request.expected_configuration != fingerprint:
        raise RecoveryBlocked("The selection or settings changed. Review the matching workspaces again.")
    return coordinator.synchronize(sync, standby_only=True, scheduled=False)


def configure_standby_defaults(coordinator, request: StandbyDefaultsRequest) -> ServiceResult:
    coordinator._wake()
    with coordinator.runtime.controller({RecoveryMode.STANDBY}):
        if request.target_capacity_id not in coordinator.recovery_set.target_capacity_ids:
            raise RecoveryBlocked("Choose a recovery capacity already authorized in Settings.")
        coordinator.runtime.put("lifecycle", "standby-defaults", request.model_dump(mode="json"))
        return ServiceResult(
            mode=coordinator.runtime.mode,
            warnings=(
                "Default saved for sources without an existing route. "
                "No workspace was moved or synchronized.",
            ),
        )


def prepared_metadata_groups(coordinator, generation, groups) -> set[str]:
    """Owned provider shells are prepared metadata, not fully restored schema/data."""
    if generation is None:
        return set()
    mappings = coordinator.item_mappings()
    items = {item.identity.key: item for item in generation.snapshot.items}
    prepared = set()
    for group in groups:
        if not group.items or any(
            item.key not in items or item.key not in mappings
            or mappings[item.key].capture_generation_id != generation.snapshot.generation_id
            for item in group.items
        ):
            continue
        if group.metadata_applied:
            prepared.add(group.group_id)
            continue
        stored = coordinator.runtime.get("groups", group.group_id) or {}
        blockers = set(stored.get("metadata_blockers", group.blockers))
        deferred = {
            f"Complete provider-owned metadata for '{items[item.key].display_name}'"
            for item in group.items if owns_schema(generation, items[item.key], coordinator.catalog)
        }
        if blockers and blockers <= deferred:
            prepared.add(group.group_id)
    return prepared


def workflow_summary(coordinator, generation=None) -> dict:
    state = coordinator.catalog.state()
    generation_id = state.current_generation_id
    groups = coordinator._groups(generation_id) if generation_id else ()
    pending = coordinator.catalog.pending_operations()
    if generation is None and generation_id:
        generation = coordinator.catalog.load_generation(generation_id)
    prepared = prepared_metadata_groups(coordinator, generation, groups)
    baseline = bool(groups) and len(prepared) == len(groups)
    testable = [group.group_id for group in groups
                if group.group_id in prepared and not group.active and not group.access_enabled]
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
        "test_eligible": idle and bool(testable),
        "test_eligible_group_ids": testable,
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
        generation = (
            coordinator.catalog.load_generation(state.current_generation_id)
            if state.current_generation_id else None
        )
        prepared = prepared_metadata_groups(coordinator, generation, groups)
        if (
            request.generation_id != state.current_generation_id
            or not groups or len(prepared) != len(groups)
            or any(group.active or group.access_enabled for group in groups)
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
