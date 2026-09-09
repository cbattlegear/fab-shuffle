"""Confirmed, journal-backed actions on stopped migrations."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fabshuffle import journal
from fabshuffle.fabric import workspaces
from fabshuffle.fabric.client import FabricApiError, FabricClient


def restart_blocker(replay: journal.Replay) -> str:
    if replay.ignored:
        return "This saved migration was ignored."
    if replay.plan.get("strategy", "rebuild") != "rebuild":
        return "A reassignment uses the source workspace itself and cannot be deleted for a restart."
    source = str(replay.plan.get("source_workspace_id") or "")
    if not source:
        return "The journal does not identify the source workspace."
    if any(
        value.casefold() == source.casefold()
        for value in (replay.target_workspace_id, replay.scratch_workspace_id) if value
    ):
        return "The recorded destination or scratch workspace is the source; deletion is refused."
    if replay.copy_jobs:
        return "Copy Jobs may still be running. Resume and reconcile them before full restart."
    return ""


def _workspace_exists(client: FabricClient, workspace_id: str) -> bool:
    try:
        workspace = workspaces.get_workspace(client, workspace_id)
    except FabricApiError as error:
        # Only an explicit service diagnosis counts as absence. A bare 404 or any access
        # failure must not be turned into a successful deletion.
        if error.status_code == 404 and error.error_code == "WorkspaceNotFound":
            return False
        raise
    if str(workspace.get("id") or "").casefold() != workspace_id.casefold():
        raise ValueError("The service returned a different workspace; full restart was refused.")
    return True


def full_restart(
    replay: journal.Replay,
    book: journal.Journal,
    *,
    source_client: FabricClient,
    target_client: FabricClient,
    confirmed_target_id: str,
    identity: Mapping[str, str],
) -> dict[str, Any]:
    """Delete only recorded workspace IDs; the caller holds the registry action claim."""
    journal.validate_replay_binding(replay, **identity)
    if confirmed_target_id != replay.target_workspace_id:
        raise ValueError(
            "The destination changed since confirmation. Reload the saved migrations and try again."
        )
    reason = restart_blocker(replay)
    if reason:
        raise ValueError(reason)
    source_id = str(replay.plan["source_workspace_id"])
    source = workspaces.get_workspace(source_client, source_id)
    if str(source.get("id") or "").casefold() != source_id.casefold():
        raise ValueError("The source workspace could not be verified. Nothing was deleted.")
    result = {
        "restartComplete": True, "runId": replay.run_id,
        "sourceWorkspace": {"id": source_id, "displayName": source.get("displayName") or source_id},
    }
    if replay.restart_state == "complete":
        return result

    # Preflight both resources before any mutation. Paired records additionally bind each
    # workspace to the authenticated destination tenant/application.
    pending = []
    for role, workspace_id in (
        ("scratch", replay.scratch_workspace_id), ("target", replay.target_workspace_id),
    ):
        if not workspace_id or workspace_id in replay.deleted_workspaces:
            continue
        if replay.tenant_binding:
            replay.owned_workspace_id(
                role, tenant_id=identity["target_tenant_id"], client_id=identity["target_client_id"],
            )
        pending.append((workspace_id, _workspace_exists(target_client, workspace_id)))
    if not replay.restart_state:
        book.recovery_action("restart_started")
    recorded = set(replay.deleted_workspaces)
    for workspace_id, exists in pending:
        if workspace_id in recorded:
            continue
        if exists:
            # Delete Workspace documents 200 completion, not an asynchronous acceptance.
            # https://learn.microsoft.com/rest/api/fabric/core/workspaces/delete-workspace
            try:
                target_client.request("DELETE", f"workspaces/{workspace_id}", expected=(200,))
            except FabricApiError as error:
                if error.status_code != 404 or error.error_code != "WorkspaceNotFound":
                    raise
        book.recovery_action("workspace_deleted", workspace_id=workspace_id)
        recorded.add(workspace_id)
    book.recovery_action("restart_completed")
    return result
