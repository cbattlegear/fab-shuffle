"""Capacity and workspace operations."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from fabshuffle.fabric.client import FabricApiError, FabricClient

# ------------------------------------------------------------------- capacities


def list_capacities(client: FabricClient) -> list[dict[str, Any]]:
    """Return every capacity the service principal can see, active ones first."""
    capacities = client.list_all("capacities")
    return sorted(
        capacities,
        key=lambda c: (c.get("state") != "Active", (c.get("displayName") or "").lower()),
    )


def get_capacity(client: FabricClient, capacity_id: str) -> dict[str, Any]:
    return client.get(f"capacities/{capacity_id}")


def capacity_region(capacity: dict[str, Any]) -> str:
    """Normalise a capacity region such as ``West Central US`` into ``westcentralus``."""
    region = capacity.get("region") or capacity.get("location") or ""
    return region.replace(" ", "").lower()


# ------------------------------------------------------------------- workspaces


def list_workspaces(client: FabricClient) -> list[dict[str, Any]]:
    workspaces = client.list_all("workspaces")
    return sorted(workspaces, key=lambda w: (w.get("displayName") or "").lower())


def get_workspace(client: FabricClient, workspace_id: str) -> dict[str, Any]:
    return client.get(f"workspaces/{workspace_id}")


def find_workspace_by_name(client: FabricClient, display_name: str) -> dict[str, Any] | None:
    for workspace in client.paged("workspaces"):
        if workspace.get("displayName") == display_name:
            return workspace
    return None


def create_workspace(
    client: FabricClient,
    display_name: str,
    capacity_id: str,
    description: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"displayName": display_name, "capacityId": capacity_id}
    if description:
        body["description"] = description
    return client.post("workspaces", json=body)


def delete_workspace(client: FabricClient, workspace_id: str) -> None:
    client.delete(f"workspaces/{workspace_id}")


# ------------------------------------------------------------- role assignments


def list_role_assignments(client: FabricClient, workspace_id: str) -> list[dict[str, Any]]:
    """Read workspace RBAC.

    Prefers the non-admin ``roleAssignments`` endpoint, which only needs workspace
    Admin rather than the tenant-wide Fabric administrator role the ``/admin`` API
    demands. Falls back to the admin API when the caller happens to have it.
    """
    try:
        return client.list_all(f"workspaces/{workspace_id}/roleAssignments")
    except FabricApiError as error:
        if error.status_code not in (401, 403):
            raise

    payload = client.get(f"admin/workspaces/{workspace_id}/users")
    assignments: list[dict[str, Any]] = []
    for detail in payload.get("accessDetails") or []:
        role = (detail.get("workspaceAccessDetails") or {}).get("workspaceRole")
        principal = detail.get("principal")
        if role and principal:
            assignments.append({"principal": principal, "role": role})
    return assignments


def add_role_assignment(
    client: FabricClient,
    workspace_id: str,
    principal_id: str,
    principal_type: str,
    role: str,
) -> dict[str, Any]:
    return client.post(
        f"workspaces/{workspace_id}/roleAssignments",
        json={"principal": {"id": principal_id, "type": principal_type}, "role": role},
    )


def copy_role_assignments(
    client: FabricClient,
    assignments: Iterable[dict[str, Any]],
    target_workspace_id: str,
    *,
    roles: set[str] | None = None,
    principal_types: set[str] | None = None,
) -> list[str]:
    """Replay source role assignments onto ``target_workspace_id``.

    Returns human readable warnings for the assignments that could not be copied,
    rather than aborting the migration over a single unresolvable principal.
    """
    warnings: list[str] = []
    for assignment in assignments:
        principal = assignment.get("principal") or {}
        principal_id = principal.get("id")
        principal_type = principal.get("type")
        role = assignment.get("role")

        if not principal_id or not principal_type or not role:
            continue
        if roles and role not in roles:
            continue
        if principal_types and principal_type not in principal_types:
            continue

        try:
            add_role_assignment(client, target_workspace_id, principal_id, principal_type, role)
        except FabricApiError as error:
            name = principal.get("displayName") or principal_id
            warnings.append(f"Could not grant {role} to {name}: HTTP {error.status_code}")
    return warnings


# ---------------------------------------------------------------------- folders


def list_folders(client: FabricClient, workspace_id: str) -> list[dict[str, Any]]:
    try:
        return client.list_all(f"workspaces/{workspace_id}/folders")
    except FabricApiError as error:
        if error.status_code in (400, 404):
            return []
        raise


def create_folder(
    client: FabricClient,
    workspace_id: str,
    display_name: str,
    parent_folder_id: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"displayName": display_name}
    if parent_folder_id:
        body["parentFolderId"] = parent_folder_id
    return client.post(f"workspaces/{workspace_id}/folders", json=body)


def clone_folder_tree(
    client: FabricClient,
    source_workspace_id: str,
    target_workspace_id: str,
) -> dict[str, str]:
    """Recreate the source workspace folder hierarchy, returning old id -> new id."""
    source_folders = list_folders(client, source_workspace_id)
    if not source_folders:
        return {}

    by_parent: dict[str | None, list[dict[str, Any]]] = {}
    for folder in source_folders:
        by_parent.setdefault(folder.get("parentFolderId"), []).append(folder)

    mapping: dict[str, str] = {}

    def create_level(parent_id: str | None) -> None:
        for folder in by_parent.get(parent_id, []):
            new_parent = mapping.get(parent_id) if parent_id else None
            created = create_folder(client, target_workspace_id, folder["displayName"], new_parent)
            mapping[folder["id"]] = created["id"]
            create_level(folder["id"])

    create_level(None)
    return mapping


__all__ = [
    "add_role_assignment",
    "capacity_region",
    "clone_folder_tree",
    "copy_role_assignments",
    "create_folder",
    "create_workspace",
    "delete_workspace",
    "find_workspace_by_name",
    "get_capacity",
    "get_workspace",
    "list_capacities",
    "list_folders",
    "list_role_assignments",
    "list_workspaces",
]
