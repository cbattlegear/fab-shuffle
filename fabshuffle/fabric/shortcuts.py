"""OneLake shortcut operations."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from fabshuffle.fabric.client import FabricApiError, FabricClient


def list_shortcuts(client: FabricClient, workspace_id: str, item_id: str) -> list[dict[str, Any]]:
    try:
        return client.list_all(f"workspaces/{workspace_id}/items/{item_id}/shortcuts")
    except FabricApiError as error:
        if error.status_code == 404:
            return []
        raise


def create_shortcut(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    shortcut: Mapping[str, Any],
    *,
    conflict_policy: str = "Abort",
) -> dict[str, Any]:
    body = {
        "path": shortcut["path"],
        "name": shortcut["name"],
        "target": shortcut["target"],
    }
    return client.post(
        f"workspaces/{workspace_id}/items/{item_id}/shortcuts",
        json=body,
        params={"shortcutConflictPolicy": conflict_policy},
    )


def remap_shortcut_target(
    shortcut: Mapping[str, Any],
    id_map: Mapping[str, str],
) -> dict[str, Any]:
    """Rewrite a shortcut so internal OneLake targets point at the migrated items.

    Only ``oneLake`` targets are rewritten. External targets (ADLS, S3, GCS, ...) reference a
    tenant-level ``connectionId`` that is region agnostic, so they are copied verbatim.
    """
    remapped = {
        "path": shortcut.get("path"),
        "name": shortcut.get("name"),
        "target": _remap_target(shortcut.get("target") or {}, id_map),
    }
    return remapped


def _remap_target(target: Mapping[str, Any], id_map: Mapping[str, str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for target_type, settings in target.items():
        # ``type`` is a discriminator string the create API does not accept back.
        if target_type == "type" or not isinstance(settings, Mapping):
            continue
        if target_type == "oneLake":
            result[target_type] = {
                **settings,
                "workspaceId": id_map.get(settings.get("workspaceId", ""), settings.get("workspaceId")),
                "itemId": id_map.get(settings.get("itemId", ""), settings.get("itemId")),
            }
        else:
            result[target_type] = dict(settings)
    return result


def _target_kind(target: Mapping[str, Any]) -> str:
    """The single target key a shortcut carries, such as ``oneLake`` or ``adlsGen2``."""
    return next((key for key in target if key != "type"), "")


def describe_failure(
    name: str,
    target: Mapping[str, Any],
    status_code: int,
    *,
    label: str = "Shortcut",
) -> str:
    """Explain why a shortcut could not be created.

    The status says which of three quite different things went wrong, and the fix differs for
    an internal OneLake target and an external one, so the message is built from both rather
    than blaming the connection for everything.
    """
    internal = _target_kind(target) == "oneLake"

    if status_code == 409:
        reason = (
            "a shortcut of that name already exists there. Delete it if you are re-running "
            "into a workspace that was already partly migrated"
        )
    elif status_code == 404 and internal:
        reason = (
            "the item it points at does not exist in the new workspace, so it was probably "
            "not migrated. Recreate the shortcut once that item is there"
        )
    elif status_code == 404:
        reason = "the path it points at was not found. Check the target still exists"
    elif status_code == 403 and internal:
        reason = (
            "this service principal cannot reach the item it points at. Grant it access to "
            "that workspace"
        )
    elif status_code == 403:
        reason = (
            "its connection denied access. The connection is tenant wide, so check the "
            "service principal is allowed to use it and its credentials are still valid"
        )
    else:
        reason = "the request was rejected. Recreate it by hand"

    return f"{label} '{name}' could not be created (HTTP {status_code}): {reason}."


def copy_shortcuts(
    client: FabricClient,
    source_workspace_id: str,
    source_item_id: str,
    target_workspace_id: str,
    target_item_id: str,
    id_map: Mapping[str, str],
) -> tuple[int, list[str]]:
    """Recreate every shortcut from a source item onto its migrated counterpart."""
    created = 0
    warnings: list[str] = []

    for shortcut in list_shortcuts(client, source_workspace_id, source_item_id):
        remapped = remap_shortcut_target(shortcut, id_map)
        if not remapped["target"]:
            warnings.append(f"Shortcut '{shortcut.get('name')}' has no recognised target, skipped")
            continue
        try:
            create_shortcut(client, target_workspace_id, target_item_id, remapped)
            created += 1
        except FabricApiError as error:
            warnings.append(
                describe_failure(
                    str(shortcut.get("name")), remapped["target"], error.status_code
                )
            )
    return created, warnings


# ------------------------------------------------------- KQL table shortcuts

# A KQL database exposes its table shortcuts on its own endpoint. They use the same target
# shape as OneLake shortcuts, but are named without a path and carry a query acceleration
# flag instead.


def list_table_shortcuts(
    client: FabricClient,
    workspace_id: str,
    database_id: str,
) -> list[dict[str, Any]]:
    try:
        return client.list_all(f"workspaces/{workspace_id}/kqlDatabases/{database_id}/shortcuts")
    except FabricApiError as error:
        if error.status_code in (400, 404):
            return []
        raise


def table_shortcut_names(
    client: FabricClient,
    workspace_id: str,
    database_id: str,
) -> set[str]:
    """Names of tables that are shortcuts rather than real tables.

    Their data lives at the target of the shortcut, so copying them would either fail or
    duplicate someone else's data into the migrated database.
    """
    return {
        shortcut["name"]
        for shortcut in list_table_shortcuts(client, workspace_id, database_id)
        if shortcut.get("name")
    }


def create_table_shortcut(
    client: FabricClient,
    workspace_id: str,
    database_id: str,
    shortcut: Mapping[str, Any],
) -> dict[str, Any]:
    return client.post(
        f"workspaces/{workspace_id}/kqlDatabases/{database_id}/shortcuts",
        json={
            "name": shortcut["name"],
            "enableQueryAcceleration": bool(shortcut.get("enableQueryAcceleration", False)),
            "target": shortcut["target"],
        },
    )


def copy_table_shortcuts(
    client: FabricClient,
    source_workspace_id: str,
    source_database_id: str,
    target_workspace_id: str,
    target_database_id: str,
    id_map: Mapping[str, str],
    *,
    shortcuts: Iterable[Mapping[str, Any]] | None = None,
) -> tuple[int, list[str]]:
    """Recreate a KQL database's table shortcuts against the migrated items."""
    source = (
        list(shortcuts)
        if shortcuts is not None
        else list_table_shortcuts(client, source_workspace_id, source_database_id)
    )

    created = 0
    warnings: list[str] = []

    for shortcut in source:
        target = _remap_target(shortcut.get("target") or {}, id_map)
        if not target:
            warnings.append(
                f"KQL table shortcut '{shortcut.get('name')}' has no recognised target, skipped"
            )
            continue
        try:
            create_table_shortcut(
                client,
                target_workspace_id,
                target_database_id,
                {
                    "name": shortcut["name"],
                    "enableQueryAcceleration": shortcut.get("enableQueryAcceleration", False),
                    "target": target,
                },
            )
            created += 1
        except FabricApiError as error:
            warnings.append(
                describe_failure(
                    str(shortcut.get("name")),
                    target,
                    error.status_code,
                    label="KQL table shortcut",
                )
            )
    return created, warnings


__all__ = [
    "copy_shortcuts",
    "copy_table_shortcuts",
    "create_shortcut",
    "create_table_shortcut",
    "describe_failure",
    "list_shortcuts",
    "list_table_shortcuts",
    "remap_shortcut_target",
    "table_shortcut_names",
]
