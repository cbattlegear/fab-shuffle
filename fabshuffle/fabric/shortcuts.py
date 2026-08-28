"""OneLake shortcut operations."""

from __future__ import annotations

from collections.abc import Mapping
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
                f"Shortcut '{shortcut.get('name')}' could not be created "
                f"(HTTP {error.status_code}); its connection may not exist in the target region"
            )
    return created, warnings


__all__ = ["copy_shortcuts", "create_shortcut", "list_shortcuts", "remap_shortcut_target"]
