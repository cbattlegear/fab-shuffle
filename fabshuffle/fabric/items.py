"""Generic Fabric item operations, including definition import/export."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from typing import Any

from fabshuffle.config import SETTINGS
from fabshuffle.fabric.client import FabricApiError, FabricClient, FabricError, OperationFailed
from fabshuffle.fabric.definitions import definition as build_definition

logger = logging.getLogger(__name__)

# Items Fabric provisions for its own use. Recreating them either fails outright or
# corrupts the target workspace, so they are skipped during migration. Matched case
# insensitively, because Fabric is not consistent about how it capitalises them.
SYSTEM_ITEM_NAMES = frozenset(
    {
        "dataflowsstaginglakehouse",
        "dataflowsstagingwarehouse",
        "monitoring eventhouse",
        "monitoring kql database",
        "monitoring_eventstream",
    }
)

# Items created by turning on workspace monitoring rather than by creating an item, so they
# cannot be migrated at all: the feature is switched on in the new workspace instead.
MONITORING_ITEM_NAMES = frozenset(
    {"monitoring eventhouse", "monitoring kql database", "monitoring_eventstream"}
)


def is_system_item(item: Mapping[str, Any]) -> bool:
    return (item.get("displayName") or "").casefold() in SYSTEM_ITEM_NAMES


def is_monitoring_item(item: Mapping[str, Any]) -> bool:
    return (item.get("displayName") or "").casefold() in MONITORING_ITEM_NAMES


def list_items(
    client: FabricClient,
    workspace_id: str,
    item_type: str | None = None,
) -> list[dict[str, Any]]:
    params = {"type": item_type} if item_type else None
    items = client.list_all(f"workspaces/{workspace_id}/items", params=params)
    return [item for item in items if not is_system_item(item)]


def get_item(client: FabricClient, workspace_id: str, item_id: str) -> dict[str, Any]:
    return client.get(f"workspaces/{workspace_id}/items/{item_id}")


def delete_item(client: FabricClient, workspace_id: str, item_id: str) -> None:
    client.delete(f"workspaces/{workspace_id}/items/{item_id}")


def get_item_definition(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    fmt: str | None = None,
) -> dict[str, Any]:
    """Export an item definition. Returns the ``definition`` object with base64 parts."""
    params = {"format": fmt} if fmt else None
    result = client.post(
        f"workspaces/{workspace_id}/items/{item_id}/getDefinition",
        params=params,
    )
    return result.get("definition") or result


def create_item(
    client: FabricClient,
    workspace_id: str,
    display_name: str,
    item_type: str,
    *,
    description: str | None = None,
    creation_payload: Mapping[str, Any] | None = None,
    parts: list[Mapping[str, str]] | None = None,
    definition_format: str | None = None,
    folder_id: str | None = None,
) -> dict[str, Any]:
    """Create an item from either a ``creationPayload`` or a definition, never both."""
    if creation_payload and parts:
        raise ValueError("Fabric rejects creationPayload and definition on the same request")

    body: dict[str, Any] = {"displayName": display_name, "type": item_type}
    if description:
        body["description"] = description
    if folder_id:
        body["folderId"] = folder_id
    if creation_payload:
        body["creationPayload"] = dict(creation_payload)
    if parts:
        body["definition"] = build_definition(parts, definition_format)

    attempts = max(1, SETTINGS.max_retries) if item_type == "Eventstream" else 1
    for attempt in range(1, attempts + 1):
        try:
            return client.post(f"workspaces/{workspace_id}/items", json=body)
        except OperationFailed as error:
            if (
                error.status != "Failed" or error.error_code != "ArtifactOperationConflict"
                or attempt == attempts
            ):
                raise
            delay = min(60.0, max(float(SETTINGS.lro_poll_seconds), 2.0 ** attempt))
            logger.warning(
                "%s; waiting %.1fs before checking the destination and retrying (%s/%s)",
                error, delay, attempt, attempts,
            )
            time.sleep(delay)
            # A failed LRO is terminal, but may have left an item. Never adopt it by name,
            # or retry an uncertain/ongoing create. List all pages before resubmitting.
            try:
                existing = list_items(client, workspace_id, item_type)
            except FabricError as read_error:
                raise FabricError(
                    f"{error}. Could not check for a partial destination item: {read_error}. "
                    "Inspect the destination before retrying."
                ) from error
            if any(
                str(item.get("displayName") or "").casefold() == display_name.casefold()
                for item in existing
            ):
                raise FabricError(
                    f"{error}. The destination already contains an Eventstream named '{display_name}'; "
                    "it was not adopted or overwritten, and no further create was submitted. "
                    "Inspect that item before resuming."
                ) from error
    raise AssertionError("Creation retry budget exhausted without returning or raising")  # pragma: no cover


def update_item_definition(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    parts: list[Mapping[str, str]],
    *,
    definition_format: str | None = None,
    update_metadata: bool = False,
) -> dict[str, Any]:
    params = {"updateMetadata": "true"} if update_metadata else None
    return client.post(
        f"workspaces/{workspace_id}/items/{item_id}/updateDefinition",
        json={"definition": build_definition(parts, definition_format)},
        params=params,
    )


def find_item_by_name(
    client: FabricClient,
    workspace_id: str,
    display_name: str,
    item_type: str | None = None,
) -> dict[str, Any] | None:
    for item in list_items(client, workspace_id, item_type):
        if item.get("displayName") == display_name:
            return item
    return None


def try_get_item_definition(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    fmt: str | None = None,
) -> dict[str, Any] | None:
    """Export a definition, returning ``None`` when the item type has no definition API."""
    try:
        return get_item_definition(client, workspace_id, item_id, fmt)
    except FabricApiError as error:
        if error.status_code in (400, 404, 415, 501):
            return None
        raise


__all__ = [
    "MONITORING_ITEM_NAMES",
    "SYSTEM_ITEM_NAMES",
    "create_item",
    "delete_item",
    "find_item_by_name",
    "get_item",
    "get_item_definition",
    "is_monitoring_item",
    "is_system_item",
    "list_items",
    "try_get_item_definition",
    "update_item_definition",
]
