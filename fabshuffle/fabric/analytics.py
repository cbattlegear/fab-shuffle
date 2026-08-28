"""Semantic model and report migration.

Both are moved by exporting their definition, rewriting every reference to a source item,
and creating the item in the target workspace. The rewrite is what actually rebinds them:

* a Direct Lake or DirectQuery semantic model embeds the SQL analytics endpoint of its
  lakehouse or warehouse plus that item's GUID, in ``model.bim`` or ``definition/*.tmdl``;
* a report records its semantic model as ``semanticmodelid=<guid>`` inside
  ``definition.pbir``, or as a relative ``byPath`` reference that needs no rewriting.

Because the rewrite is driven by the accumulated source-to-target id map, these must run
*after* every item they can reference has been created. See the ordering note in
:mod:`fabshuffle.orchestrator`.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from fabshuffle.fabric.client import FabricApiError, FabricClient
from fabshuffle.fabric.definitions import rewrite_parts, strip_part
from fabshuffle.fabric.items import create_item, get_item_definition, list_items

logger = logging.getLogger(__name__)

SEMANTIC_MODEL = "SemanticModel"
REPORT = "Report"

PBIR_PART = "definition.pbir"
PLATFORM_PART = ".platform"


@dataclass(frozen=True, slots=True)
class MigratedItem:
    source_id: str
    target_id: str
    name: str
    rebound_parts: int


def default_semantic_model_names(client: FabricClient, workspace_id: str) -> set[str]:
    """Names of semantic models Fabric creates and owns itself.

    Every lakehouse and warehouse gets a default semantic model named after it. Fabric
    provisions those alongside the parent item, so recreating them would either collide with
    the auto-created one or produce a duplicate. There is no flag on the item that marks a
    model as default, so they are matched by the name of their parent.
    """
    names: set[str] = set()
    for item in list_items(client, workspace_id):
        if item.get("type") in ("Lakehouse", "Warehouse"):
            name = item.get("displayName")
            if name:
                names.add(name)
    return names


def list_of_type(
    client: FabricClient,
    workspace_id: str,
    item_type: str,
) -> list[dict[str, Any]]:
    return [item for item in list_items(client, workspace_id, item_type) if item.get("id")]


def migrate_definition_item(
    client: FabricClient,
    *,
    source_workspace_id: str,
    target_workspace_id: str,
    item: Mapping[str, Any],
    item_type: str,
    id_map: Mapping[str, str],
    folder_id: str | None = None,
) -> MigratedItem:
    """Export one item, repoint its references, and recreate it in the target workspace."""
    name = item["displayName"]
    definition = get_item_definition(client, source_workspace_id, item["id"])
    parts = list(definition.get("parts") or [])

    rewritten, changed = rewrite_parts(parts, id_map)
    # The source platform file carries the original logical id, and Fabric respects it when
    # provided. Dropping it lets the new workspace mint its own identity for the item.
    rewritten = strip_part(rewritten, PLATFORM_PART)

    created = create_item(
        client,
        target_workspace_id,
        name,
        item_type,
        description=item.get("description") or None,
        parts=rewritten,
        definition_format=definition.get("format"),
        folder_id=folder_id,
    )
    return MigratedItem(
        source_id=item["id"],
        target_id=created["id"],
        name=name,
        rebound_parts=changed,
    )


def migrate_items(
    client: FabricClient,
    *,
    source_workspace_id: str,
    target_workspace_id: str,
    items: Iterable[Mapping[str, Any]],
    item_type: str,
    id_map: dict[str, str],
    folder_map: Mapping[str, str] | None = None,
    on_progress: Any = None,
) -> tuple[list[MigratedItem], list[str]]:
    """Migrate a batch of definition-backed items, collecting per-item failures.

    Each success is recorded in ``id_map`` immediately so later items in the same batch, and
    later phases, can be rebound to it.
    """
    migrated: list[MigratedItem] = []
    warnings: list[str] = []

    for item in items:
        name = item.get("displayName") or item.get("id")
        if on_progress:
            on_progress(f"Migrating {item_type} '{name}'")
        try:
            result = migrate_definition_item(
                client,
                source_workspace_id=source_workspace_id,
                target_workspace_id=target_workspace_id,
                item=item,
                item_type=item_type,
                id_map=id_map,
                folder_id=(folder_map or {}).get(item.get("folderId", "")),
            )
        except FabricApiError as error:
            warnings.append(
                f"{item_type} '{name}' was not migrated (HTTP {error.status_code}). "
                "Recreate it manually and check its data source bindings."
            )
            continue

        id_map[result.source_id] = result.target_id
        migrated.append(result)

    return migrated, warnings


__all__ = [
    "REPORT",
    "SEMANTIC_MODEL",
    "MigratedItem",
    "default_semantic_model_names",
    "list_of_type",
    "migrate_definition_item",
    "migrate_items",
]
