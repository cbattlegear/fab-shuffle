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
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from fabshuffle.fabric.client import (
    FabricApiError,
    FabricClient,
    FabricError,
    OperationFailed,
    OperationTimeout,
)
from fabshuffle.fabric.definitions import (
    decode_json_part,
    decode_payload,
    find_part,
    rewrite_parts,
    strip_part,
)
from fabshuffle.fabric.items import (
    create_item,
    get_item_definition,
    list_items,
    try_get_item_definition,
)

logger = logging.getLogger(__name__)

SEMANTIC_MODEL = "SemanticModel"
REPORT = "Report"
DATA_PIPELINE = "DataPipeline"
COPY_JOB = "CopyJob"
NOTEBOOK = "Notebook"
ENVIRONMENT = "Environment"
DATAFLOW = "Dataflow"
EVENTSTREAM = "Eventstream"
KQL_DASHBOARD = "KQLDashboard"
KQL_QUERYSET = "KQLQueryset"
MIRRORED_DATABASE = "MirroredDatabase"
GRAPHQL_API = "GraphQLApi"
MAP = "Map"
REFLEX = "Reflex"
SPARK_JOB_DEFINITION = "SparkJobDefinition"
VARIABLE_LIBRARY = "VariableLibrary"
MOUNTED_DATA_FACTORY = "MountedDataFactory"
GRAPH_MODEL = "GraphModel"
GRAPH_QUERY_SET = "GraphQuerySet"
MIRRORED_ADB_CATALOG = "MirroredAzureDatabricksCatalog"
SNOWFLAKE_DATABASE = "SnowflakeDatabase"

PBIR_PART = "definition.pbir"
PLATFORM_PART = ".platform"
SPARK_COMPUTE_PART = "Setting/Sparkcompute.yml"
QUERY_METADATA_PART = "queryMetadata.json"

# The definition format a Dataflow Gen2 (CI/CD) item uses. Anything else is a Gen1 dataflow
# or a classic Gen2, neither of which the item definition APIs can move.
CICD_DATAFLOW_FORMAT_VERSION = "202502"


@dataclass(frozen=True, slots=True)
class MigratedItem:
    source_id: str
    target_id: str
    name: str
    rebound_parts: int
    # Kept so the caller can inspect what the item binds without exporting it again.
    parts: tuple[dict[str, Any], ...] = ()


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
    """List items of one type.

    Filtering is done here rather than through the ``type`` query parameter, because Fabric
    documents that filtering by the dataflow item type does not return correct information.
    """
    return [
        item
        for item in list_items(client, workspace_id)
        if item.get("type") == item_type and item.get("id")
    ]


def classify_dataflow(
    client: FabricClient,
    workspace_id: str,
    item: Mapping[str, Any],
) -> tuple[list[dict[str, Any]] | None, str | None]:
    """Decide whether a dataflow can be migrated, and return its definition if so.

    Only Dataflow Gen2 (CI/CD) items work with the item definition APIs. A Gen1 dataflow, or
    a classic Gen2, either refuses ``getDefinition`` outright or comes back without the
    CI/CD format marker. Both are reported rather than half-migrated.

    Returns ``(parts, None)`` when it can move, or ``(None, reason)`` when it cannot.
    """
    name = item.get("displayName") or item.get("id")
    upgrade = (
        "Upgrade it to a Dataflow Gen2 (CI/CD) first, with the upgrade wizard or Save As, "
        "then migrate again"
    )

    definition = try_get_item_definition(client, workspace_id, item["id"])
    if definition is None:
        return None, (
            f"Dataflow '{name}' does not support the definition APIs, so it is a Gen1 "
            f"dataflow or a classic Gen2. {upgrade}."
        )

    parts = list(definition.get("parts") or [])
    metadata_part = find_part(parts, QUERY_METADATA_PART)
    if metadata_part is None:
        return None, (
            f"Dataflow '{name}' returned no {QUERY_METADATA_PART}, so it is not a Dataflow "
            f"Gen2 (CI/CD). {upgrade}."
        )

    try:
        metadata = decode_json_part(metadata_part["payload"])
    except (ValueError, KeyError):
        return None, f"Dataflow '{name}' has unreadable metadata, so it was left behind."

    version = str(metadata.get("formatVersion") or "")
    if version != CICD_DATAFLOW_FORMAT_VERSION:
        return None, (
            f"Dataflow '{name}' reports format version '{version or 'none'}' rather than "
            f"{CICD_DATAFLOW_FORMAT_VERSION}, so it is not a Dataflow Gen2 (CI/CD). {upgrade}."
        )

    return parts, None


def environment_warnings(
    name: str,
    parts: Iterable[Mapping[str, Any]],
    *,
    known_pool_ids: Collection[str] = (),
) -> list[str]:
    """Report environment settings that will not carry across.

    A custom Spark pool belongs to the workspace it was created in. Pools are recreated
    before environments are migrated, so this only fires for a pool that did not transfer and
    therefore still points at the source workspace.
    """
    warnings: list[str] = []
    compute = find_part(parts, SPARK_COMPUTE_PART)
    if not compute:
        return warnings

    try:
        text = decode_payload(compute["payload"]).decode("utf-8")
    except (UnicodeDecodeError, KeyError):
        return warnings

    for line in text.splitlines():
        key, _, value = line.partition(":")
        pool_id = value.strip()
        if key.strip() != "instance_pool_id" or not pool_id or pool_id == "null":
            continue
        if pool_id in known_pool_ids:
            continue
        warnings.append(
            f"Environment '{name}' pins the custom Spark pool '{pool_id}', which was not "
            "recreated in the new workspace. Create the pool there and repoint the "
            "environment, or it will fall back to the starter pool."
        )
    return warnings


def migrate_definition_item(
    client: FabricClient,
    *,
    source_workspace_id: str,
    target_workspace_id: str,
    item: Mapping[str, Any],
    item_type: str,
    id_map: Mapping[str, str],
    folder_id: str | None = None,
    parts: list[dict[str, Any]] | None = None,
) -> MigratedItem:
    """Export one item, repoint its references, and recreate it in the target workspace.

    ``parts`` lets a caller reuse a definition it has already fetched, which avoids a second
    export for item types that have to be inspected before they can be migrated.
    """
    name = item["displayName"]
    definition_format: str | None = None

    if parts is None:
        definition = get_item_definition(client, source_workspace_id, item["id"])
        parts = list(definition.get("parts") or [])
        definition_format = definition.get("format")

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
        definition_format=definition_format,
        folder_id=folder_id,
    )
    return MigratedItem(
        source_id=item["id"],
        target_id=created["id"],
        name=name,
        rebound_parts=changed,
        parts=tuple(rewritten),
    )


def describe_failure(item_type: str, name: str, error: FabricError) -> str:
    """Explain why one item could not be created.

    Creation is a long running operation for several item types, so the interesting failure
    usually arrives from the operation rather than from the request that started it, carrying
    an error code worth repeating back.
    """
    if isinstance(error, OperationFailed) and error.error_code == "DataSourcesValidationError":
        return (
            f"{item_type} '{name}' was not migrated: one of its sources uses a connection "
            "this service principal cannot reach. Connections are tenant wide, so grant it "
            "access to the connection in Manage Connections and Gateways, then recreate the "
            f"{item_type.lower()}."
        )
    if isinstance(error, OperationFailed):
        detail = error.detail or error.error_code or "the operation failed"
        return f"{item_type} '{name}' was not migrated: {detail}"
    if isinstance(error, OperationTimeout):
        return (
            f"{item_type} '{name}' was still being created when we stopped waiting. Check the "
            "new workspace before recreating it, in case it arrived late."
        )
    if isinstance(error, FabricApiError):
        return (
            f"{item_type} '{name}' was not migrated (HTTP {error.status_code}). "
            "Recreate it manually and check its data source bindings."
        )
    return f"{item_type} '{name}' was not migrated: {error}"


def migrate_items(
    client: FabricClient,
    *,
    source_workspace_id: str,
    target_workspace_id: str,
    items: Iterable[Mapping[str, Any]],
    item_type: str,
    id_map: dict[str, str],
    folder_map: Mapping[str, str] | None = None,
    parts_by_id: Mapping[str, list[dict[str, Any]]] | None = None,
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
                parts=(parts_by_id or {}).get(item.get("id", "")),
            )
        except FabricError as error:
            warnings.append(describe_failure(item_type, str(name), error))
            continue

        id_map[result.source_id] = result.target_id
        migrated.append(result)

    return migrated, warnings


__all__ = [
    "CICD_DATAFLOW_FORMAT_VERSION",
    "COPY_JOB",
    "DATAFLOW",
    "DATA_PIPELINE",
    "ENVIRONMENT",
    "EVENTSTREAM",
    "KQL_DASHBOARD",
    "KQL_QUERYSET",
    "MIRRORED_DATABASE",
    "NOTEBOOK",
    "REPORT",
    "SEMANTIC_MODEL",
    "MigratedItem",
    "classify_dataflow",
    "default_semantic_model_names",
    "describe_failure",
    "environment_warnings",
    "list_of_type",
    "migrate_definition_item",
    "migrate_items",
]
