"""Destination-only standby adapters. No source transport, cache fallback or activation.

The coordinator owns leases, mutation intents, target ownership and workspace access.
Every adapter validates captured bytes and bindings before its first destination mutation.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.capture import (
    EMPTY_GUID,
    EXECUTION_ITEMS,
    NO_DEFINITION,
    SECURITY_UNKNOWNS,
    capture_json,
    capture_list,
    definition_format,
    definition_parts,
    item_document,
)
from fabshuffle.bcdr.capture_sources import MAX_METADATA_BYTES, file_api_path, inspect_schema_archive
from fabshuffle.bcdr.contracts import (
    AppliedItem,
    ConnectionIdentity,
    EndpointIdentity,
    ItemIdentity,
    ItemRecord,
    PayloadPurpose,
    RecoveryOutcome,
    WorkspaceIdentity,
    canonical_id,
    canonical_json,
    digest,
)
from fabshuffle.bcdr.payloads import CapturedPayload
from fabshuffle.bcdr.registry import TYPE_REGISTRY
from fabshuffle.fabric import analytics, data_stores, eventhouses, special_items, sqldatabases
from fabshuffle.fabric.client import FabricClient, FabricError
from fabshuffle.fabric.definitions import (
    decode_json_part,
    decode_payload,
    find_part,
    part,
    replace_part,
    rewrite_parts,
    strip_part,
)
from fabshuffle.fabric.items import create_item, get_item_definition, update_item_definition
from fabshuffle.transfer import sqlschema

ItemKey = tuple[str, str, str]
WorkspaceKey = tuple[str, str]
STORES = frozenset(
    {
        "Lakehouse",
        "Warehouse",
        "SQLDatabase",
        "CosmosDBDatabase",
        "Eventhouse",
        "KQLDatabase",
        "MirroredDatabase",
        "MirroredAzureDatabricksCatalog",
        "SnowflakeDatabase",
    }
)
BLOCKED_TYPES = {
    "MirroredDatabase": (
        "A non-starting create/update contract is not established for every mirrored source type. "
        "Qualify an independently stopped mirror; separate Start/Stop APIs are not proof of safe create."
    ),
    "Eventstream": (
        "Eventstream inactive creation is not established for every source/destination node. "
        "Prepare a qualified stopped topology; do not create a live ingestion path during standby sync."
    ),
    "ApacheAirflowJob": (
        "Importing DAG files can cause scheduler parsing/execution. Prepare an independently stopped "
        "Airflow runtime and qualify its inactive file-import contract before restoration."
    ),
    "SnowflakeDatabase": (
        "The captured Snowflake creation payload has no verified inactive-sync switch. "
        "Qualify stopped creation with the external source before applying it."
    ),
    "MountedDataFactory": (
        "A mounted factory can refer to an independently active Azure Data Factory. "
        "Fence its external triggers and qualify an inactive recovery factory before mounting."
    ),
    "Dashboard": (
        "Power BI reassignment is not outage reconstruction; use platform continuity or a prepared dashboard."
    ),
    "PaginatedReport": (
        "Power BI reassignment is not outage reconstruction; supply a captured and qualified report."
    ),
}


@dataclass(frozen=True, slots=True)
class AdapterCapabilities:
    inactive_create: bool
    safe_shell: bool
    is_store: bool
    reason: str = ""


@dataclass(frozen=True, slots=True)
class AdapterResult:
    outcome: RecoveryOutcome
    applied: AppliedItem | None = None
    target_properties: dict[str, Any] = field(default_factory=dict)
    deferred_grants: tuple[dict[str, Any], ...] = ()
    diagnostics: tuple[str, ...] = ()
    metadata_applied: bool = False


def adapter_capabilities(item: ItemRecord) -> AdapterCapabilities:
    reason = BLOCKED_TYPES.get(item.item_type, "")
    if item.item_type not in TYPE_REGISTRY:
        reason = f"Supply a qualified reconstruction adapter for '{item.item_type}'."
    return AdapterCapabilities(
        inactive_create=not reason,
        safe_shell=item.item_type in {"Lakehouse", "Warehouse", "SQLDatabase", "Eventhouse"},
        is_store=item.item_type in STORES,
        reason=reason,
    )


def _key(identity: ItemIdentity) -> ItemKey:
    return identity.tenant_id, identity.workspace_id, identity.item_id


def _definition_digest(path: str, data: bytes) -> str:
    if not path.lower().endswith(".dacpac"):
        return digest(data)
    # ZIP timestamps and Origin.xml are export metadata, not destination schema drift.
    members = inspect_schema_archive(data)
    return digest(
        canonical_json(
            {
                "members": [
                    {"path": name, "sha256": digest(content)}
                    for name, content in sorted(members.items())
                    if name == "model.xml" or name.lower().endswith(".sql")
                ],
            }
        )
    )


def captured_hashes(item: ItemRecord, payloads: Sequence[CapturedPayload]) -> tuple[str, str]:
    definition_parts(item, payloads)
    by_id = {payload.descriptor.payload_id: payload for payload in payloads}
    manifest = [
        {
            "path": by_id[value].descriptor.path,
            "purpose": by_id[value].descriptor.purpose,
            "sha256": _definition_digest(by_id[value].descriptor.path, by_id[value].data),
        }
        for value in item.payload_ids
    ]
    return digest(canonical_json({"parts": manifest})), digest(canonical_json(item.properties))


def _mapping(
    item: ItemRecord,
    target_workspace: WorkspaceIdentity,
    source_items: Sequence[ItemRecord],
    item_mappings: Mapping[ItemKey, ItemIdentity],
    workspace_mappings: Mapping[WorkspaceKey, WorkspaceIdentity],
    endpoint_mappings: Sequence[tuple[EndpointIdentity, str]],
    connection_mappings: Sequence[tuple[ConnectionIdentity, ConnectionIdentity]],
    target_id: str | None,
    verified_external_connections: Sequence[ConnectionIdentity] = (),
) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    if item.identity.tenant_id != target_workspace.tenant_id:
        raise FabricError("BCDR adapters require same-tenant recovery identities")
    sources = {_key(row.identity): row for row in source_items}
    sources[_key(item.identity)] = item
    source_workspaces = {(key[0], key[1]) for key in sources}
    if (target_workspace.tenant_id, target_workspace.workspace_id) in source_workspaces:
        raise FabricError("The standby workspace must be outside the captured source estate")
    replacements: dict[str, str] = {}
    references: dict[str, dict[str, Any]] = {}

    def add(source: str, target: str) -> None:
        if not source or not target or source.casefold() == target.casefold():
            raise FabricError(f"Recovery mapping must replace source identity '{source}'")
        old = replacements.get(source.casefold())
        if old and old.casefold() != target.casefold():
            raise FabricError(f"Conflicting qualified mappings for literal '{source}'")
        replacements[source.casefold()] = target

    known_literals: dict[str, ItemKey] = {}
    for key, row in sources.items():
        old = known_literals.get(row.identity.item_id)
        if old is not None and old != key:
            raise FabricError(
                "Ambiguous item GUID across workspaces; qualify the definition binding before apply"
            )
        known_literals[row.identity.item_id] = key
        references[row.identity.item_id] = {
            "id": row.identity.item_id,
            "type": row.item_type,
            "displayName": row.display_name,
            "properties": {k: v for k, v in row.properties.items() if k != "bcdr"},
        }
        references[row.identity.workspace_id] = {
            "id": row.identity.workspace_id,
            "type": "source workspace",
            "displayName": row.identity.workspace_id,
        }
    for raw_key, target in workspace_mappings.items():
        key = tuple(canonical_id(value) for value in raw_key)
        if len(key) != 2 or key[0] != target.tenant_id or key not in source_workspaces:
            raise FabricError("Workspace mapping does not identify a captured same-tenant workspace")
        if (target.tenant_id, target.workspace_id) in source_workspaces:
            raise FabricError("Workspace mapping points back into the source estate")
        add(key[1], target.workspace_id)
    if replacements.get(item.identity.workspace_id) != target_workspace.workspace_id:
        raise FabricError("Map the captured source workspace to the exact requested standby workspace")
    for raw_key, target in item_mappings.items():
        key = tuple(canonical_id(value) for value in raw_key)
        if len(key) != 3 or key not in sources or key[0] != target.tenant_id:
            raise FabricError("Item mapping does not identify a captured same-tenant item")
        if (target.tenant_id, target.workspace_id) in source_workspaces:
            raise FabricError("Item mapping points back into the source estate")
        expected_workspace = replacements.get(key[1])
        if expected_workspace != target.workspace_id:
            raise FabricError("Item mapping disagrees with its qualified workspace mapping")
        add(key[2], target.item_id)
    if target_id:
        add(item.identity.item_id, canonical_id(target_id))
    for source, target in endpoint_mappings:
        if _key(source.item) not in sources or (
            source.item.item_id not in replacements and source.endpoint_kind != "spark_pool_id"
        ):
            raise FabricError("Map the owning item before mapping its endpoint")
        if source.endpoint_kind == "sql_database_name":
            # A bare display/catalog name is not a globally unique replacement key.
            if target != source.endpoint_id:
                raise FabricError(
                    "SQL catalog renames require a qualified SQL/model binding adapter, "
                    "not global text replacement"
                )
            replacements[source.endpoint_id.casefold()] = target
            continue
        add(source.endpoint_id, target)
    for source, target in connection_mappings:
        if source.tenant_id != item.identity.tenant_id or target.tenant_id != source.tenant_id:
            raise FabricError("Connection mappings must identify the recovery tenant")
        if source == target and source in verified_external_connections:
            if source.connection_id in references:
                raise FabricError("An unchanged connection ID collides with a captured item/workspace ID")
            replacements[source.connection_id] = target.connection_id
        else:
            add(source.connection_id, target.connection_id)
    if {value.key for value in verified_external_connections} != {
        source.key for source, target in connection_mappings if source == target
    }:
        raise FabricError("Qualify exactly the unchanged external connections in this apply operation")
    return replacements, references


def _defer_grants(parts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    deferred: list[dict[str, Any]] = []
    for candidate in parts:
        if candidate["path"].rsplit("/", 1)[-1] == ".schedules":
            deferred.append(
                {
                    "scope": "schedule",
                    "path": candidate["path"],
                    "definition": decode_payload(candidate["payload"]).decode("utf-8"),
                }
            )
    parts = [candidate for candidate in parts if candidate["path"].rsplit("/", 1)[-1] != ".schedules"]
    access = find_part(parts, "data-access-roles.json")
    if access:
        roles = decode_json_part(access["payload"])
        if not isinstance(roles, list):
            raise FabricError("OneLake role definitions are not inspectable")
        deferred.extend({"scope": "onelake", "role": role} for role in roles)
        parts = strip_part(parts, "data-access-roles.json")
    alm = find_part(parts, "alm.settings.json")
    if alm:
        settings = decode_json_part(alm["payload"])
        if not isinstance(settings, dict) or not isinstance(settings.get("objectTypes"), list):
            raise FabricError("Lakehouse ALM settings cannot be inspected for data access role import")
        for entry in settings["objectTypes"]:
            if not isinstance(entry, dict):
                raise FabricError("Invalid Lakehouse ALM object type")
            if entry.get("name") == "DataAccessRoles":
                entry["state"] = "Disabled"
        parts = replace_part(parts, "alm.settings.json", settings)
    model = find_part(parts, "model.bim")
    if model:
        document = decode_json_part(model["payload"])
        if not isinstance(document, dict) or not isinstance(document.get("model"), dict):
            raise FabricError("Supply inspectable TMSL model.bim before restoring semantic model security")
        roles = document["model"].get("roles", [])
        if not isinstance(roles, list):
            raise FabricError("Semantic model role definitions are not inspectable")
        for role in roles:
            if not isinstance(role, dict) or not isinstance(role.get("members", []), list):
                raise FabricError("Semantic model role membership is not inspectable")
            for member in role.get("members", []):
                deferred.append({"scope": "model", "role": role.get("name"), "member": member})
        parts, _ = analytics.without_model_memberships(parts)

    def inspect(node: Any, path: str) -> None:
        if isinstance(node, list):
            for value in node:
                inspect(value, path)
        elif isinstance(node, dict):
            for key, value in node.items():
                if (
                    key.casefold()
                    in {
                        "roleassignments",
                        "permissions",
                        "acl",
                        "accesscontrollist",
                        "grants",
                        "members",
                        "principals",
                        "sqlpermissions",
                        "authorization",
                    }
                    and value
                ):
                    raise FabricError(
                        f"'{path}' contains grant-bearing '{key}'. Capture/defer those grants with a "
                        "typed sanitizer before applying this definition."
                    )
                inspect(value, path)

    for candidate in parts:
        path = candidate["path"]
        if path.endswith((".json", ".bim", ".pbir", ".pbism", ".ipynb")):
            inspect(decode_json_part(candidate["payload"]), path)
        if path.lower().endswith((".dacpac", ".abf", ".pbix", ".tmdl")):
            raise FabricError(
                f"'{path}' is opaque to standby grant sanitization; use a typed safe schema path."
            )
    return parts, deferred


def _normalise_parts(parts: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Make JSON escapes visible to the legacy literal-ID rewriter without executing code."""
    normalised = []
    for entry in parts:
        path = entry["path"]
        if path.endswith((".json", ".bim", ".pbir", ".pbism", ".ipynb")):
            document = decode_json_part(entry["payload"])
            normalised.append(part(path, json.dumps(document, ensure_ascii=False, allow_nan=False)))
        else:
            normalised.append(entry)
    return normalised


def _lakehouse_local_targets(
    item: ItemRecord,
    parts: Sequence[dict[str, Any]],
    workspace: WorkspaceIdentity,
    replacements: Mapping[str, str],
) -> list[dict[str, Any]]:
    result = list(parts)
    if item.item_type != "Lakehouse":
        return result
    candidate = find_part(parts, "shortcuts.metadata.json")
    if not candidate:
        return result
    shortcuts = decode_json_part(candidate["payload"])
    if not isinstance(shortcuts, list):
        raise FabricError("Capture an inspectable Lakehouse shortcut array")
    for shortcut in shortcuts:
        target = (shortcut.get("target") or {}).get("oneLake")
        if not isinstance(target, dict):
            continue
        source_workspace = canonical_id(target["workspaceId"])
        source_item = canonical_id(target["itemId"])
        if source_workspace == EMPTY_GUID:
            source_workspace = item.identity.workspace_id
        if source_workspace == item.identity.workspace_id and source_item in {
            EMPTY_GUID,
            item.identity.item_id,
        }:
            # These sentinels are documented only within a Lakehouse shortcut OneLake target.
            # The actual target item is assigned by Fabric during create/update.
            target["workspaceId"] = workspace.workspace_id
            target["itemId"] = EMPTY_GUID
        elif source_workspace == item.identity.workspace_id:
            if source_item not in replacements:
                raise FabricError(f"Map shortcut prerequisite '{source_item}' before creating the Lakehouse")
            target["workspaceId"] = item.identity.workspace_id
    return replace_part(result, "shortcuts.metadata.json", shortcuts)


def _inactive_parts(item: ItemRecord, parts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    diagnostics: list[str] = []
    if item.item_type == "Reflex":
        candidate = find_part(parts, special_items.REFLEX_ENTITIES_PART)
        entities = decode_json_part(candidate["payload"]) if candidate else None
        if not isinstance(entities, list) or any(
            not isinstance(entity, dict) or not isinstance(entity.get("payload"), dict) for entity in entities
        ):
            raise FabricError("Capture inspectable Reflex entities/rules before creating a stopped Activator")
        parts, _ = special_items.disable_reflex_rules(parts)
        entities = decode_json_part(find_part(parts, special_items.REFLEX_ENTITIES_PART)["payload"])
        for entity in entities:
            definition = entity["payload"].get("definition")
            if isinstance(definition, dict) and definition.get("type") == "Rule":
                definition["settings"]["shouldApplyRuleOnUpdate"] = False
        parts = replace_part(parts, special_items.REFLEX_ENTITIES_PART, entities)
    elif item.item_type == "MirroredAzureDatabricksCatalog":
        candidate = find_part(parts, special_items.ADB_CATALOG_PART)
        if not candidate or not isinstance(decode_json_part(candidate["payload"]), dict):
            raise FabricError("Capture the catalog definition before disabling autoSync")
        parts, _ = special_items.disable_catalog_autosync(parts)
    elif item.item_type == "CosmosDBDatabase":
        candidate = find_part(parts, "definition.json")
        document = decode_json_part(candidate["payload"]) if candidate else {}
        if not isinstance(document.get("containers"), list):
            raise FabricError("Capture Cosmos container definitions before restoring metadata")
        for container in document["containers"]:
            resource = container.get("resource") if isinstance(container, dict) else None
            if not isinstance(resource, dict) or not resource.get("id") or not resource.get("partitionKey"):
                raise FabricError("Cosmos container name and partition key must be explicit")
            if "defaultTtl" in resource:
                resource.pop("defaultTtl")
                diagnostics.append(
                    "Cosmos TTL is disabled on standby; approve expiration separately at cutover."
                )
        parts = replace_part(parts, "definition.json", document)
    elif item.item_type == "KQLDatabase":
        if item.properties.get("databaseType") != "ReadWrite":
            raise FabricError(
                "Capture a ReadWrite KQL database for this adapter; follower or unknown modes "
                "need a qualified independent database."
            )
        candidate = find_part(parts, eventhouses.DATABASE_SCHEMA_PART)
        if candidate is None:
            raise FabricError("Capture DatabaseSchema.kql before declaring KQL metadata synchronized")
        script = decode_payload(candidate["payload"]).decode("utf-8")
        # Only table declarations are accepted by this deliberately narrow inactive schema path.
        # Other commands can ingest, grant, enable policies or execute arbitrary queries.
        commands = [line.strip() for line in script.splitlines() if line.strip()]
        for command in commands:
            if not re.fullmatch(r"\.create(?:-merge)? table [A-Za-z_]\w* \([\w\s,:]+\)", command):
                raise FabricError(
                    "KQL schema includes commands outside the safe table-only "
                    "standby grammar. Prepare a reviewed independent stopped KQL database and apply those "
                    "objects through a qualified metadata adapter."
                )
    return parts, diagnostics


def _strict_references(
    item: ItemRecord,
    parts: list[dict[str, Any]],
    replacements: Mapping[str, str],
    references: Mapping[str, Mapping[str, Any]],
    verified_external_connections: Sequence[ConnectionIdentity] = (),
) -> None:
    # Unlike legacy migration, operational self-references are not ignored before creation.
    inspected = strip_part(parts, ".platform")
    for workspace_id in analytics.referenced_workspaces(inspected):
        if workspace_id.casefold() not in replacements and workspace_id.casefold() not in {
            value.casefold() for value in replacements.values()
        }:
            raise FabricError(
                f"Map workspace '{workspace_id}' explicitly before restoring '{item.display_name}'; "
                "the adapter will not probe an unknown/source workspace during recovery."
            )
    missing = analytics.dangling_references(
        inspected,
        replacements,
        analytics.with_connection_references(inspected, references),
        ignore=tuple(value.connection_id for value in verified_external_connections),
    )
    if missing:
        raise analytics.StrandedReference(missing)
    rewritten, _ = rewrite_parts(inspected, replacements)
    retained_names = {
        key: value
        for key, value in replacements.items()
        if key.casefold() == value.casefold() and not analytics.GUID_PATTERN.fullmatch(key)
    }
    remaining = analytics.dangling_references(
        rewritten,
        retained_names,
        references,
        ignore=tuple(value.connection_id for value in verified_external_connections),
    )
    if remaining:
        raise analytics.StrandedReference(remaining)
    target_workspaces = {
        replacements[key].casefold()
        for key, value in references.items()
        if value.get("type") == "source workspace" and key in replacements
    }
    if any(value.casefold() not in target_workspaces for value in analytics.referenced_workspaces(rewritten)):
        raise FabricError("The rewritten definition retains a source or unqualified workspace reference")


def _archive_schema(data: bytes) -> None:
    """Inspect bounded DACPAC members without extracting or executing scripts."""
    content = inspect_schema_archive(data)["model.xml"]
    if re.search(
        rb"Sql(?:DmlTrigger|DatabaseDdlTrigger|ServerDdlTrigger|Assembly|ExternalDataSource)",
        content,
        re.IGNORECASE,
    ):
        raise FabricError(
            "The DACPAC contains triggers, assemblies or external sources. Prepare a "
            "self-contained schema without active/opaque objects before standby apply."
        )


def _report_binding(
    client: FabricClient,
    item: ItemRecord,
    parts: Sequence[dict[str, Any]],
    source_items: Sequence[ItemRecord],
    mappings: Mapping[ItemKey, ItemIdentity],
) -> None:
    if item.item_type != "Report":
        return
    candidate = find_part(parts, "definition.pbir")
    if not candidate:
        raise FabricError("Capture definition.pbir before restoring a report/model binding")
    document = decode_json_part(candidate["payload"])
    reference = document.get("datasetReference") if isinstance(document, dict) else None
    if not isinstance(reference, dict):
        raise FabricError("Capture an inspectable report datasetReference")
    if reference.get("byPath"):
        path = reference["byPath"].get("path", "")
        match = re.fullmatch(r"\.\./([^/\\]+)\.SemanticModel", path)
        if not match:
            raise FabricError("Resolve the report's nonstandard byPath model reference before restoring it")
        models = [
            row
            for row in source_items
            if row.item_type == "SemanticModel"
            and row.display_name == match[1]
            and row.identity.workspace_id == item.identity.workspace_id
        ]
        normalized = {tuple(canonical_id(value) for value in key): target for key, target in mappings.items()}
        if len(models) != 1 or _key(models[0].identity) not in normalized:
            raise FabricError(
                f"Map the exact captured semantic model '{match[1]}' before restoring this report"
            )
        target = normalized[_key(models[0].identity)]
        current = item_document(client, target, "SemanticModel")
        if current.get("displayName") != match[1]:
            raise FabricError("The mapped report model was renamed; supply an explicit byConnection binding")
    elif not reference.get("byConnection"):
        raise FabricError("Resolve the report's missing model binding before restoring it")


def _apply_sql_schema(
    item: ItemRecord,
    payloads: Sequence[CapturedPayload],
    target: Mapping[str, Any],
    tokens: TokenProvider,
    replacements: Mapping[str, str],
    references: Mapping[str, Mapping[str, Any]],
    mutation_guard: Callable[[], None] | None = None,
) -> None:
    schemas = [
        payload
        for payload in payloads
        if payload.descriptor.payload_id in item.payload_ids
        and payload.descriptor.purpose == PayloadPurpose.SQL_SCHEMA
    ]
    if len(schemas) != 1:
        raise FabricError("Supply exactly one captured schema-only DACPAC before applying SQL metadata")
    data = schemas[0].data
    _archive_schema(data)
    properties = target.get("properties") or {}
    server = (
        properties.get("serverFqdn") or properties.get("connectionString") or properties.get("connectionInfo")
    )
    database = properties.get("databaseName") or target.get("displayName")
    if item.item_type == "Lakehouse":
        server = (properties.get("sqlEndpointProperties") or {}).get("connectionString")
    if not server or not database:
        raise FabricError("Destination SQL metadata omitted its actual server or database catalog")
    with TemporaryDirectory(prefix="fab-bcdr-apply-") as folder:
        root = Path(folder)
        source = root / "captured.dacpac"
        source.write_bytes(data)
        output = sqlschema.script_dacpac(
            source,
            root / "safe.sql",
            server=server,
            database=database,
            tokens=tokens,
            exclude_tables=item.item_type == "Lakehouse",
            exclude_security=True,
            staging_root=root,
            max_disk_staging_bytes=MAX_METADATA_BYTES,
        )
        if output.stat().st_size > MAX_METADATA_BYTES:
            raise FabricError("Destination SQL schema script exceeds the metadata memory budget")
        script = output.read_text(encoding="utf-8-sig")
        script = sqlschema.rewrite_schema_script(
            script,
            id_map=replacements,
            source_identifiers=analytics.reference_identifiers(references),
            max_memory_bytes=MAX_METADATA_BYTES,
        )
        output.write_text(script, encoding="utf-8")

        def guarded_batch() -> bool:
            if mutation_guard is not None:
                mutation_guard()
            return False

        failures = sqlschema.apply_script(
            output,
            server=server,
            database=database,
            tokens=tokens,
            max_memory_bytes=MAX_METADATA_BYTES,
            cancel_requested=guarded_batch,
        )
        if failures:
            raise FabricError("Destination schema application failed: " + "; ".join(failures))


def observe_target(client: FabricClient, target: ItemIdentity, item_type: str) -> str:
    """REST-visible definition/property drift digest; store data/SQL runtime ACLs remain unverified."""
    document = item_document(client, target, item_type)
    definition: dict[str, Any] = {}
    if item_type not in NO_DEFINITION:
        definition = get_item_definition(
            client,
            target.workspace_id,
            target.item_id,
            fmt=definition_format(item_type),
        )
        if not isinstance(definition.get("parts"), list) or not definition["parts"]:
            raise FabricError(
                "Target definition observation omitted its parts; cannot record a drift baseline"
            )
        definition = {
            "format": definition.get("format"),
            "parts": [
                {"path": entry["path"], "sha256": _definition_digest(entry["path"], base)}
                for entry in sorted(definition["parts"], key=lambda value: value["path"])
                for base in [decode_payload(entry["payload"])]
            ],
        }
    observed = {"item": document, "definition": definition}
    if item_type == "Environment":
        root = f"workspaces/{target.workspace_id}/environments/{target.item_id}/staging"
        observed["environment_staging"] = {
            "compute": capture_json(client, f"{root}/sparkcompute", params={"beta": "false"}),
            "libraries": capture_list(
                client, f"{root}/libraries", params={"beta": "false"}, value_key="libraries"
            ),
        }
    return digest(canonical_json(observed))


def _create_store(client: FabricClient, item: ItemRecord, workspace_id: str) -> dict[str, Any]:
    if item.item_type == "Lakehouse":
        schema_enabled = item.properties.get("bcdr", {}).get("schema_enabled")
        if not isinstance(schema_enabled, bool):
            raise FabricError("Capture the lakehouse schema mode before creating its standby shell")
        return data_stores.create_lakehouse(
            client,
            workspace_id,
            item.display_name,
            schema_enabled=schema_enabled,
        )
    if item.item_type == "Warehouse":
        return data_stores.create_warehouse(
            client,
            workspace_id,
            item.display_name,
            collation_type=item.properties.get("collationType"),
        )
    if item.item_type == "SQLDatabase":
        return sqldatabases.create_sql_database(
            client,
            workspace_id,
            item.display_name,
            collation=item.properties.get("collation"),
        )
    if item.item_type == "Eventhouse":
        return eventhouses.create_eventhouse(client, workspace_id, item.display_name)
    raise FabricError(f"'{item.item_type}' has no qualified definition-free shell adapter")


def _environment_preflight(
    client: FabricClient,
    item: ItemRecord,
    parts: Sequence[dict[str, Any]],
    workspace: WorkspaceIdentity,
    replacements: Mapping[str, str],
) -> dict[str, Any]:
    configuration = item.properties.get("bcdr", {}).get("environment")
    if not isinstance(configuration, dict) or not isinstance(configuration.get("staging_compute"), dict):
        raise FabricError(
            "Capture the Environment staging compute and library inventories before restoration"
        )
    compute = json.loads(json.dumps(configuration["staging_compute"]))
    allowed = {
        "instancePool",
        "driverCores",
        "driverMemory",
        "executorCores",
        "executorMemory",
        "dynamicExecutorAllocation",
        "sparkProperties",
        "runtimeVersion",
        "customLivePoolSupport",
        "customLivePoolSettings",
    }
    if set(compute) - allowed:
        raise FabricError("Review new Environment compute fields before including them in a standby request")
    compute["customLivePoolSupport"] = "Disabled"
    pool = compute.get("instancePool")
    if isinstance(pool, dict) and pool.get("id"):
        target_pool = replacements.get(str(pool["id"]).casefold())
        if not target_pool:
            raise FabricError("Map the Environment's captured custom Spark pool before staging its settings")
        matches = [
            row
            for row in capture_list(client, f"workspaces/{workspace.workspace_id}/spark/pools")
            if str(row.get("id", "")).casefold() == target_pool.casefold()
        ]
        if len(matches) != 1:
            raise FabricError("The mapped destination Spark pool is not present uniquely in its workspace")
        compute["instancePool"] = {"name": matches[0]["name"], "type": matches[0]["type"]}
    elif isinstance(pool, dict):
        compute["instancePool"] = {key: pool[key] for key in ("name", "type") if key in pool}
    for entry in compute.get("sparkProperties", []):
        if not isinstance(entry, dict) or re.search(
            r"acl|credential|identity|principal|token|secret|spark\.hadoop",
            str(entry.get("key", "")),
            re.I,
        ):
            raise FabricError("Defer Environment Spark security/identity properties before staging compute")
    for entry in parts:
        path = entry["path"]
        if path == "Libraries/PublicLibraries/environment.yml":
            raise FabricError(
                "Capture retained the Environment external-library YAML. The stable import endpoint's upload "
                "media contract is not established here. Qualify that staged upload before restoration."
            )
        if path.startswith("Libraries/CustomLibraries/"):
            name = path.removeprefix("Libraries/CustomLibraries/")
            file_api_path(name)
            if "/" in name or not name.endswith((".jar", ".py", ".whl", ".tar.gz")):
                raise FabricError("Environment custom library has an unsupported upload path or type")
        elif path not in {".platform", "Setting/Sparkcompute.yml"}:
            raise FabricError(f"Qualify Environment part '{path}' for staging-only restoration")
    return compute


def _stage_environment(
    client: FabricClient,
    item: ItemRecord,
    parts: Sequence[dict[str, Any]],
    workspace_id: str,
    compute: dict[str, Any],
    target_id: str | None,
) -> str:
    if target_id is None:
        target_id = canonical_id(
            client.post(
                f"workspaces/{workspace_id}/environments",
                json={"displayName": item.display_name},
            )["id"]
        )
    root = f"workspaces/{workspace_id}/environments/{target_id}/staging"
    client.patch(f"{root}/sparkcompute", params={"beta": "false"}, json=compute)
    for entry in parts:
        if entry["path"].startswith("Libraries/CustomLibraries/"):
            name = entry["path"].removeprefix("Libraries/CustomLibraries/")
            client.request(
                "POST",
                f"{root}/libraries/{file_api_path(name)}",
                content=decode_payload(entry["payload"]),
                headers={"Content-Type": "application/octet-stream"},
                expected=(200,),
            )
    return target_id


def apply_captured_item(
    client: FabricClient,
    item: ItemRecord,
    payloads: Sequence[CapturedPayload],
    *,
    generation_id: str,
    operation_id: str,
    target_workspace: WorkspaceIdentity,
    source_items: Sequence[ItemRecord],
    item_mappings: Mapping[ItemKey, ItemIdentity],
    workspace_mappings: Mapping[WorkspaceKey, WorkspaceIdentity],
    endpoint_mappings: Sequence[tuple[EndpointIdentity, str]] = (),
    connection_mappings: Sequence[tuple[ConnectionIdentity, ConnectionIdentity]] = (),
    target_id: str | None = None,
    tokens: TokenProvider | None = None,
    shell_only: bool = False,
    mutation_guard: Callable[[], None] | None = None,
    destination_quiescence: str | None = None,
    verified_external_connections: Sequence[ConnectionIdentity] = (),
) -> AdapterResult:
    """Create/update only from captured bytes; returned success never implies data readiness.

    Validation failures before mutation return BLOCKED with an actionable diagnostic.
    Destination service/transport errors propagate unchanged, including uncertain operations.
    """
    capabilities = adapter_capabilities(item)
    if item.tombstone or not item.capture_complete or item.unresolved:
        return AdapterResult(
            RecoveryOutcome.BLOCKED,
            diagnostics=(
                f"Recapture complete metadata for '{item.display_name}' before applying it.",
                *item.unresolved,
            ),
        )
    if not capabilities.inactive_create:
        return AdapterResult(RecoveryOutcome.BLOCKED, diagnostics=(capabilities.reason,))
    if target_id and item.item_type == "GraphModel":
        return AdapterResult(
            RecoveryOutcome.BLOCKED,
            diagnostics=(
                "GraphModel updates have no qualified no-ingestion contract. Use a fresh REST-created model.",
            ),
        )
    if target_id and item.item_type in EXECUTION_ITEMS | {"Environment"} and not destination_quiescence:
        return AdapterResult(
            RecoveryOutcome.BLOCKED,
            diagnostics=(
                "Supply item-specific destination quiescence evidence before updating this standby.",
            ),
        )
    if shell_only and not capabilities.safe_shell:
        return AdapterResult(
            RecoveryOutcome.BLOCKED,
            diagnostics=(f"'{item.item_type}' has no qualified definition-free shell creation contract.",),
        )
    if canonical_id(client.tenant_id()) != target_workspace.tenant_id:
        raise FabricError("Destination client is authenticated to a different tenant")
    canonical_id(generation_id)
    canonical_id(operation_id)
    definition_hash, properties_hash = captured_hashes(item, payloads)
    if shell_only:
        _mapping(
            item,
            target_workspace,
            source_items,
            item_mappings,
            workspace_mappings,
            endpoint_mappings,
            connection_mappings,
            target_id,
            verified_external_connections,
        )
        target = ItemIdentity(
            tenant_id=target_workspace.tenant_id,
            workspace_id=target_workspace.workspace_id,
            item_id=canonical_id(target_id)
            if target_id
            else canonical_id(
                _create_store(client, item, target_workspace.workspace_id)["id"],
            ),
        )
        observed_document = item_document(client, target, item.item_type)
        return AdapterResult(
            RecoveryOutcome.PARTIAL,
            applied=AppliedItem(
                source=item.identity,
                target=target,
                capture_generation_id=generation_id,
                applied_at=datetime.now(UTC),
                definition_sha256=definition_hash,
                properties_sha256=properties_hash,
                target_observed_sha256=observe_target(client, target, item.item_type),
                outcome=RecoveryOutcome.PARTIAL,
                operation_id=operation_id,
            ),
            target_properties=observed_document,
            diagnostics=(
                "Definition-free shell only; metadata is NOT synchronized. Run the data/schema provider "
                "before binding consumers. Do not treat source hashes as an applied-definition baseline.",
                *SECURITY_UNKNOWNS,
            ),
        )
    parts = _normalise_parts(definition_parts(item, payloads))
    deferred: list[dict[str, Any]] = []
    diagnostics = list(SECURITY_UNKNOWNS)
    environment_compute: dict[str, Any] | None = None
    try:
        replacements, references = _mapping(
            item,
            target_workspace,
            source_items,
            item_mappings,
            workspace_mappings,
            endpoint_mappings,
            connection_mappings,
            target_id,
            verified_external_connections,
        )
        for connection in verified_external_connections:
            observed_connection = capture_json(client, f"connections/{connection.connection_id}")
            if canonical_id(observed_connection.get("id", "")) != connection.connection_id:
                raise FabricError("Destination connection verification returned a different connection ID")
            remaining = analytics.dangling_references(
                [part("connection.json", observed_connection)],
                {},
                references,
            )
            if remaining:
                raise FabricError(
                    "The reused connection still references captured source resources: "
                    + ", ".join(remaining)
                )
        parts = _lakehouse_local_targets(item, parts, target_workspace, replacements)
        parts, deferred = _defer_grants(parts)
        deferred.extend(
            {"scope": "schedule", "path": payload.descriptor.path, "definition": payload.data.decode("utf-8")}
            for payload in payloads
            if payload.descriptor.payload_id in item.payload_ids
            and payload.descriptor.path.rsplit("/", 1)[-1] == ".schedules"
            and payload.descriptor.purpose == PayloadPurpose.CONFIGURATION
        )
        parts, inactivity = _inactive_parts(item, parts)
        diagnostics.extend(inactivity)
        _strict_references(item, parts, replacements, references, verified_external_connections)
        _report_binding(client, item, parts, source_items, item_mappings)
        if target_id and item.item_type in EXECUTION_ITEMS:
            jobs = capture_list(
                client, f"workspaces/{target_workspace.workspace_id}/items/{target_id}/jobs/instances"
            )
            if any(job.get("status") not in {"Completed", "Failed", "Cancelled", "Deduped"} for job in jobs):
                raise FabricError(
                    "Stop all active/pending destination job instances before changing their definition"
                )
            current = get_item_definition(
                client, target_workspace.workspace_id, target_id, fmt=definition_format(item.item_type)
            )
            if any(
                entry.get("path", "").rsplit("/", 1)[-1] == ".schedules" for entry in current.get("parts", [])
            ):
                raise FabricError(
                    "Remove destination scheduling definitions before updating this inactive standby"
                )
        if item.item_type == "Environment":
            environment_compute = _environment_preflight(client, item, parts, target_workspace, replacements)
        diagnostics.extend(
            analytics.validate_cross_tenant_references(
                parts,
                source_workspace_id=item.identity.workspace_id,
                target_workspace_id=target_workspace.workspace_id,
                id_map=replacements,
                target_client=client,
                source_items=references,
                item_type=item.item_type,
                ignore=tuple(value.connection_id for value in verified_external_connections),
            )
        )
        if item.item_type in {"Lakehouse", "Warehouse", "SQLDatabase"}:
            if tokens is None:
                raise FabricError(
                    "Supply destination SQL tokens for captured schema-only metadata restoration"
                )
            schemas = [
                value
                for value in payloads
                if value.descriptor.payload_id in item.payload_ids
                and value.descriptor.purpose == PayloadPurpose.SQL_SCHEMA
            ]
            if len(schemas) != 1:
                raise FabricError("Capture exactly one schema-only DACPAC before restoring this SQL store")
            _archive_schema(schemas[0].data)
            sql = item.properties.get("bcdr", {}).get("sql", {})
            if any(
                row.get("referenced_database_name")
                or row.get("referenced_server_name")
                or row.get("is_caller_dependent")
                or row.get("is_ambiguous")
                for row in sql.get("sql_dependencies", [])
            ):
                raise FabricError(
                    "Resolve the captured SQL cross-database/name-dependent references with a qualified "
                    "schema adapter; global workspace-name replacement is not safe."
                )
            if item.item_type == "Lakehouse":
                shortcuts = item.properties.get("bcdr", {}).get("shortcuts")
                if not isinstance(shortcuts, list):
                    raise FabricError("Capture the lakehouse shortcut inventory before restoring metadata")
                shortcut_parts = _lakehouse_local_targets(
                    item,
                    [part("shortcuts.metadata.json", shortcuts)],
                    target_workspace,
                    replacements,
                )
                _strict_references(
                    item, shortcut_parts, replacements, references, verified_external_connections
                )
        if item.item_type not in NO_DEFINITION and not parts and item.item_type != "SQLDatabase":
            raise FabricError("The captured item has no restorable definition parts")
    except FabricError as error:
        if type(error) not in {FabricError, analytics.StrandedReference, analytics.IdentityBindingError}:
            raise
        return AdapterResult(
            RecoveryOutcome.BLOCKED,
            deferred_grants=tuple(deferred),
            diagnostics=(str(error),),
        )

    workspace_id = target_workspace.workspace_id
    if target_id:
        target_id = canonical_id(target_id)
        item_document(
            client,
            ItemIdentity(
                tenant_id=target_workspace.tenant_id,
                workspace_id=workspace_id,
                item_id=target_id,
            ),
            item.item_type,
        )
    if item.item_type == "Environment":
        if environment_compute is None:
            raise FabricError("Environment staging preflight did not produce a compute request")
        target_id = _stage_environment(client, item, parts, workspace_id, environment_compute, target_id)
        diagnostics.append("Environment is staged only. Publish after explicit recovery approval.")
    elif item.item_type in {"Lakehouse", "Warehouse", "SQLDatabase"}:
        if not target_id:
            created = _create_store(client, item, workspace_id)
            target_id = canonical_id(created["id"])
        if item.item_type == "Lakehouse":
            rebound, _ = rewrite_parts(strip_part(parts, ".platform"), replacements)
            update_item_definition(
                client,
                workspace_id,
                target_id,
                rebound,
                definition_format=item.definition_format,
            )
    elif verified_external_connections or item.definition_format not in {
        None,
        definition_format(item.item_type),
    }:
        rebound, _ = rewrite_parts(strip_part(parts, ".platform"), replacements)
        if target_id:
            update_item_definition(
                client,
                workspace_id,
                target_id,
                rebound,
                definition_format=item.definition_format,
            )
        else:
            target_id = canonical_id(
                create_item(
                    client,
                    workspace_id,
                    item.display_name,
                    item.item_type,
                    parts=rebound,
                    definition_format=item.definition_format,
                )["id"]
            )
    else:
        migrated = analytics.migrate_definition_item(
            client,
            source_workspace_id=item.identity.workspace_id,
            target_workspace_id=workspace_id,
            item={"id": item.identity.item_id, "displayName": item.display_name},
            item_type=item.item_type,
            id_map=replacements,
            parts=parts,
            source_items=references,
            target_id=target_id,
            target_client=client,
            strict_references=True,
            exclude_identity=True,
        )
        target_id = canonical_id(migrated.target_id)
        diagnostics.extend(migrated.warnings)
    target_identity = ItemIdentity(
        tenant_id=target_workspace.tenant_id,
        workspace_id=workspace_id,
        item_id=target_id,
    )
    target = item_document(client, target_identity, item.item_type)
    if item.item_type in {"Lakehouse", "Warehouse", "SQLDatabase"}:
        if tokens is None:
            raise FabricError("Destination SQL tokens disappeared after schema preflight")
        _apply_sql_schema(item, payloads, target, tokens, replacements, references, mutation_guard)
        deferred.append({"scope": "sql", "captured": item.properties.get("bcdr", {}).get("sql", {})})
        diagnostics.append(
            "SQL grants, role memberships and pre/post-deployment scripts were excluded. "
            "Data, SQL access and non-SQL lakehouse schema/file readiness require separate qualification."
        )
    outcome = RecoveryOutcome.PARTIAL if item.item_type in STORES else RecoveryOutcome.RESTORED_STOPPED
    if item.item_type in STORES:
        diagnostics.append(
            f"'{item.display_name}' has standby metadata only. Supply its data provider and verify "
            "schemas, files, shortcuts and queries before cutover. An empty store is not recovered data."
        )
    observed = observe_target(client, target_identity, item.item_type)
    return AdapterResult(
        outcome=outcome,
        applied=AppliedItem(
            source=item.identity,
            target=target_identity,
            capture_generation_id=generation_id,
            applied_at=datetime.now(UTC),
            definition_sha256=definition_hash,
            properties_sha256=properties_hash,
            target_observed_sha256=observed,
            outcome=outcome,
            operation_id=operation_id,
        ),
        target_properties=target,
        deferred_grants=tuple(deferred),
        diagnostics=tuple(diagnostics),
        metadata_applied=True,
    )
