"""Capture healthy-source metadata into immutable Warehouse generation inputs.

No publication occurs here. A failed required read propagates its original service error;
the coordinator can retain the previous generation rather than publishing missing rows.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.capture_sources import (
    MAX_METADATA_BYTES,
    MetadataReaders,
    file_api_path,
    inspect_schema_archive,
)
from fabshuffle.bcdr.catalog import CapturedGeneration
from fabshuffle.bcdr.contracts import (
    AclScope,
    CaptureSnapshot,
    ConnectionIdentity,
    DependencyEdge,
    DesiredAcl,
    ItemIdentity,
    ItemRecord,
    PayloadDescriptor,
    PayloadPurpose,
    Principal,
    ProtectionKind,
    ProtectionRecord,
    Qualification,
    RecoveryOutcome,
    RecoverySet,
    WorkspaceIdentity,
    WorkspaceRecord,
    canonical_id,
    canonical_json,
    digest,
    logical_path,
    reject_embedded_secrets,
)
from fabshuffle.bcdr.payloads import CapturedPayload, chunk_count
from fabshuffle.bcdr.registry import TYPE_REGISTRY
from fabshuffle.fabric import analytics, special_items
from fabshuffle.fabric.client import FabricClient, FabricError
from fabshuffle.fabric.definitions import decode_json_part, find_part, is_text_part, part
from fabshuffle.fabric.items import get_item_definition

TYPED_COLLECTIONS = {
    "Lakehouse": "lakehouses",
    "Warehouse": "warehouses",
    "SQLDatabase": "sqlDatabases",
    "CosmosDBDatabase": "cosmosDbDatabases",
    "Eventhouse": "eventhouses",
    "KQLDatabase": "kqlDatabases",
    "MirroredDatabase": "mirroredDatabases",
    "SnowflakeDatabase": "snowflakeDatabases",
}
NO_DEFINITION = frozenset({"Warehouse", "Dashboard", "PaginatedReport"})
SECURITY_UNKNOWNS = (
    "Item sharing, OneLake effective access and inherited permissions are not fully enumerated. "
    "Review those scopes before enabling recovery; no such grants are applied by standby adapters.",
)


@dataclass(frozen=True, slots=True)
class ItemCapture:
    record: ItemRecord
    payloads: tuple[CapturedPayload, ...]


def make_payload(
    owner: ItemIdentity,
    path: str,
    data: bytes,
    purpose: PayloadPurpose,
    *,
    encoding: str = "utf-8",
    media_type: str = "application/octet-stream",
) -> CapturedPayload:
    if len(data) > MAX_METADATA_BYTES:
        raise FabricError(f"Metadata part '{path}' exceeds the {MAX_METADATA_BYTES}-byte limit")
    if purpose == PayloadPurpose.SQL_SCHEMA and path.lower().endswith(".dacpac"):
        inspect_schema_archive(data)
    descriptor = PayloadDescriptor(
        payload_id=str(uuid4()),
        owner=owner,
        path=path,
        purpose=purpose,
        media_type=media_type,
        encoding=encoding,
        byte_length=len(data),
        sha256=digest(data),
        chunk_count=chunk_count(len(data)),
    )
    payload = CapturedPayload(descriptor, data)
    payload.validate()
    return payload


def definition_format(item_type: str) -> str | None:
    if item_type == "Lakehouse":
        return "LakehouseDefinitionV1"
    if item_type == "SemanticModel":
        return "TMSL"
    if item_type == "SQLDatabase":
        return "dacpac"
    return special_items.policy_for(item_type).export_format


def item_document(client: FabricClient, identity: ItemIdentity, item_type: str) -> dict[str, Any]:
    collection = TYPED_COLLECTIONS.get(item_type, "items")
    document = client.get(f"workspaces/{identity.workspace_id}/{collection}/{identity.item_id}")
    if canonical_id(document.get("id", "")) != identity.item_id:
        raise FabricError("The item metadata endpoint returned a different item identity")
    if document.get("type", item_type) != item_type:
        raise FabricError("The item type changed during metadata capture; recapture the inventory")
    return document


def _definition_payloads(
    identity: ItemIdentity,
    definition: Mapping[str, Any],
    item_type: str,
) -> list[CapturedPayload]:
    parts = definition.get("parts")
    if not isinstance(parts, list) or not parts:
        raise FabricError(f"{item_type} '{identity.item_id}' returned no complete definition parts")
    result: list[CapturedPayload] = []
    paths: set[str] = set()
    for candidate in parts:
        if not isinstance(candidate, dict) or candidate.get("payloadType") != "InlineBase64":
            raise FabricError(
                "Capture supports InlineBase64 definition parts only; export a supported format"
            )
        path = logical_path(candidate["path"])
        if path in paths:
            raise FabricError(f"Duplicate definition part '{path}'")
        paths.add(path)
        encoded = candidate.get("payload")
        if not isinstance(encoded, str) or len(encoded) > (MAX_METADATA_BYTES + 2) // 3 * 4:
            raise FabricError(f"Invalid or oversized encoded definition part '{path}'")
        data = base64.b64decode(encoded, validate=True)
        purpose = (
            PayloadPurpose.SQL_SCHEMA
            if path.lower().endswith(".dacpac")
            else (
                PayloadPurpose.RUNTIME_FILE
                if item_type == "SparkJobDefinition" and path.startswith(("Main/", "Libs/"))
                else PayloadPurpose.DEFINITION
            )
        )
        result.append(
            make_payload(
                identity,
                path,
                data,
                purpose,
                encoding="utf-8" if is_text_part(path) else "binary",
            )
        )
    return result


def definition_parts(item: ItemRecord, payloads: Sequence[CapturedPayload]) -> list[dict[str, Any]]:
    """Validate ownership, ordering and integrity before constructing REST parts."""
    by_id = {payload.descriptor.payload_id: payload for payload in payloads}
    if len(by_id) != len(payloads):
        raise FabricError("Duplicate payload IDs in captured input")
    result: list[dict[str, Any]] = []
    paths: set[str] = set()
    for payload_id in item.payload_ids:
        if payload_id not in by_id:
            raise FabricError(f"Missing captured payload '{payload_id}' for '{item.display_name}'")
        payload = by_id[payload_id]
        payload.validate()
        if payload.descriptor.owner != item.identity or payload.descriptor.path in paths:
            raise FabricError("Captured payload owner/path mismatch")
        paths.add(payload.descriptor.path)
        if payload.descriptor.purpose == PayloadPurpose.DEFINITION or (
            item.item_type == "SparkJobDefinition"
            and payload.descriptor.purpose == PayloadPurpose.RUNTIME_FILE
        ):
            result.append(part(payload.descriptor.path, payload.data))
    return result


def _sql_coordinates(item_type: str, name: str, properties: Mapping[str, Any]) -> tuple[str, str]:
    if item_type == "Lakehouse":
        endpoint = properties.get("sqlEndpointProperties") or {}
        return endpoint.get("connectionString", ""), name
    if item_type == "SQLDatabase":
        return properties.get("serverFqdn", ""), properties.get("databaseName", "")
    return properties.get("connectionString") or properties.get("connectionInfo", ""), name


def capture_item(
    client: FabricClient,
    identity: ItemIdentity,
    item_type: str,
    *,
    readers: MetadataReaders,
    captured_at: datetime | None = None,
) -> ItemCapture:
    captured_at = captured_at or datetime.now(UTC)
    raw = item_document(client, identity, item_type)
    name = raw["displayName"]
    properties = dict(raw.get("properties") or {})
    metadata: dict[str, Any] = {
        "raw_item": raw,
        "security_unknowns": list(SECURITY_UNKNOWNS),
        "dependency_evidence": "partial",
        "dependency_action": "Review dynamic/code/name-based dependencies before marking bindings complete.",
    }
    unresolved: list[str] = []
    payloads: list[CapturedPayload] = []
    fmt = definition_format(item_type)
    contract = TYPE_REGISTRY.get(item_type)
    if contract is None or not contract.migration_rebuild:
        metadata["unsupported_reason"] = (
            f"'{name}' ({item_type}) has no source-independent reconstruction adapter. "
            "Use documented platform continuity or supply an independently prepared replacement."
        )
    elif item_type not in NO_DEFINITION:
        definition = get_item_definition(client, identity.workspace_id, identity.item_id, fmt=fmt)
        if definition.get("format") and fmt and definition["format"] != fmt:
            raise FabricError(f"'{name}' returned '{definition['format']}' instead of requested '{fmt}'")
        fmt = fmt or definition.get("format")
        payloads.extend(_definition_payloads(identity, definition, item_type))
        parts = list(definition["parts"])
        if item_type == "SemanticModel":
            model = find_part(parts, "model.bim")
            if not model or not isinstance(decode_json_part(model["payload"]).get("model"), dict):
                unresolved.append(f"'{name}' needs a complete inspectable TMSL model.bim capture.")
        elif item_type == "SparkJobDefinition":
            if not find_part(parts, special_items.SPARK_JOB_PAYLOAD_PART):
                unresolved.append(f"'{name}' omitted its SparkJobDefinitionV1.json runtime manifest.")
            metadata["runtime_requirements"] = special_items.spark_job_warnings(parts, identity.workspace_id)
        elif item_type == "SnowflakeDatabase":
            creation = special_items.snowflake_creation_payload(raw, parts)
            if creation is None:
                unresolved.append(f"'{name}' needs its Snowflake database name and connection ID captured.")
            metadata["creation_payload"] = creation
        elif item_type == "CosmosDBDatabase":
            candidate = find_part(parts, "definition.json")
            body = decode_json_part(candidate["payload"]) if candidate else {}
            if not isinstance(body.get("containers"), list):
                unresolved.append(f"'{name}' needs its Cosmos container/partition-key metadata captured.")
            metadata["containers"] = body.get("containers")
            metadata["consistency_evidence"] = "Unknown; qualify the logical export's consistency separately."
        elif item_type == "Dataflow":
            candidate = find_part(parts, analytics.QUERY_METADATA_PART)
            body = decode_json_part(candidate["payload"]) if candidate else {}
            if str(body.get("formatVersion", "")) != analytics.CICD_DATAFLOW_FORMAT_VERSION:
                unresolved.append(f"Upgrade '{name}' to Dataflow Gen2 (CI/CD), then capture its definition.")

    if contract and contract.migration_rebuild:
        # Use the strict REST endpoint, not preview helpers that replace read failures with [].
        metadata["connections"] = client.list_all(
            f"workspaces/{identity.workspace_id}/items/{identity.item_id}/connections",
        )
    if item_type in {"Lakehouse", "Warehouse"}:
        metadata.update(readers.lakehouse_tables(identity))
        if item_type == "Lakehouse":
            metadata["schema_enabled"] = "defaultSchema" in properties
            metadata["files_inventory"] = readers.files_inventory(identity)
            metadata["shortcuts"] = client.list_all(
                f"workspaces/{identity.workspace_id}/items/{identity.item_id}/shortcuts",
            )
            metadata["data_access_roles"] = client.list_all(
                f"workspaces/{identity.workspace_id}/items/{identity.item_id}/dataAccessRoles",
            )
    if item_type in {"Lakehouse", "Warehouse", "SQLDatabase"}:
        server, database = _sql_coordinates(item_type, name, properties)
        metadata["sql"] = readers.sql_metadata(server, database)
        # Delta-only metadata cannot establish the absence of non-Delta tables.
        if item_type == "Lakehouse":
            delta_names = {
                (table["schema_name"], table["name"])
                for table in metadata.get("tables", [])
                if "storage_location" in table
            }
            metadata["non_delta_qualification"] = (
                "Inspect non-Delta/external tables and saved DDL; SQL metadata visibility is not "
                "proof of a complete Spark catalog."
            )
            metadata["delta_table_names"] = [list(value) for value in sorted(delta_names)]
        if not any(payload.descriptor.purpose == PayloadPurpose.SQL_SCHEMA for payload in payloads):
            payloads.append(
                make_payload(
                    identity,
                    "schema/source.dacpac",
                    readers.sql_schema(server, database),
                    PayloadPurpose.SQL_SCHEMA,
                    encoding="binary",
                )
            )
    if item_type == "KQLDatabase":
        metadata["kql"] = readers.kql_metadata(
            properties.get("queryServiceUri", ""),
            identity.item_id,
            follower=properties.get("databaseType") == "Shortcut",
        )
        metadata["table_shortcuts"] = client.list_all(
            f"workspaces/{identity.workspace_id}/kqlDatabases/{identity.item_id}/shortcuts",
        )
    if item_type == "ApacheAirflowJob":
        root = f"workspaces/{identity.workspace_id}/apacheAirflowJobs/{identity.item_id}/files"
        files = client.list_all(root, params={"beta": "true"})
        metadata["runtime_files"] = files
        for entry in files:
            path = logical_path(entry["filePath"])
            if entry.get("sizeInBytes", 0) > MAX_METADATA_BYTES:
                raise FabricError(f"Airflow file '{path}' exceeds the metadata payload limit")
            response = client.request("GET", f"{root}/{file_api_path(path)}", params={"beta": "true"})
            payloads.append(
                make_payload(
                    identity,
                    f"runtime/{path}",
                    response.content,
                    PayloadPurpose.RUNTIME_FILE,
                    encoding="utf-8" if is_text_part(path) else "binary",
                )
            )
    if item_type == "Eventhouse" and not isinstance(properties.get("databasesItemIds"), list):
        unresolved.append(f"'{name}' needs the Eventhouse child database IDs captured.")
    activation = "unknown"
    if item_type == "MirroredDatabase":
        metadata["mirroring_status"] = client.post(
            f"workspaces/{identity.workspace_id}/mirroredDatabases/{identity.item_id}/getMirroringStatus",
        )
        status = metadata["mirroring_status"].get("status")
        activation = (
            "stopped"
            if status in {"Stopped", "Initialized"}
            else ("running" if status in {"Running", "Starting"} else "unknown")
        )
    elif item_type == "MirroredAzureDatabricksCatalog":
        candidate = next((p for p in payloads if p.descriptor.path == "definition.json"), None)
        if candidate:
            state = json.loads(candidate.data).get("autoSync")
            activation = "stopped" if state == "Disabled" else "running" if state == "Enabled" else "unknown"
    properties["bcdr"] = metadata
    reject_embedded_secrets(canonical_json(properties))
    return ItemCapture(
        ItemRecord(
            identity=identity,
            item_type=item_type,
            display_name=name,
            captured_at=captured_at,
            api_version="v1",
            definition_format=fmt,
            properties=properties,
            payload_ids=tuple(payload.descriptor.payload_id for payload in payloads),
            source_version=raw.get("etag"),
            capture_complete=not unresolved,
            unresolved=tuple(unresolved),
            activation=activation,
        ),
        tuple(payloads),
    )


def _role_assignments(
    rows: Sequence[Mapping[str, Any]],
    *,
    workspace: WorkspaceIdentity | None = None,
    connection: ConnectionIdentity | None = None,
) -> tuple[list[DesiredAcl], list[str]]:
    target = workspace or connection
    if target is None:
        raise ValueError("Role capture requires a workspace or connection identity")
    acls: list[DesiredAcl] = []
    unknowns: list[str] = []
    for row in rows:
        principal = row.get("principal") or {}
        if principal.get("type") not in {"User", "Group", "ServicePrincipal"}:
            unknowns.append(
                f"Resolve unsupported principal type '{principal.get('type')}' on '{target.key}'."
            )
            continue
        acls.append(
            DesiredAcl(
                acl_id=str(uuid4()),
                scope=AclScope.WORKSPACE if workspace else AclScope.CONNECTION,
                workspace=workspace,
                connection=connection,
                principal=Principal(
                    tenant_id=target.tenant_id,
                    object_id=principal["id"],
                    kind=principal["type"],
                ),
                permission=row["role"],
                provenance="Fabric v1 roleAssignments",
            )
        )
    return acls, unknowns


def _dependencies(captures: Sequence[ItemCapture]) -> tuple[DependencyEdge, ...]:
    """Known literal evidence, never a claim that arbitrary executable code was analyzed."""
    edges: list[DependencyEdge] = []
    known_ids = {capture.record.identity.item_id for capture in captures}
    workspace_ids = {capture.record.identity.workspace_id for capture in captures}
    for capture in captures:
        item = capture.record
        parts = definition_parts(item, capture.payloads)
        text = "\n".join(
            payload.data.decode("utf-8")
            for payload in capture.payloads
            if payload.descriptor.encoding == "utf-8"
        ) + canonical_json(item.properties.get("bcdr", {})).decode("utf-8")
        for other in captures:
            if other.record.identity == item.identity:
                continue
            identifiers = analytics.reference_identifiers(
                {
                    other.record.identity.item_id: {
                        "id": other.record.identity.item_id,
                        "properties": other.record.properties,
                    },
                }
            )
            if any(identifier.casefold() in text.casefold() for identifier in identifiers):
                edges.append(
                    DependencyEdge(
                        edge_id=str(uuid4()),
                        consumer=item.identity,
                        prerequisite=other.record.identity,
                        phase="bind",
                        provenance="captured literal definition/metadata",
                        qualification=Qualification.DOCUMENTED,
                        detail=f"Map '{other.record.display_name}' before binding '{item.display_name}'.",
                    )
                )
        connection_ids = set(analytics.with_connection_references(parts))
        for connection in item.properties.get("bcdr", {}).get("connections", []):
            if connection.get("id"):
                connection_ids.add(connection["id"])
        for connection_id in sorted({canonical_id(value) for value in connection_ids}):
            edges.append(
                DependencyEdge(
                    edge_id=str(uuid4()),
                    consumer=item.identity,
                    prerequisite=ConnectionIdentity(
                        tenant_id=item.identity.tenant_id,
                        connection_id=connection_id,
                    ),
                    phase="bind",
                    provenance="Fabric definition/item connections",
                    qualification=Qualification.DOCUMENTED,
                    detail=f"Provision and map connection '{connection_id}' without exporting credentials.",
                )
            )
        explicit_ids = {
            canonical_id(value)
            for value in analytics._source_item_references(
                parts,
                item.identity.workspace_id,
                rewritten_workspaces=workspace_ids,
            )
        }
        for item_id in sorted(explicit_ids - known_ids):
            edges.append(
                DependencyEdge(
                    edge_id=str(uuid4()),
                    consumer=item.identity,
                    external_reference=f"uncaptured-item:{item_id}",
                    phase="bind",
                    qualification=Qualification.UNVERIFIED,
                    provenance="explicit captured item binding absent from estate inventory",
                    detail=f"Capture and qualify item '{item_id}' before binding '{item.display_name}'.",
                )
            )
        edges.append(
            DependencyEdge(
                edge_id=str(uuid4()),
                consumer=item.identity,
                external_reference=f"binding-review:{item.identity.key}",
                phase="activate",
                qualification=Qualification.UNVERIFIED,
                provenance="capture dependency coverage",
                detail="Review dynamic/name-based references and runtime access before enabling recovery.",
            )
        )
    return tuple(edges)


def capture_workspaces(
    client: FabricClient,
    recovery_set: RecoverySet,
    *,
    tokens: TokenProvider,
    workspace_ids: Sequence[str] | None = None,
    generation_id: str | None = None,
    parent_generation_id: str | None = None,
    readers: MetadataReaders | None = None,
) -> CapturedGeneration:
    """Read selected source workspaces completely; never mutate/publish the Warehouse."""
    if canonical_id(client.tenant_id()) != recovery_set.tenant_id:
        raise FabricError("The capture client is authenticated to a different tenant")
    readers = readers or MetadataReaders(tokens)
    selected = {canonical_id(value) for value in workspace_ids} if workspace_ids is not None else None
    if selected is not None and recovery_set.control_workspace.workspace_id in selected:
        raise FabricError("The control workspace cannot be captured as a business workspace")
    listed = client.list_all("workspaces")
    candidates = [
        workspace
        for workspace in listed
        if canonical_id(workspace["id"]) != recovery_set.control_workspace.workspace_id
        and (selected is None or canonical_id(workspace["id"]) in selected)
        and (
            selected is not None
            or not workspace.get("capacityId")
            or canonical_id(workspace["capacityId"]) in recovery_set.source_capacity_ids
        )
    ]
    if selected is not None and {canonical_id(workspace["id"]) for workspace in candidates} != selected:
        raise FabricError(
            "One or more explicitly selected source workspaces are absent from the visible inventory"
        )
    captured_at = datetime.now(UTC)
    workspaces: list[WorkspaceRecord] = []
    captures: list[ItemCapture] = []
    desired: list[DesiredAcl] = []
    unresolved: list[str] = []
    connections: dict[str, ConnectionIdentity] = {}
    for candidate in candidates:
        identity = WorkspaceIdentity(tenant_id=recovery_set.tenant_id, workspace_id=candidate["id"])
        root = f"workspaces/{identity.workspace_id}"
        workspace = client.get(root)
        if canonical_id(workspace["id"]) != identity.workspace_id:
            raise FabricError("Workspace metadata returned a different source identity")
        if not workspace.get("capacityId") and selected is None:
            continue
        capacity = canonical_id(workspace["capacityId"])
        if capacity not in recovery_set.source_capacity_ids:
            if selected is None:
                continue
            raise FabricError(f"'{workspace['displayName']}' is outside the explicit source capacity scope")
        if workspace.get("capacityAssignmentProgress") not in {None, "Completed"}:
            raise FabricError(f"'{workspace['displayName']}' has an unsettled source capacity assignment")
        roles = client.list_all(f"{root}/roleAssignments")
        acls, gaps = _role_assignments(roles, workspace=identity)
        desired.extend(acls)
        unresolved.extend(gaps)
        properties = {
            **workspace,
            "bcdr": {
                "control_workspace": recovery_set.control_workspace.model_dump(mode="json"),
                "folders": client.list_all(f"{root}/folders"),
                "spark_pools": client.list_all(f"{root}/spark/pools"),
                "spark_settings": client.get(f"{root}/spark/settings"),
                "role_assignments": roles,
            },
        }
        reject_embedded_secrets(canonical_json(properties))
        for raw in client.list_all(f"{root}/items"):
            capture = capture_item(
                client,
                ItemIdentity(
                    tenant_id=identity.tenant_id,
                    workspace_id=identity.workspace_id,
                    item_id=raw["id"],
                ),
                raw["type"],
                readers=readers,
            )
            captures.append(capture)
            for row in capture.record.properties.get("bcdr", {}).get("connections", []):
                if row.get("id"):
                    connection = ConnectionIdentity(tenant_id=identity.tenant_id, connection_id=row["id"])
                    connections[connection.key] = connection
        workspaces.append(
            WorkspaceRecord(
                identity=identity,
                capacity_id=capacity,
                display_name=workspace["displayName"],
                captured_at=captured_at,
                properties=properties,
                inventory_complete=True,
            )
        )
    for connection in connections.values():
        rows = client.list_all(f"connections/{connection.connection_id}/roleAssignments")
        acls, gaps = _role_assignments(rows, connection=connection)
        desired.extend(acls)
        unresolved.extend(gaps)
    payloads = tuple(payload for capture in captures for payload in capture.payloads)
    protections = tuple(
        ProtectionRecord(
            protection_id=str(uuid4()),
            item=capture.record.identity,
            kind=ProtectionKind.UNPROTECTED,
            outcome=RecoveryOutcome.PROTECTION_MISSING,
            provenance="metadata-only capture",
            action=f"Supply data protection for '{capture.record.display_name}'; metadata is not data.",
        )
        for capture in captures
        if TYPE_REGISTRY.get(capture.record.item_type)
        and TYPE_REGISTRY[capture.record.item_type].data_requirements
    )
    snapshot = CaptureSnapshot(
        recovery_set_id=recovery_set.recovery_set_id,
        generation_id=generation_id or str(uuid4()),
        parent_generation_id=parent_generation_id,
        captured_at=captured_at,
        workspaces=tuple(workspaces),
        items=tuple(capture.record for capture in captures),
        payloads=tuple(p.descriptor for p in payloads),
        dependencies=_dependencies(captures),
        desired_acls=tuple(desired),
        protections=protections,
        inventory_complete=True,
        unresolved=tuple(unresolved),
    )
    return CapturedGeneration(snapshot, payloads)
