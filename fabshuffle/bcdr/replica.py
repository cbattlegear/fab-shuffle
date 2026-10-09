"""Qualified, destination-only links to retained native OneLake data.

This adapter verifies shortcut metadata, not failover routing, data consistency or SQL
readiness. Create Shortcut has no read-only flag: an independently qualified caller-mode
access policy is required, and this adapter never grants rights or tests writes.

Create/Get/List contracts: https://learn.microsoft.com/rest/api/fabric/core/onelake-shortcuts
Schema layout: https://learn.microsoft.com/fabric/data-engineering/lakehouse-schemas
Caller vs delegated-owner access: https://learn.microsoft.com/fabric/onelake/onelake-shortcuts
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Self
from urllib.parse import quote, unquote, urlsplit

from pydantic import AwareDatetime, model_validator

from fabshuffle.bcdr.capture import capture_json, capture_list, item_document
from fabshuffle.bcdr.contracts import (
    AppliedItem,
    CaptureSnapshot,
    Digest,
    Guid,
    ItemRecord,
    Nonempty,
    Principal,
    Record,
    RecoveryDataBinding,
    RecoveryOutcome,
    RecoverySet,
    canonical_id,
    canonical_json,
    digest,
    logical_path,
)
from fabshuffle.fabric.client import FabricApiError, FabricClient, FabricError


class ReplicaAttachmentError(FabricError):
    """The requested retained-source boundary cannot be established safely."""


class ReplicaAccessEvidence(Record):
    """Externally verified enforcement evidence, not a permission grant or live probe."""

    qualification_id: Guid
    binding_sha256: Digest
    principal: Principal
    verified_at: AwareDatetime
    valid_until: AwareDatetime
    enforcement_reference: Nonempty
    access_mode: Literal["caller"] = "caller"

    @model_validator(mode="after")
    def time_window(self) -> Self:
        if self.valid_until <= self.verified_at:
            raise ValueError("Read-only qualification must expire after its verification time")
        return self


@dataclass(frozen=True, slots=True)
class CoverageResult:
    required_paths: tuple[str, ...]
    covered_paths: tuple[str, ...]
    missing_paths: tuple[str, ...]
    complete: bool


@dataclass(frozen=True, slots=True)
class AttachmentResult:
    binding: RecoveryDataBinding
    shortcut: dict[str, Any]
    changed: bool
    attachment_verified: bool
    diagnostics: tuple[str, ...]
    outcome: RecoveryOutcome = RecoveryOutcome.TEMPORARY_ATTACHED
    data_ready: Literal[False] = False
    endpoint_ready: Literal[False] = False

    @property
    def attached(self) -> bool:
        return self.attachment_verified


def binding_digest(binding: RecoveryDataBinding) -> str:
    return digest(canonical_json(binding))


def _path(value: str) -> str:
    value = logical_path(value)
    if not value.isascii() or any(char in value for char in '+?*<>|"'):
        raise ReplicaAttachmentError(
            f"'{value}' is outside the qualified ASCII shortcut syntax; copy it independently."
        )
    return value


def _location(path: str, name: str) -> str:
    _path(path)
    _path(name)
    if "/" in name:
        raise ReplicaAttachmentError("A shortcut name must be one path component")
    return _path(f"{path}/{name}")


def _metadata(source: ItemRecord) -> dict[str, Any]:
    if source.item_type != "Lakehouse":
        raise ReplicaAttachmentError(
            "Temporary attachments accept source Lakehouses, not writable Warehouse recovery"
        )
    if source.tombstone or not source.capture_complete or source.unresolved:
        raise ReplicaAttachmentError(
            "Capture complete Lakehouse metadata before qualifying a retained-source link"
        )
    metadata = source.properties.get("bcdr")
    if not isinstance(metadata, dict):
        raise ReplicaAttachmentError("The source Lakehouse has no captured recovery inventories")
    for name in ("tables", "schemas", "files_inventory", "shortcuts"):
        if not isinstance(metadata.get(name), list):
            raise ReplicaAttachmentError(f"Capture the source Lakehouse '{name}' inventory before attachment")
    if not isinstance(metadata.get("schema_enabled"), bool):
        raise ReplicaAttachmentError("Capture the source Lakehouse schema mode before attachment")
    if metadata["schema_enabled"] != ("defaultSchema" in source.properties):
        raise ReplicaAttachmentError("Captured Lakehouse schema-mode evidence is inconsistent")
    return metadata


def _native_relative_path(source: ItemRecord, location: str) -> str:
    parsed = urlsplit(location)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "onelake.dfs.fabric.microsoft.com"
        or parsed.port not in {None, 443}
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ReplicaAttachmentError(
            "Capture a GUID-addressed global OneLake storage location; "
            "external/name-based paths are not qualified"
        )
    segments = unquote(parsed.path).removeprefix("/").split("/")
    if len(segments) < 4:
        raise ReplicaAttachmentError("The captured table storage location is incomplete")
    if (
        canonical_id(segments[0]) != source.identity.workspace_id
        or canonical_id(segments[1]) != source.identity.item_id
    ):
        raise ReplicaAttachmentError("The table storage location belongs to a different source item")
    path = _path("/".join(segments[2:]))
    if not path.startswith("Tables/"):
        raise ReplicaAttachmentError("A table attachment requires its captured Tables storage path")
    return path


def _tables(source: ItemRecord) -> dict[str, dict[str, Any]]:
    metadata = _metadata(source)
    schemas = {row.get("name") for row in metadata["schemas"] if isinstance(row, dict)}
    result: dict[str, dict[str, Any]] = {}
    logical_names: set[tuple[str, str]] = set()
    for row in metadata["tables"]:
        if not isinstance(row, dict) or not isinstance(row.get("storage_location"), str):
            raise ReplicaAttachmentError("A captured table omitted its actual storage location")
        if str(row.get("data_source_format", "")).upper() != "DELTA":
            raise ReplicaAttachmentError(
                f"Table '{row.get('name')}' has no verified Delta format; prepare an independent data input."
            )
        path = _native_relative_path(source, row["storage_location"])
        name, schema = row.get("name"), row.get("schema_name")
        if not isinstance(name, str) or not isinstance(schema, str) or schema not in schemas:
            raise ReplicaAttachmentError("A captured table omitted its logical name or captured schema")
        _path(name)
        _path(schema)
        if "/" in name or "/" in schema or any(char.isspace() for char in name):
            raise ReplicaAttachmentError(
                "This table/schema name cannot be represented as a qualified Delta shortcut"
            )
        if path in result or (schema, name) in logical_names:
            raise ReplicaAttachmentError(
                "The captured table inventory has duplicate physical or logical identities"
            )
        result[path] = row
        logical_names.add((schema, name))
    return result


def _files(source: ItemRecord) -> dict[str, bool]:
    result: dict[str, bool] = {}
    for entry in _metadata(source)["files_inventory"]:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            raise ReplicaAttachmentError("A captured Files entry has no path")
        owner, separator, relative = entry["name"].partition("/")
        if not separator or canonical_id(owner) != source.identity.item_id:
            raise ReplicaAttachmentError("A captured Files entry belongs to a different item")
        path = _path(relative)
        if not path.startswith("Files/") or path in result:
            raise ReplicaAttachmentError("The captured Files inventory contains an invalid or duplicate path")
        directory = entry.get("isDirectory", False)
        if not isinstance(directory, bool):
            raise ReplicaAttachmentError("A captured Files entry has an unknown directory/file type")
        result[path] = directory
    return result


def _source_shortcuts(source: ItemRecord) -> tuple[str, ...]:
    locations = []
    for entry in _metadata(source)["shortcuts"]:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str) or not entry.get("name"):
            raise ReplicaAttachmentError("The captured shortcut inventory is incomplete")
        locations.append(_location(entry["path"].strip("/"), entry["name"]))
    return tuple(locations)


def captured_recovery_paths(source: ItemRecord) -> tuple[str, ...]:
    """Required units: exact Delta paths, whole top-level Files folders, and loose files.

    A loose root file deliberately remains a required, non-attachable unit: a folder
    shortcut under Files/recovered would change its original consumer path.
    """
    paths = set(_tables(source))
    for path in _files(source):
        paths.add("/".join(path.split("/")[:2]))
    return tuple(sorted(paths))


def _manifest_source(generation: CaptureSnapshot, source: ItemRecord) -> None:
    if generation.capture_kind != "source" or not generation.inventory_complete or generation.unresolved:
        raise ReplicaAttachmentError("Use a complete source capture generation for temporary continuity")
    matches = [row for row in generation.items if row.identity == source.identity]
    if len(matches) != 1 or matches[0] != source:
        raise ReplicaAttachmentError("The source record must exactly match the pinned capture generation")
    _metadata(source)


def _selection_entry(
    generation: CaptureSnapshot,
    binding: RecoveryDataBinding,
    source: ItemRecord,
    shortcut_path: str,
    shortcut_name: str,
) -> None:
    RecoveryDataBinding.model_validate(binding.model_dump(mode="json"))
    _manifest_source(generation, source)
    if binding.generation_id != generation.generation_id or binding.source != source.identity:
        raise ReplicaAttachmentError("The attachment binding does not match its captured generation/source")
    if any(
        row.identity.tenant_id == binding.consumer.tenant_id
        and row.identity.workspace_id == binding.consumer.workspace_id
        for row in generation.workspaces
    ):
        raise ReplicaAttachmentError("The attachment consumer cannot be inside the captured source estate")
    destination = _location(shortcut_path, shortcut_name)
    path = _path(binding.source_path)
    for existing in _source_shortcuts(source):
        if path == existing or path.startswith(existing + "/") or existing.startswith(path + "/"):
            raise ReplicaAttachmentError(
                f"'{path}' overlaps captured shortcut '{existing}'; "
                "chained/external targets need separate protection."
            )
    if path.startswith("Tables/"):
        table = _tables(source).get(path)
        if table is None:
            raise ReplicaAttachmentError(
                "Bind the exact captured Delta table storage path, not a schema or subdirectory"
            )
        parent = f"Tables/{table['schema_name']}" if _metadata(source)["schema_enabled"] else "Tables"
        logical_source = f"{parent}/{table['name']}"
        if any(
            logical_source == existing or logical_source.startswith(existing + "/")
            for existing in _source_shortcuts(source)
        ):
            raise ReplicaAttachmentError(
                f"'{logical_source}' is a captured table/schema shortcut, not qualified native table data."
            )
        if destination != f"{parent}/{table['name']}":
            raise ReplicaAttachmentError(
                "Preserve the captured table's logical schema/name in the recovery Lakehouse"
            )
    elif path.startswith("Files/"):
        files = _files(source)
        is_directory = files.get(path) is True or any(value.startswith(path + "/") for value in files)
        if not is_directory or files.get(path) is False:
            raise ReplicaAttachmentError(
                "Only captured Files directories can be attached; copy loose files independently"
            )
        if destination != path:
            raise ReplicaAttachmentError(
                "Preserve the original Files directory path; an alias is not recovery coverage"
            )
    else:
        raise ReplicaAttachmentError(
            "Attach an exact table or Files subdirectory; broad Files/Tables roots are not allowed"
        )


def validate_attachment_selection(
    generation: CaptureSnapshot,
    source: ItemRecord,
    configurations: Sequence[tuple[RecoveryDataBinding, str, str]],
) -> CoverageResult:
    _manifest_source(generation, source)
    required = captured_recovery_paths(source)
    selected: set[str] = set()
    destinations: set[str] = set()
    consumer = configurations[0][0].consumer if configurations else None
    for binding, path, name in configurations:
        _selection_entry(generation, binding, source, path, name)
        if binding.consumer != consumer:
            raise ReplicaAttachmentError("An attachment batch must cover one exact recovery consumer")
        destination = _location(path, name)
        if binding.source_path in selected or any(
            destination == other or destination.startswith(other + "/") or other.startswith(destination + "/")
            for other in destinations
        ):
            raise ReplicaAttachmentError("Attachment selections contain duplicate or overlapping paths")
        selected.add(binding.source_path)
        destinations.add(destination)
    covered = tuple(path for path in required if path in selected)
    missing = tuple(path for path in required if path not in selected)
    return CoverageResult(required, covered, missing, bool(required) and bool(configurations) and not missing)


def validate_recovery_attachment(
    generation: CaptureSnapshot,
    binding: RecoveryDataBinding,
    source: ItemRecord,
    target_mapping: AppliedItem,
    shortcut_path: str,
    shortcut_name: str,
    *,
    recovery_set: RecoverySet,
    access_evidence: ReplicaAccessEvidence,
) -> None:
    """Pure validation, including qualification expiry; no service calls or grants."""
    _selection_entry(generation, binding, source, shortcut_path, shortcut_name)
    generation.require_publishable(recovery_set)
    AppliedItem.model_validate(target_mapping.model_dump(mode="json"))
    ReplicaAccessEvidence.model_validate(access_evidence.model_dump(mode="json"))
    if (
        target_mapping.source != source.identity
        or target_mapping.target != binding.consumer
        or target_mapping.capture_generation_id != generation.generation_id
        or target_mapping.outcome
        not in {
            RecoveryOutcome.PARTIAL,
            RecoveryOutcome.RESTORED_STOPPED,
            RecoveryOutcome.TEMPORARY_ATTACHED,
        }
    ):
        raise ReplicaAttachmentError(
            "Use the catalog's owned source-to-consumer mapping for this exact generation"
        )
    if (
        binding.consumer.tenant_id != recovery_set.tenant_id
        or binding.consumer.workspace_id == recovery_set.control_workspace.workspace_id
    ):
        raise ReplicaAttachmentError(
            "The recovery control workspace cannot receive retained-source attachments"
        )
    now = datetime.now(UTC)
    if (
        access_evidence.binding_sha256 != binding_digest(binding)
        or access_evidence.enforcement_reference != binding.evidence
        or access_evidence.principal.tenant_id != source.identity.tenant_id
        or access_evidence.verified_at < generation.captured_at
        or not access_evidence.verified_at <= now < access_evidence.valid_until
    ):
        raise ReplicaAttachmentError(
            "Supply current, exact-binding caller-mode read-only enforcement evidence"
        )
    read_permissions = {"Viewer", "Read", "ReadAll", "ReadData", "SELECT", "CONNECT"}
    for acl in generation.desired_acls:
        if acl.principal != access_evidence.principal:
            continue
        names_source = acl.item == source.identity or (
            acl.workspace is not None
            and acl.workspace.tenant_id == source.identity.tenant_id
            and acl.workspace.workspace_id == source.identity.workspace_id
        )
        if names_source and acl.permission not in read_permissions:
            raise ReplicaAttachmentError(
                f"The qualified runtime principal has captured '{acl.permission}' access to the source; "
                "establish read-only effective access and recapture/requalify before attachment."
            )


validate_attachment = validate_recovery_attachment


def _same_shortcut(observed: Mapping[str, Any], desired: Mapping[str, Any]) -> bool:
    # Ignore only the documented response discriminator. Transform/other extra fields
    # are not harmless metadata: they can describe execution-bearing shortcuts.
    if set(observed) != {"path", "name", "target"} or (
        observed.get("path") != desired["path"] or observed.get("name") != desired["name"]
    ):
        return False
    target = observed.get("target")
    if not isinstance(target, dict) or set(target) - {"type", "oneLake"}:
        return False
    if target.get("type", "OneLake") != "OneLake" or not isinstance(target.get("oneLake"), dict):
        return False
    actual = dict(target["oneLake"])
    if set(actual) != {"workspaceId", "itemId", "path"}:
        return False
    actual["workspaceId"] = canonical_id(actual["workspaceId"])
    actual["itemId"] = canonical_id(actual["itemId"])
    return actual == desired["target"]["oneLake"]


def attach_recovery_data(
    client: FabricClient,
    *,
    generation: CaptureSnapshot,
    binding: RecoveryDataBinding,
    source: ItemRecord,
    target_mapping: AppliedItem,
    recovery_set: RecoverySet,
    shortcut_path: str,
    shortcut_name: str,
    access_evidence: ReplicaAccessEvidence,
    mutation_guard: Callable[[], None],
) -> AttachmentResult:
    """Create with Abort or reuse an exactly matching shortcut; never read source compute."""
    validate_recovery_attachment(
        generation,
        binding,
        source,
        target_mapping,
        shortcut_path,
        shortcut_name,
        recovery_set=recovery_set,
        access_evidence=access_evidence,
    )
    if canonical_id(client.tenant_id()) != recovery_set.tenant_id or not callable(mutation_guard):
        raise ReplicaAttachmentError("Use the recovery tenant client and a live controller mutation guard")
    consumer = binding.consumer
    workspace = capture_json(client, f"workspaces/{consumer.workspace_id}")
    if (
        canonical_id(workspace.get("id", "")) != consumer.workspace_id
        or canonical_id(workspace.get("capacityId", "")) not in recovery_set.target_capacity_ids
        or workspace.get("capacityAssignmentProgress") not in {None, "Completed"}
    ):
        raise ReplicaAttachmentError("The owned consumer workspace is not on a settled recovery capacity")
    target = item_document(client, consumer, "Lakehouse")
    if target.get("workspaceId") and canonical_id(target["workspaceId"]) != consumer.workspace_id:
        raise ReplicaAttachmentError("Destination Lakehouse properties returned the wrong workspace")
    if (
        binding.source_path.startswith("Tables/")
        and ("defaultSchema" in (target.get("properties") or {})) != _metadata(source)["schema_enabled"]
    ):
        raise ReplicaAttachmentError("Preserve the captured schema mode before attaching Delta tables")
    desired = {
        "path": shortcut_path,
        "name": shortcut_name,
        "target": {
            "oneLake": {
                "workspaceId": source.identity.workspace_id,
                "itemId": source.identity.item_id,
                "path": binding.source_path,
            }
        },
    }
    location = _location(shortcut_path, shortcut_name)
    collection = f"workspaces/{consumer.workspace_id}/items/{consumer.item_id}/shortcuts"
    get_path = f"{collection}/{quote(shortcut_path, safe='/')}/{quote(shortcut_name, safe='')}"
    matches = []
    for entry in capture_list(client, collection):
        if not isinstance(entry.get("path"), str) or not isinstance(entry.get("name"), str):
            raise ReplicaAttachmentError("The destination shortcut inventory is incomplete")
        existing = _location(entry["path"], entry["name"])
        if existing == location:
            matches.append(entry)
        elif existing.startswith(location + "/") or location.startswith(existing + "/"):
            raise ReplicaAttachmentError(
                f"Destination shortcut '{existing}' overlaps '{location}'; "
                "do not attach through/over another shortcut."
            )
    if len(matches) > 1 or (matches and not _same_shortcut(matches[0], desired)):
        raise ReplicaAttachmentError(
            f"An unrelated or ambiguous shortcut occupies '{location}'; it was not overwritten"
        )
    changed = False
    diagnostics = []
    if not matches:
        mutation_guard()
        validate_recovery_attachment(
            generation,
            binding,
            source,
            target_mapping,
            shortcut_path,
            shortcut_name,
            recovery_set=recovery_set,
            access_evidence=access_evidence,
        )
        try:
            client.request(
                "POST",
                collection,
                json=desired,
                params={"shortcutConflictPolicy": "Abort"},
                expected=(201,),
            )
            changed = True
        except FabricApiError as error:
            if error.status_code != 409:
                raise
            observed = capture_json(client, get_path)
            if not _same_shortcut(observed, desired):
                raise
            diagnostics.append(f"Create was rejected, then the exact shortcut was verified: {error}")
    observed = capture_json(client, get_path)
    if not _same_shortcut(observed, desired):
        raise ReplicaAttachmentError(f"Destination GET did not confirm the exact attachment at '{location}'")
    validate_recovery_attachment(
        generation,
        binding,
        source,
        target_mapping,
        shortcut_path,
        shortcut_name,
        recovery_set=recovery_set,
        access_evidence=access_evidence,
    )
    diagnostics.extend(
        (
            "Only the destination shortcut definition was verified; data accessibility, failover routing, "
            "Delta consistency, table registration and SQL endpoint readiness are unknown.",
            "Read-only access is externally qualified by the supplied caller-mode evidence, not enforced or "
            "measured by this adapter. Shortcut creation has no read-only flag; "
            "delegated-owner engines are not qualified.",
            f"Retain source '{source.identity.key}/{binding.source_path}' and its read-only access policy. "
            "This temporary link is not independent recovery or permission to retire the source.",
            "No jobs, schema scripts, Spark sessions, SQL refreshes or permission grants were executed.",
        )
    )
    return AttachmentResult(binding, observed, changed, True, tuple(diagnostics))
