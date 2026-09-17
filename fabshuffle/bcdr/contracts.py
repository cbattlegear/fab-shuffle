"""Versioned BCDR records. Metadata completeness is not recovery readiness."""

from __future__ import annotations

import hashlib
import json
import re
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    model_validator,
)


def canonical_id(value: str) -> str:
    return str(UUID(value))


def logical_path(value: str) -> str:
    """A captured part is a logical relative name, never an extraction path."""
    if (
        not value or len(value.encode("utf-8")) > 2048
        or "\\" in value or ":" in value or "%" in value
        or any(ord(char) < 32 for char in value)
        or value.startswith("/") or any(part in {"", ".", ".."} for part in value.split("/"))
        or str(PurePosixPath(value)) != value
    ):
        raise ValueError("Use a relative logical part path without traversal, escapes or drive prefixes")
    return value


Guid = Annotated[str, AfterValidator(canonical_id)]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
LogicalPath = Annotated[str, AfterValidator(logical_path)]
Nonempty = Annotated[str, Field(min_length=1, max_length=2048)]


class Record(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)
    schema_version: Literal[1] = 1


class WorkspaceIdentity(Record):
    tenant_id: Guid
    workspace_id: Guid

    @property
    def key(self) -> str:
        return f"{self.tenant_id}/{self.workspace_id}"


class ItemIdentity(WorkspaceIdentity):
    item_id: Guid

    @property
    def key(self) -> str:
        return f"{super().key}/{self.item_id}"


class ConnectionIdentity(Record):
    tenant_id: Guid
    connection_id: Guid

    @property
    def key(self) -> str:
        return f"{self.tenant_id}/connections/{self.connection_id}"


class EndpointIdentity(Record):
    item: ItemIdentity
    endpoint_kind: Nonempty
    endpoint_id: Nonempty

    @property
    def key(self) -> str:
        return f"{self.item.key}/endpoints/{self.endpoint_kind}/{self.endpoint_id}"


class RecoveryMode(StrEnum):
    STANDBY = "standby"
    SYNCING = "syncing"
    PARKING = "parking"
    ENABLING_RECOVERY = "enabling_recovery"
    ACTIVE_RECOVERY = "active_recovery"
    FAILING_BACK = "failing_back"
    REARMING = "rearming"


class RecoveryStrategy(StrEnum):
    TEMPORARY_CONTINUITY = "temporary_continuity"
    INDEPENDENT = "independent"


class Qualification(StrEnum):
    DOCUMENTED = "documented"
    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    MANUAL = "manual"
    NEEDS_PROVIDER = "needs_provider"


class ProtectionKind(StrEnum):
    NATIVE_ONELAKE = "native_onelake"
    PORTABLE_EXPORT = "portable_export"
    PREPARED_STANDBY = "prepared_standby"
    EXTERNAL_SOURCE = "external_source"
    UNPROTECTED = "unprotected"


class RecoveryOutcome(StrEnum):
    PROTECTED = "protected"
    PROTECTION_MISSING = "protection_missing"
    RESTORE_FAILED = "restore_failed"
    RESTORED_STOPPED = "restored_stopped"
    TEMPORARY_ATTACHED = "temporary_attached"
    READY_FOR_CUTOVER = "ready_for_cutover"
    BLOCKED = "blocked"
    PARTIAL = "partial"
    MANUAL = "manual"


class AclScope(StrEnum):
    WORKSPACE = "workspace"
    ITEM = "item"
    SQL = "sql"
    ONELAKE = "onelake"
    MODEL = "model"
    CONNECTION = "connection"


class Principal(Record):
    tenant_id: Guid
    object_id: Guid
    kind: Literal["ServicePrincipal", "User", "Group"]

    @property
    def key(self) -> str:
        return f"{self.tenant_id}/{self.object_id}"


class DesiredAcl(Record):
    acl_id: Guid
    scope: AclScope
    workspace: WorkspaceIdentity | None = None
    item: ItemIdentity | None = None
    connection: ConnectionIdentity | None = None
    principal: Principal
    permission: Nonempty
    securable: str | None = None
    provenance: Nonempty

    @model_validator(mode="after")
    def target_matches_scope(self) -> Self:
        if self.scope == AclScope.CONNECTION:
            valid = self.connection is not None and self.item is None and self.workspace is None
        elif self.scope == AclScope.WORKSPACE:
            valid = self.workspace is not None and self.item is None and self.connection is None
        else:
            valid = self.item is not None and self.workspace is None and self.connection is None
        if not valid:
            raise ValueError("ACL scope must identify exactly one matching target")
        target = self.connection or self.item or self.workspace
        if target is not None and target.tenant_id != self.principal.tenant_id:
            raise ValueError("BCDR ACL principals and targets must be in the same tenant")
        return self


class WorkspaceGrant(Record):
    principal: Principal
    role: Literal["Admin", "Member", "Contributor", "Viewer"]


class StandbyAccessPolicy(Record):
    recovery_spn: Principal
    owners: tuple[WorkspaceGrant, ...] = ()

    @model_validator(mode="after")
    def validate_owners(self) -> Self:
        if self.recovery_spn.kind != "ServicePrincipal":
            raise ValueError("The recovery controller must be a service principal")
        keys = [owner.principal.key for owner in self.owners]
        if len(keys) != len(set(keys)) or self.recovery_spn.key in keys:
            raise ValueError("Designate each owner once, separately from the recovery SPN")
        if any(owner.principal.tenant_id != self.recovery_spn.tenant_id for owner in self.owners):
            raise ValueError("BCDR owners must be in the recovery tenant")
        return self

    def workspace_grants(self) -> tuple[WorkspaceGrant, ...]:
        return (WorkspaceGrant(principal=self.recovery_spn, role="Admin"), *self.owners)

    def permits(self, acl: DesiredAcl, *, mode: RecoveryMode, control_workspace: WorkspaceIdentity) -> bool:
        workspace = acl.workspace or acl.item
        is_control = workspace is not None and (
            workspace.tenant_id, workspace.workspace_id
        ) == (control_workspace.tenant_id, control_workspace.workspace_id)
        if is_control or mode not in {RecoveryMode.ENABLING_RECOVERY, RecoveryMode.ACTIVE_RECOVERY}:
            return acl.scope == AclScope.WORKSPACE and any(
                acl.principal == grant.principal and acl.permission == grant.role
                for grant in self.workspace_grants()
            )
        return True


class RecoverySet(Record):
    recovery_set_id: Guid
    tenant_id: Guid
    control_workspace: WorkspaceIdentity
    control_warehouse: ItemIdentity
    access_policy: StandbyAccessPolicy
    source_capacity_ids: tuple[Guid, ...]
    target_capacity_ids: tuple[Guid, ...]

    @model_validator(mode="after")
    def validate_scope(self) -> Self:
        if (
            self.control_workspace.key != WorkspaceIdentity(
                tenant_id=self.control_warehouse.tenant_id,
                workspace_id=self.control_warehouse.workspace_id,
            ).key
            or self.tenant_id != self.control_workspace.tenant_id
            or self.tenant_id != self.access_policy.recovery_spn.tenant_id
        ):
            raise ValueError("The control Warehouse, workspace and recovery SPN must share the tenant")
        if not self.source_capacity_ids or not self.target_capacity_ids:
            raise ValueError("Specify explicit source and target capacity scopes")
        if set(self.source_capacity_ids) & set(self.target_capacity_ids):
            raise ValueError("Source and dedicated recovery capacities must not overlap")
        if any(len(ids) != len(set(ids)) for ids in (self.source_capacity_ids, self.target_capacity_ids)):
            raise ValueError("Capacity scopes must not contain duplicates")
        return self


class WorkspaceRecord(Record):
    identity: WorkspaceIdentity
    capacity_id: Guid
    display_name: Nonempty
    captured_at: AwareDatetime
    properties: dict[str, JsonValue] = Field(default_factory=dict)
    inventory_complete: bool
    unresolved: tuple[str, ...] = ()


class PayloadPurpose(StrEnum):
    DEFINITION = "definition"
    RUNTIME_FILE = "runtime_file"
    SQL_SCHEMA = "sql_schema"
    CONFIGURATION = "configuration"
    SECURITY = "security"


class PayloadDescriptor(Record):
    payload_id: Guid
    owner: ItemIdentity
    path: LogicalPath
    purpose: PayloadPurpose
    media_type: Nonempty
    encoding: Literal["utf-8", "binary"]
    byte_length: Annotated[int, Field(ge=0)]
    sha256: Digest
    chunk_count: Annotated[int, Field(ge=1)]


class ItemRecord(Record):
    identity: ItemIdentity
    item_type: Nonempty
    display_name: Nonempty
    captured_at: AwareDatetime
    api_version: Nonempty
    definition_format: str | None = None
    properties: dict[str, JsonValue] = Field(default_factory=dict)
    payload_ids: tuple[Guid, ...] = ()
    source_version: str | None = None
    tombstone: bool = False
    capture_complete: bool
    unresolved: tuple[str, ...] = ()
    activation: Literal["stopped", "running", "unknown"] = "unknown"


class DependencyEdge(Record):
    edge_id: Guid
    consumer: ItemIdentity
    prerequisite: ItemIdentity | ConnectionIdentity | EndpointIdentity | None = None
    external_reference: str | None = None
    phase: Literal["create", "bind", "data_ready", "activate"]
    provenance: Nonempty
    qualification: Qualification = Qualification.UNVERIFIED
    required: bool = True
    detail: Nonempty

    @model_validator(mode="after")
    def one_prerequisite(self) -> Self:
        if (self.prerequisite is None) == (self.external_reference is None):
            raise ValueError("Identify either a qualified prerequisite or an explicit external dependency")
        return self


class ProtectionRecord(Record):
    protection_id: Guid
    item: ItemIdentity
    kind: ProtectionKind
    outcome: RecoveryOutcome
    qualification: Qualification = Qualification.UNVERIFIED
    artifact_reference: str | None = None
    recovery_point: AwareDatetime | None = None
    sha256: Digest | None = None
    provenance: Nonempty
    limitations: tuple[str, ...] = ()
    action: Nonempty

    @model_validator(mode="after")
    def missing_is_not_protected(self) -> Self:
        if self.kind == ProtectionKind.UNPROTECTED and self.outcome not in {
            RecoveryOutcome.PROTECTION_MISSING, RecoveryOutcome.BLOCKED, RecoveryOutcome.MANUAL,
        }:
            raise ValueError("Missing protection cannot be reported as recovered or protected")
        if (
            self.kind in {ProtectionKind.PORTABLE_EXPORT, ProtectionKind.PREPARED_STANDBY}
            and self.outcome == RecoveryOutcome.PROTECTED
            and (not self.artifact_reference or self.recovery_point is None)
        ):
            raise ValueError("Protected exports/standbys require an artifact and recovery point")
        return self


class RecoveryDataBinding(Record):
    generation_id: Guid
    source: ItemIdentity
    source_path: LogicalPath
    consumer: ItemIdentity
    strategy: RecoveryStrategy
    qualification: Qualification
    read_only_verified: bool
    retention_acknowledged: bool
    evidence: Nonempty

    @model_validator(mode="after")
    def temporary_boundary(self) -> Self:
        if self.strategy != RecoveryStrategy.TEMPORARY_CONTINUITY:
            raise ValueError("Retained-source bindings are only for temporary continuity")
        if (
            self.qualification != Qualification.VERIFIED
            or not self.read_only_verified or not self.retention_acknowledged
        ):
            raise ValueError("Qualify read-only access and acknowledge retained-source obligations first")
        if self.source.tenant_id != self.consumer.tenant_id or self.source == self.consumer:
            raise ValueError("A temporary binding needs distinct same-tenant source and recovery items")
        return self


class CaptureSnapshot(Record):
    recovery_set_id: Guid
    generation_id: Guid
    captured_at: AwareDatetime
    parent_generation_id: Guid | None = None
    capture_kind: Literal["source", "recovery"] = "source"
    workspaces: tuple[WorkspaceRecord, ...] = ()
    items: tuple[ItemRecord, ...] = ()
    dependencies: tuple[DependencyEdge, ...] = ()
    desired_acls: tuple[DesiredAcl, ...] = ()
    protections: tuple[ProtectionRecord, ...] = ()
    payloads: tuple[PayloadDescriptor, ...] = ()
    inventory_complete: bool
    unresolved: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_references(self) -> Self:
        collections = (
            [row.identity.key for row in self.workspaces],
            [row.identity.key for row in self.items],
            [row.edge_id for row in self.dependencies],
            [row.acl_id for row in self.desired_acls],
            [row.protection_id for row in self.protections],
            [row.payload_id for row in self.payloads],
        )
        if any(len(keys) != len(set(keys)) for keys in collections):
            raise ValueError("Duplicate identities in capture generation")
        workspaces = {(row.identity.tenant_id, row.identity.workspace_id) for row in self.workspaces}
        items = {row.identity.key: row for row in self.items}
        payloads = {row.payload_id: row for row in self.payloads}
        if any((row.identity.tenant_id, row.identity.workspace_id) not in workspaces for row in self.items):
            raise ValueError("Every item must belong to a captured workspace")
        for row in self.items:
            if len(row.payload_ids) != len(set(row.payload_ids)) or any(
                part not in payloads or payloads[part].owner != row.identity for part in row.payload_ids
            ):
                raise ValueError("Item payload references must be unique and owned by that item")
        if any(
            part.owner.key not in items or part.payload_id not in items[part.owner.key].payload_ids
            for part in self.payloads
        ):
            raise ValueError("Every payload must be referenced by its captured owner")
        paths = [(part.owner.key, part.path) for part in self.payloads]
        if len(paths) != len(set(paths)):
            raise ValueError("Duplicate logical part paths for the same item")
        if any(edge.consumer.key not in items for edge in self.dependencies):
            raise ValueError("Dependency consumers must be captured items")
        if any(row.item.key not in items for row in self.protections):
            raise ValueError("Protection assessments must name captured items")
        return self

    def require_publishable(self, recovery_set: RecoverySet) -> None:
        if self.recovery_set_id != recovery_set.recovery_set_id:
            raise ValueError("Capture belongs to a different recovery set")
        if not self.inventory_complete or self.unresolved:
            raise ValueError("Complete the source inventory and resolve capture gaps before publication")
        capacity_scope = (
            recovery_set.source_capacity_ids
            if self.capture_kind == "source" else recovery_set.target_capacity_ids
        )
        for workspace in self.workspaces:
            if (
                workspace.identity.tenant_id != recovery_set.tenant_id
                or workspace.capacity_id not in capacity_scope
                or workspace.identity == recovery_set.control_workspace
            ):
                raise ValueError("Capture includes an out-of-scope or control workspace")
            if not workspace.inventory_complete or workspace.unresolved:
                raise ValueError(f"Complete the inventory for workspace '{workspace.display_name}'")
        for item in self.items:
            if not item.capture_complete or item.unresolved:
                raise ValueError(f"Complete metadata for '{item.display_name}' before publication")
        for edge in self.dependencies:
            target = edge.prerequisite
            if isinstance(target, EndpointIdentity):
                target = target.item
            if target is not None and target.tenant_id != recovery_set.tenant_id:
                raise ValueError("Qualified dependency targets must belong to the recovery tenant")
        workspace_keys = {workspace.identity.key for workspace in self.workspaces}
        item_keys = {item.identity.key for item in self.items}
        for acl in self.desired_acls:
            target = acl.item or acl.workspace or acl.connection
            if target is None or target.tenant_id != recovery_set.tenant_id:
                raise ValueError("Desired ACL targets must belong to the recovery tenant")
            if acl.principal.tenant_id != recovery_set.tenant_id:
                raise ValueError("Desired ACL principals must belong to the recovery tenant")
            if acl.item is not None and acl.item.key not in item_keys:
                raise ValueError("Desired item/data ACLs must name captured business items")
            if acl.workspace is not None and acl.workspace.key not in workspace_keys:
                raise ValueError("Desired workspace ACLs must name captured business workspaces")


class AppliedItem(Record):
    source: ItemIdentity
    target: ItemIdentity
    capture_generation_id: Guid
    applied_at: AwareDatetime
    definition_sha256: Digest
    properties_sha256: Digest
    target_observed_sha256: Digest
    outcome: RecoveryOutcome
    operation_id: Guid

    @model_validator(mode="after")
    def independent_target(self) -> Self:
        if (
            self.source.workspace_id == self.target.workspace_id
            or self.source.tenant_id != self.target.tenant_id
        ):
            raise ValueError("Applied state must identify a distinct same-tenant recovery workspace")
        return self


class OperationState(StrEnum):
    INTENT = "intent"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


class OperationRecord(Record):
    operation_id: Guid
    kind: Nonempty
    state: OperationState
    recorded_at: AwareDatetime
    generation_id: Guid | None = None
    source: ItemIdentity | None = None
    target: ItemIdentity | None = None
    capacity_id: Guid | None = None
    service_operation_id: str | None = None
    request_id: str | None = None
    error_code: str | None = None
    message: str | None = None
    ownership_evidence: Nonempty


class ControllerLease(Record):
    """Fenced explicit ownership; no timer can silently steal a live controller."""

    recovery_set_id: Guid
    controller_id: Guid
    epoch: Annotated[int, Field(ge=1)]


class CatalogState(Record):
    recovery_set_id: Guid
    controller_id: Guid | None = None
    epoch: Annotated[int, Field(ge=0)]
    revision: Annotated[int, Field(ge=0)]
    mode: RecoveryMode
    current_generation_id: Guid | None = None


class CatalogDocument(Record):
    namespace: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")]
    key: Nonempty
    revision: Annotated[int, Field(ge=1)]
    document: dict[str, JsonValue]


class GenerationInfo(Record):
    generation_id: Guid
    status: Literal["staging", "sealed", "complete"]
    manifest_sha256: Digest
    payload_count: Annotated[int, Field(ge=1)]


def canonical_json(value: BaseModel | dict[str, JsonValue]) -> bytes:
    document = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    return json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


_SECRET_KEY = re.compile(
    r'(?i)["\']?(?:client[_-]?secret|password|access[_-]?token|refresh[_-]?token|'
    r'accountkey|sharedaccesssignature)["\']?\s*[:=]\s*["\']?([^"\'\s;,}]+)'
)
_SECRET_VALUE = re.compile(
    r"(?i)-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|"
    r"\bBearer\s+[A-Za-z0-9._~-]{12,}|[?&]sig=[^&\s\"']+|"
    r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
)


def reject_embedded_secrets(data: bytes) -> None:
    """Reject recognizable credentials, never silently redact a restorable definition.

    This is a defensive detector, not proof that arbitrary code/binary files are secret-free.
    Capture adapters must also remove credential-bearing fields before constructing records.
    """
    text = data.decode("utf-8", errors="replace")
    if _SECRET_VALUE.search(text) or _SECRET_KEY.search(text):
        raise ValueError("Captured content contains a credential; remove it and supply it externally")
