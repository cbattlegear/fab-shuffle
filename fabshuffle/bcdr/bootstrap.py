"""Minimal deployment state that remains readable while the catalog is suspended.

This is not a metadata backup. Keep this file in the controller's protected,
durable recovery-region deployment volume. Credentials are supplied separately.
"""

from __future__ import annotations

import json
import os
import re
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated, Literal, Self
from uuid import UUID, uuid4

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

ARM_VERSION = "2023-11-01"
_ARM_ID = re.compile(
    r"/subscriptions/([0-9a-f-]{36})/resourcegroups/([a-z0-9_.()-]{1,90})"
    r"/providers/microsoft\.fabric/capacities/([a-z][a-z0-9]{2,62})",
    re.IGNORECASE,
)


def canonical_arm_id(value: str) -> str:
    match = _ARM_ID.fullmatch(value)
    if not match:
        raise ValueError("Supply an exact Microsoft.Fabric capacity ARM resource ID, not a URL")
    UUID(match[1])
    if match[2].endswith("."):
        raise ValueError("Resource group names cannot end in a period")
    return value.lower()


def _guid(value: str) -> str:
    return str(UUID(value))


def _host(value: str) -> str:
    value = value.lower()
    if not re.fullmatch(
        r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)*"
        r"\.datawarehouse\.fabric\.microsoft\.com", value,
    ):
        raise ValueError("Supply the public Fabric Warehouse TDS hostname, not a connection string")
    return value


Guid = Annotated[str, AfterValidator(_guid)]
ArmId = Annotated[str, AfterValidator(canonical_arm_id)]
TdsHost = Annotated[str, AfterValidator(_host)]


class BootstrapError(RuntimeError):
    """Invalid, conflicting or insufficient durable bootstrap evidence."""


class BootstrapRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


class CapacityAuthorization(BootstrapRecord):
    arm_resource_id: ArmId
    fabric_capacity_id: Guid
    dedicated_recovery: Literal[True]
    authorized_for_suspend: Literal[True]


class WorkspacePrincipal(BootstrapRecord):
    """An explicit designated owner, not a captured source-workspace ACL."""

    object_id: Guid
    principal_type: Literal["User", "Group", "ServicePrincipal"]
    role: Literal["Admin"] = "Admin"


class WorkspaceRoleIntent(BootstrapRecord):
    intent_id: Guid
    principal: WorkspacePrincipal
    assignment_id: Guid | None = None


class WorkspaceIntent(BootstrapRecord):
    owner_id: Guid
    intent_id: Guid
    display_name: Annotated[str, Field(min_length=1, max_length=256, pattern=r"^[^;=\r\n\x00]+$")]
    owner_scope_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    origin: Literal["created", "designated"] = "created"
    phase: Literal["intent", "created", "ready"] = "intent"
    workspace_id: Guid | None = None
    pending_role: WorkspaceRoleIntent | None = None

    @model_validator(mode="after")
    def evidence(self) -> Self:
        if self.phase == "intent" and self.workspace_id is not None:
            raise ValueError("Unacknowledged workspace creation cannot claim a resource ID")
        if self.origin == "designated" and self.phase != "ready":
            raise ValueError("Explicitly designated workspaces require a completed read-only access audit")
        if self.phase in {"created", "ready"} and self.workspace_id is None:
            raise ValueError("A created control workspace requires its returned resource ID")
        if self.phase == "ready" and self.pending_role is not None:
            raise ValueError("Resolve pending owner assignments before marking the workspace ready")
        return self


class WarehouseIntent(BootstrapRecord):
    owner_id: Guid
    intent_id: Guid
    display_name: Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[^;=\r\n\x00]+$")]
    phase: Literal["intent", "accepted", "created", "ready", "failed"] = "intent"
    operation_id: Guid | None = None
    warehouse_id: Guid | None = None
    request_id: Guid | None = None

    @model_validator(mode="after")
    def evidence(self) -> Self:
        if self.phase == "accepted" and self.operation_id is None:
            raise ValueError("An accepted create requires its returned operation ID")
        if self.phase in {"created", "ready"} and self.warehouse_id is None:
            raise ValueError("A created Warehouse requires its returned resource ID")
        return self


class CapacityOperation(BootstrapRecord):
    intent_id: Guid
    owner_id: Guid
    arm_resource_id: ArmId
    action: Literal["resume", "suspend"]
    phase: Literal["intent", "accepted", "succeeded", "failed"] = "intent"
    poll_url: str | None = Field(default=None, max_length=16384)
    poll_kind: Literal["async", "location"] | None = None
    request_id: str | None = Field(default=None, max_length=256, pattern=r"^[a-zA-Z0-9:_.-]+$")

    @model_validator(mode="after")
    def polling(self) -> Self:
        if (self.poll_url is None) != (self.poll_kind is None):
            raise ValueError("Polling URL and kind must be recorded together")
        if self.phase == "accepted" and self.poll_url is None:
            raise ValueError("An accepted ARM operation requires its polling URL")
        if self.poll_url is not None:
            # Import locally: bootstrap models do not depend on catalog contracts.
            from fabshuffle.bcdr.capacity import validate_poll_url

            validate_poll_url(self.poll_url, self.arm_resource_id)
        return self


class ParkingIntent(BootstrapRecord):
    owner_id: Guid
    epoch: int = Field(ge=1)
    capacity_ids: tuple[ArmId, ...] = Field(min_length=1)


class BootstrapDescriptor(BootstrapRecord):
    schema_version: Literal[1] = 1
    revision: int = Field(default=0, ge=0)
    recovery_set_id: Guid
    tenant_id: Guid
    application_id: Guid
    controller_id: Guid
    capacities: tuple[CapacityAuthorization, ...] = Field(min_length=1)
    catalog_capacity_id: ArmId
    control_workspace_id: Guid | None = None
    workspace_intent: WorkspaceIntent | None = None
    control_warehouse_id: Guid | None = None
    tds_host: TdsHost | None = None
    tds_catalog: Guid | None = None
    warehouse_intent: WarehouseIntent | None = None
    capacity_operations: tuple[CapacityOperation, ...] = ()
    parking: ParkingIntent | None = None

    @model_validator(mode="after")
    def consistency(self) -> Self:
        arm_ids = [capacity.arm_resource_id for capacity in self.capacities]
        fabric_ids = [capacity.fabric_capacity_id for capacity in self.capacities]
        if len(set(arm_ids)) != len(arm_ids) or len(set(fabric_ids)) != len(fabric_ids):
            raise ValueError("Capacity ARM and Fabric IDs must each be unique")
        if self.catalog_capacity_id not in arm_ids:
            raise ValueError("The catalog capacity must be explicitly authorized")
        if self.control_warehouse_id and self.control_workspace_id is None:
            raise ValueError("The control Warehouse requires its workspace identity")
        if self.workspace_intent and (
            self.workspace_intent.owner_id != self.controller_id
            or self.workspace_intent.workspace_id != self.control_workspace_id
        ):
            raise ValueError("Control workspace ownership and resource identity disagree")
        if (self.tds_host is None) != (self.tds_catalog is None):
            raise ValueError("TDS host and explicit catalog must be recorded together")
        if self.tds_catalog is not None and self.tds_catalog != self.control_warehouse_id:
            raise ValueError("Use the recorded Warehouse ID as the explicit TDS catalog")
        if self.warehouse_intent:
            if self.warehouse_intent.owner_id != self.controller_id:
                raise ValueError("Warehouse creation belongs to another controller")
            if self.warehouse_intent.warehouse_id != self.control_warehouse_id:
                raise ValueError("Warehouse ownership and control resource IDs disagree")
            if self.warehouse_intent.phase == "ready" and self.tds_host is None:
                raise ValueError("Ready Warehouse requires TDS coordinates")
        operation_ids = [operation.intent_id for operation in self.capacity_operations]
        if len(set(operation_ids)) != len(operation_ids):
            raise ValueError("Duplicate capacity operation intent IDs")
        for operation in self.capacity_operations:
            if operation.arm_resource_id not in arm_ids or operation.owner_id != self.controller_id:
                raise ValueError("Capacity operation is outside this controller's authorized scope")
        if self.parking:
            if self.parking.owner_id != self.controller_id:
                raise ValueError("Parking belongs to another controller")
            if len(set(self.parking.capacity_ids)) != len(self.parking.capacity_ids):
                raise ValueError("Parking capacity IDs must be unique")
            if set(self.parking.capacity_ids) != set(arm_ids):
                raise ValueError("Parking must cover precisely the authorized recovery capacities")
            if self.parking.capacity_ids[-1] != self.catalog_capacity_id:
                raise ValueError("The catalog capacity must be parked last")
        return self

    def capacity(self, arm_resource_id: str) -> CapacityAuthorization:
        arm_resource_id = canonical_arm_id(arm_resource_id)
        for capacity in self.capacities:
            if capacity.arm_resource_id == arm_resource_id:
                return capacity
        raise BootstrapError("Capacity is not explicitly authorized in this bootstrap descriptor")


class BootstrapStore:
    """Atomic revision checks under a live deployment guard (or local-only flock)."""

    MAX_BYTES = 128 * 1024

    def __init__(
        self, path: str | Path, *, guard: Callable[[], None] | None = None,
        distributed: bool = False,
    ) -> None:
        self.path = Path(path)
        self.guard = guard
        self.distributed = distributed
        self._mutex = threading.RLock()

    def _fence(self) -> None:
        if self.guard is not None:
            self.guard()

    def load(self) -> BootstrapDescriptor:
        self._fence()
        try:
            with self.path.open("rb") as stream:
                data = stream.read(self.MAX_BYTES + 1)
            if len(data) > self.MAX_BYTES:
                raise BootstrapError("Bootstrap descriptor exceeds the supported size")
            descriptor = BootstrapDescriptor.model_validate_json(data)
            self._fence()
            return descriptor
        except (ValueError, OSError) as error:
            raise BootstrapError(
                "Cannot read a valid bootstrap descriptor; repair deployment state"
            ) from error

    @contextmanager
    def _lock(self) -> Iterator[None]:
        import fcntl

        self._fence()
        if self.distributed:
            if self.guard is None:
                raise BootstrapError("Distributed bootstrap access requires a live deployment guard")
            with self._mutex:
                self._fence()
                self.path.parent.mkdir(parents=True, exist_ok=True)
                yield
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path.with_suffix(self.path.suffix + ".lock"), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)

    def save(
        self, descriptor: BootstrapDescriptor, *, expected_revision: int | None,
    ) -> BootstrapDescriptor:
        with self._lock():
            current = self.load() if self.path.exists() else None
            if (current.revision if current else None) != expected_revision:
                raise BootstrapError("Bootstrap revision changed; reload before issuing any mutation")
            return self._write(descriptor, 0 if current is None else current.revision + 1)

    def update(self, change: Callable[[BootstrapDescriptor], BootstrapDescriptor]) -> BootstrapDescriptor:
        with self._lock():
            current = self.load()
            return self._write(change(current), current.revision + 1)

    def _write(self, descriptor: BootstrapDescriptor, revision: int) -> BootstrapDescriptor:
        self._fence()
        # Revalidate model_copy/model_construct callers as well as ordinary constructors.
        document = descriptor.model_dump(mode="json")
        document["revision"] = revision
        checked = BootstrapDescriptor.model_validate(document)
        payload = json.dumps(checked.model_dump(mode="json"), ensure_ascii=False).encode("utf-8")
        if len(payload) > self.MAX_BYTES:
            raise BootstrapError("Bootstrap descriptor exceeds the supported size; reconcile old operations")
        staging = self.path.with_name(f".{self.path.name}.{uuid4().hex}.writing")
        try:
            fd = os.open(staging, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            self._fence()
            os.replace(staging, self.path)
            directory_fd = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            staging.unlink(missing_ok=True)
        return checked
