"""Synchronous BCDR application boundary shared by web workers and the CLI."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pyodbc
from pydantic import AwareDatetime, Field, JsonValue, StrictBool, model_validator

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.bootstrap import BootstrapError
from fabshuffle.bcdr.capacity import CapacityError
from fabshuffle.bcdr.catalog import CatalogError
from fabshuffle.bcdr.contracts import (
    ConnectionIdentity,
    Digest,
    Guid,
    ItemIdentity,
    Nonempty,
    Principal,
    Record,
    RecoveryMode,
    StandbyAccessPolicy,
)
from fabshuffle.bcdr.protection import ProtectionError
from fabshuffle.lifecycle import safe_text

if TYPE_CHECKING:
    from fabshuffle.bcdr.coordinator import RecoveryCoordinator
    from fabshuffle.bcdr.protection_binding import ConfigureProtectionRequest


class CapacityRoute(Record):
    source_capacity_id: Guid
    target_capacity_id: Guid


class ConnectionRoute(Record):
    source: ConnectionIdentity
    target: ConnectionIdentity
    evidence: Nonempty


class SetupCapacity(Record):
    arm_resource_id: Nonempty
    fabric_capacity_id: Guid
    dedicated_recovery: StrictBool
    authorized_for_suspend: StrictBool


class SetupRequest(Record):
    control_workspace_id: Guid | None = None
    control_workspace_name: Nonempty | None = None
    warehouse_name: Nonempty = "Fab Shuffle recovery catalog"
    source_capacity_ids: tuple[Guid, ...]
    recovery_capacities: tuple[SetupCapacity, ...]
    catalog_capacity_id: Nonempty
    access_policy: StandbyAccessPolicy

    @model_validator(mode="after")
    def one_control_workspace(self) -> SetupRequest:
        if (self.control_workspace_id is None) == (self.control_workspace_name is None):
            raise ValueError("Choose an existing control workspace ID or a name for a new one, not both")
        if not self.source_capacity_ids or len(self.source_capacity_ids) != len(
            set(self.source_capacity_ids)
        ):
            raise ValueError("Specify distinct source capacities before provisioning")
        if set(self.source_capacity_ids) & {row.fabric_capacity_id for row in self.recovery_capacities}:
            raise ValueError("Source and dedicated recovery capacities must not overlap")
        return self


class PlanRequest(Record):
    generation_id: Guid | None = None
    include_workspace_ids: tuple[Guid, ...] = ()
    exclude_workspace_ids: tuple[Guid, ...] = ()
    keywords: tuple[Nonempty, ...] = ()
    approved_addition_ids: tuple[Guid, ...] = ()
    capacity_routes: tuple[CapacityRoute, ...]
    suffix: Nonempty = " - recovery"
    connection_mappings: tuple[ConnectionRoute, ...] = ()
    target_quiescence_evidence: Nonempty | None = None


class SyncRequest(PlanRequest):
    capture: StrictBool = True
    park: StrictBool = True


class EnableRecoveryRequest(Record):
    generation_id: Guid
    group_ids: tuple[Nonempty, ...]
    approved_acl_ids: tuple[Guid, ...] = ()
    readiness: tuple[ItemReadiness, ...] = ()


class ItemReadiness(Record):
    """Time-bounded, identity-bound operator/runtime evidence, not a controller lock."""

    source: ItemIdentity
    target: ItemIdentity
    target_observed_sha256: Digest
    data_verified: StrictBool
    references_verified: StrictBool
    security_verified: StrictBool
    effective_principals: tuple[Principal, ...] = ()
    evidence: Nonempty
    observed_at: AwareDatetime
    valid_until: AwareDatetime

    @model_validator(mode="after")
    def evidence_window(self) -> ItemReadiness:
        if self.valid_until <= self.observed_at:
            raise ValueError("Readiness evidence must have a positive validity window")
        return self


class WriterFence(Record):
    expected_epoch: int = Field(ge=0)
    fenced_side: Literal["primary", "recovery"]
    writers_stopped: StrictBool
    conflicting_writes: StrictBool = False
    evidence: Nonempty
    confirmed_by: Nonempty
    observed_at: AwareDatetime
    valid_until: AwareDatetime

    def require_current(self, now: datetime, side: str, epoch: int) -> None:
        if (
            self.expected_epoch != epoch
            or self.fenced_side != side
            or not self.writers_stopped
            or self.conflicting_writes
            or not self.observed_at <= now < self.valid_until
        ):
            raise ValueError(
                "Fence every external writer on the named side, resolve conflicting writes, "
                "and supply current evidence for the displayed writer epoch"
            )


class CutoverRequest(Record):
    generation_id: Guid
    group_ids: tuple[Nonempty, ...]
    readiness: tuple[ItemReadiness, ...]
    writer_fence: WriterFence


class FailbackRequest(Record):
    generation_id: Guid
    group_ids: tuple[Nonempty, ...]
    primary_available: StrictBool
    primary_evidence: Nonempty


class ReconciliationEvidence(Record):
    source: ItemIdentity
    return_target: ItemIdentity
    capabilities: tuple[Literal["inserts", "updates", "deletes", "ttl", "schema", "offsets"], ...]
    evidence: Nonempty
    observed_at: AwareDatetime
    valid_until: AwareDatetime
    conflicts_resolved: StrictBool


class FailbackExecuteRequest(Record):
    plan_id: Guid
    writer_fence: WriterFence
    reconciliation: tuple[ReconciliationEvidence, ...] = ()


class CutbackRequest(Record):
    plan_id: Guid
    readiness: tuple[ItemReadiness, ...]
    writer_fence: WriterFence


class RearmRequest(Record):
    plan_id: Guid
    approve: StrictBool
    recovery_no_longer_serving: StrictBool
    evidence: Nonempty
    park: StrictBool = True


class ReconcileOperationRequest(Record):
    operation_id: Guid
    expected_controller_id: Guid
    expected_epoch: int = Field(ge=1)
    previous_controller_stopped: StrictBool
    fencing_evidence: Nonempty
    target_quiescence_evidence: Nonempty


class GroupStatus(Record):
    group_id: Nonempty
    items: tuple[ItemIdentity, ...]
    metadata_applied: bool = False
    data_ready: bool = False
    access_enabled: bool = False
    ready_for_cutover: bool = False
    active: bool = False
    blockers: tuple[str, ...] = ()


class ServiceResult(Record):
    mode: RecoveryMode
    outcome: Literal["succeeded", "partial", "blocked"] = "succeeded"
    exit_code: Literal[0, 2] = 0
    generation_id: Guid | None = None
    plan_id: Guid | None = None
    groups: tuple[GroupStatus, ...] = ()
    warnings: tuple[str, ...] = ()
    details: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def aggregate_outcome(self) -> ServiceResult:
        if any(group.blockers for group in self.groups):
            object.__setattr__(self, "outcome", "partial")
            object.__setattr__(self, "exit_code", 2)
        elif self.outcome != "succeeded":
            object.__setattr__(self, "exit_code", 2)
        return self


class BcdrService:
    """Policy is implemented once, outside HTTP request and CLI parsing."""

    def __init__(self, coordinator: RecoveryCoordinator) -> None:
        self.coordinator = coordinator

    @staticmethod
    def _call(action: Callable[[], ServiceResult]) -> ServiceResult:
        try:
            return action()
        except pyodbc.Error as error:
            raise CatalogError(safe_text(str(error))) from error
        except (BootstrapError, CapacityError, ProtectionError) as error:
            raise RecoveryBlocked(safe_text(str(error))) from error

    def status(self) -> ServiceResult:
        return self._call(self.coordinator.status)

    def plan(self, request: PlanRequest) -> ServiceResult:
        return self._call(lambda: self.coordinator.plan(request))

    def synchronize(self, request: SyncRequest) -> ServiceResult:
        return self._call(lambda: self.coordinator.synchronize(request))

    def configure_protection(self, request: ConfigureProtectionRequest) -> ServiceResult:
        return self._call(lambda: self.coordinator.configure_protection(request))

    def enable_recovery(self, request: EnableRecoveryRequest) -> ServiceResult:
        return self._call(lambda: self.coordinator.enable_recovery(request))

    def cutover(self, request: CutoverRequest) -> ServiceResult:
        return self._call(lambda: self.coordinator.cutover(request))

    def plan_failback(self, request: FailbackRequest) -> ServiceResult:
        return self._call(lambda: self.coordinator.failback.plan(request))

    def execute_failback(self, request: FailbackExecuteRequest) -> ServiceResult:
        return self._call(lambda: self.coordinator.failback.execute(request))

    def cutback(self, request: CutbackRequest) -> ServiceResult:
        return self._call(lambda: self.coordinator.failback.cutback(request))

    def rearm(self, request: RearmRequest) -> ServiceResult:
        return self._call(lambda: self.coordinator.failback.rearm(request))

    def reconcile_operation(self, request: ReconcileOperationRequest) -> ServiceResult:
        return self._call(lambda: self.coordinator.reconcile_operation(request))

    def close(self) -> None:
        self.coordinator.close()


def create_service(
    bootstrap_path: Path,
    *,
    target_tokens: TokenProvider,
    source_tokens: TokenProvider | None = None,
    controller_id: str | None = None,
) -> BcdrService:
    """Open a production Warehouse-backed service; no local metadata fallback."""
    from fabshuffle.bcdr.coordinator import RecoveryCoordinator

    try:
        return BcdrService(
            RecoveryCoordinator.from_bootstrap(
                bootstrap_path,
                target_tokens=target_tokens,
                source_tokens=source_tokens,
                controller_id=controller_id,
            )
        )
    except pyodbc.Error as error:
        raise CatalogError(safe_text(str(error))) from error
    except (BootstrapError, CapacityError, ProtectionError) as error:
        raise RecoveryBlocked(safe_text(str(error))) from error


def setup(
    request: SetupRequest,
    bootstrap_path: Path,
    *,
    target_tokens: TokenProvider,
) -> ServiceResult:
    """Provision a new owned Warehouse in an explicitly designated restricted workspace."""
    from fabshuffle.bcdr.production import setup_recovery

    return BcdrService._call(lambda: setup_recovery(request, bootstrap_path, target_tokens=target_tokens))
