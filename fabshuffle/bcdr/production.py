"""Production bindings for the synchronous service, with no local metadata fallback."""

from __future__ import annotations

import os
from contextlib import ExitStack
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid4, uuid5

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.backend import DurableRuntime, RecoveryBlocked, now
from fabshuffle.bcdr.bootstrap import (
    BootstrapDescriptor,
    BootstrapStore,
    CapacityAuthorization,
    WorkspacePrincipal,
)
from fabshuffle.bcdr.capacity import ArmCapacityClient, CapacityCoordinator, PauseProof
from fabshuffle.bcdr.capture import capture_list, item_document
from fabshuffle.bcdr.contracts import (
    ControllerLease,
    ItemIdentity,
    OperationRecord,
    OperationState,
    RecoveryMode,
    RecoverySet,
    WorkspaceIdentity,
)
from fabshuffle.bcdr.control_access import control_workspace_warnings
from fabshuffle.bcdr.deployment_lock import DeploymentLock, GuardedTokens
from fabshuffle.bcdr.provisioning import ControlWarehouseProvisioner, ControlWorkspaceProvisioner
from fabshuffle.bcdr.warehouse_catalog import WarehouseCatalog
from fabshuffle.config import SETTINGS
from fabshuffle.fabric.client import FabricClient
from fabshuffle.fabric.workspaces import list_role_assignments

if TYPE_CHECKING:
    from collections.abc import Sequence

    from fabshuffle.bcdr.coordinator import RecoveryCoordinator
    from fabshuffle.bcdr.service import ServiceResult, SetupRequest


def _record_capacity(store, operation):
    return store.update(
        lambda descriptor: descriptor.model_copy(
            update={
                "capacity_operations": (
                    *(row for row in descriptor.capacity_operations if row.intent_id != operation.intent_id),
                    operation,
                ),
            }
        )
    )


class ProductionCapacities:
    def __init__(self, store: BootstrapStore, tokens: TokenProvider, client: FabricClient) -> None:
        self.store = store
        self.tokens = tokens
        self.client = client
        self.arm = ArmCapacityClient(tokens, guard=store.guard)
        self.driver = CapacityCoordinator(store, self.arm)
        self.catalog: WarehouseCatalog | None = None
        self.lock: DeploymentLock | None = None

    def _open_catalog(self, descriptor: BootstrapDescriptor) -> None:
        if not descriptor.tds_host or not descriptor.tds_catalog:
            raise RecoveryBlocked("Complete control Warehouse setup before opening a recovery service")
        self.catalog = WarehouseCatalog.open_from_endpoint(
            descriptor.tds_host,
            descriptor.tds_catalog,
            self.tokens,
            expected_recovery_set_id=descriptor.recovery_set_id,
            guard=self.store.guard,
        )
        self.catalog.state()

    def _reconcile(self, descriptor: BootstrapDescriptor) -> None:
        if self.catalog is None:
            raise RecoveryBlocked("The control Warehouse did not become readable after capacity resume")
        state = self.catalog.state()
        if state.mode != RecoveryMode.PARKING:
            if descriptor.parking:
                raise RecoveryBlocked(
                    "Bootstrap parking receipt disagrees with catalog mode; inspect ownership"
                )
            return
        parking = descriptor.parking
        if (
            parking is None
            or parking.owner_id != state.controller_id
            or parking.epoch != state.epoch
            or parking.owner_id != descriptor.controller_id
        ):
            raise RecoveryBlocked("Parking ownership is incomplete; reconcile the exact controller epoch")
        lease = ControllerLease(
            recovery_set_id=descriptor.recovery_set_id,
            controller_id=parking.owner_id,
            epoch=parking.epoch,
        )
        for pending in self.catalog.pending_operations():
            capacity = next(
                (row for row in descriptor.capacities if row.fabric_capacity_id == pending.capacity_id),
                None,
            )
            if pending.kind != "suspend_capacity" or capacity is None:
                raise RecoveryBlocked("A business operation remains pending; do not resume synchronization")
            expected_id = str(uuid5(
                UUID(descriptor.recovery_set_id), f"park:{parking.epoch}:{capacity.arm_resource_id}",
            ))
            if pending.operation_id != expected_id:
                raise RecoveryBlocked(
                    "Pending suspension is not the exact recorded parking epoch; reconcile it explicitly"
                )
            evidence = [
                row
                for row in descriptor.capacity_operations
                if row.arm_resource_id == capacity.arm_resource_id
                and row.action == "suspend"
                and row.phase == "succeeded"
                and row.owner_id == parking.owner_id
            ]
            if not evidence:
                raise RecoveryBlocked(
                    f"No durable successful suspend receipt for {capacity.fabric_capacity_id}; reconcile ARM"
                )
            self.catalog.record_operation(
                lease,
                pending.model_copy(
                    update={
                        "state": OperationState.SUCCEEDED,
                        "recorded_at": now(),
                        "message": ("ARM suspension observed in durable bootstrap; catalog capacity resumed"),
                        "request_id": evidence[-1].request_id,
                    }
                ),
            )
        self.catalog.transition_mode(lease, RecoveryMode.PARKING, RecoveryMode.STANDBY, str(uuid4()))
        self.catalog.release_controller(lease)

    def resume_catalog(self) -> None:
        self.driver.resume_catalog(wait_sql=self._open_catalog, reconcile=self._reconcile)

    def resume_business(self, runtime: DurableRuntime) -> None:
        descriptor = self.store.load()
        for capacity in descriptor.capacities:
            if capacity.arm_resource_id == descriptor.catalog_capacity_id:
                continue
            runtime.fence()
            pending = [
                row
                for row in descriptor.capacity_operations
                if row.arm_resource_id == capacity.arm_resource_id and row.phase in {"intent", "accepted"}
            ]
            if pending:
                raise RecoveryBlocked(
                    "Reconcile outstanding recovery capacity work before a new business resume"
                )

            def resume(cap=capacity):
                self.arm.resume(
                    cap,
                    owner_id=descriptor.controller_id,
                    on_progress=lambda operation: _record_capacity(self.store, operation),
                )
                return {"capacity_id": cap.fabric_capacity_id, "state": "Active"}

            runtime.effect(
                "resume-capacity", f"{runtime.require_lease().epoch}/{capacity.fabric_capacity_id}", resume
            )

    def park(self, runtime: DurableRuntime, workspaces: Sequence[WorkspaceIdentity]) -> None:
        guard = CatalogPauseGuard(self, runtime, workspaces)
        self.driver.park(owner_id=runtime.controller_id, guard=guard)

    def close(self) -> None:
        self.arm.close()
        if self.lock is not None:
            self.lock.close()
            self.lock = None

    def observations(self) -> tuple[dict, ...]:
        result = []
        for capacity in self.store.load().capacities:
            state = self.arm.get(capacity)
            result.append(
                {
                    "capacity_id": capacity.fabric_capacity_id,
                    "arm_resource_id": capacity.arm_resource_id,
                    "state": state.state,
                    "provisioning_state": state.provisioning_state,
                    "observed_at": now().isoformat(),
                }
            )
        return tuple(result)


class CatalogPauseGuard:
    def __init__(self, capacities, runtime, workspaces):
        self.capacities = capacities
        self.runtime = runtime
        self.workspaces = workspaces

    def _scope(self) -> None:
        config = self.runtime.catalog.recovery_set
        descriptor = self.capacities.store.load()
        allowed = {row.workspace_id for row in self.workspaces} | {config.control_workspace.workspace_id}
        active_groups = self.runtime.catalog.list_records("groups")
        if any(row.document["group"]["active"] for row in active_groups):
            raise RecoveryBlocked("Recovery still serves an active group; do not pause its capacity")
        owned = {row.target.key for row in self.runtime.catalog.applied_items()}
        owned.add(config.control_warehouse.key)
        for capacity in descriptor.capacities:
            # The accessible-workspace list is insufficient to prove a capacity hosts no unrelated resources.
            # Learn admin/workspaces/list-workspaces documents this capacity filter and response key.
            workspaces = capture_list(
                self.capacities.client,
                "admin/workspaces",
                params={"capacityId": capacity.fabric_capacity_id, "state": "Active"},
                value_key="workspaces",
            )
            for workspace in workspaces:
                if (
                    workspace.get("id") not in allowed
                    or workspace.get("capacityId") != capacity.fabric_capacity_id
                ):
                    raise RecoveryBlocked(
                        f"Capacity {capacity.fabric_capacity_id} hosts unowned workspace "
                        f"{workspace.get('id')}; move it off the dedicated recovery capacity before parking"
                    )
                inventory = capture_list(self.capacities.client, f"workspaces/{workspace['id']}/items")
                derived = set()
                for parent in inventory:
                    identity = ItemIdentity(
                        tenant_id=config.tenant_id,
                        workspace_id=workspace["id"],
                        item_id=parent["id"],
                    )
                    if identity.key not in owned or parent.get("type") not in {
                        "Lakehouse",
                        "Warehouse",
                        "SQLDatabase",
                        "MirroredDatabase",
                    }:
                        continue
                    current = item_document(self.capacities.client, identity, parent["type"])
                    if current.get("workspaceId", workspace["id"]) != workspace["id"]:
                        raise RecoveryBlocked(
                            "Owned parent metadata returned a different workspace before parking"
                        )
                    endpoint = (current.get("properties") or {}).get("sqlEndpointProperties") or {}
                    if endpoint.get("id"):
                        derived.add(str(UUID(endpoint["id"])))
                for item in inventory:
                    identity = ItemIdentity(
                        tenant_id=config.tenant_id,
                        workspace_id=workspace["id"],
                        item_id=item["id"],
                    )
                    if identity.key not in owned and not (
                        item.get("type") in {"SQLEndpoint", "SqlAnalyticsEndpoint"}
                        and identity.item_id in derived
                    ):
                        raise RecoveryBlocked(
                            f"Workspace {workspace['id']} contains untracked item {item.get('id')}; "
                            "reconcile its ownership and activity before parking"
                        )

    def enter_parking(self, owner_id: str, capacity_ids: tuple[str, ...]) -> PauseProof:
        self.runtime.fence()
        self._scope()
        if self.runtime.mode != RecoveryMode.STANDBY or self.runtime.catalog.pending_operations():
            raise RecoveryBlocked("Commit and settle all work in standby before parking")
        self.runtime.transition(RecoveryMode.PARKING)
        lease = self.runtime.require_lease()
        descriptor = self.capacities.store.load()
        for resource in capacity_ids:
            capacity = descriptor.capacity(resource)
            intent = OperationRecord(
                operation_id=str(uuid5(UUID(descriptor.recovery_set_id), f"park:{lease.epoch}:{resource}")),
                kind="suspend_capacity",
                capacity_id=capacity.fabric_capacity_id,
                state=OperationState.INTENT,
                recorded_at=now(),
                ownership_evidence=f"single deployment {owner_id};parking epoch {lease.epoch}",
            )
            self.runtime.catalog.begin_parking_operation(lease, intent)
        return PauseProof(descriptor.recovery_set_id, owner_id, lease.epoch, "parking", True, capacity_ids)

    def assert_parking(self, proof: PauseProof) -> PauseProof:
        self.runtime.fence()
        self._scope()
        state = self.runtime.catalog.state()
        pending = self.runtime.catalog.pending_operations()
        if (
            state.mode != RecoveryMode.PARKING
            or state.controller_id != proof.owner_id
            or state.epoch != proof.epoch
            or any(row.kind != "suspend_capacity" for row in pending)
        ):
            raise RecoveryBlocked(
                "A serving mode, ownership change or unfinished job prevents capacity suspension"
            )
        return proof


def open_coordinator(
    bootstrap_path: Path,
    *,
    target_tokens: TokenProvider,
    source_tokens: TokenProvider | None,
    controller_id: str | None,
) -> RecoveryCoordinator:
    from fabshuffle.bcdr.coordinator import RecoveryCoordinator
    from fabshuffle.bcdr.protection_binding import build_data_recovery

    with ExitStack() as cleanup:
        lock = DeploymentLock(bootstrap_path, tokens=target_tokens)
        cleanup.callback(lock.close)
        target_tokens = GuardedTokens(target_tokens, lock.assert_held)
        source_tokens = GuardedTokens(source_tokens, lock.assert_held) if source_tokens else None
        store = BootstrapStore(
            bootstrap_path, guard=lock.assert_held, distributed=lock.remote is not None,
        )
        descriptor = store.load()
        if controller_id is not None and controller_id != descriptor.controller_id:
            raise RecoveryBlocked("Controller identity must match the durable bootstrap deployment")
        client = FabricClient(target_tokens)
        cleanup.callback(client.close)
        capacities = ProductionCapacities(store, target_tokens, client)
        cleanup.callback(capacities.close)
        capacities.resume_catalog()
        catalog = capacities.catalog
        if catalog is None:
            raise RecoveryBlocked("The catalog did not become available after resume")
        protected_root = os.environ.get("FAB_SHUFFLE_BCDR_PROTECTED_ROOT")
        source = FabricClient(source_tokens) if source_tokens else None
        if source is not None:
            cleanup.callback(source.close)
        coordinator = RecoveryCoordinator(
            catalog.recovery_set,
            catalog,
            client,
            capacities,
            source=source,
            tokens=target_tokens,
            source_tokens=source_tokens,
            controller_id=descriptor.controller_id,
            data_recovery=build_data_recovery(
                client=client,
                tokens=target_tokens,
                protected_root=Path(protected_root) if protected_root else None,
                scratch=SETTINGS.scratch_dir_for("bcdr"),
            ),
            protected_root=Path(protected_root) if protected_root else None,
        )
        coordinator.runtime.deployment_guard = lock.assert_held
        capacities.lock = lock
        cleanup.pop_all()
        return coordinator


def setup_recovery(
    request: SetupRequest,
    bootstrap_path: Path,
    *,
    target_tokens: TokenProvider,
) -> ServiceResult:
    from fabshuffle.bcdr.service import ServiceResult

    tenant = target_tokens.tenant_id()
    if (
        request.access_policy.recovery_spn.tenant_id != tenant
        or request.access_policy.recovery_spn.object_id != target_tokens.object_id()
    ):
        raise RecoveryBlocked("Designate the authenticated recovery tenant's service principal and owners")
    capacities = tuple(
        CapacityAuthorization.model_validate(row.model_dump(exclude={"schema_version"}))
        for row in request.recovery_capacities
    )
    lock = DeploymentLock(bootstrap_path, tokens=target_tokens)
    target_tokens = GuardedTokens(target_tokens, lock.assert_held)
    store = BootstrapStore(
        bootstrap_path, guard=lock.assert_held, distributed=lock.remote is not None,
    )
    try:
        if bootstrap_path.exists():
            descriptor = store.load()
            if (
                descriptor.tenant_id != tenant
                or descriptor.application_id != target_tokens.principal.client_id
                or (
                    request.control_workspace_id is not None
                    and descriptor.control_workspace_id != request.control_workspace_id
                )
                or descriptor.capacities != capacities
            ):
                raise RecoveryBlocked(
                    "Setup request differs from the existing owned bootstrap; do not overwrite it"
                )
        else:
            descriptor = BootstrapDescriptor(
                recovery_set_id=str(uuid4()),
                tenant_id=tenant,
                application_id=target_tokens.principal.client_id,
                controller_id=str(uuid4()),
                capacities=capacities,
                catalog_capacity_id=request.catalog_capacity_id,
                control_workspace_id=request.control_workspace_id,
            )
            store.save(descriptor, expected_revision=None)
        with ArmCapacityClient(target_tokens, guard=lock.assert_held) as arm:
            descriptor = CapacityCoordinator(store, arm).resume_catalog_capacity()
        if request.control_workspace_name is not None:
            if any(grant.role != "Admin" for grant in request.access_policy.owners):
                raise RecoveryBlocked(
                    "New control workspace provisioning supports designated owner Admin roles; "
                    "use an existing restricted workspace for a different explicit owner role"
                )
            with ControlWorkspaceProvisioner(store, target_tokens) as provisioner:
                descriptor = provisioner.ensure(
                    display_name=request.control_workspace_name,
                    owner_id=descriptor.controller_id,
                    allowed_principals=tuple(
                        WorkspacePrincipal(
                            object_id=grant.principal.object_id,
                            principal_type=grant.principal.kind,
                            role=grant.role,
                        )
                        for grant in request.access_policy.owners
                    ),
                )
        with FabricClient(target_tokens) as client:
            assignments = list_role_assignments(client, descriptor.control_workspace_id)
            access_warnings = control_workspace_warnings(
                assignments, request.access_policy.recovery_spn, request.access_policy.owners,
            )
        with ControlWarehouseProvisioner(store, target_tokens) as provisioner:
            descriptor = provisioner.ensure(
                display_name=request.warehouse_name, owner_id=descriptor.controller_id
            )
        config = RecoverySet(
            recovery_set_id=descriptor.recovery_set_id,
            tenant_id=tenant,
            control_workspace=WorkspaceIdentity(
                tenant_id=tenant, workspace_id=descriptor.control_workspace_id
            ),
            control_warehouse=ItemIdentity(
                tenant_id=tenant,
                workspace_id=descriptor.control_workspace_id,
                item_id=descriptor.control_warehouse_id,
            ),
            access_policy=request.access_policy,
            source_capacity_ids=request.source_capacity_ids,
            target_capacity_ids=tuple(row.fabric_capacity_id for row in capacities),
        )
        catalog = WarehouseCatalog.from_endpoint(
            descriptor.tds_host, descriptor.tds_catalog, target_tokens, config,
            guard=lock.assert_held,
        )
        catalog.initialize()
        return ServiceResult(
            mode=catalog.state().mode,
            warnings=access_warnings,
            details={
                "recovery_set_id": descriptor.recovery_set_id,
                "control_workspace_id": descriptor.control_workspace_id,
                "control_warehouse_id": descriptor.control_warehouse_id,
            },
        )
    finally:
        lock.close()
