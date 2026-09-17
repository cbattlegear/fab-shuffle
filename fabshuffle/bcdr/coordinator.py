"""Warehouse-authoritative, source-independent recovery lifecycle.

Synchronization captures first, applies the separate operation DAG, commits observations,
then parks only through the dedicated-capacity guard. Enabling permanently fences scheduled
source writes until an explicit, successful cutback and rearm.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from pydantic import JsonValue, TypeAdapter

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.access import AccessController, FabricAccess, remap_acl
from fabshuffle.bcdr.adapters import (
    adapter_capabilities,
    apply_captured_item,
    captured_hashes,
    observe_target,
)
from fabshuffle.bcdr.backend import (
    DurableRuntime,
    FencedFabricClient,
    RecoveryBlocked,
    now,
)
from fabshuffle.bcdr.capture import capture_workspaces, item_document
from fabshuffle.bcdr.catalog import CapturedGeneration, RecoveryCatalog
from fabshuffle.bcdr.contracts import (
    AppliedItem,
    ConnectionIdentity,
    ControllerLease,
    EndpointIdentity,
    ItemIdentity,
    ItemRecord,
    OperationState,
    RecoveryMode,
    RecoveryOutcome,
    RecoverySet,
    WorkspaceIdentity,
    canonical_json,
    digest,
)
from fabshuffle.bcdr.planner import (
    AdapterCapabilities,
    CapacityMapping,
    Dependency,
    EstateInventory,
    Evidence,
    EvidenceState,
    Item,
    OperationPlan,
    Phase,
    Replacement,
    Resource,
    ResourceKey,
    Selection,
    TargetMapping,
    Workspace,
    build_operation_plan,
)
from fabshuffle.bcdr.protection_binding import (
    ConfigureProtectionRequest,
    capture_protection_records,
    configure_protection,
    owns_schema,
)
from fabshuffle.bcdr.registry import TYPE_REGISTRY
from fabshuffle.bcdr.service import (
    CutoverRequest,
    EnableRecoveryRequest,
    GroupStatus,
    ItemReadiness,
    PlanRequest,
    ReconcileOperationRequest,
    ServiceResult,
    SyncRequest,
)
from fabshuffle.fabric.client import FabricClient
from fabshuffle.fabric.workspaces import create_workspace
from fabshuffle.lifecycle import safe_text


def workspace_key(identity: WorkspaceIdentity) -> tuple[str, str]:
    return identity.tenant_id, identity.workspace_id


def item_key(identity: ItemIdentity) -> tuple[str, str, str]:
    return *workspace_key(identity), identity.item_id


def needs_data(item: ItemRecord) -> bool:
    contract = TYPE_REGISTRY.get(item.item_type)
    return bool(contract and contract.data_requirements)


class CapacityLifecycle(Protocol):
    def resume_catalog(self) -> None: ...
    def resume_business(self, runtime: DurableRuntime) -> None: ...
    def park(self, runtime: DurableRuntime, workspaces: Sequence[WorkspaceIdentity]) -> None: ...
    def close(self) -> None: ...
    def observations(self) -> tuple[dict[str, JsonValue], ...]: ...


class DataRecovery(Protocol):
    def restore(
        self,
        generation: CapturedGeneration,
        item: ItemRecord,
        target: ItemIdentity,
        runtime: DurableRuntime,
    ) -> tuple[bool, tuple[str, ...]]: ...


class CoordinatorCatalog(RecoveryCatalog, Protocol):
    def takeover_controller(
        self,
        controller_id: str,
        *,
        expected_controller_id: str,
        expected_epoch: int,
        fencing_evidence: str,
        operation_id: str,
    ) -> ControllerLease: ...


class RecoveryCoordinator:
    def __init__(
        self,
        recovery_set: RecoverySet,
        catalog: CoordinatorCatalog,
        destination: FabricClient,
        capacities: CapacityLifecycle,
        *,
        source: FabricClient | None = None,
        tokens: TokenProvider | None = None,
        source_tokens: TokenProvider | None = None,
        controller_id: str | None = None,
        data_recovery: DataRecovery | None = None,
        capture: Callable[..., CapturedGeneration] = capture_workspaces,
        apply: Callable = apply_captured_item,
        observe: Callable = observe_target,
        capabilities: Callable = adapter_capabilities,
        workspace_namespace: str = "workspaces",
        protected_root: Path | None = None,
    ) -> None:
        self.recovery_set = recovery_set
        self.runtime = DurableRuntime(catalog, controller_id)
        self.catalog = catalog
        self.destination = FencedFabricClient(destination, self.runtime)
        self.source = source
        self.capacities = capacities
        self.tokens = tokens
        self.source_tokens = source_tokens
        self.data_recovery = data_recovery
        self.capture = capture
        self.apply = apply
        self.observe = observe
        self.capabilities = capabilities
        self.workspace_namespace = workspace_namespace
        self.protected_root = protected_root
        self.access = AccessController(self.runtime, FabricAccess(self.destination), recovery_set)
        from fabshuffle.bcdr.failback import FailbackController

        self.failback = FailbackController(self)

    @classmethod
    def from_bootstrap(
        cls,
        bootstrap_path: Path,
        *,
        target_tokens: TokenProvider,
        source_tokens: TokenProvider | None = None,
        controller_id: str | None = None,
    ) -> RecoveryCoordinator:
        from fabshuffle.bcdr.production import open_coordinator

        return open_coordinator(
            bootstrap_path,
            target_tokens=target_tokens,
            source_tokens=source_tokens,
            controller_id=controller_id,
        )

    def close(self) -> None:
        self.destination.close()
        if self.source:
            self.source.close()
        self.capacities.close()

    def _wake(self) -> None:
        self.capacities.resume_catalog()

    def _groups(self, generation_id: str) -> tuple[GroupStatus, ...]:
        return tuple(
            GroupStatus.model_validate(row.document["group"])
            for row in self.catalog.list_records("groups")
            if row.document["generation_id"] == generation_id
        )

    def _save_group(
        self,
        generation_id: str,
        group: GroupStatus,
        *,
        metadata_blockers: tuple[str, ...] | None = None,
    ) -> None:
        previous = self.runtime.get("groups", group.group_id)
        if metadata_blockers is None:
            metadata_blockers = (
                tuple(previous.get("metadata_blockers", group.blockers))
                if (previous and previous["generation_id"] == generation_id)
                else group.blockers
            )
        self.runtime.put(
            "groups",
            group.group_id,
            {
                "generation_id": generation_id,
                "group": group.model_dump(mode="json"),
                "metadata_blockers": list(metadata_blockers),
            },
        )

    def status(self) -> ServiceResult:
        self._wake()
        state = self.catalog.state()
        generation_id = state.current_generation_id
        inventory = self.catalog.load_generation(generation_id).snapshot if generation_id else None
        return ServiceResult(
            mode=state.mode,
            generation_id=generation_id,
            groups=self._groups(generation_id) if generation_id else (),
            details={
                "writer": self.runtime.get("lifecycle", "writer") or {"epoch": 0, "side": "primary"},
                "pending_operations": [
                    row.model_dump(mode="json") for row in self.catalog.pending_operations()
                ],
                "controller_id": state.controller_id,
                "controller_epoch": state.epoch,
                "capacities": list(self.capacities.observations()),
                "desired_acls": [row.model_dump(mode="json") for row in inventory.desired_acls]
                if inventory
                else [],
                "applied_items": [row.model_dump(mode="json") for row in self.catalog.applied_items()],
                "inventory": {
                    "workspaces": [row.model_dump(mode="json") for row in inventory.workspaces],
                    "items": [row.model_dump(mode="json") for row in inventory.items],
                }
                if inventory
                else {"workspaces": [], "items": []},
            },
        )

    def _inventory(self, generation: CapturedGeneration) -> EstateInventory:
        snapshot = generation.snapshot
        items = []
        for item in snapshot.items:
            cap = self.capabilities(item)
            external = [
                edge
                for edge in snapshot.dependencies
                if edge.consumer == item.identity
                and edge.external_reference
                and edge.phase in {"create", "bind"}
            ]
            evidence = Evidence(
                EvidenceState.PARTIAL if item.unresolved or external else EvidenceState.COMPLETE,
                ("captured definition and typed dependency inventory",),
                "; ".join((*item.unresolved, *(edge.detail for edge in external))),
            )
            items.append(
                Item(
                    item_key(item.identity),
                    item.display_name,
                    item.item_type,
                    evidence,
                    AdapterCapabilities(
                        inactive_create=cap.inactive_create,
                        shell_then_bind=False,
                        is_store=cap.is_store,
                        provenance=cap.reason or "Qualified destination-only capture adapter",
                    ),
                    eligible=not item.tombstone,
                    data_required=needs_data(item),
                    protection_available=any(
                        p.item == item.identity and p.outcome == RecoveryOutcome.PROTECTED
                        for p in snapshot.protections
                    ),
                    protection_action=f"Configure a protected recovery data input for '{item.display_name}'",
                )
            )
        edges = []
        resources = {}
        for edge in snapshot.dependencies:
            prerequisite = edge.prerequisite
            if prerequisite is None:
                continue
            if isinstance(prerequisite, EndpointIdentity):
                key = ResourceKey(
                    "endpoint",
                    prerequisite.item.tenant_id,
                    prerequisite.endpoint_id,
                    prerequisite.item.workspace_id,
                    prerequisite.item.item_id,
                )
                resources[key] = Resource(
                    key,
                    owner=item_key(prerequisite.item),
                    prepare_supported=True,
                    provenance="Exact typed destination endpoint observation",
                )
            elif isinstance(prerequisite, ConnectionIdentity):
                key = ResourceKey("connection", prerequisite.tenant_id, prerequisite.connection_id)
                resources[key] = Resource(key)
            else:
                key = item_key(prerequisite)
            edges.append(
                Dependency(
                    item_key(edge.consumer),
                    key,
                    edge.provenance,
                    phase=Phase(edge.phase),
                    required=edge.required,
                    qualified=not bool(edge.external_reference),
                )
            )
        return EstateInventory(
            tuple(
                Workspace(
                    workspace_key(w.identity),
                    (w.identity.tenant_id, w.capacity_id),
                    w.display_name,
                )
                for w in snapshot.workspaces
            ),
            tuple(items),
            tuple(edges),
            tuple(resources.values()),
        )

    def configure_protection(self, request: ConfigureProtectionRequest) -> ServiceResult:
        self._wake()
        with self.runtime.controller({RecoveryMode.STANDBY}):
            protection = configure_protection(self.runtime, request, protected_root=self.protected_root)
            return ServiceResult(
                mode=self.runtime.mode,
                details={"protection": protection.model_dump(mode="json")},
                warnings=("Configuration applies to the next capture; existing generations stay immutable.",),
            )

    def workspace_mappings(self) -> dict[str, WorkspaceIdentity]:
        return {
            row.key: WorkspaceIdentity.model_validate(row.document["target"])
            for row in self.catalog.list_records(self.workspace_namespace)
        }

    def item_mappings(self) -> dict[str, AppliedItem]:
        rows = self.catalog.applied_items()
        if self.workspace_namespace != "workspaces":
            owned = self.workspace_mappings()
            rows = tuple(
                row
                for row in rows
                if owned.get("/".join(row.source.key.split("/")[:2]))
                == (WorkspaceIdentity(tenant_id=row.target.tenant_id, workspace_id=row.target.workspace_id))
            )
        return {row.source.key: row for row in rows}

    def _endpoint_pairs(
        self,
        generation: CapturedGeneration,
        applied: Mapping[str, AppliedItem],
    ) -> tuple[tuple[EndpointIdentity, str], ...]:
        pairs = []
        for item in generation.snapshot.items:
            target = applied.get(item.identity.key)
            if target is None:
                continue
            source = item.properties
            if not any(
                source.get(key) for key in ("sqlEndpointProperties", "serverFqdn", "connectionString")
            ):
                continue
            observed = item_document(self.destination, target.target, item.item_type).get("properties", {})
            old_sql = source.get("sqlEndpointProperties") or {}
            new_sql = observed.get("sqlEndpointProperties") or {}
            if old_sql and new_sql.get("provisioningStatus") != "Success":
                raise RecoveryBlocked(
                    f"Wait for the destination SQL endpoint of '{item.display_name}' to report Success"
                )
            values = (
                ("sql_endpoint_id", old_sql.get("id"), new_sql.get("id")),
                ("sql_endpoint_server", old_sql.get("connectionString"), new_sql.get("connectionString")),
                ("sql_server", source.get("serverFqdn"), observed.get("serverFqdn")),
                ("sql_connection", source.get("connectionString"), observed.get("connectionString")),
                ("sql_database_name", source.get("databaseName"), observed.get("databaseName")),
            )
            for kind, old, new in values:
                if old:
                    if not isinstance(old, str) or not isinstance(new, str) or not new:
                        raise RecoveryBlocked(
                            f"Capture and observe the complete {kind} binding for {item.identity.key}"
                        )
                    pairs.append(
                        (EndpointIdentity(item=item.identity, endpoint_kind=kind, endpoint_id=old), new)
                    )
        return tuple(pairs)

    def _plan(self, generation: CapturedGeneration, request: PlanRequest) -> OperationPlan:
        tenant = self.recovery_set.tenant_id
        routes = request.capacity_routes
        if any(
            route.source_capacity_id not in self.recovery_set.source_capacity_ids
            or route.target_capacity_id not in self.recovery_set.target_capacity_ids
            for route in routes
        ):
            raise RecoveryBlocked("Choose only the configured source and dedicated recovery capacities")
        known_items = {row.identity.key for row in generation.snapshot.items if not row.tombstone}
        mappings = tuple(
            TargetMapping(
                item_key(row.source),
                item_key(row.target),
                row.operation_id,
            )
            for row in self.item_mappings().values()
            if row.source.key in known_items
        )
        replacements = []
        seen_connections = set()
        source_literals = {
            value
            for item in generation.snapshot.items
            for value in (item.identity.workspace_id, item.identity.item_id)
        }
        for item in generation.snapshot.items:
            sql_endpoint = item.properties.get("sqlEndpointProperties") or {}
            for value in (
                item.properties.get("serverFqdn"),
                item.properties.get("queryServiceUri"),
                item.properties.get("ingestionServiceUri"),
                sql_endpoint.get("connectionString"),
            ):
                if isinstance(value, str) and value:
                    source_literals.add(value.split(",", 1)[0].lower())
            connection = item.properties.get("connectionString")
            if isinstance(connection, str) and connection and ";" not in connection:
                source_literals.add(connection.lower())
        for edge in generation.snapshot.dependencies:
            if (
                isinstance(edge.prerequisite, EndpointIdentity)
                and edge.prerequisite.endpoint_kind != "sql_database_name"
            ):
                source_literals.add(edge.prerequisite.endpoint_id.lower())
        for route in request.connection_mappings:
            if route.source.key in seen_connections:
                raise RecoveryBlocked("Map each captured connection exactly once")
            seen_connections.add(route.source.key)
            if route.source.tenant_id != tenant or route.target.tenant_id != tenant:
                raise RecoveryBlocked("Connection mappings must stay in the recovery tenant")
            observed = self.destination.get(f"connections/{route.target.connection_id}")
            if observed.get("id") != route.target.connection_id:
                raise RecoveryBlocked("Connection lookup did not return the exact approved identity")
            description = canonical_json(observed).decode("utf-8").lower()
            if any(value in description for value in source_literals):
                raise RecoveryBlocked(
                    f"Connection {route.target.connection_id} still names the source estate; "
                    "provision and map an independent recovery connection"
                )
            mapping = TargetMapping(
                ResourceKey("connection", tenant, route.source.connection_id),
                ResourceKey("connection", tenant, route.target.connection_id),
                digest(canonical_json(observed)),
            )
            replacements.append(
                Replacement(
                    mapping=mapping,
                    validated=True,
                    independent=True,
                    ready=False,
                    provenance=route.evidence,
                )
            )
        return build_operation_plan(
            self._inventory(generation),
            Selection(
                tuple((tenant, identifier) for identifier in self.recovery_set.source_capacity_ids),
                include=tuple((tenant, identifier) for identifier in request.include_workspace_ids),
                exclude=tuple((tenant, identifier) for identifier in request.exclude_workspace_ids),
                keywords=request.keywords,
                approved_additions=tuple(
                    (tenant, identifier) for identifier in request.approved_addition_ids
                ),
            ),
            capacity_mappings=tuple(
                CapacityMapping(
                    (tenant, route.source_capacity_id),
                    (tenant, route.target_capacity_id),
                )
                for route in routes
            ),
            control_workspace=workspace_key(self.recovery_set.control_workspace),
            control_warehouse=item_key(self.recovery_set.control_warehouse),
            target_mappings=mappings,
            replacements=tuple(replacements),
        )

    def plan(self, request: PlanRequest) -> ServiceResult:
        self._wake()
        generation = self.catalog.load_generation(request.generation_id)
        plan = self._plan(generation, request)
        detail = TypeAdapter(dict[str, JsonValue]).validate_json(
            TypeAdapter(OperationPlan).dump_json(plan),
        )
        detail["inventory"] = {
            "workspaces": [row.model_dump(mode="json") for row in generation.snapshot.workspaces],
            "items": [row.model_dump(mode="json") for row in generation.snapshot.items],
        }
        detail["desired_acls"] = [row.model_dump(mode="json") for row in generation.snapshot.desired_acls]
        detail["applied_items"] = [row.model_dump(mode="json") for row in self.catalog.applied_items()]
        return ServiceResult(
            mode=self.catalog.state().mode,
            generation_id=generation.snapshot.generation_id,
            details=detail,
        )

    def _capture(self) -> CapturedGeneration:
        if self.source is None or self.source_tokens is None:
            raise RecoveryBlocked(
                "Supply source credentials only for pre-outage capture; enable uses the catalog"
            )
        previous_id = self.catalog.state().current_generation_id
        authority = self.runtime.get("lifecycle", "authority")
        authoritative_workspaces = (
            [WorkspaceIdentity.model_validate(row).workspace_id for row in authority["workspaces"]]
            if authority
            else None
        )
        captured = self.capture(
            self.source,
            self.recovery_set,
            tokens=self.source_tokens,
            parent_generation_id=previous_id,
            workspace_ids=authoritative_workspaces,
        )
        if previous_id:
            previous = self.catalog.load_generation(previous_id)
            present = {row.identity.key for row in captured.snapshot.items}
            workspaces = {row.identity.key: row for row in captured.snapshot.workspaces}
            deleted = []
            for item in previous.snapshot.items:
                if item.identity.key not in present:
                    deleted.append(
                        item.model_copy(
                            update={
                                "tombstone": True,
                                "payload_ids": (),
                                "capture_complete": True,
                                "unresolved": (),
                                "captured_at": captured.snapshot.captured_at,
                            }
                        )
                    )
                    ws = WorkspaceIdentity(
                        tenant_id=item.identity.tenant_id,
                        workspace_id=item.identity.workspace_id,
                    )
                    if ws.key not in workspaces:
                        workspaces[ws.key] = next(
                            row for row in previous.snapshot.workspaces if row.identity == ws
                        )
            captured = CapturedGeneration(
                captured.snapshot.model_copy(
                    update={
                        "items": (*captured.snapshot.items, *deleted),
                        "workspaces": tuple(workspaces.values()),
                    }
                ),
                captured.payloads,
            )
        configured = capture_protection_records(captured.snapshot.items, self.catalog)
        configured_items = {row.item.key for row in configured}
        captured = CapturedGeneration(
            captured.snapshot.model_copy(
                update={
                    "protections": (
                        *(
                            row
                            for row in captured.snapshot.protections
                            if row.item.key not in configured_items
                        ),
                        *configured,
                    ),
                }
            ),
            captured.payloads,
        )
        self.catalog.stage_generation(self.runtime.require_lease(), captured.snapshot, captured.payloads)
        return self.catalog.publish_generation(
            self.runtime.require_lease(),
            captured.snapshot.generation_id,
            expected_current=previous_id,
        )

    def synchronize(self, request: SyncRequest) -> ServiceResult:
        self._wake()
        with self.runtime.controller({RecoveryMode.STANDBY, RecoveryMode.SYNCING}):
            if self.runtime.mode == RecoveryMode.STANDBY:
                self.runtime.transition(RecoveryMode.SYNCING)
            self.capacities.resume_business(self.runtime)
            generation = (
                self._capture() if request.capture else self.catalog.load_generation(request.generation_id)
            )
            plan = self._plan(generation, request)
            self.runtime.put("plans", generation.snapshot.generation_id, request.model_dump(mode="json"))
            for route in request.connection_mappings:
                self.runtime.put(
                    "connections",
                    route.source.key,
                    {
                        "generation_id": generation.snapshot.generation_id,
                        "target": route.target.model_dump(mode="json"),
                        "evidence": route.evidence,
                    },
                )
            groups, warnings = self._apply_plan(generation, plan, request.suffix)
            self.runtime.transition(RecoveryMode.STANDBY)
            result = ServiceResult(
                mode=RecoveryMode.STANDBY,
                generation_id=generation.snapshot.generation_id,
                groups=groups,
                warnings=warnings,
            )
            if request.park:
                self.capacities.park(self.runtime, tuple(self.workspace_mappings().values()))
            return result.model_copy(
                update={
                    "mode": self.runtime.mode,
                    "details": {"capacities": list(self.capacities.observations())},
                }
            )

    def _apply_plan(
        self,
        generation: CapturedGeneration,
        plan: OperationPlan,
        suffix: str,
    ) -> tuple[tuple[GroupStatus, ...], tuple[str, ...]]:
        snapshot = generation.snapshot
        generation_id = snapshot.generation_id
        items = {item_key(row.identity): row for row in snapshot.items}
        source_workspaces = {workspace_key(row.identity): row for row in snapshot.workspaces}
        workspaces = {
            key: row
            for key, row in self.workspace_mappings().items()
            if tuple(key.split("/")) in source_workspaces
        }
        applied = {key: row for key, row in self.item_mappings().items() if item_key(row.source) in items}
        placements = {row.source: row for row in plan.placements}
        operations = {row.key: row for row in plan.operations}
        completed = set()
        blockers: dict[str, list[str]] = {}
        warnings = []
        plan_settings = self.runtime.get("plans", generation_id) or {}
        connections = {
            row.key: ConnectionIdentity.model_validate(row.document["target"])
            for row in self.catalog.list_records("connections")
            if row.document["generation_id"] == generation_id
        }
        self.access.restrict_workspace(self.recovery_set.control_workspace)
        for key in plan.executable_order:
            op = operations[key]
            if any(prerequisite not in completed for prerequisite in op.prerequisites):
                continue
            if op.kind == "workspace_create":
                workspace = source_workspaces[op.subject]
                target = workspaces.get(workspace.identity.key)
                if target is None:
                    placement = placements[op.subject]
                    result = self.runtime.effect(
                        "workspace-create",
                        f"{self.workspace_namespace}/{workspace.identity.key}",
                        lambda w=workspace, p=placement: create_workspace(
                            self.destination,
                            w.display_name + suffix,
                            p.target_capacity[1],
                        ),
                        generation_id=generation_id,
                    )
                    target = WorkspaceIdentity(
                        tenant_id=workspace.identity.tenant_id, workspace_id=result["id"]
                    )
                    if target == self.recovery_set.control_workspace:
                        raise RecoveryBlocked(
                            "Business mappings cannot target the permanent control workspace"
                        )
                    self.runtime.put(
                        self.workspace_namespace,
                        workspace.identity.key,
                        {
                            "target": target.model_dump(mode="json"),
                            "capacity_id": placement.target_capacity[1],
                        },
                    )
                    workspaces[workspace.identity.key] = target
                self.access.restrict_workspace(target)
                self.access.fabric.inspect_items(
                    [
                        (row.target, items[item_key(row.source)].item_type)
                        for row in applied.values()
                        if row.target.workspace_id == target.workspace_id and item_key(row.source) in items
                    ],
                    {grant.principal.key for grant in self.recovery_set.access_policy.workspace_grants()},
                )
                completed.add(key)
            elif op.kind in {"store_create", "item_create"}:
                item = items[op.subject]
                current = applied.get(item.identity.key)
                if current is None:
                    current = self._rearmed_item(generation, item)
                    if current is not None:
                        applied[item.identity.key] = current
                shell_only = owns_schema(generation, item, self.catalog)
                hashes = captured_hashes(item, generation.payloads)
                prerequisites = {
                    edge.prerequisite.key
                    for edge in snapshot.dependencies
                    if edge.consumer == item.identity and isinstance(edge.prerequisite, ItemIdentity)
                } | {item.identity.key}
                try:
                    endpoint_pairs = self._endpoint_pairs(
                        generation,
                        {key: row for key, row in applied.items() if key in prerequisites},
                    )
                except RecoveryBlocked as error:
                    blockers.setdefault(item.identity.key, []).append(str(error))
                    continue
                binding_hash = digest(
                    canonical_json(
                        {
                            "dependencies": {
                                edge.prerequisite.key: applied[edge.prerequisite.key].target.key
                                for edge in snapshot.dependencies
                                if edge.consumer == item.identity
                                and isinstance(edge.prerequisite, ItemIdentity)
                                and edge.prerequisite.key in applied
                            },
                            "connections": {source: target.key for source, target in connections.items()},
                            "endpoints": [[source.key, target] for source, target in endpoint_pairs],
                        }
                    )
                )
                prior_binding = self.runtime.get("binding-hashes", item.identity.key)
                if current:
                    observed = self.observe(self.destination, current.target, item.item_type)
                    if observed != current.target_observed_sha256:
                        blockers.setdefault(item.identity.key, []).append(
                            f"Target drift on '{item.display_name}'; "
                            "review the recovery changes before syncing"
                        )
                        continue
                    if (
                        (current.definition_sha256, current.properties_sha256) == hashes
                        and prior_binding
                        and prior_binding["sha256"] == binding_hash
                    ):

                        def unchanged(old=current) -> dict[str, JsonValue]:
                            return {
                                "applied": old.model_copy(
                                    update={
                                        "capture_generation_id": generation_id,
                                        "applied_at": now(),
                                        "operation_id": self.runtime.current_operation.operation_id,
                                    }
                                ).model_dump(mode="json")
                            }

                        result = self.runtime.effect(
                            "item-observe",
                            f"{generation_id}/{item.identity.key}",
                            unchanged,
                            generation_id=generation_id,
                            source=item.identity,
                            target=current.target,
                        )
                        updated = AppliedItem.model_validate(result["applied"])
                        self.catalog.record_applied(self.runtime.require_lease(), updated)
                        applied[item.identity.key] = updated
                        completed.add(key)
                        continue
                target_workspace = workspaces["/".join(op.subject[:2])]
                mapping_digest = digest(
                    canonical_json(
                        {
                            "maps": {key: value.target.key for key, value in applied.items()},
                            "bindings": binding_hash,
                        }
                    )
                )
                effect_key = f"{generation_id}/{item.identity.key}/{mapping_digest}"

                def apply_item(
                    i=item,
                    old=current,
                    ws=target_workspace,
                    shell=shell_only,
                    endpoints=endpoint_pairs,
                ) -> dict[str, JsonValue]:
                    result = self.apply(
                        self.destination,
                        i,
                        generation.payloads,
                        generation_id=generation_id,
                        operation_id=self.runtime.current_operation.operation_id,
                        target_workspace=ws,
                        source_items=snapshot.items,
                        item_mappings={item_key(row.source): row.target for row in applied.values()},
                        workspace_mappings={
                            tuple(key.split("/")): value for key, value in workspaces.items()
                        },
                        target_id=old.target.item_id if old else None,
                        tokens=self.tokens,
                        shell_only=shell,
                        mutation_guard=self.runtime.fence,
                        destination_quiescence=plan_settings.get("target_quiescence_evidence"),
                        verified_external_connections=tuple(
                            target for key, target in connections.items() if key == target.key
                        ),
                        endpoint_mappings=endpoints,
                        connection_mappings=tuple(
                            (
                                ConnectionIdentity(
                                    tenant_id=key.split("/")[0],
                                    connection_id=key.split("/")[-1],
                                ),
                                target,
                            )
                            for key, target in connections.items()
                        ),
                    )
                    return {
                        "applied": result.applied.model_dump(mode="json") if result.applied else None,
                        "diagnostics": list(result.diagnostics),
                        "deferred_grants": list(result.deferred_grants),
                        "metadata_applied": result.metadata_applied,
                    }

                result = self.runtime.effect(
                    "item-apply",
                    effect_key,
                    apply_item,
                    generation_id=generation_id,
                    source=item.identity,
                    target=current.target if current else None,
                )
                if result["applied"] is None:
                    blockers.setdefault(item.identity.key, []).extend(result["diagnostics"])
                    continue
                row = AppliedItem.model_validate(result["applied"])
                self.catalog.record_applied(self.runtime.require_lease(), row)
                applied[item.identity.key] = row
                self.runtime.put("binding-hashes", item.identity.key, {"sha256": binding_hash})
                self.runtime.put(
                    "item-security",
                    item.identity.key,
                    {
                        "generation_id": generation_id,
                        "target": row.target.model_dump(mode="json"),
                        "deferred_grants": result["deferred_grants"],
                        "metadata_applied": result["metadata_applied"],
                    },
                )
                completed.add(key)
            elif op.kind in {"bind", "data_ready", "ready"}:
                # Only bind is a metadata milestone. The data/ready operations need separate evidence.
                if op.kind == "bind":
                    completed.add(key)
            elif op.kind == "replacement":
                completed.add(key)
            elif op.kind == "endpoint_ready":
                endpoint = op.subject
                if any(
                    source.endpoint_id == endpoint.resource_id and source.item.item_id == endpoint.item_id
                    for source, _ in self._endpoint_pairs(generation, applied)
                ):
                    completed.add(key)
        for item in snapshot.items:
            if item.tombstone:
                warnings.append(
                    f"'{item.display_name}' was deleted at the source; "
                    "tombstone retained, standby not deleted"
                )
        groups = self._build_groups(generation, plan, applied, blockers)
        for group in groups:
            self._save_group(generation_id, group, metadata_blockers=group.blockers)
        return groups, tuple(warnings)

    def _rearmed_item(self, generation: CapturedGeneration, item: ItemRecord) -> AppliedItem | None:
        alias = self.runtime.get("rearmed-items", item.identity.key)
        if alias is None:
            return None
        target = ItemIdentity.model_validate(alias["target"])
        ownership = next(
            (row for row in self.catalog.operations() if row.operation_id == alias["ownership_operation"]),
            None,
        )
        if ownership is None or ownership.state != OperationState.SUCCEEDED or ownership.target != target:
            raise RecoveryBlocked("The retained standby target has no successful ownership receipt")
        observed = self.observe(self.destination, target, item.item_type)
        if observed != alias["observed_sha256"]:
            raise RecoveryBlocked(
                f"Retained standby {target.key} changed after rearm; review drift before syncing"
            )

        def receipt():
            row = AppliedItem(
                source=item.identity,
                target=target,
                capture_generation_id=generation.snapshot.generation_id,
                applied_at=now(),
                definition_sha256=digest(b"rearm invalidated"),
                properties_sha256=digest(b"rearm invalidated"),
                target_observed_sha256=observed,
                outcome=RecoveryOutcome.PARTIAL,
                operation_id=self.runtime.current_operation.operation_id,
            )
            return {"applied": row.model_dump(mode="json")}

        result = self.runtime.effect(
            "rearm-owned-mapping",
            f"{generation.snapshot.generation_id}/{item.identity.key}",
            receipt,
            generation_id=generation.snapshot.generation_id,
            source=item.identity,
            target=target,
        )
        row = AppliedItem.model_validate(result["applied"])
        self.catalog.record_applied(self.runtime.require_lease(), row)
        return row

    def _build_groups(
        self,
        generation: CapturedGeneration,
        plan: OperationPlan,
        applied: Mapping[str, AppliedItem],
        failures: Mapping[str, list[str]],
    ) -> tuple[GroupStatus, ...]:
        selected = {key for key, _ in plan.evidence}
        records = {
            item_key(row.identity): row
            for row in generation.snapshot.items
            if item_key(row.identity) in selected and not row.tombstone
        }
        components = [{key} for key in records]
        links = [(left, right) for left in records for right in records if left[:2] == right[:2]]
        links.extend(
            (edge.consumer, edge.prerequisite)
            for edge in plan.dependencies
            if isinstance(edge.prerequisite, tuple)
        )
        resource_consumers = {}
        for edge in plan.dependencies:
            if isinstance(edge.prerequisite, ResourceKey):
                resource_consumers.setdefault(edge.prerequisite, []).append(edge.consumer)
        links.extend(
            (consumers[0], consumer)
            for consumers in resource_consumers.values()
            for consumer in consumers[1:]
        )
        for left, right in links:
            matches = [component for component in components if left in component or right in component]
            if len(matches) > 1:
                merged = set().union(*matches)
                components = [component for component in components if component not in matches] + [merged]
        consumers = {row.item: row for row in plan.consumers}
        groups = []
        for component in sorted(components, key=lambda group: sorted(group)):
            keys = sorted(component)
            rows = [records[key] for key in keys]
            reasons = [message for row in rows for message in failures.get(row.identity.key, [])]
            for key in keys:
                reasons.extend(blocker.message for blocker in consumers[key].metadata_blockers)
            missing = [
                row.display_name
                for row in rows
                if row.identity.key not in applied
                or applied[row.identity.key].capture_generation_id != generation.snapshot.generation_id
            ]
            reasons.extend(f"Apply captured metadata for '{name}'" for name in missing)
            for row in rows:
                state = self.runtime.get("item-security", row.identity.key)
                if state and not state["metadata_applied"]:
                    reasons.append(f"Complete provider-owned metadata for '{row.display_name}'")
            groups.append(
                GroupStatus(
                    group_id=digest(canonical_json({"items": [row.identity.key for row in rows]})),
                    items=tuple(row.identity for row in rows),
                    metadata_applied=not reasons,
                    data_ready=not any(needs_data(row) for row in rows),
                    blockers=tuple(dict.fromkeys(reasons)),
                )
            )
        return tuple(groups)

    def _selected_groups(self, generation_id: str, requested: Sequence[str]) -> tuple[GroupStatus, ...]:
        if not requested or len(requested) != len(set(requested)):
            raise RecoveryBlocked("Select one or more distinct dependency groups from the current plan")
        groups = {row.group_id: row for row in self._groups(generation_id)}
        if set(requested) - groups.keys():
            raise RecoveryBlocked("Unknown or stale dependency group; preview the pinned generation again")
        return tuple(groups[key] for key in requested)

    def validate_readiness(
        self,
        generation: CapturedGeneration,
        groups: Sequence[GroupStatus],
        evidence: Sequence[ItemReadiness],
        *,
        security: bool,
        mappings: Mapping[str, AppliedItem] | None = None,
    ) -> None:
        proofs = {row.source.key: row for row in evidence}
        if len(proofs) != len(evidence):
            raise RecoveryBlocked("Readiness evidence contains duplicate item identities")
        applied = self.item_mappings() if mappings is None else mappings
        items = {row.identity.key: row for row in generation.snapshot.items}
        for group in groups:
            if not group.metadata_applied or group.blockers:
                raise RecoveryBlocked(
                    f"Resolve metadata blockers for group {group.group_id} before admission"
                )
            for identity in group.items:
                row = applied.get(identity.key)
                proof = proofs.get(identity.key)
                if row is None or proof is None or proof.target != row.target:
                    raise RecoveryBlocked(f"Supply readiness evidence for the exact target of {identity.key}")
                if (
                    row.capture_generation_id != generation.snapshot.generation_id
                    or not proof.observed_at <= now() < proof.valid_until
                    or proof.target_observed_sha256
                    != self.observe(
                        self.destination,
                        row.target,
                        items[identity.key].item_type,
                    )
                    or not proof.data_verified
                    or not proof.references_verified
                    or (security and (not proof.security_verified or not proof.effective_principals))
                ):
                    raise RecoveryBlocked(
                        f"Revalidate current data, bindings and effective identity for {identity.key}; "
                        "the evidence does not match this generation/target state"
                    )

    def enable_recovery(self, request: EnableRecoveryRequest) -> ServiceResult:
        self._wake()
        with self.runtime.controller({RecoveryMode.STANDBY, RecoveryMode.ENABLING_RECOVERY}):
            starting = self.runtime.mode == RecoveryMode.STANDBY
            if starting:
                self.runtime.transition(RecoveryMode.ENABLING_RECOVERY)
                self.runtime.put("lifecycle", "enabled-generation", {"generation_id": request.generation_id})
            pinned = self.runtime.get("lifecycle", "enabled-generation")
            if pinned and pinned["generation_id"] != request.generation_id:
                raise RecoveryBlocked("Recovery is already pinned to another generation; do not overwrite it")
            self.capacities.resume_business(self.runtime)
            generation = self.catalog.load_generation(request.generation_id)
            if starting or not any(group.access_enabled for group in self._groups(request.generation_id)):
                stored = self.runtime.get("plans", request.generation_id)
                if stored is None:
                    raise RecoveryBlocked("Preview and synchronize a pinned plan before enabling recovery")
                planned = PlanRequest.model_validate(
                    {key: value for key, value in stored.items() if key not in {"capture", "park"}}
                )
                self._apply_plan(generation, self._plan(generation, planned), planned.suffix)
            groups = self._selected_groups(request.generation_id, request.group_ids)
            applied = self.item_mappings()
            workspaces = self.workspace_mappings()
            connections = {
                row.key: ConnectionIdentity.model_validate(row.document["target"])
                for row in self.catalog.list_records("connections")
                if row.document["generation_id"] == request.generation_id
            }
            items = {row.identity.key: row for row in generation.snapshot.items}
            results = []
            approved = set(request.approved_acl_ids)
            desired = {row.acl_id: row for row in generation.snapshot.desired_acls}
            if approved - desired.keys():
                raise RecoveryBlocked("Approve only ACL IDs from the selected captured generation")
            self.access.restrict_workspace(self.recovery_set.control_workspace)
            for group in groups:
                stored_group = self.runtime.get("groups", group.group_id)
                group = group.model_copy(
                    update={
                        "blockers": tuple(stored_group.get("metadata_blockers", group.blockers)),
                    }
                )
                metadata_pending = {
                    f"Complete provider-owned metadata for '{items[source.key].display_name}'"
                    for source in group.items
                    if owns_schema(generation, items[source.key], self.catalog)
                }
                reasons = [reason for reason in group.blockers if reason not in metadata_pending]
                for source in group.items:
                    item = items[source.key]
                    if needs_data(item):
                        if self.data_recovery is None or source.key not in applied:
                            reasons.append(f"Configure optional protected data for '{item.display_name}'")
                        else:
                            prior_data = self.runtime.get(
                                "data-restored", f"{request.generation_id}/{source.key}"
                            )
                            current = applied[source.key]
                            if prior_data and prior_data["target"] == current.target.model_dump(mode="json"):
                                restored, messages = True, ()
                            else:
                                restored, messages = self.data_recovery.restore(
                                    generation,
                                    item,
                                    current.target,
                                    self.runtime,
                                )
                            if self.catalog.pending_operations():
                                raise RecoveryBlocked(
                                    f"Reconcile the failed data restoration for '{item.display_name}' "
                                    "before another group can mutate recovery resources"
                                )
                            if not restored:
                                reasons.extend(messages)
                            else:
                                self.runtime.put(
                                    "data-restored",
                                    f"{request.generation_id}/{source.key}",
                                    {
                                        "target": current.target.model_dump(mode="json"),
                                        "provider_completed": True,
                                    },
                                )
                                if owns_schema(generation, item, self.catalog):
                                    self._accept_provider_metadata(generation, item, current)
                if not reasons:
                    group = group.model_copy(update={"metadata_applied": True, "blockers": ()})
                if reasons:
                    result = group.model_copy(
                        update={"blockers": tuple(dict.fromkeys(reasons)), "data_ready": False}
                    )
                    self._save_group(request.generation_id, result)
                    results.append(result)
                    continue
                attempted_grants = []
                try:
                    self.validate_readiness(generation, (group,), request.readiness, security=False)
                    group_sources = {row.key for row in group.items}
                    group_workspaces = {"/".join(row.key.split("/")[:2]) for row in group.items}
                    relevant = [
                        acl
                        for acl in desired.values()
                        if (acl.item and acl.item.key in group_sources)
                        or (acl.workspace and acl.workspace.key in group_workspaces)
                        or (
                            acl.connection
                            and any(
                                edge.consumer.key in group_sources and edge.prerequisite == acl.connection
                                for edge in generation.snapshot.dependencies
                            )
                        )
                    ]
                    for identity in group.items:
                        security = self.runtime.get("item-security", identity.key)
                        if security and any(
                            grant.get("scope") != "schedule" for grant in security["deferred_grants"]
                        ):
                            raise RecoveryBlocked(
                                f"Apply the captured security memberships/policies for {identity.key} "
                                "through a qualified workload security adapter before admission"
                            )
                    # Check all surfaces first; an unsupported policy must not expose the group.
                    mapped = [
                        remap_acl(
                            acl, workspaces, {key: row.target for key, row in applied.items()}, connections
                        )
                        for acl in relevant
                    ]
                    mapped.sort(key=lambda acl: acl.workspace is not None)
                    for acl in mapped:
                        self.access.fabric.path(acl)
                        if acl.acl_id not in approved:
                            raise RecoveryBlocked(f"Approve deferred ACL {acl.acl_id} for this group")
                    for acl in mapped:
                        self.access.apply(acl, approved=True)
                        attempted_grants.append(acl)
                    result = group.model_copy(
                        update={
                            "access_enabled": True,
                            "data_ready": True,
                            "blockers": (),
                        }
                    )
                except RecoveryBlocked as error:
                    if self.catalog.pending_operations():
                        raise
                    self.access.rollback_grants(attempted_grants)
                    result = group.model_copy(update={"blockers": (str(error),)})
                self._save_group(request.generation_id, result)
                results.append(result)
            return ServiceResult(
                mode=self.runtime.mode,
                generation_id=request.generation_id,
                groups=tuple(results),
                warnings=(
                    "Recovery access is staged; production jobs remain stopped until separately authorized.",
                ),
            )

    def _accept_provider_metadata(self, generation, item, current) -> None:
        observed = self.observe(self.destination, current.target, item.item_type)

        def receipt():
            return {
                "applied": current.model_copy(
                    update={
                        "target_observed_sha256": observed,
                        "applied_at": now(),
                        "operation_id": self.runtime.current_operation.operation_id,
                        "outcome": RecoveryOutcome.RESTORED_STOPPED,
                    }
                ).model_dump(mode="json")
            }

        result = self.runtime.effect(
            "provider-metadata",
            f"{generation.snapshot.generation_id}/{item.identity.key}",
            receipt,
            generation_id=generation.snapshot.generation_id,
            source=item.identity,
            target=current.target,
        )
        self.catalog.record_applied(
            self.runtime.require_lease(), AppliedItem.model_validate(result["applied"])
        )
        security = self.runtime.get("item-security", item.identity.key)
        self.runtime.put("item-security", item.identity.key, {**security, "metadata_applied": True})

    def cutover(self, request: CutoverRequest) -> ServiceResult:
        self._wake()
        with self.runtime.controller({RecoveryMode.ENABLING_RECOVERY, RecoveryMode.ACTIVE_RECOVERY}):
            generation = self.catalog.load_generation(request.generation_id)
            groups = self._selected_groups(request.generation_id, request.group_ids)
            if any(not group.access_enabled or not group.data_ready for group in groups):
                raise RecoveryBlocked(
                    "Enable access and establish protected data for every selected dependency group"
                )
            self.validate_readiness(generation, groups, request.readiness, security=True)
            writer = self.runtime.get("lifecycle", "writer") or {"epoch": 0, "side": "primary"}
            request.writer_fence.require_current(now(), "primary", writer["epoch"])
            self.runtime.put(
                "lifecycle",
                "writer",
                {
                    "epoch": writer["epoch"] + 1,
                    "side": "recovery",
                    "generation_id": request.generation_id,
                    "fence": request.writer_fence.model_dump(mode="json"),
                    "evidence_kind": "operator_attestation_verified_against_current_targets",
                },
            )
            if self.runtime.mode == RecoveryMode.ENABLING_RECOVERY:
                self.runtime.transition(RecoveryMode.ACTIVE_RECOVERY)
            active = tuple(
                group.model_copy(update={"active": True, "ready_for_cutover": True}) for group in groups
            )
            for group in active:
                self._save_group(request.generation_id, group)
            return ServiceResult(
                mode=self.runtime.mode,
                generation_id=request.generation_id,
                groups=active,
                warnings=(
                    "Selected groups are admitted for consumer cutover. No archived job, mirror, rule or "
                    "ingestion process was executed automatically; follow its workload activation runbook.",
                ),
            )

    def reconcile_operation(self, request: ReconcileOperationRequest) -> ServiceResult:
        self._wake()
        state = self.catalog.state()
        if state.mode not in {
            RecoveryMode.SYNCING,
            RecoveryMode.ENABLING_RECOVERY,
            RecoveryMode.FAILING_BACK,
        }:
            raise RecoveryBlocked("Business reconciliation is not allowed while serving or parking")
        if not request.previous_controller_stopped:
            raise RecoveryBlocked("Stop and fence the previous controller before resuming its operation")
        operation = next(
            (row for row in self.catalog.operations() if row.operation_id == request.operation_id),
            None,
        )
        if operation is not None and operation.state == OperationState.SUCCEEDED:
            return self._reconcile_saved_result(operation, request)
        if (
            operation is None
            or operation.kind != "item-apply"
            or operation.source is None
            or operation.generation_id is None
            or operation.service_operation_id is None
        ):
            raise RecoveryBlocked(
                "This operation lacks a reconcilable item service receipt. Inspect its exact request/audit "
                "evidence manually; a same-name target is not ownership proof."
            )
        service_state = self.destination.get(f"operations/{operation.service_operation_id}")
        if service_state.get("status") != "Succeeded":
            raise RecoveryBlocked(
                safe_text(
                    f"Service operation {operation.service_operation_id} is {service_state.get('status')}; "
                    f"{service_state.get('error')}. Settle or inspect it before resuming metadata."
                )
            )
        receipt = self.destination.get(f"operations/{operation.service_operation_id}/result")
        generation = self.catalog.load_generation(operation.generation_id)
        item = next(row for row in generation.snapshot.items if row.identity == operation.source)
        namespace = "workspaces"
        if generation.snapshot.capture_kind == "recovery":
            plans = [
                row
                for row in self.catalog.list_records("failback")
                if row.document["dr_generation_id"] == generation.snapshot.generation_id
            ]
            if len(plans) != 1:
                raise RecoveryBlocked("Resolve the unique linked return plan before reconciling its target")
            namespace = f"return-{plans[0].key}"
        mapping = self.runtime.get(namespace, "/".join(operation.source.key.split("/")[:2]))
        receipt_id = receipt.get("id") or (operation.target.item_id if operation.target else None)
        if mapping is None or not receipt_id:
            raise RecoveryBlocked("The service receipt has no exact target ID and owned workspace placement")
        workspace = WorkspaceIdentity.model_validate(mapping["target"])
        target = ItemIdentity(
            tenant_id=workspace.tenant_id,
            workspace_id=workspace.workspace_id,
            item_id=receipt_id,
        )
        if (
            receipt.get("workspaceId", workspace.workspace_id) != workspace.workspace_id
            or receipt.get("type", item.item_type) != item.item_type
            or (operation.target is not None and operation.target != target)
        ):
            raise RecoveryBlocked("The service receipt disagrees with the committed operation target")
        item_document(self.destination, target, item.item_type)
        self.runtime.lease = self.catalog.takeover_controller(
            self.runtime.controller_id,
            expected_controller_id=request.expected_controller_id,
            expected_epoch=request.expected_epoch,
            fencing_evidence=request.fencing_evidence,
            operation_id=str(uuid4()),
        )
        self.runtime.mode = state.mode
        observed = operation.model_copy(
            update={
                "target": target,
                "state": OperationState.RUNNING,
                "recorded_at": now(),
                "message": "Exact Fabric operation result reconciled under explicit controller fencing",
            }
        )
        self.catalog.record_operation(self.runtime.require_lease(), observed)
        self.runtime.put("reconciliation-evidence", operation.operation_id, request.model_dump(mode="json"))
        source_ids = {row.identity.key for row in generation.snapshot.items}
        applied = {key: row for key, row in self.item_mappings().items() if key in source_ids}
        source_workspaces = {row.identity.key for row in generation.snapshot.workspaces}
        workspaces = {
            row.key: WorkspaceIdentity.model_validate(row.document["target"])
            for row in self.catalog.list_records(namespace)
            if row.key in source_workspaces
        }

        def finish():
            result = self.apply(
                self.destination,
                item,
                generation.payloads,
                generation_id=operation.generation_id,
                operation_id=operation.operation_id,
                target_workspace=workspace,
                source_items=generation.snapshot.items,
                target_id=target.item_id,
                tokens=self.tokens,
                item_mappings={item_key(row.source): row.target for row in applied.values()},
                workspace_mappings={tuple(key.split("/")): value for key, value in workspaces.items()},
                endpoint_mappings=self._endpoint_pairs(generation, applied),
                mutation_guard=self.runtime.fence,
                destination_quiescence=request.target_quiescence_evidence,
            )
            if result.applied is None:
                raise RecoveryBlocked("; ".join(result.diagnostics))
            return {
                "applied": result.applied.model_dump(mode="json"),
                "metadata_applied": result.metadata_applied,
                "deferred_grants": list(result.deferred_grants),
            }

        result = self.runtime.resume_effect(observed, finish)
        row = AppliedItem.model_validate(result["applied"])
        self.catalog.record_applied(self.runtime.require_lease(), row)
        self.runtime.put(
            "item-security",
            item.identity.key,
            {
                "generation_id": operation.generation_id,
                "target": target.model_dump(mode="json"),
                "metadata_applied": result["metadata_applied"],
                "deferred_grants": result["deferred_grants"],
            },
        )
        if not self.catalog.pending_operations():
            self.catalog.release_controller(self.runtime.require_lease())
            self.runtime.lease = None
        return ServiceResult(
            mode=state.mode,
            generation_id=operation.generation_id,
            details={"reconciled_operation": operation.operation_id, "applied": row.model_dump(mode="json")},
            warnings=(
                "The exact returned item was reconciled without recreating it; resume the pinned plan.",
            ),
        )

    def _reconcile_saved_result(self, operation, request) -> ServiceResult:
        result = json.loads(operation.message or "{}")
        if operation.kind != "item-apply" or not result.get("applied"):
            raise RecoveryBlocked("This settled operation has no applied-item receipt to reconcile")
        applied = AppliedItem.model_validate(result["applied"])
        generation = self.catalog.load_generation(applied.capture_generation_id)
        if (
            generation.snapshot.capture_kind == "source"
            and self.catalog.state().current_generation_id != applied.capture_generation_id
        ):
            raise RecoveryBlocked("Do not restore a superseded operation over the current capture generation")
        item = next(row for row in generation.snapshot.items if row.identity == applied.source)
        if self.observe(self.destination, applied.target, item.item_type) != applied.target_observed_sha256:
            raise RecoveryBlocked(
                "The recorded successful target has drifted; inspect it before reconciliation"
            )
        self.runtime.lease = self.catalog.takeover_controller(
            self.runtime.controller_id,
            expected_controller_id=request.expected_controller_id,
            expected_epoch=request.expected_epoch,
            fencing_evidence=request.fencing_evidence,
            operation_id=str(uuid4()),
        )
        self.runtime.mode = self.catalog.state().mode
        self.catalog.record_applied(self.runtime.require_lease(), applied)
        self.runtime.put(
            "item-security",
            applied.source.key,
            {
                "generation_id": applied.capture_generation_id,
                "target": applied.target.model_dump(mode="json"),
                "metadata_applied": result["metadata_applied"],
                "deferred_grants": result["deferred_grants"],
            },
        )
        if not self.catalog.pending_operations():
            self.catalog.release_controller(self.runtime.require_lease())
            self.runtime.lease = None
        return ServiceResult(
            mode=self.runtime.mode,
            generation_id=applied.capture_generation_id,
            details={
                "reconciled_operation": operation.operation_id,
                "applied": applied.model_dump(mode="json"),
            },
        )
