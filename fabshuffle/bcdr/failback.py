"""Linked fresh-target return plans; restoration alone never completes failback."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

from fabshuffle.bcdr.backend import RecoveryBlocked, now
from fabshuffle.bcdr.contracts import RecoveryMode, RecoverySet, WorkspaceIdentity
from fabshuffle.bcdr.service import (
    CapacityRoute,
    CutbackRequest,
    FailbackExecuteRequest,
    FailbackRequest,
    PlanRequest,
    RearmRequest,
    ServiceResult,
)
from fabshuffle.fabric.workspaces import get_capacity

if TYPE_CHECKING:
    from fabshuffle.bcdr.coordinator import RecoveryCoordinator


def required_reconciliation(item_type: str) -> set[str]:
    if item_type == "CosmosDBDatabase":
        return {"inserts", "updates", "deletes", "ttl", "schema"}
    if item_type in {"Eventhouse", "KQLDatabase", "Eventstream"}:
        return {"inserts", "updates", "deletes", "schema", "offsets"}
    return {"inserts", "updates", "deletes", "schema"}


class FailbackController:
    def __init__(self, coordinator: RecoveryCoordinator) -> None:
        self.coordinator = coordinator
        self.runtime = coordinator.runtime

    def _return_coordinator(
        self,
        plan_id: str,
        routes: tuple[CapacityRoute, ...] | None = None,
    ) -> RecoveryCoordinator:
        from fabshuffle.bcdr.coordinator import RecoveryCoordinator

        original = self.coordinator
        config = original.recovery_set
        if routes is None:
            record = self._record(plan_id)
            routes = PlanRequest.model_validate(record["request"]).capacity_routes
        reverse_config = RecoverySet.model_validate(
            {
                **config.model_dump(),
                "source_capacity_ids": tuple(sorted({route.source_capacity_id for route in routes})),
                "target_capacity_ids": tuple(sorted({route.target_capacity_id for route in routes})),
            }
        )
        coordinator = RecoveryCoordinator(
            reverse_config,
            original.catalog,
            original.destination.client,
            original.capacities,
            tokens=original.tokens,
            data_recovery=original.data_recovery,
            capture=original.capture,
            apply=original.apply,
            observe=original.observe,
            capabilities=original.capabilities,
            workspace_namespace=f"return-{plan_id}",
        )
        coordinator.runtime = original.runtime
        coordinator.destination.runtime = original.runtime
        coordinator.access.runtime = original.runtime
        coordinator.replica.runtime = original.runtime
        return coordinator

    def plan(self, request: FailbackRequest) -> ServiceResult:
        c = self.coordinator
        c._wake()
        with self.runtime.controller({RecoveryMode.ACTIVE_RECOVERY}):
            groups = c._selected_groups(request.generation_id, request.group_ids)
            if not all(group.active for group in groups):
                raise RecoveryBlocked("Select only currently active recovery groups for a linked return plan")
            if not request.primary_available or c.source is None:
                raise RecoveryBlocked(
                    "Confirm primary availability with evidence and provide primary credentials"
                )
            applied = c.item_mappings()
            targets = tuple(applied[item.key].target for group in groups for item in group.items)
            failover = c.catalog.load_generation(request.generation_id)
            selected_workspaces = {
                "/".join(item.key.split("/")[:2]) for group in groups for item in group.items
            }
            selected_primary_capacities = {
                row.capacity_id
                for row in failover.snapshot.workspaces
                if row.identity.key in selected_workspaces
            }
            observations = []
            for identifier in sorted(selected_primary_capacities):
                capacity = get_capacity(c.source, identifier)
                if capacity.get("state") != "Active":
                    raise RecoveryBlocked(
                        f"Resume and verify original capacity {identifier} before planning failback"
                    )
                observations.append({"capacity_id": identifier, "state": capacity["state"]})
            stored_request = self.runtime.get("plans", request.generation_id)
            if stored_request is None:
                raise RecoveryBlocked("The failover generation has no persisted capacity placement plan")
            forward = PlanRequest.model_validate(
                {key: value for key, value in stored_request.items() if key not in {"capture", "park"}}
            )
            capacity_destinations = {}
            for route in forward.capacity_routes:
                if route.source_capacity_id not in selected_primary_capacities:
                    continue
                if (
                    route.target_capacity_id in capacity_destinations
                    and capacity_destinations[route.target_capacity_id] != route.source_capacity_id
                ):
                    raise RecoveryBlocked(
                        "Selected original capacities share recovery compute; approve an explicit "
                        "per-workspace return plan rather than guessing a reverse capacity map"
                    )
                capacity_destinations[route.target_capacity_id] = route.source_capacity_id
            routes = tuple(
                CapacityRoute(
                    source_capacity_id=source,
                    target_capacity_id=target,
                )
                for source, target in capacity_destinations.items()
            )
            if not routes:
                raise RecoveryBlocked("The selected groups have no durable business capacity placement")
            plan_id = str(uuid4())
            return_coordinator = self._return_coordinator(plan_id, routes)
            captured = c.capture(
                c.destination,
                return_coordinator.recovery_set,
                tokens=c.tokens,
                workspace_ids=sorted({target.workspace_id for target in targets}),
                parent_generation_id=request.generation_id,
            )
            captured = type(captured)(
                captured.snapshot.model_copy(update={"capture_kind": "recovery"}), captured.payloads
            )
            return_request = PlanRequest(
                generation_id=captured.snapshot.generation_id,
                capacity_routes=routes,
                suffix=f" - return {plan_id[:8]}",
                connection_mappings=request.return_connection_mappings,
                target_quiescence_evidence=request.target_quiescence_evidence,
            )
            plan = return_coordinator._plan(captured, return_request)
            self.runtime.transition(RecoveryMode.FAILING_BACK)
            # Validate placement before the mode transition; publish DR separately from source authority.
            c.catalog.stage_failback_generation(
                self.runtime.require_lease(), captured.snapshot, captured.payloads
            )
            c.catalog.publish_failback_generation(
                self.runtime.require_lease(),
                captured.snapshot.generation_id,
                failover_generation_id=request.generation_id,
            )
            self.runtime.put("plans", captured.snapshot.generation_id, return_request.model_dump(mode="json"))
            for route in request.return_connection_mappings:
                self.runtime.put(
                    "connections",
                    route.source.key,
                    {
                        "generation_id": captured.snapshot.generation_id,
                        "target": route.target.model_dump(mode="json"),
                        "evidence": route.evidence,
                    },
                )
            self.runtime.put(
                "failback",
                plan_id,
                {
                    "failover_generation_id": request.generation_id,
                    "dr_generation_id": captured.snapshot.generation_id,
                    "groups": list(request.group_ids),
                    "request": return_request.model_dump(mode="json"),
                    "primary_observations": observations,
                    "primary_evidence": request.primary_evidence,
                    "captured_targets": [row.model_dump(mode="json") for row in targets],
                    "state": "planned",
                    "created_at": now().isoformat(),
                    "rollback_retained": True,
                },
            )
            return ServiceResult(
                mode=self.runtime.mode,
                generation_id=request.generation_id,
                plan_id=plan_id,
                details={
                    "return_generation_id": captured.snapshot.generation_id,
                    "return_placements": [
                        {"source": list(row.source), "target_capacity": list(row.target_capacity)}
                        for row in plan.placements
                    ],
                },
                warnings=(
                    "DR state captured. Fence DR writers before creating fresh return targets. "
                    "Original items and all DR rollback resources will be retained.",
                ),
            )

    def _record(self, plan_id: str) -> dict:
        record = self.runtime.get("failback", plan_id)
        if record is None:
            raise RecoveryBlocked("Select a recorded failback plan; reverse suffixes or IDs are not a plan")
        return record

    def execute(self, request: FailbackExecuteRequest) -> ServiceResult:
        from fabshuffle.bcdr.coordinator import needs_data

        c = self.coordinator
        c._wake()
        with self.runtime.controller({RecoveryMode.FAILING_BACK}):
            record = self._record(request.plan_id)
            if record["state"] not in {"planned", "reconciliation_required", "validated"}:
                raise RecoveryBlocked("This return plan has already cut back; it cannot be replayed")
            writer = self.runtime.get("lifecycle", "writer")
            if writer is None:
                raise RecoveryBlocked("The active recovery writer epoch is missing; reconcile ownership")
            request.writer_fence.require_current(now(), "recovery", writer["epoch"])
            self.runtime.put(
                "lifecycle",
                "writer",
                {
                    **writer,
                    "side": "fenced_recovery",
                    "fence": request.writer_fence.model_dump(mode="json"),
                },
            )
            generation = c.catalog.load_generation(record["dr_generation_id"])
            returned = self._return_coordinator(request.plan_id)
            plan_request = PlanRequest.model_validate(record["request"])
            plan = returned._plan(generation, plan_request)
            groups, warnings = returned._apply_plan(generation, plan, plan_request.suffix)
            evidence = {row.source.key: row for row in request.reconciliation}
            if len(evidence) != len(request.reconciliation):
                raise RecoveryBlocked("Reconciliation evidence names the same DR item more than once")
            mappings = returned.item_mappings()
            blockers = []
            for item in generation.snapshot.items:
                if not needs_data(item) or item.tombstone:
                    continue
                proof = evidence.get(item.identity.key)
                target = mappings.get(item.identity.key)
                if (
                    proof is None
                    or target is None
                    or proof.return_target != target.target
                    or not proof.conflicts_resolved
                    or not proof.observed_at <= now() < proof.valid_until
                    or proof.observed_at < target.applied_at
                    or not required_reconciliation(item.item_type) <= set(proof.capabilities)
                ):
                    blockers.append(
                        f"Reconcile {', '.join(sorted(required_reconciliation(item.item_type)))} for "
                        f"'{item.display_name}' into its fresh return target; provide current "
                        "provider/operator "
                        "evidence and resolve conflicting writers. No automatic merge is qualified."
                    )
            ready = not blockers and all(group.metadata_applied for group in groups)
            self.runtime.put(
                "failback",
                request.plan_id,
                {
                    **record,
                    "state": "validated" if ready else "reconciliation_required",
                    "reconciliation": [row.model_dump(mode="json") for row in request.reconciliation],
                    "writer_fence": request.writer_fence.model_dump(mode="json"),
                    "return_groups": [row.group_id for row in groups],
                },
            )
            return ServiceResult(
                mode=self.runtime.mode,
                plan_id=request.plan_id,
                generation_id=record["failover_generation_id"],
                groups=groups,
                outcome="succeeded" if ready else "blocked",
                warnings=(*warnings, *blockers),
                details={
                    "return_generation_id": record["dr_generation_id"],
                    "readiness_context": c.readiness_context(record["dr_generation_id"]),
                    "writer": self.runtime.get("lifecycle", "writer"),
                    "return_targets": [
                        row.model_dump(mode="json")
                        for key, row in mappings.items()
                        if key in {item.identity.key for item in generation.snapshot.items}
                    ],
                    "rollback_retained": True,
                },
            )

    def cutback(self, request: CutbackRequest) -> ServiceResult:
        c = self.coordinator
        c._wake()
        with self.runtime.controller({RecoveryMode.FAILING_BACK}):
            record = self._record(request.plan_id)
            if record["state"] != "validated":
                raise RecoveryBlocked("Complete data reconciliation into fresh return targets before cutback")
            writer = self.runtime.get("lifecycle", "writer")
            request.writer_fence.require_current(now(), "recovery", writer["epoch"])
            generation = c.catalog.load_generation(record["dr_generation_id"])
            groups = c._selected_groups(record["dr_generation_id"], record["return_groups"])
            c.validate_readiness(generation, groups, request.readiness, security=True)
            self.runtime.put(
                "lifecycle",
                "writer",
                {
                    "epoch": writer["epoch"] + 1,
                    "side": "primary_return",
                    "plan_id": request.plan_id,
                    "fence": request.writer_fence.model_dump(mode="json"),
                },
            )
            self.runtime.put(
                "failback",
                request.plan_id,
                {
                    **record,
                    "state": "cutback",
                    "readiness": [row.model_dump(mode="json") for row in request.readiness],
                },
            )
            return ServiceResult(
                mode=self.runtime.mode,
                plan_id=request.plan_id,
                generation_id=record["failover_generation_id"],
                warnings=(
                    "Return targets validated for consumer cutback. DR is retained; "
                    "explicitly rearm after sign-off.",
                ),
            )

    def rearm(self, request: RearmRequest) -> ServiceResult:
        c = self.coordinator
        c._wake()
        with self.runtime.controller({RecoveryMode.FAILING_BACK, RecoveryMode.REARMING}):
            record = self._record(request.plan_id)
            if record["state"] != "cutback" or not request.approve or not request.recovery_no_longer_serving:
                raise RecoveryBlocked(
                    "Confirm successful consumer cutback and that DR no longer serves production"
                )
            if self.runtime.mode == RecoveryMode.FAILING_BACK:
                self.runtime.transition(RecoveryMode.REARMING)
            c.access.rearm()
            allowed = {grant.principal.key for grant in c.recovery_set.access_policy.workspace_grants()}
            generation = c.catalog.load_generation(record["failover_generation_id"])
            items = {row.identity.key: row for row in generation.snapshot.items}
            mappings = c.item_mappings()
            for workspace in c.workspace_mappings().values():
                c.access.restrict_workspace(workspace)
                c.access.fabric.inspect_items(
                    [
                        (row.target, items[key].item_type)
                        for key, row in mappings.items()
                        if key in items and row.target.workspace_id == workspace.workspace_id
                    ],
                    allowed,
                )
            c.access.restrict_workspace(c.recovery_set.control_workspace)
            for group in c._groups(record["failover_generation_id"]):
                c._save_group(
                    record["failover_generation_id"],
                    group.model_copy(
                        update={
                            "active": False,
                            "access_enabled": False,
                            "ready_for_cutover": False,
                            "data_ready": False,
                        }
                    ),
                )
            # Fresh return identities are authoritative; don't resume capturing obsolete original item IDs.
            return_generation = c.catalog.load_generation(record["dr_generation_id"])
            return_sources = {row.identity.key for row in return_generation.snapshot.items}
            previous_authority = self.runtime.get("lifecycle", "authority")
            authority = {
                row.key: row
                for row in (
                    tuple(
                        WorkspaceIdentity.model_validate(value) for value in previous_authority["workspaces"]
                    )
                    if previous_authority
                    else tuple(row.identity for row in generation.snapshot.workspaces)
                )
            }
            selected_source_workspaces = {
                "/".join(identity.key.split("/")[:2])
                for group in c._selected_groups(record["failover_generation_id"], record["groups"])
                for identity in group.items
            }
            for source_workspace in selected_source_workspaces:
                authority.pop(source_workspace, None)
            for row in c.catalog.list_records(f"return-{request.plan_id}"):
                fresh = WorkspaceIdentity.model_validate(row.document["target"])
                authority[fresh.key] = fresh
                dr_tenant, dr_workspace = row.key.split("/")
                self.runtime.put(
                    "workspaces",
                    fresh.key,
                    {
                        "target": WorkspaceIdentity(
                            tenant_id=dr_tenant,
                            workspace_id=dr_workspace,
                        ).model_dump(mode="json"),
                        "capacity_id": next(
                            workspace.capacity_id
                            for workspace in return_generation.snapshot.workspaces
                            if workspace.identity.key == row.key
                        ),
                    },
                )
            for returned in mappings.values():
                if returned.source.key not in return_sources:
                    continue
                original = next(
                    (
                        row
                        for row in mappings.values()
                        if row.target == returned.source
                        and row.source.key in items
                        and not items[row.source.key].tombstone
                    ),
                    None,
                )
                if original is None:
                    raise RecoveryBlocked("A return target has no original DR ownership chain; reconcile it")
                self.runtime.put(
                    "rearmed-items",
                    returned.target.key,
                    {
                        "target": returned.source.model_dump(mode="json"),
                        "ownership_operation": original.operation_id,
                        "observed_sha256": c.observe(
                            c.destination,
                            returned.source,
                            items[original.source.key].item_type,
                        ),
                    },
                )
            self.runtime.put(
                "lifecycle",
                "authority",
                {
                    "plan_id": request.plan_id,
                    "source": "fresh_return_targets",
                    "workspaces": [row.model_dump(mode="json") for row in authority.values()],
                    "evidence": request.evidence,
                },
            )
            writer = self.runtime.get("lifecycle", "writer")
            self.runtime.put("lifecycle", "writer", {**writer, "side": "primary"})
            self.runtime.put("failback", request.plan_id, {**record, "state": "rearmed"})
            self.runtime.transition(RecoveryMode.STANDBY)
            if request.park:
                c.capacities.park(self.runtime, tuple(c.workspace_mappings().values()))
            return ServiceResult(
                mode=self.runtime.mode,
                plan_id=request.plan_id,
                generation_id=record["failover_generation_id"],
                details={"rollback_retained": True},
            )
