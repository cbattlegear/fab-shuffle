import pytest

from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.catalog import CapturedGeneration
from fabshuffle.bcdr.contracts import RecoveryMode, WorkspaceIdentity
from fabshuffle.bcdr.failback import required_reconciliation
from fabshuffle.bcdr.service import (
    CutbackRequest,
    CutoverRequest,
    EnableRecoveryRequest,
    FailbackExecuteRequest,
    FailbackRequest,
    RearmRequest,
)
from tests.test_bcdr_contracts import guid, snapshot
from tests.test_bcdr_coordinator import enable, fence, proofs
from tests.test_bcdr_coordinator import system as system


def active(system):
    enabled = enable(system)
    return system.service.cutover(
        CutoverRequest(
            generation_id=enabled.generation_id,
            group_ids=(enabled.groups[0].group_id,),
            readiness=proofs(system, enabled.generation_id),
            writer_fence=fence(),
        )
    )


def capture_dr(system):
    original = system.captured
    row = system.catalog.applied_items()[0]
    workspace = original.workspaces[0].model_copy(
        update={
            "identity": WorkspaceIdentity(
                tenant_id=row.target.tenant_id, workspace_id=row.target.workspace_id
            ),
            "capacity_id": system.config.target_capacity_ids[-1],
        }
    )
    item = original.items[0].model_copy(update={"identity": row.target})

    def capture(*args, **kwargs):
        return CapturedGeneration(
            original.model_copy(
                update={
                    "generation_id": guid(),
                    "parent_generation_id": kwargs.get("parent_generation_id"),
                    "workspaces": (workspace,),
                    "items": (item,),
                }
            ),
            (),
        )

    system.c.capture = capture


def test_return_plan_observes_primary_and_retains_failover_generation(system):
    running = active(system)
    capture_dr(system)
    result = system.service.plan_failback(
        FailbackRequest(
            generation_id=running.generation_id,
            group_ids=(running.groups[0].group_id,),
            primary_available=True,
            primary_evidence="primary-capacity-observed",
        )
    )
    assert result.plan_id
    assert system.catalog.state().current_generation_id == running.generation_id
    assert (
        system.catalog.load_generation(result.details["return_generation_id"]).snapshot.capture_kind
        == "recovery"
    )
    assert any(path.startswith("capacities/") for _, path in system.estate.calls)
    assert not any(method == "DELETE" for method, _ in system.estate.calls)


def test_failback_requires_current_external_writer_fence(system):
    running = active(system)
    capture_dr(system)
    planned = system.service.plan_failback(
        FailbackRequest(
            generation_id=running.generation_id,
            group_ids=(running.groups[0].group_id,),
            primary_available=True,
            primary_evidence="primary available",
        )
    )
    with pytest.raises(ValueError, match="Fence every external writer"):
        system.service.execute_failback(FailbackExecuteRequest(plan_id=planned.plan_id, writer_fence=fence()))
    assert len(system.estate.items) == 1


def test_metadata_return_creates_fresh_targets_then_requires_cutback_and_rearm(system):
    running = active(system)
    capture_dr(system)
    planned = system.service.plan_failback(
        FailbackRequest(
            generation_id=running.generation_id,
            group_ids=(running.groups[0].group_id,),
            primary_available=True,
            primary_evidence="primary available",
        )
    )
    result = system.service.execute_failback(
        FailbackExecuteRequest(
            plan_id=planned.plan_id,
            writer_fence=fence("recovery", 1),
        )
    )
    assert result.exit_code == 0
    assert len(system.estate.items) == 2
    original = system.captured.items[0].identity
    targets = [row.target for row in system.catalog.applied_items()]
    assert original not in targets
    assert system.catalog.state().mode == RecoveryMode.FAILING_BACK
    with pytest.raises(RecoveryBlocked, match="cutback"):
        system.service.rearm(
            RearmRequest(
                plan_id=planned.plan_id,
                approve=True,
                recovery_no_longer_serving=True,
                evidence="operator",
                park=False,
            )
        )
    return_id = planned.details["return_generation_id"]
    system.service.cutback(
        CutbackRequest(
            plan_id=planned.plan_id,
            readiness=proofs(system, return_id),
            writer_fence=fence("recovery", 1),
        )
    )
    rearmed = system.service.rearm(
        RearmRequest(
            plan_id=planned.plan_id,
            approve=True,
            recovery_no_longer_serving=True,
            evidence="sign-off",
            park=False,
        )
    )
    assert rearmed.mode == RecoveryMode.STANDBY
    assert len(system.estate.items) == 2
    assert not any(method == "DELETE" and "/items" in path for method, path in system.estate.calls)
    assert system.c.runtime.get("lifecycle", "authority")["source"] == "fresh_return_targets"
    fresh = next(
        row.target for row in system.catalog.applied_items() if row.capture_generation_id == return_id
    )
    source_workspace = system.captured.workspaces[0].model_copy(
        update={
            "identity": WorkspaceIdentity(tenant_id=fresh.tenant_id, workspace_id=fresh.workspace_id),
        }
    )
    source_item = system.captured.items[0].model_copy(update={"identity": fresh})

    def returned_source(*args, **kwargs):
        assert kwargs["workspace_ids"] == [fresh.workspace_id]
        return CapturedGeneration(
            system.captured.model_copy(
                update={
                    "generation_id": guid(),
                    "parent_generation_id": kwargs["parent_generation_id"],
                    "workspaces": (source_workspace,),
                    "items": (source_item,),
                }
            ),
            (),
        )

    system.c.capture = returned_source
    resynced = system.service.synchronize(system.request)
    assert all(group.metadata_applied for group in resynced.groups)
    assert len(system.estate.items) == 2
    retained = next(row for row in system.catalog.applied_items() if row.source == fresh)
    assert retained.target != fresh
    assert retained.target in targets
    enabled_again = system.service.enable_recovery(
        EnableRecoveryRequest(
            generation_id=resynced.generation_id,
            group_ids=tuple(row.group_id for row in resynced.groups),
            readiness=proofs(system, resynced.generation_id),
        )
    )
    active_again = system.service.cutover(
        CutoverRequest(
            generation_id=resynced.generation_id,
            group_ids=tuple(row.group_id for row in enabled_again.groups),
            readiness=proofs(system, resynced.generation_id),
            writer_fence=fence(epoch=2),
        )
    )
    capture_dr(system)
    second_plan = system.service.plan_failback(
        FailbackRequest(
            generation_id=active_again.generation_id,
            group_ids=tuple(row.group_id for row in active_again.groups),
            primary_available=True,
            primary_evidence="primary available again",
        )
    )
    second_return = system.service.execute_failback(
        FailbackExecuteRequest(
            plan_id=second_plan.plan_id,
            writer_fence=fence("recovery", 3),
        )
    )
    assert second_return.exit_code == 0
    assert len(system.estate.items) == 3
    assert all(row["target"]["item_id"] != fresh.item_id for row in second_return.details["return_targets"])


def test_failed_primary_availability_does_not_enter_failback(system):
    running = active(system)
    with pytest.raises(RecoveryBlocked, match="Confirm primary"):
        system.service.plan_failback(
            FailbackRequest(
                generation_id=running.generation_id,
                group_ids=(running.groups[0].group_id,),
                primary_available=False,
                primary_evidence="primary still down",
            )
        )
    assert system.catalog.state().mode == RecoveryMode.ACTIVE_RECOVERY


@pytest.mark.parametrize("system", [{"catalog_separate": True}], indirect=True)
def test_failback_does_not_treat_catalog_only_capacity_as_business_source(system):
    running = active(system)
    capture_dr(system)
    result = system.service.plan_failback(
        FailbackRequest(
            generation_id=running.generation_id,
            group_ids=(running.groups[0].group_id,),
            primary_available=True,
            primary_evidence="primary observed",
        )
    )
    assert result.plan_id
    record = system.c.runtime.get("failback", result.plan_id)
    routes = record["request"]["capacity_routes"]
    assert [row["source_capacity_id"] for row in routes] == [system.config.target_capacity_ids[-1]]
    assert system.config.target_capacity_ids[0] not in str(result.details["return_placements"])


@pytest.mark.parametrize("system", [{"catalog_separate": True}], indirect=True)
def test_invalid_return_plan_keeps_active_mode_and_can_retry(system):
    from fabshuffle.bcdr.contracts import ConnectionIdentity
    from fabshuffle.bcdr.planner import PlanConfigurationError
    from fabshuffle.bcdr.service import ConnectionRoute

    running = active(system)
    capture_dr(system)
    connection = ConnectionIdentity(tenant_id=system.config.tenant_id, connection_id=guid())
    request = FailbackRequest(
        generation_id=running.generation_id,
        group_ids=(running.groups[0].group_id,),
        primary_available=True,
        primary_evidence="primary available",
        return_connection_mappings=(
            ConnectionRoute(source=connection, target=connection, evidence="stale approval"),
        ),
    )
    with pytest.raises(PlanConfigurationError, match="Unknown mapping source"):
        system.service.plan_failback(request)
    assert system.catalog.state().mode == RecoveryMode.ACTIVE_RECOVERY
    assert not system.catalog.list_records("failback")
    assert system.service.plan_failback(request.model_copy(update={"return_connection_mappings": ()})).plan_id


def test_partial_failback_preserves_unaffected_source_authority(system):
    second = snapshot(system.config)
    original = system.captured
    combined = original.model_copy(
        update={
            "workspaces": (*original.workspaces, *second.workspaces),
            "items": (*original.items, *second.items),
        }
    )
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(combined, ())
    synced = system.service.synchronize(system.request)
    selected = next(group for group in synced.groups if original.items[0].identity in group.items)
    enabled = system.service.enable_recovery(
        EnableRecoveryRequest(
            generation_id=synced.generation_id,
            group_ids=(selected.group_id,),
            readiness=proofs(system, synced.generation_id),
        )
    )
    system.service.cutover(
        CutoverRequest(
            generation_id=synced.generation_id,
            group_ids=(selected.group_id,),
            readiness=proofs(system, synced.generation_id),
            writer_fence=fence(),
        )
    )
    dr = system.c.item_mappings()[original.items[0].identity.key].target
    dr_workspace = original.workspaces[0].model_copy(
        update={
            "identity": WorkspaceIdentity(tenant_id=dr.tenant_id, workspace_id=dr.workspace_id),
            "capacity_id": system.request.capacity_routes[0].target_capacity_id,
        }
    )
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(
        original.model_copy(
            update={
                "generation_id": guid(),
                "parent_generation_id": kwargs.get("parent_generation_id"),
                "workspaces": (dr_workspace,),
                "items": (original.items[0].model_copy(update={"identity": dr}),),
            }
        ),
        (),
    )
    planned = system.service.plan_failback(
        FailbackRequest(
            generation_id=synced.generation_id,
            group_ids=(selected.group_id,),
            primary_available=True,
            primary_evidence="primary available",
        )
    )
    system.service.execute_failback(
        FailbackExecuteRequest(
            plan_id=planned.plan_id,
            writer_fence=fence("recovery", 1),
        )
    )
    return_id = planned.details["return_generation_id"]
    system.service.cutback(
        CutbackRequest(
            plan_id=planned.plan_id,
            readiness=proofs(system, return_id),
            writer_fence=fence("recovery", 1),
        )
    )
    system.service.rearm(
        RearmRequest(
            plan_id=planned.plan_id,
            approve=True,
            recovery_no_longer_serving=True,
            evidence="cutback done",
            park=False,
        )
    )
    fresh = system.c.item_mappings()[dr.key].target
    authority = system.c.runtime.get("lifecycle", "authority")
    assert {row["workspace_id"] for row in authority["workspaces"]} == {
        fresh.workspace_id,
        second.workspaces[0].identity.workspace_id,
    }
    returned_item = original.items[0].model_copy(update={"identity": fresh})
    returned_workspace = original.workspaces[0].model_copy(
        update={
            "identity": WorkspaceIdentity(tenant_id=fresh.tenant_id, workspace_id=fresh.workspace_id),
        }
    )
    expected_ids = {fresh.workspace_id, second.workspaces[0].identity.workspace_id}

    def recapture(*args, **kwargs):
        assert set(kwargs["workspace_ids"]) == expected_ids
        return CapturedGeneration(
            combined.model_copy(
                update={
                    "generation_id": guid(),
                    "parent_generation_id": kwargs["parent_generation_id"],
                    "workspaces": (returned_workspace, *second.workspaces),
                    "items": (returned_item, *second.items),
                }
            ),
            (),
        )

    system.c.capture = recapture
    result = system.service.synchronize(
        system.request.model_copy(
            update={
                "include_workspace_ids": (second.workspaces[0].identity.workspace_id,),
            }
        )
    )
    current = system.catalog.load_generation(result.generation_id).snapshot
    assert not next(row for row in current.items if row.identity == second.items[0].identity).tombstone
    assert not next(row for row in current.items if row.identity == fresh).tombstone
    assert enabled.groups[0].access_enabled


def test_failback_pins_explicit_return_connections_without_inverting_forward_routes(system):
    from fabshuffle.bcdr.contracts import ConnectionIdentity, DependencyEdge
    from fabshuffle.bcdr.service import ConnectionRoute

    running = active(system)
    dr = system.c.item_mappings()[system.captured.items[0].identity.key].target
    source_connection = ConnectionIdentity(tenant_id=system.config.tenant_id, connection_id=guid())
    return_connection = ConnectionIdentity(tenant_id=system.config.tenant_id, connection_id=guid())
    row = system.captured.workspaces[0].model_copy(
        update={
            "identity": WorkspaceIdentity(tenant_id=dr.tenant_id, workspace_id=dr.workspace_id),
            "capacity_id": system.request.capacity_routes[0].target_capacity_id,
        }
    )
    item = system.captured.items[0].model_copy(update={"identity": dr})
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(
        system.captured.model_copy(
            update={
                "generation_id": guid(),
                "parent_generation_id": kwargs.get("parent_generation_id"),
                "workspaces": (row,),
                "items": (item,),
                "dependencies": (
                    DependencyEdge(
                        edge_id=guid(),
                        consumer=dr,
                        prerequisite=source_connection,
                        phase="bind",
                        provenance="DR definition",
                        detail="Explicit approved return connection required",
                    ),
                ),
            }
        ),
        (),
    )
    with pytest.raises(RecoveryBlocked, match="Resolve return metadata prerequisites"):
        system.service.plan_failback(
            FailbackRequest(
                generation_id=running.generation_id,
                group_ids=(running.groups[0].group_id,),
                primary_available=True,
                primary_evidence="primary available",
            )
        )
    assert system.catalog.state().mode == RecoveryMode.ACTIVE_RECOVERY
    assert system.catalog.state().current_generation_id == running.generation_id
    assert not system.catalog.list_records("failback")
    planned = system.service.plan_failback(
        FailbackRequest(
            generation_id=running.generation_id,
            group_ids=(running.groups[0].group_id,),
            primary_available=True,
            primary_evidence="primary available",
            return_connection_mappings=(
                ConnectionRoute(
                    source=source_connection,
                    target=return_connection,
                    evidence="return connection approved",
                ),
            ),
        )
    )
    result = system.service.execute_failback(
        FailbackExecuteRequest(
            plan_id=planned.plan_id,
            writer_fence=fence("recovery", 1),
        )
    )
    assert result.groups[0].metadata_applied
    saved = system.c.runtime.get("plans", planned.details["return_generation_id"])
    assert saved["connection_mappings"][0]["target"]["connection_id"] == return_connection.connection_id


@pytest.mark.parametrize(
    ("item_type", "capabilities"),
    [
        ("SQLDatabase", {"inserts", "updates", "deletes", "schema"}),
        ("CosmosDBDatabase", {"inserts", "updates", "deletes", "ttl", "schema"}),
        ("KQLDatabase", {"inserts", "updates", "deletes", "schema", "offsets"}),
    ],
)
def test_failback_requires_deletes_ttl_and_offsets_not_only_upserts(item_type, capabilities):
    assert required_reconciliation(item_type) == capabilities
