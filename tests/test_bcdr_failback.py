import pytest

from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.catalog import CapturedGeneration
from fabshuffle.bcdr.contracts import RecoveryMode, WorkspaceIdentity
from fabshuffle.bcdr.failback import required_reconciliation
from fabshuffle.bcdr.service import (
    CutbackRequest,
    CutoverRequest,
    FailbackExecuteRequest,
    FailbackRequest,
    RearmRequest,
)
from tests.test_bcdr_contracts import guid
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
            "capacity_id": system.config.target_capacity_ids[0],
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
