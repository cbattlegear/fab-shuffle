from __future__ import annotations

import pytest

from fabshuffle import orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric import powerbi
from fabshuffle.fabric.support import Strategy
from fabshuffle.run import MigrationRun, RunStatus

PRINCIPAL = ServicePrincipal("tenant", "client", "secret")


def make_plan(region: str = "westeurope") -> orchestrator.MigrationPlan:
    return orchestrator.MigrationPlan(
        capacity_id="cap-1",
        capacity_name="F64",
        capacity_region=region,
        source_workspace_id="ws-1",
        source_workspace_name="Sales",
        target_workspace_name="Sales",
        strategy=Strategy.REASSIGN,
    )


class FakePowerBi:
    """Stands in for PowerBiClient, recording every conversion in order."""

    def __init__(self, models: list[powerbi.SemanticModel], fail_on: str | None = None) -> None:
        self.models = models
        self.fail_on = fail_on
        self.conversions: list[tuple[str, str]] = []

    def __call__(self, _tokens) -> FakePowerBi:
        return self

    def __enter__(self) -> FakePowerBi:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def list_semantic_models(self, workspace_id: str) -> list[powerbi.SemanticModel]:
        return self.models

    def convert(self, workspace_id, model, storage_mode, *, on_progress=None):
        if self.fail_on == model.name and storage_mode == powerbi.SMALL:
            raise powerbi.PowerBiError(f"cannot shrink {model.name}")
        self.conversions.append((model.name, storage_mode))


def large(name: str) -> powerbi.SemanticModel:
    return powerbi.SemanticModel(id=name, name=name, storage_mode=powerbi.LARGE, content_provider="")


def small(name: str) -> powerbi.SemanticModel:
    return powerbi.SemanticModel(id=name, name=name, storage_mode=powerbi.SMALL, content_provider="")


@pytest.fixture
def assignments(monkeypatch) -> list[tuple[str, str]]:
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        orchestrator.workspaces,
        "assign_to_capacity",
        lambda client, workspace_id, capacity_id: calls.append((workspace_id, capacity_id)),
    )
    return calls


def run_reassign(monkeypatch, fake: FakePowerBi, plan=None) -> MigrationRun:
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", fake)
    monkeypatch.setattr(orchestrator, "FabricClient", lambda tokens: _NullClient())
    monkeypatch.setattr(orchestrator, "TokenProvider", lambda principal: object())

    run = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    orchestrator.run_migration(run, PRINCIPAL, plan or make_plan())
    return run


class _NullClient:
    def __enter__(self):
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


def test_small_models_reassign_without_any_conversion(monkeypatch, assignments):
    fake = FakePowerBi([small("A"), small("B")])
    run = run_reassign(monkeypatch, fake)

    assert run.status == RunStatus.SUCCEEDED
    assert fake.conversions == []
    assert assignments == [("ws-1", "cap-1")]
    # The workspace is moved in place, so it keeps its identity and name.
    assert run.target_workspace == {"id": "ws-1", "displayName": "Sales"}
    assert run.summary["strategy"] == "reassign"


def test_large_models_are_shrunk_then_restored_around_the_assignment(monkeypatch, assignments):
    fake = FakePowerBi([large("Big"), small("Little"), large("Huge")])
    run = run_reassign(monkeypatch, fake)

    assert run.status == RunStatus.SUCCEEDED
    assert fake.conversions == [
        ("Big", powerbi.SMALL),
        ("Huge", powerbi.SMALL),
        ("Big", powerbi.LARGE),
        ("Huge", powerbi.LARGE),
    ]
    assert assignments == [("ws-1", "cap-1")]


def test_a_model_that_cannot_shrink_rolls_back_and_never_assigns(monkeypatch, assignments):
    fake = FakePowerBi([large("Big"), large("Stubborn")], fail_on="Stubborn")
    run = run_reassign(monkeypatch, fake)

    assert run.status == RunStatus.FAILED
    assert assignments == []
    # 'Big' was already converted, so it must be put back on large storage.
    assert fake.conversions == [("Big", powerbi.SMALL), ("Big", powerbi.LARGE)]


def test_unsupported_target_region_blocks_the_move(monkeypatch, assignments):
    fake = FakePowerBi([large("Big")])
    run = run_reassign(monkeypatch, fake, plan=make_plan(region="nowhereland"))

    assert run.status == RunStatus.FAILED
    assert "does not support it" in run.error
    assert fake.conversions == []
    assert assignments == []


def test_non_convertible_model_blocks_the_move(monkeypatch, assignments):
    push = powerbi.SemanticModel(
        id="1", name="Streaming", storage_mode=powerbi.LARGE, content_provider="RealTimeInPushMode"
    )
    run = run_reassign(monkeypatch, FakePowerBi([push]))

    assert run.status == RunStatus.FAILED
    assert "cannot leave the large storage format" in run.error
    assert assignments == []


def test_failed_assignment_restores_large_storage(monkeypatch):
    fake = FakePowerBi([large("Big")])

    def explode(client, workspace_id, capacity_id):
        raise RuntimeError("capacity is full")

    monkeypatch.setattr(orchestrator.workspaces, "assign_to_capacity", explode)
    run = run_reassign(monkeypatch, fake)

    assert run.status == RunStatus.FAILED
    assert fake.conversions == [("Big", powerbi.SMALL), ("Big", powerbi.LARGE)]


def test_a_workspace_with_a_missing_capacity_explains_a_matching_assignment_failure(monkeypatch):
    """Fabric's error names the target capacity, which sends the operator to the wrong end;
    the source's missing capacity id is offered as a possibility, not asserted as the cause."""
    fake = FakePowerBi([small("A")])
    service_error = orchestrator.FabricApiError(
        "POST", "workspaces/ws-1/assignToCapacity", 400,
        '{"errorCode":"AssignWorkspaceToCapacityFailed",'
        '"message":"Workspace capacity assignment was failed"}',
    )

    def explode(client, workspace_id, capacity_id):
        raise service_error

    monkeypatch.setattr(orchestrator.workspaces, "assign_to_capacity", explode)
    plan = make_plan()
    plan.source_capacity_id_missing = True
    run = run_reassign(monkeypatch, fake, plan=plan)

    assert run.status == RunStatus.FAILED
    # What the service said is kept first, unchanged, and the interpretation is added
    # alongside it - phrased as a possibility, with no promise that restoring a capacity
    # will actually resolve it.
    assert "AssignWorkspaceToCapacityFailed" in run.error
    assert run.error.index("AssignWorkspaceToCapacityFailed") < run.error.index("names no capacity")
    assert "names no capacity" in run.error
    assert "has not been confirmed" in run.error
    assert "deleted" not in run.error.split("names no capacity")[0]


def test_an_ordinary_assignment_failure_is_not_blamed_on_a_missing_capacity(monkeypatch):
    fake = FakePowerBi([small("A")])

    def explode(client, workspace_id, capacity_id):
        raise RuntimeError("capacity is full")

    monkeypatch.setattr(orchestrator.workspaces, "assign_to_capacity", explode)
    plan = make_plan()
    plan.source_capacity_id_missing = True  # even with the hint set...
    run = run_reassign(monkeypatch, fake, plan=plan)

    assert run.status == RunStatus.FAILED
    assert "capacity is full" in run.error
    # ...a plain exception that is not the service's own AssignWorkspaceToCapacityFailed is
    # never reinterpreted as a missing-capacity condition.
    assert "names no capacity" not in run.error


def test_a_matching_error_code_is_not_misdiagnosed_without_the_missing_capacity_hint(monkeypatch):
    """The hint only ever adds context to a failure that already carries the exact service
    error; it is never volunteered when the source's own capacity was never missing."""
    fake = FakePowerBi([small("A")])
    service_error = orchestrator.FabricApiError(
        "POST", "workspaces/ws-1/assignToCapacity", 400,
        '{"errorCode":"AssignWorkspaceToCapacityFailed","message":"Workspace capacity '
        'assignment was failed"}',
    )

    def explode(client, workspace_id, capacity_id):
        raise service_error

    monkeypatch.setattr(orchestrator.workspaces, "assign_to_capacity", explode)
    run = run_reassign(monkeypatch, fake, plan=make_plan())

    assert run.status == RunStatus.FAILED
    assert "AssignWorkspaceToCapacityFailed" in run.error
    assert "names no capacity" not in run.error

