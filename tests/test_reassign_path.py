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
