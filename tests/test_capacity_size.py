"""Warning when the target capacity is not the same size as the source's.

Capacity SKU caps Spark pool and starter pool node counts, and the memory a semantic model
may use. Moving to a smaller capacity therefore succeeds right up until something does not
fit, which is a bad time to find out. The comparison happens while the plan is built, so the
warning reaches the review screen before anything has been created.
"""

from __future__ import annotations

from fabshuffle.fabric import workspaces
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.support import Strategy
from fabshuffle.orchestrator import MigrationPlan as Plan
from fabshuffle.orchestrator import build_plan


class FakeClient:
    def __init__(self, capacities: dict[str, dict] | None = None) -> None:
        self.capacities = capacities or {}
        self.requested: list[str] = []

    def get(self, path: str) -> dict:
        self.requested.append(path)
        capacity_id = path.rsplit("/", 1)[-1]
        if capacity_id not in self.capacities:
            raise FabricApiError("GET", path, 403, "Forbidden")
        return self.capacities[capacity_id]


def test_matching_capacities_say_nothing() -> None:
    assert workspaces.compare_capacities("F64", "F64") is None


def test_capacity_size_comparison_ignores_case_and_spacing() -> None:
    assert workspaces.compare_capacities(" f64 ", "F64") is None


def test_a_smaller_target_warns_that_things_may_not_fit() -> None:
    warning = workspaces.compare_capacities("F64", "F8")

    assert warning is not None
    assert "F8" in warning
    assert "F64" in warning
    assert "smaller" in warning
    assert "may not fit" in warning


def test_a_larger_target_warns_about_cost_rather_than_capacity() -> None:
    warning = workspaces.compare_capacities("F8", "F64")

    assert warning is not None
    assert "larger" in warning
    assert "bill" in warning


def test_sizes_are_compared_numerically_not_alphabetically() -> None:
    # "F8" sorts after "F64" as text, so a string comparison would call this a downgrade.
    warning = workspaces.compare_capacities("F8", "F64")

    assert warning is not None
    assert "larger" in warning


def test_a_premium_sku_is_reported_without_guessing_a_direction() -> None:
    warning = workspaces.compare_capacities("P1", "F64")

    assert warning is not None
    assert "P1" in warning
    assert "F64" in warning
    assert "smaller" not in warning
    assert "larger" not in warning


def test_an_unknown_source_capacity_is_not_worth_warning_about() -> None:
    assert workspaces.compare_capacities("", "F64") is None
    assert workspaces.compare_capacities("F64", "") is None


def test_the_source_capacity_sku_is_read_from_the_workspace() -> None:
    client = FakeClient({"cap-1": {"id": "cap-1", "sku": "F64"}})

    sku = workspaces.workspace_capacity_sku(client, {"id": "ws", "capacityId": "cap-1"})

    assert sku == "F64"
    assert client.requested == ["capacities/cap-1"]


def test_a_workspace_with_no_capacity_is_not_looked_up() -> None:
    client = FakeClient()

    assert workspaces.workspace_capacity_sku(client, {"id": "ws"}) == ""
    assert client.requested == []


def test_an_unreadable_source_capacity_does_not_fail_the_plan() -> None:
    # The service principal only needs access to the target capacity, so being unable to read
    # the source's is expected rather than exceptional.
    client = FakeClient()

    assert workspaces.workspace_capacity_sku(client, {"id": "ws", "capacityId": "cap-1"}) == ""


class PlanClient:
    """Serves the two reads build_plan performs, plus the source capacity lookup."""

    def __init__(self, source_sku: str, target_sku: str) -> None:
        self.source_sku = source_sku
        self.target_sku = target_sku

    def get(self, path: str, params: object = None) -> dict:
        if path == "capacities/target-cap":
            return {"id": "target-cap", "displayName": "big", "region": "West US", "sku": self.target_sku}
        if path == "capacities/source-cap":
            return {"id": "source-cap", "sku": self.source_sku}
        if path == "workspaces/ws-1":
            return {"id": "ws-1", "displayName": "Sales", "capacityId": "source-cap"}
        raise AssertionError(f"unexpected GET {path}")


def _plan(source_sku: str, target_sku: str) -> Plan:
    return build_plan(
        PlanClient(source_sku, target_sku),
        capacity_id="target-cap",
        source_workspace_id="ws-1",
        strategy=Strategy.REBUILD,
    )


def test_the_plan_carries_the_warning_to_the_review_screen() -> None:
    plan = _plan("F64", "F2")

    assert plan.capacity_warning is not None
    assert "smaller" in plan.capacity_warning


def test_the_plan_is_quiet_when_the_capacities_match() -> None:
    assert _plan("F64", "F64").capacity_warning is None
