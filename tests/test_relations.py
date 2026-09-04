"""Dependency analysis from the beta relations APIs.

The id map only covers items this run creates, so a dependency on another workspace, or on
an item type Fab Shuffle does not migrate, silently leaves the copy pointing at the original
region. Neither is visible from the item definitions.
"""

from __future__ import annotations

import pytest

from fabshuffle.fabric import relations
from fabshuffle.fabric.client import FabricApiError

SOURCE_WS = "ws-source"
OTHER_WS = "ws-other"

MODEL = {"id": "sm-1", "type": "SemanticModel", "displayName": "Sales Model"}
REPORT = {"id": "rp-1", "type": "Report", "displayName": "Sales"}
LAKEHOUSE = {"id": "lh-1", "type": "Lakehouse", "displayName": "bronze"}


class FakeClient:
    """Serves canned upstream responses, and records which items were asked about."""

    def __init__(self, responses: dict[str, dict], *, unavailable: bool = False) -> None:
        self.responses = responses
        self.unavailable = unavailable
        self.asked: list[str] = []

    def get(self, path, params=None):
        assert params == {"beta": "true"}, "the beta APIs require ?beta=true"
        item_id = path.split("/items/")[1].split("/")[0]
        self.asked.append(item_id)
        if self.unavailable:
            raise FabricApiError("GET", path, 400, "beta not enabled")
        return self.responses.get(item_id, {"items": [], "relations": [], "workspaces": []})


def upstream(*, items, relations_, workspaces=()):
    return {
        "items": list(items),
        "relations": list(relations_),
        "workspaces": [{"id": w, "displayName": f"{w} name"} for w in workspaces],
    }


def edge(item_id, depends_on, relation_type="Datasource"):
    return {"itemId": item_id, "dependentOnItemId": depends_on, "relationType": relation_type}


# ------------------------------------------------------------------- building


def test_unavailable_beta_api_stops_after_one_call():
    client = FakeClient({}, unavailable=True)
    graph = relations.build_graph(client, SOURCE_WS, [MODEL, REPORT, LAKEHOUSE])

    assert graph.available is False
    # Quota matters: do not retry an API that is certainly unavailable.
    assert len(client.asked) == 1


def test_relations_that_are_not_dependencies_are_ignored():
    client = FakeClient(
        {
            "sm-1": upstream(
                items=[LAKEHOUSE],
                relations_=[edge("sm-1", "lh-1", "HiddenInWorkspace")],
            )
        }
    )
    graph = relations.build_graph(client, SOURCE_WS, [MODEL])
    assert graph.dependencies.get("sm-1", set()) == set()


def test_self_references_are_ignored():
    client = FakeClient({"sm-1": upstream(items=[MODEL], relations_=[edge("sm-1", "sm-1")])})
    graph = relations.build_graph(client, SOURCE_WS, [MODEL])
    assert graph.dependencies.get("sm-1", set()) == set()


# ------------------------------------------------------------------ analysis


def build(responses, items):
    return relations.build_graph(FakeClient(responses), SOURCE_WS, items)


def test_cross_workspace_dependency_is_reported():
    external = {"id": "lh-9", "type": "Lakehouse", "displayName": "Shared", "workspaceId": OTHER_WS}
    graph = build(
        {"sm-1": upstream(items=[external], relations_=[edge("sm-1", "lh-9")], workspaces=[OTHER_WS])},
        [MODEL],
    )
    issues = relations.analyse(graph, migrated_ids={"sm-1"}, source_workspace_id=SOURCE_WS)

    assert len(issues) == 1
    assert "original region" in issues[0].message()
    assert "ws-other name" in issues[0].message()
    assert issues[0].dependency == "Shared"


def test_dependency_on_an_unmigrated_item_is_reported():
    dataflow = {"id": "df-1", "type": "Dataflow", "displayName": "Nightly", "workspaceId": SOURCE_WS}
    graph = build(
        {"sm-1": upstream(items=[dataflow], relations_=[edge("sm-1", "df-1")])},
        [MODEL],
    )
    issues = relations.analyse(graph, migrated_ids={"sm-1"}, source_workspace_id=SOURCE_WS)

    assert len(issues) == 1
    assert issues[0].message() == (
        "SemanticModel 'Sales Model' depends on Dataflow 'Nightly', "
        "which is not migrated, so the copy will be missing its source."
    )


def test_dependencies_inside_the_migration_are_not_reported():
    inside = {"id": "lh-1", "type": "Lakehouse", "displayName": "bronze", "workspaceId": SOURCE_WS}
    graph = build(
        {"sm-1": upstream(items=[inside], relations_=[edge("sm-1", "lh-1")])},
        [MODEL],
    )
    assert relations.analyse(graph, migrated_ids={"sm-1", "lh-1"}, source_workspace_id=SOURCE_WS) == []


def test_derived_items_are_not_reported():
    # A lakehouse SQL analytics endpoint is created with its lakehouse, not separately.
    endpoint = {"id": "ep-1", "type": "SQLEndpoint", "displayName": "bronze", "workspaceId": SOURCE_WS}
    graph = build(
        {"sm-1": upstream(items=[endpoint], relations_=[edge("sm-1", "ep-1")])},
        [MODEL],
    )
    assert relations.analyse(graph, migrated_ids={"sm-1"}, source_workspace_id=SOURCE_WS) == []


def test_no_analysis_when_the_api_was_unavailable():
    graph = relations.DependencyGraph(available=False)
    assert relations.analyse(graph, migrated_ids={"sm-1"}, source_workspace_id=SOURCE_WS) == []


# -------------------------------------------------------- topological order


@pytest.fixture
def composite_graph() -> relations.DependencyGraph:
    graph = relations.DependencyGraph()
    # 'b' is a composite model reading 'a'; 'c' reads 'b'.
    graph.dependencies = {"b": {"a"}, "c": {"b"}}
    return graph


def test_dependencies_come_first(composite_graph):
    assert relations.topological_order(["c", "b", "a"], composite_graph) == ["a", "b", "c"]


def test_items_with_no_dependencies_keep_their_order(composite_graph):
    ordered = relations.topological_order(["x", "y", "z"], composite_graph)
    assert ordered == ["x", "y", "z"]


def test_dependencies_outside_the_batch_do_not_constrain_it(composite_graph):
    # 'a' is not in the batch, so 'b' is free to go first.
    assert relations.topological_order(["c", "b"], composite_graph) == ["b", "c"]


def test_a_cycle_is_emitted_rather_than_dropped():
    graph = relations.DependencyGraph()
    graph.dependencies = {"a": {"b"}, "b": {"a"}}

    ordered = relations.topological_order(["a", "b"], graph)
    assert sorted(ordered) == ["a", "b"]
