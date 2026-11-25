"""Dependency analysis from the beta relations APIs.

The id map only covers items this run creates, so a dependency on another workspace, or on
an item type Fab Shuffle does not migrate, silently leaves the copy pointing at the original
region. Neither is visible from the item definitions.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

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


def test_relation_activity_reports_real_reads_and_unavailable_api():
    events = []
    client = FakeClient({}, unavailable=True)
    graph = relations.build_graph(
        client, SOURCE_WS, [MODEL, REPORT],
        on_item=lambda item, action: events.append((item["id"], action, list(client.asked))),
    )
    assert not graph.available
    assert events == [
        ("sm-1", "Reading upstream relations", []),
        ("sm-1", "Relations unavailable", ["sm-1"]),
    ]


def test_relation_reads_overlap_after_probe_and_respect_worker_limit():
    lock = threading.Lock()
    full = threading.Event()
    release = threading.Event()
    release_later = threading.Event()
    replacement_started = threading.Event()
    active = 0
    maximum = 0
    calls = []

    class BlockingClient(FakeClient):
        def get(self, path, params=None):
            nonlocal active, maximum
            item_id = path.split("/items/")[1].split("/")[0]
            with lock:
                calls.append(item_id)
                if item_id == "0":
                    return {"items": [], "relations": [], "workspaces": []}
                active += 1
                maximum = max(maximum, active)
                if active == 4:
                    full.set()
            try:
                if item_id == "1":
                    assert release.wait(5)
                else:
                    assert release_later.wait(5)
                if int(item_id) >= 5:
                    replacement_started.set()
                return upstream(
                    items=[{"id": "shared", "displayName": item_id}],
                    relations_=[edge(item_id, "shared")],
                )
            finally:
                with lock:
                    active -= 1

    items = [{"id": str(i), "type": "Notebook"} for i in range(10)]
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(relations.build_graph, BlockingClient({}), SOURCE_WS, items)
        try:
            assert full.wait(5)
            assert calls[0] == "0"
            assert len(calls) == 5
            release_later.set()
            assert replacement_started.wait(5)
            assert not release.is_set()
            assert not future.done()
        finally:
            release_later.set()
            release.set()
        graph = future.result(timeout=5)
    assert maximum == 4
    assert set(graph.items) == {str(i) for i in range(10)} | {"shared"}
    assert graph.name_of("shared") == "1"
    assert graph.dependencies == {str(i): {"shared"} for i in range(1, 10)}


def test_parallel_results_are_aggregated_in_input_order():
    second_started = threading.Event()
    release = threading.Event()
    shared = {"id": "shared", "type": "Notebook"}

    class OrderedClient(FakeClient):
        def get(self, path, params=None):
            item_id = path.split("/items/")[1].split("/")[0]
            if item_id == "0":
                return upstream(items=[], relations_=[])
            if item_id == "1":
                assert second_started.wait(5)
                assert release.wait(5)
            if item_id == "2":
                second_started.set()
                release.set()
            return upstream(items=[{**shared, "displayName": item_id}], relations_=[])

    graph = relations.build_graph(
        OrderedClient({}), SOURCE_WS, [{"id": str(i)} for i in range(3)],
    )
    assert graph.name_of("shared") == "1"


def test_cancellation_prevents_queued_relation_reads():
    full = threading.Event()
    release = threading.Event()
    cancelled = threading.Event()
    lock = threading.Lock()
    calls = []

    class BlockingClient(FakeClient):
        def get(self, path, params=None):
            item_id = path.split("/items/")[1].split("/")[0]
            with lock:
                calls.append(item_id)
                if len(calls) == 5:
                    full.set()
            if item_id != "0":
                assert release.wait(5)
            return upstream(items=[], relations_=[])

    def progress(_item, _action):
        if cancelled.is_set():
            raise RuntimeError("cancelled")

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            relations.build_graph, BlockingClient({}), SOURCE_WS,
            [{"id": str(i)} for i in range(10)], on_item=progress,
        )
        try:
            assert full.wait(5)
            cancelled.set()
        finally:
            release.set()
        with pytest.raises(RuntimeError, match="cancelled"):
            future.result(timeout=5)
    assert len(calls) == 5


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
