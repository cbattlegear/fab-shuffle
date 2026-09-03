"""What a migrated item writes to, not only what it reads.

``relations/upstream`` reports what an item depends on. A Copy Job's destination is a write
target, so it never appears there, and the two Copy Jobs whose destination was a lakehouse in
another workspace passed the dependency check and then failed mid-run with UnknownError.
"""

from __future__ import annotations

from fabshuffle.fabric import relations

SOURCE_WS = "ws-source"
OTHER_WS = "ws-elsewhere"
COPY_JOB = "cj-1"
LAKEHOUSE = "lh-1"
FOREIGN_LAKEHOUSE = "lh-foreign"


class FakeClient:
    """Answers the two relations endpoints separately, the way Fabric does."""

    def __init__(self, upstream=None, downstream=None, items=None, workspaces=None) -> None:
        self.upstream = upstream or {}
        self.downstream = downstream or {}
        self.items = items or []
        self.workspaces = workspaces or []
        self.asked: list[str] = []

    def get(self, path, params=None):
        assert (params or {}).get("beta") == "true", "the relations APIs are beta"
        item_id = path.split("/items/")[1].split("/")[0]
        direction = path.rsplit("/", 1)[-1]
        self.asked.append(f"{direction}:{item_id}")
        edges = (self.upstream if direction == "upstream" else self.downstream).get(item_id, [])
        return {"items": self.items, "relations": edges, "workspaces": self.workspaces}


def edge(item_id, depends_on):
    return {"itemId": item_id, "dependentOnItemId": depends_on, "relationType": "Datasource"}


COPY_JOB_ITEM = {"id": COPY_JOB, "displayName": "TestCopyCrossWorkspace", "type": "CopyJob"}
FOREIGN_ITEM = {
    "id": FOREIGN_LAKEHOUSE,
    "displayName": "OtherHouse",
    "type": "Lakehouse",
    "workspaceId": OTHER_WS,
}


def analyse(client, items, migrated_ids):
    graph = relations.build_graph(client, SOURCE_WS, items)
    return graph, relations.analyse(
        graph, migrated_ids=set(migrated_ids), source_workspace_id=SOURCE_WS
    )


# ------------------------------------------------------------ both directions


def test_both_directions_are_asked_for():
    client = FakeClient()
    analyse(client, [COPY_JOB_ITEM], {COPY_JOB})

    assert client.asked == [f"upstream:{COPY_JOB}", f"downstream:{COPY_JOB}"]


def test_a_write_destination_in_another_workspace_is_reported():
    """The case that got through: it is not something the Copy Job depends on."""
    client = FakeClient(
        downstream={COPY_JOB: [edge(FOREIGN_LAKEHOUSE, COPY_JOB)]},
        items=[FOREIGN_ITEM],
        workspaces=[{"id": OTHER_WS, "displayName": "Somewhere Else"}],
    )
    _, issues = analyse(client, [COPY_JOB_ITEM], {COPY_JOB})

    assert len(issues) == 1
    message = issues[0].message()
    assert "writes to Lakehouse 'OtherHouse'" in message
    assert "Somewhere Else" in message
    assert "keep writing into the original region" in message


def test_a_write_destination_that_is_not_migrating_is_reported():
    client = FakeClient(
        downstream={COPY_JOB: [edge(LAKEHOUSE, COPY_JOB)]},
        items=[{"id": LAKEHOUSE, "displayName": "Bronze", "type": "Lakehouse", "workspaceId": SOURCE_WS}],
    )
    _, issues = analyse(client, [COPY_JOB_ITEM], {COPY_JOB})

    assert "has nowhere to write" in issues[0].message()


def test_a_write_destination_that_is_migrating_is_not_reported():
    client = FakeClient(
        downstream={COPY_JOB: [edge(LAKEHOUSE, COPY_JOB)]},
        items=[{"id": LAKEHOUSE, "displayName": "Bronze", "type": "Lakehouse", "workspaceId": SOURCE_WS}],
    )
    _, issues = analyse(client, [COPY_JOB_ITEM], {COPY_JOB, LAKEHOUSE})

    assert issues == []


def test_a_source_that_is_missing_still_reads_as_a_source():
    client = FakeClient(
        upstream={COPY_JOB: [edge(COPY_JOB, LAKEHOUSE)]},
        items=[{"id": LAKEHOUSE, "displayName": "Bronze", "type": "Lakehouse", "workspaceId": SOURCE_WS}],
    )
    _, issues = analyse(client, [COPY_JOB_ITEM], {COPY_JOB})

    message = issues[0].message()
    assert "depends on" in message
    assert "missing its source" in message


# --------------------------------------------------------------- degradation


def test_an_unavailable_downstream_api_does_not_lose_the_upstream_analysis():
    """Downstream is additional. Losing it costs write destinations, not the whole check."""

    class NoDownstream(FakeClient):
        def get(self, path, params=None):
            if path.endswith("/downstream"):
                from fabshuffle.fabric.client import FabricApiError

                raise FabricApiError("GET", path, 404, "{}")
            return super().get(path, params)

    client = NoDownstream(
        upstream={COPY_JOB: [edge(COPY_JOB, LAKEHOUSE)]},
        items=[{"id": LAKEHOUSE, "displayName": "Bronze", "type": "Lakehouse", "workspaceId": SOURCE_WS}],
    )
    graph, issues = analyse(client, [COPY_JOB_ITEM], {COPY_JOB})

    assert graph.available
    assert "missing its source" in issues[0].message()


def test_an_unavailable_upstream_api_still_stops_everything():
    class Nothing(FakeClient):
        def get(self, path, params=None):
            from fabshuffle.fabric.client import FabricApiError

            raise FabricApiError("GET", path, 403, "{}")

    graph, issues = analyse(Nothing(), [COPY_JOB_ITEM], {COPY_JOB})

    assert graph.available is False
    assert issues == []


# ------------------------------------------------------------------- reverse


def test_the_reverse_index_finds_what_an_item_feeds():
    graph = relations.DependencyGraph(dependencies={LAKEHOUSE: {COPY_JOB}, "other": {COPY_JOB}})

    assert graph.dependents_of(COPY_JOB) == {LAKEHOUSE, "other"}
    assert graph.dependents_of(LAKEHOUSE) == set()
