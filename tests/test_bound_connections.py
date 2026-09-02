"""Connections the service principal cannot use, found before the run rather than during it.

In a real run five items failed for one reason: they bind tenant connections the service
principal has no access to. Each failure arrived separately, after everything else had been
built, and said nothing about the others:

    MirroredDatabase 'BattleCabbageReplTest' ... ConnectionNotAccessible
    DataPipeline 'DownloadForzaCarsLookupTable' ... User does not have access to the connection
    DataPipeline 'CopyRunFromDB' ... User does not have access to the connection
    CopyJob 'copyjob1' ... User does not have access to the connection
    CopyJob 'SqlToSqlTest' ... User does not have access to the connection

The definitions are readable up front, so the connections are reported once, before anything
is created.
"""

from __future__ import annotations

from fabshuffle import orchestrator
from fabshuffle.fabric.definitions import part

SHARED = "081da81e-f477-4715-8b66-c2a1debf8909"
REACHABLE = "1a2b3c4d-0000-1111-2222-333344445555"


def pipeline_parts(connection_id: str) -> list[dict]:
    return [
        part(
            "pipeline-content.json",
            {"properties": {"activities": [{"externalReferences": {"connection": connection_id}}]}},
        )
    ]


class Client:
    """Serves the item list, definitions, and the tenant connections.

    ``list_of_type`` deliberately lists everything and filters in Python, because Fabric
    documents its type filter as unreliable for dataflows, so the fake does the same.
    """

    def __init__(self, items: dict[str, list[dict]], visible: list[str]) -> None:
        self.items = items
        self.visible = visible
        self.definitions_read: list[str] = []
        self.binding: dict[str, str] = {}

    def list_all(self, path, params=None, value_key="value"):
        if path.rstrip("/").endswith("connections"):
            return [{"id": cid, "displayName": f"conn-{cid[:4]}"} for cid in self.visible]
        if path.endswith("/items"):
            return [
                {**item, "type": item_type}
                for item_type, entries in self.items.items()
                for item in entries
            ]
        return []

    def post(self, path, json=None, params=None, wait=True):
        item_id = path.split("/items/")[1].split("/")[0]
        self.definitions_read.append(item_id)
        return {"definition": {"parts": pipeline_parts(self.binding[item_id])}}


def build(binding: dict[str, str], visible: list[str], items: dict[str, list[dict]]) -> Client:
    client = Client(items, visible)
    client.binding = binding
    return client


def test_an_unusable_connection_is_reported_before_anything_is_built() -> None:
    client = build(
        binding={"p1": SHARED},
        visible=[REACHABLE],
        items={"DataPipeline": [{"id": "p1", "displayName": "CopyRunFromDB"}]},
    )

    warnings = orchestrator.bound_connection_warnings(
        client, source_workspace_id="ws", client_id="spn"
    )

    assert len(warnings) == 1
    assert SHARED in warnings[0]
    assert "CopyRunFromDB" in warnings[0]
    assert "Manage connections and gateways" in warnings[0]


def test_one_connection_shared_by_many_items_is_reported_once() -> None:
    """The run reported the same cause five times, once per item, after the fact."""
    client = build(
        binding={"p1": SHARED, "p2": SHARED, "j1": SHARED},
        visible=[REACHABLE],
        items={
            "DataPipeline": [
                {"id": "p1", "displayName": "DownloadForzaCarsLookupTable"},
                {"id": "p2", "displayName": "CopyRunFromDB"},
            ],
            "CopyJob": [{"id": "j1", "displayName": "copyjob1"}],
        },
    )

    warnings = orchestrator.bound_connection_warnings(
        client, source_workspace_id="ws", client_id="spn"
    )

    assert len(warnings) == 1
    # But it still names every item, so the blast radius is clear.
    for name in ("DownloadForzaCarsLookupTable", "CopyRunFromDB", "copyjob1"):
        assert name in warnings[0]


def test_a_reachable_connection_says_nothing() -> None:
    client = build(
        binding={"p1": REACHABLE},
        visible=[REACHABLE],
        items={"DataPipeline": [{"id": "p1", "displayName": "Fine"}]},
    )

    assert orchestrator.bound_connection_warnings(
        client, source_workspace_id="ws", client_id="spn"
    ) == []


def test_a_workspace_with_nothing_that_binds_a_connection_is_not_scanned() -> None:
    client = build(binding={}, visible=[REACHABLE], items={})

    assert (
        orchestrator.bound_connection_warnings(client, source_workspace_id="ws", client_id="spn")
        == []
    )
    assert client.definitions_read == []


def test_nothing_is_claimed_when_the_tenant_connections_cannot_be_read() -> None:
    # Otherwise every connection would look unreachable and the warning would be nonsense.
    client = build(
        binding={"p1": SHARED},
        visible=[],
        items={"DataPipeline": [{"id": "p1", "displayName": "CopyRunFromDB"}]},
    )

    assert (
        orchestrator.bound_connection_warnings(client, source_workspace_id="ws", client_id="spn")
        == []
    )


def test_every_connection_binding_type_is_checked() -> None:
    """A mirrored database failed the same way, so it cannot be a pipeline-only check."""
    assert set(orchestrator.CONNECTION_BINDING_TYPES) >= {
        "DataPipeline",
        "CopyJob",
        "Eventstream",
        "MirroredDatabase",
        "Reflex",
    }


def test_the_warning_reaches_the_dependency_report(monkeypatch) -> None:
    from fabshuffle.fabric import relations

    monkeypatch.setattr(
        relations, "build_graph", lambda *a, **k: relations.DependencyGraph(dependencies={})
    )
    monkeypatch.setattr(orchestrator, "connection_prerequisites", lambda *a, **k: [])
    monkeypatch.setattr(orchestrator, "bound_connection_warnings", lambda *a, **k: ["grant access"])

    report = orchestrator.dependency_warnings(
        object(), source_workspace_id="ws", migrated=[{"id": "x"}], client_id="spn"
    )

    assert "grant access" in report.messages()
