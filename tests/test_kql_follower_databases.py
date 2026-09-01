"""Shortcut (follower) KQL databases.

A follower holds no data of its own, so it is recreated pointing at the same leader rather
than copied. ``Get KQL Database`` reports ``databaseType: Shortcut`` but not the source, so
the leader comes from asking the follower's own cluster with ``.show follower database``.
Its ``OriginalDatabaseName`` is the leader's KQL Database *item id* when the leader is
another Fabric eventhouse, which is what makes it possible to follow the leader by id alone.
"""

from __future__ import annotations

from fabshuffle import orchestrator
from fabshuffle.fabric import eventhouses
from fabshuffle.transfer import kql

FOLLOWER = {
    "id": "9532ed4f-68de-4bcf-8c7c-ff236be809fa",
    "displayName": "SharedTelemetry",
    "properties": {"databaseType": "Shortcut"},
}
LEADER_ID = "47323ec0-660a-401f-a70d-2175c1b978e6"
LEADER_PATH = (
    "https://e1rvirtualengines.z29.blob.storage.azure.net/"
    "0xcenginetrdjhbppuj5wux2cjtjjzmd202502260025084853"
)


def test_a_fabric_leader_is_recognised_by_its_guid() -> None:
    source = kql.FollowerSource(database_name=LEADER_ID, leader_metadata_path=LEADER_PATH)

    assert source.is_fabric_source


def test_an_azure_data_explorer_leader_is_a_plain_name() -> None:
    # ADX names databases freely, so anything that is not a GUID is not a Fabric item id.
    assert not kql.FollowerSource(database_name="TelemetryDb").is_fabric_source


def test_a_fabric_leader_is_followed_by_id_with_no_cluster_uri() -> None:
    """Fabric resolves a leader item id within the tenant, so sending a URI can only be wrong."""
    payload = eventhouses.shortcut_creation_payload(
        FOLLOWER, "eh-new", source_database_name=LEADER_ID
    )

    assert payload == {
        "databaseType": "Shortcut",
        "parentEventhouseItemId": "eh-new",
        "sourceDatabaseName": LEADER_ID,
    }


def test_an_azure_data_explorer_leader_keeps_its_cluster_uri() -> None:
    database = {
        "id": "kql-1",
        "displayName": "Followed",
        "properties": {
            "databaseType": "Shortcut",
            "sourceClusterUri": "https://adx.westus.kusto.windows.net",
            "sourceDatabaseName": "TelemetryDb",
        },
    }

    payload = eventhouses.shortcut_creation_payload(database, "eh-new")

    assert payload["sourceClusterUri"] == "https://adx.westus.kusto.windows.net"
    assert payload["sourceDatabaseName"] == "TelemetryDb"


def test_a_resolved_leader_wins_over_stale_item_properties() -> None:
    # The live answer from the cluster is authoritative; properties are only a fallback.
    database = {
        "id": "kql-1",
        "displayName": "Followed",
        "properties": {"databaseType": "Shortcut", "sourceDatabaseName": "old-value"},
    }

    payload = eventhouses.shortcut_creation_payload(
        database, "eh-new", source_database_name=LEADER_ID
    )

    assert payload["sourceDatabaseName"] == LEADER_ID


def test_nothing_is_built_when_the_leader_is_unknown() -> None:
    payload = eventhouses.shortcut_creation_payload(FOLLOWER, "eh-new")

    assert payload is None


class FakeKustoClient:
    """Stands in for the follower's own cluster."""

    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.commands: list[tuple[str, str]] = []

    def __enter__(self) -> FakeKustoClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute_mgmt(self, database: str, command: str, properties: object = None):
        self.commands.append((database, command))
        rows = self.rows

        class Response:
            primary_results = (rows,)

        return Response()


def test_the_leader_is_read_from_the_followers_own_cluster(monkeypatch) -> None:
    kusto = FakeKustoClient(
        [{"OriginalDatabaseName": LEADER_ID, "LeaderClusterMetadataPath": LEADER_PATH}]
    )
    monkeypatch.setattr(kql, "_client", lambda *a, **k: kusto)

    source = kql.follower_source("https://cluster.kusto.fabric.microsoft.com", FOLLOWER["id"], None)

    assert source is not None
    assert source.database_name == LEADER_ID
    assert source.leader_metadata_path == LEADER_PATH


def test_the_database_name_is_quoted_because_fabric_names_are_guids(monkeypatch) -> None:
    # A Fabric KQL database is named after its item id, which often starts with a digit and
    # is a parse error as a bare identifier.
    kusto = FakeKustoClient([{"OriginalDatabaseName": LEADER_ID, "LeaderClusterMetadataPath": ""}])
    monkeypatch.setattr(kql, "_client", lambda *a, **k: kusto)

    kql.follower_source("https://cluster", "9532ed4f-68de-4bcf-8c7c-ff236be809fa", None)

    _, command = kusto.commands[0]
    assert command == '.show follower database ["9532ed4f-68de-4bcf-8c7c-ff236be809fa"]'


def test_a_database_that_follows_nothing_returns_no_source(monkeypatch) -> None:
    monkeypatch.setattr(kql, "_client", lambda *a, **k: FakeKustoClient([]))

    assert kql.follower_source("https://cluster", "db", None) is None


def test_a_blank_original_database_name_returns_no_source(monkeypatch) -> None:
    kusto = FakeKustoClient([{"OriginalDatabaseName": "", "LeaderClusterMetadataPath": ""}])
    monkeypatch.setattr(kql, "_client", lambda *a, **k: kusto)

    assert kql.follower_source("https://cluster", "db", None) is None


# --------------------------------------------------------------- in the migration


class FakeFabricClient:
    def __init__(self) -> None:
        self.created: list[dict] = []

    def post(self, path, json=None, params=None, wait=True):
        assert path.endswith("/kqlDatabases"), path
        self.created.append(json)
        return {"id": f"new-{len(self.created)}"}


def make_ctx(client) -> orchestrator._Context:
    plan = orchestrator.MigrationPlan(
        capacity_id="cap",
        capacity_name="F64",
        capacity_region="westus",
        source_workspace_id="ws-source",
        source_workspace_name="src",
        target_workspace_name="dst",
    )
    ctx = orchestrator._Context(
        client=client,
        tokens=object(),
        principal=object(),
        plan=plan,
        run=orchestrator.MigrationRun(source_workspace_name="src", capacity_name="F64"),
        scratch_dir=None,
    )
    ctx.target_workspace_id = "ws-target"
    return ctx


def install_source(monkeypatch, source: kql.FollowerSource | None) -> None:
    monkeypatch.setattr(orchestrator.kql, "follower_source", lambda *a, **k: source)


def test_a_leader_inside_this_workspace_is_followed_to_its_copy(monkeypatch) -> None:
    """Otherwise the copy reaches back across the region boundary to the workspace we left."""
    install_source(monkeypatch, kql.FollowerSource(database_name=LEADER_ID))
    client = FakeFabricClient()
    ctx = make_ctx(client)
    ctx.id_map[LEADER_ID] = "leader-in-target"

    moved, warnings = orchestrator._migrate_follower_database(
        ctx, database=FOLLOWER, target_eventhouse_id="eh-new", source_query_uri="https://cluster"
    )

    assert moved
    assert warnings == []
    assert client.created[0]["creationPayload"]["sourceDatabaseName"] == "leader-in-target"


def test_a_leader_outside_this_workspace_is_left_alone(monkeypatch) -> None:
    install_source(monkeypatch, kql.FollowerSource(database_name=LEADER_ID))
    client = FakeFabricClient()

    moved, _warnings = orchestrator._migrate_follower_database(
        make_ctx(client),
        database=FOLLOWER,
        target_eventhouse_id="eh-new",
        source_query_uri="https://cluster",
    )

    assert moved
    assert client.created[0]["creationPayload"]["sourceDatabaseName"] == LEADER_ID


def test_the_new_follower_goes_into_the_id_map(monkeypatch) -> None:
    # Reports and other items can reference the follower, so the copy has to be findable.
    install_source(monkeypatch, kql.FollowerSource(database_name=LEADER_ID))
    ctx = make_ctx(FakeFabricClient())

    orchestrator._migrate_follower_database(
        ctx, database=FOLLOWER, target_eventhouse_id="eh-new", source_query_uri="https://cluster"
    )

    assert ctx.id_map[FOLLOWER["id"]] == "new-1"


def test_an_azure_data_explorer_follower_is_reported_not_guessed_at(monkeypatch) -> None:
    install_source(monkeypatch, kql.FollowerSource(database_name="TelemetryDb"))
    client = FakeFabricClient()

    moved, warnings = orchestrator._migrate_follower_database(
        make_ctx(client),
        database=FOLLOWER,
        target_eventhouse_id="eh-new",
        source_query_uri="https://cluster",
    )

    assert not moved
    assert client.created == []
    assert "Azure Data Explorer" in warnings[0]
    assert "TelemetryDb" in warnings[0]


def test_an_unreadable_cluster_leaves_a_warning_rather_than_failing(monkeypatch) -> None:
    def explode(*_args, **_kwargs):
        raise RuntimeError("Kusto is unreachable")

    monkeypatch.setattr(orchestrator.kql, "follower_source", explode)
    client = FakeFabricClient()

    moved, warnings = orchestrator._migrate_follower_database(
        make_ctx(client),
        database=FOLLOWER,
        target_eventhouse_id="eh-new",
        source_query_uri="https://cluster",
    )

    assert not moved
    assert client.created == []
    assert "could not be determined" in warnings[0]


def test_followers_are_created_after_every_leader_in_the_workspace(monkeypatch) -> None:
    """A follower can point at a leader in a later eventhouse, which would not exist yet."""
    leader = {"id": LEADER_ID, "displayName": "Telemetry", "properties": {"databaseType": "ReadWrite"}}
    order: list[str] = []

    follower_house = {
        "id": "eh-follower",
        "displayName": "Downstream",
        "properties": {"databasesItemIds": [FOLLOWER["id"]]},
    }
    leader_house = {
        "id": "eh-leader",
        "displayName": "Upstream",
        "properties": {"databasesItemIds": [LEADER_ID]},
    }
    # The follower's eventhouse is listed first, so a single pass would reach it too early.
    monkeypatch.setattr(
        orchestrator.eventhouses,
        "list_eventhouses",
        lambda *a, **k: [follower_house, leader_house],
    )
    monkeypatch.setattr(
        orchestrator.eventhouses, "create_eventhouse", lambda c, w, name, **kwargs: {"id": f"new-{name}"}
    )
    monkeypatch.setattr(
        orchestrator.eventhouses, "get_eventhouse", lambda c, w, id_: {"id": id_, "properties": {}}
    )
    monkeypatch.setattr(orchestrator.eventhouses, "eventhouse_databases", lambda *a, **k: {})
    monkeypatch.setattr(
        orchestrator.eventhouses,
        "get_kql_database",
        lambda c, w, id_: FOLLOWER if id_ == FOLLOWER["id"] else leader,
    )

    def fake_leader(ctx, step, *, database, **_kwargs):
        order.append(f"leader:{database['id']}")
        ctx.id_map[database["id"]] = "leader-in-target"
        return True, [], None

    def fake_follower(ctx, *, database, **_kwargs):
        order.append(f"follower:{database['id']}")
        # The leader must already be mapped by the time the follower is built.
        assert ctx.id_map[LEADER_ID] == "leader-in-target"
        return True, []

    monkeypatch.setattr(orchestrator, "_migrate_kql_database", fake_leader)
    monkeypatch.setattr(orchestrator, "_migrate_follower_database", fake_follower)

    orchestrator._migrate_eventhouses(make_ctx(FakeFabricClient()))

    assert order == [f"leader:{LEADER_ID}", f"follower:{FOLLOWER['id']}"]
