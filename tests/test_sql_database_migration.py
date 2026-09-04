"""Migrating a Fabric SQL database.

Two things make it unlike the other data stores. Its schema travels as an item definition
rather than through sqlpackage, and its rows move with bcp rather than a Copy Job, because
the SQL database in Fabric connector accepts only an organizational account and Fab Shuffle
signs in as a service principal.
"""

from __future__ import annotations

from pathlib import Path

from fabshuffle import orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric import sqldatabases
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.run import MigrationRun

SOURCE_WS = "ws-source"
TARGET_WS = "ws-target"
SCRATCH_WS = "ws-scratch"
SOURCE_DB = "sqldb-source"
TARGET_DB = "sqldb-target"
SOURCE_SERVER = "src.database.fabric.microsoft.com,1433"
TARGET_SERVER = "dst.database.fabric.microsoft.com,1433"

SOURCE_ITEM = {
    "id": SOURCE_DB,
    "displayName": "RegionBounceTest",
    "type": "SQLDatabase",
    "properties": {
        "collation": "SQL_Latin1_General_CP1_CI_AS",
        "backupRetentionDays": 7,
        "databaseName": "RegionBounceTest-1111",
        "serverFqdn": SOURCE_SERVER,
    },
}
TARGET_ITEM = {
    "id": TARGET_DB,
    "displayName": "RegionBounceTest",
    "properties": {"databaseName": "RegionBounceTest-2222", "serverFqdn": TARGET_SERVER},
}

SUPPORTED_SQL = {
    "type": "SQL",
    "supportedCredentialTypes": ["ServicePrincipal", "Basic"],
    "creationMethods": [
        {
            "name": "SQL",
            "parameters": [
                {"name": "server", "dataType": "Text", "required": True},
                {"name": "database", "dataType": "Text", "required": True},
            ],
        }
    ],
}


class FakeClient:
    def __init__(self, **overrides) -> None:
        self.posts: list[tuple[str, dict]] = []
        self.overrides = overrides

    def list_all(self, path, params=None, value_key="value"):
        if path == f"workspaces/{SOURCE_WS}/sqlDatabases":
            return [SOURCE_ITEM]
        return []

    def get(self, path, params=None):
        if path == f"workspaces/{TARGET_WS}/sqlDatabases/{TARGET_DB}":
            return TARGET_ITEM
        raise AssertionError(f"unexpected GET {path}")

    def post(self, path, json=None, params=None, wait=True):
        self.posts.append((path, json or {}))
        if path == f"workspaces/{TARGET_WS}/sqlDatabases":
            return {"id": TARGET_DB}
        if path.endswith("/getDefinition"):
            return {"definition": {"parts": [{"path": "sqldb.dacpac", "payload": "AAA="}]}}
        return {}

    def delete(self, path, params=None):
        return None


def make_ctx(client, monkeypatch, tables=(("dbo", "Orders"),), copied=None):
    monkeypatch.setattr(orchestrator.sqlschema, "list_base_tables", lambda *a, **k: list(tables))

    calls: list[dict] = []

    def copy_tables(**kwargs):
        calls.append(kwargs)
        return copied if copied is not None else []

    monkeypatch.setattr(orchestrator.bulkcopy, "copy_tables", copy_tables)
    client.bulk_copies = calls

    plan = orchestrator.MigrationPlan(
        capacity_id="cap",
        capacity_name="F64",
        capacity_region="westus",
        source_workspace_id=SOURCE_WS,
        source_workspace_name="src",
        target_workspace_name="dst",
    )
    ctx = orchestrator._Context(
        client=client,
        tokens=object(),
        principal=ServicePrincipal("tenant-1", "client-1", "shhh"),
        plan=plan,
        run=MigrationRun(source_workspace_name="src", capacity_name="F64"),
        scratch_dir=Path("scratch"),
    )
    ctx.target_workspace_id = TARGET_WS
    ctx.scratch_workspace_id = SCRATCH_WS
    return ctx


def step_of(ctx):
    return next(s for s in ctx.run.snapshot()["steps"] if s["id"] == "sqldatabases")


# ------------------------------------------------------------------ creation


def test_collation_travels_on_the_create_because_it_cannot_be_set_later(monkeypatch):
    client = FakeClient()
    orchestrator._migrate_sql_databases(make_ctx(client, monkeypatch))

    _, body = next(p for p in client.posts if p[0] == f"workspaces/{TARGET_WS}/sqlDatabases")
    assert body["creationPayload"] == {
        "creationMode": "New",
        "collation": "SQL_Latin1_General_CP1_CI_AS",
        "backupRetentionDays": 7,
    }
    # Fabric rejects a creationPayload and a definition on the same request, so the schema
    # cannot ride along here.
    assert "definition" not in body


def test_the_schema_is_applied_as_a_second_call(monkeypatch):
    client = FakeClient()
    orchestrator._migrate_sql_databases(make_ctx(client, monkeypatch))

    path, body = next(p for p in client.posts if p[0].endswith("/updateDefinition"))
    assert path == f"workspaces/{TARGET_WS}/sqlDatabases/{TARGET_DB}/updateDefinition"
    # Echoed back exactly as read: the part name differs between Learn articles and the
    # payload is opaque binary, so nothing is assumed about either.
    assert body["definition"]["parts"] == [{"path": "sqldb.dacpac", "payload": "AAA="}]
    assert body["definition"]["format"] == "dacpac"


def test_the_server_address_is_mapped_so_later_items_rebind(monkeypatch):
    ctx = make_ctx(FakeClient(), monkeypatch)
    orchestrator._migrate_sql_databases(ctx)

    assert ctx.id_map[SOURCE_DB] == TARGET_DB
    assert ctx.id_map[SOURCE_SERVER] == TARGET_SERVER




# ------------------------------------------------------------------- the rows


def test_rows_move_with_bcp_between_the_two_databases(monkeypatch):
    client = FakeClient()
    orchestrator._migrate_sql_databases(make_ctx(client, monkeypatch))

    assert len(client.bulk_copies) == 1
    call = client.bulk_copies[0]
    assert call["source_server"] == SOURCE_SERVER
    assert call["target_server"] == TARGET_SERVER
    # The catalog name is read back from each item; Fabric appends a GUID of its own that
    # differs between the source and its copy.
    assert call["source_database"] == "RegionBounceTest-1111"
    assert call["target_database"] == "RegionBounceTest-2222"
    assert [(t.schema, t.name) for t in call["tables"]] == [("dbo", "Orders")]


def test_no_connection_is_created_for_the_copy(monkeypatch):
    """The Fabric SQL connector takes an organizational account only, so ours never worked."""
    client = FakeClient()
    orchestrator._migrate_sql_databases(make_ctx(client, monkeypatch))

    assert not any(path == "connections" for path, _ in client.posts)


def test_per_table_failures_name_their_database(monkeypatch):
    client = FakeClient()
    ctx = make_ctx(client, monkeypatch, copied=["Rows for [dbo].[Orders] did not copy: nope"])
    orchestrator._migrate_sql_databases(ctx)

    assert step_of(ctx)["warnings"] == [
        "SQL database 'RegionBounceTest': Rows for [dbo].[Orders] did not copy: nope"
    ]


def test_a_database_with_no_tables_copies_nothing(monkeypatch):
    client = FakeClient()
    orchestrator._migrate_sql_databases(make_ctx(client, monkeypatch, tables=()))

    assert client.bulk_copies == []


def test_no_rows_move_when_the_plan_says_not_to(monkeypatch):
    client = FakeClient()
    ctx = make_ctx(client, monkeypatch)
    ctx.plan.include_data = False
    orchestrator._migrate_sql_databases(ctx)

    assert client.bulk_copies == []


def test_a_failed_schema_stops_the_row_copy(monkeypatch):
    client = FakeClient()
    monkeypatch.setattr(
        orchestrator.sqldatabases,
        "copy_schema",
        lambda *a, **k: (_ for _ in ()).throw(FabricApiError("POST", "x", 400, "{}")),
    )
    ctx = make_ctx(client, monkeypatch)
    orchestrator._migrate_sql_databases(ctx)

    assert any("was not copied either" in w for w in step_of(ctx)["warnings"])
    assert client.bulk_copies == []


def test_a_bulk_copy_that_fails_outright_is_reported(monkeypatch):
    client = FakeClient()
    ctx = make_ctx(client, monkeypatch)
    monkeypatch.setattr(
        orchestrator.bulkcopy,
        "copy_tables",
        lambda **k: (_ for _ in ()).throw(orchestrator.bulkcopy.BulkCopyError("bcp is missing")),
    )
    orchestrator._migrate_sql_databases(ctx)

    assert any("bcp is missing" in w for w in step_of(ctx)["warnings"])


# ------------------------------------------------------------- module basics


def test_the_database_name_is_read_not_derived():
    # Fabric appends a GUID of its own, which differs between the source and its copy.
    assert sqldatabases.database_name(SOURCE_ITEM) == "RegionBounceTest-1111"
    assert sqldatabases.database_name(TARGET_ITEM) == "RegionBounceTest-2222"


def test_a_server_that_already_carries_a_port_is_not_given_another():
    from fabshuffle.transfer.sqlschema import _server_with_port

    assert _server_with_port(SOURCE_SERVER) == SOURCE_SERVER
    assert _server_with_port("warehouse.datawarehouse.fabric.microsoft.com") == (
        "warehouse.datawarehouse.fabric.microsoft.com,1433"
    )
