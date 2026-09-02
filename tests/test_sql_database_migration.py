"""Migrating a Fabric SQL database.

Two things make it unlike the other data stores. Its schema travels as an item definition
rather than through sqlpackage, and its Copy Job will not bind by item id alone: each end
needs a connection, which has to be created for the run and taken away again afterwards.
"""

from __future__ import annotations

import pytest

from fabshuffle import orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric import connections, copyjobs, sqldatabases
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.data_stores import TableRef
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
        self.deleted: list[str] = []
        self.next_connection = 0
        self.overrides = overrides

    def list_all(self, path, params=None, value_key="value"):
        if path == f"workspaces/{SOURCE_WS}/sqlDatabases":
            return [SOURCE_ITEM]
        if path == "connections/supportedConnectionTypes":
            return self.overrides.get("supported", [SUPPORTED_SQL])
        return []

    def get(self, path, params=None):
        if path == f"workspaces/{TARGET_WS}/sqlDatabases/{TARGET_DB}":
            return TARGET_ITEM
        if "/jobs/instances/" in path:
            return {"status": "Completed"}
        raise AssertionError(f"unexpected GET {path}")

    def post(self, path, json=None, params=None, wait=True):
        self.posts.append((path, json or {}))
        if path == f"workspaces/{TARGET_WS}/sqlDatabases":
            return {"id": TARGET_DB}
        if path.endswith("/getDefinition"):
            return {"definition": {"parts": [{"path": "sqldb.dacpac", "payload": "AAA="}]}}
        if path == "connections":
            if self.overrides.get("connections_fail"):
                raise FabricApiError("POST", path, 403, '{"errorCode":"Forbidden"}')
            self.next_connection += 1
            return {"id": f"conn-{self.next_connection}"}
        if path.endswith("/copyJobs"):
            return {"id": "copyjob-1"}
        return {}

    def request(self, method, path, expected=None, **kwargs):
        class Response:
            headers: dict = {"Location": "https://api/instances/inst-1"}  # noqa: RUF012

        return Response()

    def delete(self, path, params=None):
        self.deleted.append(path)


def make_ctx(client, monkeypatch, tables=(("dbo", "Orders"),)):
    monkeypatch.setattr(orchestrator.sqlschema, "list_base_tables", lambda *a, **k: list(tables))
    # The batch runner polls between passes; nothing here needs real time to pass.
    monkeypatch.setattr(orchestrator.copyjobs.SETTINGS, "copy_job_poll_seconds", 0)

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
        scratch_dir=None,
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


# ------------------------------------------------------- connections and data


def test_a_copy_job_connection_is_created_at_each_end_and_deleted_after(monkeypatch):
    client = FakeClient()
    orchestrator._migrate_sql_databases(make_ctx(client, monkeypatch))

    created = [body for path, body in client.posts if path == "connections"]
    assert len(created) == 2
    assert [c["connectionDetails"]["parameters"][0]["value"] for c in created] == [
        SOURCE_SERVER,
        TARGET_SERVER,
    ]
    # They hold our own secret, so they must not outlive the copy.
    assert client.deleted == ["connections/conn-1", "connections/conn-2"]


def test_the_connection_authenticates_as_the_migrating_principal(monkeypatch):
    client = FakeClient()
    orchestrator._migrate_sql_databases(make_ctx(client, monkeypatch))

    credentials = client.posts[[p for p, _ in client.posts].index("connections")][1]
    assert credentials["credentialDetails"]["credentials"] == {
        "credentialType": "ServicePrincipal",
        "tenantId": "tenant-1",
        "servicePrincipalClientId": "client-1",
        "servicePrincipalSecret": "shhh",
    }


def test_the_copy_job_binds_both_ends_through_their_connections(monkeypatch):
    client = FakeClient()
    orchestrator._migrate_sql_databases(make_ctx(client, monkeypatch))

    _, body = next(p for p in client.posts if p[0].endswith("/copyJobs"))
    content = next(
        part for part in body["definition"]["parts"] if part["path"] == copyjobs.COPY_JOB_CONTENT_PART
    )
    import base64
    import json

    job = json.loads(base64.b64decode(content["payload"]))
    source = job["properties"]["source"]
    assert source["type"] == "FabricSqlDatabaseTable"
    assert source["connectionSettings"]["externalReferences"] == {"connection": "conn-1"}
    assert source["connectionSettings"]["typeProperties"] == {
        "workspaceId": SOURCE_WS,
        "artifactId": SOURCE_DB,
    }
    # The schema half already created the tables, so rows are added rather than replaced.
    assert job["activities"][0]["properties"]["destination"]["writeBehavior"] == "Append"


def test_a_connection_that_cannot_be_created_leaves_the_database_in_place(monkeypatch):
    client = FakeClient(connections_fail=True)
    ctx = make_ctx(client, monkeypatch)
    orchestrator._migrate_sql_databases(ctx)

    step = step_of(ctx)
    assert step["detail"] == "Migrated 1 SQL database(s)"
    assert any("could not be created" in w for w in step["warnings"])
    # No half-built job, and nothing left holding our credentials.
    assert not any(path.endswith("/copyJobs") for path, _ in client.posts)


def test_no_data_is_copied_when_the_plan_says_not_to(monkeypatch):
    client = FakeClient()
    ctx = make_ctx(client, monkeypatch)
    ctx.plan.include_data = False
    orchestrator._migrate_sql_databases(ctx)

    assert not any(path == "connections" for path, _ in client.posts)
    assert not any(path.endswith("/copyJobs") for path, _ in client.posts)


def test_a_failed_schema_stops_the_data_copy(monkeypatch):
    client = FakeClient()
    monkeypatch.setattr(
        orchestrator.sqldatabases,
        "copy_schema",
        lambda *a, **k: (_ for _ in ()).throw(FabricApiError("POST", "x", 400, "{}")),
    )
    ctx = make_ctx(client, monkeypatch)
    orchestrator._migrate_sql_databases(ctx)

    assert any("was not copied either" in w for w in step_of(ctx)["warnings"])
    assert not any(path.endswith("/copyJobs") for path, _ in client.posts)


# --------------------------------------------------------- connection helper


def test_a_tenant_without_the_sql_connection_type_is_refused_not_guessed():
    client = FakeClient(supported=[])
    with pytest.raises(connections.ConnectionUnavailable, match="does not report"):
        connections.create_own_connection(
            client,
            ServicePrincipal("t", "c", "s"),
            connection_type="SQL",
            path="server;database",
            display_name="x",
        )


def test_a_principal_that_cannot_authenticate_that_type_is_refused():
    client = FakeClient(supported=[{**SUPPORTED_SQL, "supportedCredentialTypes": ["Basic"]}])
    with pytest.raises(connections.ConnectionUnavailable, match="service principal"):
        connections.create_own_connection(
            client,
            ServicePrincipal("t", "c", "s"),
            connection_type="SQL",
            path="server;database",
            display_name="x",
        )


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


def test_tables_are_qualified_by_schema_in_the_copy_job():
    job = copyjobs.build_sql_database_copy_job(
        source_workspace_id=SOURCE_WS,
        source_item_id=SOURCE_DB,
        source_connection_id="c1",
        target_workspace_id=TARGET_WS,
        target_item_id=TARGET_DB,
        target_connection_id="c2",
        tables=[TableRef(name="Orders", schema="sales")],
    )
    settings = job["activities"][0]["properties"]["source"]["datasetSettings"]
    assert settings == {"table": "Orders", "schema": "sales"}
