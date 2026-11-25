"""Migrating a Fabric SQL database."""

from __future__ import annotations

from pathlib import Path

import pytest

from fabshuffle import journal, orchestrator
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


def make_ctx(client, monkeypatch, tables=(("dbo", "Orders"),), copied=None, *, provision=False):
    monkeypatch.setattr(orchestrator.sqlschema, "list_base_tables", lambda *a, **k: list(tables))
    def suspend(server, database, tokens, table_names, *, record, prior=None):
        state = prior or {"tables": table_names}
        if prior is None:
            record(state)
        return state
    monkeypatch.setattr(orchestrator.sqlschema, "suspend_copy_effects", suspend)
    if not provision:
        monkeypatch.setattr(
            orchestrator.sql_connections, "ensure_connection",
            lambda *args, name, **kwargs: "src-conn" if "-source-" in name else "dst-conn",
        )

    calls: list[dict] = []

    def run_copy_jobs(ctx, step, specs, what, **kwargs):
        calls.extend((spec, kwargs["before_submit"]) for spec in specs)
        return copied if copied is not None else []

    monkeypatch.setattr(orchestrator, "_run_copy_jobs", run_copy_jobs)
    client.bulk_copies = calls

    plan = orchestrator.MigrationPlan(
        capacity_id="cap",
        capacity_name="F64",
        capacity_region="westus",
        source_workspace_id=SOURCE_WS,
        source_workspace_name="src",
        target_workspace_name="dst",
        write_freeze_confirmed=True,
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
    assert ctx.id_map["RegionBounceTest-1111"] == "RegionBounceTest-2222"




# ------------------------------------------------------------------- the rows


def test_rows_use_sql_server_copy_job_between_migration_owned_connections(monkeypatch):
    client = FakeClient()
    orchestrator._migrate_sql_databases(make_ctx(client, monkeypatch))

    assert len(client.bulk_copies) == 1
    job = client.bulk_copies[0][0].content
    assert job["properties"]["source"]["connectionSettings"]["externalReferences"]["connection"] == "src-conn"
    destination = job["properties"]["destination"]["connectionSettings"]["externalReferences"]
    assert destination["connection"] == "dst-conn"
    assert job["activities"][0]["properties"]["destination"]["writeBehavior"] == "Append"
    assert job["activities"][0]["properties"]["source"]["datasetSettings"] == {
        "schema": "dbo", "table": "Orders",
    }


def test_per_table_failures_name_their_database(monkeypatch):
    client = FakeClient()
    ctx = make_ctx(client, monkeypatch, copied=["Rows for [dbo].[Orders] did not copy: nope"])
    orchestrator._migrate_sql_databases(ctx)

    assert step_of(ctx)["warnings"] == ["Rows for [dbo].[Orders] did not copy: nope"]


def test_submission_checks_connections_and_empty_target(monkeypatch):
    class Client(FakeClient):
        def get(self, path, params=None):
            if path == "connections/src-conn":
                return {"connectionDetails": {
                    "type": "SQL", "path": f"{SOURCE_SERVER};RegionBounceTest-1111",
                }}
            if path == "connections/dst-conn":
                return {"connectionDetails": {
                    "type": "SQL", "path": f"{TARGET_SERVER};RegionBounceTest-2222",
                }}
            return super().get(path, params=params)

    client = Client()
    ctx = make_ctx(client, monkeypatch)
    ctx.plan.write_freeze_confirmed = True
    checked = []
    monkeypatch.setattr(orchestrator.sqlschema, "require_empty_tables", lambda *args: checked.append(args))
    orchestrator._migrate_sql_databases(ctx)
    spec, preflight = client.bulk_copies[0]
    preflight(spec)
    assert checked == [(TARGET_SERVER, "RegionBounceTest-2222", ctx.tokens, [("dbo", "Orders")])] * 2


def test_sql_job_suspends_before_submit_and_restores_before_checkpoint(monkeypatch, tmp_path):
    class Client(FakeClient):
        def get(self, path, params=None):
            if path == "connections/src-conn":
                return {"connectionDetails": {
                    "type": "SQL", "path": f"{SOURCE_SERVER};RegionBounceTest-1111",
                }}
            if path == "connections/dst-conn":
                return {"connectionDetails": {
                    "type": "SQL", "path": f"{TARGET_SERVER};RegionBounceTest-2222",
                }}
            return super().get(path, params=params)

    runner = orchestrator._run_copy_jobs
    ctx = make_ctx(Client(), monkeypatch, tables=(("SalesLT", "Address"), ("SalesLT", "CustomerAddress")))
    monkeypatch.setattr(orchestrator, "_run_copy_jobs", runner)
    ctx.journal = journal.Journal(tmp_path / "run.jsonl")
    ctx.journal.run_created({"source_workspace_id": SOURCE_WS}, cleanup=False)
    events = []
    def suspend(server, database, tokens, tables, *, record, prior=None):
        record({"tables": tables})
        events.append("suspended")
        return {"tables": tables}
    def submit(*_):
        assert journal.read(ctx.journal.path).sql_effects[SOURCE_DB]["target"] == TARGET_DB
        assert events == ["suspended"]
        events.append("submitted")
        return orchestrator.copyjobs._CopyJobSubmission("instance", 0)
    def restore(*_args, **_kwargs):
        assert not journal.read(ctx.journal.path).data_is_done(SOURCE_DB, "tables")
        events.append("restored")
    monkeypatch.setattr(orchestrator.sqlschema, "suspend_copy_effects", suspend)
    monkeypatch.setattr(orchestrator.sqlschema, "restore_copy_effects", restore)
    monkeypatch.setattr(orchestrator.sqlschema, "require_empty_tables", lambda *_: None)
    monkeypatch.setattr(orchestrator.copyjobs, "create_copy_job", lambda *_: {"id": "copy"})
    monkeypatch.setattr(orchestrator.copyjobs, "_submit_copy_job", submit)
    monkeypatch.setattr(orchestrator.copyjobs, "_job_status", lambda *_: ("Completed", {}))
    monkeypatch.setattr(orchestrator.copyjobs.time, "sleep", lambda _: None)
    monkeypatch.setattr(orchestrator.copyjobs.SETTINGS, "copy_job_poll_seconds", 0)
    orchestrator._migrate_sql_databases(ctx)
    replay = journal.read(ctx.journal.path)
    assert events == ["suspended", "submitted", "restored"]
    assert replay.sql_effects == {}
    assert replay.data_is_done(SOURCE_DB, "tables")


def test_submission_rejects_mismatched_destination_without_freeze_gate(monkeypatch):
    class Client(FakeClient):
        def get(self, path, params=None):
            if path.startswith("connections/"):
                return {"connectionDetails": {
                    "type": "SQL", "path": f"{SOURCE_SERVER};RegionBounceTest-1111",
                }}
            return super().get(path, params=params)

    client = Client()
    ctx = make_ctx(client, monkeypatch)
    orchestrator._migrate_sql_databases(ctx)
    spec, preflight = client.bulk_copies[0]
    ctx.plan.write_freeze_confirmed = False
    with pytest.raises(orchestrator.sqlschema.SchemaTransferError, match="dst-conn"):
        preflight(spec)


@pytest.mark.parametrize("recorded_target", [TARGET_DB, "other-target", None])
def test_only_confirmed_matching_failed_job_can_truncate(monkeypatch, recorded_target):
    class Client(FakeClient):
        def get(self, path, params=None):
            if path == "connections/src-conn":
                return {"connectionDetails": {
                    "type": "SQL", "path": f"{SOURCE_SERVER};RegionBounceTest-1111",
                }}
            if path == "connections/dst-conn":
                return {"connectionDetails": {
                    "type": "SQL", "path": f"{TARGET_SERVER};RegionBounceTest-2222",
                }}
            return super().get(path, params=params)

    client = Client()
    ctx = make_ctx(client, monkeypatch)
    ctx.plan.write_freeze_confirmed = False
    orchestrator._migrate_sql_databases(ctx)
    spec, preflight = client.bulk_copies[0]
    key = (SOURCE_DB, "sql_retry", spec.fingerprint)
    if recorded_target:
        ctx.prior = journal.Replay(data_done={key}, data_targets={key: recorded_target})
    checked, truncated = [], []

    def check(*args):
        checked.append(args)
        if len(checked) == 1:
            raise orchestrator.sqlschema.NonEmptyTables("not empty")

    monkeypatch.setattr(orchestrator.sqlschema, "require_empty_tables", check)
    monkeypatch.setattr(orchestrator.sqlschema, "clear_failed_sql_copy", lambda *args: truncated.append(args))
    if recorded_target == TARGET_DB:
        preflight(spec)
        assert len(truncated) == 1 and len(checked) == 2
    else:
        with pytest.raises(orchestrator.sqlschema.SchemaTransferError, match="not empty"):
            preflight(spec)
        assert not truncated


def test_changed_target_is_not_written_or_truncated(monkeypatch):
    client = FakeClient()
    ctx = make_ctx(client, monkeypatch)
    ctx.plan.write_freeze_confirmed = True
    orchestrator._migrate_sql_databases(ctx)
    spec, preflight = client.bulk_copies[0]
    monkeypatch.setattr(
        orchestrator.sqldatabases, "get_sql_database",
        lambda *args: {**TARGET_ITEM, "properties": {"serverFqdn": "replacement", "databaseName": "other"}},
    )
    monkeypatch.setattr(
        orchestrator.sqlschema, "truncate_tables", lambda *args: pytest.fail("must not truncate"),
    )
    with pytest.raises(orchestrator.sqlschema.SchemaTransferError, match="target SQL database changed"):
        preflight(spec)


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


def test_automatic_connections_do_not_require_sql_write_freeze(monkeypatch):
    client = FakeClient()
    ctx = make_ctx(client, monkeypatch)
    ctx.plan.write_freeze_confirmed = False
    orchestrator._migrate_sql_databases(ctx)

    spec, preflight = client.bulk_copies[0]
    monkeypatch.setattr(orchestrator.sql_connections, "matches_endpoint", lambda *args: True)
    monkeypatch.setattr(orchestrator.sqlschema, "require_empty_tables", lambda *args: None)
    original_get = client.get
    def get(path, **kwargs):
        return {} if path.startswith("connections/") else original_get(path, **kwargs)

    client.get = get
    preflight(spec)


def test_unfinished_legacy_job_refuses_without_adopting_old_plan_connections(monkeypatch):
    client = FakeClient()
    ctx = make_ctx(client, monkeypatch, provision=True)
    ctx.prior = journal.Replay(plan={
        **orchestrator._plan_record(ctx.plan),
        "sql_copy_connections": {SOURCE_DB: {"source": "src-conn", "destination": "dst-conn"}},
    })
    ctx.plan = orchestrator.plan_from_journal(ctx.prior)
    ctx.active_copy_jobs[(SOURCE_DB, "CopyJob_SQLDatabase_RegionBounceTest")] = {
        "job": {"instance_id": "running"}, "target": TARGET_DB,
    }
    with pytest.raises(orchestrator.ResumeRefused, match="no recorded migration-owned source connection"):
        orchestrator._migrate_sql_databases(ctx)
    assert not client.bulk_copies
    assert not any(path.endswith("/updateDefinition") for path, _ in client.posts)


def test_automatically_provisions_both_connections_and_reuses_them(monkeypatch, tmp_path):
    class Client(FakeClient):
        def __init__(self):
            super().__init__()
            self.created = {}

        def list_all(self, path, **kwargs):
            if path == "connections":
                return list(self.created.values())
            return super().list_all(path, **kwargs)

        def post(self, path, json=None, **kwargs):
            if path != "connections":
                return super().post(path, json=json, **kwargs)
            assert any(p.endswith("/updateDefinition") for p, _ in self.posts)
            assert json["credentialDetails"]["credentials"] == {
                "credentialType": "ServicePrincipal", "tenantId": "tenant-1",
                "servicePrincipalClientId": "client-1", "servicePrincipalSecret": "shhh",
            }
            assert json["credentialDetails"]["skipTestConnection"] is False
            server, catalog = (parameter["value"] for parameter in json["connectionDetails"]["parameters"])
            cid = f"00000000-0000-0000-0000-{len(self.created) + 1:012d}"
            response = {
                "id": cid, "displayName": json["displayName"], "connectivityType": "ShareableCloud",
                "myRole": "Owner", "credentialDetails": {"credentialType": "ServicePrincipal"},
                "connectionDetails": {"type": "SQL", "path": f"{server};{catalog}"},
            }
            self.created[cid] = response
            return response

        def get(self, path, **kwargs):
            if path.startswith("connections/"):
                return self.created[path.split("/")[-1]]
            return super().get(path, **kwargs)

    client = Client()
    ctx = make_ctx(client, monkeypatch, provision=True)
    ctx.journal = journal.Journal(tmp_path / "run.jsonl")
    ctx.plan.write_freeze_confirmed = False
    orchestrator._migrate_sql_databases(ctx)
    spec, preflight = client.bulk_copies[0]
    monkeypatch.setattr(orchestrator.sqlschema, "require_empty_tables", lambda *args: None)
    preflight(spec)
    assert len(client.created) == 2
    assert "shhh" not in ctx.journal.path.read_text()
    ctx.journal.sql_effects(SOURCE_DB, TARGET_DB, None)
    ctx.sql_effects.clear()
    ctx.prior = journal.read(ctx.journal.path)
    assert set(ctx.prior.sql_connections.values()) == set(client.created)
    orchestrator._copy_sql_database_tables(ctx, "sqldatabases", [(SOURCE_ITEM, TARGET_ITEM)])
    assert len(client.created) == 2
    assert client.bulk_copies[-1][0].fingerprint == spec.fingerprint
    ctx.active_copy_jobs[(SOURCE_DB, spec.display_name)] = {"job": {"instance_id": "running"}}
    client.created.clear()
    ctx.plan.write_freeze_confirmed = False
    orchestrator._copy_sql_database_tables(ctx, "sqldatabases", [(SOURCE_ITEM, TARGET_ITEM)])
    assert not client.created
    assert client.bulk_copies[-1][0].fingerprint == spec.fingerprint


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


def test_resumed_first_database_uses_its_verified_schema_destination(monkeypatch):
    client = FakeClient()
    ctx = make_ctx(client, monkeypatch)
    ctx.prior = journal.Replay(id_map={SOURCE_DB: TARGET_DB})
    ctx.id_map.update(ctx.prior.id_map)
    orchestrator._migrate_sql_databases(ctx)
    assert not any(path.endswith("/sqlDatabases") for path, _ in client.posts)
    assert any(path == f"workspaces/{TARGET_WS}/sqlDatabases/{TARGET_DB}/updateDefinition"
               for path, _ in client.posts)
    assert client.bulk_copies[0][0].item_id == SOURCE_DB


def test_mixed_created_and_adopted_databases_do_not_share_schema_destinations(monkeypatch):
    client = FakeClient()
    ctx = make_ctx(client, monkeypatch)
    adopted = {**SOURCE_ITEM, "id": "adopted", "displayName": "Adopted"}
    monkeypatch.setattr(sqldatabases, "list_sql_databases", lambda *a: [SOURCE_ITEM, adopted])
    monkeypatch.setattr(
        sqldatabases, "get_sql_database",
        lambda client, workspace, item_id: {**TARGET_ITEM, "id": item_id},
    )
    schemas = []
    monkeypatch.setattr(
        sqldatabases, "copy_schema",
        lambda client, **kwargs: schemas.append((kwargs["source_id"], kwargs["target_id"])),
    )
    ctx.prior = journal.Replay(id_map={"adopted": "adopted-target"})
    ctx.id_map.update(ctx.prior.id_map)
    orchestrator._migrate_sql_databases(ctx)
    assert schemas == [(SOURCE_DB, TARGET_DB), ("adopted", "adopted-target")]
