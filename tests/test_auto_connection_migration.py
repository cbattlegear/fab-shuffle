"""Rebind only SQL connections whose endpoint and credential provenance are known."""

from types import SimpleNamespace

import pytest

from fabshuffle import journal, orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric import connections, sql_connections
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import decode_json_part, part, rewrite_parts
from fabshuffle.run import MigrationRun

SOURCE_CONNECTION = "11111111-1111-1111-1111-111111111111"
TARGET_CONNECTION = "22222222-2222-2222-2222-222222222222"
SOURCE_DB = "33333333-3333-3333-3333-333333333333"
TARGET_DB = "44444444-4444-4444-4444-444444444444"
SOURCE = {
    "id": SOURCE_DB, "type": "SQLDatabase", "displayName": "Orders",
    "properties": {"serverFqdn": "source.example.com,1433", "databaseName": "Orders-123"},
}
TARGET = {
    "id": TARGET_DB,
    "properties": {"serverFqdn": "target.example.com,1433", "databaseName": "Orders-456"},
}


def connection(path="source.example.com;Orders-123", **fields):
    return {
        "id": SOURCE_CONNECTION, "displayName": "Orders source", "connectivityType": "ShareableCloud",
        "myRole": "Owner", "privacyLevel": "Organizational",
        "credentialDetails": {"credentialType": "ServicePrincipal", "singleSignOnType": "None"},
        "connectionDetails": {"type": "SQL", "path": path}, **fields,
    }


class Client:
    def get(self, path):
        assert path == f"connections/{SOURCE_CONNECTION}"
        return connection()


def context(tmp_path, prior=None):
    book = journal.Journal(tmp_path / ("resume.jsonl" if prior else "first.jsonl"))
    book.run_created({"source_workspace_id": "workspace"}, cleanup=False, prior=prior)
    run = MigrationRun(source_workspace_name="source", capacity_name="capacity")
    run.lineage_id = "lineage"
    ctx = orchestrator._Context(
        client=Client(), tokens=object(), principal=ServicePrincipal("tenant", "app", "secret"),
        plan=orchestrator.MigrationPlan(
            capacity_id="cap", capacity_name="F64", capacity_region="westus",
            source_workspace_id="workspace", source_workspace_name="source", target_workspace_name="target",
        ),
        run=run, scratch_dir=tmp_path, target_workspace_id="target-workspace", journal=book,
        prior=prior, id_map={SOURCE_DB: TARGET_DB},
    )
    ctx.source_items.update({SOURCE_DB: SOURCE, SOURCE_CONNECTION: {
        **connection(), "type": "Connection",
    }})
    ctx.bound_connections[SOURCE_CONNECTION] = connection()
    ctx.connection_sql_owners[SOURCE_CONNECTION] = {SOURCE_DB}
    return ctx


def names():
    return (
        sql_connections.migration_connection_name("lineage", SOURCE_DB, TARGET_DB, "source"),
        sql_connections.migration_connection_name("lineage", SOURCE_DB, TARGET_DB, "destination"),
    )


def test_exact_sql_endpoint_matching_ignores_names_and_rejects_ambiguous_pairs():
    assert connections.source_sql_owners(connection(), {SOURCE_DB: SOURCE}) == {SOURCE_DB}
    assert not connections.source_sql_owners(connection("other.example.com;Orders-123"), {SOURCE_DB: SOURCE})
    assert connections.source_sql_owners(connection("source.example.com"), {SOURCE_DB: SOURCE}) is None
    duplicate = {**SOURCE, "id": "different", "displayName": "Different"}
    assert connections.source_sql_owners(connection(), {SOURCE_DB: SOURCE, "different": duplicate}) == {
        SOURCE_DB, "different",
    }


def test_dependency_scan_traces_copy_job_and_notebook_connections(tmp_path, monkeypatch):
    ctx = context(tmp_path)
    external_id = "55555555-5555-5555-5555-555555555555"
    external = connection("external.example.com;Orders-123", id=external_id)
    ctx.client.list_all = lambda path: [connection(), external] if path == "connections" else []
    ctx.source_items = {SOURCE_DB: SOURCE}
    ctx.assessment = SimpleNamespace(migrated=[
        {"id": "copy", "type": "CopyJob"}, {"id": "notebook", "type": "Notebook"},
    ])
    monkeypatch.setattr(orchestrator.sqldatabases, "list_sql_databases", lambda *_: [SOURCE])
    monkeypatch.setattr(orchestrator.items_module, "try_get_item_definition", lambda _c, _w, item: {
        "parts": [part("content.json", {
            "connectionId": SOURCE_CONNECTION if item == "copy" else external_id,
        })],
    })
    orchestrator._load_source_references(ctx)
    assert ctx.connection_sql_owners[SOURCE_CONNECTION] == {SOURCE_DB}
    assert ctx.source_items[SOURCE_CONNECTION]["type"] == "Connection"
    assert external_id in ctx.bound_connections and external_id not in ctx.source_items


def test_dependency_scan_does_not_continue_after_definition_read_fails(tmp_path, monkeypatch):
    ctx = context(tmp_path)
    ctx.client.list_all = lambda _path: []
    ctx.assessment = SimpleNamespace(migrated=[{"id": "copy", "type": "CopyJob"}])
    monkeypatch.setattr(orchestrator.sqldatabases, "list_sql_databases", lambda *_: [SOURCE])
    def denied(*_args):
        raise FabricApiError("POST", "definition", 403, '{"errorCode":"Denied","message":"grant access"}')
    monkeypatch.setattr(orchestrator.items_module, "try_get_item_definition", denied)
    with pytest.raises(orchestrator.FabricError, match=r"Denied.*grant access"):
        orchestrator._load_source_references(ctx)


@pytest.mark.parametrize("prior_reference_type", ["Connection", "ExternalReference"])
@pytest.mark.parametrize("external_path", [
    "battlecabbagevideo.database.windows.net;BattleCabbageReplTest",
    "source.example.com;Orders-456",
])
def test_missing_mirror_metadata_does_not_veto_external_or_exact_sql_connections(
    tmp_path, monkeypatch, prior_reference_type, external_path,
):
    ctx = context(tmp_path)
    external_id = "55555555-5555-5555-5555-555555555555"
    external = connection(external_path, id=external_id, displayName="fab-sql-dest")
    ctx.client.list_all = lambda path: [connection(), external] if path == "connections" else []
    mirror = {"id": "mirror", "type": "MirroredDatabase", "displayName": "BattleCabbageReplTest"}
    ctx.source_items.update({mirror["id"]: mirror, external_id: {**external, "type": prior_reference_type}})
    ctx.id_map[external_id] = TARGET_CONNECTION
    ctx.assessment = SimpleNamespace(migrated=[{"id": "copy", "type": "CopyJob"}, mirror])
    monkeypatch.setattr(orchestrator.sqldatabases, "list_sql_databases", lambda *_: [SOURCE])
    monkeypatch.setattr(orchestrator.data_stores, "list_mirrored_databases", lambda *_: [mirror])
    monkeypatch.setattr(orchestrator.items_module, "try_get_item_definition", lambda _c, _w, item: {
        "parts": [part("content.json", {
            "connectionId": SOURCE_CONNECTION if item == "copy" else external_id,
        })],
    })
    orchestrator._load_source_references(ctx)
    assert external_id not in ctx.source_items and external_id not in ctx.id_map
    assert ctx.connection_sql_owners[SOURCE_CONNECTION] == {SOURCE_DB}
    monkeypatch.setattr(sql_connections, "ensure_connection", lambda *a, **k: TARGET_CONNECTION)
    monkeypatch.setattr(connections, "reconcile_role_assignments", lambda *a, **k: [])
    monkeypatch.setattr(orchestrator.sqldatabases, "get_sql_database", lambda *_: TARGET)
    orchestrator._validate_connection_mappings(ctx)
    assert ctx.id_map[SOURCE_CONNECTION] == TARGET_CONNECTION
    assert external_id not in ctx.id_map


@pytest.mark.parametrize("server_known", [True, False])
def test_catalog_matching_source_guid_needs_relevant_server_evidence(tmp_path, monkeypatch, server_known):
    ctx = context(tmp_path)
    external = connection(f"external.example.com;{SOURCE_DB}")
    ctx.client.list_all = lambda path: [external] if path == "connections" else []
    source = SOURCE if server_known else {**SOURCE, "properties": {}}
    ctx.source_items = {SOURCE_DB: source}
    ctx.assessment = SimpleNamespace(migrated=[{"id": "copy", "type": "CopyJob"}])
    monkeypatch.setattr(orchestrator.sqldatabases, "list_sql_databases", lambda *_: [source])
    monkeypatch.setattr(orchestrator.items_module, "try_get_item_definition", lambda *_: {
        "parts": [part("content.json", {"connectionId": SOURCE_CONNECTION})],
    })
    orchestrator._load_source_references(ctx)
    assert (SOURCE_CONNECTION in ctx.source_items) is not server_known
    assert SOURCE_CONNECTION not in ctx.id_map


def test_owned_sql_connection_reused_for_multiple_consumers_and_resume(tmp_path, monkeypatch):
    ctx = context(tmp_path)
    source_name, target_name = names()
    ctx.journal.sql_connection(source_name, SOURCE_CONNECTION)
    ctx.sql_connections[source_name] = SOURCE_CONNECTION
    calls = []
    def ensure(_client, _principal, *, name, recorded_id, record, **kwargs):
        calls.append((name, recorded_id, kwargs["server"], kwargs["database"]))
        if recorded_id is None:
            record(TARGET_CONNECTION)
        else:
            assert recorded_id == TARGET_CONNECTION
        return TARGET_CONNECTION
    monkeypatch.setattr(sql_connections, "ensure_connection", ensure)
    monkeypatch.setattr(connections, "reconcile_role_assignments", lambda *a, **k: [])
    monkeypatch.setattr(orchestrator.sqldatabases, "get_sql_database", lambda *_: TARGET)
    orchestrator._validate_connection_mappings(ctx)
    assert ctx.id_map[SOURCE_CONNECTION] == TARGET_CONNECTION
    for payload in (
        {"externalReferences": {"connection": SOURCE_CONNECTION}},
        {"activities": [{"connectionId": SOURCE_CONNECTION}]},
    ):
        rewritten, _ = rewrite_parts([part("content.json", payload)], ctx.id_map)
        assert TARGET_CONNECTION in str(decode_json_part(rewritten[0]["payload"]))
    prior = journal.read(ctx.journal.path)
    resumed = context(tmp_path, prior)
    orchestrator._validate_connection_mappings(resumed)
    assert resumed.id_map[SOURCE_CONNECTION] == TARGET_CONNECTION
    assert calls[-1] == (target_name, TARGET_CONNECTION, "target.example.com,1433", "Orders-456")
    assert "secret" not in ctx.journal.path.read_text()


@pytest.mark.parametrize("case", ["ambiguous", "unsupported", "cross_tenant", "retargeted"])
def test_unverifiable_connection_never_gets_a_replacement(tmp_path, monkeypatch, case):
    ctx = context(tmp_path)
    if case == "ambiguous":
        ctx.connection_sql_owners[SOURCE_CONNECTION] = {SOURCE_DB, "another"}
    if case == "unsupported":
        ctx.connection_sql_owners[SOURCE_CONNECTION] = None
    if case == "cross_tenant":
        monkeypatch.setattr(type(ctx.plan), "cross_tenant", property(lambda _: True))
    if case == "retargeted":
        monkeypatch.setattr(ctx.client, "get", lambda _: connection(
            "other.example.com;Orders-123",
        ))
    monkeypatch.setattr(sql_connections, "ensure_connection", lambda *a, **k: pytest.fail("must not create"))
    orchestrator._validate_connection_mappings(ctx)
    assert SOURCE_CONNECTION not in ctx.id_map
    assert ctx.warnings and "could not be migrated" in ctx.warnings[0]


def test_service_error_invalidates_stale_mapping_without_replacing_identity(tmp_path, monkeypatch):
    ctx = context(tmp_path)
    source_name, _ = names()
    ctx.sql_connections[source_name] = SOURCE_CONNECTION
    ctx.id_map[SOURCE_CONNECTION] = TARGET_CONNECTION
    def denied(*_args, **_kwargs):
        raise FabricApiError("GET", "connections", 403, '{"errorCode":"Denied","message":"grant access"}')
    monkeypatch.setattr(sql_connections, "ensure_connection", denied)
    monkeypatch.setattr(orchestrator.sqldatabases, "get_sql_database", lambda *_: TARGET)
    orchestrator._validate_connection_mappings(ctx)
    assert SOURCE_CONNECTION not in ctx.id_map
    assert any("Denied" in warning and "grant access" in warning for warning in ctx.warnings)


def test_preexisting_sql_connection_uses_distinct_replacement_and_reports_identity(tmp_path, monkeypatch):
    ctx = context(tmp_path)
    calls = []
    def ensure(_client, _principal, *, name, recorded_id, record, **_kwargs):
        calls.append(name)
        record(TARGET_CONNECTION)
        return TARGET_CONNECTION
    monkeypatch.setattr(sql_connections, "ensure_connection", ensure)
    monkeypatch.setattr(connections, "reconcile_role_assignments", lambda *a, **k: [])
    monkeypatch.setattr(orchestrator.sqldatabases, "get_sql_database", lambda *_: TARGET)
    orchestrator._validate_connection_mappings(ctx)
    assert ctx.id_map[SOURCE_CONNECTION] == TARGET_CONNECTION
    assert calls == [sql_connections.migration_connection_name(
        "lineage", SOURCE_DB, TARGET_DB, f"binding-{SOURCE_CONNECTION}",
    )]
    warning = next(warning for warning in ctx.warnings if "Created replacement connection" in warning)
    assert "supplied service principal app" in warning
    assert TARGET_CONNECTION in warning
    assert "original credentials and connection settings were not preserved" in warning
    assert "Settings > Manage connections and gateways" in warning


@pytest.mark.parametrize(
    "fields",
    [
        {"credentialDetails": {"credentialType": "Basic", "singleSignOnType": "Prompt"}},
        {"credentialDetails": {"credentialType": "OAuth2"}},
        {"credentialDetails": {}},
        {"credentialDetails": None, "privacyLevel": None},
        {"connectivityType": "OnPremisesGateway"},
        {"privacyLevel": "Private"},
        {"allowUsageInUserControlledCode": True},
        {"allowConnectionUsageInGateway": True},
    ],
)
def test_exact_source_endpoint_rebinds_different_original_settings_without_copying_roles(
    tmp_path, monkeypatch, fields,
):
    ctx = context(tmp_path)
    original = connection(**fields)
    monkeypatch.setattr(ctx.client, "get", lambda path: original)
    monkeypatch.setattr(sql_connections, "ensure_connection", lambda *a, **k: TARGET_CONNECTION)
    monkeypatch.setattr(
        connections, "reconcile_role_assignments",
        lambda *a, **k: pytest.fail("changed connection identity must not copy original role grants"),
    )
    monkeypatch.setattr(orchestrator.sqldatabases, "get_sql_database", lambda *_: TARGET)
    orchestrator._validate_connection_mappings(ctx)
    assert ctx.id_map[SOURCE_CONNECTION] == TARGET_CONNECTION
    warning = next(warning for warning in ctx.warnings if "Created replacement connection" in warning)
    assert TARGET_CONNECTION in warning
    assert "supplied service principal app" in warning
    assert "Manage connections and gateways" in warning


def test_previously_eligible_source_settings_keep_role_reconciliation(tmp_path, monkeypatch):
    ctx = context(tmp_path)
    calls = []
    monkeypatch.setattr(sql_connections, "ensure_connection", lambda *a, **k: TARGET_CONNECTION)
    monkeypatch.setattr(
        connections, "reconcile_role_assignments", lambda *a, **k: calls.append(a) or [],
    )
    monkeypatch.setattr(orchestrator.sqldatabases, "get_sql_database", lambda *_: TARGET)
    orchestrator._validate_connection_mappings(ctx)
    assert len(calls) == 1


def test_preexisting_connection_creation_is_journaled_and_reused_on_resume(tmp_path, monkeypatch):
    class Fabric:
        def __init__(self):
            self.created = None
            self.posts = []

        def get(self, path):
            if path == f"connections/{SOURCE_CONNECTION}":
                return connection(
                    credentialDetails={"credentialType": "OAuth2", "singleSignOnType": "Prompt"},
                    connectivityType="OnPremisesGateway",
                    privacyLevel="Private",
                    allowUsageInUserControlledCode=True,
                    allowConnectionUsageInGateway=True,
                )
            if path == f"connections/{TARGET_CONNECTION}":
                return self.created
            raise AssertionError(path)

        def list_all(self, path):
            if path == "connections":
                return [connection()] + ([self.created] if self.created else [])
            if path.endswith("/roleAssignments"):
                return []
            raise AssertionError(path)

        def post(self, path, json):
            assert path == "connections"
            self.posts.append(json)
            self.created = {
                "id": TARGET_CONNECTION, "displayName": json["displayName"],
                "connectivityType": "ShareableCloud", "myRole": "Owner",
                "credentialDetails": {"credentialType": "ServicePrincipal"},
                "connectionDetails": {"type": "SQL", "path": "target.example.com,1433;Orders-456"},
            }
            return self.created

    client = Fabric()
    ctx = context(tmp_path)
    ctx.client = client
    monkeypatch.setattr(orchestrator.sqldatabases, "get_sql_database", lambda *_: TARGET)
    orchestrator._validate_connection_mappings(ctx)
    assert ctx.id_map[SOURCE_CONNECTION] == TARGET_CONNECTION
    assert client.posts[0]["credentialDetails"]["credentials"]["servicePrincipalClientId"] == "app"
    assert client.posts[0]["connectivityType"] == "ShareableCloud"
    assert client.posts[0]["privacyLevel"] == "Organizational"
    assert client.posts[0]["credentialDetails"]["singleSignOnType"] == "None"
    assert not client.posts[0].get("allowUsageInUserControlledCode", False)
    assert not client.posts[0].get("allowConnectionUsageInGateway", False)
    assert "secret" not in ctx.journal.path.read_text()
    resumed = context(tmp_path, journal.read(ctx.journal.path))
    resumed.client = client
    orchestrator._validate_connection_mappings(resumed)
    assert resumed.id_map[SOURCE_CONNECTION] == TARGET_CONNECTION
    assert len(client.posts) == 1


@pytest.mark.parametrize("changed", ["source", "target"])
def test_changed_endpoint_refuses_provisioning(tmp_path, monkeypatch, changed):
    ctx = context(tmp_path)
    if changed == "source":
        monkeypatch.setattr(ctx.client, "get", lambda _: connection("other.example.com;Orders-123"))
    monkeypatch.setattr(
        orchestrator.sqldatabases, "get_sql_database",
        lambda *_: {**TARGET, "id": "different"} if changed == "target" else TARGET,
    )
    monkeypatch.setattr(sql_connections, "ensure_connection", lambda *a, **k: pytest.fail("must not create"))
    orchestrator._validate_connection_mappings(ctx)
    assert SOURCE_CONNECTION not in ctx.id_map
    assert ctx.warnings


def test_role_reconciliation_skips_existing_and_reports_failed_grants():
    class RoleClient:
        def __init__(self):
            self.posts = []

        def list_all(self, path):
            if SOURCE_CONNECTION in path:
                return [
                    {"principal": {"id": "first", "type": "User"}, "role": "User"},
                    {"principal": {"id": "second", "type": "Group"}, "role": "User"},
                ]
            return [{"principal": {"id": "first", "type": "User"}, "role": "User"}]

        def post(self, path, json):
            self.posts.append((path, json))
            raise FabricApiError("POST", path, 403, '{"errorCode":"Denied","message":"cannot share"}')

    client = RoleClient()
    warnings = connections.reconcile_role_assignments(
        client, SOURCE_CONNECTION, TARGET_CONNECTION, client_id="app",
    )
    assert [entry[1]["principal"]["id"] for entry in client.posts] == ["second"]
    assert "Denied" in warnings[0] and "cannot share" in warnings[0]
