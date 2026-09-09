"""Exercise the full rebuild with disjoint tenants, not a client that accepts both sides."""

from __future__ import annotations

import json

import pytest

from fabshuffle import journal, orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.config import SETTINGS
from fabshuffle.fabric import migration_refs
from fabshuffle.fabric.definitions import decode_json_part, decode_payload, part
from fabshuffle.fabric.support import Strategy, assess_workspace
from fabshuffle.run import MigrationRun, RunStatus

SOURCE = ServicePrincipal("aaaaaaaa-1111-2222-3333-444444444444", "source-app", "source-secret")
TARGET = ServicePrincipal("bbbbbbbb-1111-2222-3333-444444444444", "target-app", "target-secret")
SOURCE_WS = "11111111-1111-1111-1111-111111111111"
TARGET_WS = "22222222-2222-2222-2222-222222222222"
ITEM = "33333333-3333-3333-3333-333333333333"
NEW_ITEM = "44444444-4444-4444-4444-444444444444"
CONNECTION = "55555555-5555-5555-5555-555555555555"
NEW_CONNECTION = "66666666-6666-6666-6666-666666666666"


def connection(identifier):
    return {
        "id": identifier, "displayName": "External source", "connectivityType": "ShareableCloud",
        "connectionDetails": {"type": "Web", "path": "https://data.example.test"},
        "credentialDetails": {"credentialType": "Anonymous"},
    }


class Tokens:
    def __init__(self, principal):
        self.principal = principal

    def tenant_id(self):
        return self.principal.tenant_id


class Fabric:
    def __init__(self, side, *, item_type="Notebook", uses_connection=False):
        self.side = side
        self.item_type = item_type
        self.uses_connection = uses_connection
        self.calls = []
        self.created = []
        self.parts = [part("notebook-content.py", "# independent notebook")]
        if uses_connection:
            self.parts = [part("pipeline-content.json", {
                "properties": {"activities": [{"externalReferences": {"connection": CONNECTION}}]},
            })]

    @property
    def workspace(self):
        return SOURCE_WS if self.side == "source" else TARGET_WS

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return None

    def _record(self, method, path):
        self.calls.append((method, path))
        other = TARGET_WS if self.side == "source" else SOURCE_WS
        assert other not in path, f"{self.side} client used for {path}"

    def get(self, path, params=None):
        self._record("GET", path)
        if path.endswith("/relations/upstream"):
            return {"items": [], "relations": [], "workspaces": []}
        if path.endswith("/spark/settings"):
            return {}
        if path == f"connections/{CONNECTION}" and self.side == "source":
            return connection(CONNECTION)
        if path == f"connections/{NEW_CONNECTION}" and self.side == "target":
            return connection(NEW_CONNECTION)
        raise AssertionError(f"Unexpected {self.side} GET {path}")

    def list_all(self, path, params=None, value_key="value"):
        self._record("GET", path)
        if path == "connections":
            return [connection(CONNECTION if self.side == "source" else NEW_CONNECTION)] if (
                self.uses_connection
            ) else []
        if path == f"workspaces/{SOURCE_WS}/items":
            item_type = (params or {}).get("type")
            return [{"id": ITEM, "displayName": "Independent", "type": self.item_type}] if (
                not item_type or item_type == self.item_type
            ) else []
        if path.startswith(f"workspaces/{self.workspace}/"):
            assert not path.endswith("/roleAssignments"), "Cross-tenant run read workspace RBAC to replay it"
            return []
        raise AssertionError(f"Unexpected {self.side} listing {path}")

    def paged(self, path, params=None, value_key="value"):
        yield from self.list_all(path, params=params, value_key=value_key)

    def post(self, path, json=None, params=None, wait=True):
        self._record("POST", path)
        if self.side == "source":
            assert path.endswith("/getDefinition"), f"Source mutation: {path}"
            return {"definition": {"parts": self.parts}}
        if path == "workspaces":
            self.created.append((path, json))
            return {"id": TARGET_WS, "displayName": json["displayName"]}
        if path == f"workspaces/{TARGET_WS}/items":
            self.created.append((path, json))
            return {"id": NEW_ITEM, "displayName": json["displayName"], "type": json["type"]}
        if path == f"workspaces/{TARGET_WS}/items/{NEW_ITEM}/updateDefinition":
            self.created.append((path, json))
            return {}
        raise AssertionError(f"Unexpected target mutation {path}")

    def delete(self, path):
        raise AssertionError(f"Unexpected {self.side} deletion: {path}")


def plan(**options):
    values = {
        "capacity_id": "target-cap", "capacity_name": "Target capacity", "capacity_region": "westeurope",
        "source_workspace_id": SOURCE_WS, "source_workspace_name": "Source",
        "target_workspace_name": "Source-copy", "strategy": Strategy.REBUILD,
        "source_tenant_id": SOURCE.tenant_id, "target_tenant_id": TARGET.tenant_id,
        "source_client_id": SOURCE.client_id, "target_client_id": TARGET.client_id,
        "include_data": False, "include_files": False,
    }
    return orchestrator.MigrationPlan(**{**values, **options})


def install(monkeypatch, *, uses_connection=False):
    source = Fabric("source", item_type="DataPipeline" if uses_connection else "Notebook",
                    uses_connection=uses_connection)
    target = Fabric("target", uses_connection=uses_connection)
    monkeypatch.setattr(orchestrator, "TokenProvider", Tokens)
    monkeypatch.setattr(
        orchestrator, "FabricClient", lambda tokens: source if tokens.principal == SOURCE else target,
    )
    return source, target


def test_full_rebuild_exports_from_source_creates_only_in_destination(monkeypatch):
    source, target = install(monkeypatch)
    run = MigrationRun(source_workspace_name="Source", capacity_name="Target capacity")
    orchestrator.run_migration(run, SOURCE, plan(), target_principal=TARGET)
    assert run.status is RunStatus.SUCCEEDED, run.error
    assert run.target_workspace["id"] == TARGET_WS
    assert run.scratch_workspace is None
    assert [path for path, _body in target.created] == ["workspaces", f"workspaces/{TARGET_WS}/items"]
    assert any(path.endswith("/getDefinition") for _method, path in source.calls)
    assert all(method == "GET" or path.endswith("/getDefinition") for method, path in source.calls)
    book = SETTINGS.journal_for_plan(run.id, run.plan)
    text = book.read_text(encoding="utf-8")
    assert SOURCE.tenant_id in text and TARGET.tenant_id in text
    assert SOURCE.client_secret not in text and TARGET.client_secret not in text
    assert not SETTINGS.journal_for(run.id).exists()


def test_cross_tenant_bound_connection_uses_operator_destination_mapping(monkeypatch):
    _source, target = install(monkeypatch, uses_connection=True)
    run = MigrationRun(source_workspace_name="Source", capacity_name="Target capacity")
    orchestrator.run_migration(
        run, SOURCE, plan(connection_mappings={CONNECTION: NEW_CONNECTION}), target_principal=TARGET,
    )
    assert run.status is RunStatus.SUCCEEDED, run.error
    definitions = [body["definition"]["parts"] for path, body in target.created if path.endswith("/items")]
    assert len(definitions) == 1, run.summary
    text = "\n".join(decode_payload(entry["payload"]).decode() for entry in definitions[0])
    assert NEW_CONNECTION in text
    assert CONNECTION not in text
    assert not any("roleAssignments" in path for _method, path in target.calls)


def test_missing_connection_mapping_never_creates_source_bound_consumer(monkeypatch):
    _source, target = install(monkeypatch, uses_connection=True)
    run = MigrationRun(source_workspace_name="Source", capacity_name="Target capacity")
    orchestrator.run_migration(run, SOURCE, plan(), target_principal=TARGET)
    assert not any(path.endswith("/items") for path, _body in target.created)
    assert "connection" in json.dumps(run.summary).lower()


@pytest.mark.parametrize("option", [
    {"include_data": True},
    {"include_files": True},
])
def test_cross_tenant_data_requires_write_freeze_before_creating_resources(monkeypatch, option):
    source, target = install(monkeypatch)
    run = MigrationRun(source_workspace_name="Source", capacity_name="Target capacity")
    with pytest.raises(ValueError, match="write freeze"):
        orchestrator.run_migration(run, SOURCE, plan(**option), target_principal=TARGET)
    assert not source.calls and not target.calls
    assert not SETTINGS.journal_for_plan(run.id, orchestrator._plan_record(plan(**option))).exists()


def test_swapped_credentials_never_reach_a_fabric_client(monkeypatch):
    source, target = install(monkeypatch)
    run = MigrationRun(source_workspace_name="Source", capacity_name="Target capacity")
    with pytest.raises(ValueError, match="credentials do not match"):
        orchestrator.run_migration(run, TARGET, plan(), target_principal=SOURCE)
    assert not source.calls and not target.calls


def test_external_mapping_checks_both_sides_and_does_not_match_by_display_name():
    class Client:
        def __init__(self, ws, item_id):
            self.ws = ws
            self.item_id = item_id
            self.calls = []

        def get(self, path):
            self.calls.append(path)
            assert path == f"workspaces/{self.ws}/items/{self.item_id}"
            return {"id": self.item_id, "type": "SemanticModel", "displayName": "Identical name"}

    source, target = Client("external-source", ITEM), Client("external-target", NEW_ITEM)
    mappings, items = migration_refs.resolve(source, target, [{
        "source_workspace_id": source.ws, "source_item_id": ITEM,
        "target_workspace_id": target.ws, "target_item_id": NEW_ITEM,
    }], migrating_workspace_id=SOURCE_WS)
    assert mappings[source.ws] == target.ws
    assert mappings[ITEM] == NEW_ITEM
    assert items[ITEM]["id"] == ITEM
    assert source.calls and target.calls


def test_named_onelake_references_map_both_workspace_and_item_to_guids():
    item = {"id": ITEM, "type": "Lakehouse", "displayName": "Orders"}
    roots = migration_refs.onelake_aliases(SOURCE_WS, "Sales data", item, TARGET_WS, NEW_ITEM)
    named = "https://onelake.dfs.fabric.microsoft.com/Sales%20data/Orders.Lakehouse/"
    assert roots[named] == f"https://onelake.dfs.fabric.microsoft.com/{TARGET_WS}/{NEW_ITEM}/"
    assert f"https://onelake.dfs.fabric.microsoft.com/{SOURCE_WS}/Orders.Lakehouse/" not in roots
    assert all("Orders" not in destination for destination in roots.values())


def test_removing_connection_mapping_invalidates_inherited_consumers(monkeypatch, tmp_path):
    source, target = install(monkeypatch, uses_connection=True)
    item = {"id": ITEM, "displayName": "Consumer", "type": "DataPipeline"}
    ctx = orchestrator._Context(
        source, Tokens(SOURCE), SOURCE, plan(),
        MigrationRun(source_workspace_name="Source", capacity_name="Destination"), tmp_path,
        target_client=target, source_items={ITEM: item},
        prior=journal.Replay(plan={"connection_mappings": {CONNECTION: NEW_CONNECTION}}),
        id_map={SOURCE_WS: TARGET_WS, ITEM: NEW_ITEM, CONNECTION: NEW_CONNECTION},
        adopted_targets={ITEM: NEW_ITEM},
        assessment=assess_workspace([item], force_rebuild=True),
    )
    orchestrator._load_source_references(ctx)
    assert CONNECTION not in ctx.id_map
    assert ITEM in ctx.refresh_needed
    assert ctx.to_migrate([item]) == [item]


def test_removed_external_mapping_does_not_retain_unattributed_endpoint_alias(monkeypatch, tmp_path):
    source, target = install(monkeypatch)
    item = {"id": ITEM, "displayName": "Consumer", "type": "Notebook"}
    previous = [{
        "source_workspace_id": "external-source", "source_item_id": "external-item",
        "target_workspace_id": "external-target", "target_item_id": "external-copy",
    }]
    ctx = orchestrator._Context(
        source, Tokens(SOURCE), SOURCE, plan(),
        MigrationRun(source_workspace_name="Source", capacity_name="Destination"), tmp_path,
        target_client=target, source_items={ITEM: item},
        prior=journal.Replay(plan={"reference_mappings": previous}),
        id_map={SOURCE_WS: TARGET_WS, ITEM: NEW_ITEM, "old.sql.example": "new.sql.example"},
        adopted_targets={ITEM: NEW_ITEM},
        assessment=assess_workspace([item], force_rebuild=True),
    )
    orchestrator._load_source_references(ctx)
    assert "old.sql.example" not in ctx.id_map
    assert "old.sql.example" in ctx.source_items
    assert ITEM in ctx.refresh_needed


def test_kql_parent_is_validated_in_source_form_before_retargeting(monkeypatch, tmp_path):
    source, target = install(monkeypatch)
    source_house = "77777777-7777-7777-7777-777777777777"
    target_house = "88888888-8888-8888-8888-888888888888"
    database = {"id": ITEM, "displayName": "Database", "type": "KQLDatabase"}
    ctx = orchestrator._Context(
        source, Tokens(SOURCE), SOURCE, plan(),
        MigrationRun(source_workspace_name="Source", capacity_name="Destination"), tmp_path,
        target_client=target, target_principal=TARGET, target_tokens=Tokens(TARGET),
        target_workspace_id=TARGET_WS,
        id_map={SOURCE_WS: TARGET_WS, source_house: target_house},
        source_items={source_house: {"id": source_house, "displayName": "House", "type": "Eventhouse"}},
    )
    monkeypatch.setattr(
        orchestrator.eventhouses, "kql_database_definition_parts",
        lambda *a: [part("DatabaseProperties.json", {"parentEventhouseItemId": source_house})],
    )
    monkeypatch.setattr(
        orchestrator.eventhouses, "create_kql_database", lambda *a, **kw: {"id": NEW_ITEM},
    )
    applied = []

    def adopt(client, workspace, name, *, parts, **kwargs):
        assert client is target
        applied.append(parts)
        assert decode_json_part(parts[0]["payload"])["parentEventhouseItemId"] == target_house
        return {"id": NEW_ITEM}, True

    monkeypatch.setattr(orchestrator.eventhouses, "create_or_adopt_kql_database", adopt)
    monkeypatch.setattr(orchestrator.shortcuts, "list_table_shortcuts", lambda *a: [])
    monkeypatch.setattr(orchestrator.kql, "stop_database_update_policies", lambda **kw: [])
    moved, warnings, _adopted = orchestrator._migrate_kql_database(
        ctx, "eventhouses", database=database, target_eventhouse_id=target_house,
        source_query_uri="https://source.kusto.fabric.microsoft.com",
        target_query_uri="https://target.kusto.fabric.microsoft.com", existing_databases={},
    )
    assert moved and not warnings
    assert applied


def test_removed_endpoint_tombstone_survives_admission_crash_and_two_resumes(monkeypatch, tmp_path):
    original = plan(reference_mappings=[{
        "source_workspace_id": "external-source", "source_item_id": "external-item",
        "target_workspace_id": "external-target", "target_item_id": "external-copy",
    }])
    first = journal.Journal(tmp_path / "first.jsonl")
    first.run_created(orchestrator._plan_record(original), cleanup=True)
    first.workspace("target", TARGET_WS)
    first.item(ITEM, NEW_ITEM, "Notebook", "Consumer")
    first.mapping(SOURCE_WS, TARGET_WS)
    first.mapping("old.sql.example", "new.sql.example")
    changed = plan()
    second = journal.Journal(tmp_path / "second.jsonl")
    second.run_created(
        orchestrator._plan_record(changed), cleanup=True, prior=journal.read(first.path),
    )
    # No phase ran: admission must already retain removal intent atomically.
    replay = journal.read(second.path)
    assert replay.blocked_references["old.sql.example"] == "old.sql.example"
    assert "old.sql.example" not in replay.id_map
    assert ITEM in replay.refresh_needed
    third = journal.Journal(tmp_path / "third.jsonl")
    third.run_created(orchestrator._plan_record(changed), cleanup=True, prior=replay)
    replay = journal.read(third.path)
    source, target = install(monkeypatch)
    item = {"id": ITEM, "displayName": "Consumer", "type": "Notebook"}
    ctx = orchestrator._Context(
        source, Tokens(SOURCE), SOURCE, changed,
        MigrationRun(source_workspace_name="Source", capacity_name="Destination"), tmp_path,
        target_client=target, source_items={ITEM: item}, prior=replay,
        id_map=dict(replay.id_map), adopted_targets={ITEM: NEW_ITEM},
        assessment=assess_workspace([item], force_rebuild=True),
    )
    orchestrator._load_source_references(ctx)
    assert "old.sql.example" in ctx.source_items
    assert "old.sql.example" not in ctx.id_map
    needed = orchestrator.analytics.dangling_references(
        [part("notebook-content.py", 'server = "old.sql.example"')], ctx.id_map, ctx.source_items,
    )
    assert needed
    # A later verified replacement may reuse the identifier; the tombstone is history,
    # not a permanent ban on a destination mapping.
    third.mapping("old.sql.example", "verified.sql.example")
    fourth = journal.Journal(tmp_path / "fourth.jsonl")
    fourth.run_created(
        orchestrator._plan_record(changed), cleanup=True, prior=journal.read(third.path),
    )
    assert journal.read(fourth.path).id_map["old.sql.example"] == "verified.sql.example"


def test_admission_preserves_actual_definition_items_when_external_mapping_is_removed(monkeypatch, tmp_path):
    source, target = install(monkeypatch)
    original = plan(reference_mappings=[{
        "source_workspace_id": "external-source", "source_item_id": "external-item",
        "target_workspace_id": "external-target", "target_item_id": "external-copy",
    }])
    first = journal.Journal(tmp_path / "definition-first.jsonl")
    first.run_created(orchestrator._plan_record(original), cleanup=True)
    first.workspace("target", TARGET_WS)
    first.mapping(SOURCE_WS, TARGET_WS)
    first.mapping("old.sql.example", "new.sql.example")
    item = {"id": ITEM, "displayName": "Consumer", "type": "Notebook"}
    ctx = orchestrator._Context(
        source, Tokens(SOURCE), SOURCE, original,
        MigrationRun(source_workspace_name="Source", capacity_name="Destination"), tmp_path,
        target_client=target, target_workspace_id=TARGET_WS, journal=first,
        id_map={SOURCE_WS: TARGET_WS, "old.sql.example": "new.sql.example"},
    )
    orchestrator._migrate_definition_items(
        ctx, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        items=[item], item_type="Notebook", id_map=ctx.id_map,
    )
    prior = journal.read(first.path)
    assert ITEM in prior.items
    assert ITEM in prior.outcomes
    # Older generic adapters recorded only MAPPING and OUTCOME. Their real identities
    # must also survive upgrading into the new admission format.
    prior.items.clear()
    second = journal.Journal(tmp_path / "definition-second.jsonl")
    second.run_created(orchestrator._plan_record(plan()), cleanup=True, prior=prior)
    replay = journal.read(second.path)
    assert replay.id_map[ITEM] == NEW_ITEM
    assert ITEM in replay.refresh_needed
    assert "old.sql.example" not in replay.id_map
    target.created.clear()
    resumed = orchestrator._Context(
        source, Tokens(SOURCE), SOURCE, plan(),
        MigrationRun(source_workspace_name="Source", capacity_name="Destination"), tmp_path,
        target_client=target, target_workspace_id=TARGET_WS, journal=second, prior=replay,
        id_map=dict(replay.id_map), refresh_needed=set(replay.refresh_needed),
    )
    orchestrator._migrate_definition_items(
        resumed, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        items=[item], item_type="Notebook", id_map=resumed.id_map,
    )
    assert [path for path, _body in target.created] == [
        f"workspaces/{TARGET_WS}/items/{NEW_ITEM}/updateDefinition",
    ]
