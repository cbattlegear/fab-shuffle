from __future__ import annotations

import pytest

from fabshuffle import journal, orchestrator
from fabshuffle.fabric.definitions import part
from fabshuffle.run import MigrationRun
from fabshuffle.transfer import kql


def context(tmp_path, client, mappings):
    return orchestrator._Context(
        client=client, tokens=object(), principal=object(),
        plan=orchestrator.MigrationPlan(
            capacity_id="cap", capacity_name="F64", capacity_region="westus",
            source_workspace_id="source", source_workspace_name="source", target_workspace_name="target",
        ),
        run=MigrationRun(source_workspace_name="source", capacity_name="F64"),
        scratch_dir=tmp_path, target_workspace_id="target",
        prior=journal.Replay(id_map=dict(mappings)), id_map=mappings,
    )


class EventhouseClient:
    def __init__(self):
        self.created = []
        self.updated = []
        self.databases = {
            "db-old": {
                "id": "db-old", "displayName": "Telemetry", "properties": {"databaseType": "ReadWrite"},
            },
            "db-follower": {
                "id": "db-follower", "displayName": "Follow", "properties": {"databaseType": "Shortcut"},
            },
            "db-new": {
                "id": "db-new", "displayName": "Fresh", "properties": {"databaseType": "ReadWrite"},
            },
            "target-db": {
                "id": "target-db", "displayName": "RenamedTarget",
                "properties": {"databaseType": "ReadWrite"},
            },
        }

    def list_all(self, path, params=None, value_key="value"):
        if path == "workspaces/source/eventhouses":
            return [
                {"id": "eh-old", "displayName": "Original", "properties": {
                    "databasesItemIds": ["db-old", "db-follower"], "queryServiceUri": "https://source-old",
                    "ingestionServiceUri": "https://ingest-source",
                }},
                {"id": "eh-new", "displayName": "Fresh", "properties": {
                    "databasesItemIds": ["db-new"], "queryServiceUri": "https://source-new",
                }},
            ]
        return []

    def get(self, path, params=None):
        item_id = path.rsplit("/", 1)[-1]
        if "/eventhouses/" in path:
            return {"id": item_id, "properties": {
                "databasesItemIds": ["target-db"] if item_id == "target-eh" else [],
                "queryServiceUri": f"https://{item_id}", "ingestionServiceUri": "https://ingest-target",
            }}
        return self.databases[item_id]

    def post(self, path, json=None, params=None, wait=True):
        if path.endswith("/getDefinition"):
            return {"definition": {"parts": [
                part(
                    "DatabaseProperties.json", {"databaseType": "ReadWrite", "parentEventhouseItemId": "old"},
                ),
                part("DatabaseSchema.kql", ".create-merge table T (i:int)"),
            ]}}
        if path.endswith("/updateDefinition"):
            self.updated.append((path, json))
            return {}
        self.created.append((path, json))
        return {"id": "new-eh" if path.endswith("/eventhouses") else "new-db"}


def test_mixed_eventhouse_and_kql_adoption_preserves_ids_and_continues_data(tmp_path, monkeypatch):
    client = EventhouseClient()
    ctx = context(tmp_path, client, {
        "eh-old": "target-eh", "db-old": "target-db", "db-follower": "target-follower",
    })
    copies = []
    monkeypatch.setattr(orchestrator.shortcuts, "list_table_shortcuts", lambda *a: [{"name": "shortcut"}])

    def copy(**kwargs):
        copies.append(kwargs)
        return {"tables": 1}

    monkeypatch.setattr(orchestrator.kql, "copy_database", copy)
    orchestrator._migrate_eventhouses(ctx)
    assert [body["displayName"] for _, body in client.created] == ["Fresh", "Fresh"]
    assert ctx.id_map["eh-old"] == "target-eh"
    assert ctx.id_map["db-old"] == "target-db"
    assert ctx.id_map["db-follower"] == "target-follower"
    assert ctx.id_map["https://source-old"] == "https://target-eh"
    assert "target-db/updateDefinition" in client.updated[0][0]
    assert ctx.kql_databases == [("db-old", "target-db", "Telemetry"), ("db-new", "new-db", "Fresh")]
    assert ctx.kql_table_shortcuts["db-old"] == [{"name": "shortcut"}]
    assert len(copies) == 2
    assert copies[0]["target_cluster_uri"] == "https://target-eh"
    assert copies[0]["exclude"] == {"shortcut"}


def test_kql_adoption_does_not_repeat_finished_data(tmp_path, monkeypatch):
    client = EventhouseClient()
    ctx = context(tmp_path, client, {"db-old": "target-db"})
    ctx.prior.data_done.add(("db-old", "kql", ""))
    monkeypatch.setattr(orchestrator.shortcuts, "list_table_shortcuts", lambda *a: [])
    monkeypatch.setattr(
        orchestrator.kql, "copy_database", lambda **k: pytest.fail("must not copy finished data"),
    )
    moved, warnings, _ = orchestrator._migrate_kql_database(
        ctx, "eventhouses", database=client.databases["db-old"], target_eventhouse_id="target-eh",
        source_query_uri="https://source-old", target_query_uri="https://target-eh",
        existing_databases={},
    )
    assert moved and warnings == []
    assert client.created == []
    assert len(client.updated) == 1


def test_changed_follower_leader_is_not_silently_adopted(tmp_path, monkeypatch):
    ctx = context(tmp_path, EventhouseClient(), {"follower": "old-follower", "leader": "old-leader"})
    ctx.id_map["leader"] = "new-leader"
    ctx.refresh_needed.add("follower")
    ctx.prior.follower_bindings["follower"] = {
        "target": "old-follower", "leader": "old-leader", "parent": "target-eh",
    }
    monkeypatch.setattr(kql, "follower_source", lambda *a: kql.FollowerSource(database_name="leader"))
    # Use a Fabric-shaped leader so it is resolved through the item ID mapping.
    monkeypatch.setattr(kql.FollowerSource, "is_fabric_source", property(lambda self: True))
    with pytest.raises(orchestrator.ResumeRefused, match=r"Delete.*old-follower"):
        orchestrator._migrate_follower_database(
            ctx, database={"id": "follower", "displayName": "Follow",
                           "properties": {"databaseType": "Shortcut"}},
            target_eventhouse_id="target-eh", source_query_uri="https://source",
        )
    assert ctx.client.created == []


def test_repeated_resume_cannot_relabel_a_followers_old_binding_as_current(tmp_path, monkeypatch):
    ctx = context(tmp_path, EventhouseClient(), {"follower": "old-follower", "leader": "old-leader"})
    ctx.prior.follower_bindings["follower"] = {
        "target": "old-follower", "leader": "old-leader", "parent": "target-eh",
    }
    ctx.prior.refresh_needed.add("follower")
    ctx.refresh_needed.add("follower")
    ctx.journal = journal.Journal(tmp_path / "attempt.jsonl")
    ctx.journal.run_created({}, cleanup=False, prior=ctx.prior)
    ctx.id_map["leader"] = "new-leader"
    monkeypatch.setattr(kql, "follower_source", lambda *a: kql.FollowerSource(database_name="leader"))
    monkeypatch.setattr(kql.FollowerSource, "is_fabric_source", property(lambda self: True))
    database = {"id": "follower", "displayName": "Follow", "properties": {"databaseType": "Shortcut"}}
    for _ in range(2):
        with pytest.raises(orchestrator.ResumeRefused, match="old-leader"):
            orchestrator._migrate_follower_database(
                ctx, database=database, target_eventhouse_id="target-eh", source_query_uri="https://source",
            )
        ctx.prior = journal.read(ctx.journal.path)
        assert ctx.prior.id_map["leader"] == "new-leader"
        assert ctx.prior.follower_bindings["follower"]["leader"] == "old-leader"
    assert ctx.client.created == []


def test_snowflake_mixed_adoption_only_creates_missing_item(tmp_path):
    class Client:
        def __init__(self):
            self.created = []

        def post(self, path, json=None, **kwargs):
            if path.endswith("/getDefinition"):
                return {"definition": {"parts": []}}
            self.created.append(json)
            return {"id": "new-snowflake"}

    client = Client()
    ctx = context(tmp_path, client, {"existing": "target-existing"})
    items = [{"id": item_id, "displayName": item_id, "properties": {
        "snowflakeDatabaseName": "snowflake", "connectionId": "external-connection",
    }} for item_id in ("existing", "new")]
    assert orchestrator._migrate_snowflake_databases(ctx, "mirrored", items, lambda _: None) == []
    assert [item["displayName"] for item in client.created] == ["new"]
    assert ctx.id_map["existing"] == "target-existing"
    assert ctx.id_map["new"] == "new-snowflake"


def test_spark_mapping_change_durably_rechecks_retained_consumers(tmp_path):
    ctx = context(tmp_path, object(), {"pool": "old-pool", "notebook": "existing-notebook"})
    ctx.journal = journal.Journal(tmp_path / "attempt.jsonl")
    ctx.source_items = {"notebook": {"id": "notebook", "type": "Notebook"}}
    ctx.adopted_targets.update(ctx.id_map)
    ctx.invalidate_mapping("pool")
    assert "pool" not in ctx.id_map
    assert ctx.refresh_needed == {"notebook"}
    assert journal.read(ctx.journal.path).refresh_needed == {"notebook"}
