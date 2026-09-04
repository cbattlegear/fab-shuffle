"""Adapter observations, rather than phase success or mapping counts, drive readiness."""

import pytest

from fabshuffle import journal, orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric import analytics, special_items
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import part
from fabshuffle.lifecycle import CopyOutcome, EvidenceState, readiness_report
from fabshuffle.run import MigrationRun
from fabshuffle.transfer import files


@pytest.fixture
def ctx(tmp_path):
    return orchestrator._Context(
        client=object(), tokens=object(),
        principal=ServicePrincipal("tenant", "app", "never-echo-this-credential"),
        plan=orchestrator.MigrationPlan(
            capacity_id="cap", capacity_name="F64", capacity_region="westus",
            source_workspace_id="source-ws", source_workspace_name="Source",
            target_workspace_name="Target",
        ),
        run=MigrationRun(source_workspace_name="Source", capacity_name="F64"),
        scratch_dir=tmp_path, target_workspace_id="target-ws",
        journal=journal.Journal(tmp_path / "run.jsonl"),
    )


def report(ctx):
    return readiness_report(
        ctx.run.lifecycle.snapshot(), run_id=ctx.run.id, lineage_id=ctx.run.id,
        run_status=ctx.run.status.value, inventory_complete=True,
    )


def source(kind="Lakehouse"):
    return {"id": "source-item", "displayName": "Orders", "type": kind}


def test_stores_reuse_creation_contract_without_claiming_data_readiness(ctx):
    for kind in ("Lakehouse", "Warehouse", "SQLDatabase"):
        item = {**source(kind), "id": kind}
        assert ctx.resolve_or_create(
            item, kind, lambda kind=kind: {"id": f"target-{kind}"},
        ) == f"target-{kind}"
    rows = report(ctx)["items"]
    assert all(row["disposition"] == "created" and row["state"] == "unknown" for row in rows)
    assert all(any(step["step"] == "data" and step["state"] == "unknown"
                   for step in row["steps"]) for row in rows)
    assert len(journal.read(ctx.journal.path).outcomes) == 3


def test_adoption_does_not_create_a_second_target_or_invent_copy_evidence(ctx):
    ctx.prior = journal.Replay(id_map={"source-item": "target"})
    ctx.id_map.update(ctx.prior.id_map)
    def no_create():
        pytest.fail("An adopted target must not be recreated")
    ctx.resolve_or_create(source(), "Lakehouse", no_create)
    row = report(ctx)["items"][0]
    assert row["disposition"] == "adopted" and row["state"] == "unknown"


def test_skipped_options_are_evidence_not_run_failures(ctx):
    ctx.plan.include_data = False
    ctx.plan.include_files = False
    ctx.resolve_or_create(source(), "Lakehouse", lambda: {"id": "target"})
    row = report(ctx)["items"][0]
    assert row["state"] == "needs_attention"
    steps = {step["step"]: step for step in row["steps"]}
    assert steps["data"]["state"] == steps["files"]["state"] == "skipped"
    assert ctx.run.status.value == "pending"


def test_empty_table_enumeration_is_distinct_from_enumeration_failure(ctx, monkeypatch):
    ctx.resolve_or_create(source(), "Lakehouse", lambda: {"id": "target"})
    pairs = [(source(), {"id": "target"}, False)]
    monkeypatch.setattr(orchestrator, "_lakehouse_tables", lambda *a, **k: [])
    orchestrator._copy_lakehouse_tables(ctx, "lakehouses", pairs)
    evidence = ctx.run.lifecycle.snapshot()["source-item"].steps["data"]
    assert evidence.state == EvidenceState.SUCCEEDED and "No copyable" in evidence.reason

    error = FabricApiError("GET", "tables", 403, '{"errorCode":"Forbidden","message":"Read denied"}')
    def failed(*args, **kwargs):
        raise error
    monkeypatch.setattr(orchestrator, "_lakehouse_tables", failed)
    orchestrator._copy_lakehouse_tables(ctx, "lakehouses", pairs)
    evidence = ctx.run.lifecycle.snapshot()["source-item"].steps["data"]
    assert evidence.state == EvidenceState.FAILED
    assert evidence.errorCode == "Forbidden" and evidence.message == "Read denied"


def test_data_completion_is_written_after_the_strict_checkpoint(ctx, monkeypatch):
    ctx.resolve_or_create(source(), "Lakehouse", lambda: {"id": "target"})
    def failed(*args, **kwargs):
        raise OSError("journal unavailable")
    monkeypatch.setattr(ctx.journal, "data", failed)
    with pytest.raises(OSError, match="journal unavailable"):
        ctx.data_copied("source-item", "tables")
    assert "data" not in ctx.run.lifecycle.snapshot()["source-item"].steps


def test_file_copy_empty_and_missing_paths_are_different(ctx, monkeypatch):
    ctx.resolve_or_create(source(), "Lakehouse", lambda: {"id": "target"})
    orchestrator._lakehouse_file_job(ctx, source(), {"id": "target"})()
    assert ctx.run.lifecycle.snapshot()["source-item"].steps["files"].state == EvidenceState.UNKNOWN
    monkeypatch.setattr(files, "copy_files", lambda **kwargs: CopyOutcome("files", empty=True))
    src = {**source(), "properties": {"oneLakeFilesPath": "source-path"}}
    target = {"id": "target", "properties": {"oneLakeFilesPath": "target-path"}}
    orchestrator._lakehouse_file_job(ctx, src, target)()
    evidence = ctx.run.lifecycle.snapshot()["source-item"].steps["files"]
    assert evidence.state == EvidenceState.SUCCEEDED and "No copyable" in evidence.reason
    replay = journal.read(ctx.journal.path)
    assert replay.data_targets[("source-item", "files", "")] == "target"


def test_file_transfer_reports_an_observed_empty_download(tmp_path, monkeypatch):
    monkeypatch.setattr(files, "_azcopy", lambda *args: None)
    result = files.copy_files(
        source_files_path="source", target_files_path="target",
        principal=ServicePrincipal("tenant", "client", "secret"), scratch_dir=tmp_path,
    )
    assert result == CopyOutcome("files", empty=True)


class DefinitionClient:
    def post(self, path, json=None, **kwargs):
        return {"id": "target"}


@pytest.mark.parametrize("kind,action", [
    ("Environment", "Publish"), ("GraphModel", "Refresh"), ("MirroredDatabase", "Start mirroring"),
])
def test_definition_activation_tasks_are_explicit_and_survive_replay(ctx, kind, action):
    item = source(kind)
    analytics.migrate_definition_item(
        DefinitionClient(), source_workspace_id="source-ws", target_workspace_id="target-ws",
        item=item, item_type=kind, id_map={}, parts=[part("definition.json", {})],
        lifecycle=ctx.lifecycle(item),
    )
    row = report(ctx)["items"][0]
    assert row["disposition"] == "created" and row["state"] == "needs_attention"
    assert any(action in task for task in row["actions"])
    assert journal.read(ctx.journal.path).outcomes[item["id"]].steps["activation"].state == "skipped"


def test_definition_rebinding_names_unmigrated_dependency(ctx):
    item = source("Notebook")
    with pytest.raises(analytics.StrandedReference):
        analytics.migrate_definition_item(
            DefinitionClient(), source_workspace_id="source-ws", target_workspace_id="target-ws",
            item=item, item_type="Notebook", id_map={}, parts=[part("notebook.py", "missing-store")],
            source_items={"missing-store": {"id": "missing-store", "displayName": "Revenue",
                                             "type": "Warehouse"}},
            lifecycle=ctx.lifecycle(item),
        )
    row = report(ctx)["items"][0]
    assert row["state"] == "needs_attention" and not row["targetId"]
    assert row["unresolvedReferences"] == ["Warehouse 'Revenue'"]
    assert row["dependencies"] == ["missing-store"]


@pytest.mark.parametrize("kind,parts", [
    ("Reflex", [part(special_items.REFLEX_ENTITIES_PART, [{
        "payload": {"definition": {"type": "Rule", "settings": {"shouldRun": True}}},
    }])]),
    ("MirroredAzureDatabricksCatalog", [part("definition.json", {"autoSync": "Enabled"})]),
])
def test_arrive_stopped_policies_emit_actions_without_warning_parsing(ctx, kind, parts):
    analytics.migrate_definition_item(
        DefinitionClient(), source_workspace_id="source-ws", target_workspace_id="target-ws",
        item=source(kind), item_type=kind, id_map={}, parts=parts,
        lifecycle=ctx.lifecycle(source(kind)),
    )
    row = report(ctx)["items"][0]
    assert row["state"] == "needs_attention"
    assert any("cutover" in action or "cut over" in action for action in row["actions"])


def test_completed_definition_does_not_assert_semantic_model_data_readiness(ctx):
    analytics.migrate_definition_item(
        DefinitionClient(), source_workspace_id="source-ws", target_workspace_id="target-ws",
        item=source("SemanticModel"), item_type="SemanticModel",
        id_map={}, parts=[part("model.bim", {})],
        lifecycle=ctx.lifecycle(source("SemanticModel")),
    )
    assert report(ctx)["items"][0]["state"] == "unknown"


def test_completed_sql_copy_requires_callbacks_for_every_table(ctx, monkeypatch):
    item = source("SQLDatabase")
    ctx.resolve_or_create(item, "SQLDatabase", lambda: {"id": "target"})
    monkeypatch.setattr(orchestrator.sqldatabases, "server_fqdn", lambda item: "server")
    monkeypatch.setattr(orchestrator.sqldatabases, "database_name", lambda item: "catalog")
    monkeypatch.setattr(orchestrator.sqlschema, "list_base_tables",
                        lambda *args: [("dbo", "A"), ("dbo", "B")])
    def partial_copy(**kwargs):
        kwargs["on_copied"]("dbo.A")
        return []
    monkeypatch.setattr(orchestrator.bulkcopy, "copy_tables", partial_copy)
    orchestrator._copy_sql_database_tables(ctx, "sql", [(item, {"id": "target"})])
    evidence = ctx.run.lifecycle.snapshot()["source-item"].steps["data"]
    assert evidence.state == "failed" and "dbo.B" in evidence.reason and "dbo.A" not in evidence.reason
    assert journal.read(ctx.journal.path).data_targets[("source-item", "table", "dbo.A")] == "target"


def test_copy_checkpoint_after_recreation_cannot_reuse_old_data(ctx):
    ctx.prior = journal.Replay(
        id_map={"source-item": "old"},
        data_done={("source-item", "tables", "")},
        data_targets={("source-item", "tables", ""): "old"},
    )
    ctx.resolve_item(source(), "new", "Lakehouse")
    assert not ctx.already_copied("source-item", "tables")
    assert ctx.run.lifecycle.snapshot()["source-item"].targetId == "new"
