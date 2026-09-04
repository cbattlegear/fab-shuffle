"""Regressions for interrupted observations and known migration omissions."""

from contextlib import nullcontext
from dataclasses import replace

import pytest
from test_airflow_job_migration import JOB_ITEM, FakeClient
from test_store_lifecycle import DefinitionClient, report, source
from test_store_lifecycle import ctx as ctx

from fabshuffle import journal, orchestrator
from fabshuffle.fabric import analytics
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import part
from fabshuffle.lifecycle import EvidenceState, readiness_report
from fabshuffle.run import CancelledError


def ready_store(ctx, item):
    evidence = ctx.resolve_item(item, f"target-{item['id']}", item["type"])
    for step in ctx.run.lifecycle.get(item["id"]).required:
        evidence.step(step, EvidenceState.SUCCEEDED, "Recorded operation completed.")
    return evidence


@pytest.mark.parametrize("kind", ["Environment", "GraphModel", "MirroredDatabase", "SemanticModel"])
def test_interrupted_definition_success_cannot_omit_known_obligations(ctx, monkeypatch, kind):
    original = ctx.journal.outcome
    def interrupt(record):
        original(record)
        if record["steps"].get("definition", {}).get("state") == "succeeded":
            raise CancelledError("interrupted after definition success")
    monkeypatch.setattr(ctx.run.lifecycle, "_record", interrupt)
    with pytest.raises(CancelledError):
        analytics.migrate_definition_item(
            DefinitionClient(), source_workspace_id="source-ws", target_workspace_id="target-ws",
            item=source(kind), item_type=kind, id_map={}, parts=[part("definition.json", {})],
            lifecycle=ctx.lifecycle(source(kind)),
        )
    saved = journal.read(ctx.journal.path)
    result = readiness_report(
        saved.outcomes, run_id="run", lineage_id="run", run_status="interrupted",
        inventory_complete=True,
    )
    assert result["state"] != "ready"
    assert ("data" if kind == "SemanticModel" else "activation") in result["items"][0]["required"]


@pytest.mark.parametrize("status", [403, 404, 503])
def test_failed_target_verification_cannot_retain_ready_live_or_saved(ctx, monkeypatch, status):
    ready_store(ctx, source("Notebook"))
    prior = journal.Replay(
        run_id="prior", lineage_id="prior", plan=orchestrator._plan_record(ctx.plan),
        target_workspace_id="target-ws", id_map={"source-item": "target-source-item"},
        outcomes=ctx.run.lifecycle.snapshot(), inventory_complete=True,
    )
    error = FabricApiError(
        "GET", "workspaces/target-ws/items", status,
        '{"errorCode":"WorkspaceUnavailable","message":"Target inventory cannot be read"}',
    )
    class Client:
        def list_all(self, *args, **kwargs):
            raise error
    monkeypatch.setattr(orchestrator, "FabricClient", lambda tokens: nullcontext(Client()))
    monkeypatch.setattr(orchestrator, "TokenProvider", lambda principal: object())
    monkeypatch.setattr(orchestrator, "SETTINGS", replace(
        orchestrator.SETTINGS, scratch_root=ctx.scratch_dir,
    ))
    orchestrator.run_migration(ctx.run, ctx.principal, ctx.plan, cleanup=False, prior=prior)
    assert ctx.run.status.value == "failed"
    assert report(ctx)["state"] == "unknown"
    saved = journal.read(orchestrator.SETTINGS.journal_for(ctx.run.id))
    assert saved.outcomes["source-item"].steps["verification"].state == "unknown"
    assert saved.outcomes["source-item"].steps["verification"].errorCode == "WorkspaceUnavailable"


@pytest.mark.parametrize("interrupted", [False, True])
def test_endpoint_refresh_failure_replaces_successful_prior_observation(ctx, monkeypatch, interrupted):
    item = source()
    ready_store(ctx, item)
    monkeypatch.setattr(orchestrator, "list_items", lambda *args: [item])
    monkeypatch.setattr(orchestrator.data_stores, "list_lakehouses", lambda *args: [item])
    monkeypatch.setattr(orchestrator.shortcuts, "copy_shortcuts", lambda *args, **kwargs: (0, []))
    monkeypatch.setattr(orchestrator.data_stores, "get_lakehouse", lambda *args: {
        "id": "target-source-item", "properties": {"sqlEndpointProperties": {"id": "endpoint"}},
    })
    def fail(*args, **kwargs):
        if interrupted:
            raise CancelledError("stopped during endpoint refresh")
        raise FabricApiError("POST", "refresh", 400, '{"errorCode":"RefreshFailed","message":"Not synced"}')
    monkeypatch.setattr(orchestrator.data_stores, "refresh_sql_endpoint_metadata", fail)
    with pytest.raises((CancelledError, FabricApiError)):
        orchestrator._migrate_shortcuts_and_endpoints(ctx)
    expected = "unknown" if interrupted else "failed"
    assert ctx.run.lifecycle.get(item["id"]).steps["endpoint"].state == expected
    assert journal.read(ctx.journal.path).outcomes[item["id"]].steps["endpoint"].state == expected
    assert report(ctx)["state"] != "ready"


def test_airflow_dependency_evidence_includes_rebound_file_and_configuration_references(ctx):
    store = {**source(), "id": "lh-source"}
    evidence = ready_store(ctx, store)
    evidence.step("data", EvidenceState.SKIPPED, "Data copy disabled.")
    ctx.client = FakeClient(config={"environmentVariables": {"STORE": "lh-source"}})
    ctx.client.contents["dags/my_dag.py"] = b"lakehouse = 'lh-source'"
    orchestrator._migrate_airflow_jobs(ctx, "orchestration", [JOB_ITEM], lambda _message: None)
    outcome = ctx.run.lifecycle.get(JOB_ITEM["id"])
    assert outcome.dependencies == ["lh-source"]
    row = next(item for item in report(ctx)["items"] if item["sourceId"] == JOB_ITEM["id"])
    assert row["state"] == "needs_attention"
    assert any("Orders" in reason for reason in row["reasons"])


def test_airflow_successful_retry_clears_old_unresolved_references(ctx):
    store = {**source(), "id": "lh-source"}
    ctx.source_items[store["id"]] = store
    ctx.client = FakeClient(config={"environmentVariables": {"STORE": "lh-source"}})
    orchestrator._migrate_airflow_jobs(ctx, "orchestration", [JOB_ITEM], lambda _message: None)
    assert ctx.run.lifecycle.get(JOB_ITEM["id"]).unresolvedReferences
    ready_store(ctx, store)
    orchestrator._migrate_airflow_jobs(ctx, "orchestration", [JOB_ITEM], lambda _message: None)
    assert ctx.run.lifecycle.get(JOB_ITEM["id"]).unresolvedReferences == []
    row = next(item for item in report(ctx)["items"] if item["sourceId"] == JOB_ITEM["id"])
    assert row["state"] == "ready"


def test_airflow_secret_omission_is_an_explicit_operator_task(ctx):
    ctx.client = FakeClient(config={"secrets": [{"name": "api_key"}]})
    orchestrator._migrate_airflow_jobs(ctx, "orchestration", [JOB_ITEM], lambda _message: None)
    outcome = ctx.run.lifecycle.get(JOB_ITEM["id"])
    assert outcome.steps["configuration"].state == "skipped"
    assert report(ctx)["state"] == "needs_attention"
    assert any("secrets" in action for action in report(ctx)["items"][0]["actions"])


@pytest.mark.parametrize("reference", [{}, {"byConnection": {"connectionString": "datasource=something"}}])
def test_uncheckable_report_binding_remains_unknown(ctx, reference):
    analytics.migrate_definition_item(
        DefinitionClient(), source_workspace_id="source-ws", target_workspace_id="target-ws",
        item=source("Report"), item_type="Report", id_map={},
        parts=[part("definition.pbir", {"datasetReference": reference})],
        lifecycle=ctx.lifecycle(source("Report")),
    )
    assert report(ctx)["state"] == "unknown"
    assert ctx.run.lifecycle.get("source-item").steps["binding"].state == "unknown"


def test_explicit_external_report_binding_is_not_an_unresolved_source_dependency(ctx):
    analytics.migrate_definition_item(
        DefinitionClient(), source_workspace_id="source-ws", target_workspace_id="target-ws",
        item=source("Report"), item_type="Report", id_map={},
        parts=[part("definition.pbir", {"datasetReference": {
            "byConnection": {"connectionString": "semanticmodelid=external-model"},
        }})],
        lifecycle=ctx.lifecycle(source("Report")),
    )
    assert report(ctx)["state"] == "ready"
    assert ctx.run.lifecycle.get("source-item").unresolvedReferences == []


def test_endpoint_alias_dependency_uses_owning_lakehouse(ctx):
    store = {**source(), "properties": {"sqlEndpointProperties": {
        "id": "source-sql-endpoint", "connectionString": "source.datawarehouse.fabric.microsoft.com",
    }}}
    ready_store(ctx, store)
    notebook = {**source("Notebook"), "id": "notebook"}
    ctx.source_items["source-sql-endpoint"] = {
        "id": "source-sql-endpoint", "type": "SQLEndpoint", "displayName": "Orders endpoint",
    }
    ctx.id_map["source.datawarehouse.fabric.microsoft.com"] = "target.datawarehouse.fabric.microsoft.com"
    ctx.id_map["source-sql-endpoint"] = "target-sql-endpoint"
    analytics.migrate_definition_item(
        DefinitionClient(), source_workspace_id="source-ws", target_workspace_id="target-ws",
        item=notebook, item_type="Notebook", id_map=ctx.id_map,
        parts=[part("notebook.py", "source.datawarehouse.fabric.microsoft.com")],
        source_items=ctx.source_items, lifecycle=ctx.lifecycle(notebook),
    )
    assert ctx.run.lifecycle.get("notebook").dependencies == ["source-item"]
    assert report(ctx)["state"] == "ready"
