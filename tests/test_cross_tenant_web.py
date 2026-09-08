"""The web boundary executes paired plans without exposing another pair's run state."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from fabshuffle import journal
from fabshuffle.config import SETTINGS
from fabshuffle.orchestrator import MigrationPlan
from fabshuffle.run import RunRegistry, RunStatus
from fabshuffle.web import app as web
from tests.test_tenant_assessment import SOURCE, TARGET, StubTokens


@pytest.fixture
def pair_api(monkeypatch, tmp_path):
    monkeypatch.setattr(SETTINGS, "scratch_root", tmp_path)
    monkeypatch.setattr(web, "REGISTRY", RunRegistry())
    monkeypatch.setattr(web, "SESSIONS", web.SessionStore())
    session = web.SESSIONS.create(SOURCE, StubTokens(SOURCE), target_tokens=StubTokens(TARGET))
    prepared = []
    workers = []

    class Client:
        def __init__(self, tokens):
            self.tokens = tokens

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

    class Thread:
        def __init__(self, *, target, **_):
            self.work = target

        def start(self):
            workers.append(self.work)

    def build(source, **options):
        prepared.append((source, options))
        return MigrationPlan(
            capacity_id=options["capacity_id"], capacity_name="Destination", capacity_region="eastus",
            source_workspace_id=options["source_workspace_id"], source_workspace_name="Sales",
            target_workspace_name=options.get("target_workspace_name") or "Sales-copy",
            **{key: options[key] for key in (
                "source_tenant_id", "target_tenant_id", "source_client_id", "target_client_id",
                "include_data", "include_files", "copy_permissions", "write_freeze_confirmed",
                "connection_mappings", "reference_mappings",
            )},
        )

    monkeypatch.setattr(web, "FabricClient", Client)
    monkeypatch.setattr(web, "build_plan", build)
    monkeypatch.setattr(web, "threading", SimpleNamespace(Thread=Thread))
    with TestClient(web.create_app()) as client:
        yield client, session, prepared, workers


def headers(session):
    return {web.SESSION_HEADER: session.id}


def body(**options):
    return {
        "capacity_id": "capacity", "source_workspace_id": "workspace",
        "include_data": False, "include_files": False, **options,
    }


def test_start_passes_the_bound_pair_and_operator_options_to_execution(pair_api, monkeypatch):
    client, session, prepared, workers = pair_api
    mappings = {"11111111-1111-1111-1111-111111111111": "22222222-2222-2222-2222-222222222222"}
    result = client.post("/api/runs", headers=headers(session), json=body(connection_mappings=mappings))
    assert result.status_code == 200, result.text
    source, options = prepared[0]
    assert source.tokens is session.tokens
    assert options["target_client"].tokens is session.destination_tokens
    assert options["source_client_id"] == SOURCE.client_id
    assert options["target_client_id"] == TARGET.client_id
    assert result.json()["plan"]["copyPermissions"] is False
    seen = []

    def migration(run, principal, plan, **kwargs):
        seen.append((principal, plan, kwargs))
        run.mark_finished(RunStatus.SUCCEEDED)

    monkeypatch.setattr(web, "run_migration", migration)
    web.SESSIONS.drop(session.id)
    workers[0]()
    assert seen[0][0] is SOURCE
    assert seen[0][2]["target_principal"] is TARGET
    assert seen[0][1].connection_mappings == mappings
    path = web._session_journal(session, result.json()["runId"])
    assert path.is_file()
    assert not SETTINGS.journal_for(result.json()["runId"]).exists()
    assert SOURCE.client_secret not in path.read_text()
    assert TARGET.client_secret not in path.read_text()


@pytest.mark.parametrize("options", [
    {"include_data": True}, {"include_files": True},
    {"include_files": True, "include_data": True},
])
@pytest.mark.parametrize("same_tenant", [False, True])
def test_missing_write_freeze_rejected_before_admission(pair_api, options, same_tenant):
    client, session, _, workers = pair_api
    if same_tenant:
        destination = replace(TARGET, tenant_id=SOURCE.tenant_id)
        session = web.SESSIONS.create(SOURCE, StubTokens(SOURCE), target_tokens=StubTokens(destination))
    response = client.post("/api/runs", headers=headers(session), json=body(**options))
    assert response.status_code == 409
    assert "Pause source writes" in response.json()["detail"]
    assert not workers
    assert not web._session_directory(session).exists()


def test_write_freeze_allows_data_copy_admission(pair_api):
    client, session, _, workers = pair_api
    response = client.post(
        "/api/runs", headers=headers(session),
        json=body(include_data=True, write_freeze_confirmed=True),
    )
    assert response.status_code == 200
    assert len(workers) == 1


def test_two_apps_in_the_same_tenant_still_create_a_bound_paired_run(pair_api):
    client, _, _, workers = pair_api
    destination = replace(TARGET, tenant_id=SOURCE.tenant_id)
    session = web.SESSIONS.create(
        SOURCE, StubTokens(SOURCE), target_tokens=StubTokens(destination),
    )
    response = client.post(
        "/api/runs", headers=headers(session), json=body(include_data=True, write_freeze_confirmed=True),
    )
    assert response.status_code == 200, response.text
    assert len(workers) == 1
    assert response.json()["plan"]["sourceTenantId"] == response.json()["plan"]["targetTenantId"]
    assert response.json()["plan"]["targetClientId"] == TARGET.client_id
    assert response.json()["plan"]["copyPermissions"] is False
    assert not SETTINGS.journal_for(response.json()["runId"]).exists()


def test_worker_failure_is_recorded_in_the_paired_directory(pair_api, monkeypatch):
    client, session, _, workers = pair_api
    run_id = client.post("/api/runs", headers=headers(session), json=body()).json()["runId"]

    def fail(*_, **__):
        raise RuntimeError("Destination errorCode: CapacityNotActive")

    monkeypatch.setattr(web, "run_migration", fail)
    workers[0]()
    run = web.REGISTRY.get(run_id)
    assert run.status is RunStatus.FAILED
    assert "CapacityNotActive" in run.error
    assert journal.read(web._session_journal(session, run_id)).status == "failed"


@pytest.mark.parametrize("route,method", [
    ("", "GET"), ("/readiness", "GET"), ("/cancel", "POST"),
    ("/events", "GET"), ("/resume", "POST"), ("/cleanup", "POST"),
    ("/resume-plan", "GET"), ("/resume-preview", "POST"),
])
@pytest.mark.parametrize("mode", ["different-app", "different-tenant", "single"])
def test_run_endpoints_do_not_accept_an_unrelated_sign_in(pair_api, route, method, mode):
    client, session, _, _ = pair_api
    run_id = client.post("/api/runs", headers=headers(session), json=body()).json()["runId"]
    if mode == "single":
        other = web.SESSIONS.create(SOURCE, StubTokens(SOURCE))
    else:
        principal = replace(TARGET, client_id="another-app") if mode == "different-app" else SOURCE
        other = web.SESSIONS.create(SOURCE, StubTokens(SOURCE), target_tokens=StubTokens(principal))
    response = client.request(
        method, f"/api/runs/{run_id}{route}", headers=headers(other),
        params={"session_id": other.id} if route == "/events" else None,
        json={} if route == "/resume-preview" else None,
    )
    assert response.status_code in {404, 409}, response.text
    assert not web.REGISTRY.get(run_id).cancelled


def test_run_read_and_events_accept_the_bound_pair(pair_api):
    client, session, _, _ = pair_api
    run_id = client.post("/api/runs", headers=headers(session), json=body()).json()["runId"]
    run = web.REGISTRY.get(run_id)
    run.mark_finished(RunStatus.SUCCEEDED)
    for path in ("", "/readiness", "/events"):
        response = client.get(
            f"/api/runs/{run_id}{path}", headers=headers(session),
            params={"session_id": session.id},
        )
        assert response.status_code == 200, response.text


def test_restart_lists_and_resumes_only_the_same_pair(pair_api):
    client, session, _, workers = pair_api
    run_id = client.post("/api/runs", headers=headers(session), json=body()).json()["runId"]
    web.REGISTRY = RunRegistry()
    other = web.SESSIONS.create(
        SOURCE, StubTokens(SOURCE), target_tokens=StubTokens(replace(TARGET, client_id="other-app")),
    )
    assert client.get("/api/resumable", headers=headers(other)).json()["runs"] == []
    listed = client.get("/api/resumable", headers=headers(session)).json()["runs"]
    assert [item["runId"] for item in listed] == [run_id]
    assert listed[0]["targetClientId"] == TARGET.client_id
    response = client.post(f"/api/runs/{run_id}/resume", headers=headers(session))
    assert response.status_code == 200, response.text
    assert response.json()["resumedFrom"] == run_id
    assert len(workers) == 2


def test_saved_readiness_does_not_cross_app_boundary(pair_api):
    client, session, _, _ = pair_api
    run_id = client.post("/api/runs", headers=headers(session), json=body()).json()["runId"]
    web.REGISTRY = RunRegistry()
    other = web.SESSIONS.create(
        SOURCE, StubTokens(SOURCE), target_tokens=StubTokens(replace(TARGET, client_id="other-app")),
    )
    assert client.get(f"/api/runs/{run_id}/readiness", headers=headers(other)).status_code == 409
    assert client.get(f"/api/runs/{run_id}/readiness", headers=headers(session)).status_code == 200


def test_connections_use_the_selected_principal_and_only_export_safe_metadata(pair_api, monkeypatch):
    client, session, _, _ = pair_api
    calls = []

    def connections(client, path):
        calls.append((client.tokens, path))
        return [{"id": "connection", "displayName": "Shared", "connectivityType": "ShareableCloud",
                 "credentialDetails": {"secret": "secret-marker"}, "connectionDetails": {"path": "private"}}]

    original = web.FabricClient
    monkeypatch.setattr(original, "list_all", connections, raising=False)
    for side, tokens in (("source", session.tokens), ("target", session.destination_tokens)):
        result = client.get("/api/connections", headers=headers(session), params={"side": side})
        assert result.status_code == 200
        assert calls[-1] == (tokens, "connections")
        assert set(result.json()["connections"][0]) == {"id", "displayName", "connectivityType"}
        assert "private" not in result.text and "secret-marker" not in result.text
    assert client.get("/api/connections?side=wrong", headers=headers(session)).status_code == 422


def test_paired_cleanup_uses_destination_client_and_owned_run(pair_api, monkeypatch):
    client, session, _, _ = pair_api
    run_id = client.post("/api/runs", headers=headers(session), json=body()).json()["runId"]
    run = web.REGISTRY.get(run_id)
    run.mark_finished(RunStatus.SUCCEEDED)
    web.REGISTRY.release(run_id)
    book = journal.Journal(web._session_journal(session, run_id))
    book.finished("succeeded")
    seen = []
    monkeypatch.setattr(web, "cleanup_run", lambda run, target: seen.append(target.tokens) or [])
    response = client.post(f"/api/runs/{run_id}/cleanup", headers=headers(session))
    assert response.status_code == 200, response.text
    assert seen == [session.destination_tokens]


@pytest.mark.parametrize("options", [
    {"connection_mappings": {"../unexpected": "target"}},
    {"connection_mappings": {"11111111-1111-1111-1111-111111111111": "not-a-guid"}},
    {"reference_mappings": [{"source_item_id": "missing-workspaces"}]},
    {"reference_mappings": [{
        "source_workspace_id": "11111111-1111-1111-1111-111111111111",
        "source_item_id": "22222222-2222-2222-2222-222222222222",
        "target_workspace_id": "33333333-3333-3333-3333-333333333333",
        "target_item_id": "44444444-4444-4444-4444-444444444444",
        "credential": "must-not-be-journaled",
    }]},
])
def test_malformed_mapping_inputs_are_rejected_before_planning(pair_api, options):
    client, session, prepared, workers = pair_api
    response = client.post("/api/runs", headers=headers(session), json=body(**options))
    assert response.status_code == 422, response.text
    assert not prepared and not workers


@pytest.mark.parametrize("mapped", [False, True])
def test_recheck_requires_destination_connection_proof_not_source_visibility(pair_api, monkeypatch, mapped):
    client, session, prepared, _ = pair_api
    source_id = "11111111-1111-1111-1111-111111111111"
    target_id = "22222222-2222-2222-2222-222222222222"
    monkeypatch.setattr(web, "list_items", lambda *_: [
        {"id": "pipeline", "displayName": "Ingest", "type": "DataPipeline"},
    ])
    monkeypatch.setattr(web, "get_item_definition", lambda *_: {"parts": [
        web.definitions.part("pipeline-content.json", {"externalReferences": {"connection": source_id}}),
    ]})
    monkeypatch.setattr(web.relations, "build_graph", lambda *_: web.relations.DependencyGraph())
    checked = []

    def connection(client, path):
        checked.append((client.tokens, path))
        expected_id = source_id if client.tokens is session.tokens else target_id
        assert path == f"connections/{expected_id}"
        return {
            "id": expected_id, "connectivityType": "ShareableCloud",
            "connectionDetails": {"type": "Web", "path": "https://example.org/external"},
        }

    monkeypatch.setattr(web.FabricClient, "get", connection, raising=False)
    options = {source_id: target_id} if mapped else {}
    response = client.post(
        "/api/preview/dependencies", headers=headers(session),
        json=body(connection_mappings=options),
    )
    assert response.status_code == 200, response.text
    report = response.json()
    assert report["connectionAccess"] is None
    assert report["connectionAccessScope"] == "destination"
    assert "not data-copy or cutover readiness" in report["assessmentNotice"]
    assert prepared[0][1]["connection_mappings"] == options
    if mapped:
        assert checked == [(session.tokens, f"connections/{source_id}"),
                           (session.destination_tokens, f"connections/{target_id}")]
        assert not report["dependencies"]
    else:
        assert not checked
        assert source_id in report["dependencies"][0]
        assert "will not be created" in report["dependencies"][0]


def test_recheck_and_start_pass_the_same_external_operator_mapping(pair_api, monkeypatch):
    client, session, prepared, _ = pair_api
    external = [{
        "source_workspace_id": "11111111-1111-1111-1111-111111111111",
        "source_item_id": "22222222-2222-2222-2222-222222222222",
        "target_workspace_id": "33333333-3333-3333-3333-333333333333",
        "target_item_id": "44444444-4444-4444-4444-444444444444",
    }]
    seen = []
    monkeypatch.setattr(
        web, "_paired_dependency_report",
        lambda session, source, target, plan: seen.append(plan.reference_mappings)
        or {"dependencies": [], "connectionAccess": None, "blockers": []},
    )
    for route in ("/api/preview/dependencies", "/api/runs"):
        response = client.post(route, headers=headers(session), json=body(reference_mappings=external))
        assert response.status_code == 200, response.text
    assert seen == [external]
    assert prepared[0][1]["reference_mappings"] == prepared[1][1]["reference_mappings"] == external


def test_resume_updates_mappings_and_keeps_target_and_checkpoints(pair_api, monkeypatch):
    client, session, _, workers = pair_api
    old_mapping = {"11111111-1111-1111-1111-111111111111": "22222222-2222-2222-2222-222222222222"}
    new_mapping = {"11111111-1111-1111-1111-111111111111": "33333333-3333-3333-3333-333333333333"}
    external = [{
        "source_workspace_id": "44444444-4444-4444-4444-444444444444",
        "source_item_id": "55555555-5555-5555-5555-555555555555",
        "target_workspace_id": "66666666-6666-6666-6666-666666666666",
        "target_item_id": "77777777-7777-7777-7777-777777777777",
    }]
    run_id = client.post(
        "/api/runs", headers=headers(session),
        json=body(connection_mappings=old_mapping, include_data=True, write_freeze_confirmed=True),
    ).json()["runId"]
    book = journal.Journal(web._session_journal(session, run_id))
    book.workspace("target", "created-target", "Sales-copy")
    book.item("lakehouse-source", "lakehouse-target", "Lakehouse", "Bronze")
    book.data("lakehouse-source", "files", target_id="lakehouse-target")
    web.REGISTRY = RunRegistry()
    captured = []
    monkeypatch.setattr(
        web, "run_migration", lambda run, principal, plan, **kwargs: captured.append((plan, kwargs)),
    )
    saved = client.get(f"/api/runs/{run_id}/resume-plan", headers=headers(session))
    assert saved.status_code == 200, saved.text
    assert saved.json()["plan"]["connectionMappings"] == old_mapping
    assert saved.json()["targetWorkspaceId"] == "created-target"
    assert saved.json()["items"] == [{
        "sourceId": "lakehouse-source", "targetId": "lakehouse-target", "type": "Lakehouse", "name": "Bronze",
    }]
    resumed = client.post(
        f"/api/runs/{run_id}/resume", headers=headers(session),
        json={"connection_mappings": new_mapping, "reference_mappings": external},
    )
    assert resumed.status_code == 200, resumed.text
    workers[-1]()
    plan, kwargs = captured[0]
    assert plan.connection_mappings == new_mapping
    assert plan.reference_mappings == external
    assert plan.include_data and plan.write_freeze_confirmed
    assert plan.source_workspace_id == "workspace" and plan.capacity_id == "capacity"
    assert plan.source_client_id == SOURCE.client_id and plan.target_client_id == TARGET.client_id
    assert kwargs["prior"].target_workspace_id == "created-target"
    assert kwargs["prior"].id_map["lakehouse-source"] == "lakehouse-target"
    assert kwargs["prior"].data_is_done("lakehouse-source", "files")
    assert web.REGISTRY.get(resumed.json()["runId"]).lineage_id == run_id
    assert journal.read(web._session_journal(session, run_id)).plan["connection_mappings"] == old_mapping


@pytest.mark.parametrize("field", [
    "source_tenant_id", "target_tenant_id", "source_client_id", "target_client_id",
    "source_workspace_id", "target_workspace_id", "target_workspace_name", "capacity_id",
    "copy_epoch", "include_data", "write_freeze_confirmed",
])
def test_resume_body_cannot_change_the_bound_plan_or_copy_epoch(pair_api, field):
    client, session, _, workers = pair_api
    run_id = client.post("/api/runs", headers=headers(session), json=body()).json()["runId"]
    response = client.post(f"/api/runs/{run_id}/resume", headers=headers(session), json={field: "changed"})
    assert response.status_code == 422, response.text
    assert len(workers) == 1


def test_resume_preview_keeps_omitted_mappings_and_uses_prior_target(pair_api, monkeypatch):
    client, session, _, _ = pair_api
    mapping = {"11111111-1111-1111-1111-111111111111": "22222222-2222-2222-2222-222222222222"}
    run_id = client.post(
        "/api/runs", headers=headers(session), json=body(connection_mappings=mapping),
    ).json()["runId"]
    book = journal.Journal(web._session_journal(session, run_id))
    book.workspace("target", "existing-target", "Sales-copy")
    captured = []

    def preview(session, source, target, plan, *, prior):
        captured.append((plan, prior))
        return {"dependencies": [], "connectionAccess": None, "blockers": []}

    monkeypatch.setattr(web, "_paired_dependency_report", preview)
    response = client.post(f"/api/runs/{run_id}/resume-preview", headers=headers(session), json={})
    assert response.status_code == 200, response.text
    assert captured[0][0].connection_mappings == mapping
    assert captured[0][1].target_workspace_id == "existing-target"


@pytest.mark.parametrize("route", ["/api/preview", "/api/preview/dependencies", "/api/runs"])
def test_mapping_type_conflicts_are_actionable_http_conflicts_not_server_errors(pair_api, monkeypatch, route):
    client, session, _, workers = pair_api
    detail = "External item 'Orders' is Warehouse, but destination item replacement is Lakehouse."

    def conflicting_mapping(*_, **__):
        raise ValueError(detail)

    monkeypatch.setattr(web, "build_plan", conflicting_mapping)
    if route == "/api/preview":
        response = client.get(
            route, headers=headers(session),
            params={"capacity_id": "capacity", "source_workspace_id": "workspace"},
        )
    else:
        response = client.post(route, headers=headers(session), json=body())
    assert response.status_code == 409, response.text
    assert response.json()["detail"] == detail
    assert not workers


def test_resume_mapping_resolver_errors_are_http_conflicts(pair_api, monkeypatch):
    client, session, _, workers = pair_api
    run_id = client.post("/api/runs", headers=headers(session), json=body()).json()["runId"]
    detail = "External reference has conflicting destination mappings."

    def conflicting_mapping(*_, **__):
        raise ValueError(detail)

    monkeypatch.setattr(web, "list_items", lambda *_: [])
    monkeypatch.setattr(web.migration_refs, "resolve", conflicting_mapping)
    response = client.post(
        f"/api/runs/{run_id}/resume-preview", headers=headers(session), json={},
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"] == detail
    assert len(workers) == 1


def test_same_tenant_second_app_can_reuse_visible_external_references(pair_api, monkeypatch):
    client, _, _, _ = pair_api
    destination = replace(TARGET, tenant_id=SOURCE.tenant_id)
    session = web.SESSIONS.create(SOURCE, StubTokens(SOURCE), target_tokens=StubTokens(destination))
    connection_id = "11111111-1111-1111-1111-111111111111"
    monkeypatch.setattr(web, "list_items", lambda *_: [
        {"id": "pipeline", "displayName": "Ingest", "type": "DataPipeline"},
    ])
    monkeypatch.setattr(web, "get_item_definition", lambda *_: {"parts": [
        web.definitions.part("pipeline-content.json", {"externalReferences": {"connection": connection_id}}),
    ]})
    graph = web.relations.DependencyGraph(
        dependencies={"pipeline": {"external"}},
        items={"external": {"id": "external", "workspaceId": "another-workspace"}},
    )
    monkeypatch.setattr(web.relations, "build_graph", lambda *_: graph)
    checked = []

    def connection(client, path):
        checked.append(client.tokens)
        assert path == f"connections/{connection_id}"
        return {
            "id": connection_id, "connectivityType": "ShareableCloud",
            "connectionDetails": {"type": "Web", "path": "https://example.org/external"},
        }

    monkeypatch.setattr(web.FabricClient, "get", connection, raising=False)
    response = client.post("/api/preview/dependencies", headers=headers(session), json=body())
    assert response.status_code == 200, response.text
    assert not response.json()["dependencies"]
    assert checked == [session.tokens, session.destination_tokens]


def test_mapping_resolver_fabric_errors_preserve_details(pair_api, monkeypatch):
    client, session, _, workers = pair_api
    detail = "External target details could not be read: the Fabric operation timed out."

    def failed_mapping(*_, **__):
        raise web.FabricError(detail)

    monkeypatch.setattr(web, "build_plan", failed_mapping)
    response = client.post("/api/preview/dependencies", headers=headers(session), json=body())
    assert response.status_code == 502, response.text
    assert response.json()["detail"] == detail
    assert not workers


@pytest.mark.parametrize("definition_kind", ["members", "rls-only", "opaque", "credentials"])
@pytest.mark.parametrize("same_tenant", [False, True])
def test_semantic_model_recheck_requests_tmsl_and_preserves_security_definitions(
    pair_api, monkeypatch, definition_kind, same_tenant,
):
    client, session, _, _ = pair_api
    if same_tenant:
        destination = replace(TARGET, tenant_id=SOURCE.tenant_id)
        session = web.SESSIONS.create(SOURCE, StubTokens(SOURCE), target_tokens=StubTokens(destination))
    assert session.cross_tenant == (not same_tenant)
    model = {"model": {"roles": [{
        "name": "Regional readers",
        "members": [{"memberName": "private-source-principal"}] if definition_kind == "members" else [],
        "tablePermissions": [{"name": "Sales", "filterExpression": '[Region] = "West"'}],
    }]}}
    if definition_kind == "credentials":
        model["model"]["dataSources"] = [
            {"name": "Source", "credential": {"private_secret": "private-source-credential"}},
        ]
    definition = {"parts": [
        web.definitions.part("definition/model.tmdl", "opaque role membership")
        if definition_kind == "opaque" else web.definitions.part("model.bim", model),
    ]}
    original = repr(definition)
    formats = []
    validated = []
    validate_identities = web.analytics.validate_cross_tenant_identities
    monkeypatch.setattr(web, "list_items", lambda *_: [
        {"id": "model", "displayName": "Sales model", "type": "SemanticModel"},
    ])

    def export(source, workspace_id, item_id, fmt=None):
        assert source.tokens is session.tokens
        assert workspace_id == "workspace" and item_id == "model"
        formats.append(fmt)
        return definition

    def validate(parts, *, item_type):
        validated.extend(parts)
        return validate_identities(parts, item_type=item_type)

    monkeypatch.setattr(web, "get_item_definition", export)
    monkeypatch.setattr(web.analytics, "validate_cross_tenant_identities", validate)
    monkeypatch.setattr(web.relations, "build_graph", lambda *_: web.relations.DependencyGraph())
    response = client.post("/api/preview/dependencies", headers=headers(session), json=body())
    assert response.status_code == 200, response.text
    assert formats == ["TMSL"]
    assert repr(definition) == original
    assert "private-source-principal" not in response.text
    assert "private-source-credential" not in response.text
    messages = " ".join(response.json()["dependencies"])
    if definition_kind == "members":
        assert "copied without principal memberships" in messages
        assert "filters are retained" in messages
        assert "assign destination members separately" in messages
        assert "will not be created" not in messages
        copied = web.definitions.decode_json_part(validated[0]["payload"])
        assert "members" not in copied["model"]["roles"][0]
        assert (
            copied["model"]["roles"][0]["tablePermissions"]
            == model["model"]["roles"][0]["tablePermissions"]
        )
    elif definition_kind == "opaque":
        assert "Export a TMSL definition" in messages
    elif definition_kind == "credentials":
        assert "will not be created" in messages
        assert "account or credential object" in messages
    else:
        assert not messages
