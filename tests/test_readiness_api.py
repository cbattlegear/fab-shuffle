"""Authenticated advisory reports must survive restart without exporting raw run data."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from fabshuffle import journal
from fabshuffle.auth import ServicePrincipal, TokenProvider
from fabshuffle.config import SETTINGS
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.lifecycle import Disposition, EvidenceState, Lifecycle
from fabshuffle.run import MigrationRun, RunRegistry, RunStatus
from fabshuffle.web import app as web


class StubTokens(TokenProvider):
    def __init__(self):
        self.principal = ServicePrincipal("tenant", "client", "session-secret-marker")

    def token(self, scope):
        pytest.fail("Reading recorded readiness must not contact an external service.")


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(SETTINGS, "scratch_root", tmp_path)
    monkeypatch.setattr(web, "REGISTRY", RunRegistry())
    monkeypatch.setattr(web, "SESSIONS", web.SessionStore())
    with TestClient(web.app) as client:
        yield client


@pytest.fixture
def session_id(client):
    tokens = StubTokens()
    session = web.SESSIONS.create(tokens.principal, tokens)
    yield session.id
    web.SESSIONS.drop(session.id)


@pytest.fixture
def run(client):
    return web.REGISTRY.add(MigrationRun(source_workspace_name="Sales", capacity_name="F64"))


def auth(session_id):
    return {web.SESSION_HEADER: session_id}


def ready_notebook(run, source="notebook", name="Ingest"):
    item = run.lifecycle.item(source, name, "Notebook")
    item.resolve(f"target-{source}", "target-workspace", Disposition.CREATED)
    item.step("definition", EvidenceState.SUCCEEDED, "Definition copied.")
    item.step("rebind", EvidenceState.SUCCEEDED, "References repointed.")
    item.complete()
    return item


def saved_book(run_id="saved"):
    book = journal.Journal(SETTINGS.journal_for(run_id))
    book.run_created({"source_workspace_id": "source-workspace"}, cleanup=True)
    book.workspace("target", "target-workspace", "Migrated Sales")
    return book


@pytest.mark.parametrize("download", [False, True])
@pytest.mark.parametrize("credential", ["missing", "forged", "query-only", "expired"])
def test_readiness_requires_a_current_authenticated_header(
    client, session_id, run, monkeypatch, download, credential,
):
    def unexpected_report(*args, **kwargs):
        pytest.fail("Unauthenticated callers must not read evidence or journals.")

    monkeypatch.setattr(web, "_readiness", unexpected_report)
    headers = {}
    params = {"download": str(download).lower()}
    if credential == "forged":
        headers = auth("forged-session")
    elif credential == "query-only":
        params["session_id"] = session_id
    elif credential == "expired":
        web.SESSIONS.drop(session_id)
        headers = auth(session_id)
    response = client.get(f"/api/runs/{run.id}/readiness", headers=headers, params=params)
    assert response.status_code == 401
    assert "Sign in" in response.json()["detail"]


@pytest.mark.parametrize("download", [False, True])
def test_authenticated_readiness_returns_the_full_json_report(client, session_id, run, download):
    ready_notebook(run)
    skipped = ready_notebook(run, "warehouse-copy", "Orders data copy")
    skipped.step(
        "data", EvidenceState.SKIPPED, "Data copying was disabled.",
        action="Resume Orders with data copying enabled.",
    )
    run.inventory_complete = True
    run.mark_finished(RunStatus.SUCCEEDED)

    response = client.get(
        f"/api/runs/{run.id}/readiness", headers=auth(session_id),
        params={"download": str(download).lower()},
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert response.headers["cache-control"] == "no-store"
    if download:
        assert response.headers["content-disposition"] == f'attachment; filename="cutover-{run.id}.json"'
    else:
        assert "content-disposition" not in response.headers
    body = response.json()
    assert body["schemaVersion"] == 1
    assert body["runId"] == body["lineageId"] == run.id
    assert body["runStatus"] == "succeeded"
    assert body["state"] == "needs_attention"
    assert body["counts"] == {"ready": 1, "unknown": 0, "needs_attention": 1}
    assert {item["sourceId"] for item in body["items"]} == {"notebook", "warehouse-copy"}
    assert all(item["steps"] and item["reasons"] for item in body["items"])
    assert body["limits"]
    assert any("not permission to delete" in limit for limit in body["limits"])
    other = client.get(
        f"/api/runs/{run.id}/readiness", headers=auth(session_id),
        params={"download": str(not download).lower()},
    )
    assert other.json() == body


@pytest.mark.parametrize("download", [False, True])
def test_missing_run_and_missing_saved_journal_return_not_found(client, session_id, download):
    response = client.get(
        "/api/runs/not-found/readiness", headers=auth(session_id),
        params={"download": str(download).lower()},
    )
    assert response.status_code == 404
    assert "journal" in response.json()["detail"].lower()
    assert "items" not in response.json()


@pytest.mark.parametrize("run_id", [
    "..%5cescape",
    "..%2fescape",
    "C%3A%5cescape",
    "bad%22name",
    "bad%0D%0AX-Injected%3Ayes",
    "bad%00name",
    "x" * 129,
])
def test_invalid_run_ids_cannot_reach_the_filesystem_or_download_header(
    client, session_id, monkeypatch, run_id,
):
    def unexpected_path(*args, **kwargs):
        pytest.fail("Reject unsafe run IDs before constructing a journal path.")

    monkeypatch.setattr(type(SETTINGS), "journal_for", unexpected_path)
    response = client.get(
        f"/api/runs/{run_id}/readiness", headers=auth(session_id),
        params={"download": "true"},
    )
    assert response.status_code in (400, 404)
    assert "content-disposition" not in response.headers
    assert "x-injected" not in response.headers


def test_invalid_in_memory_run_id_is_rejected_as_well(client, session_id, run, monkeypatch):
    run.id = "bad:run"
    web.REGISTRY.add(run)

    def unexpected_path(*args, **kwargs):
        pytest.fail("In-memory runs must not bypass run-ID validation.")

    monkeypatch.setattr(type(SETTINGS), "journal_for", unexpected_path)
    response = client.get("/api/runs/bad%3Arun/readiness?download=true", headers=auth(session_id))
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid run ID"


def test_valid_saved_run_id_allows_letters_digits_underscores_and_hyphens(client, session_id):
    book = saved_book("Saved_run-09")
    book.inventory()
    book.finished("succeeded")
    response = client.get("/api/runs/Saved_run-09/readiness?download=true", headers=auth(session_id))
    assert response.status_code == 200
    assert response.json()["state"] == "ready"
    assert response.headers["content-disposition"] == 'attachment; filename="cutover-Saved_run-09.json"'


def test_saved_evidence_is_identical_after_registry_restart(client, session_id, run, monkeypatch):
    book = saved_book(run.id)
    run.lifecycle = Lifecycle(
        attempt_id=run.id, source_workspace="source-workspace",
        record=book.outcome, changed=run.readiness_changed,
    )
    ready_notebook(run)
    book.item("notebook", "target-notebook", "Notebook", "Ingest")
    book.inventory()
    run.inventory_complete = True
    book.finished("succeeded")
    run.mark_finished(RunStatus.SUCCEEDED)
    before = client.get(f"/api/runs/{run.id}/readiness", headers=auth(session_id))
    original_bytes = book.path.read_bytes()

    monkeypatch.setattr(web, "REGISTRY", RunRegistry())
    after = client.get(f"/api/runs/{run.id}/readiness?download=true", headers=auth(session_id))
    assert before.status_code == after.status_code == 200
    assert after.json() == before.json()
    assert after.json()["state"] == "ready"
    assert web.REGISTRY.get(run.id) is None
    assert after.headers["cache-control"] == "no-store"
    assert book.path.read_bytes() == original_bytes


@pytest.mark.parametrize("inventory_complete", [False, True])
def test_legacy_item_data_and_mapping_checkpoints_are_not_success_evidence(
    client, session_id, inventory_complete,
):
    book = saved_book()
    book.item("warehouse", "target-warehouse", "Warehouse", "Orders")
    book.data("warehouse", "schema", target_id="target-warehouse")
    book.data("warehouse", "tables", target_id="target-warehouse")
    book.mapping("old-server", "new-server", owner="warehouse")
    if inventory_complete:
        book.inventory()
    book.finished("succeeded")

    response = client.get("/api/runs/saved/readiness", headers=auth(session_id))
    assert response.status_code == 200
    body = response.json()
    assert body["state"] == "unknown"
    assert body["runStatus"] == "succeeded"
    assert body["inventoryComplete"] is inventory_complete
    assert body["counts"] == {"ready": 0, "unknown": 1, "needs_attention": 0}
    assert len(body["items"]) == 1
    outcome = body["items"][0]
    assert outcome["name"] == "Orders"
    assert outcome["sourceId"] == "warehouse"
    assert outcome["targetId"] == "target-warehouse"
    assert outcome["disposition"] == "unknown"
    assert outcome["state"] == "unknown"
    assert {step["step"] for step in outcome["steps"]} == {"target", "schema", "data"}
    assert all(step["state"] == "unknown" for step in outcome["steps"])


def test_legacy_endpoint_mappings_do_not_invent_migrated_items(client, session_id):
    book = saved_book()
    book.mapping("source-sql-endpoint", "target-sql-endpoint")
    book.data("unidentified-source", "tables")
    book.finished("succeeded")
    response = client.get("/api/runs/saved/readiness", headers=auth(session_id))
    assert response.status_code == 200
    assert response.json()["state"] == "unknown"
    assert response.json()["items"] == []
    assert response.json()["inventoryComplete"] is False


def test_saved_outcome_without_required_steps_or_evidence_is_unknown(client, session_id):
    book = saved_book()
    book.outcome({
        "sourceId": "notebook", "name": "Ingest", "itemType": "Notebook",
        "targetId": "target-notebook",
    })
    book.inventory()
    book.finished("succeeded")
    response = client.get("/api/runs/saved/readiness", headers=auth(session_id))
    assert response.status_code == 200
    assert response.json()["state"] == "unknown"
    assert response.json()["items"][0]["state"] == "unknown"


def test_newer_target_checkpoint_cannot_reuse_evidence_for_the_previous_target(client, session_id, run):
    book = saved_book()
    run.lifecycle = Lifecycle(attempt_id="saved", record=book.outcome)
    ready_notebook(run)
    book.inventory()
    book.item("notebook", "replacement-notebook", "Notebook", "Ingest")
    book.finished("succeeded")

    response = client.get("/api/runs/saved/readiness", headers=auth(session_id))
    assert response.status_code == 200
    body = response.json()
    assert body["state"] == "unknown"
    assert body["items"][0]["targetId"] == "replacement-notebook"
    assert body["items"][0]["state"] == "unknown"


@pytest.mark.parametrize(("inventory_complete", "expected"), [(False, "unknown"), (True, "ready")])
def test_saved_empty_inventory_requires_an_explicit_completion_record(
    client, session_id, inventory_complete, expected,
):
    book = saved_book()
    if inventory_complete:
        book.inventory()
    book.finished("succeeded")
    response = client.get("/api/runs/saved/readiness", headers=auth(session_id))
    assert response.status_code == 200
    assert response.json()["state"] == expected
    assert response.json()["items"] == []


def test_truncated_journal_cannot_certify_a_complete_inventory(client, session_id):
    book = saved_book()
    book.inventory()
    book.finished("succeeded")
    with book.path.open("a", encoding="utf-8") as handle:
        handle.write('{"t":"outcome","item":')
    response = client.get("/api/runs/saved/readiness", headers=auth(session_id))
    assert response.status_code == 200
    assert response.json()["inventoryComplete"] is False
    assert response.json()["state"] == "unknown"


@pytest.mark.parametrize("malformed", [
    None,
    [],
    {"sourceId": "notebook", "steps": {"definition": None}},
    {"sourceId": "notebook", "steps": {"definition": []}},
    {"sourceId": "notebook", "steps": []},
    {"sourceId": "notebook", "steps": [{"state": "succeeded"}]},
], ids=["null-item", "list-item", "null-step", "list-step", "empty-list-steps", "list-steps"])
def test_malformed_saved_outcomes_do_not_crash_readiness_or_resumable_api(
    client, session_id, run, malformed,
):
    book = saved_book()
    run.lifecycle = Lifecycle(attempt_id="saved", record=book.outcome)
    ready_notebook(run)
    book.item("notebook", "target-notebook", "Notebook", "Ingest")
    book.data("notebook", "files", target_id="target-notebook")
    book.inventory()
    book.outcome(malformed)
    book.finished("failed", "Interrupted before verification.")

    response = client.get("/api/runs/saved/readiness?download=true", headers=auth(session_id))
    assert response.status_code == 200
    body = response.json()
    assert body["state"] == "unknown"
    assert body["inventoryComplete"] is False
    assert body["runStatus"] == "failed"
    assert body["items"][0]["targetId"] == "target-notebook"
    assert all(step["state"] == "unknown" for step in body["items"][0]["steps"])
    assert any(
        "Journal contains incomplete records; previous evidence is uncertain." in reason
        for reason in body["items"][0]["reasons"]
    )
    assert response.headers["cache-control"] == "no-store"
    resumable = client.get("/api/resumable", headers=auth(session_id))
    assert resumable.status_code == 200
    assert [entry["runId"] for entry in resumable.json()["runs"]] == ["saved"]
    replay = journal.read(book.path)
    assert replay.damaged_lines == 1
    assert replay.data_is_done("notebook", "files")


@pytest.mark.parametrize("saved", [False, True])
@pytest.mark.parametrize("download", [False, True])
def test_failure_exports_sanitized_service_code_and_message_not_raw_payload_or_tokens(
    client, session_id, run, monkeypatch, saved, download,
):
    book = saved_book(run.id)
    run.lifecycle = Lifecycle(attempt_id=run.id, record=book.outcome, changed=run.readiness_changed)
    item = ready_notebook(run)
    error = FabricApiError(
        "POST", "https://example.invalid/items?sig=url-secret-marker", 403, json.dumps({
            "errorCode": "InsufficientPrivileges",
            "message": "Orders access denied. Bearer bearer-secret-marker "
                       "password=password-secret-marker; client_secret=client-secret-marker",
            "payload": {"parts": [{"payload": "raw-definition-marker"}]},
            "access_token": "raw-access-token-marker",
        }),
    )
    with pytest.raises(FabricApiError):
        with item.operation("definition", "Definition copied."):
            raise error
    book.inventory()
    run.inventory_complete = True
    book.warning("Warning contains warning-secret-marker")
    book.finished("failed", "Raw run error contains run-error-secret-marker")
    run.summary = {"payload": "summary-secret-marker"}
    run.mark_finished(RunStatus.FAILED, "Raw run error contains run-error-secret-marker")
    if saved:
        monkeypatch.setattr(web, "REGISTRY", RunRegistry())

    response = client.get(
        f"/api/runs/{run.id}/readiness", headers=auth(session_id),
        params={"download": str(download).lower()},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["state"] == "needs_attention"
    failed = next(step for step in body["items"][0]["steps"] if step["step"] == "definition")
    assert failed["state"] == "failed"
    assert failed["errorCode"] == "InsufficientPrivileges"
    assert "Orders access denied." in failed["message"]
    for secret in (
        "bearer-secret-marker", "password-secret-marker", "client-secret-marker",
        "url-secret-marker", "raw-definition-marker", "raw-access-token-marker",
        "run-error-secret-marker", "summary-secret-marker", "warning-secret-marker",
        "session-secret-marker",
    ):
        assert secret not in response.text
    assert response.headers["cache-control"] == "no-store"


def test_saved_historical_outcomes_are_allowlisted_and_sanitized_too(client, session_id, run):
    item = ready_notebook(run)
    item.step(
        "definition", EvidenceState.FAILED, "The definition could not be copied.",
        error=FabricApiError("POST", "https://example.invalid/items", 403, json.dumps({
            "errorCode": "CopyDenied", "message": "Orders could not be copied.",
        })),
    )
    outcome = run.lifecycle.snapshot()["notebook"].record()
    previous = run.lifecycle.snapshot()["notebook"].record()
    previous.pop("history")
    previous["payload"] = {"parts": [{"payload": "historical-definition-marker"}]}
    previous["access_token"] = "historical-access-token-marker"
    previous["steps"]["definition"]["message"] += " Bearer historical-bearer-marker"
    outcome["history"] = [previous]
    outcome["payload"] = "top-level-definition-marker"
    book = saved_book()
    book.outcome(outcome)
    book.inventory()
    book.finished("failed")

    response = client.get("/api/runs/saved/readiness?download=true", headers=auth(session_id))
    assert response.status_code == 200
    for secret in (
        "historical-definition-marker", "historical-access-token-marker",
        "historical-bearer-marker", "top-level-definition-marker",
    ):
        assert secret not in response.text
    history = response.json()["items"][0]["history"]
    assert len(history) == 1
    assert history[0]["steps"]["definition"]["errorCode"] == "CopyDenied"
    assert "Orders could not be copied." in history[0]["steps"]["definition"]["message"]


def test_saved_attempt_metadata_omits_raw_run_errors_and_payload_fields(client, session_id):
    first = saved_book("first")
    first.inventory()
    first.phase_started("definitions")
    first.finished("failed", "Bearer attempt-token-marker payload=attempt-payload-marker")
    prior = journal.read(first.path)
    second = journal.Journal(SETTINGS.journal_for("second"))
    second.run_created({"source_workspace_id": "source-workspace"}, cleanup=True, prior=prior)
    response = client.get("/api/runs/second/readiness?download=true", headers=auth(session_id))
    assert response.status_code == 200
    assert response.json()["lineageId"] == "first"
    assert response.json()["runStatus"] == "interrupted"
    assert response.json()["attempts"] == [
        {"run_id": "first", "status": "failed", "last_phase": "definitions"},
    ]
    assert "attempt-token-marker" not in response.text
    assert "attempt-payload-marker" not in response.text


@pytest.mark.parametrize("run_status", list(RunStatus))
@pytest.mark.parametrize(("evidence_state", "readiness"), [
    (EvidenceState.SUCCEEDED, "ready"),
    (EvidenceState.UNKNOWN, "unknown"),
    (EvidenceState.FAILED, "needs_attention"),
])
def test_readiness_is_advisory_and_never_changes_run_status_or_progress(
    client, session_id, run, run_status, evidence_state, readiness,
):
    item = ready_notebook(run)
    item.step("definition", evidence_state, "Definition observation.")
    run.inventory_complete = True
    run.status = run_status
    run.error = "Existing run error"
    run.cancel()
    before = run.snapshot()
    evidence_before = run.lifecycle.snapshot()

    for download in ("false", "true"):
        response = client.get(
            f"/api/runs/{run.id}/readiness", headers=auth(session_id),
            params={"download": download},
        )
        assert response.status_code == 200
        assert response.json()["runStatus"] == run_status.value
        assert response.json()["state"] == readiness
    assert run.snapshot() == before
    assert run.lifecycle.snapshot() == evidence_before
    assert run.cancelled is True


def test_readiness_updates_only_a_revision_in_normal_progress_snapshots(run):
    subscriber = run.subscribe()
    try:
        initial = subscriber.get_nowait()
        ready_notebook(run, "private-source-marker", "Private item-name marker")
        assert run.readiness_revision > initial["readinessRevision"]
        assert subscriber.empty()
        run.start_step("definitions", "Copying definitions")
        snapshot = subscriber.get_nowait()
        assert snapshot["readinessRevision"] == run.readiness_revision
        assert snapshot["steps"][0]["id"] == "definitions"
        assert set(key for key in snapshot if "readiness" in key.lower()) == {"readinessRevision"}
        assert not {"items", "outcomes", "lifecycle"} & snapshot.keys()
        encoded = json.dumps(snapshot)
        assert "private-source-marker" not in encoded
        assert "Private item-name marker" not in encoded
    finally:
        run.unsubscribe(subscriber)


def test_sse_transmits_the_readiness_revision_without_full_item_lists(client, session_id, run):
    ready_notebook(run, "private-source-marker", "Private item-name marker")
    run.inventory_complete = True
    run.mark_finished(RunStatus.SUCCEEDED)

    with client.stream(
        "GET", f"/api/runs/{run.id}/events", params={"session_id": session_id},
    ) as stream:
        assert stream.status_code == 200
        snapshots = [
            json.loads(line[6:]) for line in stream.iter_lines() if line.startswith("data: ")
        ]
    assert snapshots
    for snapshot in snapshots:
        assert snapshot["readinessRevision"] == run.readiness_revision
        assert snapshot["status"] == "succeeded"
        assert not {"items", "outcomes", "lifecycle", "readiness"} & snapshot.keys()
        assert "private-source-marker" not in json.dumps(snapshot)
        assert "Private item-name marker" not in json.dumps(snapshot)


# ------------------------------------------------------- connection advisories section


def test_connection_advisories_defaults_to_unknown_not_a_false_green(client, session_id, run):
    run.plan = {"strategy": "rebuild"}
    run.mark_finished(RunStatus.SUCCEEDED)

    response = client.get(f"/api/runs/{run.id}/readiness", headers=auth(session_id))
    assert response.status_code == 200
    advisories = response.json()["connectionAdvisories"]
    assert advisories["scanState"] == "unknown"
    assert advisories["connections"] == []
    # It must never contribute to the item-evidence counts or overall state.
    assert response.json()["state"] in ("ready", "unknown", "needs_attention")


def test_connection_advisories_from_this_attempt_are_reported_as_is(client, session_id, run):
    run.plan = {"strategy": "rebuild"}
    run.set_connection_advisory({
        "matcherVersion": 2,
        "scanState": "complete", "sourceWorkspaceId": "src", "targetWorkspaceId": "tgt",
        "attemptId": run.id, "connections": [{"connectionId": "conn-1", "connectionName": "Bronze SQL"}],
        "message": "1 tenant-visible connection(s)...",
    })
    run.mark_finished(RunStatus.SUCCEEDED)

    response = client.get(f"/api/runs/{run.id}/readiness", headers=auth(session_id))
    advisories = response.json()["connectionAdvisories"]
    assert advisories["scanState"] == "complete"
    assert advisories["connections"][0]["connectionId"] == "conn-1"
    assert "attemptId" not in advisories


def test_connection_advisories_from_an_earlier_attempt_are_marked_stale(client, session_id, run):
    run.plan = {"strategy": "rebuild"}
    run.set_connection_advisory({
        "matcherVersion": 2,
        "scanState": "complete", "connections": [{"connectionId": "conn-1"}],
        "attemptId": "an-earlier-attempt", "message": "1 tenant-visible connection(s)...",
    })
    run.mark_finished(RunStatus.SUCCEEDED)

    response = client.get(f"/api/runs/{run.id}/readiness", headers=auth(session_id))
    advisories = response.json()["connectionAdvisories"]
    assert advisories["scanState"] == "stale"
    assert "earlier attempt" in advisories["message"]
    assert advisories["connections"] == [{"connectionId": "conn-1"}]


def test_reassign_runs_report_connection_advisories_as_not_applicable(client, session_id, run):
    """A reassign never scans, and never should: nothing changes ids or paths."""
    run.plan = {"strategy": "reassign"}
    run.mark_finished(RunStatus.SUCCEEDED)

    response = client.get(f"/api/runs/{run.id}/readiness", headers=auth(session_id))
    advisories = response.json()["connectionAdvisories"]
    assert advisories["scanState"] == "not_applicable"
    assert advisories["connections"] == []


def test_saved_journal_reports_its_recorded_connection_advisory(client, session_id):
    book = saved_book()
    book.connection_advisory({
        "matcherVersion": 2,
        "scanState": "complete", "connections": [{"connectionId": "conn-1"}], "attemptId": "saved",
        "message": "1 tenant-visible connection(s)...",
    })
    book.finished("succeeded")

    response = client.get("/api/runs/saved/readiness", headers=auth(session_id))
    assert response.status_code == 200
    advisories = response.json()["connectionAdvisories"]
    assert advisories["scanState"] == "complete"
    assert advisories["connections"][0]["connectionId"] == "conn-1"


def test_saved_journal_without_a_recorded_scan_is_unknown(client, session_id):
    book = saved_book()
    book.finished("succeeded")

    response = client.get("/api/runs/saved/readiness", headers=auth(session_id))
    advisories = response.json()["connectionAdvisories"]
    assert advisories["scanState"] == "unknown"


def test_connection_advisories_survive_registry_restart(client, session_id, run, monkeypatch):
    book = saved_book(run.id)
    run.plan = {"strategy": "rebuild"}
    payload = {
        "matcherVersion": 2,
        "scanState": "complete", "connections": [{"connectionId": "conn-1"}], "attemptId": run.id,
        "message": "1 tenant-visible connection(s)...",
    }
    book.connection_advisory(payload)
    run.set_connection_advisory(payload)
    run.mark_finished(RunStatus.SUCCEEDED)
    before = client.get(f"/api/runs/{run.id}/readiness", headers=auth(session_id))

    monkeypatch.setattr(web, "REGISTRY", RunRegistry())
    after = client.get(f"/api/runs/{run.id}/readiness", headers=auth(session_id))

    assert before.status_code == after.status_code == 200
    assert before.json()["connectionAdvisories"] == after.json()["connectionAdvisories"]
    assert after.json()["connectionAdvisories"]["connections"][0]["connectionId"] == "conn-1"


def test_retired_matcher_results_are_withheld_in_live_saved_and_downloaded_reports(
    client, session_id, run, monkeypatch,
):
    book = saved_book(run.id)
    run.plan = {"strategy": "rebuild"}
    payload = {
        "scanState": "complete", "attemptId": run.id,
        "connections": [{"connectionId": "misleading-match", "expectedNewPath": "wrong-server;new-db"}],
    }
    book.connection_advisory(payload)
    book.finished("succeeded")
    run.set_connection_advisory(payload)
    run.mark_finished(RunStatus.SUCCEEDED)
    for registry in (web.REGISTRY, RunRegistry()):
        monkeypatch.setattr(web, "REGISTRY", registry)
        for suffix in ("", "?download=true"):
            response = client.get(f"/api/runs/{run.id}/readiness{suffix}", headers=auth(session_id))
            assert response.status_code == 200
            section = response.json()["connectionAdvisories"]
            assert section["scanState"] == "stale"
            assert section["connections"] == []
            assert "wrong-server" not in response.text
    assert journal.read(book.path).connection_advisory == payload
