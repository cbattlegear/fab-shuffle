"""The download endpoint for the connection advisory lookup script.

Same authorization as the cutover readiness report it is generated from - a run this session
cannot see refuses here too - and the response is a plain-text ``.ps1`` attachment, never a
JSON payload duplicating the readiness report.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from fabshuffle import journal
from fabshuffle.auth import ServicePrincipal, TokenProvider
from fabshuffle.config import SETTINGS
from fabshuffle.run import MigrationRun, RunRegistry, RunStatus
from fabshuffle.web import app as web

TENANT_ID = "72f988bf-86f1-41af-91ab-2d7cd011db47"
CONNECTION_A = "081da81e-f477-4715-8b66-c2a1debf8909"
CONNECTION_B = "aa89a365-f638-49a8-81d7-ded77940ce84"


class StubTokens(TokenProvider):
    def __init__(self):
        self.principal = ServicePrincipal(TENANT_ID, "client", "session-secret-marker")

    def token(self, scope):
        pytest.fail("Downloading a recorded lookup script must not contact an external service.")


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


def with_advisory(run, connection_ids, *, strategy="rebuild"):
    run.plan = {"strategy": strategy}
    run.set_connection_advisory({
        "scanState": "complete", "attemptId": run.id,
        "connections": [{"connectionId": cid} for cid in connection_ids],
        "message": f"{len(connection_ids)} tenant-visible connection(s)...",
    })
    run.mark_finished(RunStatus.SUCCEEDED)


def test_the_script_is_a_plain_text_attachment_not_json(client, session_id, run):
    with_advisory(run, [CONNECTION_A, CONNECTION_B])

    response = client.get(f"/api/runs/{run.id}/connections/script", headers=auth(session_id))

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["content-disposition"] == f'attachment; filename="connection-lookup-{run.id}.ps1"'
    assert f"'{CONNECTION_A}'" in response.text
    assert f"'{CONNECTION_B}'" in response.text
    assert f"$tenantId = '{TENANT_ID}'" in response.text


def test_requires_the_same_authentication_as_the_readiness_report(client, run):
    with_advisory(run, [CONNECTION_A])

    response = client.get(f"/api/runs/{run.id}/connections/script")

    assert response.status_code == 401


def test_a_forged_or_unknown_session_cannot_reach_a_real_run(client, run):
    with_advisory(run, [CONNECTION_A])

    response = client.get(f"/api/runs/{run.id}/connections/script", headers=auth("forged-session"))

    assert response.status_code == 401


def test_a_run_with_no_recorded_connections_still_gets_a_fallback_script(client, session_id, run):
    """A report predating this scan, or one where the scan simply found nothing, must not be
    a dead end: the script itself falls back to listing every connection the operator can see."""
    run.plan = {"strategy": "rebuild"}
    run.mark_finished(RunStatus.SUCCEEDED)

    response = client.get(f"/api/runs/{run.id}/connections/script", headers=auth(session_id))

    assert response.status_code == 200
    assert "$recordedConnectionIds = @(" in response.text
    assert "$fabric/v1/connections" in response.text
    assert "[string[]] $ConnectionId = @()" in response.text


def test_a_reassign_run_never_scanned_has_nothing_to_download(client, session_id, run):
    run.plan = {"strategy": "reassign"}
    run.mark_finished(RunStatus.SUCCEEDED)

    response = client.get(f"/api/runs/{run.id}/connections/script", headers=auth(session_id))

    assert response.status_code == 404


def test_an_unknown_run_id_is_not_found(client, session_id):
    response = client.get("/api/runs/does-not-exist/connections/script", headers=auth(session_id))

    assert response.status_code == 404


def test_the_response_never_contains_a_secret_or_a_live_token(client, session_id, run):
    with_advisory(run, [CONNECTION_A])

    response = client.get(f"/api/runs/{run.id}/connections/script", headers=auth(session_id))

    assert "session-secret-marker" not in response.text
    assert "NetworkCredential" not in response.text


def test_a_saved_run_serves_its_recorded_scan_too(client, session_id):
    book = journal.Journal(SETTINGS.journal_for("saved"))
    book.run_created({"source_workspace_id": "source-workspace", "strategy": "rebuild"}, cleanup=True)
    book.connection_advisory({
        "scanState": "complete", "attemptId": "saved",
        "connections": [{"connectionId": CONNECTION_A}], "message": "1 tenant-visible connection(s)...",
    })
    book.finished("succeeded")

    response = client.get("/api/runs/saved/connections/script", headers=auth(session_id))

    assert response.status_code == 200
    assert f"'{CONNECTION_A}'" in response.text
