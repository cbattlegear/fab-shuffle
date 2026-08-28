from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from fabshuffle.auth import ServicePrincipal, TokenProvider
from fabshuffle.orchestrator import MigrationPlan
from fabshuffle.run import REGISTRY, RunStatus, StepStatus
from fabshuffle.web import app as web


class StubTokens(TokenProvider):
    def __init__(self) -> None:
        self.principal = ServicePrincipal("tenant", "client", "secret")

    def token(self, scope: str) -> str:
        return "stub-token"


PLAN = MigrationPlan(
    capacity_id="cap-1",
    capacity_name="F64",
    capacity_region="westeurope",
    source_workspace_id="ws-1",
    source_workspace_name="Sales",
    target_workspace_name="Sales-westeurope",
)


@pytest.fixture
def client() -> TestClient:
    return TestClient(web.app)


@pytest.fixture
def session_id() -> str:
    session = web.SESSIONS.create(ServicePrincipal("tenant", "client", "secret"), StubTokens())
    yield session.id
    web.SESSIONS.drop(session.id)


def auth(session_id: str) -> dict[str, str]:
    return {web.SESSION_HEADER: session_id}


def test_endpoints_require_a_session(client: TestClient):
    for path in ("/api/capacities", "/api/workspaces", "/api/runs/does-not-exist"):
        assert client.get(path).status_code == 401


def test_login_rejects_bad_credentials(client: TestClient, monkeypatch):
    from fabshuffle.auth import AuthError

    def explode(self):
        raise AuthError("invalid client secret")

    monkeypatch.setattr(TokenProvider, "verify", explode)
    response = client.post(
        "/api/login",
        json={"tenant_id": "t", "client_id": "c", "client_secret": "bad"},
    )
    assert response.status_code == 401
    assert "invalid client secret" in response.json()["detail"]


def test_unknown_run_is_404(client: TestClient, session_id: str):
    response = client.get("/api/runs/nope", headers=auth(session_id))
    assert response.status_code == 404


def test_run_lifecycle_and_event_stream(client: TestClient, session_id: str, monkeypatch):
    monkeypatch.setattr(web, "build_plan", lambda *args, **kwargs: PLAN)

    def fake_migration(run, principal, plan, cleanup=True):
        run.mark_running()
        run.start_step("workspaces", "Creating target and scratch workspaces")
        run.scratch_workspace = {"id": "scratch-1", "displayName": "scratch"}
        run.finish_step("workspaces", StepStatus.SUCCEEDED, f"Created '{plan.target_workspace_name}'")
        run.mark_finished(RunStatus.SUCCEEDED)

    monkeypatch.setattr(web, "run_migration", fake_migration)

    started = client.post(
        "/api/runs",
        headers=auth(session_id),
        json={"capacity_id": "cap-1", "source_workspace_id": "ws-1"},
    )
    assert started.status_code == 200
    run_id = started.json()["runId"]
    assert started.json()["plan"]["targetWorkspaceName"] == "Sales-westeurope"

    # The SSE stream must terminate once the run finishes, and carry the final snapshot.
    with client.stream(
        "GET", f"/api/runs/{run_id}/events", params={"session_id": session_id}
    ) as stream:
        assert stream.status_code == 200
        snapshots = [
            json.loads(line[6:])
            for line in stream.iter_lines()
            if line.startswith("data: ")
        ]

    assert snapshots, "expected at least one snapshot on the event stream"
    final = snapshots[-1]
    assert final["status"] == "succeeded"
    assert final["steps"][0]["detail"] == "Created 'Sales-westeurope'"

    run = REGISTRY.get(run_id)
    assert run is not None and run.status == RunStatus.SUCCEEDED


def test_event_stream_requires_a_session(client: TestClient, session_id: str, monkeypatch):
    monkeypatch.setattr(web, "build_plan", lambda *args, **kwargs: PLAN)
    monkeypatch.setattr(web, "run_migration", lambda run, *a, **k: run.mark_finished(RunStatus.SUCCEEDED))

    run_id = client.post(
        "/api/runs",
        headers=auth(session_id),
        json={"capacity_id": "cap-1", "source_workspace_id": "ws-1"},
    ).json()["runId"]

    assert client.get(f"/api/runs/{run_id}/events", params={"session_id": "forged"}).status_code == 401


def test_cleanup_is_refused_while_a_run_is_in_flight(client: TestClient, session_id: str, monkeypatch):
    monkeypatch.setattr(web, "build_plan", lambda *args, **kwargs: PLAN)

    def never_finishes(run, principal, plan, cleanup=True):
        run.mark_running()

    monkeypatch.setattr(web, "run_migration", never_finishes)

    run_id = client.post(
        "/api/runs",
        headers=auth(session_id),
        json={"capacity_id": "cap-1", "source_workspace_id": "ws-1"},
    ).json()["runId"]

    response = client.post(f"/api/runs/{run_id}/cleanup", headers=auth(session_id))
    assert response.status_code == 409

    cancelled = client.post(f"/api/runs/{run_id}/cancel", headers=auth(session_id))
    assert cancelled.status_code == 200
    assert REGISTRY.get(run_id).cancelled is True
