from __future__ import annotations

import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from fabshuffle import journal
from fabshuffle.auth import ServicePrincipal, TokenProvider
from fabshuffle.config import SETTINGS
from fabshuffle.orchestrator import MigrationPlan
from fabshuffle.run import REGISTRY, MigrationRun, RunStatus, StepStatus
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
def client(monkeypatch) -> TestClient:
    from fabshuffle.run import RunRegistry

    registry = RunRegistry()
    monkeypatch.setattr(web, "REGISTRY", registry)
    monkeypatch.setitem(globals(), "REGISTRY", registry)
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


# ------------------------------------------------------- picking up an old run


@pytest.fixture
def journal_dir(tmp_path, monkeypatch):
    """A journal directory of this test's own.

    The session-wide one accumulates a file for every run the whole suite starts, so a test
    about which runs are offered back has to be looking at a directory it controls.
    """
    monkeypatch.setattr(SETTINGS, "scratch_root", tmp_path)
    return SETTINGS.journal_dir


def write_journal(*, finished=False, plan=True, target="ws-target"):
    """A journal on disk, as a previous run of the container would have left it."""
    run_id = uuid.uuid4().hex
    book = journal.Journal(SETTINGS.journal_for(run_id))
    if plan:
        book.run_created(
            {
                "source_workspace_id": "ws-1",
                "source_workspace_name": "Sales",
                "target_workspace_name": "Sales-westeurope",
                "capacity_id": "cap-1",
                "capacity_name": "F64",
                "capacity_region": "westeurope",
            },
            cleanup=True,
        )
    else:
        book.run_created({}, cleanup=True)
    book.workspace("target", target, "Sales-westeurope")
    book.phase_started("lakehouses")
    book.item("lh-src", "lh-new", "Lakehouse", "bronze")
    if finished:
        book.finished("succeeded")
    return run_id


def test_the_resumable_list_needs_a_session(client: TestClient):
    assert client.get("/api/resumable").status_code == 401


def test_only_runs_that_never_finished_are_offered_back(client: TestClient, session_id: str, journal_dir):
    interrupted = write_journal()
    write_journal(finished=True, target="other-target")

    listed = client.get("/api/resumable", headers=auth(session_id)).json()["runs"]

    assert [entry["runId"] for entry in listed] == [interrupted]
    entry = listed[0]
    assert entry["sourceWorkspaceName"] == "Sales"
    assert entry["targetWorkspaceName"] == "Sales-westeurope"
    assert entry["targetWorkspaceId"] == "ws-target"
    assert entry["itemsCreated"] == 1
    assert entry["lastPhase"] == "lakehouses"


def test_resuming_a_run_nobody_has_a_journal_for_is_404(client: TestClient, session_id: str):
    response = client.post("/api/runs/nope/resume", headers=auth(session_id))
    assert response.status_code == 404


def test_resuming_a_run_that_finished_is_allowed(
    client: TestClient, session_id: str, journal_dir, monkeypatch
):
    """It used to be refused. Retrying the items a run left behind reads the same journal."""
    run_id = write_journal(finished=True)
    monkeypatch.setattr(web, "run_migration", lambda run, *a, **k: run.mark_finished(RunStatus.SUCCEEDED))
    response = client.post(f"/api/runs/{run_id}/resume", headers=auth(session_id))

    assert response.status_code == 200


def test_resuming_a_run_that_finished_retries_what_it_left_behind(
    client: TestClient, session_id: str, monkeypatch, journal_dir
):
    """A finished run is picked up too: that is how the items it could not move are retried."""
    seen = {}

    def fake_migration(run, principal, plan, cleanup=True, prior=None):
        seen["prior"] = prior
        run.mark_finished(RunStatus.SUCCEEDED)

    monkeypatch.setattr(web, "run_migration", fake_migration)
    run_id = write_journal(finished=True)

    response = client.post(f"/api/runs/{run_id}/resume", headers=auth(session_id))

    assert response.status_code == 200
    # Everything the first run did is inherited, so only what is missing gets attempted.
    assert seen["prior"].id_map["lh-src"] == "lh-new"
    assert seen["prior"].status == "succeeded"


def test_a_journal_that_never_recorded_a_plan_is_refused(client: TestClient, session_id: str, journal_dir):
    run_id = write_journal(plan=False)
    response = client.post(f"/api/runs/{run_id}/resume", headers=auth(session_id))

    assert response.status_code == 409
    assert "does not say what it was migrating" in response.json()["detail"]


def test_resuming_hands_the_migration_the_earlier_attempt(
    client: TestClient, session_id: str, monkeypatch, journal_dir
):
    """The plan comes back from the journal, and so does everything already done."""
    seen = {}

    def fake_migration(run, principal, plan, cleanup=True, prior=None):
        seen["plan"] = plan
        seen["prior"] = prior
        seen["cleanup"] = cleanup
        run.mark_finished(RunStatus.SUCCEEDED)

    monkeypatch.setattr(web, "run_migration", fake_migration)
    run_id = write_journal()

    response = client.post(f"/api/runs/{run_id}/resume", headers=auth(session_id))

    assert response.status_code == 200
    assert response.json()["resumedFrom"] == run_id
    # A new run id: the old one is a record, not something to write over.
    assert response.json()["runId"] != run_id
    assert seen["plan"].target_workspace_name == "Sales-westeurope"
    assert seen["prior"].id_map["lh-src"] == "lh-new"
    assert seen["prior"].target_workspace_id == "ws-target"


def test_active_original_is_not_discovered_or_resumed(client, session_id, journal_dir):
    run_id = write_journal()
    active = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    active.id = active.lineage_id = run_id
    active.target_workspace = {"id": "ws-target"}
    active.mark_running()
    REGISTRY.add(active)
    assert client.get("/api/resumable", headers=auth(session_id)).json()["runs"] == []
    response = client.post(f"/api/runs/{run_id}/resume", headers=auth(session_id))
    assert response.status_code == 409
    assert response.json()["detail"]["runId"] == run_id


def test_simultaneous_resumes_admit_exactly_one_worker(client, session_id, journal_dir, monkeypatch):
    release = threading.Event()
    entered = threading.Event()
    runs = []

    def worker(run, *args, **kwargs):
        runs.append(run)
        run.mark_running()
        entered.set()
        release.wait(10)
        run.mark_finished(RunStatus.SUCCEEDED)

    monkeypatch.setattr(web, "run_migration", worker)
    run_id = write_journal()
    barrier = threading.Barrier(2)

    def submit():
        barrier.wait()
        with TestClient(web.app) as browser:
            return browser.post(f"/api/runs/{run_id}/resume", headers=auth(session_id))

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(pool.map(lambda _: submit(), range(2)))
        assert sorted(response.status_code for response in responses) == [200, 409]
        assert entered.wait(2)
        assert len(runs) == 1
        child_id = next(response.json()["runId"] for response in responses if response.status_code == 200)
        assert next(response for response in responses if response.status_code == 409).json()[
            "detail"
        ]["runId"] == child_id
        assert client.get("/api/resumable", headers=auth(session_id)).json()["runs"] == []
        child = journal.read(SETTINGS.journal_for(child_id))
        assert child.ancestors == [run_id]
        assert child.id_map["lh-src"] == "lh-new"
        assert child.lineage_id == run_id
        assert client.post(f"/api/runs/{run_id}/resume", headers=auth(session_id)).status_code == 409
    finally:
        release.set()


def test_superseded_ancestor_stays_hidden_after_registry_restart(
    client, session_id, journal_dir, monkeypatch
):
    from fabshuffle.run import RunRegistry

    ancestor = write_journal()
    child = journal.Journal(SETTINGS.journal_for("child"))
    child.run_created(PLAN.__dict__, cleanup=False, prior=journal.read(SETTINGS.journal_for(ancestor)))
    child.finished("failed", "retryable failure")
    monkeypatch.setattr(web, "REGISTRY", RunRegistry())
    listed = client.get("/api/resumable", headers=auth(session_id)).json()["runs"]
    assert [entry["runId"] for entry in listed] == ["child"]
    response = client.post(f"/api/runs/{ancestor}/resume", headers=auth(session_id))
    assert response.status_code == 409
    assert response.json()["detail"]["runId"] == "child"


def test_cleanup_of_an_ancestor_is_refused_while_child_is_active(
    client, session_id, journal_dir
):
    ancestor = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    ancestor.target_workspace = {"id": "ws-target"}
    ancestor.mark_finished(RunStatus.FAILED)
    REGISTRY.add(ancestor)
    child = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    child.lineage_id = ancestor.lineage_id
    child.target_workspace = ancestor.target_workspace
    REGISTRY.add(child)
    response = client.post(f"/api/runs/{ancestor.id}/cleanup", headers=auth(session_id))
    assert response.status_code == 409
    assert response.json()["detail"]["runId"] == child.id
    assert client.post("/api/scratch-workspaces/cleanup", headers=auth(session_id)).status_code == 409


def test_worker_start_failure_releases_claim_and_keeps_failure_journal(
    client, session_id, journal_dir, monkeypatch
):
    class BrokenThread:
        def __init__(self, **kwargs):
            pass

        def start(self):
            raise RuntimeError("worker unavailable")

    # Replace only the web module's reference, not Python's threading module used by TestClient.
    from types import SimpleNamespace

    monkeypatch.setattr(web, "threading", SimpleNamespace(Thread=BrokenThread))
    ancestor = write_journal()
    response = client.post(f"/api/runs/{ancestor}/resume", headers=auth(session_id))
    assert response.status_code == 500
    attempts = REGISTRY.resumable(SETTINGS.journal_dir)
    assert len(attempts) == 1
    assert attempts[0].run_id != ancestor
    assert attempts[0].status == "failed"
    assert attempts[0].error == "worker unavailable"
    assert not REGISTRY._claims


def test_cleanup_is_refused_for_remote_jobs_after_worker_failure(client, session_id, journal_dir):
    run_id = write_journal()
    book = journal.Journal(SETTINGS.journal_for(run_id))
    book.copy_job({
        "workspace_id": "scratch", "copy_job_id": "copy", "instance_id": "instance",
        "item_id": "lh-src", "display_name": "copy-lh", "label": "Lakehouse",
    }, target_id="lh-new")
    book.finished("failed", "completion unknown")
    failed = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    failed.id = failed.lineage_id = run_id
    failed.target_workspace = {"id": "ws-target"}
    failed.mark_finished(RunStatus.FAILED)
    REGISTRY.add(failed)
    for url in (f"/api/runs/{run_id}/cleanup", "/api/scratch-workspaces/cleanup"):
        response = client.post(url, headers=auth(session_id))
        assert response.status_code == 409
        assert "Unfinished Copy Jobs" in response.json()["detail"]["message"]


def test_resume_is_refused_during_cleanup(client, session_id, journal_dir):
    run_id = write_journal()
    cleaning = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    cleaning.lineage_id = run_id
    cleaning.target_workspace = {"id": "ws-target"}
    with REGISTRY.cleanup_claim(cleaning):
        response = client.post(f"/api/runs/{run_id}/resume", headers=auth(session_id))
        assert response.status_code == 409
        assert "cleanup" in response.json()["detail"]["message"]
