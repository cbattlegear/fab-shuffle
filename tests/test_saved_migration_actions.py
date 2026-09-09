"""Saved run actions persist intent and never delete the source or bypass recovery claims."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from fabshuffle import journal
from fabshuffle.auth import ServicePrincipal, TokenProvider
from fabshuffle.config import SETTINGS
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.orchestrator import MigrationPlan, _plan_record
from fabshuffle.run import MigrationRun, RunConflict, RunRegistry, RunStatus
from fabshuffle.web import app as web
from tests.test_token_claims import jwt

SOURCE = "11111111-1111-1111-1111-111111111111"
TARGET = "22222222-2222-2222-2222-222222222222"
SCRATCH = "33333333-3333-3333-3333-333333333333"
SOURCE_TENANT = "aaaaaaaa-1111-1111-1111-111111111111"
TARGET_TENANT = "bbbbbbbb-2222-2222-2222-222222222222"


class Tokens(TokenProvider):
    def token(self, scope):
        return jwt({"tid": self.principal.tenant_id})


class Endpoint:
    def __init__(self):
        self.calls = []
        self.existing = {SOURCE, TARGET, SCRATCH}
        self.failures = {}
        self.on_delete = None

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return None

    def get(self, path):
        self.calls.append(("GET", path))
        if error := self.failures.get(("GET", path)):
            raise error
        identifier = path.split("/")[-1]
        if identifier not in self.existing:
            raise FabricApiError(
                "GET", path, 404, '{"errorCode":"WorkspaceNotFound","message":"Workspace does not exist"}',
            )
        return {"id": identifier, "displayName": "Source" if identifier == SOURCE else "Destination"}

    def request(self, method, path, *, expected):
        self.calls.append((method, path))
        assert expected == (200,)
        assert path != f"workspaces/{SOURCE}", "The source must never be deleted"
        if self.on_delete:
            self.on_delete()
        if error := self.failures.get((method, path)):
            raise error
        self.existing.remove(path.split("/")[-1])


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setattr(SETTINGS, "scratch_root", tmp_path)
    registry = RunRegistry()
    monkeypatch.setattr(web, "REGISTRY", registry)
    monkeypatch.setattr(web, "SESSIONS", web.SessionStore())
    endpoints = {}
    monkeypatch.setattr(web, "FabricClient", lambda tokens: endpoints[tokens.principal.client_id])
    return tmp_path, registry, endpoints, TestClient(web.create_app())


def prepare(setup, *, paired=False, strategy="rebuild", target=TARGET, scratch=SCRATCH, status="failed"):
    _tmp_path, _registry, endpoints, _api = setup
    source_principal = ServicePrincipal(SOURCE_TENANT, "source-app", "source-secret")
    source_tokens = Tokens(source_principal)
    target_tokens = Tokens(ServicePrincipal(TARGET_TENANT, "target-app", "target-secret")) if paired else None
    session = web.SESSIONS.create(source_principal, source_tokens, target_tokens=target_tokens)
    source = Endpoint()
    destination = Endpoint() if paired else source
    endpoints["source-app"] = source
    if paired:
        endpoints["target-app"] = destination
        source.existing = {SOURCE}
        destination.existing = {TARGET, SCRATCH}
    record = _plan_record(MigrationPlan(
        capacity_id="cap", capacity_name="Capacity", capacity_region="westus",
        source_workspace_id=SOURCE, source_workspace_name="Source", target_workspace_name="Destination",
        source_tenant_id=SOURCE_TENANT if paired else "", target_tenant_id=TARGET_TENANT if paired else "",
        source_client_id="source-app" if paired else "", target_client_id="target-app" if paired else "",
    ))
    record["strategy"] = strategy
    path = web._session_journal(session, "saved")
    book = journal.Journal(path)
    book.run_created(record, cleanup=True)
    if target:
        book.workspace("target", target, "Destination")
        book.mapping(SOURCE, target)
        book.item("old-item", "new-item", "Lakehouse", "Data")
        book.data("old-item", "table", "Orders", target_id="new-item")
    if scratch:
        book.workspace("scratch", scratch)
    if status:
        book.finished(status)
    return session, book, source, destination


def headers(session):
    return {web.SESSION_HEADER: session.id}


def post(api, session, action, **body):
    return api.post(f"/api/runs/saved/{action}", json=body, headers=headers(session))


@pytest.mark.parametrize("paired", [False, True])
def test_ignore_is_durable_and_performs_no_fabric_call(setup, paired):
    _tmp, _registry, _endpoints, api = setup
    session, book, source, target = prepare(setup, paired=paired)
    before = journal.read(book.path)
    assert post(api, session, "ignore", confirmed=True).json() == {"ignored": True, "runId": "saved"}
    after = journal.read(book.path)
    assert after.ignored
    assert after.id_map == before.id_map and after.data_done == before.data_done
    assert after.target_workspace_id == TARGET and after.scratch_workspace_id == SCRATCH
    assert not source.calls and not target.calls
    fresh_registry = RunRegistry()
    assert fresh_registry.resumable(book.path.parent, **web._session_identity(session)) == []
    assert post(api, session, "ignore", confirmed=True).status_code == 200
    assert post(api, session, "resume").status_code == 409


@pytest.mark.parametrize("action", ["ignore", "restart"])
@pytest.mark.parametrize("confirmation", [{}, {"confirmed": False}, {"confirmed": "yes"}])
def test_both_actions_require_explicit_confirmation(setup, action, confirmation):
    _tmp, _registry, _endpoints, api = setup
    session, book, source, target = prepare(setup)
    before = book.path.read_bytes()
    if action == "restart":
        confirmation = {**confirmation, "target_workspace_id": TARGET}
    response = post(api, session, action, **confirmation)
    assert response.status_code in (400, 422)
    assert book.path.read_bytes() == before
    assert not source.calls and not target.calls


@pytest.mark.parametrize("paired", [False, True])
def test_full_restart_deletes_only_recorded_workspaces_and_returns_fresh_source(setup, paired):
    _tmp, registry, _endpoints, api = setup
    session, book, source, target = prepare(setup, paired=paired)
    response = post(api, session, "restart", confirmed=True, target_workspace_id=TARGET)
    assert response.status_code == 200, response.text
    assert response.json() == {
        "restartComplete": True, "runId": "saved", "sourceWorkspace": {"id": SOURCE, "displayName": "Source"},
    }
    assert [path for method, path in target.calls if method == "DELETE"] == [
        f"workspaces/{SCRATCH}", f"workspaces/{TARGET}",
    ]
    if paired:
        assert source.calls == [("GET", f"workspaces/{SOURCE}")]
    assert not list(registry.all()), "Full restart must not automatically admit a new run"
    replay = journal.read(book.path)
    assert replay.restart_state == "complete"
    assert registry.resumable(book.path.parent, **web._session_identity(session)) == []
    assert post(api, session, "resume").status_code == 409
    assert api.get("/api/runs/saved/readiness", headers=headers(session)).status_code == 409
    # Retry after the response was lost is idempotent and must not delete anything again.
    target.calls.clear()
    assert post(api, session, "restart", confirmed=True, target_workspace_id=TARGET).status_code == 200
    assert not any(method == "DELETE" for method, _path in target.calls)


def test_wrong_confirmation_target_is_never_used_as_a_delete_path(setup):
    _tmp, _registry, _endpoints, api = setup
    session, book, source, target = prepare(setup)
    before = book.path.read_bytes()
    response = post(api, session, "restart", confirmed=True, target_workspace_id=SOURCE)
    assert response.status_code == 409
    assert not source.calls and not target.calls
    assert book.path.read_bytes() == before


@pytest.mark.parametrize(("strategy", "target", "scratch"), [
    ("reassign", TARGET, ""), ("rebuild", SOURCE, ""), ("rebuild", TARGET, SOURCE),
])
def test_source_and_reassignment_workspaces_cannot_be_deleted(setup, strategy, target, scratch):
    _tmp, _registry, _endpoints, api = setup
    session, book, source, destination = prepare(setup, strategy=strategy, target=target, scratch=scratch)
    response = post(api, session, "restart", confirmed=True, target_workspace_id=target)
    assert response.status_code == 409
    assert not source.calls and not destination.calls
    assert not journal.read(book.path).restart_state


@pytest.mark.parametrize("paired", [False, True])
def test_a_run_that_never_created_any_destination_can_start_fresh(setup, paired):
    _tmp, _registry, _endpoints, api = setup
    session, book, _source, target = prepare(setup, target="", scratch="", paired=paired)
    response = post(api, session, "restart", confirmed=True, target_workspace_id="")
    assert response.status_code == 200
    assert journal.read(book.path).restart_state == "complete"
    assert not any(method == "DELETE" for method, _path in target.calls)


@pytest.mark.parametrize("action", ["ignore", "restart"])
def test_active_related_run_blocks_saved_actions(setup, action):
    _tmp, registry, _endpoints, api = setup
    session, book, source, target = prepare(setup)
    running = MigrationRun(source_workspace_name="Source", capacity_name="Capacity")
    registry.admit(running, directory=book.path.parent, plan=journal.read(book.path).plan,
                   cleanup=False, prior=journal.read(book.path))
    response = post(api, session, action, confirmed=True, **(
        {"target_workspace_id": TARGET} if action == "restart" else {}
    ))
    assert response.status_code == 409
    assert not source.calls and not target.calls


def test_unresolved_copy_job_blocks_deletion_but_ignore_does_not_discard_its_evidence(setup):
    _tmp, _registry, _endpoints, api = setup
    session, book, source, target = prepare(setup)
    book.copy_job({
        "workspace_id": SCRATCH, "item_id": "old-item", "display_name": "Copy",
        "copy_job_id": "job", "submission_state": "submitting",
    }, target_id="new-item")
    assert post(api, session, "restart", confirmed=True, target_workspace_id=TARGET).status_code == 409
    assert not source.calls and not target.calls
    assert post(api, session, "ignore", confirmed=True).status_code == 200
    assert journal.read(book.path).copy_jobs


def test_partial_deletion_failure_can_finish_restart_but_cannot_resume_old_data(setup):
    _tmp, _registry, _endpoints, api = setup
    session, book, _source, target = prepare(setup, paired=True)
    error = FabricApiError(
        "DELETE", "target", 403, '{"errorCode":"Denied","message":"Destination is protected"}',
    )
    target.failures[("DELETE", f"workspaces/{TARGET}")] = error
    response = post(api, session, "restart", confirmed=True, target_workspace_id=TARGET)
    assert response.status_code == 403
    assert "Denied" in response.text and "Destination is protected" in response.text
    replay = journal.read(book.path)
    assert replay.restart_state == "pending" and replay.deleted_workspaces == {SCRATCH}
    assert post(api, session, "resume").status_code == 409
    listed = api.get("/api/resumable", headers=headers(session)).json()["runs"]
    assert listed[0]["recoveryAction"] == "restart_pending"
    target.failures.clear()
    assert post(api, session, "restart", confirmed=True, target_workspace_id=TARGET).status_code == 200
    assert sum(method == "DELETE" and path == f"workspaces/{SCRATCH}" for method, path in target.calls) == 1


def test_access_failure_on_preflight_does_not_delete_the_other_workspace(setup):
    _tmp, _registry, _endpoints, api = setup
    session, book, _source, target = prepare(setup)
    target.failures[("GET", f"workspaces/{TARGET}")] = FabricApiError(
        "GET", "target", 403, '{"errorCode":"Denied","message":"No destination access"}',
    )
    response = post(api, session, "restart", confirmed=True, target_workspace_id=TARGET)
    assert response.status_code == 403
    assert not journal.read(book.path).restart_state
    assert not any(method == "DELETE" for method, _path in target.calls)


def test_explicitly_missing_workspace_can_finish_restart_without_guessing_from_status(setup):
    _tmp, _registry, _endpoints, api = setup
    session, book, _source, target = prepare(setup)
    target.existing.remove(TARGET)
    assert post(api, session, "restart", confirmed=True, target_workspace_id=TARGET).status_code == 200
    assert journal.read(book.path).restart_state == "complete"
    assert ("DELETE", f"workspaces/{TARGET}") not in target.calls


def test_bare_404_is_not_treated_as_proof_of_deletion(setup):
    _tmp, _registry, _endpoints, api = setup
    session, book, _source, target = prepare(setup)
    target.failures[("GET", f"workspaces/{TARGET}")] = FabricApiError("GET", "target", 404, "not accessible")
    assert post(api, session, "restart", confirmed=True, target_workspace_id=TARGET).status_code == 404
    assert not journal.read(book.path).restart_state
    assert not any(method == "DELETE" for method, _path in target.calls)


@pytest.mark.parametrize("action", ["ignore", "restart"])
def test_wrong_paired_application_cannot_change_saved_state(setup, action):
    _tmp, _registry, _endpoints, api = setup
    session, book, source, target = prepare(setup, paired=True)
    wrong = web.SESSIONS.create(
        session.principal, session.tokens,
        target_tokens=Tokens(ServicePrincipal(TARGET_TENANT, "wrong-app", "secret")),
    )
    before = book.path.read_bytes()
    response = post(api, wrong, action, confirmed=True, **(
        {"target_workspace_id": TARGET} if action == "restart" else {}
    ))
    assert response.status_code == 409
    assert not source.calls and not target.calls
    assert book.path.read_bytes() == before


def test_full_restart_claim_blocks_simultaneous_resume_cleanup_and_actions(setup):
    _tmp, registry, _endpoints, api = setup
    session, book, _source, target = prepare(setup)
    prior = journal.read(book.path)

    def race():
        with pytest.raises((RunConflict, journal.TenantBindingError)):
            registry.admit(MigrationRun(source_workspace_name="Source", capacity_name="Capacity"),
                           directory=book.path.parent, plan=prior.plan, cleanup=False, prior=prior)
        with pytest.raises(RunConflict):
            with registry.saved_action_claim("saved", directory=book.path.parent):
                pytest.fail("Concurrent actions must not proceed")
        with pytest.raises(RunConflict):
            with registry.cleanup_claim(directory=book.path.parent):
                pytest.fail("Cleanup must not race with full restart")

    target.on_delete = race
    assert post(api, session, "restart", confirmed=True, target_workspace_id=TARGET).status_code == 200


def test_ignore_cannot_reveal_older_attempts_after_pruning(setup):
    _tmp, _registry, _endpoints, api = setup
    session, first, _source, _target = prepare(setup)
    second = journal.Journal(first.path.parent / "latest.jsonl")
    second.run_created(journal.read(first.path).plan, cleanup=False, prior=journal.read(first.path))
    second.recovery_action("ignore")
    assert api.post(
        "/api/runs/latest/ignore", json={"confirmed": True}, headers=headers(session),
    ).status_code == 200
    journal.prune(first.path.parent, keep=0)
    assert second.path.exists()
    assert RunRegistry().resumable(first.path.parent) == []


def test_cancelled_migrations_are_available_for_ignore_or_full_restart(setup):
    _tmp, _registry, _endpoints, api = setup
    session, _book, _source, _target = prepare(setup, status="cancelled")
    runs = api.get("/api/resumable", headers=headers(session)).json()["runs"]
    assert runs[0]["runId"] == "saved"
    assert runs[0]["canRestart"]


@pytest.mark.parametrize("action", ["ignore", "restart"])
def test_saved_actions_require_sign_in(setup, action):
    _tmp, _registry, _endpoints, api = setup
    body = {"confirmed": True}
    if action == "restart":
        body["target_workspace_id"] = TARGET
    assert api.post(f"/api/runs/saved/{action}", json=body).status_code == 401


def test_restart_rejects_asynchronous_acceptance_instead_of_claiming_deletion_complete(setup):
    _tmp, _registry, _endpoints, api = setup
    session, book, _source, target = prepare(setup, scratch="")
    target.failures[("DELETE", f"workspaces/{TARGET}")] = FabricApiError(
        "DELETE", "target", 202, "Deletion was accepted, completion unknown",
    )
    response = post(api, session, "restart", confirmed=True, target_workspace_id=TARGET)
    assert response.status_code == 502
    assert "completion unknown" in response.json()["detail"]
    assert journal.read(book.path).restart_state == "pending"
    assert post(api, session, "resume").status_code == 409


def test_pending_restart_survives_server_restart_and_only_deletes_remaining_target(setup, monkeypatch):
    _tmp, _registry, _endpoints, api = setup
    session, book, _source, target = prepare(setup, paired=True)
    book.recovery_action("restart_started")
    book.recovery_action("workspace_deleted", workspace_id=SCRATCH)
    target.existing.remove(SCRATCH)
    monkeypatch.setattr(web, "REGISTRY", RunRegistry())
    response = post(api, session, "restart", confirmed=True, target_workspace_id=TARGET)
    assert response.status_code == 200, response.text
    assert [path for method, path in target.calls if method == "DELETE"] == [f"workspaces/{TARGET}"]


def test_reconciled_ancestor_job_summary_does_not_block_restart_of_latest_attempt(setup):
    _tmp, registry, _endpoints, api = setup
    session, first, _source, _target = prepare(setup)
    job = {"workspace_id": SCRATCH, "item_id": "old-item", "display_name": "Copy", "copy_job_id": "job"}
    first.copy_job(job, target_id="new-item")
    old = journal.read(first.path)
    attempt = MigrationRun(source_workspace_name="Source", capacity_name="Capacity")
    attempt.id = old.run_id
    attempt.lineage_id = old.lineage_id
    attempt.plan = old.plan
    attempt.target_workspace = {"id": TARGET}
    attempt.summary["unresolvedCopyJobs"] = [job]
    attempt.mark_finished(RunStatus.FAILED)
    registry.add(attempt)
    latest = journal.Journal(first.path.parent / "latest.jsonl")
    latest.run_created(old.plan, cleanup=True, prior=old)
    latest.copy_job(job, target_id="new-item", active=False)
    latest.finished("failed")
    response = api.post(
        "/api/runs/latest/restart",
        json={"confirmed": True, "target_workspace_id": TARGET}, headers=headers(session),
    )
    assert response.status_code == 200, response.text
    assert journal.read(latest.path).restart_state == "complete"


def test_other_lineage_with_same_target_prevents_deletion_even_if_hidden_by_list_deduplication(setup):
    _tmp, registry, _endpoints, _api = setup
    _session, first, _source, _target = prepare(setup)
    second = journal.Journal(first.path.parent / "other.jsonl")
    second.run_created(journal.read(first.path).plan, cleanup=False)
    second.workspace("target", TARGET)
    second.finished("failed")
    visible = journal.latest_runs(first.path.parent)[0]
    with pytest.raises(RunConflict, match="also owns"):
        with registry.saved_action_claim(visible.run_id, directory=first.path.parent, destructive=True):
            pytest.fail("List deduplication must not hide a second resource owner")


def test_ignore_write_failure_does_not_hide_run_or_hold_action_lock(setup, monkeypatch):
    _tmp, registry, _endpoints, api = setup
    session, book, _source, _target = prepare(setup)
    original = journal.Journal.recovery_action

    def fail(*_args, **_kwargs):
        raise OSError("journal volume unavailable")

    monkeypatch.setattr(journal.Journal, "recovery_action", fail)
    response = post(api, session, "ignore", confirmed=True)
    assert response.status_code == 500
    assert "journal volume unavailable" in response.json()["detail"]
    assert not journal.read(book.path).ignored
    assert registry.resumable(book.path.parent)
    monkeypatch.setattr(journal.Journal, "recovery_action", original)
    assert post(api, session, "ignore", confirmed=True).status_code == 200


def test_restart_intent_must_be_durable_before_deleting_any_workspace(setup, monkeypatch):
    _tmp, _registry, _endpoints, api = setup
    session, book, _source, target = prepare(setup)
    original = journal.Journal.recovery_action

    def fail(self, action, **kwargs):
        if action == "restart_started":
            raise OSError("journal write refused")
        return original(self, action, **kwargs)

    monkeypatch.setattr(journal.Journal, "recovery_action", fail)
    response = post(api, session, "restart", confirmed=True, target_workspace_id=TARGET)
    assert response.status_code == 500
    assert "journal write refused" in response.json()["detail"]
    assert not any(method == "DELETE" for method, _path in target.calls)
    assert not journal.read(book.path).restart_state


def test_response_loss_after_successful_delete_does_not_repeat_already_absent_workspace(setup, monkeypatch):
    _tmp, _registry, _endpoints, api = setup
    session, book, _source, target = prepare(setup)
    original = journal.Journal.recovery_action
    failed_once = False

    def fail(self, action, **kwargs):
        nonlocal failed_once
        if action == "workspace_deleted" and not failed_once:
            failed_once = True
            raise OSError("lost journal write")
        return original(self, action, **kwargs)

    monkeypatch.setattr(journal.Journal, "recovery_action", fail)
    response = post(api, session, "restart", confirmed=True, target_workspace_id=TARGET)
    assert response.status_code == 500
    assert "lost journal write" in response.json()["detail"]
    assert journal.read(book.path).restart_state == "pending"
    assert SCRATCH not in target.existing
    assert post(api, session, "restart", confirmed=True, target_workspace_id=TARGET).status_code == 200
    assert sum(method == "DELETE" and path.endswith(SCRATCH) for method, path in target.calls) == 1
