from __future__ import annotations

from threading import Event, Thread
from unittest.mock import Mock

import pytest

from fabshuffle.bcdr.activity import ACTIVITY, ActivityTracker, report_activity
from fabshuffle.bcdr.backend import RecoveryBlocked, now
from fabshuffle.bcdr.contracts import OperationRecord, OperationState, RecoveryMode
from fabshuffle.bcdr.deployment_lock import DeploymentLock
from fabshuffle.bcdr.service import ReconcileSyncOwnerRequest, RetryStandbyRequest, StandbySelectionRequest
from fabshuffle.bcdr.sync_recovery import SYNC_ATTEMPT, sync_recovery_state
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.web import app as web
from fabshuffle.web import bcdr
from tests.test_bcdr_contracts import guid
from tests.test_bcdr_coordinator import system as system
from tests.test_bcdr_product import product as product
from tests.test_standby_selection import scope as scope


def test_activity_read_does_not_compete_with_real_deployment_lock(product):
    client, headers, _, created, path = product
    session = web.SESSIONS.get(headers[web.SESSION_HEADER])
    key = bcdr._activity_key(session)
    entered, release = Event(), Event()

    def work():
        lock = DeploymentLock(path)
        try:
            report_activity("Capturing fixture metadata", detail="Waiting for a fixture read")
            entered.set()
            assert release.wait(10)
        finally:
            lock.close()

    worker = Thread(target=lambda: ACTIVITY.run(key, "Fixture sync", work))
    worker.start()
    try:
        assert entered.wait(5)
        response = client.get("/api/bcdr/activity", headers=headers)
        assert response.status_code == 200 and not created
        active = response.json()["active"]
        assert active[0]["action"] == "Fixture sync"
        assert active[0]["owns_deployment"] and active[0]["phase"] == "Capturing fixture metadata"
        with pytest.raises(RecoveryBlocked, match="Another worker"):
            DeploymentLock(path)
        assert client.get("/api/bcdr/activity").status_code == 401
    finally:
        release.set()
        worker.join(timeout=5)
    response = client.get("/api/bcdr/activity", headers=headers).json()
    assert not response["active"]
    assert response["recent"][0]["status"] == "completed"


def test_activity_is_identity_scoped_and_redacts_errors():
    tracker = ActivityTracker()

    def fail():
        report_activity("Reading metadata")
        raise RuntimeError("Access denied; password=never-visible;")

    with pytest.raises(RuntimeError):
        tracker.run(("tenant", "application"), "Fixture failure", fail)
    assert not tracker.snapshot(("other", "application"))["recent"]
    result = tracker.snapshot(("tenant", "application"))["recent"][0]
    assert result["status"] == "failed" and result["phase"] == "Reading metadata"
    assert "never-visible" not in result["error"]
    assert not any(key.startswith("_") for key in result)


def test_failure_before_capture_retains_request_and_retry_reuses_it(scope):
    original = scope.c.capture
    request = StandbySelectionRequest(workspace_ids=(scope.scope_entries[0]["id"],))
    preview = scope.service.preview_standby_selection(request).details["standby_preview"]
    scope.c.capture = Mock(side_effect=FabricApiError(
        "GET", "https://api.fabric.microsoft.com/v1/workspaces/example/spark/pools", 404,
        '{"errorCode":"NotFound","message":"Required metadata was not found","requestId":"request-123"}',
    ))
    with pytest.raises(FabricApiError):
        scope.service.create_standby(request.model_copy(update={
            "expected_configuration": preview["configuration"],
        }))
    attempt = scope.c.runtime.get("lifecycle", SYNC_ATTEMPT)
    assert attempt["status"] == "failed" and "NotFound" in attempt["error"]
    assert attempt["request"]["include_workspace_ids"] == list(request.workspace_ids)
    assert scope.catalog.state().mode == RecoveryMode.SYNCING
    assert scope.catalog.state().controller_id is None
    scope.source_read.reset_mock()
    options = scope.service.standby_scope_options()
    assert options.details["standby_scope"]["resume_required"]
    assert options.details["sync_recovery"]["can_retry"]
    scope.source_read.assert_not_called()
    with pytest.raises(RecoveryBlocked, match="Retry saved sync"):
        scope.service.preview_standby_selection(request)
    with pytest.raises(RecoveryBlocked, match="current sync recovery"):
        scope.service.retry_standby(RetryStandbyRequest(expected_attempt_id=guid()))
    scope.c.capture = original
    result = scope.service.retry_standby(RetryStandbyRequest(expected_attempt_id=attempt["attempt_id"]))
    assert result.details["sync_summary"]["metadata_ready"]
    saved = scope.c.runtime.get("plans", result.generation_id)
    assert saved == attempt["request"]


def test_legacy_interrupted_sync_requires_explicit_reviewed_selection(scope):
    scope.harness.raw("UPDATE bcdr.control SET mode = ?", ("syncing",))
    info = scope.service.standby_scope_options().details["sync_recovery"]
    assert info["requires_selection"] and not info["can_retry"]
    request = StandbySelectionRequest(workspace_ids=(scope.scope_entries[0]["id"],))
    preview = scope.service.preview_standby_selection(request).details["standby_preview"]
    assert preview["resume_interrupted"]
    with pytest.raises(RecoveryBlocked, match="changed"):
        scope.service.create_standby(request.model_copy(update={
            "expected_configuration": preview["configuration"],
        }))
    assert not scope.captures
    result = scope.service.create_standby(request.model_copy(update={
        "expected_configuration": preview["configuration"], "resume_interrupted": True,
    }))
    assert result.details["sync_summary"]["metadata_ready"]
    saved = scope.c.runtime.get("lifecycle", SYNC_ATTEMPT)["request"]
    assert saved["include_workspace_ids"] == list(request.workspace_ids)


def test_legacy_failure_does_not_mistake_previous_completed_scope_for_failed_request(scope, monkeypatch):
    scope.service.synchronize(scope.request)
    original = scope.c.runtime.get
    monkeypatch.setattr(scope.c.runtime, "get",
                        lambda namespace, key: None if key == SYNC_ATTEMPT else original(namespace, key))
    scope.harness.raw("UPDATE bcdr.control SET mode = ?", ("syncing",))
    result = scope.service.standby_scope_options()
    assert result.details["sync_recovery"]["requires_selection"]
    assert not result.details["sync_recovery"]["can_retry"]
    assert result.details["standby_scope"]["saved_selection"] is None

@pytest.mark.parametrize("same_controller", [False, True])
def test_abandoned_sync_owner_requires_explicit_fencing_and_does_not_start_work(system, same_controller):
    owner = system.catalog.acquire_controller(system.c.runtime.controller_id if same_controller else guid())
    system.catalog.transition_mode(owner, RecoveryMode.STANDBY, RecoveryMode.SYNCING, guid())
    info = sync_recovery_state(system.c)
    assert info["can_reconcile_owner"] and not info["can_retry"]
    request = ReconcileSyncOwnerRequest(
        expected_controller_id=owner.controller_id, expected_epoch=owner.epoch,
        previous_controller_stopped=True, fencing_evidence="Previous worker stopped and fenced",
    )
    result = system.service.reconcile_sync_owner(request)
    assert result.mode == RecoveryMode.SYNCING
    assert system.catalog.state().controller_id is None
    assert result.details["sync_recovery"]["requires_selection"]
    assert not system.captures and not system.estate.calls


def test_receipt_race_keeps_new_owner_fenced(system, monkeypatch):
    owner = system.catalog.acquire_controller(guid())
    system.catalog.transition_mode(owner, RecoveryMode.STANDBY, RecoveryMode.SYNCING, guid())
    original = system.catalog.takeover_controller

    def racing(*args, **kwargs):
        lease = original(*args, **kwargs)
        system.catalog.begin_operation(lease, OperationRecord(
            operation_id=guid(), kind="item-apply", state=OperationState.INTENT,
            recorded_at=now(), ownership_evidence="A receipt appeared during takeover",
        ))
        return lease

    monkeypatch.setattr(system.catalog, "takeover_controller", racing)
    request = ReconcileSyncOwnerRequest(
        expected_controller_id=owner.controller_id, expected_epoch=owner.epoch,
        previous_controller_stopped=True, fencing_evidence="Previous worker fenced",
    )
    with pytest.raises(RecoveryBlocked, match="Ownership remains fenced"):
        system.service.reconcile_sync_owner(request)
    assert system.catalog.state().controller_id == system.c.runtime.controller_id
    assert system.catalog.pending_operations()

def test_pending_mutation_prevents_owner_release_and_retry(system):
    owner = system.catalog.acquire_controller(guid())
    system.catalog.transition_mode(owner, RecoveryMode.STANDBY, RecoveryMode.SYNCING, guid())
    operation = OperationRecord(operation_id=guid(), kind="item-apply", state=OperationState.INTENT,
                                recorded_at=now(), ownership_evidence="Recorded uncertain mutation")
    system.catalog.begin_operation(owner, operation)
    request = ReconcileSyncOwnerRequest(
        expected_controller_id=owner.controller_id, expected_epoch=owner.epoch,
        previous_controller_stopped=True, fencing_evidence="Worker stopped",
    )
    with pytest.raises(RecoveryBlocked, match="pending mutation"):
        system.service.reconcile_sync_owner(request)
    assert system.catalog.state().controller_id == owner.controller_id
    assert system.catalog.pending_operations()[0].operation_id == operation.operation_id
    assert not sync_recovery_state(system.c)["can_retry"]


def test_retry_transport_uses_source_only_after_explicit_confirmation(product):
    from tests.test_bcdr_product import GENERATION

    client, headers, service, created, _ = product
    response = client.post("/api/bcdr/retry-standby", headers=headers, json={
        "confirmation": "retry-standby", "request": {"expected_attempt_id": GENERATION},
    })
    assert response.status_code == 200
    assert service.calls[0][0] == "retry-standby"
    assert created[0][1]["source_tokens"] is not None
    assert client.post("/api/bcdr/retry-standby", json={
        "confirmation": "retry-standby", "request": {"expected_attempt_id": GENERATION},
    }).status_code == 401
