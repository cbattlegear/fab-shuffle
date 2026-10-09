import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from fabshuffle.bcdr import __main__ as cli
from fabshuffle.bcdr.backend import RecoveryBlocked, now
from fabshuffle.bcdr.contracts import OperationRecord, OperationState, RecoveryMode
from fabshuffle.bcdr.deployment_lock import LEASE_ENV
from fabshuffle.bcdr.scheduled import MAX_REQUEST_BYTES, read_request, scheduled_sync
from tests.test_bcdr_contracts import guid
from tests.test_bcdr_coordinator import production_capacities
from tests.test_bcdr_coordinator import system as system
from tests.test_bcdr_deployment_lock import URL, BlobServer


@pytest.fixture
def scheduled(system, monkeypatch, tmp_path):
    monkeypatch.setenv(LEASE_ENV, URL)
    guard = Mock()
    system.capacities.lock = SimpleNamespace(remote=object(), assert_held=guard)
    system.factory = Mock(return_value=system.service)
    system.path = tmp_path / "bootstrap.json"
    system.request = system.request.model_copy(update={"park": True})
    system.run = lambda: scheduled_sync(
        system.path, system.request, target_tokens=system.c.tokens, service_factory=system.factory,
    )
    return system


def test_scheduled_metadata_sync_uses_existing_capture_and_parks(scheduled):
    result = scheduled.run()
    assert result.details["scheduled_status"] == "completed"
    assert result.mode == RecoveryMode.PARKING
    assert len(scheduled.captures) == 1
    assert any(isinstance(row, tuple) and row[0] == "park" for row in scheduled.capacities.events)
    assert scheduled.capacities.events[-1] == "close"
    assert not any("jobs/instances" in path and method == "POST" for method, path in scheduled.estate.calls)
    assert scheduled.factory.call_count == 1


@pytest.mark.parametrize("mode", [mode for mode in RecoveryMode if mode != RecoveryMode.STANDBY])
def test_scheduled_skips_any_nonstandby_without_capture_business_resume_or_pause(scheduled, mode):
    scheduled.harness.raw("UPDATE bcdr.control SET mode = ?", (mode.value,))
    result = scheduled.run()
    assert result.outcome == "blocked"
    assert result.exit_code == 2
    assert result.details["scheduled_status"] == "skipped"
    assert not scheduled.captures
    assert scheduled.capacities.events == ["close"]
    assert scheduled.estate.calls == []


@pytest.mark.parametrize("same_controller", [False, True])
def test_scheduled_never_steals_persisted_owner_even_same_deployment(scheduled, same_controller):
    owner = scheduled.c.runtime.controller_id if same_controller else guid()
    scheduled.catalog.acquire_controller(owner)
    result = scheduled.run()
    assert result.exit_code == 2
    assert result.details["controller_id"] == owner
    assert scheduled.catalog.state().controller_id == owner
    assert not scheduled.captures
    assert scheduled.capacities.events == ["close"]


def test_scheduled_blocks_pending_operation_without_capture_or_pause(scheduled):
    owner = scheduled.catalog.acquire_controller(scheduled.c.runtime.controller_id)
    scheduled.catalog.transition_mode(owner, RecoveryMode.STANDBY, RecoveryMode.SYNCING, guid())
    operation = OperationRecord(
        operation_id=guid(), kind="test", state=OperationState.RUNNING, recorded_at=now(),
        ownership_evidence="Previous controller committed this operation intent.",
    )
    scheduled.catalog.begin_operation(owner, operation.model_copy(update={"state": OperationState.INTENT}))
    # Simulate an inconsistent/abandoned standby row. It must not become permission to retry.
    scheduled.harness.raw("UPDATE bcdr.control SET mode = ?, controller_id = NULL", ("standby",))
    result = scheduled.run()
    assert result.details["pending_operation_ids"] == [operation.operation_id]
    assert result.exit_code == 2
    assert not scheduled.captures


def test_scheduled_rechecks_mode_under_epoch_not_only_preflight(scheduled):
    original = scheduled.c.synchronize

    def raced(request, **kwargs):
        scheduled.harness.raw("UPDATE bcdr.control SET mode = ?", (RecoveryMode.SYNCING.value,))
        return original(request, **kwargs)

    scheduled.c.synchronize = raced
    with pytest.raises(RecoveryBlocked, match="Operation not allowed"):
        scheduled.run()
    assert not scheduled.captures
    assert "resume_business" not in scheduled.capacities.events


def test_scheduled_refuses_local_only_before_service_creation(scheduled, monkeypatch):
    monkeypatch.delenv(LEASE_ENV)
    with pytest.raises(RecoveryBlocked, match="requires FAB_SHUFFLE"):
        scheduled.run()
    scheduled.factory.assert_not_called()


def test_scheduled_refuses_missing_acquired_distributed_guard(scheduled):
    scheduled.capacities.lock.remote = None
    with pytest.raises(RecoveryBlocked, match="did not acquire"):
        scheduled.run()
    assert not scheduled.captures
    assert scheduled.capacities.events[-1] == "close"


def test_web_service_excludes_scheduled_job_before_bootstrap_and_capacity(
    scheduled, monkeypatch, tmp_path,
):
    import httpx

    from fabshuffle.bcdr import deployment_lock, production
    from fabshuffle.bcdr.capacity import ArmCapacityClient
    from fabshuffle.bcdr.deployment_lock import BlobLease, BlobLeaseError
    from fabshuffle.bcdr.service import create_service

    capacities, store, state = production_capacities(scheduled, tmp_path, monkeypatch)
    state["value"] = "Suspended"
    transport = capacities.arm.http._transport
    server = BlobServer()
    scheduled.c.tokens.storage_token.return_value = "lease-test-token"
    monkeypatch.setattr(deployment_lock, "BlobLease", lambda url, token: BlobLease(
        url, token, transport=httpx.MockTransport(server.handle),
    ))
    monkeypatch.setattr(production, "ArmCapacityClient", lambda tokens, **kwargs: ArmCapacityClient(
        tokens, transport=transport, **kwargs,
    ))
    monkeypatch.setattr(production, "FabricClient", lambda tokens: scheduled.c.destination.client)
    web = create_service(store.path, target_tokens=scheduled.c.tokens)
    try:
        assert state["value"] == "Active"
        assert scheduled.catalog.state().controller_id is None
        before = store.path.read_bytes(), tuple(state["calls"])
        with pytest.raises(BlobLeaseError, match="LeaseAlreadyPresent"):
            scheduled_sync(store.path, scheduled.request, target_tokens=scheduled.c.tokens)
        assert (store.path.read_bytes(), tuple(state["calls"])) == before
        assert not scheduled.captures
    finally:
        web.close()
        capacities.close()
    assert not server.state["owner"]


@pytest.mark.parametrize("field", ["capture", "park"])
def test_scheduled_disallows_non_capture_or_unparked_requests(scheduled, field):
    scheduled.request = scheduled.request.model_copy(update={field: False})
    with pytest.raises(ValueError, match="capture=true and park=true"):
        scheduled.run()
    scheduled.factory.assert_not_called()


@pytest.mark.parametrize("content", [
    b'{"capture":true,"capture":false}', b'{"capture":NaN}', b"{", b"\xff",
    b" " * (MAX_REQUEST_BYTES + 1), b'{"capacity_routes":[],"capture":"true"}',
    b'{"capacity_routes":[],"enable":true}',
])
def test_scheduled_request_rejects_oversize_ambiguous_or_untyped_json(tmp_path, content):
    path = tmp_path / "request.json"
    path.write_bytes(content)
    with pytest.raises(ValueError):
        read_request(path)


def test_cli_scheduled_confirm_request_and_dispatch(scheduled, tmp_path, monkeypatch, capsys):
    path = tmp_path / "approved.json"
    path.write_text(scheduled.request.model_dump_json())
    monkeypatch.setattr(cli, "credential_provider", lambda environ: scheduled.c.tokens)
    monkeypatch.setattr(cli, "scheduled_sync", lambda *args, **kwargs: scheduled.run())
    assert cli.main(["scheduled-sync", "--bootstrap", str(scheduled.path),
                     "--request", str(path)]) == 1
    scheduled.factory.assert_not_called()
    assert cli.main(["scheduled-sync", "--bootstrap", str(scheduled.path),
                     "--confirm", "scheduled-sync"]) == 1
    scheduled.factory.assert_not_called()
    assert cli.main(["scheduled-sync", "--bootstrap", str(scheduled.path),
                     "--request", str(path), "--confirm", "scheduled-sync"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["details"]["scheduled_status"] == "completed"


def test_cli_missing_bootstrap_and_service_errors_are_not_success(tmp_path, monkeypatch, capsys):
    path = tmp_path / "approved.json"
    path.write_text('{"capacity_routes":[]}')
    monkeypatch.setenv(LEASE_ENV, URL)
    monkeypatch.setattr(cli, "credential_provider", lambda environ: Mock())
    monkeypatch.setattr(cli, "scheduled_sync", Mock(side_effect=RecoveryBlocked("Repair bootstrap")))
    assert cli.main(["scheduled-sync", "--bootstrap", str(tmp_path / "missing.json"),
                     "--request", str(path), "--confirm", "scheduled-sync"]) == 1
    output = capsys.readouterr()
    assert not output.out
    assert "Repair bootstrap" in output.err
