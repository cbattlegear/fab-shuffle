"""Deterministic lease service, never Azure credentials or cloud resources."""

import multiprocessing
import threading
from unittest.mock import Mock

import httpx
import pytest

from fabshuffle.bcdr import deployment_lock, production
from fabshuffle.bcdr.backend import DurableRuntime, RecoveryBlocked
from fabshuffle.bcdr.bootstrap import BootstrapStore
from fabshuffle.bcdr.contracts import RecoveryMode
from fabshuffle.bcdr.deployment_lock import (
    LEASE_ENV,
    BlobLease,
    BlobLeaseError,
    DeploymentLock,
    GuardedTokens,
)
from fabshuffle.bcdr.warehouse_catalog import WarehouseCatalog
from tests.test_bcdr_bootstrap import descriptor
from tests.test_bcdr_contracts import recovery_set
from tests.test_bcdr_warehouse_catalog import SqlHarness

URL = "https://recoverylocks.blob.core.windows.net/fab-shuffle-locks/deployment.lock"


class BlobServer:
    def __init__(self, state=None, mutex=None):
        self.state = state if state is not None else {"exists": False, "owner": ""}
        self.mutex = mutex if mutex is not None else threading.Lock()
        self.calls = []
        self.failure = None

    def handle(self, request):
        action = request.headers.get("x-ms-lease-action", "create")
        self.calls.append(action)
        assert request.content == b""
        assert request.headers["Authorization"] == "Bearer lease-test-token"
        assert request.headers["x-ms-version"]
        assert request.headers["x-ms-date"]
        with self.mutex:
            if self.failure:
                result = self.failure(request)
                if result is not None:
                    return result
            if action == "create":
                assert request.headers["If-None-Match"] == "*"
                assert request.headers["x-ms-blob-type"] == "BlockBlob"
                if self.state["exists"]:
                    return self.error(412, "ConditionNotMet", "The specified condition was not met.")
                self.state["exists"] = True
                return httpx.Response(201)
            if action == "acquire":
                assert request.headers["x-ms-lease-duration"] == "60"
                if self.state["owner"]:
                    return self.error(409, "LeaseAlreadyPresent", "There is already a lease present.")
                owner = request.headers["x-ms-proposed-lease-id"]
                self.state["owner"] = owner
                return httpx.Response(201, headers={"x-ms-lease-id": owner})
            assert action in {"renew", "release"}
            owner = request.headers["x-ms-lease-id"]
            if self.state["owner"] != owner:
                return self.error(412, "LeaseIdMismatchWithLeaseOperation", "Lease IDs did not match.")
            if action == "release":
                self.state["owner"] = ""
            return httpx.Response(200, headers={"x-ms-lease-id": owner})

    @staticmethod
    def error(status, code, message):
        return httpx.Response(status, headers={"x-ms-request-id": "test-request"}, content=(
            f"<Error><Code>{code}</Code><Message>{message}</Message></Error>".encode()
        ))


def tokens():
    value = Mock()
    value.storage_token.return_value = "lease-test-token"
    return value


def lease(server, **kwargs):
    return BlobLease(URL, tokens(), transport=httpx.MockTransport(server.handle), **kwargs)


@pytest.mark.parametrize("url", [
    URL + "?sig=secret", URL + "?", URL + "#x", URL + "#", URL.replace("https:", "http:"),
    URL.replace("recoverylocks.", "user@recoverylocks."),
    URL.replace(".net/", ".net:443/"), URL.replace(".blob.core.windows.net", ".evil.example"),
    URL.replace(".net/", ".net.evil.example/"), URL.replace("deployment.lock", "../wrong"),
    URL.replace("deployment.lock", "%2Fother"), URL.replace("deployment.lock", "a\\b"),
    URL.replace("deployment.lock", "//other"), "https://recoverylocks.blob.core.windows.net/container",
    URL + "\n", URL.replace("fab-shuffle-locks", "bad--container"),
])
def test_url_validation_precedes_tokens_or_network(url):
    token = tokens()
    transport = Mock()
    with pytest.raises(RecoveryBlocked, match="fixed public Azure Blob"):
        BlobLease(url, token, transport=transport)
    token.storage_token.assert_not_called()
    transport.handle_request.assert_not_called()


def test_create_acquire_renew_release_and_existing_blob():
    server = BlobServer()
    first = lease(server)
    first.assert_held()
    first.renew()
    with pytest.raises(BlobLeaseError, match=r"409; LeaseAlreadyPresent.*already a lease"):
        lease(server)
    assert server.state["owner"] == first.lease_id
    first.close()
    first.close()
    assert not first._thread.is_alive()
    second = lease(server)
    assert second.lease_id != first.lease_id
    second.close()
    assert server.calls == ["create", "acquire", "renew", "create", "acquire", "release",
                            "create", "acquire", "release"]


def test_acquire_response_loss_never_retries_or_speculatively_releases():
    server = BlobServer()

    def lose(request):
        if request.headers.get("x-ms-lease-action") == "acquire":
            server.state["owner"] = request.headers["x-ms-proposed-lease-id"]
            raise httpx.ReadTimeout("sensitive underlying request details", request=request)

    server.failure = lose
    with pytest.raises(RecoveryBlocked, match="acquire response was lost") as error:
        lease(server)
    assert "sensitive" not in str(error.value)
    assert server.calls == ["create", "acquire"]
    assert server.state["owner"]


@pytest.mark.parametrize("status", [302, 307, 500, 403])
def test_redirects_and_errors_never_fall_back_or_forward_tokens(status):
    server = BlobServer()
    server.failure = lambda request: httpx.Response(
        status, headers={"Location": "https://evil.example/collect", "x-ms-error-code": "Denied"},
        text="Service denied the request.",
    )
    with pytest.raises(BlobLeaseError, match=f"HTTP {status}; Denied"):
        lease(server)
    assert server.calls == ["create"]


def test_401_refresh_is_bounded_and_preserves_final_service_error():
    server = BlobServer()
    count = 0

    def reject(request):
        nonlocal count
        count += 1
        return server.error(401, "AuthenticationFailed", "Token was rejected.")

    server.failure = reject
    token = tokens()
    with pytest.raises(BlobLeaseError, match="AuthenticationFailed: Token was rejected"):
        BlobLease(URL, token, transport=httpx.MockTransport(server.handle))
    assert count == 2
    assert token.storage_token.call_count == 2
    token.invalidate.assert_called_once_with()


def test_401_rejection_refreshes_once_then_can_acquire():
    server = BlobServer()
    rejected = [False]

    def reject_once(request):
        if not rejected[0]:
            rejected[0] = True
            return server.error(401, "AuthenticationFailed", "Token was rejected.")

    server.failure = reject_once
    token = tokens()
    current = BlobLease(URL, token, transport=httpx.MockTransport(server.handle))
    current.assert_held()
    current.close()
    token.invalidate.assert_called_once_with()
    assert server.calls == ["create", "create", "acquire", "release"]


def test_slow_acquire_success_is_not_accepted_after_local_deadline():
    server = BlobServer()
    clock = [0.0]

    def delay(request):
        if request.headers.get("x-ms-lease-action") == "acquire":
            clock[0] = 60.0

    server.failure = delay
    with pytest.raises(RecoveryBlocked, match="deadline expired"):
        lease(server, clock=lambda: clock[0])
    assert server.calls == ["create", "acquire"]


def test_lease_deadline_is_send_time_not_response_time_and_never_revived():
    server = BlobServer()
    clock = [100.0]
    first = lease(server, clock=lambda: clock[0])
    clock[0] = 155.0
    with pytest.raises(RecoveryBlocked, match="deadline expired"):
        first.assert_held()
    with pytest.raises(RecoveryBlocked, match="deadline expired"):
        first.renew()
    with pytest.raises(RecoveryBlocked, match="deadline expired"):
        first.close()
    assert "renew" not in server.calls
    assert not first._thread.is_alive()


def test_late_successful_renewal_does_not_revive_expired_owner():
    server = BlobServer()
    clock = [0.0]
    first = lease(server, clock=lambda: clock[0])

    def delay(request):
        if request.headers.get("x-ms-lease-action") == "renew":
            clock[0] = 56.0

    server.failure = delay
    with pytest.raises(RecoveryBlocked, match="deadline expired"):
        first.renew()
    with pytest.raises(RecoveryBlocked, match="deadline expired"):
        first.close()


def test_heartbeat_failure_is_foreground_visible_and_cleanup_stops_thread(monkeypatch):
    server = BlobServer()
    failed = threading.Event()

    def fail(request):
        if request.headers.get("x-ms-lease-action") == "renew":
            failed.set()
            return server.error(412, "LeaseLost", "Operator broke lease.")

    server.failure = fail
    monkeypatch.setattr(BlobLease, "RENEW_INTERVAL", 0.01)
    first = lease(server)
    assert failed.wait(timeout=2)
    first._thread.join(timeout=2)
    with pytest.raises(RecoveryBlocked, match="LeaseLost: Operator broke lease"):
        first.assert_held()
    with pytest.raises(RecoveryBlocked, match="LeaseLost"):
        first.close()
    assert not first._thread.is_alive()


def test_release_failure_is_not_success_or_retried():
    server = BlobServer()
    first = lease(server)
    server.failure = lambda request: server.error(503, "ServerBusy", "Try later.")
    with pytest.raises(BlobLeaseError, match=r"release.*ServerBusy: Try later"):
        first.close()
    assert server.calls.count("release") == 1
    assert not first._thread.is_alive()


def test_expiry_while_fetching_release_token_is_not_success():
    server = BlobServer()
    clock = [0.0]
    token = tokens()
    current = BlobLease(URL, token, transport=httpx.MockTransport(server.handle), clock=lambda: clock[0])

    def slow_token():
        clock[0] = 56.0
        return "lease-test-token"

    token.storage_token.side_effect = slow_token
    with pytest.raises(RecoveryBlocked, match="deadline expired"):
        current.close()
    assert "release" not in server.calls
    assert not current._thread.is_alive()


def test_old_owner_release_cannot_release_replacement_lease():
    server = BlobServer()
    first = lease(server)
    server.state["owner"] = "new-controller-lease"
    with pytest.raises(BlobLeaseError, match="LeaseIdMismatchWithLeaseOperation"):
        first.close()
    assert server.state["owner"] == "new-controller-lease"


def test_renewal_authentication_failure_is_sticky_and_visible():
    server = BlobServer()
    token = tokens()
    current = BlobLease(URL, token, transport=httpx.MockTransport(server.handle))
    token.storage_token.side_effect = RecoveryBlocked("Refresh authentication for the deployment identity")
    with pytest.raises(RecoveryBlocked, match="Refresh authentication"):
        current.renew()
    token.storage_token.side_effect = None
    with pytest.raises(RecoveryBlocked, match="Refresh authentication"):
        current.assert_held()
    with pytest.raises(RecoveryBlocked, match="Refresh authentication"):
        current.close()


def test_guarded_tokens_check_again_after_slow_token_response():
    guard = Mock()
    delegate = tokens()
    wrapped = GuardedTokens(delegate, guard)
    assert wrapped.storage_token() == "lease-test-token"
    assert guard.call_count == 2
    guard.side_effect = [None, RecoveryBlocked("lease lost")]
    with pytest.raises(RecoveryBlocked, match="lease lost"):
        wrapped.storage_token()


def test_production_refuses_before_bootstrap_or_arm_on_held_remote_lease(tmp_path, monkeypatch):
    server = BlobServer()
    first = lease(server)
    original = BlobLease
    monkeypatch.setenv(LEASE_ENV, URL)
    monkeypatch.setattr(deployment_lock, "BlobLease", lambda url, token: original(
        url, token, transport=httpx.MockTransport(server.handle),
    ))
    store = Mock(side_effect=AssertionError("bootstrap touched before guard"))
    capacity = Mock(side_effect=AssertionError("capacity touched before guard"))
    monkeypatch.setattr(production, "BootstrapStore", store)
    monkeypatch.setattr(production, "ProductionCapacities", capacity)
    with pytest.raises(BlobLeaseError, match="LeaseAlreadyPresent"):
        production.open_coordinator(
            tmp_path / "missing" / "bootstrap.json",
            target_tokens=tokens(), source_tokens=None, controller_id=None,
        )
    store.assert_not_called()
    capacity.assert_not_called()
    assert not (tmp_path / "missing").exists()
    first.close()


def test_no_remote_environment_preserves_local_only_exclusion(tmp_path, monkeypatch):
    monkeypatch.delenv(LEASE_ENV, raising=False)
    first = DeploymentLock(tmp_path / "bootstrap.json")
    first.assert_held()
    assert first.remote is None
    with pytest.raises(RecoveryBlocked, match="Another worker"):
        DeploymentLock(tmp_path / "bootstrap.json")
    first.close()
    with pytest.raises(RecoveryBlocked, match="closed"):
        first.assert_held()


def test_bootstrap_reads_and_writes_fail_before_file_effect(tmp_path):
    guard = Mock(side_effect=RecoveryBlocked("lease lost"))
    store = BootstrapStore(tmp_path / "missing.json", guard=guard, distributed=True)
    with pytest.raises(RecoveryBlocked, match="lease lost"):
        store.load()
    with pytest.raises(RecoveryBlocked, match="lease lost"):
        store.update(lambda descriptor: descriptor)
    with pytest.raises(RecoveryBlocked, match="lease lost"):
        store.save(None, expected_revision=None)
    assert list(tmp_path.iterdir()) == []


def test_distributed_bootstrap_initializes_a_fresh_share_directory(tmp_path):
    parent = tmp_path / "new-share" / "bcdr"
    guard = Mock()
    store = BootstrapStore(parent / "bootstrap.json", guard=guard, distributed=True)
    saved = store.save(descriptor(), expected_revision=None)
    assert saved.revision == 0
    assert store.load() == saved
    assert guard.call_count > 1
    assert not store.path.with_suffix(".json.lock").exists()


def test_distributed_bootstrap_does_not_create_parent_after_lease_loss(tmp_path):
    parent = tmp_path / "new-share" / "bcdr"
    guard = Mock(side_effect=[None, RecoveryBlocked("lease lost")])
    store = BootstrapStore(parent / "bootstrap.json", guard=guard, distributed=True)
    with pytest.raises(RecoveryBlocked, match="lease lost"):
        store.save(descriptor(), expected_revision=None)
    assert not parent.exists()


def test_arm_resume_loss_after_intent_never_sends_mutation():
    from fabshuffle.bcdr.capacity import ArmCapacityClient
    from tests.test_bcdr_capacity import Tokens, capacity, resource

    calls = []
    lost = [False]

    def guard():
        if lost[0]:
            raise RecoveryBlocked("deployment lease lost")

    def handler(request):
        calls.append(request.method)
        return httpx.Response(200, json=resource("Suspended"))

    def progress(operation):
        lost[0] = True

    with ArmCapacityClient(Tokens(), transport=httpx.MockTransport(handler), guard=guard) as arm:
        with pytest.raises(RecoveryBlocked, match="deployment lease lost"):
            arm.resume(capacity(), owner_id=recovery_set().recovery_set_id, on_progress=progress)
    assert calls == ["GET"]


def test_arm_throttle_retry_checks_guard_again():
    from fabshuffle.bcdr.capacity import ArmCapacityClient
    from tests.test_bcdr_capacity import Tokens, capacity

    calls = []
    lost = [False]

    def guard():
        if lost[0]:
            raise RecoveryBlocked("deployment lease lost")

    def handler(request):
        calls.append(request.method)
        return httpx.Response(429, headers={"Retry-After": "0"})

    def sleep(seconds):
        lost[0] = True

    with ArmCapacityClient(
        Tokens(), transport=httpx.MockTransport(handler), guard=guard, sleep=sleep,
    ) as arm:
        with pytest.raises(RecoveryBlocked, match="deployment lease lost"):
            arm.get(capacity())
    assert calls == ["GET"]


def test_warehouse_loss_rolls_back_and_blocks_next_statement(tmp_path):
    harness = SqlHarness(tmp_path / "catalog.db")
    catalog = WarehouseCatalog(harness.connect, recovery_set())
    catalog.initialize()
    starting = catalog.state()
    lost = [False]

    def guard():
        if lost[0]:
            raise RecoveryBlocked("lease lost")

    catalog.guard = guard
    with pytest.raises(RecoveryBlocked, match="lease lost"):
        with catalog._connection() as connection:
            cursor = connection.cursor()
            cursor.execute("UPDATE bcdr.control SET revision = revision + 1 WHERE singleton = 1")
            lost[0] = True
            cursor.execute("UPDATE bcdr.control SET revision = revision + 1 WHERE singleton = 1")
    assert len([row for row in harness.sql if "revision = revision + 1" in row[0]]) == 1
    catalog.guard = None
    assert catalog.state() == starting


def test_runtime_loss_retains_intent_and_warehouse_owner_for_explicit_reconciliation(tmp_path):
    harness = SqlHarness(tmp_path / "catalog.db")
    catalog = WarehouseCatalog(harness.connect, recovery_set())
    catalog.initialize()
    runtime = DurableRuntime(catalog)
    lost = [False]

    def guard():
        if lost[0]:
            raise RecoveryBlocked("lease lost; reconcile")

    runtime.deployment_guard = guard

    def effect():
        lost[0] = True
        return {"external_result": "could have completed"}

    with pytest.raises(RecoveryBlocked, match="lease lost"):
        with runtime.controller({RecoveryMode.STANDBY}):
            runtime.transition(RecoveryMode.SYNCING)
            runtime.effect("test-effect", "one", effect)
    assert catalog.pending_operations()
    assert catalog.state().controller_id == runtime.controller_id
    assert catalog.state().mode == RecoveryMode.SYNCING
    with pytest.raises(RecoveryBlocked, match="lease lost"):
        runtime.effect("test-effect", "two", Mock())


def _process_contender(state, mutex, pipe, release):
    server = BlobServer(state, mutex)
    try:
        current = lease(server)
    except RecoveryBlocked as error:
        pipe.send(("blocked", str(error)))
        return
    # Models bootstrap/capacity work *before* Warehouse acquisition.
    with mutex:
        state["effects"] = state["effects"] + 1
    pipe.send(("acquired", current.lease_id))
    release.wait(timeout=10)
    current.close()


def test_web_and_job_processes_share_remote_exclusion_before_warehouse():
    context = multiprocessing.get_context("spawn")
    with context.Manager() as manager:
        state = manager.dict(exists=False, owner="", effects=0)
        mutex = manager.Lock()
        release = context.Event()
        parent, child = context.Pipe()
        web = context.Process(target=_process_contender, args=(state, mutex, child, release))
        job = context.Process(target=_process_contender, args=(state, mutex, child, release))
        try:
            web.start()
            assert parent.poll(5)
            assert parent.recv()[0] == "acquired"
            job.start()
            assert parent.poll(5)
            assert parent.recv()[0] == "blocked"
            assert state["effects"] == 1
        finally:
            release.set()
            web.join(5)
            job.join(5)
            if web.is_alive():
                web.terminate()
                web.join()
            if job.is_alive():
                job.terminate()
                job.join()
        assert web.exitcode == job.exitcode == 0
        assert not state["owner"]
