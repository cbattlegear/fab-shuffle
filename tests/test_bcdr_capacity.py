from __future__ import annotations

import shutil
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import httpx
import pytest

from fabshuffle.auth import ServicePrincipal
from fabshuffle.bcdr.bootstrap import (
    ARM_VERSION,
    BootstrapDescriptor,
    BootstrapError,
    BootstrapStore,
    CapacityAuthorization,
    CapacityOperation,
)
from fabshuffle.bcdr.capacity import (
    ARM_BASE,
    ARM_SCOPE,
    ArmCapacityClient,
    CapacityCoordinator,
    CapacityError,
    CapacityOutcomeUnknown,
    CatalogPauseGuard,
    PauseProof,
    validate_poll_url,
)
from fabshuffle.bcdr.contracts import CatalogState, ControllerLease, RecoveryMode

TENANT = "20000000-0000-0000-0000-000000000001"
APP = "20000000-0000-0000-0000-000000000002"
OWNER = "20000000-0000-0000-0000-000000000003"
SET = "20000000-0000-0000-0000-000000000004"
WORKSPACE = "20000000-0000-0000-0000-000000000005"
FABRIC_CAP = "20000000-0000-0000-0000-000000000007"
OP = "20000000-0000-0000-0000-000000000008"
ARM_ID = f"/subscriptions/{TENANT}/resourcegroups/recovery/providers/microsoft.fabric/capacities/catalog"
OTHER_ID = ARM_ID.replace("catalog", "business")
POLL = (
    f"{ARM_BASE}/subscriptions/{TENANT}/providers/Microsoft.Fabric/locations/westus/"
    f"operationresults/{OP}?api-version={ARM_VERSION}"
)


def capacity(resource_id=ARM_ID, fabric_id=FABRIC_CAP):
    return CapacityAuthorization(
        arm_resource_id=resource_id, fabric_capacity_id=fabric_id,
        dedicated_recovery=True, authorized_for_suspend=True,
    )


class Tokens:
    principal = ServicePrincipal(TENANT, APP, "not-a-real-secret")

    def __init__(self):
        self.scopes = []

    def tenant_id(self):
        return TENANT

    def token(self, scope):
        self.scopes.append(scope)
        return "arm-test-token"


@pytest.fixture
def store():
    directory = Path.cwd() / f".bcdr-capacity-test-{uuid4().hex}"
    directory.mkdir()
    result = BootstrapStore(directory / "bootstrap.json")
    result.save(BootstrapDescriptor(
        recovery_set_id=SET, tenant_id=TENANT, application_id=APP, controller_id=OWNER,
        capacities=(capacity(), capacity(OTHER_ID, WORKSPACE)),
        catalog_capacity_id=ARM_ID, control_workspace_id=WORKSPACE,
    ), expected_revision=None)
    try:
        yield result
    finally:
        shutil.rmtree(directory)


def resource(state="Active", resource_id=ARM_ID):
    return {
        "id": resource_id, "type": "Microsoft.Fabric/capacities",
        "sku": {"name": "F2", "tier": "Fabric"},
        "properties": {"state": state, "provisioningState": "Succeeded"},
    }


@pytest.mark.parametrize("url", [
    POLL.replace("https:", "http:"),
    POLL.replace("management.azure.com", "evil.example"),
    POLL.replace("management.azure.com", "management.azure.com.evil.example"),
    POLL.replace("management.azure.com", "management.azure.com@evil.example"),
    POLL.replace("management.azure.com", "management.azure.com:443"),
    POLL.replace(TENANT, APP),
    POLL.replace("Microsoft.Fabric", "Microsoft.Compute"),
    POLL.replace("/locations/westus/operationresults/", "/capacities/"),
    POLL.replace("/locations/westus/", "/locations/../westus/"),
    POLL.replace("/locations/westus/", "/locations/%2e%2e/westus/"),
    POLL + "&sig=secret",
    POLL + "&api-version=2023-11-01",
    POLL + "#fragment",
])
def test_unsafe_polling_scope_is_rejected_before_token_acquisition(url):
    tokens = Tokens()
    with ArmCapacityClient(tokens, transport=httpx.MockTransport(lambda _: pytest.fail("network"))) as arm:
        with pytest.raises((BootstrapError, ValueError)):
            arm._request("GET", url, capacity(), polling=True)
    assert tokens.scopes == []


def test_poll_url_allows_documented_context_and_pins_operation():
    assert validate_poll_url(POLL + "&t=123&c=opaque", ARM_ID)
    with pytest.raises(BootstrapError, match="different operation"):
        validate_poll_url(POLL.replace(OP, APP), ARM_ID, previous=POLL)
    with pytest.raises(BootstrapError, match="different operation"):
        validate_poll_url(POLL.replace("westus", "eastus"), ARM_ID, previous=POLL)


def test_arm_get_uses_arm_not_fabric_audience_and_disallows_redirects():
    tokens = Tokens()
    calls = []

    def handler(request):
        calls.append(request)
        assert request.headers["authorization"] == "Bearer arm-test-token"
        return httpx.Response(302, headers={"Location": "https://evil.example/token"})

    with ArmCapacityClient(tokens, transport=httpx.MockTransport(handler)) as arm:
        with pytest.raises(CapacityError, match="HTTP 302"):
            arm.get(capacity())
    assert len(calls) == 1 and tokens.scopes == [ARM_SCOPE]


def test_arm_resume_lro_persists_receipts_before_poll_and_waits_for_actual_state():
    phases, sleeps, calls = [], [], []
    reads = 0

    def handler(request):
        nonlocal reads
        calls.append((request.method, request.url.path))
        if request.method == "POST":
            assert phases[-1].phase == "intent"
            return httpx.Response(202, headers={
                "Azure-AsyncOperation": POLL, "Retry-After": "5", "x-ms-request-id": OP,
            })
        if "operationresults" in request.url.path:
            assert phases[-1].phase == "accepted" and phases[-1].poll_url == POLL
            return httpx.Response(200, json={"status": "Succeeded"})
        reads += 1
        return httpx.Response(200, json=resource("Suspended" if reads == 1 else "Active"))

    tokens = Tokens()
    with ArmCapacityClient(
        tokens, transport=httpx.MockTransport(handler), sleep=sleeps.append,
    ) as arm:
        assert arm.resume(capacity(), owner_id=OWNER, on_progress=phases.append).state == "Active"
    assert [op.phase for op in phases] == ["intent", "accepted", "succeeded"]
    assert phases[1].request_id == OP and 5 in sleeps
    assert set(tokens.scopes) == {ARM_SCOPE}
    assert [method for method, _ in calls].count("POST") == 1


def test_arm_location_polling_honors_202_then_204():
    calls, sleeps, observations = [], [], []
    reads = 0
    polls = 0

    def handler(request):
        nonlocal reads, polls
        calls.append(request.method)
        if request.method == "POST":
            return httpx.Response(202, headers={"Location": POLL, "Retry-After": "7"})
        if "operationresults" in request.url.path:
            polls += 1
            return httpx.Response(202 if polls == 1 else 204, headers={"Retry-After": "9"})
        reads += 1
        return httpx.Response(200, json=resource("Active" if reads == 1 else "Suspended"))

    with ArmCapacityClient(
        Tokens(), transport=httpx.MockTransport(handler), sleep=sleeps.append,
    ) as arm:
        arm.suspend(capacity(), owner_id=OWNER, on_progress=observations.append, authorize=lambda: None)
    assert polls == 2 and 7 in sleeps and 9 in sleeps
    assert observations[-1].phase == "succeeded"


def test_arm_lro_failure_exposes_service_code_message_request_id():
    observations = []

    def handler(request):
        if request.method == "POST":
            return httpx.Response(202, headers={"Azure-AsyncOperation": POLL, "Retry-After": "0"})
        if "operationresults" in request.url.path:
            return httpx.Response(200, json={
                "status": "Failed", "error": {"code": "PolicyDenied", "message": "Contact capacity owner"},
            }, headers={"x-ms-request-id": OP})
        return httpx.Response(200, json=resource("Suspended"))

    with ArmCapacityClient(
        Tokens(), transport=httpx.MockTransport(handler), sleep=lambda _: None,
    ) as arm:
        with pytest.raises(CapacityError) as failure:
            arm.resume(capacity(), owner_id=OWNER, on_progress=observations.append)
    assert failure.value.error_code == "PolicyDenied"
    assert failure.value.detail == "Contact capacity owner"
    assert failure.value.request_id == OP
    assert observations[-1].phase == "failed"


def test_returned_arm_poll_url_is_revalidated_without_sending_token():
    calls, observations = [], []

    def handler(request):
        calls.append(request.url.host)
        if request.method == "POST":
            return httpx.Response(202, headers={"Azure-AsyncOperation": POLL, "Retry-After": "0"})
        if "operationresults" in request.url.path:
            return httpx.Response(202, headers={
                "Location": POLL.replace("management.azure.com", "evil.test"),
            })
        return httpx.Response(200, json=resource("Suspended"))

    with ArmCapacityClient(
        Tokens(), transport=httpx.MockTransport(handler), sleep=lambda _: None,
    ) as arm:
        with pytest.raises(BootstrapError):
            arm.resume(capacity(), owner_id=OWNER, on_progress=observations.append)
    assert set(calls) == {"management.azure.com"}
    assert observations[-1].phase == "accepted"


def test_mutation_transport_loss_keeps_intent_and_is_not_replayed():
    calls, observations = [], []

    def handler(request):
        calls.append(request.method)
        if request.method == "POST":
            raise httpx.ReadTimeout("lost", request=request)
        return httpx.Response(200, json=resource("Suspended"))

    with ArmCapacityClient(Tokens(), transport=httpx.MockTransport(handler)) as arm:
        with pytest.raises(CapacityOutcomeUnknown):
            arm.resume(capacity(), owner_id=OWNER, on_progress=observations.append)
        with pytest.raises(CapacityOutcomeUnknown, match="no receipt"):
            arm.resume(capacity(), owner_id=OWNER, on_progress=observations.append, pending=observations[0])
    assert calls.count("POST") == 1
    assert observations[0].phase == "intent"


def test_arm_429_retries_honor_delay_and_500_post_does_not_replay():
    calls, sleeps, observations = [], [], []

    def handler(request):
        calls.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, json=resource("Suspended"))
        if calls.count("POST") == 1:
            return httpx.Response(429, headers={"Retry-After": "11"})
        return httpx.Response(500, json={"error": {"code": "Internal", "message": "Service response"}})

    with ArmCapacityClient(
        Tokens(), transport=httpx.MockTransport(handler), sleep=sleeps.append,
    ) as arm:
        with pytest.raises(CapacityError, match="Internal: Service response"):
            arm.resume(capacity(), owner_id=OWNER, on_progress=observations.append)
    assert calls.count("POST") == 2 and sleeps == [11]
    assert observations[-1].phase == "intent"


class Guard:
    def __init__(self, *, mode="parking", drained=True):
        self.mode = mode
        self.drained = drained
        self.checks = 0
        self.entered = False

    def enter_parking(self, owner_id, capacity_ids):
        self.entered = True
        return PauseProof(SET, owner_id, 1, self.mode, self.drained, capacity_ids)

    def assert_parking(self, proof):
        self.checks += 1
        return replace(proof, mode=self.mode, drained=self.drained)


def test_parking_catalog_last_commits_intent_before_post_and_no_sql_after_final_pause(store):
    guard = Guard()
    posts = []
    states = {ARM_ID: "Active", OTHER_ID: "Active"}

    def handler(request):
        resource_id = request.url.path
        if request.method == "POST":
            resource_id = resource_id.removesuffix("/suspend")
            assert guard.entered
            assert store.load().parking.capacity_ids == (OTHER_ID, ARM_ID)
            assert store.load().capacity_operations[-1].phase == "intent"
            assert states[ARM_ID] == "Active"
            posts.append(resource_id)
            states[resource_id] = "Suspended"
            return httpx.Response(200)
        return httpx.Response(200, json=resource(states[resource_id], resource_id))

    with ArmCapacityClient(Tokens(), transport=httpx.MockTransport(handler)) as arm:
        result = CapacityCoordinator(store, arm).park(owner_id=OWNER, guard=guard)
    assert posts == [OTHER_ID, ARM_ID]
    assert result.parking is not None
    assert all(op.phase == "succeeded" for op in result.capacity_operations)


@pytest.mark.parametrize("mode,drained", [
    ("standby", True), ("syncing", True), ("enabling_recovery", True),
    ("active_recovery", True), ("unknown", True), ("parking", False),
])
def test_parking_refuses_serving_unknown_or_undrained_work(store, mode, drained):
    with ArmCapacityClient(
        Tokens(), transport=httpx.MockTransport(lambda _: pytest.fail("ARM must not be called")),
    ) as arm:
        with pytest.raises(BootstrapError, match="proof"):
            CapacityCoordinator(store, arm).park(owner_id=OWNER, guard=Guard(mode=mode, drained=drained))
    assert store.load().parking is None


def test_enable_race_before_suspend_prevents_post(store):
    guard = Guard()

    def handler(request):
        assert request.method == "GET"
        guard.mode = "enabling_recovery"
        return httpx.Response(200, json=resource("Active", request.url.path))

    with ArmCapacityClient(Tokens(), transport=httpx.MockTransport(handler)) as arm:
        with pytest.raises(BootstrapError, match="proof"):
            CapacityCoordinator(store, arm).park(owner_id=OWNER, guard=guard)
    assert store.load().capacity_operations[-1].phase == "intent"


def test_failed_first_suspend_never_pauses_catalog(store):
    posts = []

    def handler(request):
        if request.method == "GET":
            return httpx.Response(200, json=resource("Active", request.url.path))
        posts.append(request.url.path)
        return httpx.Response(403, json={"error": {"code": "Forbidden", "message": "Missing ARM role"}})

    with ArmCapacityClient(Tokens(), transport=httpx.MockTransport(handler)) as arm:
        with pytest.raises(CapacityError, match="Missing ARM role"):
            CapacityCoordinator(store, arm).park(owner_id=OWNER, guard=Guard())
    assert posts == [OTHER_ID + "/suspend"]


def test_resume_before_sql_and_reconcile_with_receipts_then_clear(store):
    events = []
    state = "Suspended"

    def handler(request):
        nonlocal state
        if request.method == "POST":
            events.append("resume")
            assert store.load().capacity_operations[-1].phase == "intent"
            state = "Active"
            return httpx.Response(200)
        events.append("arm-get")
        return httpx.Response(200, json=resource(state))

    def wait_sql(current):
        assert state == "Active"
        assert current.capacity_operations[-1].phase == "succeeded"
        events.append("sql-ready")

    def reconcile(current):
        assert current.capacity_operations
        events.append("catalog-commit")

    with ArmCapacityClient(Tokens(), transport=httpx.MockTransport(handler)) as arm:
        result = CapacityCoordinator(store, arm).resume_catalog(wait_sql=wait_sql, reconcile=reconcile)
    assert events.index("resume") < events.index("sql-ready") < events.index("catalog-commit")
    assert result.capacity_operations == ()


def test_sql_unavailable_keeps_observation_and_never_suspends_in_finally(store):
    calls = []

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(200, json=resource())

    def unavailable(_):
        raise RuntimeError("SQL endpoint still cold")

    with ArmCapacityClient(Tokens(), transport=httpx.MockTransport(handler)) as arm:
        with pytest.raises(RuntimeError, match="cold"):
            CapacityCoordinator(store, arm).resume_catalog(
                wait_sql=unavailable, reconcile=lambda _: pytest.fail("SQL not ready"),
            )
    assert all(not path.endswith("/suspend") for path in calls)


def test_pending_suspend_is_observed_before_catalog_resume(store):
    pending = CapacityOperation(
        intent_id=OP, owner_id=OWNER, arm_resource_id=ARM_ID, action="suspend",
        phase="accepted", poll_url=POLL, poll_kind="async",
    )
    store.update(lambda current: current.model_copy(update={"capacity_operations": (pending,)}))
    calls = []
    state = "Suspended"

    def handler(request):
        nonlocal state
        calls.append((request.method, request.url.path))
        if "operationresults" in request.url.path:
            return httpx.Response(200, json={"status": "Succeeded"})
        if request.method == "POST":
            assert request.url.path.endswith("/resume")
            state = "Active"
            return httpx.Response(200)
        return httpx.Response(200, json=resource(state))

    reconciled = []
    with ArmCapacityClient(
        Tokens(), transport=httpx.MockTransport(handler), sleep=lambda _: None,
    ) as arm:
        CapacityCoordinator(store, arm).resume_catalog(
            wait_sql=lambda _: None,
            reconcile=lambda current: reconciled.extend(current.capacity_operations),
        )
    assert [op.action for op in reconciled] == ["suspend", "resume"]
    assert all(op.phase == "succeeded" for op in reconciled)
    assert not any(path.endswith("/suspend") for _, path in calls)


def catalog_guard(*, mode=RecoveryMode.STANDBY, pending=()):
    lease = ControllerLease(recovery_set_id=SET, controller_id=OWNER, epoch=2)
    catalog = Mock()
    catalog.state.return_value = CatalogState(
        recovery_set_id=SET, controller_id=OWNER, epoch=2, revision=7, mode=mode,
    )
    catalog.pending_operations.return_value = pending

    def transition(actual_lease, expected, desired, operation_id):
        assert actual_lease == lease
        assert expected == RecoveryMode.STANDBY and desired == RecoveryMode.PARKING
        assert operation_id
        catalog.state.return_value = catalog.state.return_value.model_copy(update={
            "mode": desired, "revision": 8,
        })
        return catalog.state.return_value

    catalog.transition_mode.side_effect = transition
    authorizations = Mock(return_value=(capacity(), capacity(OTHER_ID, WORKSPACE)))
    return catalog, authorizations, CatalogPauseGuard(
        catalog, lease, read_authorized_capacities=authorizations,
    )


def test_catalog_guard_transitions_standby_and_retains_held_lease():
    catalog, authorizations, guard = catalog_guard()
    proof = guard.enter_parking(OWNER, (OTHER_ID, ARM_ID))
    assert proof.mode == RecoveryMode.PARKING and proof.epoch == 2
    assert guard.assert_parking(proof) == proof
    assert authorizations.call_count == 3
    catalog.acquire_controller.assert_not_called()
    catalog.release_controller.assert_not_called()


@pytest.mark.parametrize("mode", [
    RecoveryMode.SYNCING, RecoveryMode.PARKING, RecoveryMode.ENABLING_RECOVERY,
    RecoveryMode.ACTIVE_RECOVERY, RecoveryMode.FAILING_BACK, RecoveryMode.REARMING,
])
def test_catalog_guard_requires_completed_return_to_standby(mode):
    catalog, _, guard = catalog_guard(mode=mode)
    with pytest.raises(BootstrapError, match="mode changed"):
        guard.enter_parking(OWNER, (OTHER_ID, ARM_ID))
    catalog.transition_mode.assert_not_called()


def test_catalog_guard_rechecks_scope_pending_work_and_epoch():
    catalog, authorizations, guard = catalog_guard()
    proof = guard.enter_parking(OWNER, (OTHER_ID, ARM_ID))
    catalog.pending_operations.return_value = (object(),)
    with pytest.raises(BootstrapError, match="undrained"):
        guard.assert_parking(proof)
    catalog.pending_operations.return_value = ()
    authorizations.return_value = (capacity(),)
    with pytest.raises(BootstrapError, match="scope"):
        guard.assert_parking(proof)
    authorizations.return_value = (capacity(), capacity(OTHER_ID, WORKSPACE))
    catalog.state.return_value = catalog.state.return_value.model_copy(update={"epoch": 3})
    with pytest.raises(BootstrapError, match="ownership"):
        guard.assert_parking(proof)


def test_catalog_guard_rejects_nonatomic_state_reads():
    catalog, _, guard = catalog_guard()
    original = catalog.state.return_value
    catalog.state.side_effect = [original, original.model_copy(update={"revision": 8})]
    with pytest.raises(BootstrapError, match="mode changed"):
        guard.enter_parking(OWNER, (OTHER_ID, ARM_ID))
    catalog.transition_mode.assert_not_called()
