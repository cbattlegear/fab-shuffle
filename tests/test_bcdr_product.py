"""Product transports call the real public service boundary, never recovery policy copies."""

from __future__ import annotations

import asyncio
import json
import threading
from io import StringIO
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

from fabshuffle.auth import ServicePrincipal, TokenProvider
from fabshuffle.bcdr import __main__ as cli
from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.bootstrap import (
    BootstrapDescriptor,
    BootstrapStore,
    CapacityAuthorization,
    WarehouseIntent,
)
from fabshuffle.bcdr.catalog import CatalogConflict, CatalogError
from fabshuffle.bcdr.contracts import RecoveryMode
from fabshuffle.bcdr.service import (
    ConfigureReplicaRequest,
    ReconcileOperationRequest,
    ServiceResult,
    SetupRequest,
    SyncRequest,
)
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.web import app as web
from fabshuffle.web import bcdr

TENANT = "00000000-0000-0000-0000-000000000001"
CLIENT = "00000000-0000-0000-0000-000000000002"
SOURCE_CAPACITY = "00000000-0000-0000-0000-000000000003"
TARGET_CAPACITY = "00000000-0000-0000-0000-000000000004"
GENERATION = "00000000-0000-0000-0000-000000000005"
SPN = "00000000-0000-0000-0000-000000000006"
CONTROL = "00000000-0000-0000-0000-000000000007"
ARM = (
    "/subscriptions/00000000-0000-0000-0000-000000000008"
    "/resourceGroups/dr/providers/Microsoft.Fabric/capacities/recovery"
)
SETUP = {
    "control_workspace_id": CONTROL,
    "source_capacity_ids": [SOURCE_CAPACITY],
    "recovery_capacities": [{
        "arm_resource_id": ARM, "fabric_capacity_id": TARGET_CAPACITY,
        "dedicated_recovery": True, "authorized_for_suspend": True,
    }],
    "catalog_capacity_id": ARM,
    "access_policy": {
        "recovery_spn": {"tenant_id": TENANT, "object_id": SPN, "kind": "ServicePrincipal"},
        "owners": [],
    },
}
SYNC = {"capacity_routes": [{
    "source_capacity_id": SOURCE_CAPACITY, "target_capacity_id": TARGET_CAPACITY,
}]}
FENCE = {
    "expected_epoch": 0, "fenced_side": "primary", "writers_stopped": True,
    "evidence": "Operator evidence reference", "confirmed_by": "Recovery operator",
    "observed_at": "2026-09-17T10:00:00Z", "valid_until": "2026-09-17T11:00:00Z",
}
RECONCILE = {
    "operation_id": GENERATION, "expected_controller_id": CONTROL, "expected_epoch": 4,
    "previous_controller_stopped": True, "fencing_evidence": "Stopped and fenced controller worker.",
    "target_quiescence_evidence": "Destination jobs and writers stopped.",
}
REPLICA = {
    "generation_id": GENERATION,
    "source": {"tenant_id": TENANT, "workspace_id": CONTROL, "item_id": SPN},
    "attachments": [{
        "binding": {
            "generation_id": GENERATION,
            "source": {"tenant_id": TENANT, "workspace_id": CONTROL, "item_id": SPN},
            "source_path": "Tables/dbo/orders",
            "consumer": {"tenant_id": TENANT, "workspace_id": SOURCE_CAPACITY, "item_id": TARGET_CAPACITY},
            "strategy": "temporary_continuity", "qualification": "verified",
            "read_only_verified": True, "retention_acknowledged": True,
            "evidence": "Independent caller access qualification",
        },
        "shortcut_path": "Tables/dbo", "shortcut_name": "orders",
        "access_evidence": {
            "qualification_id": GENERATION, "binding_sha256": "a" * 64,
            "principal": {"tenant_id": TENANT, "object_id": SPN, "kind": "ServicePrincipal"},
            "verified_at": "2026-09-17T10:00:00Z", "valid_until": "2026-09-17T11:00:00Z",
            "enforcement_reference": "Independent caller access qualification", "access_mode": "caller",
        },
    }],
    "qualified_at": "2026-09-17T10:00:00Z", "valid_until": "2026-09-17T11:00:00Z",
    "qualification_evidence": "Independently qualified incident reference",
}
LAKEHOUSE = {
    "source": {"tenant_id": TENANT, "workspace_id": CONTROL, "item_id": SPN},
    "configuration": {
        "provider": "lakehouse",
        "descriptor": {
            "source": {"tenant_id": TENANT, "workspace_id": CONTROL, "item_id": SPN},
            "captured_at": "2026-09-17T10:00:00Z", "completed_at": "2026-09-17T10:01:00Z",
            "consistency": {
                "reference": "snapshot_freeze",
                "verified_at": "2026-09-17T09:00:00Z", "valid_until": "2026-09-17T11:00:00Z",
                "writes_quiesced": True,
            },
            "storage_read_approval_ref": "approved_snapshot_access",
            "snapshot_qualification_ref": "qualified_snapshot",
            "source_region": "eastus", "recovery_region": "westus",
            "files": [], "directories": ["Tables", "Files"], "source_paths_verified_local": True,
        },
        "max_age_seconds": 3600, "target_approval_ref": "approved_target",
    },
}
LIFECYCLE_REQUESTS = [
    ("start-dr-test", {"generation_id": GENERATION, "group_ids": ["sales"]}),
    ("continue-dr-test", {"test_id": GENERATION}),
    ("end-dr-test", {"test_id": GENERATION}),
    ("schedule-guide", {"generation_id": GENERATION, "approve_scope": True}),
    ("reconcile-operation", RECONCILE),
    ("configure-replica", REPLICA),
    ("configure-protection", LAKEHOUSE),
    ("cutover", {
        "generation_id": GENERATION, "group_ids": ["sales"], "readiness": [], "writer_fence": FENCE,
    }),
    ("plan-failback", {
        "generation_id": GENERATION, "group_ids": ["sales"],
        "primary_available": True, "primary_evidence": "Primary availability evidence",
    }),
    ("execute-failback", {
        "plan_id": GENERATION, "writer_fence": {**FENCE, "fenced_side": "recovery"},
    }),
    ("cutback", {
        "plan_id": GENERATION, "readiness": [], "writer_fence": {**FENCE, "fenced_side": "recovery"},
    }),
    ("rearm", {
        "plan_id": GENERATION, "approve": True, "recovery_no_longer_serving": True,
        "evidence": "Consumer cutback evidence",
    }),
]


class Tokens(TokenProvider):
    def __init__(self, tenant=TENANT):
        super().__init__(ServicePrincipal(tenant, CLIENT, "never-persist-this"))

    def tenant_id(self):
        return self.principal.tenant_id

    def fabric_token(self):
        raise AssertionError("Transport must not query Fabric for recovery metadata")

    def object_id(self):
        return SPN


class Service:
    def __init__(self):
        self.calls = []
        self.closed = False
        self.result = ServiceResult(
            mode=RecoveryMode.STANDBY, generation_id=GENERATION,
            outcome="partial", exit_code=2, warnings=("Orders: supply an off-region export.",),
        )

    def status(self):
        self.calls.append(("status", None))
        return self.result

    def plan(self, request):
        self.calls.append(("plan", request))
        return self.result

    def synchronize(self, request):
        self.calls.append(("synchronize", request))
        return self.result

    def enable_recovery(self, request):
        self.calls.append(("enable-recovery", request))
        return self.result

    def start_dr_test(self, request):
        self.calls.append(("start-dr-test", request))
        return self.result

    def continue_dr_test(self, request):
        self.calls.append(("continue-dr-test", request))
        return self.result

    def end_dr_test(self, request):
        self.calls.append(("end-dr-test", request))
        return self.result

    def schedule_guide(self, request):
        self.calls.append(("schedule-guide", request))
        return self.result

    def cutover(self, request):
        self.calls.append(("cutover", request))
        return self.result

    def plan_failback(self, request):
        self.calls.append(("plan-failback", request))
        return self.result

    def execute_failback(self, request):
        self.calls.append(("execute-failback", request))
        return self.result

    def cutback(self, request):
        self.calls.append(("cutback", request))
        return self.result

    def rearm(self, request):
        self.calls.append(("rearm", request))
        return self.result

    def reconcile_operation(self, request):
        self.calls.append(("reconcile-operation", request))
        return self.result

    def configure_replica(self, request):
        self.calls.append(("configure-replica", request))
        return self.result

    def configure_protection(self, request):
        self.calls.append(("configure-protection", request))
        return self.result

    def close(self):
        self.closed = True


@pytest.fixture
def product(monkeypatch, tmp_path):
    service = Service()
    created = []
    bootstrap = tmp_path / "controller" / "bootstrap.json"
    monkeypatch.setenv("FAB_SHUFFLE_BCDR_BOOTSTRAP", str(bootstrap))

    def factory(path, **kwargs):
        created.append((path, kwargs, threading.current_thread().name))
        return service

    monkeypatch.setattr(bcdr, "create_service", factory)
    session = web.SESSIONS.create(Tokens().principal, Tokens())
    client = TestClient(web.create_app())
    yield client, {web.SESSION_HEADER: session.id}, service, created, bootstrap
    web.SESSIONS.drop(session.id)


def test_all_product_routes_require_authentication(product):
    client, _, service, _, _ = product
    for path in ("forms", "status", "discovery"):
        assert client.get(f"/api/bcdr/{path}").status_code == 401
    for path in (
        "setup", "configure-protection", "plan", "synchronize", "enable-recovery", "cutover",
        "plan-failback", "execute-failback", "cutback", "rearm", "reconcile-operation", "configure-replica",
    ):
        assert client.post(f"/api/bcdr/{path}", json={}).status_code == 401
    assert not service.calls


def test_forms_do_not_resume_capacity_or_read_catalog(product):
    client, headers, _, created, _ = product
    response = client.get("/api/bcdr/forms", headers=headers)
    assert response.status_code == 200
    commands = {entry["name"]: entry for entry in response.json()["commands"]}
    assert commands["synchronize"]["schema"] == SyncRequest.model_json_schema()
    assert commands["setup"]["schema"] == SetupRequest.model_json_schema()
    assert commands["reconcile-operation"]["schema"] == ReconcileOperationRequest.model_json_schema()
    assert commands["configure-replica"]["schema"] == ConfigureReplicaRequest.model_json_schema()
    assert response.json()["identity"] == {"tenant_id": TENANT, "client_id": CLIENT, "object_id": SPN}
    assert "NOT Enable" in commands["synchronize"]["confirmation"]
    assert "control workspace" in commands["enable-recovery"]["confirmation"]
    assert created == []

@pytest.mark.parametrize(("command", "payload"), [
    ("reconcile-operation", RECONCILE),
    ("configure-replica", REPLICA),
])
def test_new_actions_require_exact_confirmation_and_complete_evidence(product, command, payload):
    client, headers, service, created, _ = product
    assert client.post(f"/api/bcdr/{command}", headers=headers, json=payload).status_code == 422
    assert client.post(f"/api/bcdr/{command}", headers=headers, json={
        "confirmation": "enable-recovery", "request": payload,
    }).status_code == 409
    incomplete = {key: value for key, value in payload.items() if key not in {
        "fencing_evidence", "attachments",
    }}
    assert client.post(f"/api/bcdr/{command}", headers=headers, json={
        "confirmation": command, "request": incomplete,
    }).status_code == 422
    assert not service.calls and not created


def test_setup_uses_public_facade_not_an_existing_catalog(product, monkeypatch, caplog):
    client, headers, service, created, bootstrap = product
    calls = []

    def setup(request, path, *, target_tokens):
        calls.append((request, path, target_tokens))
        return service.result

    monkeypatch.setattr(bcdr, "setup_recovery", setup)
    monkeypatch.setattr(bcdr, "_capacity_choices", lambda *args: [
        {"id": TARGET_CAPACITY, "matchStatus": "matched", "arm_resource_id": ARM.lower()},
    ])
    monkeypatch.setattr(bcdr.workspaces, "get_workspace", lambda *_: {"capacityId": TARGET_CAPACITY})
    caplog.set_level("INFO", logger="fabshuffle.web.bcdr")
    response = client.post("/api/bcdr/setup", headers=headers, json={
        "confirmation": "setup", "request": SETUP,
    })
    assert response.status_code == 200
    assert not created
    assert isinstance(calls[0][0], SetupRequest)
    assert calls[0][0].warehouse_name == "Fab Shuffle recovery catalog"
    assert calls[0][0].access_policy.recovery_spn.object_id == SPN
    assert calls[0][1] == bootstrap.resolve()
    assert calls[0][2].principal.client_id == CLIENT
    assert CONTROL in caplog.text and TARGET_CAPACITY in caplog.text and ARM.lower() in caplog.text
    assert "never-persist-this" not in caplog.text


def test_forms_show_owned_saved_setup_without_service_reads(product):
    client, headers, _, created, path = product
    store = BootstrapStore(path)
    store.save(BootstrapDescriptor(
        recovery_set_id=GENERATION, tenant_id=TENANT, application_id=CLIENT, controller_id=SPN,
        capacities=(CapacityAuthorization(
            arm_resource_id=ARM, fabric_capacity_id=TARGET_CAPACITY,
            dedicated_recovery=True, authorized_for_suspend=True,
        ),), catalog_capacity_id=ARM, control_workspace_id=CONTROL,
        warehouse_intent=WarehouseIntent(
            owner_id=SPN, intent_id=GENERATION, display_name="BCDR_Metadata",
            phase="accepted", operation_id=GENERATION,
        ),
    ), expected_revision=None)
    response = client.get("/api/bcdr/forms", headers=headers)
    assert response.status_code == 200
    saved = response.json()["savedSetup"]
    assert saved["warehouseName"] == "BCDR_Metadata" and saved["warehousePhase"] == "accepted"
    assert saved["workspaceId"] == CONTROL and saved["hasOperation"]
    assert "operation_id" not in str(saved) and "never-persist-this" not in response.text
    assert not created
    mismatch = client.get(f"/api/bcdr/workspaces/{SOURCE_CAPACITY}/warehouses", headers=headers)
    assert mismatch.status_code == 409
    store.update(lambda row: row.model_copy(update={"application_id": CONTROL}))
    denied = client.get("/api/bcdr/forms", headers=headers)
    assert denied.status_code == 409 and "BCDR_Metadata" not in denied.text


def test_warehouse_choices_are_scoped_readonly_paginated_and_nonsecret(product, monkeypatch):
    import httpx

    from fabshuffle.fabric.client import FabricClient

    client, headers, _, created, _ = product
    calls = []

    def handler(request):
        calls.append(request)
        assert request.method == "GET"
        assert request.url.path.startswith(f"/v1/workspaces/{CONTROL}")
        if request.url.path.endswith("/warehouses"):
            if "continuationToken" in request.url.params:
                return httpx.Response(200, json={"value": []})
            return httpx.Response(200, json={"value": [{
                "id": GENERATION, "workspaceId": CONTROL, "type": "Warehouse", "displayName": "Metadata",
                "properties": {"connectionString": "do-not-return-host"}, "unexpectedSecret": "do-not-return",
            }], "continuationToken": "next"})
        return httpx.Response(200, json={"id": CONTROL, "displayName": "Recovery metadata"})

    tokens = Tokens()
    tokens.fabric_token = lambda: "fabric-token"
    session = web.SESSIONS.create(tokens.principal, tokens)
    monkeypatch.setattr(bcdr, "FabricClient",
                        lambda provider: FabricClient(provider, transport=httpx.MockTransport(handler)))
    try:
        response = client.get(f"/api/bcdr/workspaces/{CONTROL}/warehouses",
                              headers={web.SESSION_HEADER: session.id})
        assert response.status_code == 200
        assert response.json()["warehouses"] == [{
            "id": GENERATION, "displayName": "Metadata", "recorded": False, "endpointReported": True,
        }]
        assert len(calls) == 3 and not created
        assert "do-not-return" not in response.text and "fabric-token" not in response.text
        assert client.get("/api/bcdr/workspaces/not-a-guid/warehouses", headers=headers).status_code == 422
        assert client.get(f"/api/bcdr/workspaces/{CONTROL}/warehouses").status_code == 401
    finally:
        web.SESSIONS.drop(session.id)


@pytest.mark.parametrize("fields", [
    {"warehouse_action": "existing"},
    {"warehouse_action": "create", "warehouse_id": GENERATION},
    {"warehouse_action": "continue", "warehouse_id": GENERATION},
    {"warehouse_action": "existing", "warehouse_id": GENERATION, "control_workspace_id": None,
     "control_workspace_name": "new"},
])
def test_warehouse_action_contract_rejects_incomplete_or_conflicting_selection(fields):
    with pytest.raises(ValueError):
        SetupRequest.model_validate({**SETUP, **fields})


@pytest.mark.parametrize("failed", [None, "capacities", "workspaces", "mapping"])
def test_preparation_discovery_is_explicit_principal_scoped_and_nonsecret(
    product, monkeypatch, caplog, failed,
):
    client, headers, _, created, _ = product
    calls = []
    caplog.set_level("INFO", logger="fabshuffle.web.bcdr")

    def fail(kind):
        if failed == kind:
            raise FabricApiError("GET", f"https://api.fabric.microsoft.com/v1/{kind}", 403,
                                 '{"errorCode":"AccessDenied","message":"Grant read access"}')

    class Client:
        def __init__(self, tokens):
            self.tokens = tokens

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def capacities(client):
        calls.append(("capacities", client.tokens))
        fail("capacities")
        return [{"id": TARGET_CAPACITY, "displayName": "Recovery F64", "region": "West US"}]

    def workspaces(client):
        calls.append(("workspaces", client.tokens))
        fail("workspaces")
        return [{
            "id": CONTROL, "displayName": "Restricted control", "type": "Workspace",
            "capacityId": TARGET_CAPACITY, "unexpectedSecret": "not-returned",
        }]

    monkeypatch.setattr(bcdr, "FabricClient", Client)
    def matches(*args):
        if failed == "mapping":
            raise bcdr.BootstrapError("Grant Azure capacity read access")
        return []

    monkeypatch.setattr(bcdr, "_capacity_choices", matches)
    monkeypatch.setattr(bcdr.workspaces, "list_capacities", capacities)
    monkeypatch.setattr(bcdr.workspaces, "list_workspaces", workspaces)
    response = client.get("/api/bcdr/discovery", headers=headers)
    assert response.status_code == 200
    assert len(calls) == 2
    result = response.json()
    if failed == "mapping":
        assert "Grant Azure" in result["recovery"]["errors"]["capacityMapping"]
        assert result["source"]["errors"] == {}
        assert "capacityChoices" not in result["recovery"]
    elif failed:
        assert "AccessDenied" in result["source"]["errors"][failed]
        assert failed not in result["source"]
    else:
        assert result["source"]["errors"] == {}
    if failed != "workspaces":
        assert result["recovery"]["workspaces"][0]["id"] == CONTROL
        assert CONTROL in caplog.text and "Restricted control" in caplog.text
    if failed != "capacities":
        assert result["source"]["capacities"][0]["id"] == TARGET_CAPACITY
    assert result["discovery_id"] in caplog.text
    assert "not-returned" not in response.text
    assert "not-returned" not in caplog.text and "never-persist-this" not in caplog.text
    assert created == []


def test_discovery_capacity_matching_uses_recovery_identity(product, monkeypatch):
    client, _, _, _, _ = product
    source, target = Tokens(), Tokens()
    session = web.SESSIONS.create(source.principal, source, target_tokens=target)
    matched_tokens = []

    class Client:
        def __init__(self, tokens):
            self.tokens = tokens

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def matches(tokens, capacities):
        matched_tokens.append(tokens)
        assert capacities[0]["id"] == TARGET_CAPACITY
        return []

    monkeypatch.setattr(bcdr, "FabricClient", Client)
    monkeypatch.setattr(bcdr.workspaces, "list_workspaces", lambda _: [])
    monkeypatch.setattr(bcdr.workspaces, "list_capacities", lambda client: [{
        "id": TARGET_CAPACITY if client.tokens is target else SOURCE_CAPACITY,
        "displayName": "Recovery" if client.tokens is target else "Primary",
    }])
    monkeypatch.setattr(bcdr, "_capacity_choices", matches)
    try:
        response = client.get("/api/bcdr/discovery", headers={web.SESSION_HEADER: session.id})
        assert response.status_code == 200
        assert response.json()["source"]["capacities"][0]["id"] == SOURCE_CAPACITY
        assert response.json()["recovery"]["capacities"][0]["id"] == TARGET_CAPACITY
        assert matched_tokens == [target]
    finally:
        web.SESSIONS.drop(session.id)


@pytest.mark.parametrize("problem", ["changed", "ambiguous", "missing", "catalog", "workspace", "duplicate"])
def test_web_setup_revalidates_capacity_matching_before_any_effect(product, monkeypatch, problem):
    client, headers, _, created, _ = product
    row = {"id": TARGET_CAPACITY, "matchStatus": "matched", "arm_resource_id": ARM.lower()}
    if problem == "changed":
        row["arm_resource_id"] = ARM.lower().replace("/recovery", "/different")
    if problem == "ambiguous":
        row["matchStatus"] = "ambiguous"
    monkeypatch.setattr(bcdr, "_capacity_choices", lambda *args: [] if problem == "missing" else [row])
    workspace_capacity = SOURCE_CAPACITY if problem == "workspace" else TARGET_CAPACITY
    monkeypatch.setattr(bcdr.workspaces, "get_workspace", lambda *_: {"capacityId": workspace_capacity})
    monkeypatch.setattr(bcdr, "setup_recovery", lambda *_args, **_kwargs: pytest.fail("must not mutate"))
    request = {**SETUP}
    if problem == "catalog":
        request["catalog_capacity_id"] = ARM.replace("/recovery", "/different")
    if problem == "duplicate":
        request["recovery_capacities"] = SETUP["recovery_capacities"] * 2
    response = client.post("/api/bcdr/setup", headers=headers, json={
        "confirmation": "setup", "request": request,
    })
    assert response.status_code == 409 and response.json()["detail"]
    assert not created


def test_status_uses_real_factory_with_pinned_credentials_and_closes(product):
    client, headers, service, created, bootstrap = product
    response = client.get("/api/bcdr/status", headers=headers)
    assert response.status_code == 200
    assert response.json()["outcome"] == "partial"
    assert response.json()["exit_code"] == 2
    assert created[0][0] == bootstrap.resolve()
    assert created[0][1]["target_tokens"].principal.client_secret == "never-persist-this"
    assert created[0][1]["source_tokens"] is None
    assert created[0][2] != "MainThread"
    assert service.calls == [("status", None)]
    assert service.closed
    assert "never-persist-this" not in response.text


def test_sync_requires_explicit_matching_confirmation(product):
    client, headers, service, _, _ = product
    for body, code in (
        (SYNC, 422),
        ({"request": SYNC, "confirmation": "enable-recovery"}, 409),
        ({"request": {**SYNC, "park": "yes"}, "confirmation": "synchronize"}, 422),
    ):
        response = client.post("/api/bcdr/synchronize", headers=headers, json=body)
        assert response.status_code == code
    assert service.calls == []
    response = client.post("/api/bcdr/synchronize", headers=headers, json={
        "request": SYNC, "confirmation": "synchronize",
    })
    assert response.status_code == 200
    name, request = service.calls[0]
    assert name == "synchronize" and isinstance(request, SyncRequest)
    assert request.capture is True and request.park is True
    assert service.closed


def test_recovery_does_not_synchronize_or_read_source(product):
    client, headers, service, _, _ = product
    response = client.post("/api/bcdr/enable-recovery", headers=headers, json={
        "confirmation": "enable-recovery",
        "request": {"generation_id": GENERATION, "group_ids": ["sales"], "approved_acl_ids": []},
    })
    assert response.status_code == 200
    assert service.calls[0][0] == "enable-recovery"
    assert service.calls[0][1].generation_id == GENERATION


@pytest.mark.parametrize(("command", "payload"), LIFECYCLE_REQUESTS)
def test_every_lifecycle_route_calls_its_exact_public_service_method(product, command, payload):
    client, headers, service, created, _ = product
    body = payload if command == "plan-failback" else {"confirmation": command, "request": payload}
    response = client.post(f"/api/bcdr/{command}", headers=headers, json=body)
    assert response.status_code == 200
    assert service.calls[0][0] == command
    assert service.closed
    assert (created[0][1]["source_tokens"] is not None) == (command == "plan-failback")


def test_request_validation_does_not_echo_pasted_secret_or_allow_server_paths(product):
    client, headers, _, created, _ = product
    response = client.post("/api/bcdr/plan", headers=headers, json={
        **SYNC, "client_secret": "accidentally-pasted", "bootstrap_path": "/private/elsewhere",
    })
    assert response.status_code == 422
    assert "accidentally-pasted" not in response.text
    assert "/private/elsewhere" not in response.text
    assert created == []


def test_service_error_preserves_code_and_message_and_closes(product, monkeypatch):
    client, headers, service, _, _ = product

    def fail():
        raise FabricApiError("GET", "https://api.fabric.microsoft.com/v1/items", 403,
                             '{"errorCode":"InsufficientPrivileges","message":"Grant access to catalog"}')

    monkeypatch.setattr(service, "status", fail)
    response = client.get("/api/bcdr/status", headers=headers)
    assert response.status_code == 403
    assert "InsufficientPrivileges" in response.text
    assert "Grant access to catalog" in response.text
    assert service.closed


@pytest.mark.parametrize(("error", "status"), [
    (RecoveryBlocked("Fence the named primary writer."), 409),
    (CatalogConflict("Reconcile the pending controller lease."), 409),
    (CatalogError("SQLSTATE 08S01: metadata Warehouse unavailable."), 503),
])
def test_backend_blockers_never_become_success(product, monkeypatch, error, status):
    client, headers, service, _, _ = product

    def fail():
        raise error

    monkeypatch.setattr(service, "status", fail)
    response = client.get("/api/bcdr/status", headers=headers)
    assert response.status_code == status
    assert str(error) in response.json()["detail"]
    assert service.closed


@pytest.mark.asyncio
async def test_cancelled_request_waits_for_worker_to_settle(product, monkeypatch):
    _, headers, service, _, _ = product
    entered, release = threading.Event(), threading.Event()

    def status():
        entered.set()
        assert release.wait(10)
        return service.result

    monkeypatch.setattr(service, "status", status)
    async with AsyncClient(transport=ASGITransport(app=web.create_app()), base_url="http://test") as client:
        task = asyncio.create_task(client.get("/api/bcdr/status", headers=headers))
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not service.closed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert service.closed


def test_cross_tenant_recovery_is_refused_before_factory(product):
    client, _, _, created, _ = product
    source = Tokens()
    target = Tokens("00000000-0000-0000-0000-000000000099")
    session = web.SESSIONS.create(source.principal, source, target_tokens=target)
    try:
        headers = {web.SESSION_HEADER: session.id}
        assert client.get("/api/bcdr/status", headers=headers).status_code == 409
        assert client.get("/api/bcdr/forms", headers=headers).status_code == 409
        assert created == []
    finally:
        web.SESSIONS.drop(session.id)


@pytest.fixture
def cli_service(monkeypatch):
    service = Service()
    factories = []
    monkeypatch.setenv("FAB_SHUFFLE_BCDR_TENANT_ID", TENANT)
    monkeypatch.setenv("FAB_SHUFFLE_BCDR_CLIENT_ID", CLIENT)
    monkeypatch.setenv("FAB_SHUFFLE_BCDR_CLIENT_SECRET", "runtime-secret")
    monkeypatch.delenv("FAB_SHUFFLE_BCDR_CLIENT_SECRET_FILE", raising=False)

    def factory(path, **kwargs):
        factories.append((path, kwargs))
        return service

    monkeypatch.setattr(cli, "create_service", factory)
    return service, factories


def test_cli_status_is_source_free_and_partial_exits_nonzero(cli_service, capsys):
    service, factories = cli_service
    assert cli.main(["status", "--bootstrap", "/controller/bootstrap.json"]) == 2
    assert factories[0][0] == Path("/controller/bootstrap.json")
    assert factories[0][1]["source_tokens"] is None
    assert service.closed
    output = capsys.readouterr()
    assert json.loads(output.out)["outcome"] == "partial"
    assert "runtime-secret" not in output.out + output.err


def test_cli_requires_named_confirmation_before_opening_service(cli_service, capsys):
    service, factories = cli_service
    assert cli.main(["synchronize", "--bootstrap", "/controller/bootstrap.json"]) == 1
    assert "--confirm synchronize" in capsys.readouterr().err
    assert not factories and not service.calls


def test_cli_request_from_stdin_is_typed_and_runtime_credential_not_in_request(
    cli_service, monkeypatch, capsys,
):
    service, factories = cli_service
    monkeypatch.setattr("sys.stdin", StringIO(json.dumps(SYNC)))
    code = cli.main(["synchronize", "--bootstrap", "/controller/bootstrap.json", "--confirm", "synchronize"])
    assert code == 2
    assert isinstance(service.calls[0][1], SyncRequest)
    assert factories[0][1]["source_tokens"] is factories[0][1]["target_tokens"]
    assert "runtime-secret" not in capsys.readouterr().out


def test_cli_validation_redacts_input_and_does_not_open_service(cli_service, monkeypatch, capsys):
    _, factories = cli_service
    monkeypatch.setattr("sys.stdin", StringIO(json.dumps({**SYNC, "client_secret": "pasted-secret"})))
    assert cli.main(["plan", "--bootstrap", "/controller/bootstrap.json"]) == 2
    assert "pasted-secret" not in capsys.readouterr().err
    assert not factories


def test_cli_secret_file_is_external_and_exclusive(tmp_path):
    secret = tmp_path / "secret"
    secret.write_text("file-secret\n", encoding="utf-8")
    environ = {
        "FAB_SHUFFLE_BCDR_TENANT_ID": TENANT, "FAB_SHUFFLE_BCDR_CLIENT_ID": CLIENT,
        "FAB_SHUFFLE_BCDR_CLIENT_SECRET_FILE": str(secret),
    }
    assert cli.credential_provider(environ).principal.client_secret == "file-secret"
    with pytest.raises(ValueError, match="not both"):
        cli.credential_provider({**environ, "FAB_SHUFFLE_BCDR_CLIENT_SECRET": "env-secret"})


def test_cli_schema_does_not_require_credentials_or_bootstrap(cli_service, capsys):
    _, factories = cli_service
    assert cli.main(["synchronize", "--schema"]) == 0
    assert json.loads(capsys.readouterr().out) == SyncRequest.model_json_schema()
    assert not factories


def test_cli_never_echoes_mistaken_secret_options(capsys):
    with pytest.raises(SystemExit) as stopped:
        cli.main(["status", "--client-secret", "should-not-appear"])
    assert stopped.value.code == 2
    assert "should-not-appear" not in capsys.readouterr().err


def test_cli_setup_uses_public_facade_before_catalog_exists(cli_service, monkeypatch, capsys):
    service, factories = cli_service
    calls = []
    monkeypatch.setattr("sys.stdin", StringIO(json.dumps(SETUP)))

    def setup(request, path, *, target_tokens):
        calls.append((request, path, target_tokens))
        return service.result

    monkeypatch.setattr(cli, "setup_recovery", setup)
    assert cli.main(["setup", "--bootstrap", "/controller/bootstrap.json", "--confirm", "setup"]) == 2
    assert not factories
    assert isinstance(calls[0][0], SetupRequest)
    assert calls[0][1] == Path("/controller/bootstrap.json")
    assert calls[0][2].principal.client_id == CLIENT
    assert "runtime-secret" not in capsys.readouterr().out


@pytest.mark.parametrize(("command", "payload"), LIFECYCLE_REQUESTS)
def test_every_lifecycle_cli_command_calls_its_exact_public_service_method(
    cli_service, monkeypatch, capsys, command, payload,
):
    service, factories = cli_service
    monkeypatch.setattr("sys.stdin", StringIO(json.dumps(payload)))
    args = [command, "--bootstrap", "/controller/bootstrap.json"]
    if command in cli.CONSEQUENTIAL:
        args += ["--confirm", command]
    assert cli.main(args) == 2
    assert service.calls[0][0] == command
    assert service.closed
    assert (factories[0][1]["source_tokens"] is not None) == (command == "plan-failback")
    assert "runtime-secret" not in capsys.readouterr().out


@pytest.mark.parametrize("descriptor_exists", [False, True])
def test_real_factory_rejects_missing_or_wrong_tenant_bootstrap_before_source_reads(
    tmp_path, monkeypatch, descriptor_exists,
):
    path = tmp_path / "controller" / "bootstrap.json"
    monkeypatch.setenv("FAB_SHUFFLE_BCDR_BOOTSTRAP", str(path))
    if descriptor_exists:
        BootstrapStore(path).save(BootstrapDescriptor(
            recovery_set_id=GENERATION, tenant_id="00000000-0000-0000-0000-000000000099",
            application_id=CLIENT, controller_id=SPN,
            capacities=(CapacityAuthorization(
                arm_resource_id=ARM, fabric_capacity_id=TARGET_CAPACITY,
                dedicated_recovery=True, authorized_for_suspend=True,
            ),),
            catalog_capacity_id=ARM, control_workspace_id=CONTROL,
        ), expected_revision=None)
    tokens = Tokens()
    session = web.SESSIONS.create(tokens.principal, tokens)
    try:
        with TestClient(web.create_app()) as client:
            response = client.get("/api/bcdr/status", headers={web.SESSION_HEADER: session.id})
        assert response.status_code == 409
        assert response.json()["detail"]
        assert "never-persist-this" not in response.text
    finally:
        web.SESSIONS.drop(session.id)


def test_real_cli_factory_reports_missing_bootstrap_without_source_reads(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli, "credential_provider", lambda environ: Tokens())
    code = cli.main(["status", "--bootstrap", str(tmp_path / "missing.json")])
    assert code == 1
    output = capsys.readouterr()
    assert "bootstrap" in output.err.lower()
    assert not output.out
    assert "never-persist-this" not in output.err
