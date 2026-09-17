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
from fabshuffle.bcdr.catalog import CatalogConflict, CatalogError
from fabshuffle.bcdr.contracts import RecoveryMode
from fabshuffle.bcdr.service import ServiceResult, SetupRequest, SyncRequest
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
    "/resourceGroups/dr/providers/Microsoft.Fabric/capacities/dr"
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
LIFECYCLE_REQUESTS = [
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
        "plan-failback", "execute-failback", "cutback", "rearm",
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
    assert response.json()["identity"] == {"tenant_id": TENANT, "client_id": CLIENT, "object_id": SPN}
    assert "NOT Enable" in commands["synchronize"]["confirmation"]
    assert "control workspace" in commands["enable-recovery"]["confirmation"]
    assert created == []


def test_setup_uses_public_facade_not_an_existing_catalog(product, monkeypatch):
    client, headers, service, created, bootstrap = product
    calls = []

    def setup(request, path, *, target_tokens):
        calls.append((request, path, target_tokens))
        return service.result

    monkeypatch.setattr(bcdr, "setup_recovery", setup)
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


def test_preparation_discovery_is_explicit_principal_scoped_and_nonsecret(product, monkeypatch):
    client, headers, _, created, _ = product
    calls = []

    class Client:
        def __init__(self, tokens):
            self.tokens = tokens

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def capacities(client):
        calls.append(("capacities", client.tokens))
        return [{"id": TARGET_CAPACITY, "displayName": "Recovery F64", "region": "West US"}]

    def workspaces(client):
        calls.append(("workspaces", client.tokens))
        return [{
            "id": CONTROL, "displayName": "Restricted control", "type": "Workspace",
            "capacityId": TARGET_CAPACITY, "unexpectedSecret": "not-returned",
        }]

    monkeypatch.setattr(bcdr, "FabricClient", Client)
    monkeypatch.setattr(bcdr.workspaces, "list_capacities", capacities)
    monkeypatch.setattr(bcdr.workspaces, "list_workspaces", workspaces)
    response = client.get("/api/bcdr/discovery", headers=headers)
    assert response.status_code == 200
    assert len(calls) == 2
    assert response.json()["recovery"]["workspaces"][0]["id"] == CONTROL
    assert "not-returned" not in response.text
    assert created == []


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
