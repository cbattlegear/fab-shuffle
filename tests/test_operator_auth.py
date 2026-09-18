from __future__ import annotations

import base64
import json

import pytest
from fastapi import Depends
from fastapi.testclient import TestClient

from fabshuffle.auth import AuthError, ManagedIdentity, TokenProvider
from fabshuffle.web import app as web
from fabshuffle.web.operators import OperatorPolicy

TENANT = "11111111-1111-4111-8111-111111111111"
OPERATOR = "22222222-2222-4222-8222-222222222222"
OTHER = "33333333-3333-4333-8333-333333333333"
CLIENT = "44444444-4444-4444-8444-444444444444"


def identity_headers(*, tenant=TENANT, object_id=OPERATOR, provider="aad", claims=None):
    document = {
        "auth_typ": provider,
        "claims": claims if claims is not None else [
            {"typ": "http://schemas.microsoft.com/identity/claims/tenantid", "val": tenant},
            {"typ": "http://schemas.microsoft.com/identity/claims/objectidentifier", "val": object_id},
        ],
    }
    return {
        "X-MS-CLIENT-PRINCIPAL": base64.b64encode(json.dumps(document).encode()).decode(),
        "X-MS-CLIENT-PRINCIPAL-ID": object_id,
        "X-MS-CLIENT-PRINCIPAL-IDP": provider,
        "X-Fab-Shuffle-Request": "1",
    }


@pytest.fixture
def azure(monkeypatch):
    monkeypatch.setenv("CONTAINER_APP_NAME", "fab-shuffle-test")
    monkeypatch.setenv("FAB_SHUFFLE_EASYAUTH_ENABLED", "true")
    monkeypatch.setenv("FAB_SHUFFLE_EASYAUTH_READY", "true")
    monkeypatch.setenv("FAB_SHUFFLE_EASYAUTH_TENANT_ID", TENANT)
    monkeypatch.setenv("FAB_SHUFFLE_EASYAUTH_ALLOWED_OBJECT_IDS", f"{OPERATOR},{OTHER}")
    monkeypatch.setattr(web, "SESSIONS", web.SessionStore())
    monkeypatch.setattr(web, "managed_identity_principal", lambda: ManagedIdentity(TENANT, CLIENT))
    monkeypatch.setattr(TokenProvider, "verify", lambda self: None)
    app = web.create_app()

    @app.get("/api/test-session")
    def session_identity(session=Depends(web.require_session)):
        return {"operator": session.operator_id, "managed": isinstance(session.principal, ManagedIdentity)}

    with TestClient(app) as client:
        yield client


@pytest.mark.parametrize("path", ["/", "/static/app.js", "/api/docs", "/openapi.json", "/api/auth/options"])
def test_azure_routes_fail_closed_without_sidecar_identity(azure, path):
    response = azure.get(path)
    assert response.status_code == 401
    assert response.headers["cache-control"] == "no-store"


def test_health_is_the_only_anonymous_read(azure):
    assert azure.get("/api/health").status_code == 200
    assert azure.post("/api/health").status_code == 401


def test_pending_auth_deployment_rejects_even_forged_operator_headers(azure, monkeypatch):
    monkeypatch.setenv("FAB_SHUFFLE_EASYAUTH_READY", "false")
    with TestClient(web.create_app()) as client:
        assert client.get("/api/health").status_code == 200
        response = client.post(
            "/api/login/managed-identity", json={}, headers=identity_headers(),
        )
        assert response.status_code == 503
        assert "Finish the Azure EasyAuth deployment" in response.json()["detail"]
        assert not web.SESSIONS._sessions


@pytest.mark.parametrize(
    "kwargs,status",
    [
        ({"object_id": CLIENT}, 403),
        ({"tenant": CLIENT}, 403),
        ({"provider": "google"}, 401),
        ({"claims": []}, 401),
        ({"claims": [{"typ": "oid", "val": OPERATOR}, {"typ": "tid", "val": "invalid"}]}, 401),
        ({"claims": [
            {"typ": "oid", "val": OPERATOR}, {"typ": "oid", "val": OTHER},
            {"typ": "tid", "val": TENANT},
        ]}, 401),
    ],
)
def test_unauthorized_or_ambiguous_principals_cannot_invoke_identity(azure, kwargs, status):
    response = azure.post("/api/login/managed-identity", json={}, headers=identity_headers(**kwargs))
    assert response.status_code == status
    assert not web.SESSIONS._sessions


@pytest.mark.parametrize("value", ["not base64", "W10=", "e30=", "A" * 32769])
def test_malformed_platform_claims_are_not_trusted(azure, value):
    response = azure.get("/api/auth/options", headers={"X-MS-CLIENT-PRINCIPAL": value})
    assert response.status_code == 401


def test_duplicate_platform_identity_headers_are_rejected(azure):
    headers = list(identity_headers().items())
    headers.append(("X-MS-CLIENT-PRINCIPAL", identity_headers()["X-MS-CLIENT-PRINCIPAL"]))
    assert azure.get("/api/auth/options", headers=headers).status_code == 401


def test_conflicting_sidecar_id_header_is_rejected(azure):
    headers = {**identity_headers(), "X-MS-CLIENT-PRINCIPAL-ID": OTHER}
    assert azure.get("/api/auth/options", headers=headers).status_code == 401


def test_duplicate_encoded_json_identity_fields_are_rejected(azure):
    encoded = base64.b64encode(b'{"auth_typ":"aad","auth_typ":"aad","claims":[]}').decode()
    response = azure.get("/api/auth/options", headers={"X-MS-CLIENT-PRINCIPAL": encoded})
    assert response.status_code == 401


def test_options_report_runtime_identity_without_requesting_its_tokens(azure, monkeypatch):
    def forbidden(*args):
        raise AssertionError("Options must not acquire an ambient credential.")

    monkeypatch.setattr(TokenProvider, "verify", forbidden)
    response = azure.get("/api/auth/options", headers=identity_headers())
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "easyAuth": True,
        "operator": {"tenantId": TENANT, "objectId": OPERATOR},
        "managedIdentity": {"tenant_id": TENANT, "client_id": CLIENT},
        "servicePrincipal": True,
    }


def test_managed_identity_is_fixed_by_deployment_and_session_is_operator_bound(azure):
    headers = identity_headers()
    response = azure.post("/api/login/managed-identity", json={}, headers=headers)
    assert response.status_code == 200
    body = response.json()
    assert body["managedIdentity"] is True and body["paired"] is False
    assert "secret" not in response.text
    session_id = body["sessionId"]
    assert azure.get(
        "/api/test-session", headers={**headers, web.SESSION_HEADER: session_id},
    ).json() == {"operator": f"{TENANT}/{OPERATOR}", "managed": True}
    other_headers = {**identity_headers(object_id=OTHER), web.SESSION_HEADER: session_id}
    assert azure.get("/api/test-session", headers=other_headers).status_code == 403
    assert azure.post("/api/logout", headers=other_headers).status_code == 403
    assert web.SESSIONS.get(session_id) is not None
    assert azure.get(
        "/api/runs/unknown/events", params={"session_id": session_id}, headers=other_headers,
    ).status_code == 403
    assert azure.post(
        "/api/logout", headers={**headers, web.SESSION_HEADER: session_id},
    ).status_code == 200
    assert web.SESSIONS.get(session_id) is None


def test_secret_sessions_are_also_bound_to_the_easyauth_operator(azure):
    response = azure.post(
        "/api/login",
        json={"tenant_id": TENANT, "client_id": CLIENT, "client_secret": "test-secret"},
        headers=identity_headers(),
    )
    assert response.status_code == 200
    assert web.SESSIONS.get(response.json()["sessionId"]).operator_id == f"{TENANT}/{OPERATOR}"


@pytest.mark.parametrize("body", [{"client_id": OTHER}, {"tenant_id": OTHER}, {"destination": {}}])
def test_caller_cannot_select_another_host_identity(azure, body):
    response = azure.post("/api/login/managed-identity", json=body, headers=identity_headers())
    assert response.status_code == 422


@pytest.mark.parametrize(
    "extra",
    [
        {"Origin": "https://attacker.example"},
        {"Origin": "null"},
        {"Origin": "https://testserver.evil"},
        {"Origin": "https://user@testserver"},
        {"Origin": "http://testserver"},
        {"Sec-Fetch-Site": "cross-site"},
    ],
)
def test_cookie_authenticated_cross_site_writes_are_refused(azure, extra):
    response = azure.post(
        "/api/login/managed-identity", json={}, headers={**identity_headers(), **extra},
    )
    assert response.status_code == 403
    assert not web.SESSIONS._sessions


def test_mutations_require_non_simple_request_header(azure):
    headers = identity_headers()
    del headers["X-Fab-Shuffle-Request"]
    response = azure.post("/api/login/managed-identity", json={}, headers=headers)
    assert response.status_code == 403
    assert azure.post(
        "/api/login/managed-identity", json={},
        headers={**identity_headers(), "Origin": "https://testserver"},
    ).status_code == 200


def test_managed_identity_provider_errors_remain_visible(azure, monkeypatch):
    def fail():
        raise AuthError("The configured managed identity is unavailable.")

    monkeypatch.setattr(web, "managed_identity_principal", fail)
    assert azure.get("/api/auth/options", headers=identity_headers()).status_code == 503
    response = azure.post("/api/login/managed-identity", json={}, headers=identity_headers())
    assert response.status_code == 401
    assert "unavailable" in response.json()["detail"]
    assert not web.SESSIONS._sessions


def test_no_configured_identity_reports_disabled_without_fallback(azure, monkeypatch):
    monkeypatch.setattr(web, "managed_identity_principal", lambda: None)
    assert azure.get("/api/auth/options", headers=identity_headers()).json()["managedIdentity"] is None
    assert azure.post(
        "/api/login/managed-identity", json={}, headers=identity_headers(),
    ).status_code == 409


def test_local_mode_never_trusts_headers_to_enable_managed_identity(monkeypatch):
    monkeypatch.delenv("FAB_SHUFFLE_EASYAUTH_ENABLED", raising=False)
    monkeypatch.setattr(web, "SESSIONS", web.SessionStore())

    def forbidden():
        raise AssertionError("Local web mode must never select ambient Azure credentials.")

    monkeypatch.setattr(web, "managed_identity_principal", forbidden)
    client = TestClient(web.create_app())
    assert client.get("/").status_code == 200
    assert client.get("/api/auth/options", headers=identity_headers()).json()["managedIdentity"] is None
    assert client.post(
        "/api/login/managed-identity", json={}, headers=identity_headers(),
    ).status_code == 403


@pytest.mark.parametrize(
    "environment",
    [
        {"FAB_SHUFFLE_EASYAUTH_ENABLED": "yes"},
        {"FAB_SHUFFLE_EASYAUTH_ENABLED": "true"},
        {
            "FAB_SHUFFLE_EASYAUTH_ENABLED": "true", "CONTAINER_APP_NAME": "app",
            "FAB_SHUFFLE_EASYAUTH_TENANT_ID": TENANT,
        },
        {
            "FAB_SHUFFLE_EASYAUTH_ENABLED": "true", "CONTAINER_APP_NAME": "app",
            "FAB_SHUFFLE_EASYAUTH_TENANT_ID": TENANT, "FAB_SHUFFLE_EASYAUTH_ALLOWED_OBJECT_IDS": " ",
        },
    ],
)
def test_incomplete_easyauth_configuration_fails_closed(environment):
    with pytest.raises(ValueError):
        OperatorPolicy.from_environment(environment)
