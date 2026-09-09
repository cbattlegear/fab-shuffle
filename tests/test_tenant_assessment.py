"""Tenant discovery and assessment keep source and destination credentials separate."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from fabshuffle import auth, journal, orchestrator
from fabshuffle.auth import AuthError, ServicePrincipal, TokenProvider
from fabshuffle.config import SCOPE_FABRIC, SCOPE_SQL
from fabshuffle.fabric.support import Strategy, assess_workspace
from fabshuffle.run import MigrationRun
from fabshuffle.web import app as web
from tests.test_token_claims import jwt

SOURCE_TENANT = "aaaaaaaa-1111-2222-3333-444444444444"
TARGET_TENANT = "bbbbbbbb-1111-2222-3333-444444444444"
SOURCE = ServicePrincipal(SOURCE_TENANT, "source-app", "source-secret")
TARGET = ServicePrincipal(TARGET_TENANT, "target-app", "target-secret")
ITEMS = [
    {"id": "model", "displayName": "Model", "type": "SemanticModel"},
    {"id": "report", "displayName": "Report", "type": "Report"},
    {"id": "dashboard", "displayName": "Dashboard", "type": "Dashboard"},
    {"id": "paginated", "displayName": "Paginated", "type": "PaginatedReport"},
]


class StubTokens(TokenProvider):
    def token(self, scope: str) -> str:
        if self.principal.client_secret == "bad":
            raise AuthError("AADSTS7000215: Invalid client secret")
        tenant = self.principal.tenant_id
        if tenant == "source.onmicrosoft.com":
            tenant = SOURCE_TENANT
        return jwt({"tid": tenant, "appid": self.principal.client_id, "aud": scope})


class DisjointFabric:
    def __init__(self, tokens: TokenProvider, calls: list[tuple[str, str]]) -> None:
        self.side = "source" if tokens.principal.client_id == SOURCE.client_id else "target"
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return None

    def get(self, path, params=None):
        self.calls.append((self.side, path))
        if self.side == "source":
            if path == "workspaces/source-ws":
                return {"id": "source-ws", "displayName": "Sales", "capacityId": "source-cap"}
            if path == "capacities/source-cap":
                return {"id": "source-cap", "sku": "F64"}
        elif path == "capacities/target-cap":
            return {
                "id": "target-cap", "displayName": "Destination", "region": "West Europe", "sku": "F32",
            }
        raise AssertionError(f"Wrong-side read: {self.side} {path}")

    def list_all(self, path, params=None):
        self.calls.append((self.side, path))
        if self.side == "source":
            if path == "workspaces/source-ws/items":
                return list(ITEMS)
            if path == "workspaces":
                return [{"id": "source-ws", "displayName": "Sales"}]
        elif path == "capacities":
            return [{"id": "target-cap", "displayName": "Destination", "region": "West Europe"}]
        raise AssertionError(f"Wrong-side listing: {self.side} {path}")


@pytest.fixture
def api(monkeypatch):
    monkeypatch.setattr(web, "SESSIONS", web.SessionStore())
    monkeypatch.setattr(web, "TokenProvider", StubTokens)
    with TestClient(web.create_app()) as client:
        yield client


@pytest.fixture
def calls(monkeypatch):
    recorded = []
    monkeypatch.setattr(web, "FabricClient", lambda tokens: DisjointFabric(tokens, recorded))
    return recorded


def credentials(principal=SOURCE):
    return {**principal.redacted(), "client_secret": principal.client_secret}


def login(api, *, destination=TARGET):
    body = credentials()
    if destination is not None:
        body["destination"] = credentials(destination)
    response = api.post("/api/login", json=body)
    assert response.status_code == 200, response.text
    return response.json()


def headers(result):
    return {web.SESSION_HEADER: result["sessionId"]}


def cannot_run(*args, **kwargs):
    raise AssertionError("Assessment reached execution or an unexpected service call")


def test_single_principal_login_contract_is_unchanged(api):
    result = login(api, destination=None)
    assert set(result) == {"sessionId", "principal"}
    assert result["principal"] == SOURCE.redacted()
    session = web.SESSIONS.get(result["sessionId"])
    assert session.destination_tokens is session.tokens
    assert not session.paired
    assert not session.cross_tenant


def test_paired_login_keeps_independent_immutable_contexts_and_redacts_secrets(api):
    result = login(api)
    session = web.SESSIONS.get(result["sessionId"])
    assert result["paired"] is True
    assert result["sourceTenantId"] == SOURCE_TENANT
    assert result["targetTenantId"] == TARGET_TENANT
    assert result["destinationPrincipal"] == TARGET.redacted()
    assert session.tokens is not session.destination_tokens
    assert session.tokens.principal == SOURCE
    assert session.destination_tokens.principal == TARGET
    assert session.cross_tenant
    for secret in (SOURCE.client_secret, TARGET.client_secret):
        assert secret not in str(result)
        assert secret not in repr(session)
        assert secret not in repr(web.LoginRequest(**credentials(), destination=credentials(TARGET)))
    with pytest.raises(FrozenInstanceError):
        session.target_tokens = session.tokens


def test_same_tenant_alias_is_not_mistaken_for_cross_tenant(api):
    result = login(api, destination=ServicePrincipal("source.onmicrosoft.com", "target-app", "secret"))
    session = web.SESSIONS.get(result["sessionId"])
    assert result["sourceTenantId"] == result["targetTenantId"] == SOURCE_TENANT
    assert not session.cross_tenant
    assert session.paired


@pytest.mark.parametrize("side", ["Source", "Destination"])
def test_failed_endpoint_sign_in_preserves_service_error_without_registering_pair(api, side):
    body = credentials()
    body["destination"] = credentials(TARGET)
    if side == "Source":
        body["client_secret"] = "bad"
    else:
        body["destination"]["client_secret"] = "bad"
    response = api.post("/api/login", json=body)
    assert response.status_code == 401
    assert side in response.json()["detail"]
    assert "AADSTS7000215: Invalid client secret" in response.json()["detail"]
    assert not web.SESSIONS._sessions


def test_login_and_logout_do_not_replace_another_pair(api):
    first = login(api)
    first_session = web.SESSIONS.get(first["sessionId"])
    second = login(api, destination=ServicePrincipal(SOURCE_TENANT, "other-app", "other-secret"))
    assert web.SESSIONS.get(first["sessionId"]) is first_session
    assert first_session.destination_tokens.principal == TARGET
    assert api.post("/api/logout", headers=headers(first)).status_code == 200
    assert web.SESSIONS.get(first["sessionId"]) is None
    assert web.SESSIONS.get(second["sessionId"]) is not None
    assert api.get("/api/workspaces", headers=headers(first)).status_code == 401


def test_discovery_routes_source_workspaces_and_destination_capacities(api, calls):
    result = login(api)
    assert api.get("/api/workspaces", headers=headers(result)).json()["workspaces"][0]["id"] == "source-ws"
    assert api.get("/api/capacities", headers=headers(result)).json()["capacities"][0]["id"] == "target-cap"
    assert calls == [("source", "workspaces"), ("target", "capacities")]


def test_preview_forces_rebuild_without_claiming_transport_readiness(api, calls, monkeypatch):
    monkeypatch.setattr(web, "PowerBiClient", cannot_run)
    result = login(api)
    response = api.get(
        "/api/preview", headers=headers(result),
        params={"capacity_id": "target-cap", "source_workspace_id": "source-ws"},
    )
    assert response.status_code == 200, response.text
    preview = response.json()
    assert preview["strategy"] == "rebuild"
    assert preview["copyPermissions"] is False
    assert preview["targetWorkspaceName"] == "Sales-westeurope"
    assert preview["sourceTenantId"] == SOURCE_TENANT
    assert preview["targetTenantId"] == TARGET_TENANT
    assert preview["paired"] is True
    assert preview["migratedTotal"] == 2
    assert set(preview["unsupportedItemTypes"]) == {"Dashboard", "PaginatedReport"}
    assert preview["blockers"] == []
    assert "not qualified" in preview["assessmentNotice"]
    assert "smaller" in preview["capacityWarning"]
    assert ("target", "capacities/target-cap") in calls
    assert ("source", "capacities/source-cap") in calls
    assert ("source", "workspaces/source-ws") in calls


def test_source_dependency_inventory_is_not_skipped_for_power_bi_cross_tenant(api, calls, monkeypatch):
    seen = []

    def dependency_report(client, **kwargs):
        assert client.side == "source"
        assert kwargs["source_workspace_id"] == "source-ws"
        assert kwargs["client_id"] == SOURCE.client_id
        seen.extend(item["id"] for item in kwargs["migrated"])
        return {"dependencies": ["External model needs a destination mapping"], "connectionAccess": None}

    monkeypatch.setattr(web, "_dependency_report", dependency_report)
    result = login(api)
    response = api.get(
        "/api/preview/dependencies", headers=headers(result),
        params={"source_workspace_id": "source-ws"},
    )
    assert response.status_code == 200
    report = response.json()
    assert seen == ["model", "report"]
    assert report["connectionAccessScope"] == "source"
    assert report["paired"] is True
    assert report["blockers"] == []
    assert "does not establish" in report["assessmentNotice"]
    assert all(side == "source" for side, _path in calls)


def test_same_tenant_paired_preview_does_not_have_a_blanket_execution_blocker(api, calls, monkeypatch):
    class SourcePowerBi:
        def __init__(self, tokens):
            assert tokens.principal == SOURCE

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

        def list_semantic_models(self, workspace_id):
            assert workspace_id == "source-ws"
            return []

    monkeypatch.setattr(web, "PowerBiClient", SourcePowerBi)
    result = login(api, destination=ServicePrincipal("source.onmicrosoft.com", "target-app", "secret"))
    response = api.get(
        "/api/preview", headers=headers(result),
        params={"capacity_id": "target-cap", "source_workspace_id": "source-ws"},
    )
    assert response.status_code == 200
    preview = response.json()
    assert preview["strategy"] == "rebuild"
    assert preview["sourceTenantId"] == preview["targetTenantId"] == SOURCE_TENANT
    assert preview["paired"] is True
    assert preview["blockers"] == []


GLOBAL_ENDPOINTS = [
    ("POST", "/api/scratch-workspaces/cleanup", None),
    ("POST", "/api/workspaces/restore-access", {
        "source_workspace_id": "source-ws", "target_workspace_id": "target-ws",
    }),
    ("GET", "/api/scratch-workspaces", None),
]


@pytest.mark.parametrize("same_tenant", [False, True])
@pytest.mark.parametrize(("method", "path", "body"), GLOBAL_ENDPOINTS)
def test_paired_sessions_cannot_enter_unscoped_cleanup(api, monkeypatch, same_tenant, method, path, body):
    destination = ServicePrincipal(SOURCE_TENANT, "target-app", "target-secret") if same_tenant else TARGET
    result = login(api, destination=destination)
    monkeypatch.setattr(web, "FabricClient", cannot_run)
    monkeypatch.setattr(web.REGISTRY, "admit", cannot_run)
    monkeypatch.setattr(web, "_require_run", cannot_run)
    monkeypatch.setattr(web.journal, "read", cannot_run)
    response = api.request(
        method, path, json=body, headers=headers(result),
        params={"session_id": result["sessionId"]} if path.endswith("/events") else None,
    )
    assert response.status_code == 409, response.text
    assert "disabled for paired sign-ins" in response.json()["detail"]


def make_plan(**changes):
    values = {
        "capacity_id": "target-cap", "capacity_name": "Destination", "capacity_region": "westeurope",
        "source_workspace_id": "source-ws", "source_workspace_name": "Sales",
        "target_workspace_name": "Sales-copy", "source_tenant_id": SOURCE_TENANT,
        "target_tenant_id": TARGET_TENANT, "copy_permissions": False,
        "source_client_id": SOURCE.client_id, "target_client_id": TARGET.client_id,
        "write_freeze_confirmed": True,
    }
    return orchestrator.MigrationPlan(**{**values, **changes})


def test_paired_plan_cannot_be_started_with_only_a_source_sign_in(api, monkeypatch):
    plan = make_plan()
    run = MigrationRun(source_workspace_name="Sales", capacity_name="Destination")
    monkeypatch.setattr(orchestrator, "TokenProvider", cannot_run)
    monkeypatch.setattr(orchestrator.journal_module, "Journal", cannot_run)
    with pytest.raises(ValueError):
        orchestrator.run_migration(run, SOURCE, plan)
    assert not run.journal_started

    single = login(api, destination=None)
    session = web.SESSIONS.get(single["sessionId"])
    monkeypatch.setattr(web.REGISTRY, "admit", cannot_run)
    with pytest.raises(HTTPException) as error:
        web._start_attempt(session, plan, cleanup=True)
    assert error.value.status_code == 409


def test_endpoint_identity_survives_plan_serialization_without_credentials():
    plan = make_plan()
    record = orchestrator._plan_record(plan)
    replay = journal.Replay(plan=record)
    restored = orchestrator.plan_from_journal(replay)
    assert restored.cross_tenant
    assert restored.source_client_id == SOURCE.client_id
    assert restored.target_client_id == TARGET.client_id
    for secret in (SOURCE.client_secret, TARGET.client_secret):
        assert secret not in repr(record)


@pytest.mark.parametrize("target_tenant", [SOURCE_TENANT.upper(), TARGET_TENANT.upper()])
def test_qualified_plans_are_canonical(target_tenant):
    plan = make_plan(source_tenant_id=SOURCE_TENANT.upper(), target_tenant_id=target_tenant)
    assert plan.source_tenant_id == SOURCE_TENANT
    assert plan.target_tenant_id == target_tenant.lower()


def test_incomplete_plan_identity_is_rejected():
    with pytest.raises(ValueError, match="both source and target"):
        make_plan(target_tenant_id="")


@pytest.mark.parametrize("force_rebuild", [False, True])
def test_reassignment_only_types_are_reported_when_reconstruction_is_required(force_rebuild):
    assessment = assess_workspace(ITEMS, force_rebuild=force_rebuild)
    if force_rebuild:
        assert assessment.strategy is Strategy.REBUILD
        assert {item.type for item in assessment.unsupported} == {"Dashboard", "PaginatedReport"}
        assert assessment.migrated_total == 2
    else:
        assert assessment.strategy is Strategy.REASSIGN
        assert not assessment.unsupported


def test_build_plan_rejects_cross_tenant_reassignment_before_service_calls():
    with pytest.raises(ValueError, match="reassignment"):
        orchestrator.build_plan(
            None, target_client=object(), capacity_id="target-cap", source_workspace_id="source-ws",
            source_tenant_id=SOURCE_TENANT, target_tenant_id=TARGET_TENANT, strategy=Strategy.REASSIGN,
        )


@pytest.mark.parametrize("inputs", [
    {"target_client": object()},
    {"source_tenant_id": SOURCE_TENANT, "target_tenant_id": TARGET_TENANT},
    {"target_client": object(), "source_tenant_id": SOURCE_TENANT},
])
def test_build_plan_never_guesses_a_missing_tenant_or_destination_client(inputs):
    with pytest.raises(ValueError, match="both source and target"):
        orchestrator.build_plan(None, capacity_id="target-cap", source_workspace_id="source-ws", **inputs)


def test_token_providers_keep_tenant_authorities_and_audiences_separate(monkeypatch):
    applications = []

    class FakeMsal:
        def __init__(self, *, client_id, client_credential, authority):
            self.client_id = client_id
            self.authority = authority
            self.scopes = []
            applications.append(self)

        def acquire_token_for_client(self, *, scopes):
            self.scopes.append(scopes[0])
            return {"access_token": jwt({
                "tid": self.authority.rsplit("/", 1)[1], "aud": scopes[0], "appid": self.client_id,
            })}

    monkeypatch.setattr(auth.msal, "ConfidentialClientApplication", FakeMsal)
    source = TokenProvider(SOURCE)
    target = TokenProvider(TARGET)
    assert source.tenant_id() == SOURCE_TENANT
    assert target.tenant_id() == TARGET_TENANT
    assert source.sql_token() != target.sql_token()
    assert len(applications) == 2
    assert applications[0].authority.endswith(SOURCE_TENANT)
    assert applications[1].authority.endswith(TARGET_TENANT)
    assert applications[0].scopes == applications[1].scopes == [SCOPE_FABRIC, SCOPE_SQL]


@pytest.mark.parametrize(("claims", "configured_tenant", "expected"), [
    ({"tid": SOURCE_TENANT.upper()}, "source.onmicrosoft.com", SOURCE_TENANT),
    ({}, SOURCE_TENANT.upper(), SOURCE_TENANT),
])
def test_tenant_identity_uses_claim_or_authenticated_explicit_directory_guid(
    monkeypatch, claims, configured_tenant, expected,
):
    tokens = TokenProvider(ServicePrincipal(configured_tenant, "app", "secret"))
    monkeypatch.setattr(tokens, "fabric_token", lambda: jwt(claims))
    assert tokens.tenant_id() == expected


@pytest.mark.parametrize("claims", [{}, {"tid": "not-a-guid"}, {"tid": ["invalid"]}])
def test_unknown_tenant_identity_requires_explicit_correction(monkeypatch, claims):
    tokens = TokenProvider(ServicePrincipal("source.onmicrosoft.com", "app", "secret"))
    monkeypatch.setattr(tokens, "fabric_token", lambda: jwt(claims))
    with pytest.raises(AuthError, match="directory tenant GUID"):
        tokens.tenant_id()


@pytest.mark.parametrize("payload", [[], None, "text", 123])
def test_non_object_token_payload_has_no_claims(payload):
    assert auth.token_claim(jwt(payload), "tid") == ""
