"""Explicit ACA identity selection, SDK resource caches and durable BCDR identity binding."""

from __future__ import annotations

import base64
import json
from io import StringIO
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

import pytest
from azure.core.pipeline.transport import HttpResponse, HttpTransport
from azure.identity import ManagedIdentityCredential

from fabshuffle import auth
from fabshuffle.bcdr import __main__ as cli
from fabshuffle.bcdr import production
from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.bootstrap import BootstrapDescriptor, BootstrapStore, CapacityAuthorization
from fabshuffle.bcdr.service import SetupRequest

TENANT = "10000000-0000-0000-0000-000000000001"
CLIENT = "10000000-0000-0000-0000-000000000002"
OBJECT = "10000000-0000-0000-0000-000000000003"
OTHER = "10000000-0000-0000-0000-000000000004"
ENV = {
    "FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID": TENANT,
    "FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID": CLIENT,
    "IDENTITY_ENDPOINT": "http://localhost:42356/msi/token",
    "IDENTITY_HEADER": "platform-header-not-an-operator-credential",
}


def jwt(claims):
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"eyJhbGciOiJub25lIn0.{payload}.test-signature"


class Response(HttpResponse):
    def __init__(self, request, data, status_code=200):
        super().__init__(request, None)
        self.status_code = status_code
        self.headers = {"content-type": "application/json"}
        self.data = data

    def body(self):
        return json.dumps(self.data).encode()

    def text(self, encoding=None):
        return self.body().decode()


class IdentityEndpoint(HttpTransport):
    """No network: exercise the installed SDK's ACA request, cache and refresh path."""

    def __init__(self):
        self.now = 1_800_000_000
        self.claims = {"tid": TENANT, "appid": CLIENT, "oid": OBJECT}
        self.calls = []
        self.error = None

    def open(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def send(self, request, **kwargs):
        self.calls.append(request)
        assert request.url.startswith(ENV["IDENTITY_ENDPOINT"])
        assert request.headers["X-IDENTITY-HEADER"] == ENV["IDENTITY_HEADER"]
        query = parse_qs(urlsplit(request.url).query)
        assert query["client_id"] == [CLIENT]
        assert query["api-version"] == ["2019-08-01"]
        if self.error:
            return Response(request, self.error, 400)
        resource = query["resource"][0]
        expires = self.now + 3600
        return Response(request, {
            "access_token": jwt({
                **self.claims, "aud": resource, "exp": expires, "sequence": len(self.calls),
            }),
            "expires_on": str(expires), "resource": resource, "token_type": "Bearer",
        })


@pytest.fixture
def endpoint(monkeypatch):
    for name in (
        "IDENTITY_ENDPOINT", "IDENTITY_HEADER", "IDENTITY_SERVER_THUMBPRINT",
        "MSI_ENDPOINT", "MSI_SECRET", "IMDS_ENDPOINT", "AZURE_CLIENT_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    for name, value in ENV.items():
        monkeypatch.setenv(name, value)
    endpoint = IdentityEndpoint()
    monkeypatch.setattr(auth.time, "time", lambda: endpoint.now)
    monkeypatch.setattr(
        auth, "ManagedIdentityCredential",
        lambda **kwargs: ManagedIdentityCredential(**kwargs, transport=endpoint, retry_total=0),
    )
    monkeypatch.setattr(
        auth.msal, "ConfidentialClientApplication",
        Mock(side_effect=AssertionError("Managed identity must not call secret authentication")),
    )
    return endpoint


def provider():
    return auth.TokenProvider(auth.ManagedIdentity(TENANT, CLIENT))


def test_deployment_helper_is_explicit_secretless_and_not_an_operator_login():
    assert auth.managed_identity_principal({}) is None
    principal = auth.managed_identity_principal(ENV)
    assert principal == auth.ManagedIdentity(TENANT, CLIENT)
    assert principal.redacted() == {"tenant_id": TENANT, "client_id": CLIENT}
    assert not hasattr(principal, "client_secret")
    assert ENV["IDENTITY_HEADER"] not in repr(principal)


@pytest.mark.parametrize("missing", list(ENV))
def test_partial_configuration_or_missing_runtime_fails_closed(missing):
    with pytest.raises(auth.AuthError):
        auth.managed_identity_principal({name: value for name, value in ENV.items() if name != missing})


@pytest.mark.parametrize("name", [
    "FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID", "FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID",
])
def test_mi_requires_guids_not_domains_or_ambient_identity(name):
    with pytest.raises(auth.AuthError, match="GUID"):
        auth.managed_identity_principal({**ENV, name: "tenant.onmicrosoft.com"})


def test_unsupported_identity_runtime_is_not_selected():
    with pytest.raises(auth.AuthError, match="Container Apps"):
        auth.managed_identity_principal({**ENV, "IDENTITY_SERVER_THUMBPRINT": "service-fabric"})


def test_direct_provider_does_not_probe_imds_or_use_environment_sp(monkeypatch):
    monkeypatch.delenv("IDENTITY_ENDPOINT", raising=False)
    monkeypatch.setenv("AZURE_CLIENT_ID", OTHER)
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "must-not-be-used")
    credential = Mock(side_effect=AssertionError("No SDK or IMDS without ACA endpoint"))
    monkeypatch.setattr(auth, "ManagedIdentityCredential", credential)
    with pytest.raises(auth.AuthError, match="Container Apps"):
        provider().fabric_token()
    credential.assert_not_called()


def test_sdk_cache_refresh_scope_isolation_and_identity_claims(endpoint, monkeypatch):
    monkeypatch.setenv("AZURE_CLIENT_ID", OTHER)
    tokens = provider()
    first = tokens.fabric_token()
    assert tokens.fabric_token() == first
    assert tokens.tenant_id() == TENANT
    assert tokens.object_id() == OBJECT
    assert len(endpoint.calls) == 1
    for method, scope in (
        (tokens.storage_token, auth.SCOPE_STORAGE), (tokens.sql_token, auth.SCOPE_SQL),
        (tokens.powerbi_token, auth.SCOPE_POWERBI), (tokens.kusto_token, auth.SCOPE_KUSTO),
        (lambda: tokens.token("https://management.azure.com/.default"),
         "https://management.azure.com/.default"),
        (lambda: tokens.token("https://cosmos.azure.com/.default"),
         "https://cosmos.azure.com/.default"),
    ):
        value = method()
        assert auth.token_claim(value, "aud") == scope.removesuffix("/.default")
        assert method() == value
    assert len(endpoint.calls) == 7
    endpoint.now += 3601
    assert tokens.fabric_token() != first
    assert len(endpoint.calls) == 8


@pytest.mark.parametrize("claim,value", [
    ("tid", OTHER), ("appid", OTHER), ("tid", ""), ("appid", ""), ("oid", ""),
    ("oid", "not-an-object-guid"),
])
def test_wrong_or_unverifiable_identity_never_leaves_provider(endpoint, claim, value):
    endpoint.claims[claim] = value
    with pytest.raises(auth.AuthError, match="managed identity token"):
        provider().fabric_token()


def test_v2_application_claim_is_supported(endpoint):
    endpoint.claims.pop("appid")
    endpoint.claims["azp"] = CLIENT
    assert provider().object_id() == OBJECT


def test_invalidate_reacquires_all_scopes_without_changing_pinned_identity(endpoint):
    tokens = provider()
    first = tokens.storage_token()
    tokens.fabric_token()
    assert len(endpoint.calls) == 2
    tokens.invalidate()
    assert tokens.storage_token() != first
    assert len(endpoint.calls) == 3
    endpoint.claims["oid"] = OTHER
    with pytest.raises(auth.AuthError, match="object ID changed"):
        tokens.fabric_token()


def test_object_id_cannot_change_between_audiences(endpoint):
    tokens = provider()
    assert tokens.object_id() == OBJECT
    endpoint.claims["oid"] = OTHER
    with pytest.raises(auth.AuthError, match="object ID changed"):
        tokens.sql_token()


def test_identity_service_error_is_not_replaced_by_a_guess(endpoint):
    endpoint.error = {"statusCode": 400, "message": "Identity has not been assigned"}
    with pytest.raises(auth.AuthError, match="Identity has not been assigned"):
        provider().fabric_token()


def test_expired_sdk_token_is_rejected(endpoint, monkeypatch):
    from azure.core.credentials import AccessToken

    monkeypatch.setattr(
        auth, "ManagedIdentityCredential",
        Mock(return_value=Mock(get_token=Mock(return_value=AccessToken("expired", endpoint.now - 1)))),
    )
    with pytest.raises(auth.AuthError, match="expired token"):
        provider().fabric_token()


def test_secret_principal_remains_lazy_and_uses_only_msal(monkeypatch):
    app = Mock()
    app.acquire_token_for_client.return_value = {"access_token": "secret-mode-token"}
    constructor = Mock(return_value=app)
    monkeypatch.setattr(auth.msal, "ConfidentialClientApplication", constructor)
    monkeypatch.setattr(auth, "ManagedIdentityCredential", Mock(side_effect=AssertionError("No MI fallback")))
    tokens = auth.TokenProvider(auth.ServicePrincipal("tenant", "client", "secret"))
    constructor.assert_not_called()
    assert tokens.fabric_token() == "secret-mode-token"
    assert constructor.call_args.kwargs["client_credential"] == "secret"
    app.acquire_token_for_client.assert_called_once_with(scopes=[auth.SCOPE_FABRIC])
    tokens.invalidate()
    assert tokens.fabric_token() == "secret-mode-token"
    assert constructor.call_count == 2


def test_cli_defaults_to_secret_even_when_deployment_mi_is_available():
    with pytest.raises(ValueError, match="FAB_SHUFFLE_BCDR_TENANT_ID"):
        cli.credential_provider(ENV)
    selected = cli.credential_provider({**ENV, "FAB_SHUFFLE_BCDR_AUTH_MODE": "managed_identity"})
    assert selected.principal == auth.ManagedIdentity(TENANT, CLIENT)


@pytest.mark.parametrize("field", [
    "FAB_SHUFFLE_BCDR_TENANT_ID", "FAB_SHUFFLE_BCDR_CLIENT_ID",
    "FAB_SHUFFLE_BCDR_CLIENT_SECRET", "FAB_SHUFFLE_BCDR_CLIENT_SECRET_FILE",
])
def test_cli_mi_rejects_mixed_secret_configuration_before_reading_files(field):
    with pytest.raises(ValueError, match="cannot include BCDR"):
        cli.credential_provider({
            **ENV, "FAB_SHUFFLE_BCDR_AUTH_MODE": "managed_identity", field: "/not-read/secret",
        })


def test_cli_mode_does_not_default_on_typos_or_missing_mi():
    with pytest.raises(ValueError, match="AUTH_MODE"):
        cli.credential_provider({"FAB_SHUFFLE_BCDR_AUTH_MODE": "auto"})
    with pytest.raises(auth.AuthError, match="FAB_SHUFFLE_MANAGED_IDENTITY"):
        cli.credential_provider({"FAB_SHUFFLE_BCDR_AUTH_MODE": "managed_identity"})


def test_cli_status_uses_deployment_identity_without_secrets(endpoint, monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("FAB_SHUFFLE_BCDR_AUTH_MODE", "managed_identity")
    result = Mock()
    result.exit_code = 0
    result.model_dump_json.return_value = '{"status": "ready"}'
    service = Mock()
    service.status.return_value = result

    def create_service(path, *, target_tokens, source_tokens):
        assert target_tokens.principal == auth.ManagedIdentity(TENANT, CLIENT)
        assert target_tokens.object_id() == OBJECT
        assert source_tokens is None
        return service

    monkeypatch.setattr(cli, "create_service", create_service)
    monkeypatch.setattr("sys.stdin", StringIO(""))
    assert cli.main(["status", "--bootstrap", str(tmp_path / "bootstrap.json")]) == 0
    assert '"status": "ready"' in capsys.readouterr().out
    service.close.assert_called_once()


def test_existing_bootstrap_is_not_replaced_when_switching_to_mi(endpoint, tmp_path):
    arm = f"/subscriptions/{TENANT}/resourceGroups/dr/providers/Microsoft.Fabric/capacities/recovery"
    capacity = CapacityAuthorization(
        arm_resource_id=arm, fabric_capacity_id=OTHER,
        dedicated_recovery=True, authorized_for_suspend=True,
    )
    descriptor = BootstrapDescriptor(
        recovery_set_id=OBJECT, tenant_id=TENANT, application_id=OTHER, controller_id=OBJECT,
        capacities=(capacity,), catalog_capacity_id=arm, control_workspace_id=OTHER,
    )
    path = tmp_path / "bootstrap.json"
    BootstrapStore(path).save(descriptor, expected_revision=None)
    original = path.read_bytes()
    request = SetupRequest(
        control_workspace_id=OTHER, source_capacity_ids=[OBJECT],
        recovery_capacities=[capacity.model_dump()], catalog_capacity_id=arm,
        access_policy={
            "recovery_spn": {"tenant_id": TENANT, "object_id": OBJECT, "kind": "ServicePrincipal"},
            "owners": [],
        },
    )
    with pytest.raises(RecoveryBlocked, match="do not overwrite"):
        production.setup_recovery(request, path, target_tokens=provider())
    assert path.read_bytes() == original
