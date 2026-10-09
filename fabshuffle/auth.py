"""Explicit application identities for every backend Fab Shuffle talks to."""

from __future__ import annotations

import base64
import binascii
import json
import os
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from uuid import UUID

import msal
from azure.core.exceptions import AzureError
from azure.identity import ManagedIdentityCredential

from fabshuffle.config import (
    AUTHORITY_TEMPLATE,
    SCOPE_FABRIC,
    SCOPE_KUSTO,
    SCOPE_POWERBI,
    SCOPE_SQL,
    SCOPE_STORAGE,
)


class AuthError(RuntimeError):
    """Raised when a token cannot be acquired for the configured identity."""


@dataclass(frozen=True, slots=True)
class ServicePrincipal:
    tenant_id: str
    client_id: str
    client_secret: str = field(repr=False)

    def redacted(self) -> dict[str, str]:
        return {"tenant_id": self.tenant_id, "client_id": self.client_id}


@dataclass(frozen=True, slots=True)
class ManagedIdentity:
    """An explicitly selected user-assigned identity, never a credential chain."""

    tenant_id: str
    client_id: str

    def __post_init__(self) -> None:
        for name in ("tenant_id", "client_id"):
            try:
                object.__setattr__(self, name, str(UUID(getattr(self, name))))
            except ValueError as error:
                raise AuthError(f"Managed identity {name} must be a directory GUID.") from error

    def redacted(self) -> dict[str, str]:
        return {"tenant_id": self.tenant_id, "client_id": self.client_id}


AuthPrincipal = ServicePrincipal | ManagedIdentity


def _require_managed_identity_runtime(environ: Mapping[str, str]) -> None:
    # ACA supplies these values. Never fall through to IMDS, CLI or developer credentials.
    # https://learn.microsoft.com/azure/container-apps/managed-identity
    if not environ.get("IDENTITY_ENDPOINT") or not environ.get("IDENTITY_HEADER"):
        raise AuthError(
            "Managed identity requires the Azure Container Apps identity endpoint. "
            "Attach the configured user-assigned identity to this app or job and restart it."
        )
    if environ.get("IDENTITY_SERVER_THUMBPRINT"):
        raise AuthError("This managed identity configuration requires an Azure Container Apps runtime.")


def managed_identity_principal(environ: Mapping[str, str] | None = None) -> ManagedIdentity | None:
    """Read deployment configuration, not request data; this does not authorize an operator."""
    environment = os.environ if environ is None else environ
    tenant = environment.get("FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID", "").strip()
    client = environment.get("FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID", "").strip()
    if not tenant and not client:
        return None
    if not tenant or not client:
        raise AuthError(
            "Set both FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID and "
            "FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID to enable managed identity."
        )
    principal = ManagedIdentity(tenant, client)
    _require_managed_identity_runtime(environment)
    return principal


class TokenProvider:
    """Caches one explicit credential and hands out per-resource tokens.

    MSAL and Azure Identity own their in-memory per-resource caches and expiry refresh.
    The extra lock keeps concurrent migration steps from racing into duplicate requests.

    The MSAL client is built lazily: its constructor performs tenant discovery over the
    network, and a bad tenant id must surface as an :class:`AuthError` at sign-in rather
    than as an unhandled exception while constructing the provider.
    """

    def __init__(self, principal: AuthPrincipal) -> None:
        self.principal = principal
        self._lock = threading.Lock()
        self._client: msal.ConfidentialClientApplication | None = None
        self._managed_credential: ManagedIdentityCredential | None = None
        self._managed_object_id: str | None = None

    def _app(self) -> msal.ConfidentialClientApplication:
        if not isinstance(self.principal, ServicePrincipal):
            raise AuthError("Managed identities cannot use client-secret authentication.")
        if self._client is None:
            try:
                self._client = msal.ConfidentialClientApplication(
                    client_id=self.principal.client_id,
                    client_credential=self.principal.client_secret,
                    authority=AUTHORITY_TEMPLATE.format(tenant_id=self.principal.tenant_id),
                )
            except ValueError as error:
                raise AuthError(f"Could not reach the Entra tenant: {error}") from error
        return self._client

    def _managed_token(self, scope: str, principal: ManagedIdentity) -> str:
        try:
            if self._managed_credential is None:
                _require_managed_identity_runtime(os.environ)
                self._managed_credential = ManagedIdentityCredential(client_id=principal.client_id)
            result = self._managed_credential.get_token(scope)
        except (AzureError, ValueError) as error:
            raise AuthError(f"Could not acquire a managed identity token for {scope}. {error}") from error
        if result.expires_on <= time.time():
            raise AuthError(f"The managed identity endpoint returned an expired token for {scope}.")
        # ManagedIdentityCredential ignores tenant_id: enforce our deployment/BCDR binding
        # against the acquired token, never by silently substituting its actual tenant.
        # https://learn.microsoft.com/python/api/azure-identity/azure.identity.managedidentitycredential
        # https://learn.microsoft.com/entra/identity-platform/access-token-claims-reference
        try:
            tenant = str(UUID(token_claim(result.token, "tid")))
            client = str(UUID(token_claim(result.token, "appid") or token_claim(result.token, "azp")))
            object_id = str(UUID(token_claim(result.token, "oid")))
        except ValueError as error:
            raise AuthError(
                "The managed identity token omitted a valid tenant, application or object ID; "
                "check the configured user-assigned identity before retrying."
            ) from error
        if tenant != principal.tenant_id or client != principal.client_id:
            raise AuthError(
                "The managed identity token does not match the configured tenant and client ID. "
                "Correct the deployment identity; existing recovery records cannot switch identities."
            )
        if self._managed_object_id is not None and self._managed_object_id != object_id:
            raise AuthError(
                "The managed identity object ID changed; restart with the original recovery identity."
            )
        self._managed_object_id = object_id
        return result.token

    def token(self, scope: str) -> str:
        self.assert_active()
        with self._lock:
            if isinstance(self.principal, ManagedIdentity):
                result = {"access_token": self._managed_token(scope, self.principal)}
            else:
                result = self._app().acquire_token_for_client(scopes=[scope])
        self.assert_active()
        if not isinstance(result, dict) or "access_token" not in result:
            description = ""
            if isinstance(result, dict):
                description = result.get("error_description") or result.get("error") or ""
            raise AuthError(f"Could not acquire a token for {scope}. {description}".strip())
        return str(result["access_token"])

    def assert_active(self) -> None:
        """Recovery wrappers override this to fence new calls after deployment lease loss.

        This cannot cancel or roll back a request already accepted by a remote service.
        """

    def invalidate(self) -> None:
        """Discard local resource caches after an explicit authentication rejection.

        Azure Identity exposes no per-scope force refresh. Recreate the explicit credential,
        retaining its verified object ID. The Azure endpoint can still return a cached token;
        this is not a guarantee of revocation propagation or permission propagation.
        https://learn.microsoft.com/azure/container-apps/managed-identity
        """
        self.assert_active()
        with self._lock:
            if self._managed_credential is not None:
                self._managed_credential.close()
                self._managed_credential = None
            self._client = None

    def fabric_token(self) -> str:
        return self.token(SCOPE_FABRIC)

    def storage_token(self) -> str:
        return self.token(SCOPE_STORAGE)

    def kusto_token(self) -> str:
        return self.token(SCOPE_KUSTO)

    def sql_token(self) -> str:
        return self.token(SCOPE_SQL)

    def powerbi_token(self) -> str:
        return self.token(SCOPE_POWERBI)

    def verify(self) -> None:
        """Fail fast at login time rather than midway through a migration."""
        self.fabric_token()

    def tenant_id(self) -> str:
        """Resolve the tenant for this provider, not an ambient user's signed-in tenant.

        A directory domain and its GUID can identify the same tenant. Prefer the ``tid``
        in our own acquired token; an explicit directory GUID also works if that claim
        is absent. Neither value grants access: Fabric still authorizes every request.
        https://learn.microsoft.com/entra/identity-platform/access-token-claims-reference
        """
        tenant = token_claim(self.fabric_token(), "tid") or self.principal.tenant_id
        try:
            return str(UUID(tenant))
        except ValueError as error:
            raise AuthError(
                "Could not identify the authenticated tenant. Sign in using its directory "
                "tenant GUID rather than a domain name."
            ) from error

    def object_id(self) -> str:
        """This service principal's object id in the tenant, from its own token.

        Fabric's role assignment API identifies a principal by object id, but a service
        principal is configured with its *application* id, and the two are different GUIDs.
        Rather than ask Microsoft Graph for the mapping, which needs a directory permission
        this application otherwise has no use for, it is read from the ``oid`` claim of a
        token we already hold. For an app-only token that claim is the service principal.
        """
        return token_claim(self.fabric_token(), "oid")


def token_claim(token: str, name: str) -> str:
    """Read one claim from a JWT without validating it.

    The token is one this process just obtained for itself, so there is nothing to verify
    against. Claims identify our configured tenant or fill in an operator script; they do
    not authorize requests or accept caller-supplied bearer tokens.
    """
    try:
        payload = token.split(".")[1]
    except IndexError:
        return ""
    # JWT uses base64url without padding.
    padded = payload + "=" * (-len(payload) % 4)
    try:
        claims = json.loads(base64.urlsafe_b64decode(padded))
    except (ValueError, binascii.Error):
        return ""
    if not isinstance(claims, dict):
        return ""
    value = claims.get(name)
    return str(value) if value else ""


def sql_access_token_struct(token: str) -> bytes:
    """Pack an access token the way ODBC's SQL_COPT_SS_ACCESS_TOKEN attribute expects."""
    encoded = token.encode("utf-16-le")
    return len(encoded).to_bytes(4, byteorder="little") + encoded


def wait_backoff(attempt: int, base_seconds: float = 2.0, cap_seconds: float = 60.0) -> None:
    time.sleep(min(cap_seconds, base_seconds * (2 ** max(0, attempt - 1))))


__all__ = [
    "SCOPE_FABRIC",
    "SCOPE_KUSTO",
    "SCOPE_POWERBI",
    "SCOPE_SQL",
    "SCOPE_STORAGE",
    "AuthError",
    "AuthPrincipal",
    "ManagedIdentity",
    "ServicePrincipal",
    "TokenProvider",
    "managed_identity_principal",
    "sql_access_token_struct",
    "token_claim",
    "wait_backoff",
]
