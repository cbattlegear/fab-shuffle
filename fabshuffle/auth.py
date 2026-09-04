"""Service principal authentication for every backend Fab Shuffle talks to."""

from __future__ import annotations

import base64
import binascii
import json
import threading
import time
from dataclasses import dataclass

import msal

from fabshuffle.config import (
    AUTHORITY_TEMPLATE,
    SCOPE_FABRIC,
    SCOPE_KUSTO,
    SCOPE_POWERBI,
    SCOPE_SQL,
    SCOPE_STORAGE,
)


class AuthError(RuntimeError):
    """Raised when a token cannot be acquired for the service principal."""


@dataclass(frozen=True, slots=True)
class ServicePrincipal:
    tenant_id: str
    client_id: str
    client_secret: str

    def redacted(self) -> dict[str, str]:
        return {"tenant_id": self.tenant_id, "client_id": self.client_id}


class TokenProvider:
    """Caches one MSAL confidential client and hands out per-resource tokens.

    MSAL keeps its own in-memory cache keyed by scope, so ``acquire_token_for_client``
    is cheap on repeat calls. The extra lock keeps concurrent migration steps from
    racing each other into duplicate token requests.

    The MSAL client is built lazily: its constructor performs tenant discovery over the
    network, and a bad tenant id must surface as an :class:`AuthError` at sign-in rather
    than as an unhandled exception while constructing the provider.
    """

    def __init__(self, principal: ServicePrincipal) -> None:
        self.principal = principal
        self._lock = threading.Lock()
        self._client: msal.ConfidentialClientApplication | None = None

    def _app(self) -> msal.ConfidentialClientApplication:
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

    def token(self, scope: str) -> str:
        with self._lock:
            result = self._app().acquire_token_for_client(scopes=[scope])
        if not isinstance(result, dict) or "access_token" not in result:
            description = ""
            if isinstance(result, dict):
                description = result.get("error_description") or result.get("error") or ""
            raise AuthError(f"Could not acquire a token for {scope}. {description}".strip())
        return str(result["access_token"])

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
    against and nothing is trusted on the strength of it: the value is only used to fill in
    a script for the operator to read.
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
    "ServicePrincipal",
    "TokenProvider",
    "sql_access_token_struct",
    "token_claim",
    "wait_backoff",
]
