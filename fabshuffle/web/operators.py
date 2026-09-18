"""Operator authorization behind the Azure Container Apps EasyAuth sidecar.

These headers are trusted only in explicitly configured Container Apps deployments.
EasyAuth validates tokens and strips externally supplied identity headers:
https://learn.microsoft.com/azure/container-apps/authentication#access-user-claims-in-application-code
"""

from __future__ import annotations

import base64
import binascii
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit
from uuid import UUID

from fastapi import HTTPException, Request
from starlette.datastructures import MutableHeaders
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

REQUEST_HEADER = "X-Fab-Shuffle-Request"
_OBJECT_CLAIMS = {"oid", "http://schemas.microsoft.com/identity/claims/objectidentifier"}
_TENANT_CLAIMS = {"tid", "http://schemas.microsoft.com/identity/claims/tenantid"}


@dataclass(frozen=True)
class Operator:
    tenant_id: str
    object_id: str

    @property
    def key(self) -> str:
        return f"{self.tenant_id}/{self.object_id}"


@dataclass(frozen=True)
class OperatorPolicy:
    enabled: bool = False
    tenant_id: str = ""
    allowed_objects: frozenset[str] = frozenset()
    ready: bool = False

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> OperatorPolicy:
        env = os.environ if environment is None else environment
        flag = env.get("FAB_SHUFFLE_EASYAUTH_ENABLED", "false").casefold()
        if flag not in {"true", "false"}:
            raise ValueError("FAB_SHUFFLE_EASYAUTH_ENABLED must be true or false.")
        if flag == "false":
            return cls()
        ready = env.get("FAB_SHUFFLE_EASYAUTH_READY", "false").casefold()
        if ready not in {"true", "false"}:
            raise ValueError("FAB_SHUFFLE_EASYAUTH_READY must be true or false.")
        if not env.get("CONTAINER_APP_NAME"):
            raise ValueError("EasyAuth header trust requires an Azure Container Apps deployment.")
        try:
            tenant = str(UUID(env.get("FAB_SHUFFLE_EASYAUTH_TENANT_ID", "")))
            values = env.get("FAB_SHUFFLE_EASYAUTH_ALLOWED_OBJECT_IDS", "").split(",")
            allowed = frozenset(str(UUID(value.strip())) for value in values)
        except ValueError as error:
            raise ValueError(
                "Configure an EasyAuth tenant GUID and a nonempty comma-separated "
                "operator object-ID allowlist."
            ) from error
        return cls(True, tenant, allowed, ready == "true")

    def authorize(self, request: Request) -> Operator | None:
        if not self.enabled:
            return None
        if not self.ready:
            raise HTTPException(
                status_code=503,
                detail="Finish the Azure EasyAuth deployment before activating operator access.",
            )
        values = request.headers.getlist("x-ms-client-principal")
        if len(values) != 1 or not values[0] or len(values[0]) > 32768:
            raise HTTPException(status_code=401, detail="Sign in through Azure Entra authentication.")
        try:
            document = json.loads(
                base64.b64decode(values[0], validate=True), object_pairs_hook=_unique_fields,
            )
            if not isinstance(document, dict) or document.get("auth_typ") != "aad":
                raise ValueError("The provider is not Entra.")
            claims = document.get("claims")
            if not isinstance(claims, list) or any(
                not isinstance(claim, dict)
                or not isinstance(claim.get("typ"), str)
                or not isinstance(claim.get("val"), str)
                for claim in claims
            ):
                raise ValueError("The identity claims are malformed.")
            tenant = _identity_claim(claims, _TENANT_CLAIMS)
            object_id = _identity_claim(claims, _OBJECT_CLAIMS)
            for header, expected in (
                ("x-ms-client-principal-id", object_id), ("x-ms-client-principal-idp", "aad"),
            ):
                observed = request.headers.getlist(header)
                if len(observed) > 1 or (observed and observed[0].casefold() != expected):
                    raise ValueError("The identity headers disagree.")
        except (ValueError, TypeError, UnicodeError, binascii.Error) as error:
            raise HTTPException(
                status_code=401, detail="Azure operator identity is invalid. Sign in again.",
            ) from error
        if tenant != self.tenant_id or object_id not in self.allowed_objects:
            raise HTTPException(
                status_code=403,
                detail="This Entra identity is not an authorized Fab Shuffle operator.",
            )
        return Operator(tenant, object_id)


def _unique_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate identity field.")
        result[key] = value
    return result


def _identity_claim(claims: list[dict[str, str]], names: set[str]) -> str:
    values = {str(UUID(claim["val"])) for claim in claims if claim["typ"] in names}
    if len(values) != 1:
        raise ValueError("The identity claim is missing or ambiguous.")
    return values.pop()


def _require_same_origin(request: Request) -> None:
    if request.headers.getlist(REQUEST_HEADER) != ["1"]:
        raise HTTPException(
            status_code=403,
            detail=f"Authenticated API writes require the {REQUEST_HEADER}: 1 header.",
        )
    if request.headers.get("sec-fetch-site") == "cross-site":
        raise HTTPException(status_code=403, detail="Cross-site operator requests are refused.")
    origins = request.headers.getlist("origin")
    if not origins:
        return
    try:
        origin = urlsplit(origins[0])
        host = urlsplit("//" + request.headers.get("host", ""))
        valid = (
            len(origins) == 1
            and origin.scheme == "https"
            and not origin.username and not origin.password
            and not origin.path and not origin.query and not origin.fragment
            and origin.hostname == host.hostname
            and (origin.port or 443) == (host.port or 443)
        )
    except ValueError:
        valid = False
    if not valid:
        raise HTTPException(status_code=403, detail="Cross-origin operator requests are refused.")


class OperatorMiddleware:
    def __init__(self, app: ASGIApp, policy: OperatorPolicy) -> None:
        self.app, self.policy = app, policy

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        health = request.url.path == "/api/health" and request.method in {"GET", "HEAD"}
        try:
            operator = None if health else self.policy.authorize(request)
            if self.policy.enabled and not health and request.method not in {"GET", "HEAD", "OPTIONS"}:
                _require_same_origin(request)
            scope.setdefault("state", {})["operator"] = operator
        except HTTPException as error:
            await JSONResponse(
                {"detail": error.detail}, status_code=error.status_code,
                headers={"Cache-Control": "no-store"},
            )(scope, receive, send)
            return

        async def protected_send(message: Message) -> None:
            if self.policy.enabled and not health and message["type"] == "http.response.start":
                MutableHeaders(scope=message)["Cache-Control"] = "no-store"
            await send(message)

        await self.app(scope, receive, protected_send)


def operator_key(request: Request) -> str | None:
    operator: Operator | None = getattr(request.state, "operator", None)
    return operator.key if operator is not None else None
