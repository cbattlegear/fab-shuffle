from __future__ import annotations

import httpx
import pytest

from fabshuffle.auth import ServicePrincipal, TokenProvider
from fabshuffle.fabric.client import FabricApiError, FabricClient, OperationFailed


class StubTokens(TokenProvider):
    """Token provider that never talks to Entra."""

    def __init__(self) -> None:
        self.principal = ServicePrincipal("tenant", "client", "secret")

    def token(self, scope: str) -> str:
        return "stub-token"


def make_client(handler) -> FabricClient:
    return FabricClient(StubTokens(), transport=httpx.MockTransport(handler))


def test_get_sends_bearer_token():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers["Authorization"]
        return httpx.Response(200, json={"id": "abc"})

    with make_client(handler) as client:
        assert client.get("workspaces/abc") == {"id": "abc"}
    assert seen["auth"] == "Bearer stub-token"


def test_paged_follows_continuation_token():
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        token = request.url.params.get("continuationToken")
        calls.append(token or "first")
        if token is None:
            return httpx.Response(200, json={"value": [{"id": 1}], "continuationToken": "t1"})
        return httpx.Response(200, json={"value": [{"id": 2}]})

    with make_client(handler) as client:
        assert client.list_all("workspaces") == [{"id": 1}, {"id": 2}]
    assert calls == ["first", "t1"]


def test_paged_follows_continuation_uri():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/next"):
            return httpx.Response(200, json={"data": [{"id": 2}]})
        return httpx.Response(
            200,
            json={
                "data": [{"id": 1}],
                "continuationUri": "https://api.fabric.microsoft.com/v1/next",
            },
        )

    with make_client(handler) as client:
        assert client.list_all("tables", value_key="data") == [{"id": 1}, {"id": 2}]


def test_post_resolves_long_running_operation():
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/lakehouses"):
            return httpx.Response(
                202,
                headers={"x-ms-operation-id": "op-1", "Retry-After": "0"},
            )
        if path.endswith("/operations/op-1"):
            return httpx.Response(200, json={"status": "Succeeded"})
        if path.endswith("/operations/op-1/result"):
            return httpx.Response(200, json={"id": "new-lakehouse"})
        raise AssertionError(f"unexpected path {path}")

    with make_client(handler) as client:
        assert client.post("workspaces/w/lakehouses", json={}) == {"id": "new-lakehouse"}


def test_failed_operation_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/warehouses"):
            return httpx.Response(202, headers={"x-ms-operation-id": "op-2", "Retry-After": "0"})
        return httpx.Response(200, json={"status": "Failed", "error": {"message": "nope"}})

    with make_client(handler) as client:
        with pytest.raises(OperationFailed):
            client.post("workspaces/w/warehouses", json={})


def test_retries_on_throttling(monkeypatch):
    monkeypatch.setattr("fabshuffle.fabric.client.time.sleep", lambda _: None)
    attempts = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        if attempts["count"] < 3:
            return httpx.Response(429, headers={"Retry-After": "0"}, json={"errorCode": "RequestBlocked"})
        return httpx.Response(200, json={"ok": True})

    with make_client(handler) as client:
        assert client.get("capacities") == {"ok": True}
    assert attempts["count"] == 3


def test_error_response_raises_with_status():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="forbidden")

    with make_client(handler) as client:
        with pytest.raises(FabricApiError) as error:
            client.get("workspaces")
    assert error.value.status_code == 403
