from __future__ import annotations

import httpx
import pytest

from fabshuffle.auth import ServicePrincipal, TokenProvider
from fabshuffle.fabric.client import (
    SETTINGS,
    FabricApiError,
    FabricClient,
    FabricError,
    FabricTransportError,
    OperationFailed,
)


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


@pytest.mark.parametrize("method", ["get", "HEAD", "OPTIONS"])
def test_safe_read_retries_transport_failures_and_recovers(method, monkeypatch):
    delays = []
    calls = []
    monkeypatch.setattr(SETTINGS, "max_retries", 3)
    monkeypatch.setattr("fabshuffle.fabric.client.time.sleep", delays.append)

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            raise httpx.ConnectError("connection refused", request=request)
        if len(calls) == 2:
            raise httpx.ReadTimeout("lost response", request=request)
        return httpx.Response(200, json={"ok": True})

    with make_client(handler) as client:
        assert client.request(method, "workspaces").status_code == 200
    assert len(calls) == 3
    assert delays == [2.0, 4.0]


@pytest.mark.parametrize(
    "error_type",
    [httpx.ConnectError, httpx.ReadTimeout, httpx.ReadError, httpx.RemoteProtocolError],
)
def test_exhausted_transport_failures_are_domain_errors(error_type, monkeypatch):
    attempts = []
    delays = []
    monkeypatch.setattr(SETTINGS, "max_retries", 2)
    monkeypatch.setattr("fabshuffle.fabric.client.time.sleep", delays.append)

    def handler(request):
        error = error_type("network detail", request=request)
        attempts.append(error)
        raise error

    with make_client(handler) as client, pytest.raises(FabricError) as caught:
        client.get("workspaces")
    error = caught.value
    assert isinstance(error, FabricTransportError)
    assert error.__cause__ is attempts[-1]
    assert error.attempts == len(attempts) == 2
    assert error.method == "GET"
    assert error.url.endswith("/workspaces")
    assert error_type.__name__ in str(error)
    assert "network detail" in str(error)
    assert not error.outcome_unknown
    assert delays == [2.0]


@pytest.mark.parametrize("method", ["POST", "PATCH", "PUT", "DELETE"])
@pytest.mark.parametrize("error_type", [httpx.ReadTimeout, httpx.ConnectError])
def test_uncertain_mutations_are_wrapped_but_never_replayed(method, error_type, monkeypatch):
    calls = []
    delays = []
    monkeypatch.setattr("fabshuffle.fabric.client.time.sleep", delays.append)

    def handler(request):
        calls.append(request)
        raise error_type("response lost", request=request)

    with make_client(handler) as client, pytest.raises(FabricTransportError) as caught:
        client.request(method, "items")
    assert caught.value.outcome_unknown
    assert caught.value.attempts == len(calls) == 1
    assert "Check Fabric before retrying" in str(caught.value)
    assert delays == []


@pytest.mark.parametrize("method", ["POST", "PATCH", "PUT", "DELETE"])
@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_mutation_error_responses_are_not_blindly_replayed(method, status, monkeypatch):
    calls = []
    delays = []
    body = '{"errorCode":"ServiceBusy","message":"capacity unavailable"}'
    monkeypatch.setattr("fabshuffle.fabric.client.time.sleep", delays.append)

    def handler(request):
        calls.append(request)
        return httpx.Response(status, text=body)

    with make_client(handler) as client, pytest.raises(FabricApiError) as caught:
        client.request(method, "items")
    assert len(calls) == 1
    assert caught.value.body == body
    assert caught.value.error_code == "ServiceBusy"
    assert caught.value.detail == "capacity unavailable"
    assert delays == []


@pytest.mark.parametrize("method", ["POST", "PATCH", "PUT", "DELETE"])
def test_throttled_mutation_retries_with_retry_after(method, monkeypatch):
    calls = []
    delays = []
    monkeypatch.setattr("fabshuffle.fabric.client.time.sleep", delays.append)

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(
                429, headers={"Retry-After": "7"},
                json={"errorCode": "RequestBlocked", "message": "wait for quota"},
            )
        return httpx.Response(200, json={"id": "created"})

    with make_client(handler) as client:
        assert client.request(method, "items").json() == {"id": "created"}
    assert len(calls) == 2
    assert delays == [7.0]


def test_capacity_throttling_without_retry_after_uses_bounded_backoff(monkeypatch):
    calls = []
    delays = []
    monkeypatch.setattr(SETTINGS, "max_retries", 3)
    monkeypatch.setattr("fabshuffle.fabric.client.time.sleep", delays.append)

    def handler(request):
        calls.append(request)
        return httpx.Response(
            429, json={"errorCode": "CapacityLimitExceeded", "message": "Try again later."},
        )

    with make_client(handler) as client, pytest.raises(FabricApiError) as caught:
        client.post("items")
    assert len(calls) == 3
    assert delays == [2.0, 4.0]
    assert caught.value.error_code == "CapacityLimitExceeded"
    assert caught.value.detail == "Try again later."


def test_throttled_post_does_not_retry_subsequent_transport_loss(monkeypatch):
    calls = []
    delays = []
    monkeypatch.setattr("fabshuffle.fabric.client.time.sleep", delays.append)

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "4"})
        raise httpx.ReadTimeout("lost write response", request=request)

    with make_client(handler) as client, pytest.raises(FabricTransportError) as caught:
        client.post("items")
    assert len(calls) == caught.value.attempts == 2
    assert caught.value.outcome_unknown
    assert delays == [4.0]


def test_status_and_transport_retries_share_one_budget(monkeypatch):
    calls = []
    delays = []
    monkeypatch.setattr(SETTINGS, "max_retries", 3)
    monkeypatch.setattr("fabshuffle.fabric.client.time.sleep", delays.append)

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(503, headers={"Retry-After": "1"}, text="busy")
        raise httpx.ReadTimeout("still unreadable", request=request)

    with make_client(handler) as client, pytest.raises(FabricTransportError) as caught:
        client.get("operations/op-1")
    assert caught.value.attempts == len(calls) == 3
    assert delays == [1.0, 4.0]


def test_non_transport_request_error_is_wrapped_without_retries(monkeypatch):
    delays = []
    monkeypatch.setattr("fabshuffle.fabric.client.time.sleep", delays.append)

    def handler(request):
        raise httpx.TooManyRedirects("redirect loop", request=request)

    with make_client(handler) as client, pytest.raises(FabricTransportError, match="redirect loop"):
        client.get("workspaces")
    assert delays == []


def test_zero_retry_budget_still_attempts_once(monkeypatch):
    monkeypatch.setattr(SETTINGS, "max_retries", 0)
    with make_client(lambda _: httpx.Response(200, json={"id": "one"})) as client:
        assert client.get("items/one") == {"id": "one"}


def test_lro_polling_retries_reads_without_replaying_creation(monkeypatch):
    calls = []
    monkeypatch.setattr("fabshuffle.fabric.client.time.sleep", lambda _: None)

    def handler(request):
        calls.append(request.method)
        if request.method == "POST":
            return httpx.Response(202, headers={"x-ms-operation-id": "op"})
        if len(calls) == 2:
            raise httpx.ReadTimeout("poll unavailable", request=request)
        if request.url.path.endswith("/result"):
            return httpx.Response(200, json={"id": "created"})
        return httpx.Response(200, json={"status": "Succeeded"})

    with make_client(handler) as client:
        assert client.post("items") == {"id": "created"}
    assert calls == ["POST", "GET", "GET", "GET"]
