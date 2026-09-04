"""Thin, synchronous Fabric REST API client.

Everything Fab Shuffle needs from Fabric goes through :class:`FabricClient`. It owns the
three behaviours every Fabric caller has to get right:

* continuation-token paging on list endpoints,
* long running operations (``202`` + ``Location`` + ``Retry-After`` + ``/operations/{id}``),
* throttling (``429``) and transient ``5xx`` retries.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import httpx

from fabshuffle.auth import TokenProvider
from fabshuffle.config import FABRIC_API_BASE, SETTINGS

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_TERMINAL_OPERATION_STATES = frozenset({"Succeeded", "Failed", "Undefined"})


class FabricError(RuntimeError):
    """Anything that went wrong talking to Fabric.

    Creating an item can fail two ways: the request itself is rejected, or it is accepted and
    the long running operation behind it fails later. Callers that handle one item at a time
    almost always want to treat those the same, so they share a base.
    """


class FabricApiError(FabricError):
    """A Fabric REST call returned an error response."""

    def __init__(self, method: str, url: str, status_code: int, body: str) -> None:
        self.method = method
        self.url = url
        self.status_code = status_code
        self.body = body
        super().__init__(f"{method} {url} failed with HTTP {status_code}: {body}")

    @property
    def error_code(self) -> str:
        """The service's own error code, which says far more than the status does."""
        return self._from_body("errorCode")

    @property
    def detail(self) -> str:
        return self._from_body("message")

    def _from_body(self, key: str) -> str:
        try:
            parsed = json.loads(self.body)
        except (TypeError, ValueError):
            return ""
        if not isinstance(parsed, Mapping):
            return ""
        value = parsed.get(key)
        if value:
            return str(value)
        # Some responses nest the useful part one level down.
        for nested in parsed.values():
            if isinstance(nested, Mapping) and nested.get(key):
                return str(nested[key])
        return ""


class OperationFailed(FabricError):
    """A long running operation finished in a non-success state."""

    def __init__(self, operation_id: str, status: str, error: Mapping[str, Any] | None) -> None:
        self.operation_id = operation_id
        self.status = status
        self.error_code = str((error or {}).get("errorCode") or "")
        self.detail = str((error or {}).get("message") or "")
        super().__init__(
            f"Operation {operation_id} ended as {status}"
            + (f": {self.error_code} {self.detail}".rstrip() if self.error_code or self.detail else "")
        )


class OperationTimeout(FabricError):
    """A long running operation did not finish inside the configured budget."""


def _retry_after_seconds(response: httpx.Response, fallback: float) -> float:
    raw = response.headers.get("Retry-After")
    if not raw:
        return fallback
    try:
        return max(0.0, float(raw))
    except ValueError:
        return fallback


class FabricClient:
    """Synchronous client for ``https://api.fabric.microsoft.com/v1``."""

    def __init__(
        self,
        tokens: TokenProvider,
        base_url: str = FABRIC_API_BASE,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._tokens = tokens
        self.base_url = base_url.rstrip("/")
        self._http = httpx.Client(
            timeout=httpx.Timeout(SETTINGS.request_timeout_seconds),
            transport=transport,
            follow_redirects=True,
        )

    # ------------------------------------------------------------------ plumbing

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> FabricClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return f"{self.base_url}/{path.lstrip('/')}"

    def _headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._tokens.fabric_token()}",
            "Accept": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    def request(
        self,
        method: str,
        path: str,
        *,
        json: Any | None = None,
        content: bytes | None = None,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        expected: Sequence[int] | None = None,
    ) -> httpx.Response:
        """Issue a single request, retrying throttled and transient failures.

        ``content`` sends a raw body instead of JSON, which the file APIs need: they carry
        file bytes rather than a document.
        """
        url = self._url(path)
        last_response: httpx.Response | None = None

        for attempt in range(1, SETTINGS.max_retries + 1):
            response = self._http.request(
                method.upper(),
                url,
                json=json,
                content=content,
                params=params,
                headers=self._headers(headers),
            )
            last_response = response

            if response.status_code in RETRYABLE_STATUS and attempt < SETTINGS.max_retries:
                delay = _retry_after_seconds(response, fallback=min(60.0, 2.0 ** attempt))
                logger.warning(
                    "%s %s returned %s, retrying in %.1fs (attempt %s/%s)",
                    method.upper(),
                    url,
                    response.status_code,
                    delay,
                    attempt,
                    SETTINGS.max_retries,
                )
                time.sleep(delay)
                continue

            if expected is not None:
                if response.status_code in expected:
                    return response
            elif response.is_success:
                return response

            raise FabricApiError(method.upper(), url, response.status_code, response.text)

        assert last_response is not None  # pragma: no cover - loop always assigns
        raise FabricApiError(method.upper(), url, last_response.status_code, last_response.text)

    # --------------------------------------------------------------- verb helpers

    def get(self, path: str, *, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        response = self.request("GET", path, params=params)
        return self._json(response)

    def delete(self, path: str, *, params: Mapping[str, Any] | None = None) -> None:
        self.request("DELETE", path, params=params, expected=(200, 202, 204))

    def post(
        self,
        path: str,
        *,
        json: Any | None = None,
        params: Mapping[str, Any] | None = None,
        wait: bool = True,
    ) -> dict[str, Any]:
        response = self.request("POST", path, json=json, params=params, expected=(200, 201, 202))
        return self._settle(response, wait=wait)

    def patch(
        self,
        path: str,
        *,
        json: Any | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = self.request("PATCH", path, json=json, params=params, expected=(200, 201, 202))
        return self._settle(response, wait=True)

    def put(
        self,
        path: str,
        *,
        json: Any | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = self.request("PUT", path, json=json, params=params, expected=(200, 201, 202))
        return self._settle(response, wait=True)

    @staticmethod
    def _json(response: httpx.Response) -> dict[str, Any]:
        if not response.content:
            return {}
        try:
            payload = response.json()
        except ValueError:
            return {}
        return payload if isinstance(payload, dict) else {"value": payload}

    # ------------------------------------------------------------------- paging

    def paged(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        value_key: str = "value",
    ) -> Iterator[dict[str, Any]]:
        """Yield every element of a Fabric list endpoint, following continuation tokens."""
        next_path = path
        query: dict[str, Any] | None = dict(params or {})

        while True:
            payload = self.get(next_path, params=query)
            yield from payload.get(value_key) or []

            continuation_uri = payload.get("continuationUri")
            continuation_token = payload.get("continuationToken")

            if continuation_uri:
                next_path, query = continuation_uri, None
            elif continuation_token:
                next_path = path
                query = dict(params or {}) | {"continuationToken": continuation_token}
            else:
                return

    def list_all(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        value_key: str = "value",
    ) -> list[dict[str, Any]]:
        return list(self.paged(path, params=params, value_key=value_key))

    # --------------------------------------------------- long running operations

    def _settle(self, response: httpx.Response, *, wait: bool) -> dict[str, Any]:
        """Resolve a response that may be a long running operation."""
        if response.status_code != 202:
            return self._json(response)

        operation_id = response.headers.get("x-ms-operation-id")
        location = response.headers.get("Location")
        if not wait:
            return {
                "status": "Running",
                "operationId": operation_id,
                "location": location,
                "retryAfter": _retry_after_seconds(response, SETTINGS.lro_poll_seconds),
            }

        poll_seconds = _retry_after_seconds(response, SETTINGS.lro_poll_seconds)
        if operation_id:
            return self.wait_for_operation(operation_id, poll_seconds=poll_seconds)
        if location:
            return self._poll_location(location, poll_seconds=poll_seconds)
        return self._json(response)

    def wait_for_operation(self, operation_id: str, *, poll_seconds: float | None = None) -> dict[str, Any]:
        """Poll ``/operations/{id}`` until it succeeds, then return its result body."""
        delay = poll_seconds or SETTINGS.lro_poll_seconds
        deadline = time.monotonic() + SETTINGS.lro_timeout_seconds

        while True:
            state = self.get(f"operations/{operation_id}")
            status = state.get("status", "Running")

            if status == "Succeeded":
                try:
                    return self.get(f"operations/{operation_id}/result")
                except FabricApiError as error:
                    # Not every operation exposes a result document.
                    if error.status_code in (400, 404):
                        return state
                    raise
            if status in _TERMINAL_OPERATION_STATES:
                raise OperationFailed(operation_id, status, state.get("error"))

            if time.monotonic() > deadline:
                raise OperationTimeout(
                    f"Operation {operation_id} did not complete within "
                    f"{SETTINGS.lro_timeout_seconds}s (last status {status})"
                )
            time.sleep(delay)

    def _poll_location(self, location: str, *, poll_seconds: float) -> dict[str, Any]:
        deadline = time.monotonic() + SETTINGS.lro_timeout_seconds
        while True:
            response = self.request("GET", location, expected=(200, 201, 202))
            if response.status_code != 202:
                return self._json(response)
            if time.monotonic() > deadline:
                raise OperationTimeout(f"Operation at {location} did not complete in time")
            time.sleep(_retry_after_seconds(response, poll_seconds))


__all__ = [
    "FabricApiError",
    "FabricClient",
    "FabricError",
    "OperationFailed",
    "OperationTimeout",
]
