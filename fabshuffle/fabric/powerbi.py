"""Power BI REST calls for semantic model storage format.

Large semantic models are backed by Azure Premium Files, which pins them to their region:
a workspace containing one cannot be reassigned to a capacity elsewhere. Converting those
models to the small (``Abf``) format releases that pin, which is what makes the
reassignment strategy possible.

These endpoints live on ``api.powerbi.com`` and require the Power BI audience, so they use
their own token rather than the Fabric one.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx

from fabshuffle.auth import TokenProvider
from fabshuffle.config import POWERBI_API_BASE, SETTINGS

logger = logging.getLogger(__name__)

SMALL = "Abf"
LARGE = "PremiumFiles"

CONVERSION_POLL_SECONDS = 10
CONVERSION_TIMEOUT_SECONDS = 1800

# Push and streaming models never use the large format and cannot be converted at all.
NON_CONVERTIBLE_PROVIDERS = frozenset(
    {"RealTimeInPushMode", "RealTimeInPubNubMode", "RealTimeInStreamingMode"}
)


class PowerBiError(RuntimeError):
    """A Power BI REST call failed."""


@dataclass(frozen=True, slots=True)
class SemanticModel:
    id: str
    name: str
    storage_mode: str
    content_provider: str

    @property
    def is_large(self) -> bool:
        return self.storage_mode == LARGE

    @property
    def convertible(self) -> bool:
        return self.content_provider not in NON_CONVERTIBLE_PROVIDERS


class PowerBiClient:
    """Minimal Power BI REST client scoped to what the reassignment path needs."""

    def __init__(self, tokens: TokenProvider, transport: httpx.BaseTransport | None = None) -> None:
        self._tokens = tokens
        self._http = httpx.Client(
            base_url=POWERBI_API_BASE,
            timeout=httpx.Timeout(SETTINGS.request_timeout_seconds),
            transport=transport,
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> PowerBiClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _request(self, method: str, path: str, json: Any | None = None) -> httpx.Response:
        response = self._http.request(
            method,
            path,
            json=json,
            headers={
                "Authorization": f"Bearer {self._tokens.powerbi_token()}",
                "Accept": "application/json",
            },
        )
        if not response.is_success:
            raise PowerBiError(f"{method} {path} failed with HTTP {response.status_code}: {response.text}")
        return response

    def list_semantic_models(self, workspace_id: str) -> list[SemanticModel]:
        payload = self._request("GET", f"/groups/{workspace_id}/datasets").json()
        return [
            SemanticModel(
                id=str(dataset.get("id")),
                name=dataset.get("name") or str(dataset.get("id")),
                # Absent means the caller lacks write permission, or the model predates the
                # setting. Both behave as the small format.
                storage_mode=dataset.get("targetStorageMode") or SMALL,
                content_provider=dataset.get("ContentProviderType") or "",
            )
            for dataset in payload.get("value") or []
        ]

    def get_semantic_model(self, workspace_id: str, model_id: str) -> SemanticModel | None:
        for model in self.list_semantic_models(workspace_id):
            if model.id == model_id:
                return model
        return None

    def set_storage_mode(self, workspace_id: str, model_id: str, storage_mode: str) -> None:
        self._request(
            "PATCH",
            f"/groups/{workspace_id}/datasets/{model_id}",
            json={"targetStorageMode": storage_mode},
        )

    def convert(
        self,
        workspace_id: str,
        model: SemanticModel,
        storage_mode: str,
        *,
        on_progress: Callable[[str], None] | None = None,
    ) -> None:
        """Switch a model's storage format and wait for the conversion to land.

        The PATCH returns immediately, so the new format has to be confirmed by polling;
        otherwise a reassignment could start while a model is still Premium Files backed.
        """
        target = "small" if storage_mode == SMALL else "large"
        if on_progress:
            on_progress(f"Converting '{model.name}' to {target} semantic model storage")

        self.set_storage_mode(workspace_id, model.id, storage_mode)

        deadline = time.monotonic() + CONVERSION_TIMEOUT_SECONDS
        while True:
            current = self.get_semantic_model(workspace_id, model.id)
            if current is None:
                raise PowerBiError(f"Semantic model '{model.name}' disappeared during conversion")
            if current.storage_mode == storage_mode:
                return
            if time.monotonic() > deadline:
                raise PowerBiError(
                    f"Semantic model '{model.name}' was still {current.storage_mode} after "
                    f"{CONVERSION_TIMEOUT_SECONDS}s"
                )
            time.sleep(CONVERSION_POLL_SECONDS)


__all__ = [
    "LARGE",
    "SMALL",
    "PowerBiClient",
    "PowerBiError",
    "SemanticModel",
]
