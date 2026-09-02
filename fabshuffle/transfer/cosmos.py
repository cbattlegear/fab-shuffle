"""Cosmos DB document transfer.

The Fabric item definition carries the container metadata, so the containers themselves
arrive with the item. Documents do not: there is no Fabric API that reads or writes them, so
they go over the Cosmos NoSQL data plane with the ``azure-cosmos`` SDK, the same way OneLake
files go through azcopy and warehouse schema goes through sqlpackage.

Two things are worth knowing before changing anything here.

**Gateway mode only.** Fabric's Cosmos endpoint does not expose the per-partition addresses
Direct mode needs. The Python SDK defaults to Gateway, so this simply never asks for Direct.

**Documents are read whole and written back whole**, minus the properties Cosmos owns. A
document's ``id`` and partition key values are part of the data and are preserved; the
system metadata (``_rid``, ``_self``, ``_etag``, ``_attachments``, ``_ts``) belongs to the
container it came from and is dropped, because writing it back is rejected.

Writes are upserts so that re-running a migration over a partly copied database converges
rather than failing on the first document that already arrived.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Mapping
from typing import Any

from fabshuffle.auth import TokenProvider

logger = logging.getLogger(__name__)

# Properties Cosmos generates and owns. They describe where a document lives, not what it
# says, and the service rejects a write that tries to set them.
SYSTEM_PROPERTIES = frozenset({"_rid", "_self", "_etag", "_attachments", "_ts"})

# How often to report progress while a large container is streaming.
PROGRESS_EVERY = 500


class CosmosTransferError(RuntimeError):
    """Documents could not be copied."""


class _MsalCredential:
    """Adapts our MSAL token provider to the credential azure-core expects.

    The SDK derives the scope from the account endpoint and falls back to a generic Cosmos
    scope if that is refused, so whatever it asks for is passed straight through rather than
    being pinned here.
    """

    def __init__(self, tokens: TokenProvider) -> None:
        self._tokens = tokens

    def get_token(self, *scopes: str, **_kwargs: Any) -> Any:
        from azure.core.credentials import AccessToken

        from fabshuffle.auth import token_claim

        scope = scopes[0] if scopes else "https://cosmos.azure.com/.default"
        token = self._tokens.token(scope)
        # azure-core needs an expiry to decide when to refresh. MSAL caches for us, so a
        # slightly early expiry only costs a cache hit.
        expires_on = int(token_claim(token, "exp") or 0)
        return AccessToken(token, expires_on)


def strip_system_properties(document: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in document.items() if key not in SYSTEM_PROPERTIES}


def _client(endpoint: str, tokens: TokenProvider) -> Any:
    try:
        from azure.cosmos import CosmosClient
    except ImportError as error:  # pragma: no cover - dependency is declared
        raise CosmosTransferError(
            "Copying Cosmos DB documents needs the azure-cosmos package, which is not "
            "installed in this image."
        ) from error
    # Gateway is the SDK default and the only mode Fabric's endpoint supports.
    return CosmosClient(endpoint, credential=_MsalCredential(tokens))


def container_names(endpoint: str, database: str, tokens: TokenProvider) -> list[str]:
    from azure.cosmos import exceptions

    try:
        client = _client(endpoint, tokens)
        return [
            container["id"]
            for container in client.get_database_client(database).list_containers()
            if container.get("id")
        ]
    except exceptions.CosmosHttpResponseError as error:
        raise CosmosTransferError(_describe(error)) from error


def _read_all(container: Any) -> Iterator[dict[str, Any]]:
    # Cross-partition is required: a container is partitioned and we want all of it.
    return container.query_items("SELECT * FROM c", enable_cross_partition_query=True)


def copy_documents(
    *,
    source_endpoint: str,
    source_database: str,
    target_endpoint: str,
    target_database: str,
    tokens: TokenProvider,
    on_progress: Callable[[str], None] | None = None,
) -> list[str]:
    """Copy every document in every container. Returns per-container warnings.

    A container that fails is reported and the rest are still attempted: losing one
    collection should not cost the operator the other twenty.
    """
    from azure.cosmos import exceptions

    warnings: list[str] = []
    try:
        source = _client(source_endpoint, tokens).get_database_client(source_database)
        target = _client(target_endpoint, tokens).get_database_client(target_database)
        containers = [c["id"] for c in source.list_containers() if c.get("id")]
    except exceptions.CosmosHttpResponseError as error:
        raise CosmosTransferError(_describe(error)) from error

    for name in containers:
        if on_progress:
            on_progress(f"Copying documents in container '{name}'")
        try:
            copied = _copy_container(source, target, name, on_progress)
            logger.debug("Copied %s documents into %s", copied, name)
        except exceptions.CosmosHttpResponseError as error:
            warnings.append(f"Documents in container '{name}' did not copy: {_describe(error)}")

    return warnings


def _copy_container(
    source: Any,
    target: Any,
    name: str,
    on_progress: Callable[[str], None] | None,
) -> int:
    reader = source.get_container_client(name)
    writer = target.get_container_client(name)

    copied = 0
    for document in _read_all(reader):
        writer.upsert_item(strip_system_properties(document))
        copied += 1
        if on_progress and copied % PROGRESS_EVERY == 0:
            on_progress(f"Copied {copied} document(s) into container '{name}'")
    return copied


def _describe(error: Any) -> str:
    """Say what Cosmos actually refused, rather than repeating a stack of SDK wrapping."""
    status = getattr(error, "status_code", None)
    message = (getattr(error, "message", "") or str(error)).strip()
    # The SDK appends its own multi-line activity trace, which is noise in a warning.
    first_line = message.splitlines()[0] if message else ""
    if status == 403:
        return (
            "access was denied. A Cosmos database in Fabric authorises the data plane "
            "separately from the item, so grant this service principal a data reader role on "
            f"the source and a writer role on the copy ({first_line})"
        )
    return f"HTTP {status}: {first_line}" if status else first_line


__all__ = [
    "PROGRESS_EVERY",
    "SYSTEM_PROPERTIES",
    "CosmosTransferError",
    "container_names",
    "copy_documents",
    "strip_system_properties",
]
