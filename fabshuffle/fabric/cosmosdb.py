"""Cosmos DB database (Fabric) migration.

The item half is unusually simple: ``definition.json`` lists the containers with their
partition keys, indexing and vector policies, unique keys, TTL and autoscale throughput, and
holds **no Fabric item ids at all**. So it is a straight definition round trip with nothing
to rewrite, handled by the generic machinery in :mod:`fabshuffle.fabric.analytics`.

What is *not* in the definition is the documents. Those move over the Cosmos data plane; see
:mod:`fabshuffle.transfer.cosmos`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fabshuffle.fabric.client import FabricClient
from fabshuffle.fabric.items import is_system_item

COSMOS_DB_DATABASE = "CosmosDBDatabase"


def list_cosmos_databases(client: FabricClient, workspace_id: str) -> list[dict[str, Any]]:
    databases = client.list_all(f"workspaces/{workspace_id}/cosmosDbDatabases")
    return [db for db in databases if not is_system_item(db)]


def get_cosmos_database(client: FabricClient, workspace_id: str, database_id: str) -> dict[str, Any]:
    return client.get(f"workspaces/{workspace_id}/cosmosDbDatabases/{database_id}")


def server_fqdn(database: Mapping[str, Any]) -> str:
    return (database.get("properties") or {}).get("serverFqdn") or ""


def database_name(database: Mapping[str, Any]) -> str:
    """The database name on the data plane.

    Read-only, and derived by Fabric from the display name at creation. Since the copy keeps
    the display name, this normally matches, but it is read back rather than assumed.
    """
    return (database.get("properties") or {}).get("databaseName") or ""


def endpoint_url(database: Mapping[str, Any]) -> str:
    """The Cosmos NoSQL endpoint the SDK connects to.

    ``serverFqdn`` is a bare host, and the SDK wants an absolute URL.
    """
    host = server_fqdn(database)
    if not host:
        return ""
    if host.startswith("http://") or host.startswith("https://"):
        return host
    return f"https://{host}:443/"


__all__ = [
    "COSMOS_DB_DATABASE",
    "database_name",
    "endpoint_url",
    "get_cosmos_database",
    "list_cosmos_databases",
    "server_fqdn",
]
