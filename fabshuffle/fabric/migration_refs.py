"""Resolve operator-selected external items through independent source and target clients."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import quote

from fabshuffle.fabric.client import FabricClient
from fabshuffle.fabric.items import get_item

_DETAIL_ROUTES = {
    "Lakehouse": "lakehouses",
    "Warehouse": "warehouses",
    "Eventhouse": "eventhouses",
    "KQLDatabase": "kqlDatabases",
    "SQLDatabase": "sqlDatabases",
    "MirroredDatabase": "mirroredDatabases",
    "CosmosDBDatabase": "cosmosDbDatabases",
}
_ENDPOINT_KINDS = {
    "connectionString": "sql_connection", "connectionInfo": "sql_connection_info",
    "serverFqdn": "sql_server", "queryServiceUri": "kql_query_uri",
    "ingestionServiceUri": "kql_ingestion_uri", "databaseName": "sql_database_name",
}


def endpoint_values(item: Mapping[str, Any]) -> dict[str, str]:
    """Pure endpoint inventory shared by live migration and captured recovery."""
    properties = item.get("properties") or {}
    result = {
        kind: value for key, kind in _ENDPOINT_KINDS.items()
        if isinstance(value := properties.get(key), str) and value
    }
    endpoint = properties.get("sqlEndpointProperties") or {}
    for key, kind in (("id", "sql_endpoint_id"), ("connectionString", "sql_endpoint_server")):
        if isinstance(value := endpoint.get(key), str) and value:
            result[kind] = value
    return result


def referenced_onelake_roots(text: str) -> set[str]:
    """Complete named or GUID roots, including both HTTP and ABFS spellings."""
    return {
        match.group(0).rstrip("/") + "/"
        for pattern in (
            r"https://onelake\.(?:dfs|blob)\.fabric\.microsoft\.com/[^/\r\n\"'?#]+/[^/\r\n\"'?#]+/?",
            r"abfss?://[^@/\r\n\"'?#]+@onelake\.dfs\.fabric\.microsoft\.com/[^/\r\n\"'?#]+/?",
        )
        for match in re.finditer(pattern, text, re.IGNORECASE)
    }


def onelake_aliases(
    workspace_id: str, workspace_name: str, item: Mapping[str, Any],
    target_workspace_id: str = "", target_item_id: str = "",
) -> dict[str, str]:
    """Map complete item roots; OneLake requires both names or both GUIDs, not a mixture."""
    item_id, item_type = str(item.get("id") or ""), str(item.get("type") or "")
    if not item_id or item_type in ("Workspace", "Connection", "SparkPool"):
        return {}
    coordinates = [(workspace_id, item_id)]
    if workspace_name and item.get("displayName") and item_type:
        coordinates.append((workspace_name, f"{item['displayName']}.{item_type}"))
    aliases = {}
    for workspace, name in coordinates:
        for encoded_workspace, encoded_name in (
            (workspace, name), (quote(workspace, safe=""), quote(name, safe="")),
        ):
            for endpoint in ("dfs", "blob"):
                base = f"https://onelake.{endpoint}.fabric.microsoft.com/"
                aliases[f"{base}{encoded_workspace}/{encoded_name}/"] = (
                    f"{base}{target_workspace_id}/{target_item_id}/"
                )
            for scheme in ("abfs", "abfss"):
                root = f"{scheme}://{encoded_workspace}@onelake.dfs.fabric.microsoft.com/{encoded_name}/"
                aliases[root] = (
                    f"{scheme}://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_item_id}/"
                )
    return aliases


def _item(client: FabricClient, workspace_id: str, item_id: str) -> dict[str, Any]:
    item = get_item(client, workspace_id, item_id)
    route = _DETAIL_ROUTES.get(item.get("type"))
    if route:
        item = {**item, **client.get(f"workspaces/{workspace_id}/{route}/{item_id}")}
    return item


def resolve(
    source: FabricClient,
    target: FabricClient,
    entries: Iterable[Mapping[str, str]],
    *,
    migrating_workspace_id: str,
) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    mappings: dict[str, str] = {}
    source_items: dict[str, dict[str, Any]] = {}

    def bind(old: str, new: str) -> None:
        if not old or not new:
            return
        existing_key = next((key for key in mappings if key.casefold() == old.casefold()), old)
        previous = mappings.get(existing_key)
        if previous is not None and previous.casefold() != new.casefold():
            raise ValueError(f"External reference '{old}' has conflicting destination mappings.")
        mappings[existing_key] = new

    for entry in entries:
        keys = ("source_workspace_id", "source_item_id", "target_workspace_id", "target_item_id")
        if any(not isinstance(entry.get(key), str) or not entry[key].strip() for key in keys):
            raise ValueError("External item mappings require source and destination workspace and item IDs.")
        source_ws, source_id, target_ws, target_id = (entry[key].strip() for key in keys)
        if source_ws.casefold() == migrating_workspace_id.casefold():
            raise ValueError(
                "External item mappings cannot replace items inside the workspace being migrated."
            )
        old = _item(source, source_ws, source_id)
        new = _item(target, target_ws, target_id)
        if old.get("type") != new.get("type"):
            raise ValueError(
                f"External item '{old.get('displayName') or source_id}' is {old.get('type')}, "
                f"but destination item {target_id} is {new.get('type')}."
            )
        bind(source_ws, target_ws)
        bind(source_id, target_id)
        for old_path, new_path in onelake_aliases(source_ws, "", old, target_ws, target_id).items():
            bind(old_path, new_path)
        target_endpoints = endpoint_values(new)
        for kind, value in endpoint_values(old).items():
            bind(value, target_endpoints.get(kind, ""))
        source_items[source_id] = old
        source_items[source_ws] = {
            "id": source_ws, "displayName": source_ws, "type": "Workspace",
        }
    return mappings, source_items
