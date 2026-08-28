"""Eventhouse and KQL database operations."""

from __future__ import annotations

from typing import Any

from fabshuffle.fabric.client import FabricClient
from fabshuffle.fabric.definitions import decode_json_part, find_part, replace_part
from fabshuffle.fabric.items import get_item_definition, is_system_item

DATABASE_PROPERTIES_PART = "DatabaseProperties.json"
DATABASE_SCHEMA_PART = "DatabaseSchema.kql"


def list_eventhouses(client: FabricClient, workspace_id: str) -> list[dict[str, Any]]:
    eventhouses = client.list_all(f"workspaces/{workspace_id}/eventhouses")
    return [eh for eh in eventhouses if not is_system_item(eh)]


def get_eventhouse(client: FabricClient, workspace_id: str, eventhouse_id: str) -> dict[str, Any]:
    return client.get(f"workspaces/{workspace_id}/eventhouses/{eventhouse_id}")


def create_eventhouse(
    client: FabricClient,
    workspace_id: str,
    display_name: str,
    *,
    description: str | None = None,
    folder_id: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"displayName": display_name}
    if description:
        body["description"] = description
    if folder_id:
        body["folderId"] = folder_id
    return client.post(f"workspaces/{workspace_id}/eventhouses", json=body)


def list_kql_databases(client: FabricClient, workspace_id: str) -> list[dict[str, Any]]:
    databases = client.list_all(f"workspaces/{workspace_id}/kqlDatabases")
    return [db for db in databases if not is_system_item(db)]


def get_kql_database(client: FabricClient, workspace_id: str, database_id: str) -> dict[str, Any]:
    return client.get(f"workspaces/{workspace_id}/kqlDatabases/{database_id}")


def database_type(database: dict[str, Any]) -> str:
    return (database.get("properties") or {}).get("databaseType") or "ReadWrite"


def create_kql_database(
    client: FabricClient,
    workspace_id: str,
    display_name: str,
    *,
    parts: list[dict[str, str]] | None = None,
    creation_payload: dict[str, Any] | None = None,
    description: str | None = None,
    folder_id: str | None = None,
) -> dict[str, Any]:
    if parts and creation_payload:
        raise ValueError("Fabric rejects creationPayload and definition on the same request")

    body: dict[str, Any] = {"displayName": display_name}
    if description:
        body["description"] = description
    if folder_id:
        body["folderId"] = folder_id
    if creation_payload:
        body["creationPayload"] = creation_payload
    if parts:
        body["definition"] = {"parts": parts}
    return client.post(f"workspaces/{workspace_id}/kqlDatabases", json=body)


def kql_database_definition_parts(
    client: FabricClient,
    workspace_id: str,
    database_id: str,
) -> list[dict[str, Any]]:
    definition = get_item_definition(client, workspace_id, database_id)
    return list(definition.get("parts") or [])


def retarget_database_definition(
    parts: list[dict[str, Any]],
    new_eventhouse_id: str,
) -> list[dict[str, Any]]:
    """Point an exported KQL database definition at the eventhouse in the target region.

    ``DatabaseProperties.json`` carries the source ``parentEventhouseItemId``; importing it
    unchanged would try to attach the database to an eventhouse in the old region.
    """
    properties_part = find_part(parts, DATABASE_PROPERTIES_PART)
    if not properties_part:
        return parts

    properties = decode_json_part(properties_part["payload"])
    properties["parentEventhouseItemId"] = new_eventhouse_id
    return replace_part(parts, DATABASE_PROPERTIES_PART, properties)


def shortcut_creation_payload(
    database: dict[str, Any],
    new_eventhouse_id: str,
) -> dict[str, Any] | None:
    """Build a Shortcut (follower) KQL database payload if the source exposes enough detail.

    ``Get KQL Database`` does not return the follower source for shortcut databases, so this
    only succeeds when the exported definition happens to carry the source coordinates.
    """
    properties = database.get("properties") or {}
    source_cluster = properties.get("sourceClusterUri")
    source_database = properties.get("sourceDatabaseName")
    if not source_cluster and not source_database:
        return None

    payload: dict[str, Any] = {
        "databaseType": "Shortcut",
        "parentEventhouseItemId": new_eventhouse_id,
    }
    if source_cluster:
        payload["sourceClusterUri"] = source_cluster
    if source_database:
        payload["sourceDatabaseName"] = source_database
    return payload


__all__ = [
    "DATABASE_PROPERTIES_PART",
    "DATABASE_SCHEMA_PART",
    "create_eventhouse",
    "create_kql_database",
    "database_type",
    "get_eventhouse",
    "get_kql_database",
    "kql_database_definition_parts",
    "list_eventhouses",
    "list_kql_databases",
    "retarget_database_definition",
    "shortcut_creation_payload",
]
