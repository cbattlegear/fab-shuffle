"""SQL database (Fabric) migration.

A Fabric SQL database is the one data store that carries both a readable schema *and* a
definition API, so it moves in two halves:

* **Schema** through the item definition, in ``dacpac`` format. Nothing in that payload
  references another Fabric item, so it is copied across untouched rather than rewritten.
* **Data** through a Copy Job, the same mechanism lakehouses and warehouses use, but with a
  connection the caller has to supply. See :mod:`fabshuffle.fabric.copyjobs`.

The two cannot be done in one request. ``collation`` is only settable through
``creationPayload``, and Fabric rejects ``creationPayload`` and ``definition`` on the same
create, so the database is created first and its definition applied straight after.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from fabshuffle.fabric.client import FabricClient
from fabshuffle.fabric.definitions import strip_part
from fabshuffle.fabric.items import get_item_definition, is_system_item

logger = logging.getLogger(__name__)

SQL_DATABASE = "SQLDatabase"

# ``dacpac`` is the default and the format sqlpackage speaks. ``sqlproj`` would give us the
# schema as .sql files, which is nicer to read and much more to reassemble correctly.
DEFINITION_FORMAT = "dacpac"

PLATFORM_PART = ".platform"


def list_sql_databases(client: FabricClient, workspace_id: str) -> list[dict[str, Any]]:
    databases = client.list_all(f"workspaces/{workspace_id}/sqlDatabases")
    return [db for db in databases if not is_system_item(db)]


def server_fqdn(database: Mapping[str, Any]) -> str:
    """The address to connect to, which already carries ``,1433``."""
    return (database.get("properties") or {}).get("serverFqdn") or ""


def database_name(database: Mapping[str, Any]) -> str:
    """The catalog name to connect to.

    This is *not* the display name and *not* derivable from the item id: Fabric appends a
    GUID of its own choosing, which differs between the source and its copy. Always read it
    back from the created item rather than constructing it.
    """
    return (database.get("properties") or {}).get("databaseName") or ""


def create_sql_database(
    client: FabricClient,
    workspace_id: str,
    display_name: str,
    *,
    collation: str | None = None,
    backup_retention_days: int | None = None,
    description: str | None = None,
    folder_id: str | None = None,
) -> dict[str, Any]:
    """Create an empty SQL database, carrying over the settings that are fixed at creation.

    ``collation`` cannot be changed afterwards, so it has to go on this request even though
    that rules out sending the definition at the same time.
    """
    body: dict[str, Any] = {"displayName": display_name}
    if description:
        body["description"] = description
    if folder_id:
        body["folderId"] = folder_id

    payload: dict[str, Any] = {"creationMode": "New"}
    if collation:
        payload["collation"] = collation
    if backup_retention_days:
        payload["backupRetentionDays"] = backup_retention_days
    if len(payload) > 1:
        body["creationPayload"] = payload

    return client.post(f"workspaces/{workspace_id}/sqlDatabases", json=body)


def get_sql_database(client: FabricClient, workspace_id: str, database_id: str) -> dict[str, Any]:
    return client.get(f"workspaces/{workspace_id}/sqlDatabases/{database_id}")


def copy_schema(
    client: FabricClient,
    *,
    source_workspace_id: str,
    source_id: str,
    target_workspace_id: str,
    target_id: str,
) -> None:
    """Apply the source database's schema to its copy.

    The parts are sent back exactly as they were read. The definition article names the
    dacpac part ``sqldb.dacpac`` while the create and update examples both show
    ``definition.dacpac``, and the payload is an opaque binary either way, so the only safe
    thing is to echo whatever this tenant's service actually returned.

    The source ``.platform`` part is dropped, as everywhere else: it carries the original
    item's logical id, and leaving it in makes the copy claim to be the original.
    """
    definition = get_item_definition(
        client, source_workspace_id, source_id, fmt=DEFINITION_FORMAT
    )
    parts = strip_part(definition.get("parts") or [], PLATFORM_PART)
    if not parts:
        return

    client.post(
        f"workspaces/{target_workspace_id}/sqlDatabases/{target_id}/updateDefinition",
        json={"definition": {"format": DEFINITION_FORMAT, "parts": parts}},
    )


__all__ = [
    "DEFINITION_FORMAT",
    "SQL_DATABASE",
    "copy_schema",
    "create_sql_database",
    "database_name",
    "get_sql_database",
    "list_sql_databases",
    "server_fqdn",
]
