"""Lakehouse, warehouse, and SQL analytics endpoint operations."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from fabshuffle.config import SETTINGS
from fabshuffle.fabric.client import FabricApiError, FabricClient
from fabshuffle.fabric.items import is_system_item

CASE_INSENSITIVE_COLLATION = "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"
CASE_SENSITIVE_COLLATION = "Latin1_General_100_BIN2_UTF8"

# A metadata refresh is flaky, and a table it does not sync is invisible on the endpoint
# until something built on it fails to deploy. Cheap to ask again, expensive to miss.
REFRESH_ATTEMPTS = 3
# A table that reports Failure may recover, so it is worth a real pause. A table that reports
# NotRun is usually just current already, which is the normal steady state of a healthy
# endpoint, so asking again costs a moment rather than most of a minute.
REFRESH_WAIT_SECONDS = 20
REFRESH_NOT_RUN_WAIT_SECONDS = 3


@dataclass(frozen=True, slots=True)
class TableRef:
    """A table inside a lakehouse or warehouse, optionally namespaced by schema."""

    name: str
    schema: str | None = None

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.name}" if self.schema else self.name


# ------------------------------------------------------------------- lakehouses


def list_lakehouses(client: FabricClient, workspace_id: str) -> list[dict[str, Any]]:
    lakehouses = client.list_all(f"workspaces/{workspace_id}/lakehouses")
    return [lh for lh in lakehouses if not is_system_item(lh)]


def get_lakehouse(client: FabricClient, workspace_id: str, lakehouse_id: str) -> dict[str, Any]:
    return client.get(f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}")


def is_schema_enabled(lakehouse: dict[str, Any]) -> bool:
    """Schema-enabled lakehouses are the only ones that report ``defaultSchema``."""
    return "defaultSchema" in (lakehouse.get("properties") or {})


def create_lakehouse(
    client: FabricClient,
    workspace_id: str,
    display_name: str,
    *,
    schema_enabled: bool = False,
    description: str | None = None,
    folder_id: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"displayName": display_name}
    if description:
        body["description"] = description
    if folder_id:
        body["folderId"] = folder_id
    if schema_enabled:
        # The API only accepts ``true`` here; omitting the payload creates a classic lakehouse.
        body["creationPayload"] = {"enableSchemas": True}
    return client.post(f"workspaces/{workspace_id}/lakehouses", json=body)


def list_lakehouse_tables(
    client: FabricClient,
    workspace_id: str,
    lakehouse_id: str,
) -> list[dict[str, Any]]:
    """List lakehouse tables. This endpoint returns its rows under ``data``, not ``value``."""
    return client.list_all(
        f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
        params={"maxResults": 100},
        value_key="data",
    )


def _schema_from_location(location: str) -> str | None:
    """Pull the schema segment out of ``.../Tables/<schema>/<table>``."""
    parts = [segment for segment in location.replace("\\", "/").split("/") if segment]
    if "Tables" not in parts:
        return None
    tail = parts[parts.index("Tables") + 1 :]
    return tail[0] if len(tail) >= 2 else None


def managed_tables(
    client: FabricClient,
    workspace_id: str,
    lakehouse_id: str,
    *,
    schema_enabled: bool,
) -> list[TableRef]:
    """Return the lakehouse's managed tables.

    This cannot exclude shortcuts, because nothing in the response marks one: ``TableType``
    is only ever ``Managed`` or ``External``, and a shortcut to a delta table is reported as
    ``Managed``. Callers that are about to copy data must filter the result against the
    shortcuts API, which is the only thing that knows.
    """
    refs: list[TableRef] = []
    for table in list_lakehouse_tables(client, workspace_id, lakehouse_id):
        if table.get("type") != "Managed":
            continue
        name = table.get("name")
        if not name:
            continue
        schema = _schema_from_location(table.get("location") or "") if schema_enabled else None
        refs.append(TableRef(name=name, schema=schema))
    return refs


# -------------------------------------------------------------------- warehouses


def list_warehouses(client: FabricClient, workspace_id: str) -> list[dict[str, Any]]:
    warehouses = client.list_all(f"workspaces/{workspace_id}/warehouses")
    return [wh for wh in warehouses if not is_system_item(wh)]


def get_warehouse(client: FabricClient, workspace_id: str, warehouse_id: str) -> dict[str, Any]:
    return client.get(f"workspaces/{workspace_id}/warehouses/{warehouse_id}")


def warehouse_connection_string(warehouse: dict[str, Any]) -> str:
    properties = warehouse.get("properties") or {}
    return properties.get("connectionString") or properties.get("connectionInfo") or ""


def create_warehouse(
    client: FabricClient,
    workspace_id: str,
    display_name: str,
    *,
    collation_type: str | None = None,
    description: str | None = None,
    folder_id: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"displayName": display_name}
    if description:
        body["description"] = description
    if folder_id:
        body["folderId"] = folder_id
    if collation_type:
        body["creationPayload"] = {"collationType": collation_type}
    return client.post(f"workspaces/{workspace_id}/warehouses", json=body)


# ------------------------------------------------------------- sql endpoints


def lakehouse_sql_endpoint(lakehouse: dict[str, Any]) -> dict[str, Any]:
    return (lakehouse.get("properties") or {}).get("sqlEndpointProperties") or {}


def refresh_sql_endpoint_metadata(
    client: FabricClient,
    workspace_id: str,
    sql_endpoint_id: str,
    *,
    timeout_minutes: int = 20,
    attempts: int = REFRESH_ATTEMPTS,
    on_progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Force the SQL analytics endpoint to pick up newly created tables and shortcuts.

    A long running operation, so the client waits for it to settle. Settling is not the same
    as succeeding: the result carries a status *per table*, and the call reports success
    while individual tables failed to sync or were never attempted.

    That is worth retrying rather than reporting, because it is flaky in practice and because
    a table missing from the endpoint is not a visible problem until something built on it
    fails to deploy, naming the view rather than the table.

    The retry is a whole refresh rather than one scoped to the tables that did not sync.
    Scoping means naming a schema, and Fabric resolves those against the default schema for
    an item that is not schema enabled, reporting anything else as ``DeltaTableNotFound`` —
    so a selective retry can invent failures that were not there.
    """
    result: dict[str, Any] = {}
    for attempt in range(1, max(1, attempts) + 1):
        result = _refresh_once(client, workspace_id, sql_endpoint_id, timeout_minutes)
        pending = unsynced_tables(result)
        if not pending:
            return result
        if attempt < attempts:
            failed = [table for table in pending if table.get("status") == "Failure"]
            if failed and on_progress:
                on_progress(
                    f"{len(failed)} table(s) failed to sync onto the SQL endpoint, "
                    f"refreshing again (attempt {attempt} of {attempts})"
                )
            time.sleep(REFRESH_WAIT_SECONDS if failed else REFRESH_NOT_RUN_WAIT_SECONDS)
    return result


def _refresh_once(
    client: FabricClient,
    workspace_id: str,
    sql_endpoint_id: str,
    timeout_minutes: int,
) -> dict[str, Any]:
    body = {"timeout": {"timeUnit": "Minutes", "value": timeout_minutes}}
    try:
        return client.post(
            f"workspaces/{workspace_id}/sqlEndpoints/{sql_endpoint_id}/refreshMetadata",
            json=body,
        )
    except FabricApiError as error:
        # A refresh failure should not sink the migration; the schema copy will surface it.
        if error.status_code in (400, 404):
            return {"status": "Skipped", "reason": error.body[:400]}
        raise


def unsynced_tables(response: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Tables the refresh did not confirm as synced.

    ``Failure`` and ``NotRun`` both count. Only ``Success`` says the endpoint can see the
    table, and ``NotRun`` says in as many words that the operation did not run.
    """
    return [
        table
        for table in response.get("value") or []
        if table.get("status") and table.get("status") != "Success"
    ]


def sync_failures(response: Mapping[str, Any]) -> list[str]:
    """Tables the endpoint could not sync, once the retries are done.

    A table that is not on the endpoint is invisible, so whatever reads it fails to deploy
    with an error naming the reader: ``CREATE VIEW v AS SELECT * FROM t`` comes back as
    ``42S02`` and nothing anywhere points at ``t``.

    Only ``Failure`` is reported. ``NotRun`` is worth another attempt, because the operation
    genuinely did not run, but it is also the ordinary answer for a table that was already
    current — which on a healthy endpoint is most of them. Reporting it named nearly every
    table in the workspace and buried the two lines that mattered.
    """
    messages: list[str] = []
    for table in unsynced_tables(response):
        if table.get("status") != "Failure":
            continue
        error = table.get("error") or {}
        said = " ".join(
            part for part in (error.get("errorCode"), error.get("message")) if part
        ).strip()
        name = table.get("tableName") or "an unnamed table"
        messages.append(
            f"table '{name}' did not sync onto the SQL endpoint"
            + (f": {said}" if said else "")
            + ". Anything reading it, such as a view, will not deploy either."
        )
    return messages


def wait_for_sql_endpoint(
    client: FabricClient,
    workspace_id: str,
    lakehouse_id: str,
) -> dict[str, Any]:
    """Block until the lakehouse SQL analytics endpoint finishes provisioning."""
    import time

    deadline = time.monotonic() + SETTINGS.sql_endpoint_timeout_seconds
    while True:
        lakehouse = get_lakehouse(client, workspace_id, lakehouse_id)
        endpoint = lakehouse_sql_endpoint(lakehouse)
        status = endpoint.get("provisioningStatus")
        if status == "Success" and endpoint.get("connectionString"):
            return endpoint
        if status == "Failed":
            raise RuntimeError(f"SQL analytics endpoint provisioning failed for lakehouse {lakehouse_id}")
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"SQL analytics endpoint for lakehouse {lakehouse_id} was still {status} after "
                f"{SETTINGS.sql_endpoint_timeout_seconds}s"
            )
        time.sleep(15)


# ------------------------------------------------------------ mirrored databases

# Mirroring does not start on its own when a mirrored database is created from a definition,
# and Fabric rejects startMirroring while the item is still Initializing.
MIRRORING_INACTIVE = frozenset({"Stopped", "Stopping", "Initializing"})


def list_mirrored_databases(client: FabricClient, workspace_id: str) -> list[dict[str, Any]]:
    try:
        databases = client.list_all(f"workspaces/{workspace_id}/mirroredDatabases")
    except FabricApiError as error:
        if error.status_code in (401, 403, 404):
            return []
        raise
    return [db for db in databases if not is_system_item(db)]


def mirroring_status(client: FabricClient, workspace_id: str, database_id: str) -> str | None:
    """Current mirroring status, or ``None`` when it cannot be read."""
    try:
        result = client.post(
            f"workspaces/{workspace_id}/mirroredDatabases/{database_id}/getMirroringStatus"
        )
    except FabricApiError as error:
        if error.status_code in (400, 401, 403, 404):
            return None
        raise
    return result.get("status")


def mirrored_database_sql_endpoint(database: dict[str, Any]) -> dict[str, Any]:
    return (database.get("properties") or {}).get("sqlEndpointProperties") or {}


__all__ = [
    "CASE_INSENSITIVE_COLLATION",
    "CASE_SENSITIVE_COLLATION",
    "MIRRORING_INACTIVE",
    "TableRef",
    "create_lakehouse",
    "create_warehouse",
    "get_lakehouse",
    "get_warehouse",
    "is_schema_enabled",
    "lakehouse_sql_endpoint",
    "list_lakehouse_tables",
    "list_lakehouses",
    "list_mirrored_databases",
    "list_warehouses",
    "managed_tables",
    "mirrored_database_sql_endpoint",
    "mirroring_status",
    "refresh_sql_endpoint_metadata",
    "sync_failures",
    "wait_for_sql_endpoint",
    "warehouse_connection_string",
]
