"""Cross-cluster KQL data movement.

Fabric has no REST API that copies KQL table data between eventhouses, so this uses the
Kusto control plane directly: ``.set-or-replace`` with a cross-cluster query pulls each
table from the source cluster into the target.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import timedelta

from azure.kusto.data import ClientRequestProperties, KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError

from fabshuffle.auth import ServicePrincipal

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 5
# Cross-cluster ingestion of a large table can run long, so the server timeout is raised
# from the default. The Kusto SDK adds a client/server delta to this value, so it has to be
# a timedelta rather than the "hh:mm:ss" string the KQL language itself uses.
INGEST_TIMEOUT = timedelta(hours=1)
# Tables Fabric manages itself; re-ingesting them corrupts the target database.
SYSTEM_TABLE_PREFIXES = ("$",)


def _client(cluster_uri: str, principal: ServicePrincipal) -> KustoClient:
    connection = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        cluster_uri,
        principal.client_id,
        principal.client_secret,
        principal.tenant_id,
    )
    return KustoClient(connection)


def list_tables(cluster_uri: str, database: str, principal: ServicePrincipal) -> list[str]:
    with _client(cluster_uri, principal) as client:
        response = client.execute_mgmt(database, ".show tables | project TableName")
        rows = response.primary_results[0]
        return [
            str(row["TableName"])
            for row in rows
            if not str(row["TableName"]).startswith(SYSTEM_TABLE_PREFIXES)
        ]


def copy_database(
    *,
    source_cluster_uri: str,
    target_cluster_uri: str,
    database: str,
    principal: ServicePrincipal,
    on_progress: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Copy every table from the source database into the same-named target database.

    Both URIs must be *query* endpoints: cross-cluster ``.set-or-replace`` is executed on the
    target's query endpoint and reads through ``cluster(...).database(...)``, so the ingestion
    endpoint is not involved.
    """
    tables = list_tables(source_cluster_uri, database, principal)
    if not tables:
        return {"tables": 0, "rows": 0}

    total_rows = 0
    with _client(target_cluster_uri, principal) as target:
        properties = ClientRequestProperties()
        properties.set_option(ClientRequestProperties.request_timeout_option_name, INGEST_TIMEOUT)

        for table in tables:
            if on_progress:
                on_progress(f"Ingesting {database}.{table}")
            command = (
                f".set-or-replace {table} with(distributed=true) <| "
                f"cluster('{source_cluster_uri}').database('{database}').{table}"
            )
            total_rows += _execute_with_retry(target, database, command, properties, table)

    return {"tables": len(tables), "rows": total_rows}


def _execute_with_retry(
    client: KustoClient,
    database: str,
    command: str,
    properties: ClientRequestProperties,
    table: str,
) -> int:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = client.execute_mgmt(database, command, properties)
            table_result = response.primary_results[0]
            # ``.set-or-replace`` reports the ingested extent row count in this column.
            columns = {column.column_name for column in table_result.columns}
            if "RowCount" not in columns:
                return 0
            return sum(int(row["RowCount"]) for row in table_result)
        except KustoServiceError as error:
            if attempt == MAX_ATTEMPTS:
                raise RuntimeError(
                    f"Could not copy KQL table {database}.{table} after {MAX_ATTEMPTS} attempts: {error}"
                ) from error
            delay = 5 * attempt
            logger.warning("Copy of %s.%s failed (%s), retrying in %ss", database, table, error, delay)
            time.sleep(delay)
    return 0


__all__ = ["copy_database", "list_tables"]
