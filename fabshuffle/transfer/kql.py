"""KQL data movement, using cross-cluster queries only for the legacy same-tenant path.

Paired-principal moves stream source queries into destination-authenticated ingestion.
They never ask the destination to query the source and leave target update policies stopped.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import time
from collections.abc import Callable, Collection
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, NotRequired, TypedDict
from urllib.parse import quote

from azure.kusto.data import ClientRequestProperties, DataFormat, KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError

from fabshuffle.auth import AuthPrincipal, ManagedIdentity, TokenProvider
from fabshuffle.lifecycle import CopyOutcome
from fabshuffle.transfer.common import (
    StagingBudgetError,
    check_cancelled,
    resolve_memory_budget,
)

logger = logging.getLogger(__name__)

_GUID = re.compile(r"[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}")

MAX_ATTEMPTS = 5
# Cross-cluster ingestion of a large table can run long, so the server timeout is raised
# from the default. The Kusto SDK adds a client/server delta to this value, so it has to be
# a timedelta rather than the "hh:mm:ss" string the KQL language itself uses.
INGEST_TIMEOUT = timedelta(hours=1)
# Tables Fabric manages itself; re-ingesting them corrupts the target database.
SYSTEM_TABLE_PREFIXES = ("$",)


class KqlTransferError(RuntimeError):
    """A table was not completely read and ingested."""


class DatabaseCopyResult(TypedDict):
    tables: int
    rows: int
    warnings: NotRequired[list[str]]


@dataclass(frozen=True, slots=True)
class FollowerSource:
    """Where a shortcut (follower) KQL database gets its data from."""

    database_name: str
    leader_metadata_path: str = ""

    @property
    def is_fabric_source(self) -> bool:
        """Whether the leader is a Fabric eventhouse rather than an Azure Data Explorer cluster.

        Fabric names a KQL database after its item id, so a GUID means the leader is another
        Fabric database and can be followed by id alone. Anything else is an ADX database
        name, which is only meaningful alongside its cluster URI.
        """
        return _GUID.fullmatch(self.database_name) is not None


def kusto_connection(
    cluster_uri: str, tokens: TokenProvider, *, use_token_provider: bool = False,
) -> KustoConnectionStringBuilder:
    """Share explicit credentials with capture; a callback refreshes MI tokens per request.

    https://learn.microsoft.com/kusto/api/get-started/app-authentication-methods
    """
    principal = tokens.principal
    if use_token_provider or isinstance(principal, ManagedIdentity):
        return KustoConnectionStringBuilder.with_token_provider(cluster_uri, tokens.kusto_token)
    return KustoConnectionStringBuilder.with_aad_application_key_authentication(
        cluster_uri,
        principal.client_id,
        principal.client_secret,
        principal.tenant_id,
    )


def _client(
    cluster_uri: str, principal: AuthPrincipal, *, tokens: TokenProvider | None = None,
) -> KustoClient:
    if tokens is not None:
        if tokens.principal != principal:
            raise KqlTransferError("KQL tokens must match the selected application identity.")
        tokens.assert_active()
        return KustoClient(kusto_connection(cluster_uri, tokens, use_token_provider=True))
    return KustoClient(kusto_connection(cluster_uri, TokenProvider(principal)))


def follower_source(
    cluster_uri: str,
    database: str,
    principal: AuthPrincipal,
) -> FollowerSource | None:
    """Resolve what a shortcut (follower) KQL database actually follows.

    ``Get KQL Database`` reports ``databaseType: Shortcut`` but not the source, so the only
    way to recover it is to ask the follower's own cluster. ``.show follower database``
    returns ``OriginalDatabaseName`` — the leader's KQL Database *item id* when the leader is
    a Fabric eventhouse — and ``LeaderClusterMetadataPath``, which identifies the leader
    cluster's storage container.
    """
    with _client(cluster_uri, principal) as client:
        response = client.execute_mgmt(database, f".show follower database {_ident(database)}")
        rows = list(response.primary_results[0])

    if not rows:
        return None

    row = rows[0]
    original = str(row["OriginalDatabaseName"] or "").strip()
    if not original:
        return None
    return FollowerSource(
        database_name=original,
        leader_metadata_path=str(row["LeaderClusterMetadataPath"] or "").strip(),
    )


def _ident(name: str) -> str:
    """Quote a database name for a control command.

    Fabric names its KQL databases after the item GUID, which starts with a digit often
    enough that an unquoted identifier is a parse error.
    """
    return "[" + json.dumps(name, ensure_ascii=False) + "]"


def list_tables(
    cluster_uri: str,
    database: str,
    principal: AuthPrincipal,
    *,
    exclude: Collection[str] = (),
) -> list[str]:
    excluded = {name.casefold() for name in exclude}
    with _client(cluster_uri, principal) as client:
        response = client.execute_mgmt(database, ".show tables | project TableName")
        rows = response.primary_results[0]
        return [
            str(row["TableName"])
            for row in rows
            if not str(row["TableName"]).startswith(SYSTEM_TABLE_PREFIXES)
            and str(row["TableName"]).casefold() not in excluded
        ]


def copy_database(
    *,
    source_cluster_uri: str,
    target_cluster_uri: str,
    database: str,
    principal: AuthPrincipal,
    target_principal: AuthPrincipal | None = None,
    source_database: str | None = None,
    target_database: str | None = None,
    max_staging_bytes: int | None = None,
    max_memory_bytes: int | None = None,
    cancel_requested: Callable[[], bool] | None = None,
    on_copied: Callable[[str], None] | None = None,
    on_complete: Callable[[CopyOutcome], None] | None = None,
    on_warning: Callable[[str], None] | None = None,
    exclude: Collection[str] = (),
    on_progress: Callable[[str], None] | None = None,
) -> DatabaseCopyResult:
    """Copy every table from the source database into the same-named target database.

    Both URIs must be *query* endpoints: cross-cluster ``.set-or-replace`` is executed on the
    target's query endpoint and reads through ``cluster(...).database(...)``, so the ingestion
    endpoint is not involved.

    ``exclude`` names tables that must not be copied, which is how table shortcuts are kept
    out: their data belongs to the shortcut target, and the shortcut is recreated separately.
    ``source_database`` and ``target_database`` override the legacy ``database`` name, so
    paired callers can supply the actual Fabric KQL database item IDs independently.
    """
    if target_principal is not None:
        return copy_database_streaming(
            source_cluster_uri=source_cluster_uri, target_cluster_uri=target_cluster_uri,
            database=database, source_database=source_database, target_database=target_database,
            principal=principal,
            target_principal=target_principal, max_staging_bytes=max_staging_bytes,
            max_memory_bytes=max_memory_bytes,
            cancel_requested=cancel_requested, exclude=exclude, on_progress=on_progress,
            on_copied=on_copied, on_complete=on_complete, on_warning=on_warning,
        )
    check_cancelled(cancel_requested)
    origin = source_database or database
    tables = list_tables(source_cluster_uri, origin, principal, exclude=exclude)
    if not tables:
        if on_complete:
            on_complete(CopyOutcome("kql", empty=True))
        return {"tables": 0, "rows": 0}

    total_rows = 0
    with _client(target_cluster_uri, principal) as target:
        properties = ClientRequestProperties()
        properties.set_option(ClientRequestProperties.request_timeout_option_name, INGEST_TIMEOUT)

        for table in tables:
            check_cancelled(cancel_requested)
            if on_progress:
                on_progress(f"Ingesting {origin}.{table}")
            command = (
                f".set-or-replace {table} with(distributed=true) <| "
                f"cluster('{source_cluster_uri}').database('{origin}').{table}"
            )
            total_rows += _execute_with_retry(target, target_database or database, command, properties, table)
            if on_copied:
                on_copied(table)

    if on_complete:
        on_complete(CopyOutcome("kql", empty=total_rows == 0))
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


def _stream_properties() -> ClientRequestProperties:
    properties = ClientRequestProperties()
    properties.set_option(ClientRequestProperties.request_timeout_option_name, INGEST_TIMEOUT)
    properties.set_option("notruncation", True)
    properties.set_option("results_progressive_enabled", False)
    properties.set_option("query_results_cache_max_age", timedelta(0))
    properties.set_option("request_block_row_level_security", True)
    return properties


def _table_data(table: str) -> str:
    # table() prefers the physical table over a same-named function; explicitly include cold data.
    # https://learn.microsoft.com/kusto/query/table-function
    return f'table({json.dumps(table)}, "all")'


def _count(client: KustoClient, database: str, table: str) -> int:
    response = client.execute_query(
        database, f"{_table_data(table)} | count", _stream_properties(),
    )
    return int(next(iter(response.primary_results[0]))[0])


def _csv_row(values: list[Any]) -> bytes:
    text = io.StringIO(newline="")
    csv.writer(text, lineterminator="\r\n").writerow([
        json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        if isinstance(value, (dict, list, bool)) else value
        for value in values
    ])
    return text.getvalue().encode("utf-8")


def _policy_rules(value: Any, entity: str) -> list[dict[str, Any]]:
    if value is None or value == "":
        return []
    try:
        rules = json.loads(value)
    except (ValueError, TypeError) as exc:
        raise KqlTransferError(
            f"KQL returned an unreadable update policy for {entity}: {exc}. Inspect the target policy."
        ) from exc
    if rules is None:
        return []
    if not isinstance(rules, list) or any(not isinstance(rule, dict) for rule in rules):
        raise KqlTransferError(f"KQL returned an unrecognized update policy for {entity}; inspect it.")
    return rules


def _require_quiet_target(target: KustoClient, database: str) -> None:
    # A later source-table load can append through a policy into a table already checkpointed
    # earlier. Check every target policy before any writes, not only the table being loaded.
    # https://learn.microsoft.com/kusto/management/show-table-update-policy-command
    response = target.execute_mgmt(database, ".show table * policy update")
    active: list[str] = []
    for row in response.primary_results[0]:
        policy = _policy_rules(row["Policy"], str(row["EntityName"]))
        if any(rule.get("IsEnabled") is not False for rule in policy):
            active.append(str(row["EntityName"]))
    if active:
        raise KqlTransferError(
            f"Target update policies are active on {', '.join(active)}. Set IsEnabled=false "
            "in those target tables' update policies before retrying; replaying rows while "
            "they run can duplicate data in already-copied tables."
        )


def _update_policy(target: KustoClient, database: str, table: str) -> list[dict[str, Any]]:
    response = target.execute_mgmt(database, f".show table {_ident(table)} policy update")
    rows = list(response.primary_results[0])
    if not rows:
        return []
    if len(rows) != 1:
        raise KqlTransferError(f"KQL returned multiple update policies for {database}.{table}; inspect it.")
    return _policy_rules(rows[0]["Policy"], f"{database}.{table}")


def _writable_policy(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # This read-only field is set by Fabric for the target caller, not imported as an identity.
    # https://learn.microsoft.com/kusto/management/alter-table-update-policy-command
    return [
        {key: value for key, value in rule.items() if key != "OwnerPrincipalDetails"}
        for rule in rules
    ]


def _stop_target_policies(
    target: KustoClient,
    database: str,
    *,
    on_warning: Callable[[str], None] | None = None,
    on_progress: Callable[[str], None] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> tuple[list[str], list[str]]:
    """Leave all destination update rules stopped, without editing exported KQL or rule logic."""
    check_cancelled(cancel_requested)
    response = target.execute_mgmt(database, ".show tables | project TableName")
    tables = [str(row["TableName"]) for row in response.primary_results[0]]
    stopped: list[str] = []
    warnings: list[str] = []
    for table in tables:
        check_cancelled(cancel_requested)
        if table.startswith(SYSTEM_TABLE_PREFIXES):
            continue
        rules = _update_policy(target, database, table)
        if not rules:
            continue
        desired = [{**rule, "IsEnabled": False} for rule in _writable_policy(rules)]
        if any(rule.get("IsEnabled") is not False for rule in rules):
            # Obfuscate the string in Kusto's command logs: a policy query may contain secrets.
            serialized = json.dumps(desired, ensure_ascii=False, separators=(",", ":"))
            literal = "h" + json.dumps(serialized, ensure_ascii=False)
            check_cancelled(cancel_requested)
            target.execute_mgmt(
                database, f".alter table {_ident(table)} policy update {literal}",
            )
            check_cancelled(cancel_requested)
            actual = _writable_policy(_update_policy(target, database, table))
            if actual != desired:
                raise KqlTransferError(
                    f"Target update policy for {database}.{table} was not preserved in its stopped state. "
                    "Inspect the target policy and set IsEnabled=false before retrying; no rows were copied."
                )
        message = (
            f"Update policies on target KQL table {database}.{table} remain stopped. After cutover, "
            f"inspect '.show table {_ident(table)} policy update' and use '.alter table "
            f"{_ident(table)} policy update' with IsEnabled=true only for rules you intend to resume."
        )
        stopped.append(table)
        warnings.append(message)
        if on_warning:
            on_warning(message)
        elif on_progress:
            on_progress(message)
    check_cancelled(cancel_requested)
    _require_quiet_target(target, database)
    return stopped, warnings


def stop_update_policies(
    cluster_uri: str,
    database: str,
    principal: AuthPrincipal,
    on_progress: Callable[[str], None] | None = None,
    *,
    cancel_requested: Callable[[], bool] | None = None,
) -> list[str]:
    """Stop destination update policies and return their table names for activation tracking.

    Call after every destination schema application, including replay. Nonempty policies
    that are already disabled are included so retries retain the manual activation context.
    Rule logic/settings are preserved; only IsEnabled is disabled and service-owned
    OwnerPrincipalDetails is omitted from writes. No source client is opened.
    """
    check_cancelled(cancel_requested)
    with _client(cluster_uri, principal) as target:
        tables, _ = _stop_target_policies(
            target, database, on_progress=on_progress, cancel_requested=cancel_requested,
        )
    return tables


def stop_target_update_policies(
    target: KustoClient,
    database: str,
    *,
    on_warning: Callable[[str], None] | None = None,
    on_progress: Callable[[str], None] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> list[str]:
    """Stop destination update policies and return actionable warnings."""
    _, warnings = _stop_target_policies(
        target, database, on_warning=on_warning, on_progress=on_progress,
        cancel_requested=cancel_requested,
    )
    return warnings


def stop_database_update_policies(
    *,
    target_cluster_uri: str,
    target_database: str,
    target_principal: AuthPrincipal,
    on_warning: Callable[[str], None] | None = None,
    on_progress: Callable[[str], None] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> list[str]:
    """Stop destination policies independently of data copying, including schema-only moves."""
    check_cancelled(cancel_requested)
    with _client(target_cluster_uri, target_principal) as target:
        return stop_target_update_policies(
            target, target_database, on_warning=on_warning, on_progress=on_progress,
            cancel_requested=cancel_requested,
        )


def _stream_table(
    source: KustoClient, target: KustoClient, database: str, target_database: str,
    table: str, max_memory_bytes: int, cancel_requested: Callable[[], bool] | None,
) -> int:
    properties = _stream_properties()
    expected = _count(source, database, table)
    source_schema = source.execute_query(
        database, f"{_table_data(table)} | take 0", properties,
    ).primary_results[0]
    target_schema = target.execute_query(
        target_database, f"{_table_data(table)} | take 0", properties,
    ).primary_results[0]

    def signature(result: Any) -> list[tuple[str, str]]:
        return [(c.column_name, c.column_type) for c in result.columns]

    if signature(source_schema) != signature(target_schema):
        raise KqlTransferError(
            f"KQL table {table} has a different target schema; deploy its source schema and retry."
        )
    check_cancelled(cancel_requested)
    _require_quiet_target(target, target_database)
    target.execute_mgmt(target_database, f".clear table {_ident(table)} data")
    response = source.execute_streaming_query(
        database, _table_data(table), timeout=INGEST_TIMEOUT, properties=properties,
    )
    limit = min(max_memory_bytes, 1024 * 1024)
    payload = bytearray()
    count = 0
    primary_seen = False
    completed = False

    def ingest() -> None:
        if payload:
            check_cancelled(cancel_requested)
            _require_quiet_target(target, target_database)
            # The data SDK exposes the documented streaming-ingest REST API; unlike
            # .ingest inline this is a supported production ingestion path.
            target.execute_streaming_ingest(
                quote(target_database, safe=""), quote(table, safe=""),
                stream=io.BytesIO(payload), blob_url=None, stream_format=DataFormat.CSV,
            )
            payload.clear()

    # The current SDK's high-level iterator drops DataSetCompletion, including HasErrors.
    # Consume its incremental wire parser to validate those flags and preserve datetime
    # precision (the typed row adapter converts them to Python's microsecond datetime).
    # https://learn.microsoft.com/kusto/api/rest/response-v2
    for frame in response.streamed_data:
        check_cancelled(cancel_requested)
        frame_type = getattr(frame["FrameType"], "name", frame["FrameType"])
        if completed:
            raise KqlTransferError(f"KQL returned frames after completion for {table}.")
        if frame_type == "DataTable":
            if frame.get("TableKind") != "PrimaryResult":
                for _ in frame["Rows"]:
                    pass
                continue
            if primary_seen:
                raise KqlTransferError(f"KQL returned multiple primary results for table {table}.")
            primary_seen = True
            names = [column["ColumnName"] for column in frame["Columns"]]
            columns = [(c["ColumnName"], c["ColumnType"]) for c in frame["Columns"]]
            if columns != signature(source_schema):
                raise KqlTransferError(
                    f"KQL table {table} changed schema during transfer; freeze writes and retry."
                )
            for row in frame["Rows"]:
                check_cancelled(cancel_requested)
                if len(row) != len(names):
                    raise KqlTransferError(f"KQL returned an incomplete row for table {table}.")
                encoded = _csv_row(row)
                if len(encoded) > limit:
                    raise StagingBudgetError(
                        f"A KQL row in {table} requires {len(encoded)} bytes, above the "
                        f"{limit}-byte memory streaming batch limit. Increase max_memory_bytes "
                        "or use an operator-managed queued ingestion for oversized rows."
                    )
                if len(payload) + len(encoded) > limit:
                    ingest()
                payload.extend(encoded)
                count += 1
        elif frame_type == "DataSetCompletion":
            if frame.get("HasErrors") is not False or frame.get("Cancelled") is not False:
                raise KqlTransferError(
                    f"KQL source query for {table} did not complete: "
                    f"{json.dumps(frame.get('OneApiErrors', frame), default=str)}"
                )
            completed = True
    if not completed or not primary_seen:
        raise KqlTransferError(f"KQL source stream for {table} ended without complete result evidence.")
    if count != expected:
        raise KqlTransferError(
            f"KQL table {table} source count={expected}, streamed={count}; keep writes frozen and retry."
        )
    ingest()
    actual = _count(target, target_database, table)
    if actual != expected:
        raise KqlTransferError(
            f"KQL table {table} source count={expected}, target={actual}. "
            "Stop target writers/update policies and retry the full table."
        )
    _require_quiet_target(target, target_database)
    return count


def copy_database_streaming(
    *,
    source_cluster_uri: str,
    target_cluster_uri: str,
    database: str,
    principal: AuthPrincipal,
    target_principal: AuthPrincipal,
    source_database: str | None = None,
    target_database: str | None = None,
    max_staging_bytes: int | None = None,
    max_memory_bytes: int | None = None,
    exclude: Collection[str] = (),
    on_progress: Callable[[str], None] | None = None,
    on_copied: Callable[[str], None] | None = None,
    on_complete: Callable[[CopyOutcome], None] | None = None,
    on_warning: Callable[[str], None] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> DatabaseCopyResult:
    """Stream a frozen KQL database through this client, never through a target-side source query.

    No disk is used. Requests are at most 1 MiB (below streaming ingestion's 4 MiB limit).
    Supply source/target database item IDs via ``source_database``/``target_database``;
    the existing ``database`` argument remains their backwards-compatible default.
    Source and destination external writers must remain frozen. Target update policies are
    stopped and verified before any row copies and remain stopped after completion; actionable
    warnings are returned and optionally emitted through ``on_warning``. Destination tables
    must already exist. Any failure leaves that table uncheckpointed; retry clears and reloads
    the whole table instead of repeating an append with uncertain completion.
    https://learn.microsoft.com/kusto/api/rest/streaming-ingest
    https://learn.microsoft.com/kusto/management/clear-table-data-command
    """
    memory_budget = resolve_memory_budget(
        max_staging_bytes=max_staging_bytes,
        max_memory_bytes=max_memory_bytes,
    )
    check_cancelled(cancel_requested)
    origin = source_database or database
    destination = target_database or database
    if source_cluster_uri == target_cluster_uri and origin == destination:
        raise KqlTransferError("Source and destination KQL databases must differ.")
    tables = list_tables(source_cluster_uri, origin, principal, exclude=exclude)
    rows = 0
    with (
        _client(source_cluster_uri, principal) as source,
        _client(target_cluster_uri, target_principal) as target,
    ):
        warnings = stop_target_update_policies(
            target, destination, on_warning=on_warning, on_progress=on_progress,
            cancel_requested=cancel_requested,
        )
        for table in tables:
            check_cancelled(cancel_requested)
            if on_progress:
                on_progress(f"Streaming KQL table {origin}.{table}")
            rows += _stream_table(
                source, target, origin, destination, table, memory_budget, cancel_requested,
            )
            check_cancelled(cancel_requested)
            if on_copied:
                on_copied(table)
    outcome = CopyOutcome("kql", empty=rows == 0)
    if on_complete:
        on_complete(outcome)
    result: DatabaseCopyResult = {"tables": len(tables), "rows": rows}
    if warnings:
        result["warnings"] = warnings
    return result


__all__ = [
    "DatabaseCopyResult", "FollowerSource", "KqlTransferError", "copy_database", "copy_database_streaming",
    "follower_source", "list_tables", "stop_database_update_policies", "stop_target_update_policies",
    "stop_update_policies",
]
