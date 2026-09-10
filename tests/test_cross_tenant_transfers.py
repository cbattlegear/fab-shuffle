"""The client, not either tenant's service principal, bridges the tenant boundary."""

from __future__ import annotations

import csv
import io
import json
import os
import sys
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pyodbc
import pytest

from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric.data_stores import TableRef
from fabshuffle.run import CancelledError
from fabshuffle.transfer import bulkcopy, cosmos, files, kql, sqlschema
from fabshuffle.transfer.common import StagingBudgetError, TransferCancelled

SOURCE = ServicePrincipal("source-tenant", "source-app", "source-secret")
TARGET = ServicePrincipal("target-tenant", "target-app", "target-secret")


def test_transport_cancellation_retains_the_existing_lifecycle_exception_identity():
    assert TransferCancelled is CancelledError


class Tokens:
    def __init__(self, value):
        self.value = value

    def storage_token(self):
        return self.value


class OneLake:
    def __init__(self, payload=b"0123456789", root="Files"):
        self.root = root
        self.payload = payload
        self.target = bytearray()
        self.requests = []
        self.flushes = 0
        self.ignore_range = False
        self.reject_read = False
        self.reject_flush = False
        self.mutated = False
        self.shortcut = False
        self.pagination = False
        self.file_name = "data.bin"

    def handle(self, request):
        self.requests.append(request)
        source = request.url.path.startswith("/source")
        assert request.headers["Authorization"] == ("Bearer source" if source else "Bearer target")
        if source:
            assert request.method in {"GET", "HEAD"}, "Source credential must never write."
            if request.url.path == "/source":
                if self.pagination and not request.url.params.get("continuation"):
                    return httpx.Response(200, json={"paths": []}, headers={"x-ms-continuation": "page/2+="})
                entries = [{
                    "name": f"lake/{self.root}/{self.file_name}", "etag": '"frozen"',
                    "contentLength": len(self.payload),
                }]
                if self.shortcut:
                    entries.append({"name": f"lake/{self.root}/shortcut", "isDirectory": True})
                return httpx.Response(200, json={"paths": entries})
            if request.method == "HEAD":
                return httpx.Response(200, headers={
                    "Content-Length": str(len(self.payload)),
                    "ETag": '"changed"' if self.mutated else '"frozen"',
                })
            if self.reject_read:
                return httpx.Response(403, json={
                    "error": {"code": "AuthorizationPermissionMismatch", "message": "Read action was denied"},
                })
            assert request.headers["If-Match"] == '"frozen"'
            begin, end = map(int, request.headers["Range"].removeprefix("bytes=").split("-"))
            if self.ignore_range:
                return httpx.Response(200, content=self.payload)
            return httpx.Response(206, content=self.payload[begin:end + 1], headers={
                "Content-Range": f"bytes {begin}-{end}/{len(self.payload)}",
            })
        if request.method == "PUT":
            assert request.url.params["resource"] == "file"
            self.target.clear()
            return httpx.Response(201)
        if request.method == "PATCH":
            if request.url.params["action"] == "append":
                assert int(request.url.params["position"]) == len(self.target)
                self.target.extend(request.content)
            else:
                if self.reject_flush:
                    return httpx.Response(400, json={
                        "error": {
                            "code": "InvalidFlushPosition", "message": "Uploaded data is not contiguous",
                        },
                    })
                assert int(request.url.params["position"]) == len(self.target)
                self.flushes += 1
            return httpx.Response(200)
        assert request.method == "HEAD"
        return httpx.Response(200, headers={"Content-Length": str(len(self.target))})


def install_onelake(monkeypatch, service):
    client = httpx.Client
    monkeypatch.setattr(
        files.httpx, "Client",
        lambda **kwargs: client(transport=httpx.MockTransport(service.handle), **kwargs),
    )
    monkeypatch.setattr(files, "_azcopy", lambda *a, **k: pytest.fail("Native copy must not bridge tenants"))


def relay_files(**kwargs):
    root = kwargs.pop("root", "Files")
    return files.copy_tree_streaming(
        source_path=f"https://onelake.dfs.fabric.microsoft.com/source/lake/{root}",
        target_path=f"https://onelake.dfs.fabric.microsoft.com/target/lake/{root}",
        tokens=Tokens("source"), target_tokens=Tokens("target"), **kwargs,
    )


def test_onelake_chunks_route_each_credential_and_bound_bytes(monkeypatch):
    root = "Files"
    service = OneLake(root=root)
    service.pagination = True
    service.shortcut = True
    install_onelake(monkeypatch, service)
    completed = []
    outcome = relay_files(
        root=root, max_staging_bytes=6, exclude_paths=[f"{root}/shortcut"], on_complete=completed.append,
    )
    assert service.target == service.payload
    assert completed == [outcome]
    assert not outcome.empty
    assert service.flushes == 1
    chunks = [r.content for r in service.requests if r.url.params.get("action") == "append"]
    assert len(chunks) == 4
    assert max(map(len, chunks)) <= 6
    assert any(r.url.params.get("continuation") == "page/2+=" for r in service.requests)
    assert not any("shortcut" in r.url.path for r in service.requests)
    assert not any(r.method == "PUT" and r.url.path.endswith(root) for r in service.requests)


def test_onelake_disk_budget_does_not_increase_http_chunk_size(monkeypatch):
    service = OneLake(payload=b"0123456789abcdef")
    install_onelake(monkeypatch, service)

    relay_files(max_memory_bytes=8, max_disk_staging_bytes=10 * 1024 * 1024)

    chunks = [r.content for r in service.requests if r.url.params.get("action") == "append"]
    assert chunks
    assert max(map(len, chunks)) == 4


@pytest.mark.parametrize("kind", ["lakehouse", "files"])
def test_unvalidated_delta_layout_fails_before_upload_or_completion(monkeypatch, kind, tmp_path):
    service = OneLake(root="Tables")
    install_onelake(monkeypatch, service)
    completed = []
    with pytest.raises(files.DeltaValidationRequired, match="outside a validated Delta table"):
        files.copy_tree_streaming(
            source_path="https://onelake.dfs.fabric.microsoft.com/source/lake/Tables",
            target_path="https://onelake.dfs.fabric.microsoft.com/target/lake/Tables",
            tokens=Tokens("source"), target_tokens=Tokens("target"), kind=kind,
            on_complete=completed.append, scratch_dir=tmp_path,
        )
    assert all(request.url.path.startswith("/source") for request in service.requests)
    assert not completed


def test_ordinary_files_named_tables_are_not_mistaken_for_managed_tables(monkeypatch):
    service = OneLake()
    service.file_name = "Tables"
    install_onelake(monkeypatch, service)
    assert not relay_files().empty
    assert service.target == service.payload


@pytest.mark.parametrize("failure", ["ignore_range", "reject_read", "reject_flush", "mutated"])
def test_onelake_failure_never_reports_completion_and_retains_service_error(monkeypatch, failure):
    service = OneLake()
    setattr(service, failure, True)
    install_onelake(monkeypatch, service)
    completed = []
    with pytest.raises(files.FileTransferError) as error:
        relay_files(max_staging_bytes=8, on_complete=completed.append)
    assert not completed
    if failure == "reject_read":
        assert "AuthorizationPermissionMismatch" in str(error.value)
        assert "Read action was denied" in str(error.value)
    if failure == "reject_flush":
        assert "InvalidFlushPosition" in str(error.value)
    if failure in {"ignore_range", "mutated"}:
        assert not service.target


def test_onelake_cancel_then_retry_recreates_file_instead_of_repeating_append(monkeypatch):
    service = OneLake()
    install_onelake(monkeypatch, service)
    completed = []
    with pytest.raises(CancelledError):
        relay_files(
            max_staging_bytes=8, cancel_requested=lambda: bool(service.target),
            on_complete=completed.append,
        )
    assert not completed and 0 < len(service.target) < len(service.payload)
    relay_files(max_staging_bytes=8, on_complete=completed.append)
    assert service.target == service.payload
    assert len(completed) == 1


def test_onelake_shortcut_exclusion_does_not_skip_distinct_case_sensitive_file(monkeypatch):
    service = OneLake()
    service.shortcut = True
    service.file_name = "Shortcut"
    install_onelake(monkeypatch, service)
    assert not relay_files(exclude_paths=["shortcut"]).empty
    assert service.target == service.payload
    assert not any(request.url.path.endswith("/shortcut") for request in service.requests)


@pytest.mark.parametrize("budget", [0, -1, True, 1.5])
def test_invalid_budget_fails_before_authentication(budget):
    with pytest.raises(ValueError, match="positive integer"):
        relay_files(max_staging_bytes=budget)


class SqlCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = iter(())

    def execute(self, query, *args):
        owner = self.connection
        owner.statements.append((query, args))
        if "FROM sys.columns" in query:
            self.rows = iter(owner.columns)
        elif "FROM sys.foreign_keys" in query:
            self.rows = iter(owner.foreign_keys)
        elif "FROM sys.triggers" in query:
            self.rows = iter(owner.triggers)
        elif "COUNT_BIG" in query:
            count = owner.count_override if owner.count_override is not None else len(owner.rows)
            self.rows = iter([(count,)])
        elif query.startswith("SELECT "):
            assert owner.side == "source"
            self.rows = iter(owner.rows)
        elif query.startswith(("TRUNCATE", "DELETE")):
            assert owner.side == "target"
            owner.rows.clear()
        return self

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return next(self.rows, None)

    def executemany(self, query, rows):
        assert self.connection.side == "target"
        if self.connection.fail_insert:
            raise pyodbc.Error("42000", "NativeInsertDenied: INSERT permission denied")
        self.connection.batches.append(list(rows))
        self.connection.rows.extend(rows)

    def close(self):
        pass


class SqlConnection:
    def __init__(self, side, rows):
        self.side = side
        self.rows = list(rows)
        self.columns = [
            ("id", True, False, 0, "bigint"),
            ("amount", False, False, 0, "decimal"),
            ("at", False, False, 0, "datetime2"),
        ]
        self.foreign_keys = []
        self.triggers = []
        self.statements = []
        self.batches = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.fail_insert = False
        self.count_override = None

    def cursor(self):
        return SqlCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1
        self.rows.clear()

    def close(self):
        self.closed = True


def install_sql(monkeypatch, rows):
    source = SqlConnection("source", rows)
    target = SqlConnection("target", [])
    credentials = []

    def connect(server, database, tokens, **kwargs):
        credentials.append((server, database, tokens))
        assert tokens == server
        return source if server == "source" else target

    monkeypatch.setattr(bulkcopy.sqlschema, "connect", connect)
    return source, target, credentials


def relay_sql(**kwargs):
    return bulkcopy.copy_tables_streaming(
        source_server="source", source_database="original",
        target_server="target", target_database="copy",
        tokens="source", target_tokens="target", tables=[TableRef(name="Odd]Table", schema="dbo")],
        **kwargs,
    )


def test_sql_streams_typed_rows_to_correct_tenant_and_preserves_identity_precision(monkeypatch):
    rows = [
        (i, Decimal("12345678901234567890.123456789"), "2026-01-01T12:34:56.1234567")
        for i in range(1001)
    ]
    source, target, credentials = install_sql(monkeypatch, rows)
    checkpoints = []
    completed = []
    assert relay_sql(
        on_copied=checkpoints.append, on_complete=completed.append, target_type="Warehouse",
    ) == []
    assert target.rows == rows
    assert len(target.batches) == 2
    assert target.commits == 1
    assert credentials == [("source", "original", "source"), ("target", "copy", "target")]
    assert all("OFFSET" not in sql for sql, _ in source.statements)
    assert sum("CONVERT(nvarchar(48)" in sql for sql, _ in source.statements) == 1
    assert any(sql == "SET IDENTITY_INSERT [dbo].[Odd]]Table] ON" for sql, _ in target.statements)
    assert any(sql.startswith("DBCC CHECKIDENT") for sql, _ in target.statements)
    assert checkpoints == ["dbo.Odd]Table"]
    assert len(completed) == 1
    assert source.closed and target.closed


def test_sql_indivisible_oversize_row_fails_without_checkpoint(monkeypatch):
    source, target, _ = install_sql(monkeypatch, [(1, Decimal("1"), "x" * 1000)])
    checkpoints = []
    with pytest.raises(StagingBudgetError, match="memory budget"):
        relay_sql(max_staging_bytes=128, on_copied=checkpoints.append)
    assert not target.rows and not checkpoints and target.rollbacks == 1
    assert source.closed and target.closed


def test_sql_target_constraints_and_triggers_are_restored_before_checkpoint(monkeypatch):
    _, target, _ = install_sql(monkeypatch, [(1, Decimal("1"), None)])
    target.foreign_keys = [("dbo", "Odd]Table", "FK_parent", False, False, "dbo", "Odd]Table")]
    target.triggers = [("dbo", "Odd]Table", "AuditWrites")]
    checkpoints = []

    def checkpoint(name):
        assert target.commits == 1
        statements = [sql for sql, _ in target.statements]
        assert statements[-2:] == [
            "ALTER TABLE [dbo].[Odd]]Table] WITH CHECK CHECK CONSTRAINT [FK_parent]",
            "ENABLE TRIGGER [dbo].[AuditWrites] ON [dbo].[Odd]]Table]",
        ]
        checkpoints.append(name)

    assert relay_sql(on_copied=checkpoint) == []
    statements = [sql for sql, _ in target.statements]
    assert statements.index("DISABLE TRIGGER [dbo].[AuditWrites] ON [dbo].[Odd]]Table]") < next(
        i for i, sql in enumerate(statements) if sql.startswith("TRUNCATE")
    )
    assert checkpoints == ["dbo.Odd]Table"]


@pytest.mark.parametrize("failure", ["native", "truncated"])
def test_sql_never_checkpoints_failed_or_incomplete_rows(monkeypatch, failure):
    _, target, _ = install_sql(monkeypatch, [(1, Decimal("1"), None)])
    target.fail_insert = failure == "native"
    target.count_override = 5 if failure == "truncated" else None
    checkpoints = []
    warnings = relay_sql(on_copied=checkpoints.append)
    assert len(warnings) == 1 and not checkpoints
    assert not target.rows and target.commits == 0
    assert ("NativeInsertDenied" if failure == "native" else "row counts differ") in warnings[0]


def test_existing_sql_entry_point_selects_bounded_paired_transport(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(bulkcopy, "copy_tables_streaming", lambda **kwargs: calls.append(kwargs) or [])
    bulkcopy.copy_tables(
        source_server="source", source_database="original", target_server="target", target_database="copy",
        tokens="source", target_tokens="target", tables=[], scratch_dir=tmp_path, max_staging_bytes=123,
    )
    assert calls[0]["target_tokens"] == "target"
    assert calls[0]["max_staging_bytes"] == 123
    assert not list(tmp_path.iterdir())


class KqlResult(list):
    columns = (SimpleNamespace(column_name="Value", column_type="string"),)


class KqlClient:
    def __init__(self, side, rows):
        self.side = side
        self.rows = rows
        self.commands = []
        self.queries = []
        self.payloads = []
        self.partial_error = False
        self.omit_completion = False
        self.truncate = False
        self.uncertain_ingest = False
        self.properties = None
        self.policies = {}
        self.ignore_policy_alter = False
        self.policy_error = None
        self.policy_changes = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def execute_query(self, database, query, properties):
        self.queries.append((database, query))
        assert database == ("original" if self.side == "source" else "copy")
        return SimpleNamespace(primary_results=[KqlResult([(len(self.rows),)])])

    def execute_mgmt(self, database, command):
        assert self.side == "target" and database == "copy"
        if command == ".show tables | project TableName":
            tables = list(dict.fromkeys(["Events", *self.policies]))
            return SimpleNamespace(primary_results=[KqlResult([{"TableName": name} for name in tables])])
        if command == ".show table * policy update":
            return SimpleNamespace(primary_results=[KqlResult([
                {"EntityName": name, "Policy": json.dumps(rules)}
                for name, rules in self.policies.items()
            ])])
        if command.startswith(".show table ") and command.endswith(" policy update"):
            identifier = command.removeprefix(".show table ").removesuffix(" policy update")
            name = json.loads(identifier[1:-1])
            return SimpleNamespace(primary_results=[KqlResult([
                {"EntityName": name, "Policy": json.dumps(self.policies.get(name, []))},
            ])])
        if command.startswith(".alter table "):
            self.commands.append(command)
            if self.policy_error:
                raise self.policy_error
            identifier, _, literal = command.removeprefix(".alter table ").partition(" policy update ")
            name = json.loads(identifier[1:-1])
            assert literal.startswith('h"')
            rules = json.loads(json.loads(literal[1:]))
            self.policy_changes.append((name, rules))
            if not self.ignore_policy_alter:
                self.policies[name] = [
                    {**rule, "OwnerPrincipalDetails": {"ObjectId": "destination-owner"}}
                    for rule in rules
                ]
            return SimpleNamespace(primary_results=[KqlResult([])])
        self.commands.append(command)
        assert command == '.clear table ["Events"] data'
        self.rows.clear()

    def execute_streaming_query(self, database, query, timeout, properties):
        assert self.side == "source" and database == "original" and query == 'table("Events", "all")'
        self.queries.append((database, query))
        self.properties = properties

        def frames():
            yield {
                "FrameType": "DataTable", "TableKind": "PrimaryResult",
                "Columns": [{"ColumnName": "Value", "ColumnType": "string"}],
                "Rows": iter(self.rows[:-1] if self.truncate else self.rows),
            }
            if not self.omit_completion:
                yield {
                    "FrameType": "DataSetCompletion", "HasErrors": self.partial_error, "Cancelled": False,
                    "OneApiErrors": [{"code": "E_QUERY_RESULT_SET_TOO_LARGE", "message": "Limit reached"}],
                }
        return SimpleNamespace(streamed_data=frames())

    def execute_streaming_ingest(self, database, table, *, stream, blob_url, stream_format):
        assert self.side == "target"
        assert all(rule["IsEnabled"] is False for rules in self.policies.values() for rule in rules)
        assert database == "copy" and table == "Events" and blob_url is None
        payload = stream.read()
        self.payloads.append(payload)
        self.rows.extend(list(csv.reader(io.StringIO(payload.decode("utf-8")))))
        if self.uncertain_ingest:
            raise RuntimeError("ServiceUnavailable: response lost after ingest")


def install_kql(monkeypatch, rows):
    source = KqlClient("source", rows)
    target = KqlClient("target", [])
    credentials = []

    def client(uri, principal):
        credentials.append((uri, principal))
        assert principal is (SOURCE if uri == "source" else TARGET)
        return source if uri == "source" else target

    def list_tables(uri, database, principal, **kwargs):
        assert uri == "source" and database == "original" and principal is SOURCE
        return ["Events"]

    monkeypatch.setattr(kql, "_client", client)
    monkeypatch.setattr(kql, "list_tables", list_tables)
    return source, target, credentials


def relay_kql(**kwargs):
    return kql.copy_database_streaming(
        source_cluster_uri="source", target_cluster_uri="target", database="original", target_database="copy",
        principal=SOURCE, target_principal=TARGET, **kwargs,
    )


def test_kql_client_streams_complete_rows_with_destination_only_ingestion(monkeypatch):
    rows = [[str(i)] for i in range(40)]
    source, target, credentials = install_kql(monkeypatch, rows)
    checkpoints = []
    assert relay_kql(max_staging_bytes=16, on_copied=checkpoints.append) == {"tables": 1, "rows": 40}
    assert target.rows == rows
    assert credentials == [("source", SOURCE), ("target", TARGET)]
    assert all(len(payload) <= 16 for payload in target.payloads)
    assert source.properties.get_option("notruncation", None) is True
    assert source.properties.get_option("results_progressive_enabled", None) is False
    assert checkpoints == ["Events"]
    assert all("cluster(" not in query for _, query in target.queries)
    assert all(query.startswith('table("Events", "all")') for _, query in source.queries + target.queries)


def test_kql_streaming_uses_explicit_database_ids_not_display_name(monkeypatch):
    source, target, _ = install_kql(monkeypatch, [["one"]])
    result = kql.copy_database_streaming(
        source_cluster_uri="source", target_cluster_uri="target",
        database="Unreliable display name", source_database="original", target_database="copy",
        principal=SOURCE, target_principal=TARGET,
    )
    assert result == {"tables": 1, "rows": 1}
    assert all(database == "original" for database, _ in source.queries)
    assert all(database == "copy" for database, _ in target.queries)


def test_kql_combined_entry_point_forwards_explicit_paired_database_ids(monkeypatch):
    calls = []
    monkeypatch.setattr(
        kql, "copy_database_streaming",
        lambda **kwargs: calls.append(kwargs) or {"tables": 1, "rows": 3},
    )
    result = kql.copy_database(
        source_cluster_uri="source", target_cluster_uri="target", database="Display",
        source_database="source-item-id", target_database="target-item-id",
        principal=SOURCE, target_principal=TARGET,
    )
    assert result == {"tables": 1, "rows": 3}
    assert calls[0]["database"] == "Display"
    assert calls[0]["source_database"] == "source-item-id"
    assert calls[0]["target_database"] == "target-item-id"


@pytest.mark.parametrize("failure", ["partial_error", "omit_completion", "truncate"])
def test_kql_requires_explicit_untruncated_completion_and_never_checkpoints_failure(monkeypatch, failure):
    source, _, _ = install_kql(monkeypatch, [["one"], ["two"], ["three"]])
    setattr(source, failure, True)
    checkpoints = []
    with pytest.raises(kql.KqlTransferError) as error:
        relay_kql(max_staging_bytes=8, on_copied=checkpoints.append)
    assert not checkpoints
    if failure == "partial_error":
        assert "E_QUERY_RESULT_SET_TOO_LARGE" in str(error.value)
        assert "Limit reached" in str(error.value)


def test_kql_uncertain_ingest_retries_full_table_without_duplicate_append(monkeypatch):
    _, target, _ = install_kql(monkeypatch, [["one"], ["two"], ["three"]])
    target.uncertain_ingest = True
    checkpoints = []
    with pytest.raises(RuntimeError, match="response lost"):
        relay_kql(max_staging_bytes=8, on_copied=checkpoints.append)
    assert target.rows and not checkpoints
    target.uncertain_ingest = False
    assert relay_kql(max_staging_bytes=8, on_copied=checkpoints.append)["rows"] == 3
    assert target.rows == [["one"], ["two"], ["three"]]
    assert len(target.commands) == 2
    assert checkpoints == ["Events"]


def test_kql_oversized_record_is_not_ingested(monkeypatch):
    _, target, _ = install_kql(monkeypatch, [["too large"]])
    with pytest.raises(StagingBudgetError):
        relay_kql(max_staging_bytes=3)
    assert not target.payloads


def test_kql_stops_all_target_update_policies_without_changing_rule_logic(monkeypatch):
    _, target, _ = install_kql(monkeypatch, [["one"]])
    rules = [
        {
            "IsEnabled": True, "Source": "Events", "Query": 'Events | where Value == "quoted\\\\value"',
            "IsTransactional": True, "PropagateIngestionProperties": False,
            "OwnerPrincipalDetails": {"ObjectId": "previous-target-owner"},
        },
        {
            "IsEnabled": False, "Source": "Events", "Query": "Events | where false",
            "IsTransactional": False, "PropagateIngestionProperties": True,
        },
    ]
    target.policies = {"AlreadyCopied": rules}
    warnings = []
    checkpoints = []
    completed = []
    result = relay_kql(
        on_warning=warnings.append, on_copied=checkpoints.append, on_complete=completed.append,
    )
    assert target.policy_changes == [("AlreadyCopied", [
        {**{key: value for key, value in rule.items() if key != "OwnerPrincipalDetails"}, "IsEnabled": False}
        for rule in rules
    ])]
    assert rules[0]["IsEnabled"] is True, "The read-back source objects must not be mutated."
    assert target.commands[0].startswith('.alter table ["AlreadyCopied"] policy update ')
    assert target.commands[1] == '.clear table ["Events"] data'
    assert target.rows == [["one"]] and checkpoints == ["Events"] and len(completed) == 1
    assert result["tables"] == 1 and result["warnings"] == warnings
    assert "After cutover" in warnings[0] and "IsEnabled=true only for rules" in warnings[0]


def test_kql_policy_readback_must_prove_stopped_before_copying_rows(monkeypatch):
    _, target, _ = install_kql(monkeypatch, [["one"]])
    target.policies = {"AlreadyCopied": [{"IsEnabled": True, "Source": "Events", "Query": "Events"}]}
    target.ignore_policy_alter = True
    with pytest.raises(kql.KqlTransferError, match="was not preserved in its stopped state"):
        relay_kql(on_copied=lambda name: pytest.fail("Must not checkpoint with an active update policy"))
    assert not any(command.startswith(".clear") for command in target.commands)
    assert not target.payloads


def test_kql_policy_failure_preserves_native_error_and_does_not_copy_rows(monkeypatch):
    _, target, _ = install_kql(monkeypatch, [["one"]])
    target.policies = {"Events": [{"IsEnabled": True, "Source": "Input", "Query": "Input"}]}
    target.policy_error = RuntimeError("AuthorizationFailed: policy alteration requires Table Admin")
    with pytest.raises(RuntimeError, match="AuthorizationFailed: policy alteration requires Table Admin"):
        relay_kql()
    assert not target.payloads


def test_kql_already_stopped_policy_stays_unchanged_and_still_gets_activation_instructions(monkeypatch):
    _, target, _ = install_kql(monkeypatch, [["one"]])
    rules = [{"IsEnabled": False, "Source": "Events", "Query": "Events"}]
    target.policies = {"AlreadyCopied": rules}
    result = relay_kql()
    assert not target.policy_changes
    assert target.policies["AlreadyCopied"] == rules
    assert "remain stopped" in result["warnings"][0]


@pytest.mark.parametrize("rules", [{}, False, 0, "", [None]])
def test_kql_malformed_policy_is_not_mistaken_for_an_empty_policy(monkeypatch, rules):
    _, target, _ = install_kql(monkeypatch, [["one"]])
    target.policies = {"Events": rules}
    with pytest.raises(kql.KqlTransferError, match="unrecognized update policy"):
        relay_kql()
    assert not target.commands and not target.payloads


def test_kql_policy_readback_must_preserve_query_and_not_only_disabled_flag(monkeypatch):
    _, target, _ = install_kql(monkeypatch, [["one"]])
    target.policies = {"Events": [{"IsEnabled": True, "Source": "Input", "Query": "Input"}]}
    execute = target.execute_mgmt

    def change_query(database, command):
        result = execute(database, command)
        if command.startswith(".alter table"):
            target.policies["Events"][0]["Query"] = "Input | where false"
        return result

    monkeypatch.setattr(target, "execute_mgmt", change_query)
    with pytest.raises(kql.KqlTransferError, match="was not preserved"):
        relay_kql()
    assert not target.payloads


def test_kql_stops_policies_even_when_no_source_tables_remain_to_copy(monkeypatch):
    _, target, _ = install_kql(monkeypatch, [])
    monkeypatch.setattr(kql, "list_tables", lambda *args, **kwargs: [])
    target.policies = {"Events": [{"IsEnabled": True, "Source": "Input", "Query": "Input"}]}
    result = relay_kql()
    assert result["tables"] == 0 and result["rows"] == 0 and result["warnings"]
    assert target.policies["Events"][0]["IsEnabled"] is False
    assert not target.payloads


def test_kql_schema_only_policy_stop_opens_only_a_destination_authenticated_client(monkeypatch):
    source, target, credentials = install_kql(monkeypatch, [])
    target.policies = {"Events": [{"IsEnabled": True, "Source": "Input", "Query": "Input"}]}
    warnings = kql.stop_database_update_policies(
        target_cluster_uri="target", target_database="copy", target_principal=TARGET,
    )
    assert credentials == [("target", TARGET)]
    assert target.policies["Events"][0]["IsEnabled"] is False and warnings
    assert not source.queries and not target.payloads


def test_kql_stop_update_policies_returns_table_names_for_replay_safe_activation_tracking(monkeypatch):
    source, target, credentials = install_kql(monkeypatch, [])
    target.policies = {
        "Events": [{"IsEnabled": True, "Source": "Input", "Query": "Input", "IsTransactional": True}],
        "AlreadyStopped": [{"IsEnabled": False, "Source": "Input", "Query": "Input | where false"}],
    }
    progress = []
    for _ in range(2):
        assert kql.stop_update_policies("target", "copy", TARGET, progress.append) == [
            "Events", "AlreadyStopped",
        ]
    assert credentials == [("target", TARGET), ("target", TARGET)]
    assert len(target.policy_changes) == 1
    assert target.policies["Events"][0]["IsTransactional"] is True
    assert "IsEnabled=true only for rules you intend to resume" in progress[0]
    assert not source.queries and not target.payloads


def test_kql_stop_update_policies_preserves_native_alter_failure(monkeypatch):
    _, target, credentials = install_kql(monkeypatch, [])
    target.policies = {"Events": [{"IsEnabled": True, "Source": "Input", "Query": "Input"}]}
    target.policy_error = RuntimeError("AuthorizationFailed: Table Admin is required")
    with pytest.raises(RuntimeError, match="AuthorizationFailed: Table Admin is required"):
        kql.stop_update_policies("target", "copy", TARGET)
    assert credentials == [("target", TARGET)]
    assert not target.payloads


def test_kql_streaming_guard_catches_policy_reenabled_after_preparation(monkeypatch):
    _, target, _ = install_kql(monkeypatch, [["one"]])
    target.policies = {"Derived": [{"IsEnabled": False, "Source": "Events", "Query": "Events"}]}

    def reenable_before_table(message):
        if message.startswith("Streaming KQL table"):
            target.policies["Derived"][0]["IsEnabled"] = True

    with pytest.raises(kql.KqlTransferError, match="Target update policies are active"):
        relay_kql(
            on_progress=reenable_before_table,
            on_copied=lambda table: pytest.fail("No table may be checkpointed after policy re-enablement"),
        )
    assert not target.commands and not target.payloads


def test_kql_streaming_guard_checks_again_before_each_ingestion(monkeypatch):
    source, target, _ = install_kql(monkeypatch, [["one"]])
    target.policies = {"Derived": [{"IsEnabled": False, "Source": "Events", "Query": "Events"}]}
    execute = source.execute_streaming_query

    def reenable_during_query(*args, **kwargs):
        response = execute(*args, **kwargs)

        def frames():
            for frame in response.streamed_data:
                if frame["FrameType"] == "DataSetCompletion":
                    target.policies["Derived"][0]["IsEnabled"] = True
                yield frame

        return SimpleNamespace(streamed_data=frames())

    monkeypatch.setattr(source, "execute_streaming_query", reenable_during_query)
    with pytest.raises(kql.KqlTransferError, match="Target update policies are active"):
        relay_kql(on_copied=lambda table: pytest.fail("Must not checkpoint a policy race"))
    assert target.commands == ['.clear table ["Events"] data']
    assert not target.payloads


def test_kql_policy_preparation_checks_cancellation_after_read_before_alter(monkeypatch):
    _, target, _ = install_kql(monkeypatch, [["one"]])
    target.policies = {"Events": [{"IsEnabled": True, "Source": "Input", "Query": "Input"}]}
    cancelled = False
    execute = target.execute_mgmt

    def cancel_after_read(database, command):
        nonlocal cancelled
        result = execute(database, command)
        if command == '.show table ["Events"] policy update':
            cancelled = True
        return result

    monkeypatch.setattr(target, "execute_mgmt", cancel_after_read)
    with pytest.raises(CancelledError):
        relay_kql(cancel_requested=lambda: cancelled)
    assert not target.commands and not target.payloads


def test_cosmos_preserves_multiline_native_error_and_keeps_permission_advice_side_specific():
    message = "Forbidden\nMissingDataPlaneRole: the requested database is outside the permitted scope"
    detail = cosmos._describe(SimpleNamespace(status_code=403, message=message))
    assert message in detail
    assert "read access for the source service principal" in detail
    assert "write access for the destination service principal" in detail
    assert "only on their respective databases" in detail
    assert "Grant Read and Write respectively" in detail
    assert "rather than configuring the data plane separately from the item" in detail


def test_cosmos_keeps_native_error_details_after_an_activity_trace_line():
    message = "Forbidden\nActivityId: abc\nMissingDataPlaneRole: reader permission is required"
    detail = cosmos._describe(SimpleNamespace(status_code=403, message=message))
    assert "ActivityId" not in detail
    assert "MissingDataPlaneRole: reader permission is required" in detail


def test_kql_checks_real_sdk_wire_completion_frame_not_only_primary_results(monkeypatch):
    from azure.kusto.data.streaming_response import JsonTokenReader, StreamingDataSetEnumerator

    source, target, _ = install_kql(monkeypatch, [["one"]])
    wire = [
        {"FrameType": "DataSetHeader", "IsProgressive": False, "Version": "v2.0"},
        {
            "FrameType": "DataTable", "TableId": 0,
            "TableKind": "PrimaryResult", "TableName": "PrimaryResult",
            "Columns": [{"ColumnName": "Value", "ColumnType": "string"}], "Rows": [["one"]],
        },
        {
            "FrameType": "DataSetCompletion", "HasErrors": True, "Cancelled": False,
            "OneApiErrors": [{"code": "PartialFailure", "message": "The source stream failed"}],
        },
    ]
    response = SimpleNamespace(streamed_data=StreamingDataSetEnumerator(
        JsonTokenReader(io.BytesIO(json.dumps(wire).encode("utf-8"))),
    ))
    monkeypatch.setattr(source, "execute_streaming_query", lambda *a, **kw: response)
    with pytest.raises(kql.KqlTransferError, match="PartialFailure"):
        relay_kql()
    assert not target.rows


def test_kql_csv_preserves_newlines_dynamic_and_seven_digit_timestamp():
    values = ["a\rb\nc", {"id": 9007199254740993}, "2026-01-01T12:34:56.1234567Z", None, True]
    encoded = kql._csv_row(values)
    decoded = next(csv.reader(io.StringIO(encoded.decode("utf-8"), newline="")))
    assert decoded == [
        "a\rb\nc", '{"id":9007199254740993}', "2026-01-01T12:34:56.1234567Z", "", "true",
    ]


@pytest.mark.parametrize("exclude_security", [None, True, False])
def test_schema_uses_source_principal_and_destination_tokens_with_explicit_security_policy(
    monkeypatch, tmp_path, exclude_security,
):
    calls = []
    monkeypatch.setattr(
        sqlschema, "wait_for_database",
        lambda server, db, tokens, **kw: calls.append(("wait", server, db, tokens)),
    )
    monkeypatch.setattr(sqlschema, "extract_dacpac", lambda **kw: calls.append(("extract", kw)))
    monkeypatch.setattr(
        sqlschema, "script_dacpac",
        lambda *a, **kw: calls.append(("script", kw)) or _write_script(a[1], "SELECT 1"),
    )
    monkeypatch.setattr(sqlschema, "apply_script", lambda *a, **kw: calls.append(("apply", kw)) or [])
    assert sqlschema.transfer_schema(
        source_server="source", target_server="target", database="original", target_database="copy",
        principal=SOURCE, tokens="source", target_tokens="target", scratch_dir=tmp_path,
        source_type="Warehouse",
        **({} if exclude_security is None else {"exclude_security": exclude_security}),
    ) == []
    assert calls[:2] == [("wait", "source", "original", "source"), ("wait", "target", "copy", "target")]
    assert calls[2][1]["principal"] is SOURCE
    assert calls[3][1]["tokens"] == "target" and calls[3][1]["database"] == "copy"
    assert calls[3][1]["exclude_security"] is (exclude_security is not False)
    assert calls[4][1]["tokens"] == "target" and calls[4][1]["database"] == "copy"


def _write_script(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_schema_unpack_path_is_disk_bounded_without_target_aware_script(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(sqlschema, "wait_for_database", lambda *a, **kw: None)

    def extract(**kwargs):
        calls.append(("extract", kwargs))
        kwargs["output"].write_bytes(b"dacpac")

    def unpack(dacpac, destination, **kwargs):
        calls.append(("unpack", kwargs))
        assert dacpac.parent == kwargs["staging_root"]
        return _write_script(destination / "Deploy.sql", "SELECT 1")

    monkeypatch.setattr(sqlschema, "extract_dacpac", extract)
    monkeypatch.setattr(sqlschema, "unpack_dacpac", unpack)
    monkeypatch.setattr(sqlschema, "script_dacpac", lambda *a, **kw: pytest.fail("legacy script changed"))
    monkeypatch.setattr(sqlschema, "apply_script", lambda *a, **kw: calls.append(("apply", kw)) or [])

    assert sqlschema.transfer_schema(
        source_server="source", target_server="target", database="original",
        principal=SOURCE, tokens="source", scratch_dir=tmp_path, source_type="Warehouse",
        max_memory_bytes=1024, max_disk_staging_bytes=2048,
    ) == []

    assert calls[0][1]["max_disk_staging_bytes"] == 2048
    assert calls[1][1]["max_disk_staging_bytes"] == 2048
    assert calls[2][1]["max_memory_bytes"] == 1024


def test_schema_rewrites_source_workspace_item_server_and_catalog_before_apply(monkeypatch, tmp_path):
    script = tmp_path / "Deploy.sql"
    script.write_text(
        "/* DacFx header */\nGO\n"
        "CREATE VIEW dbo.CrossReference AS SELECT * "
        "FROM [SOURCE.EXAMPLE].[SourceCatalog].[dbo].[Orders];\nGO\n"
        "CREATE VIEW dbo.Storage AS SELECT "
        "'https://onelake.dfs.fabric.microsoft.com/old-workspace/old-item/Files/file' AS path;",
        encoding="utf-8",
    )
    applied = []
    monkeypatch.setattr(sqlschema, "wait_for_database", lambda *a, **kw: None)
    monkeypatch.setattr(sqlschema, "extract_dacpac", lambda **kw: None)
    monkeypatch.setattr(
        sqlschema, "script_dacpac",
        lambda *a, **kw: _write_script(a[1], script.read_text(encoding="utf-8")),
    )
    monkeypatch.setattr(
        sqlschema, "apply_script",
        lambda path, **kw: applied.append((path.read_text(encoding="utf-8"), kw)) or [],
    )
    assert sqlschema.transfer_schema(
        source_server="source.example", target_server="target.example", database="SourceCatalog",
        target_database="DestinationCatalog", principal=SOURCE, tokens="source", target_tokens="target",
        scratch_dir=tmp_path, source_type="SQLDatabase",
        id_map={"old-workspace": "new-workspace", "old-item": "new-item"},
        source_identifiers={"old-workspace", "old-item", "source.example", "SourceCatalog"},
    ) == []
    assert len(applied) == 1
    text, options = applied[0]
    assert "[target.example].[DestinationCatalog]" in text
    assert "/new-workspace/new-item/Files/file" in text
    assert options["tokens"] == "target" and options["database"] == "DestinationCatalog"


def test_schema_unresolved_late_batch_prevents_all_schema_execution(monkeypatch, tmp_path):
    script = tmp_path / "Deploy.sql"
    script.write_text(
        "/* DacFx header */\nGO\nCREATE TABLE dbo.First (id int);\nGO\n"
        "CREATE VIEW dbo.Late AS SELECT 'SOURCE-NOT-MIGRATED' AS dependency;",
        encoding="utf-8",
    )
    monkeypatch.setattr(sqlschema, "wait_for_database", lambda *a, **kw: None)
    monkeypatch.setattr(sqlschema, "extract_dacpac", lambda **kw: None)
    monkeypatch.setattr(
        sqlschema, "script_dacpac",
        lambda *a, **kw: _write_script(a[1], script.read_text(encoding="utf-8")),
    )
    monkeypatch.setattr(
        sqlschema, "apply_script", lambda *a, **kw: pytest.fail("Must validate the entire script first"),
    )
    with pytest.raises(sqlschema.SchemaTransferError, match="source-not-migrated"):
        sqlschema.transfer_schema(
            source_server="source.example", target_server="target.example", database="original",
            target_database="copy", principal=SOURCE, tokens="source", target_tokens="target",
            scratch_dir=tmp_path, source_type="SQLDatabase", id_map={},
            source_identifiers={"source-not-migrated"},
        )


def test_schema_rewrite_is_single_pass_and_protects_complete_destination_endpoints():
    source_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    destination_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    old_endpoint = f"{source_id}.source.example"
    new_endpoint = f"new-{source_id}.target.example"
    script = f"SELECT '{old_endpoint.upper()}', '{source_id}';"
    rebound = sqlschema.rewrite_schema_script(
        script, id_map={old_endpoint: new_endpoint, source_id: destination_id},
        source_identifiers={source_id, old_endpoint},
    )
    assert rebound == f"SELECT '{new_endpoint}', '{destination_id}';"


def test_schema_short_target_value_cannot_mask_longer_unresolved_source_identifier():
    with pytest.raises(sqlschema.SchemaTransferError, match="new-unmigrated-id"):
        sqlschema.rewrite_schema_script(
            "SELECT 'old', 'new-unmigrated-id';",
            id_map={"old": "new"}, source_identifiers={"old", "new-unmigrated-id"},
        )


def test_schema_self_mapping_and_mapping_to_another_source_identity_are_not_rebinding():
    for mapping in ({"source-one": "source-one"}, {"source-one": "source-two"}):
        with pytest.raises(sqlschema.SchemaTransferError, match="source identities"):
            sqlschema.rewrite_schema_script(
                "SELECT 'source-one';", id_map=mapping, source_identifiers={"source-one", "source-two"},
            )


@pytest.mark.parametrize(
    "outcome",
    ["success", "extract-error", "script-error", "apply-error", "cancel", "budget", "rewrite-budget"],
)
def test_paired_schema_cleans_only_owned_staging_on_every_exit(monkeypatch, tmp_path, outcome):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    untouched = scratch / "keep.txt"
    untouched.write_text("caller-owned", encoding="utf-8")
    stages = []
    applied = []
    cancelled = False
    monkeypatch.setattr(sqlschema, "wait_for_database", lambda *a, **kw: None)

    def extract(**kwargs):
        nonlocal cancelled
        stage = kwargs["staging_root"]
        stages.append(stage)
        assert stage.parent == scratch and stage != scratch
        assert kwargs["max_disk_staging_bytes"] == 32
        kwargs["output"].write_bytes(b"x" * (33 if outcome == "budget" else 7))
        if outcome == "extract-error":
            raise sqlschema.SchemaTransferError("SourceReadDenied: cannot extract")
        if outcome == "cancel":
            cancelled = True

    def script(dacpac, output, **kwargs):
        assert dacpac.parent == stages[0] and kwargs["staging_root"] == stages[0]
        assert kwargs["max_disk_staging_bytes"] == 32
        text = "SELECT 'old';" if outcome == "rewrite-budget" else "SELECT 1"
        _write_script(output, text)
        if outcome == "script-error":
            raise sqlschema.SchemaTransferError("TargetSchemaDenied: cannot compare")
        return output

    def apply(path, **kwargs):
        assert path.exists()
        applied.append(path)
        if outcome == "apply-error":
            raise sqlschema.SchemaTransferError("NativeSqlError: cannot apply")
        return []

    monkeypatch.setattr(sqlschema, "extract_dacpac", extract)
    monkeypatch.setattr(sqlschema, "script_dacpac", script)
    monkeypatch.setattr(sqlschema, "apply_script", apply)

    def run():
        return sqlschema.transfer_schema(
            source_server="source.example", target_server="target.example", database="original",
            target_database="copy", principal=SOURCE, tokens="source", target_tokens="target",
            scratch_dir=scratch, source_type="SQLDatabase", max_disk_staging_bytes=32,
            max_memory_bytes=128 if outcome == "rewrite-budget" else 1024,
            cancel_requested=lambda: cancelled,
            id_map={"old": "x" * 100} if outcome == "rewrite-budget" else {},
        )

    if outcome == "success":
        assert run() == []
    else:
        expected = CancelledError if outcome == "cancel" else sqlschema.SchemaTransferError
        with pytest.raises(expected):
            run()
    assert stages and all(not stage.exists() for stage in stages)
    assert list(scratch.iterdir()) == [untouched]
    if outcome not in {"success", "apply-error"}:
        assert not applied


def test_schema_tool_redirects_temporary_locations_and_counts_their_files(tmp_path):
    root = tmp_path / "stage"
    root.mkdir()
    previous = os.environ.get("TMPDIR")
    code = (
        "import os,json,pathlib; "
        "names=['TMP','TEMP','TMPDIR','DOTNET_CLI_HOME','DOTNET_BUNDLE_EXTRACT_BASE_DIR']; "
        "pathlib.Path('environment.json').write_text(json.dumps({n:os.environ[n] for n in names})); "
        "pathlib.Path(os.environ['TMPDIR'],'temporary.bin').write_bytes(b'x'*24)"
    )
    sqlschema._run_bounded(
        [sys.executable, "-c", code], what="schema environment test",
        staging_root=root, max_staging_bytes=16384,
    )
    environment = json.loads((root / "environment.json").read_text())
    assert all(str(root.resolve()) in path for path in environment.values())
    assert (root / ".tool-work" / "temporary.bin").stat().st_size == 24
    assert sqlschema._staging_size(root) >= 24
    assert os.environ.get("TMPDIR") == previous


def test_bounded_schema_extract_explicitly_excludes_rows_and_redirects_table_temporary_files(
    monkeypatch, tmp_path,
):
    calls = []
    monkeypatch.setattr(sqlschema, "_run_bounded", lambda command, **kw: calls.append((command, kw)))
    with sqlschema._schema_staging(tmp_path, True) as root:
        sqlschema.extract_dacpac(
            server="source.example", database="original", principal=SOURCE,
            output=root / "schema.dacpac", staging_root=root, max_staging_bytes=64,
        )
        command, options = calls[0]
        assert "/p:ExtractAllTableData=False" in command
        assert (
            f"/p:TempDirectoryForTableData={(root / '.tool-work' / 'table-data').resolve()}" in command
        )
        assert options["staging_root"] == root and options["max_disk_staging_bytes"] == 64
    assert not root.exists()


def test_schema_tool_budget_breach_terminates_child_and_cleans_staging(monkeypatch, tmp_path):
    processes = []
    popen = sqlschema.subprocess.Popen

    def start(*args, **kwargs):
        process = popen(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(sqlschema.subprocess, "Popen", start)
    code = (
        "import os,pathlib,time; "
        "pathlib.Path(os.environ['TMPDIR'],'oversize.bin').write_bytes(b'x'*65); time.sleep(20)"
    )
    with pytest.raises(sqlschema.SchemaStagingError, match="64-byte staging budget"):
        with sqlschema._schema_staging(tmp_path, True) as root:
            sqlschema._run_bounded(
                [sys.executable, "-c", code], what="schema budget test",
                staging_root=root, max_staging_bytes=64,
            )
    assert processes and all(process.poll() is not None for process in processes)
    assert not root.exists()


def test_schema_tool_cancellation_terminates_child_and_cleans_staging(monkeypatch, tmp_path):
    processes = []
    popen = sqlschema.subprocess.Popen

    def start(*args, **kwargs):
        process = popen(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(sqlschema.subprocess, "Popen", start)
    code = "import pathlib,time; pathlib.Path('cancel-now').write_text('ready'); time.sleep(20)"
    with pytest.raises(CancelledError):
        with sqlschema._schema_staging(tmp_path, True) as root:
            sqlschema._run_bounded(
                [sys.executable, "-c", code], what="schema cancellation test",
                staging_root=root, max_staging_bytes=4096,
                cancel_requested=lambda: (root / "cancel-now").exists(),
            )
    assert processes and all(process.poll() is not None for process in processes)
    assert not root.exists()


def test_schema_tool_preserves_native_error_code_message_and_cleans_staging(tmp_path):
    code = (
        "import sys; print('Native403: service principal cannot read this schema', flush=True); sys.exit(9)"
    )
    with pytest.raises(sqlschema.SchemaTransferError) as error:
        with sqlschema._schema_staging(tmp_path, True) as root:
            sqlschema._run_bounded(
                [sys.executable, "-c", code], what="schema native failure",
                staging_root=root, max_staging_bytes=4096,
            )
    assert "Native403: service principal cannot read this schema" in str(error.value)
    assert "exit code 9" in str(error.value)
    assert not root.exists()


def test_dacfx_filters_principals_grants_and_role_membership_before_generating_sql(monkeypatch, tmp_path):
    commands = []
    destination = tmp_path / "unpacked"
    destination.mkdir()
    (destination / "Deploy.sql").write_text("SELECT 1", encoding="utf-8")
    monkeypatch.setattr(sqlschema, "_run", lambda command, **kwargs: commands.append(command))
    sqlschema.unpack_dacpac(
        tmp_path / "schema.dacpac", destination, exclude_tables=False, exclude_security=True,
    )
    exclusions = [
        commands[0][i + 1] for i, value in enumerate(commands[0])
        if value == "--deploy-script-exclude-object-type"
    ]
    assert {"Users", "Logins", "Permissions", "DatabaseRoles", "RoleMembership"}.issubset(exclusions)
    assert "Schemas" not in exclusions


def test_target_aware_dacfx_script_disables_security_and_embedded_scripts(monkeypatch, tmp_path):
    output = tmp_path / "Deploy.sql"
    output.write_text("SELECT 1", encoding="utf-8")
    commands = []
    tokens = SimpleNamespace(sql_token=lambda: "destination-access-token")
    monkeypatch.setattr(sqlschema, "_run", lambda command, **kwargs: commands.append(command))
    assert sqlschema.script_dacpac(
        tmp_path / "schema.dacpac", output, server="target", database="copy",
        tokens=tokens, exclude_tables=True,
    ) == output
    command = commands[0]
    assert "/TargetServerName:target" in command and "/TargetDatabaseName:copy" in command
    assert "/AccessToken:destination-access-token" in command
    assert "/p:IgnoreAuthorizer=True" in command and "/p:IgnorePermissions=True" in command
    assert "/p:IgnoreRoleMembership=True" in command
    assert "/p:IgnorePreDeployScript=True" in command and "/p:IgnorePostDeployScript=True" in command
    assert "/p:CreateNewDatabase=False" in command and "/p:DropObjectsNotInSource=False" in command
    assert any(arg.startswith("/p:ExcludeObjectTypes=") and arg.endswith(";Tables") for arg in command)


def test_target_aware_dacfx_script_honors_explicit_same_tenant_security_handling(monkeypatch, tmp_path):
    output = _write_script(tmp_path / "Deploy.sql", "SELECT 1")
    commands = []
    monkeypatch.setattr(sqlschema, "_run", lambda command, **kwargs: commands.append(command))
    sqlschema.script_dacpac(
        tmp_path / "schema.dacpac", output, server="target", database="copy",
        tokens=SimpleNamespace(sql_token=lambda: "target-token"), exclude_security=False, exclude_tables=True,
    )
    command = commands[0]
    assert "/p:ExcludeObjectTypes=Tables" in command
    assert not any(
        arg.startswith(("/p:IgnorePermissions=", "/p:IgnoreRoleMembership=", "/p:IgnoreAuthorizer="))
        for arg in command
    )


def test_cosmos_separate_gateway_clients_and_whole_container_completion(monkeypatch):
    copied = []
    routed = []

    class Container:
        def __init__(self, side):
            self.side = side

        def query_items(self, *args, **kwargs):
            assert self.side == "source"
            assert kwargs["enable_cross_partition_query"] is True
            assert kwargs["max_item_count"] == 1
            yield {"id": "one", "pk": 7, "value": "text", "_etag": "source-etag"}
            yield {"id": "two", "pk": 8, "value": "more"}

        def upsert_item(self, document):
            assert self.side == "target"
            copied.append(document)

    class Client:
        def __init__(self, side):
            self.side = side

        def get_database_client(self, name):
            return self

        def list_containers(self):
            assert self.side == "source"
            return [{"id": "container"}]

        def get_container_client(self, name):
            return Container(self.side)

        def close(self):
            pass

    def client(endpoint, tokens):
        routed.append((endpoint, tokens))
        return Client(endpoint)

    monkeypatch.setattr(cosmos, "_client", client)
    completed = []
    assert cosmos.copy_documents(
        source_endpoint="source", target_endpoint="target",
        source_database="original", target_database="copy",
        tokens=SOURCE, target_tokens=TARGET, on_complete=completed.append,
    ) == []
    assert routed == [("source", SOURCE), ("target", TARGET)]
    assert copied[0] == {"id": "one", "pk": 7, "value": "text"}
    assert len(completed) == 1 and not completed[0].empty


def test_cosmos_oversize_and_cancellation_never_upsert_or_checkpoint(monkeypatch):
    payload = {"id": "one", "value": "x" * 1000}
    source = SimpleNamespace(get_container_client=lambda name: object())
    target = SimpleNamespace(get_container_client=lambda name: SimpleNamespace(
        upsert_item=lambda item: pytest.fail("Should not upsert an oversized/cancelled document"),
    ))
    monkeypatch.setattr(cosmos, "_read_all", lambda reader: iter([payload]))
    with pytest.raises(StagingBudgetError):
        cosmos._copy_container(source, target, "container", None, max_staging_bytes=32)
    with pytest.raises(CancelledError):
        cosmos._copy_container(source, target, "container", None, cancel_requested=lambda: True)
