"""Every formerly secret-only transport accepts the same explicit managed identity."""

from __future__ import annotations

import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_managed_identity import CLIENT, OBJECT, TENANT, provider
from test_managed_identity import endpoint as endpoint

from fabshuffle import auth
from fabshuffle.bcdr import capture_sources, protection_kql
from fabshuffle.bcdr.protection import ProtectionLimits
from fabshuffle.lifecycle import CopyOutcome
from fabshuffle.transfer import bulkcopy, cosmos, files, kql, sqlschema


def test_mi_files_choose_bounded_streaming_not_azcopy(endpoint, monkeypatch, tmp_path):
    tokens = provider()
    relay = Mock(return_value=CopyOutcome("files", empty=False))
    monkeypatch.setattr(files, "copy_tree_streaming", relay)
    monkeypatch.setattr(files, "_azcopy", Mock(side_effect=AssertionError("No AzCopy MSI assumption")))
    progress, complete, cancel = Mock(), Mock(), Mock(return_value=False)
    assert files.copy_files(
        source_files_path="https://onelake.dfs.fabric.microsoft.com/source/lakehouse/Files",
        target_files_path="https://onelake.dfs.fabric.microsoft.com/target/lakehouse/Files",
        principal=tokens.principal, tokens=tokens, scratch_dir=tmp_path,
        max_memory_bytes=1024, max_disk_staging_bytes=2048,
        cancel_requested=cancel, on_progress=progress, on_complete=complete,
    ) == CopyOutcome("files", empty=False)
    args = relay.call_args.kwargs
    assert args["tokens"] is args["target_tokens"] is tokens
    assert args["scratch_dir"] == tmp_path
    assert args["max_memory_bytes"] == 1024
    assert args["max_disk_staging_bytes"] == 2048
    assert args["on_complete"] is complete and args["cancel_requested"] is cancel
    assert not list(tmp_path.iterdir())


def test_mi_files_reconstruct_provider_and_preserve_separate_destination(endpoint, monkeypatch, tmp_path):
    target = auth.TokenProvider(auth.ServicePrincipal("other-tenant", "other-client", "secret"))
    relay = Mock()
    monkeypatch.setattr(files, "copy_tree_streaming", relay)
    files.copy_files(
        source_files_path="source", target_files_path="target", principal=provider().principal,
        target_tokens=target, scratch_dir=tmp_path,
    )
    args = relay.call_args.kwargs
    assert args["target_tokens"] is target
    assert args["tokens"].object_id() == OBJECT


def test_kusto_managed_callback_refreshes_through_shared_provider(endpoint, monkeypatch):
    tokens = provider()
    builder = Mock()
    callback = Mock(return_value=builder)
    monkeypatch.setattr(kql.KustoConnectionStringBuilder, "with_token_provider", callback)
    monkeypatch.setattr(
        kql.KustoConnectionStringBuilder, "with_aad_application_key_authentication",
        Mock(side_effect=AssertionError("MI must not supply an app key")),
    )
    assert kql.kusto_connection("https://query.kusto.fabric.microsoft.com", tokens) is builder
    get_token = callback.call_args.args[1]
    first = get_token()
    assert auth.token_claim(first, "aud") == auth.SCOPE_KUSTO.removesuffix("/.default")
    endpoint.now += 3601
    assert get_token() != first
    constructed = Mock()
    monkeypatch.setattr(kql, "KustoClient", constructed)
    kql._client("https://query.kusto.fabric.microsoft.com", tokens.principal)
    constructed.assert_called_once_with(builder)


def test_kusto_sp_keeps_existing_sdk_authentication(monkeypatch):
    builder = Mock()
    factory = Mock(return_value=builder)
    monkeypatch.setattr(kql.KustoConnectionStringBuilder, "with_aad_application_key_authentication", factory)
    tokens = auth.TokenProvider(auth.ServicePrincipal("tenant", "client", "secret"))
    assert kql.kusto_connection("https://cluster", tokens) is builder
    factory.assert_called_once_with("https://cluster", "client", "secret", "tenant")


def test_metadata_capture_uses_shared_mi_kusto_connection(endpoint, monkeypatch):
    tokens = provider()
    reader = capture_sources.MetadataReaders(tokens)
    builder = Mock()
    connection = Mock(return_value=builder)
    monkeypatch.setattr(capture_sources, "kusto_connection", connection)
    client = Mock()
    client.execute_mgmt.return_value.primary_results = [[{"DatabaseName": "actual"}]]
    # Stop immediately after identity discovery: no schema fixtures hide auth-path mistakes.
    client.execute_mgmt.side_effect = [
        client.execute_mgmt.return_value, RuntimeError("metadata query reached"),
    ]
    manager = Mock()
    manager.__enter__ = Mock(return_value=client)
    manager.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(capture_sources, "KustoClient", Mock(return_value=manager))
    with pytest.raises(RuntimeError, match="metadata query reached"):
        reader.kql_metadata("https://cluster", "database", follower=False)
    connection.assert_called_once_with("https://cluster", tokens, use_token_provider=True)


def test_sql_extract_mi_uses_fresh_token_on_retry_not_secret(endpoint, monkeypatch, tmp_path):
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        if len(commands) == 1:
            endpoint.now += 3601
            raise sqlschema.SchemaTransferError("connection attempt timed out")

    monkeypatch.setattr(sqlschema, "_run", run)
    monkeypatch.setattr(sqlschema.time, "sleep", lambda _: None)
    tokens = provider()
    sqlschema.extract_dacpac(
        server="source.datawarehouse.fabric.microsoft.com", database="warehouse",
        principal=tokens.principal, tokens=tokens, output=tmp_path / "schema.dacpac",
    )
    assert len(commands) == 2
    access_tokens = []
    for command in commands:
        connection = next(arg for arg in command if arg.startswith("/SourceConnectionString:"))
        assert all(word not in connection.casefold() for word in ("password", "user id", "authentication"))
        token = next(arg.partition(":")[2] for arg in command if arg.startswith("/AccessToken:"))
        assert auth.token_claim(token, "appid") == CLIENT
        assert auth.token_claim(token, "aud") == auth.SCOPE_SQL.removesuffix("/.default")
        access_tokens.append(token)
    assert access_tokens[0] != access_tokens[1]


def test_sql_extract_reconstructs_mi_and_honors_bounded_runner(endpoint, monkeypatch, tmp_path):
    runner = Mock()
    monkeypatch.setattr(sqlschema, "_run_bounded", runner)
    sqlschema.extract_dacpac(
        server="source", database="database", principal=provider().principal,
        output=tmp_path / "schema.dacpac", staging_root=tmp_path, max_disk_staging_bytes=2048,
    )
    assert any(arg.startswith("/AccessToken:") for arg in runner.call_args.args[0])
    assert runner.call_args.kwargs["max_disk_staging_bytes"] == 2048


def test_sql_extract_rejects_mismatched_token_provider(endpoint, tmp_path):
    with pytest.raises(sqlschema.SchemaTransferError, match="must match"):
        sqlschema.extract_dacpac(
            server="source", database="database", principal=provider().principal,
            tokens=auth.TokenProvider(auth.ManagedIdentity(TENANT, OBJECT)),
            output=tmp_path / "schema.dacpac",
        )


def test_sql_tool_failure_retains_service_words_not_token(endpoint, monkeypatch):
    token = provider().sql_token()
    monkeypatch.setattr(sqlschema.subprocess, "run", Mock(return_value=subprocess.CompletedProcess(
        args=[], returncode=1, stdout="", stderr=f"Login failed for /AccessToken:{token}",
    )))
    with pytest.raises(sqlschema.SchemaTransferError, match="Login failed") as failure:
        sqlschema._run(["sqlpackage", f"/AccessToken:{token}"], what="extract")
    assert token not in str(failure.value)


def test_odbc_and_bcp_use_sql_token_without_identity_secret(endpoint, monkeypatch):
    tokens = provider()
    connect = Mock()
    monkeypatch.setattr(sqlschema, "_driver", lambda: "ODBC Driver 18 for SQL Server")
    monkeypatch.setattr(sqlschema.pyodbc, "connect", connect)
    sqlschema.connect("source", "database", tokens)
    assert "Authentication=" not in connect.call_args.args[0]
    encoded = connect.call_args.kwargs["attrs_before"][sqlschema.SQL_COPT_SS_ACCESS_TOKEN]
    assert encoded == auth.sql_access_token_struct(tokens.sql_token())
    with bulkcopy.token_file(tokens) as path:
        assert path.read_bytes().decode("utf-16-le") == tokens.sql_token()
        assert path.stat().st_mode & 0o777 == 0o600
    assert not path.exists()


def test_cosmos_adapter_uses_mi_audience_and_actual_expiry(endpoint):
    tokens = provider()
    credential = cosmos._TokenCredential(tokens)
    value = credential.get_token("https://cosmos.azure.com/.default")
    assert value.expires_on == endpoint.now + 3600
    assert auth.token_claim(value.token, "appid") == CLIENT
    assert auth.token_claim(value.token, "aud") == "https://cosmos.azure.com"
    endpoint.now += 3601
    assert credential.get_token("https://cosmos.azure.com/.default").token != value.token


class FencedTokens(auth.TokenProvider):
    def __init__(self):
        super().__init__(auth.ManagedIdentity(TENANT, CLIENT))
        self.active = True

    def assert_active(self):
        if not self.active:
            raise RuntimeError("deployment lease lost")


def test_inactive_provider_cannot_get_cached_tokens_or_invalidate(endpoint):
    tokens = FencedTokens()
    tokens.fabric_token()
    tokens.active = False
    with pytest.raises(RuntimeError, match="lease lost"):
        tokens.fabric_token()
    with pytest.raises(RuntimeError, match="lease lost"):
        tokens.invalidate()
    assert len(endpoint.calls) == 1


def test_provider_checks_lease_after_identity_endpoint_returns(endpoint, monkeypatch):
    tokens = FencedTokens()
    original = endpoint.send

    def expired_during_request(*args, **kwargs):
        response = original(*args, **kwargs)
        tokens.active = False
        return response

    monkeypatch.setattr(endpoint, "send", expired_during_request)
    with pytest.raises(RuntimeError, match="lease lost"):
        tokens.fabric_token()
    assert len(endpoint.calls) == 1


def test_kql_supplied_provider_is_not_reconstructed_or_downgraded_to_secret(monkeypatch):
    tokens = auth.TokenProvider(auth.ServicePrincipal("tenant", "client", "secret"))
    callback = Mock(return_value=Mock())
    monkeypatch.setattr(kql.KustoConnectionStringBuilder, "with_token_provider", callback)
    monkeypatch.setattr(kql, "KustoClient", Mock())
    kql._client("https://cluster", tokens.principal, tokens=tokens)
    assert callback.call_args.args[1].__self__ is tokens
    with pytest.raises(kql.KqlTransferError, match="must match"):
        kql._client("https://cluster", auth.ManagedIdentity(TENANT, CLIENT), tokens=tokens)


def test_metadata_sql_loss_stops_next_query_on_existing_connection(monkeypatch):
    tokens = FencedTokens()
    cursor = Mock()
    cursor.execute.return_value = cursor
    cursor.fetchone.return_value = [1]
    cursor.description = [("name",)]

    def fetched(_):
        tokens.active = False
        return [["orders"]]

    cursor.fetchmany.side_effect = fetched
    connection = Mock()
    connection.cursor.return_value = cursor
    monkeypatch.setattr(sqlschema, "connect", Mock(return_value=connection))
    with pytest.raises(RuntimeError, match="lease lost"):
        capture_sources.MetadataReaders(tokens).sql_metadata("server", "database")
    assert cursor.execute.call_count == 2  # permission check, then first inventory query only
    connection.close.assert_called_once()


def test_metadata_kql_loss_stops_next_management_read(monkeypatch):
    tokens = FencedTokens()
    client = Mock()

    def execute(*_):
        tokens.active = False
        return SimpleNamespace(primary_results=[[{"DatabaseName": "actual"}]])

    client.execute_mgmt.side_effect = execute
    manager = Mock(__enter__=Mock(return_value=client), __exit__=Mock(return_value=False))
    monkeypatch.setattr(capture_sources, "KustoClient", Mock(return_value=manager))
    with pytest.raises(RuntimeError, match="lease lost"):
        capture_sources.MetadataReaders(tokens).kql_metadata("https://cluster", "database", follower=False)
    client.execute_mgmt.assert_called_once_with("database", ".show database identity")


def test_kql_prepared_probe_preserves_provider_and_stops_after_loss(monkeypatch):
    tokens = FencedTokens()
    client = Mock()

    def execute(*args, **kwargs):
        tokens.active = False
        return SimpleNamespace(primary_results=[[{"TableName": "orders"}]])

    client.execute_mgmt.side_effect = execute
    manager = Mock(__enter__=Mock(return_value=client), __exit__=Mock(return_value=False))
    factory = Mock(return_value=manager)
    monkeypatch.setattr(protection_kql, "kql_client", factory)
    prepared = SimpleNamespace(
        target=SimpleNamespace(endpoint="https://standby", database="database"),
        tables=(protection_kql.KqlTable("orders", 1),),
    )
    with pytest.raises(RuntimeError, match="lease lost"):
        protection_kql._probe_tables(prepared, tokens, ProtectionLimits(), None)
    factory.assert_called_once_with("https://standby", tokens.principal, tokens=tokens)
    client.execute_query.assert_not_called()


def test_sql_existing_batch_mutation_guard_stops_after_lost_lease(monkeypatch, tmp_path):
    tokens = FencedTokens()
    path = tmp_path / "schema.sql"
    path.write_text(
        "-- SqlPackage preamble\nGO\n"
        "CREATE VIEW first AS SELECT 1 AS value;\nGO\nCREATE VIEW second AS SELECT 2 AS value;"
    )
    cursor = Mock()
    cursor.execute.side_effect = lambda _: setattr(tokens, "active", False)
    connection = Mock()
    connection.cursor.return_value = cursor
    manager = Mock(__enter__=Mock(return_value=connection), __exit__=Mock(return_value=False))
    monkeypatch.setattr(sqlschema, "connect", Mock(return_value=manager))

    def batch_guard():
        tokens.assert_active()
        return False

    with pytest.raises(RuntimeError, match="lease lost"):
        sqlschema.apply_script(
            path, server="server", database="database", tokens=tokens, cancel_requested=batch_guard,
        )
    cursor.execute.assert_called_once()
