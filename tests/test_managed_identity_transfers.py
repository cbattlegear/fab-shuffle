"""Every formerly secret-only transport accepts the same explicit managed identity."""

from __future__ import annotations

import subprocess
from unittest.mock import Mock

import pytest
from test_managed_identity import CLIENT, OBJECT, TENANT, provider
from test_managed_identity import endpoint as endpoint

from fabshuffle import auth
from fabshuffle.bcdr import capture_sources
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
    connection.assert_called_once_with("https://cluster", tokens)


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
