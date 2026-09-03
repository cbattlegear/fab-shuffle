"""Retrying a sqlpackage extract that could not connect.

Waiting for the endpoint over ODBC first proves it answers a login, not that sqlpackage will
finish a metadata read: sqlpackage is its own process with its own connection and its own
timeout. A SQL analytics endpoint refreshed moments earlier regularly refuses the first
attempt and accepts the next.
"""

from __future__ import annotations

import subprocess

import pytest

from fabshuffle.auth import ServicePrincipal
from fabshuffle.transfer import sqlschema

PRINCIPAL = ServicePrincipal("tenant", "client", "secret")

TIMED_OUT = (
    "*** Error extracting database:Could not extract package from specified database. "
    "A network-related or instance-specific error occurred while establishing a connection "
    "to SQL Server. (provider: TCP Provider, error: 35 - An internal exception was caught) "
    "The connection attempt timed out."
)
DENIED = "*** Error extracting database:Login failed for user '<token-identified principal>'."


@pytest.fixture(autouse=True)
def _no_waiting(monkeypatch):
    monkeypatch.setattr(sqlschema.time, "sleep", lambda _seconds: None)


def runner(monkeypatch, failures, stderr=TIMED_OUT):
    """Fail the first ``failures`` invocations, then succeed."""
    calls = {"n": 0}

    def run(command, capture_output=True, text=True, check=False):
        calls["n"] += 1

        class Result:
            returncode = 1 if calls["n"] <= failures else 0

        Result.stderr = stderr if calls["n"] <= failures else ""
        Result.stdout = ""
        return Result

    monkeypatch.setattr(subprocess, "run", run)
    return calls


def extract(tmp_path, **kwargs):
    return sqlschema.extract_dacpac(
        server="dst.datawarehouse.fabric.microsoft.com",
        database="CloneTest",
        principal=PRINCIPAL,
        output=tmp_path / "out.dacpac",
        **kwargs,
    )


def test_a_connection_timeout_is_tried_again(monkeypatch, tmp_path):
    calls = runner(monkeypatch, failures=1)
    extract(tmp_path)

    assert calls["n"] == 2


def test_it_gives_up_after_the_last_attempt(monkeypatch, tmp_path):
    calls = runner(monkeypatch, failures=99)

    with pytest.raises(sqlschema.SchemaTransferError, match="connection attempt timed out"):
        extract(tmp_path, attempts=3)

    assert calls["n"] == 3


def test_a_permission_failure_is_not_retried(monkeypatch, tmp_path):
    """Retrying a refusal turns a clear error into a long wait and the same clear error."""
    calls = runner(monkeypatch, failures=99, stderr=DENIED)

    with pytest.raises(sqlschema.SchemaTransferError, match="Login failed"):
        extract(tmp_path)

    assert calls["n"] == 1


def test_a_first_attempt_that_works_is_not_repeated(monkeypatch, tmp_path):
    calls = runner(monkeypatch, failures=0)
    extract(tmp_path)

    assert calls["n"] == 1


def test_a_partly_written_file_is_removed_before_retrying(monkeypatch, tmp_path):
    output = tmp_path / "out.dacpac"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b"half a dacpac")

    seen: list[bool] = []
    calls = {"n": 0}

    def run(command, capture_output=True, text=True, check=False):
        calls["n"] += 1
        seen.append(output.exists())

        class Result:
            returncode = 1 if calls["n"] == 1 else 0

        Result.stderr = TIMED_OUT if calls["n"] == 1 else ""
        Result.stdout = ""
        return Result

    monkeypatch.setattr(subprocess, "run", run)
    extract(tmp_path)

    # The leftover from the failed attempt must not be mistaken for a real extract.
    assert seen == [True, False]


@pytest.mark.parametrize(
    "message",
    [
        "The connection attempt timed out.",
        "A network-related or instance-specific error occurred",
        "The server was not found or was not accessible",
        "error: 35 - An internal exception was caught",
        "Timeout expired",
        "Database 'x' is not currently available",
    ],
)
def test_these_read_as_the_endpoint_not_being_ready(message):
    assert sqlschema._is_transient_tool_failure(message)


@pytest.mark.parametrize(
    "message",
    ["Login failed for user", "Invalid object name 'dbo.Orders'", "exit code 1: "],
)
def test_these_do_not(message):
    assert not sqlschema._is_transient_tool_failure(message)
