"""Waiting out a SQL endpoint that is not answering yet.

A SQL analytics endpoint is created asynchronously and stays unreachable for minutes. Every
caller went through ``connect``, which made exactly one attempt and raised the raw pyodbc
error, so a login timeout in the middle of a run ended the whole migration -- past the
handler that already treats a schema transfer as best effort, because that handler catches
SchemaTransferError and pyodbc.OperationalError is not one.
"""

from __future__ import annotations

import pyodbc
import pytest

from fabshuffle.transfer import sqlschema

LOGIN_TIMEOUT = pyodbc.OperationalError(
    "HYT00", "[HYT00] [Microsoft][ODBC Driver 18 for SQL Server]Login timeout expired (0)"
)
LINK_FAILURE = pyodbc.OperationalError("08S01", "[08S01] Communication link failure")
REJECTED = pyodbc.OperationalError("08004", "[08004] Server rejected the connection")
UNAVAILABLE = pyodbc.ProgrammingError(
    "42000", "[42000] Database 'X' on server 'Y' is not currently available"
)
NO_PERMISSION = pyodbc.ProgrammingError("42000", "[42000] Login failed for user")


class Tokens:
    def __init__(self) -> None:
        self.issued = 0

    def sql_token(self) -> str:
        self.issued += 1
        return "token"


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(sqlschema.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(sqlschema, "_driver", lambda: "ODBC Driver 18 for SQL Server")
    monkeypatch.setattr(sqlschema, "sql_access_token_struct", lambda token: b"")


@pytest.mark.parametrize("error", [LOGIN_TIMEOUT, LINK_FAILURE, REJECTED, UNAVAILABLE])
def test_a_cold_endpoint_is_waited_out(error, monkeypatch) -> None:
    attempts = {"n": 0}

    def flaky(*_args, **_kwargs):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise error
        return "connection"

    monkeypatch.setattr(sqlschema.pyodbc, "connect", flaky)

    assert sqlschema.connect("srv", "db", Tokens()) == "connection"
    assert attempts["n"] == 3


@pytest.mark.parametrize("error", [LOGIN_TIMEOUT, LINK_FAILURE, REJECTED, UNAVAILABLE])
def test_transient_failures_are_recognised(error) -> None:
    assert sqlschema.is_transient(error)


def test_a_login_failure_is_not_retried() -> None:
    # Retrying a permission problem turns a clear error into a ten minute hang.
    assert not sqlschema.is_transient(NO_PERMISSION)


def test_a_login_failure_fails_immediately(monkeypatch) -> None:
    attempts = {"n": 0}

    def always_denied(*_args, **_kwargs):
        attempts["n"] += 1
        raise NO_PERMISSION

    monkeypatch.setattr(sqlschema.pyodbc, "connect", always_denied)

    with pytest.raises(sqlschema.SchemaTransferError, match="Could not connect"):
        sqlschema.connect("srv", "db", Tokens())

    assert attempts["n"] == 1


def test_giving_up_raises_something_the_caller_already_handles(monkeypatch) -> None:
    """The phase catches SchemaTransferError; a raw pyodbc error sailed straight past it."""
    monkeypatch.setattr(
        sqlschema.pyodbc, "connect", lambda *a, **k: (_ for _ in ()).throw(LOGIN_TIMEOUT)
    )

    with pytest.raises(sqlschema.SchemaTransferError, match="never became available"):
        sqlschema.connect("srv", "db", Tokens(), attempts=3)


def test_a_fresh_token_is_taken_for_each_attempt(monkeypatch) -> None:
    # Waiting out a cold endpoint can outlast the token we started with.
    tokens = Tokens()
    attempts = {"n": 0}

    def flaky(*_args, **_kwargs):
        attempts["n"] += 1
        if attempts["n"] < 4:
            raise LOGIN_TIMEOUT
        return "connection"

    monkeypatch.setattr(sqlschema.pyodbc, "connect", flaky)
    sqlschema.connect("srv", "db", tokens)

    assert tokens.issued == 4


def test_progress_is_reported_while_waiting(monkeypatch) -> None:
    monkeypatch.setattr(
        sqlschema.pyodbc, "connect", lambda *a, **k: (_ for _ in ()).throw(LOGIN_TIMEOUT)
    )
    messages: list[str] = []

    with pytest.raises(sqlschema.SchemaTransferError):
        sqlschema.connect("srv", "db", Tokens(), attempts=5, on_progress=messages.append)

    assert messages
    assert "come online" in messages[0]


def test_a_successful_connection_reports_nothing(monkeypatch) -> None:
    monkeypatch.setattr(sqlschema.pyodbc, "connect", lambda *a, **k: "connection")
    messages: list[str] = []

    sqlschema.connect("srv", "db", Tokens(), on_progress=messages.append)

    assert messages == []


def test_apply_script_waits_rather_than_failing(monkeypatch, tmp_path) -> None:
    """The reported crash: apply_script opened a connection with no retry at all."""
    attempts = {"n": 0}

    class FakeConnection:
        autocommit = False

        def cursor(self):
            class Cursor:
                def execute(self, _batch):
                    return None

            return Cursor()

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

    def flaky(*_args, **_kwargs):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise LOGIN_TIMEOUT
        return FakeConnection()

    monkeypatch.setattr(sqlschema.pyodbc, "connect", flaky)

    script = tmp_path / "Deploy.sql"
    script.write_text("CREATE TABLE [dbo].[T] ([Id] INT);\n", encoding="utf-8")

    assert sqlschema.apply_script(script, server="srv", database="db", tokens=Tokens()) == []
    assert attempts["n"] == 3
