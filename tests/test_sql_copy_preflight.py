"""Fail closed before submitting an append-only SQL Server Copy Job."""

import pytest

from fabshuffle.transfer import sqlschema


class Cursor:
    def __init__(self, *, control=True, policy=False, rows=False):
        self.control = control
        self.policy = policy
        self.rows = rows
        self.sql = ""
        self.statements = []

    def execute(self, sql):
        self.sql = sql
        self.statements.append(sql)
        return self

    def fetchval(self):
        return int(self.control)

    def fetchone(self):
        if "security_policies" in self.sql:
            return (1,) if self.policy else None
        return (1,) if self.rows else None


class Connection:
    def __init__(self, cursor):
        self.cursor_value = cursor
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_value

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


@pytest.mark.parametrize(
    ("options", "message"),
    [({"control": False}, "CONTROL"), ({"policy": True}, "security policy"), ({"rows": True}, "not empty")],
)
def test_unprovable_or_nonempty_tables_refuse_copy(monkeypatch, options, message):
    cursor = Cursor(**options)
    monkeypatch.setattr(sqlschema, "connect", lambda *args: Connection(cursor))
    with pytest.raises(sqlschema.SchemaTransferError, match=message):
        sqlschema.require_empty_tables("server", "database", object(), [("dbo", "Orders")])


def test_empty_table_is_checked_with_escaped_identifier(monkeypatch):
    cursor = Cursor()
    monkeypatch.setattr(sqlschema, "connect", lambda *args: Connection(cursor))
    sqlschema.require_empty_tables("server", "database", object(), [("sch]", "table]")])
    assert cursor.statements[-1] == "SELECT TOP (1) 1 FROM [sch]]].[table]]]"


def test_retry_truncates_all_tables_in_one_transaction(monkeypatch):
    cursor = Cursor()
    connection = Connection(cursor)
    monkeypatch.setattr(sqlschema, "connect", lambda *args: connection)
    sqlschema.truncate_tables("server", "database", object(), [("dbo", "A"), ("sch]", "B")])
    assert "TRUNCATE TABLE [dbo].[A]" in cursor.statements
    assert "TRUNCATE TABLE [sch]]].[B]" in cursor.statements
    assert connection.committed and not connection.rolled_back and connection.closed


def test_truncation_error_rolls_back_and_reports_service_error(monkeypatch):
    class FailingCursor(Cursor):
        def execute(self, sql):
            if sql == "TRUNCATE TABLE [dbo].[B]":
                raise sqlschema.pyodbc.Error("foreign key prevents truncate")
            return super().execute(sql)

    connection = Connection(FailingCursor())
    monkeypatch.setattr(sqlschema, "connect", lambda *args: connection)
    with pytest.raises(sqlschema.SchemaTransferError, match="foreign key prevents truncate"):
        sqlschema.truncate_tables("server", "database", object(), [("dbo", "A"), ("dbo", "B")])
    assert connection.rolled_back and not connection.committed and connection.closed
