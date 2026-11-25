"""SQL Copy Job constraint and trigger suspension is durable and target-bound."""

import copy

import pytest

from fabshuffle.transfer import sqlschema


class EffectsCursor:
    def __init__(self):
        self.statements = []
        self.current = ""
        self.fail_on = ""
        self.foreign_keys = [
            [11, 101, 102, "SalesLT", "CustomerAddress", "FK_Address", 0, 0, "SalesLT", "Address"],
            [12, 101, 102, "SalesLT", "CustomerAddress", "FK_Disabled", 1, 1, "SalesLT", "Address"],
            [13, 101, 102, "SalesLT", "CustomerAddress", "FK_Untrusted", 0, 1, "SalesLT", "Address"],
        ]
        self.checks = [[21, 101, "SalesLT", "CustomerAddress", "CK_Name", 0, 0]]
        self.triggers = [[31, 101, "SalesLT", "CustomerAddress", "Audit", 0],
                         [32, 101, "SalesLT", "CustomerAddress", "DisabledAudit", 1]]
        self.ids = {"[SalesLT].[CustomerAddress]": 101, "[SalesLT].[Address]": 102}
        self.arg = ""

    def execute(self, sql, *args):
        self.statements.append(sql)
        self.current = sql
        self.arg = args[0] if args else ""
        if self.fail_on and self.fail_on in sql:
            raise sqlschema.pyodbc.Error("simulated SQL failure")
        for rows, name_index, disabled_index, trust_index in (
            (self.foreign_keys, 5, 6, 7), (self.checks, 4, 5, 6), (self.triggers, 4, 5, None),
        ):
            for row in rows:
                if f"[{row[name_index]}]" not in sql:
                    continue
                if "NOCHECK CONSTRAINT" in sql or sql.startswith("DISABLE TRIGGER"):
                    row[disabled_index] = 1
                    if trust_index is not None:
                        row[trust_index] = 1
                elif "CHECK CONSTRAINT" in sql or sql.startswith("ENABLE TRIGGER"):
                    row[disabled_index] = 0
                    if trust_index is not None and "WITH CHECK CHECK" in sql:
                        row[trust_index] = 0
        return self

    def fetchall(self):
        if "sys.foreign_keys" in self.current:
            return copy.deepcopy(self.foreign_keys)
        if "sys.check_constraints" in self.current:
            return copy.deepcopy(self.checks)
        if "sys.triggers" in self.current:
            return copy.deepcopy(self.triggers)
        raise AssertionError(self.current)

    def fetchval(self):
        return self.ids.get(self.arg) if "OBJECT_ID" in self.current else 1

    def fetchone(self):
        return None


class EffectsConnection:
    def __init__(self, cursor):
        self.value = cursor
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.value

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


@pytest.fixture
def sql(monkeypatch):
    cursor = EffectsCursor()
    connections = []
    def connect(*_):
        connection = EffectsConnection(cursor)
        connections.append(connection)
        return connection
    monkeypatch.setattr(sqlschema, "connect", connect)
    return cursor, connections


TABLES = [("SalesLT", "Address"), ("SalesLT", "CustomerAddress")]


def suspend(sql):
    cursor, _ = sql
    saved = []
    def record(state):
        assert not any(
            "NOCHECK CONSTRAINT" in s or s.startswith("DISABLE TRIGGER") for s in cursor.statements
        )
        saved.append(copy.deepcopy(state))
    state = sqlschema.suspend_copy_effects("server", "database", object(), TABLES, record=record)
    assert saved == [state]
    return state


def test_suspends_relevant_objects_and_restores_original_enabled_and_trust_states(sql):
    cursor, connections = sql
    state = suspend(sql)
    assert "ALTER TABLE [SalesLT].[CustomerAddress] NOCHECK CONSTRAINT [FK_Address]" in cursor.statements
    assert "DISABLE TRIGGER [SalesLT].[Audit] ON [SalesLT].[CustomerAddress]" in cursor.statements
    assert not any("NOCHECK CONSTRAINT [FK_Disabled]" in s for s in cursor.statements)
    assert not any("DISABLE TRIGGER [SalesLT].[DisabledAudit]" in s for s in cursor.statements)
    sqlschema.restore_copy_effects("server", "database", object(), state)
    assert (
        "ALTER TABLE [SalesLT].[CustomerAddress] WITH CHECK CHECK CONSTRAINT [FK_Address]"
        in cursor.statements
    )
    assert "ALTER TABLE [SalesLT].[CustomerAddress] CHECK CONSTRAINT [FK_Untrusted]" in cursor.statements
    assert "ENABLE TRIGGER [SalesLT].[Audit] ON [SalesLT].[CustomerAddress]" in cursor.statements
    assert all(connection.committed for connection in connections)
    assert cursor.foreign_keys[1][6:] == [1, 1, "SalesLT", "Address"]
    assert cursor.triggers[1][5] == 1


def test_restore_failure_rolls_back_and_keeps_recorded_state_for_retry(sql):
    cursor, connections = sql
    state = suspend(sql)
    cursor.fail_on = "WITH CHECK CHECK CONSTRAINT [FK_Address]"
    with pytest.raises(sqlschema.SchemaTransferError, match="simulated SQL failure"):
        sqlschema.restore_copy_effects("server", "database", object(), state)
    assert connections[-1].rolled_back and not connections[-1].committed


def test_restore_reports_both_sql_and_rollback_failures(sql, monkeypatch):
    cursor, _ = sql
    state = suspend(sql)
    cursor.fail_on = "WITH CHECK CHECK CONSTRAINT [FK_Address]"
    def failed_rollback(_self):
        raise sqlschema.pyodbc.Error("rollback failed")
    monkeypatch.setattr(EffectsConnection, "rollback", failed_rollback)
    with pytest.raises(sqlschema.SchemaTransferError, match=r"simulated SQL failure.*rollback failed"):
        sqlschema.restore_copy_effects("server", "database", object(), state)


def test_confirmed_failed_retry_deletes_referenced_tables_with_effects_disabled(sql):
    cursor, connections = sql
    state = suspend(sql)
    sqlschema.clear_failed_sql_copy("server", "database", object(), state)
    assert "DELETE FROM [SalesLT].[Address]" in cursor.statements
    assert "DELETE FROM [SalesLT].[CustomerAddress]" in cursor.statements
    assert not any(s.startswith("TRUNCATE TABLE") for s in cursor.statements)
    assert connections[-1].committed


def test_resume_after_restoration_commit_does_not_clear_failed_copy(sql):
    cursor, _ = sql
    state = suspend(sql)
    sqlschema.restore_copy_effects("server", "database", object(), state)
    cursor.statements.clear()
    sqlschema.restore_copy_effects("server", "database", object(), state, clear_failed=True)
    assert not any(statement.startswith("DELETE ") for statement in cursor.statements)


def test_partial_external_restoration_refuses_to_clear_failed_copy(sql):
    cursor, _ = sql
    state = suspend(sql)
    cursor.foreign_keys[0][6:8] = [0, 0]
    with pytest.raises(sqlschema.SchemaTransferError, match="only partially restored"):
        sqlschema.restore_copy_effects("server", "database", object(), state, clear_failed=True)
    assert not any(statement.startswith("DELETE ") for statement in cursor.statements)


def test_replaced_table_or_changed_constraint_refuses_restoration(sql):
    cursor, _ = sql
    state = suspend(sql)
    cursor.ids["[SalesLT].[Address]"] = 999
    with pytest.raises(sqlschema.SchemaTransferError, match="tables changed"):
        sqlschema.restore_copy_effects("server", "database", object(), state)
    cursor.ids["[SalesLT].[Address]"] = 102
    cursor.foreign_keys[0][0] = 99
    with pytest.raises(sqlschema.SchemaTransferError, match="constraints or triggers changed"):
        sqlschema.restore_copy_effects("server", "database", object(), state)
