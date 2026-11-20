"""Independently authenticated SQL table streaming."""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterable
from contextlib import closing
from pathlib import Path
from typing import Any

import pyodbc

from fabshuffle.auth import TokenProvider
from fabshuffle.fabric.data_stores import TableRef
from fabshuffle.lifecycle import CopyOutcome
from fabshuffle.transfer import sqlschema
from fabshuffle.transfer.common import (
    StagingBudgetError,
    check_cancelled,
    resolve_memory_budget,
)


class BulkCopyError(RuntimeError):
    """A SQL table could not be transferred."""


def _qualified(table: TableRef) -> str:
    """Return the table's checkpoint name as ``schema.table``."""
    return f"{table.schema or 'dbo'}.{table.name}"


#: The same name, for a caller that needs to record which tables it has already moved. Public
#: so the journal and this module cannot drift into disagreeing about what a table is called.
qualified_name = _qualified


def _bracketed(table: TableRef) -> str:
    """Return a safely quoted T-SQL identifier."""
    schema = (table.schema or "dbo").replace("]", "]]")
    name = table.name.replace("]", "]]")
    return f"[{schema}].[{name}]"


def _clear_table(cursor: Any, table: TableRef) -> None:
    """Empty a target table so that loading it a second time does not double its rows.

    ``TRUNCATE`` is preferred: it deallocates pages rather than logging a row at a time. It is
    [refused on a table referenced by a foreign key](https://learn.microsoft.com/sql/t-sql/statements/truncate-table-transact-sql),
    and the documentation's own answer to that is ``DELETE``, so that is the fallback rather
    than a failure. Both are supported on SQL database in Fabric.
    """
    target = _bracketed(table)
    try:
        cursor.execute(f"TRUNCATE TABLE {target}")
    except pyodbc.Error:
        # Almost always the foreign key restriction above. DELETE has no such limit, and if it
        # fails too the error is the caller's to report.
        cursor.execute(f"DELETE FROM {target}")


def _columns(cursor: Any, table: TableRef) -> list[Any]:
    return cursor.execute(
        "SELECT c.name, c.is_identity, c.is_computed, c.generated_always_type, t.name "
        "FROM sys.columns AS c JOIN sys.types AS t ON c.user_type_id = t.user_type_id "
        "WHERE c.object_id = OBJECT_ID(?) ORDER BY c.column_id",
        _bracketed(table),
    ).fetchall()


def _column_name(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _row_bytes(row: tuple[Any, ...]) -> int:
    return sys.getsizeof(row) + sum(sys.getsizeof(value) for value in row)


def _defer_target_effects(
    target: Any, tables: list[TableRef], target_type: str,
) -> list[str]:
    """Defer only relevant target FKs/DML triggers, in the data transaction.

    Cyclic/self-referencing foreign keys cannot be solved by sorting source rows. Restoring
    them before commit validates the complete graph. Trigger side effects must not run while
    replaying existing rows. Originally disabled objects are left disabled.
    https://learn.microsoft.com/sql/relational-databases/tables/disable-foreign-key-constraints-with-insert-and-update-statements
    https://learn.microsoft.com/sql/t-sql/statements/disable-trigger-transact-sql
    """
    if target_type != "SQLDatabase":
        return []
    wanted = {((table.schema or "dbo"), table.name) for table in tables}
    restore: list[str] = []
    with closing(target.cursor()) as cursor:
        foreign_keys = cursor.execute(
            "SELECT OBJECT_SCHEMA_NAME(parent_object_id), OBJECT_NAME(parent_object_id), "
            "name, is_disabled, is_not_trusted, OBJECT_SCHEMA_NAME(referenced_object_id), "
            "OBJECT_NAME(referenced_object_id) FROM sys.foreign_keys"
        ).fetchall()
        for schema, table, constraint, disabled, untrusted, ref_schema, ref_table in foreign_keys:
            if disabled or not ({(schema, table), (ref_schema, ref_table)} & wanted):
                continue
            name = _bracketed(TableRef(name=table, schema=schema))
            constraint = _column_name(constraint)
            cursor.execute(f"ALTER TABLE {name} NOCHECK CONSTRAINT {constraint}")
            check = "CHECK CONSTRAINT" if untrusted else "WITH CHECK CHECK CONSTRAINT"
            restore.append(f"ALTER TABLE {name} {check} {constraint}")
        triggers = cursor.execute(
            "SELECT OBJECT_SCHEMA_NAME(parent_id), OBJECT_NAME(parent_id), name "
            "FROM sys.triggers WHERE parent_class = 1 AND is_disabled = 0 AND is_ms_shipped = 0"
        ).fetchall()
        for schema, table, trigger in triggers:
            if (schema, table) not in wanted:
                continue
            name = _bracketed(TableRef(name=table, schema=schema))
            trigger = _column_name(schema) + "." + _column_name(trigger)
            cursor.execute(f"DISABLE TRIGGER {trigger} ON {name}")
            restore.append(f"ENABLE TRIGGER {trigger} ON {name}")
    return restore


def copy_tables_streaming(
    *,
    source_server: str,
    source_database: str,
    target_server: str,
    target_database: str,
    tables: Iterable[TableRef],
    tokens: TokenProvider,
    target_tokens: TokenProvider,
    max_staging_bytes: int | None = None,
    max_memory_bytes: int | None = None,
    target_type: str = "SQLDatabase",
    scratch_dir: Path | None = None,
    on_progress: Callable[[str], None] | None = None,
    on_copied: Callable[[str], None] | None = None,
    on_complete: Callable[[CopyOutcome], None] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> list[str]:
    """Copy frozen SQLDB/Warehouse rows using independent ODBC connections and bounded batches.

    No local files are written. One forward-only SELECT is used per table: OFFSET paging
    would lose/duplicate rows without a proven unique ordering. Retry clears a table before
    reloading it; a completion callback follows commit and an exact count check, never a
    partial batch. With foreign keys or DML triggers, the requested set commits atomically
    after restoring constraints/triggers; an interruption rolls all of it back. The caller
    must not pass a Lakehouse SQL analytics endpoint as a writer.

    Temporal columns travel as ISO text because Python datetime would discard the seventh
    fractional digit. Other native ODBC values (Decimal, bytes, integers, NULL) stay typed.
    https://learn.microsoft.com/fabric/data-warehouse/identity
    https://learn.microsoft.com/sql/t-sql/statements/set-identity-insert-transact-sql
    """
    memory_budget = resolve_memory_budget(
        max_staging_bytes=max_staging_bytes,
        max_memory_bytes=max_memory_bytes,
    )
    check_cancelled(cancel_requested)
    if source_server == target_server and source_database == target_database:
        raise BulkCopyError("Source and destination SQL databases must differ.")
    if target_type not in {"SQLDatabase", "Warehouse"}:
        raise BulkCopyError("Rows can be written to SQLDatabase or Warehouse, not a SQL analytics endpoint.")
    wanted = list(tables)
    if not wanted:
        if on_complete:
            on_complete(CopyOutcome("tables", empty=True))
        return []
    warnings: list[str] = []
    copied_total = 0
    with (
        closing(sqlschema.connect(source_server, source_database, tokens, on_progress=on_progress)) as source,
        closing(sqlschema.connect(
            target_server, target_database, target_tokens, on_progress=on_progress,
        )) as target,
    ):
        source.autocommit = True
        target.autocommit = False
        restore = _defer_target_effects(target, wanted, target_type)
        deferred_checkpoints: list[str] = []
        for table in wanted:
            check_cancelled(cancel_requested)
            name = _bracketed(table)
            identity = False
            with closing(source.cursor()) as reader, closing(target.cursor()) as writer:
                try:
                    source_columns = _columns(reader, table)
                    target_columns = _columns(writer, table)
                    if (
                        not source_columns
                        or list(map(tuple, source_columns)) != list(map(tuple, target_columns))
                    ):
                        raise BulkCopyError(
                            f"Schema for {_qualified(table)} differs; deploy its source schema and retry."
                        )
                    if any(str(c[4]).lower() in {"timestamp", "rowversion"} for c in source_columns):
                        raise BulkCopyError(
                            f"{_qualified(table)} has a server-generated rowversion which cannot be "
                            "inserted unchanged. Convert it to binary(8) in a migration schema "
                            "before retrying."
                        )
                    if any(c[3] for c in source_columns):
                        raise BulkCopyError(
                            f"{_qualified(table)} has GENERATED ALWAYS columns. Deploy a writable "
                            "migration schema before copying historical/generated values."
                        )
                    writable = [c for c in source_columns if not c[2] and not c[3]]
                    if not writable:
                        raise BulkCopyError(f"{_qualified(table)} has no writable columns.")
                    columns = ", ".join(_column_name(str(c[0])) for c in writable)
                    projections = ", ".join(
                        f"CONVERT(nvarchar(48), {_column_name(str(c[0]))}, 127)"
                        if str(c[4]).lower() in {
                            "datetime", "smalldatetime", "datetime2", "datetimeoffset", "date", "time",
                        }
                        else _column_name(str(c[0]))
                        for c in writable
                    )
                    expected = int(reader.execute(f"SELECT COUNT_BIG(*) FROM {name}").fetchone()[0])
                    reader.execute(f"SELECT {projections} FROM {name}")
                    _clear_table(writer, table)
                    if any(c[1] for c in writable):
                        writer.execute(f"SET IDENTITY_INSERT {name} ON")
                        identity = True
                    statement = f"INSERT INTO {name} ({columns}) VALUES ({', '.join('?' for _ in writable)})"
                    batch: list[tuple[Any, ...]] = []
                    batch_bytes = 0
                    count = 0
                    while True:
                        check_cancelled(cancel_requested)
                        row = reader.fetchone()
                        if row is None:
                            break
                        values = tuple(row)
                        size = _row_bytes(values)
                        if size > memory_budget:
                            raise StagingBudgetError(
                                f"A row in {_qualified(table)} needs {size} bytes, above the "
                                f"{memory_budget}-byte memory budget. Increase max_memory_bytes and retry."
                            )
                        if batch and (
                            batch_bytes + size > min(memory_budget, 4 * 1024 * 1024)
                            or len(batch) >= 1000
                        ):
                            writer.executemany(statement, batch)
                            count += len(batch)
                            batch.clear()
                            batch_bytes = 0
                        batch.append(values)
                        batch_bytes += size
                    if batch:
                        writer.executemany(statement, batch)
                        count += len(batch)
                    if identity:
                        writer.execute(f"SET IDENTITY_INSERT {name} OFF")
                        identity = False
                        if target_type == "Warehouse":
                            escaped = name.replace("'", "''")
                            writer.execute(f"DBCC CHECKIDENT ('{escaped}', RESEED)")
                    actual = int(writer.execute(f"SELECT COUNT_BIG(*) FROM {name}").fetchone()[0])
                    if count != expected or actual != expected:
                        raise BulkCopyError(
                            f"{_qualified(table)} row counts differ: source={expected}, "
                            f"read={count}, target={actual}. Keep source writes frozen and retry."
                        )
                    check_cancelled(cancel_requested)
                    if not restore:
                        target.commit()
                    copied_total += count
                    if on_progress:
                        on_progress(f"Copied {count} rows into {_qualified(table)}")
                    if restore:
                        deferred_checkpoints.append(_qualified(table))
                    elif on_copied:
                        on_copied(_qualified(table))
                except (pyodbc.Error, BulkCopyError) as error:
                    target.rollback()
                    warnings.append(f"Rows for {_qualified(table)} did not copy: {error}")
                    if restore:
                        break
                except BaseException:
                    target.rollback()
                    raise
                finally:
                    if identity:
                        try:
                            writer.execute(f"SET IDENTITY_INSERT {name} OFF")
                        except pyodbc.Error:
                            # Do not mask the original service failure. Closing the connection
                            # below also removes the session-scoped identity setting.
                            target.close()
                            raise
        if restore and not warnings:
            try:
                check_cancelled(cancel_requested)
                with closing(target.cursor()) as cursor:
                    for statement in restore:
                        cursor.execute(statement)
                target.commit()
            except BaseException:
                target.rollback()
                raise
            if on_copied:
                for name in deferred_checkpoints:
                    on_copied(name)
    check_cancelled(cancel_requested)
    if not warnings and on_complete:
        on_complete(CopyOutcome("tables", empty=copied_total == 0))
    return warnings


__all__ = ["BulkCopyError", "copy_tables_streaming", "qualified_name"]
