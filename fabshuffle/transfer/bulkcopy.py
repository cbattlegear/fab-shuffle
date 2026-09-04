"""Bulk table copy between Fabric SQL databases, using bcp.

A Copy Job cannot do this for us. The SQL database in Fabric connector
[supports only an organizational account](https://learn.microsoft.com/fabric/data-factory/connector-sql-database),
and Fab Shuffle signs in as a service principal, so there is no connection it can create that
the job would be able to use.

bcp can. It supports SQL database in Fabric directly, and on Linux it authenticates with an
access token read from a file, for the ``https://database.windows.net`` resource we already
hold a token for and already use to list these tables over TDS.

The token file has to be UTF-16LE with no BOM, is written with owner-only permissions, and is
removed as soon as the copy is done. Data goes out to a native-format file and straight back
in, so nothing is parsed or retyped on the way past.

One connection is opened to the target, but only to empty each table before it is loaded:
``bcp in`` appends, so without that a repeated copy would double every row.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import tempfile
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pyodbc

from fabshuffle.auth import TokenProvider
from fabshuffle.config import SETTINGS
from fabshuffle.fabric.data_stores import TableRef
from fabshuffle.transfer import sqlschema

logger = logging.getLogger(__name__)

# bcp reports what it did on stdout; this is the only part worth repeating.
_ROWS_COPIED = re.compile(r"^(\d+)\s+rows copied\.", re.MULTILINE | re.IGNORECASE)


class BulkCopyError(RuntimeError):
    """bcp could not move a table."""


@contextmanager
def token_file(tokens: TokenProvider):
    """Write the SQL access token where bcp can read it, and take it away afterwards.

    bcp wants UTF-16LE without a byte order mark. The file holds a live credential, so it is
    created with owner-only permissions and deleted in a finally rather than left in scratch.
    """
    handle, path = tempfile.mkstemp(prefix="fabshuffle-token-", suffix=".tok")
    os.close(handle)
    location = Path(path)
    try:
        os.chmod(location, 0o600)
        location.write_bytes(tokens.sql_token().encode("utf-16-le"))
        yield location
    finally:
        location.unlink(missing_ok=True)


def _qualified(table: TableRef) -> str:
    """The name to hand bcp, as ``schema.table``.

    Deliberately not bracketed. ``-q`` sets ``QUOTED_IDENTIFIER ON`` and expects the name in
    quotation marks, not square brackets, so a bracketed name reaches the server as a single
    literal identifier and comes back as "Invalid object name '[dbo].[Orders]'". The name is
    one argument in the argv list, so a space in it needs no quoting from us.
    """
    return f"{table.schema or 'dbo'}.{table.name}"


#: The same name, for a caller that needs to record which tables it has already moved. Public
#: so the journal and this module cannot drift into disagreeing about what a table is called.
qualified_name = _qualified


def _bracketed(table: TableRef) -> str:
    """The same table as a T-SQL identifier, which *does* want brackets.

    The opposite of :func:`_qualified`, and deliberately so: what bcp takes as an argument and
    what the server parses as T-SQL are not the same thing. A closing bracket inside a name is
    doubled, which is how T-SQL escapes it.
    """
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


def _run(command: list[str], *, what: str) -> str:
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
    except FileNotFoundError as error:
        raise BulkCopyError(
            f"{what} could not run because '{command[0]}' is not installed in this image. "
            "Copying SQL database rows needs bcp."
        ) from error
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()[-1000:]
        raise BulkCopyError(f"{what} failed with exit code {result.returncode}: {detail}")
    return result.stdout or ""


def _bcp(
    table: TableRef,
    direction: str,
    data_file: Path,
    *,
    server: str,
    database: str,
    token: Path,
    extra: Iterable[str] = (),
) -> str:
    command = [
        SETTINGS.bcp_path,
        _qualified(table),
        direction,
        str(data_file),
        "-S",
        server,
        "-d",
        database,
        # Entra access token, read from a file. Linux only, which is what we run on.
        "-G",
        "-P",
        str(token),
        # Native format: the two ends are both Fabric SQL databases, so there is no reason to
        # go through a text representation and risk losing precision on the way.
        "-n",
        # Quoted identifiers, so a table or schema named with a reserved word still works.
        "-q",
        *extra,
    ]
    return _run(command, what=f"bcp {direction} of {_qualified(table)}")


def copy_tables(
    *,
    source_server: str,
    source_database: str,
    target_server: str,
    target_database: str,
    tables: Iterable[TableRef],
    tokens: TokenProvider,
    scratch_dir: Path,
    on_progress: Callable[[str], None] | None = None,
    on_copied: Callable[[str], None] | None = None,
) -> list[str]:
    """Copy every table's rows from one SQL database to another. Returns per-table warnings.

    The tables already exist, created from the source's dacpac, so this only moves rows.
    Identity values are preserved: a copy whose keys differ from the original is not a copy.

    Each target table is emptied immediately before it is loaded, because ``bcp in`` appends.
    Without that, copying a table twice doubles its rows, which makes the whole operation
    unsafe to repeat. Every other data mover here overwrites, and this one now matches them.

    ``on_copied`` is told each table's qualified name as it lands, so a caller can write down
    what it will not have to do again.

    One table failing is reported and the rest are still attempted, because losing one table
    should not cost the operator the other fifty.
    """
    wanted = list(tables)
    if not wanted:
        return []

    scratch_dir.mkdir(parents=True, exist_ok=True)
    warnings: list[str] = []

    # One connection for the whole batch, used only to empty each table before it is loaded.
    # The schema was deployed over this same endpoint moments ago, so it is known to answer.
    with (
        sqlschema.connect(target_server, target_database, tokens, on_progress=on_progress) as target,
        token_file(tokens) as token,
    ):
        target.autocommit = True
        cursor = target.cursor()
        for index, table in enumerate(wanted, start=1):
            if on_progress:
                on_progress(f"Copying {_qualified(table)} ({index} of {len(wanted)})")
            data_file = scratch_dir / f"{index}.bcp"
            try:
                _bcp(
                    table,
                    "out",
                    data_file,
                    server=source_server,
                    database=source_database,
                    token=token,
                )
                _clear_table(cursor, table)
                output = _bcp(
                    table,
                    "in",
                    data_file,
                    server=target_server,
                    database=target_database,
                    token=token,
                    # Keep identity values rather than letting the target mint new ones.
                    extra=("-E",),
                )
                match = _ROWS_COPIED.search(output)
                logger.debug("Copied %s rows into %s", match.group(1) if match else "?", _qualified(table))
                if on_copied:
                    on_copied(_qualified(table))
            except BulkCopyError as error:
                warnings.append(f"Rows for {_qualified(table)} did not copy: {error}")
            except pyodbc.Error as error:
                warnings.append(
                    f"Rows for {_qualified(table)} did not copy: the table could not be "
                    f"emptied first, and loading it as it stands would duplicate its rows: {error}"
                )
            finally:
                data_file.unlink(missing_ok=True)

    return warnings


__all__ = ["BulkCopyError", "copy_tables", "qualified_name", "token_file"]
