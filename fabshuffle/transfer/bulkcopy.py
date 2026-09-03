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

from fabshuffle.auth import TokenProvider
from fabshuffle.config import SETTINGS
from fabshuffle.fabric.data_stores import TableRef

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


def _quoted(table: TableRef) -> str:
    schema = table.schema or "dbo"
    return f"[{schema}].[{table.name}]"


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
        _quoted(table),
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
    return _run(command, what=f"bcp {direction} of {_quoted(table)}")


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
) -> list[str]:
    """Copy every table's rows from one SQL database to another. Returns per-table warnings.

    The tables already exist, created from the source's dacpac, so this only moves rows.
    Identity values are preserved: a copy whose keys differ from the original is not a copy.

    One table failing is reported and the rest are still attempted, because losing one table
    should not cost the operator the other fifty.
    """
    wanted = list(tables)
    if not wanted:
        return []

    scratch_dir.mkdir(parents=True, exist_ok=True)
    warnings: list[str] = []

    with token_file(tokens) as token:
        for index, table in enumerate(wanted, start=1):
            if on_progress:
                on_progress(f"Copying {_quoted(table)} ({index} of {len(wanted)})")
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
                logger.debug("Copied %s rows into %s", match.group(1) if match else "?", _quoted(table))
            except BulkCopyError as error:
                warnings.append(f"Rows for {_quoted(table)} did not copy: {error}")
            finally:
                data_file.unlink(missing_ok=True)

    return warnings


__all__ = ["BulkCopyError", "copy_tables", "token_file"]
