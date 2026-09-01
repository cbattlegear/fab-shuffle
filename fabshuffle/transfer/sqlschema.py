"""T-SQL schema transfer for warehouses and lakehouse SQL analytics endpoints.

There is still no Fabric REST API that exports a warehouse or SQL analytics endpoint
schema, so this shells out to ``sqlpackage`` for the DACPAC extract and applies the
generated script over TDS with ``pyodbc``.
"""

from __future__ import annotations

import logging
import re
import subprocess
import time
import uuid
from collections.abc import Callable
from pathlib import Path

import pyodbc

from fabshuffle.auth import ServicePrincipal, TokenProvider, sql_access_token_struct
from fabshuffle.config import SETTINGS

logger = logging.getLogger(__name__)

SQL_COPT_SS_ACCESS_TOKEN = 1256
CONNECT_ATTEMPTS = 40
CONNECT_WAIT_SECONDS = 15

# sqlpackage emits a SQLCMD preamble that Fabric's endpoint cannot parse. Everything up to
# the first GO after the header block is boilerplate, so the script is trimmed there.
_HEADER_END = re.compile(r"^GO\s*$", re.IGNORECASE | re.MULTILINE)
_BATCH_SEPARATOR = re.compile(r"^\s*GO\s*$", re.IGNORECASE | re.MULTILINE)

# SQLCMD directives. These are interpreted by the sqlcmd utility, not by the server, so a
# plain TDS connection reports them as syntax errors.
_SQLCMD_DIRECTIVE = re.compile(r"^\s*:[A-Za-z!].*$", re.MULTILINE)
_SETVAR = re.compile(
    r"""^\s*:setvar\s+(?P<name>\w+)\s+(?:"(?P<quoted>[^"]*)"|(?P<bare>\S*))\s*$""",
    re.IGNORECASE | re.MULTILINE,
)
_SQLCMD_VARIABLE = re.compile(r"\$\((\w+)\)")
# The deployment script switches into its own database. We are already connected to the
# target, whose database is named after the *target* item, so this can only ever be wrong,
# and Fabric rejects it outright (08004). Matched line by line, which is how every generated
# script writes it, so a string literal mentioning USE elsewhere is left alone.
_USE_STATEMENT = re.compile(
    r"^[ \t]*USE\s+(?:\[[^\]\r\n]*\]|\"[^\"\r\n]*\"|[^\s;]+)[ \t]*;?[ \t]*$",
    re.IGNORECASE | re.MULTILINE,
)
# DacFx guards its script with a SQLCMD-mode check that turns execution off for the rest of
# the session. It must never reach the server: NOEXEC is connection scoped, so one stray
# batch silently turns every later batch into a no-op and the schema is never applied.
_NOEXEC = re.compile(r"\bSET\s+NOEXEC\s+ON\b", re.IGNORECASE)
# Comments, so a batch left holding nothing but commentary can be recognised as empty.
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT = re.compile(r"--[^\r\n]*")


class SchemaTransferError(RuntimeError):
    """Schema extraction or deployment failed."""


def _driver() -> str:
    drivers = [d for d in pyodbc.drivers() if "ODBC Driver" in d and "SQL Server" in d]
    if not drivers:
        raise SchemaTransferError(
            "No Microsoft ODBC Driver for SQL Server is installed; cannot transfer T-SQL schema"
        )
    return sorted(drivers)[-1]


def connect(server: str, database: str, tokens: TokenProvider) -> pyodbc.Connection:
    connection_string = (
        f"Driver={{{_driver()}}};Server={server},1433;Database={database};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    token = sql_access_token_struct(tokens.sql_token())
    return pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token})


def wait_for_database(
    server: str,
    database: str,
    tokens: TokenProvider,
    *,
    on_progress: Callable[[str], None] | None = None,
) -> None:
    """Block until the endpoint accepts a query, which lags item creation by minutes."""
    last_error: Exception | None = None
    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        try:
            with connect(server, database, tokens) as connection:
                connection.cursor().execute("SELECT 1").fetchall()
            return
        except pyodbc.Error as error:
            last_error = error
            if on_progress and attempt % 4 == 1:
                on_progress(f"Waiting for SQL endpoint {database} to come online")
            time.sleep(CONNECT_WAIT_SECONDS)
    raise SchemaTransferError(
        f"SQL endpoint {server}/{database} never became available: {last_error}"
    )


def extract_dacpac(
    *,
    server: str,
    database: str,
    principal: ServicePrincipal,
    output: Path,
) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    connection_string = (
        f"Server={server};Initial Catalog={database};Encrypt=True;TrustServerCertificate=False;"
        "Connection Timeout=60;Authentication=Active Directory Service Principal;"
        f"User Id={principal.client_id};Password={principal.client_secret}"
    )
    _run(
        [
            SETTINGS.sqlpackage_path,
            "/Action:Extract",
            f"/TargetFile:{output}",
            f"/SourceConnectionString:{connection_string}",
        ],
        what=f"sqlpackage extract of {database}",
    )
    return output


def unpack_dacpac(dacpac: Path, destination: Path, *, exclude_tables: bool) -> Path:
    """Turn a DACPAC into a deployable script, optionally without table DDL.

    Lakehouse SQL analytics endpoints materialise their own tables from the delta files, so
    replaying table and schema DDL there would conflict with the endpoint's own metadata.
    """
    destination.mkdir(parents=True, exist_ok=True)
    command = [SETTINGS.unpackdacpac_path, "unpack", str(dacpac), str(destination)]
    if exclude_tables:
        command += [
            "--deploy-script-exclude-object-type",
            "Tables",
            "--deploy-script-exclude-object-type",
            "Schemas",
        ]
    _run(command, what=f"unpackdacpac of {dacpac.name}")

    script = destination / "Deploy.sql"
    if not script.exists():
        raise SchemaTransferError(f"unpackdacpac did not produce {script}")
    return script


def _strip_sqlcmd_header(script: str) -> str:
    match = _HEADER_END.search(script)
    return script[match.end() :] if match else script


def resolve_sqlcmd(script: str) -> str:
    """Turn a sqlpackage deployment script into something a plain TDS connection can run.

    sqlpackage writes for the sqlcmd utility: ``:setvar`` directives define variables, and the
    body references them as ``$(Name)``. Sent over ODBC the directives are syntax errors and
    the references are never substituted, which is how ``$(__IsSqlCmdEnabled)`` ends up
    failing DacFx's own SQLCMD-mode check and switching NOEXEC on for the whole session.

    This mirrors what ``Invoke-Sqlcmd -DisableCommands`` did in v1: read the variables, drop
    the directives, and substitute the references so the guard evaluates the way it would
    under real sqlcmd.
    """
    variables = {
        match.group("name"): (match.group("quoted") or match.group("bare") or "")
        for match in _SETVAR.finditer(script)
    }
    script = _SQLCMD_DIRECTIVE.sub("", script)
    script = _USE_STATEMENT.sub("", script)
    return _SQLCMD_VARIABLE.sub(lambda m: variables.get(m.group(1), m.group(0)), script)


def _is_noise(batch: str) -> bool:
    """Whether a batch is deployment scaffolding rather than schema.

    Removing the SQLCMD directives and USE statements can leave a batch holding only the
    comment that introduced them, which is not worth a round trip and reads as a failure if
    the endpoint rejects it.
    """
    if _NOEXEC.search(batch):
        return True
    without_comments = _LINE_COMMENT.sub("", _BLOCK_COMMENT.sub("", batch))
    return not without_comments.strip()


def _batches(script: str) -> list[str]:
    return [
        batch.strip()
        for batch in _BATCH_SEPARATOR.split(script)
        if batch.strip() and not _is_noise(batch.strip())
    ]


def apply_script(
    script_path: Path,
    *,
    server: str,
    database: str,
    tokens: TokenProvider,
    on_progress: Callable[[str], None] | None = None,
) -> list[str]:
    """Execute a deployment script batch by batch, collecting per-batch failures."""
    script = resolve_sqlcmd(_strip_sqlcmd_header(script_path.read_text(encoding="utf-8-sig")))
    batches = _batches(script)
    if not batches:
        return []

    warnings: list[str] = []
    with connect(server, database, tokens) as connection:
        connection.autocommit = True
        cursor = connection.cursor()
        for index, batch in enumerate(batches, start=1):
            try:
                cursor.execute(batch)
            except pyodbc.Error as error:
                summary = " ".join(batch.split())[:120]
                warnings.append(f"Batch {index} failed ({error.args[0] if error.args else error}): {summary}")
            if on_progress and index % 25 == 0:
                on_progress(f"Applied {index}/{len(batches)} schema batches to {database}")
    return warnings


def transfer_schema(
    *,
    source_server: str,
    target_server: str,
    database: str,
    principal: ServicePrincipal,
    tokens: TokenProvider,
    scratch_dir: Path,
    source_type: str,
    on_progress: Callable[[str], None] | None = None,
) -> list[str]:
    """Copy the T-SQL schema of ``database`` from one endpoint to another.

    ``source_type`` is ``"Lakehouse"`` or ``"Warehouse"``; lakehouse endpoints skip table
    and schema objects because the endpoint derives those from OneLake itself.
    """
    wait_for_database(target_server, database, tokens, on_progress=on_progress)

    transfer_id = uuid.uuid4().hex[:8]
    dacpac = scratch_dir / f"{database}-{transfer_id}.dacpac"
    unpacked = scratch_dir / f"{database}-{transfer_id}"

    if on_progress:
        on_progress(f"Extracting schema from {database}")
    extract_dacpac(server=source_server, database=database, principal=principal, output=dacpac)

    script = unpack_dacpac(dacpac, unpacked, exclude_tables=source_type == "Lakehouse")

    if on_progress:
        on_progress(f"Applying schema to {database}")
    return apply_script(
        script,
        server=target_server,
        database=database,
        tokens=tokens,
        on_progress=on_progress,
    )


def list_base_tables(
    server: str,
    database: str,
    tokens: TokenProvider,
) -> list[tuple[str, str]]:
    """Return ``(schema, table)`` for every base table, over TDS.

    Used where no REST listing is available: warehouses have none at all, and the lakehouse
    tables API rejects schema-enabled lakehouses outright.
    """
    with connect(server, database, tokens) as connection:
        rows = (
            connection.cursor()
            .execute(
                "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE'"
            )
            .fetchall()
        )
    return [(row[0], row[1]) for row in rows]


def _run(command: list[str], *, what: str) -> None:
    logger.debug("Running %s", command[0])
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
    except FileNotFoundError as error:
        raise SchemaTransferError(
            f"{what} could not run because '{command[0]}' is not installed in this image. "
            "T-SQL schema transfer needs sqlpackage and unpackdacpac."
        ) from error
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()[-1500:]
        raise SchemaTransferError(f"{what} failed with exit code {result.returncode}: {detail}")


__all__ = [
    "SchemaTransferError",
    "apply_script",
    "connect",
    "extract_dacpac",
    "list_base_tables",
    "transfer_schema",
    "unpack_dacpac",
    "wait_for_database",
]
