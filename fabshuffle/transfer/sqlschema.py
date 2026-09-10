"""T-SQL schema transfer for warehouses and lakehouse SQL analytics endpoints.

There is still no Fabric REST API that exports a warehouse or SQL analytics endpoint
schema, so this shells out to ``sqlpackage`` for the DACPAC extract and applies the
generated script over TDS with ``pyodbc``.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Collection, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

import pyodbc

from fabshuffle.auth import ServicePrincipal, TokenProvider, sql_access_token_struct
from fabshuffle.config import SETTINGS
from fabshuffle.fabric.definitions import build_rewriter
from fabshuffle.transfer.common import (
    StagingBudgetError,
    check_budget,
    check_cancelled,
    resolve_memory_budget,
    resolve_transfer_budgets,
)

logger = logging.getLogger(__name__)

SQL_COPT_SS_ACCESS_TOKEN = 1256
CONNECT_ATTEMPTS = 40
CONNECT_WAIT_SECONDS = 15
# sqlpackage runs as its own process with its own connection and its own timeout, so waiting
# for the endpoint over ODBC first does not stop it timing out on the way in.
EXTRACT_ATTEMPTS = 4
EXTRACT_WAIT_SECONDS = 30

# DacFx exclusions, not SQL-text removal. Enum names are verified at
# https://learn.microsoft.com/dotnet/api/microsoft.sqlserver.dac.objecttype
SECURITY_OBJECT_TYPES = (
    "Users", "Logins", "DatabaseRoles", "ApplicationRoles", "ServerRoles",
    "RoleMembership", "ServerRoleMembership", "Permissions", "Credentials",
    "DatabaseScopedCredentials", "LinkedServerLogins",
)

# What a command line tool prints when the endpoint is not ready rather than not reachable.
# These are separate processes, so there is no status to inspect, only what they said.
_TRANSIENT_TOOL_MESSAGES = (
    "connection attempt timed out",
    "network-related or instance-specific error",
    "server was not found or was not accessible",
    "an internal exception was caught",
    "timeout expired",
    "is not currently available",
)

# SQLSTATE prefixes worth retrying. 08 is the connection class (link failure, server
# rejected, unable to establish) and HYT is timeouts, both of which a SQL analytics endpoint
# produces freely while it is still waking up.
_TRANSIENT_SQLSTATES = ("08", "HYT")
# Messages the endpoint returns while a database exists but is not yet servable. These do not
# come back under a connection-class SQLSTATE, so they are matched on text. In particular a
# freshly created warehouse answers a login with 28000 and "the database was not found",
# which reads like a permission problem and is really the endpoint still catching up.
_TRANSIENT_MESSAGES = (
    "not currently available",
    "is not available",
    "try the connection later",
    "please retry",
    "database was not found",
    "cannot open database",
    "cannot open server",
)

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
_SQLCMD_GUARD = re.compile(
    r"IF\s+N?'(?:''|[^'])*'\s+NOT\s+IN\s*\(\s*N?'True'\s*,\s*N?'False'\s*\)"
    r"\s*BEGIN\s+PRINT\s+N?'SQLCMD mode must be enabled to successfully execute this script\.'\s*;"
    r"\s*SET\s+NOEXEC\s+ON\s*;\s*END\s*;?",
    re.IGNORECASE,
)
_MODULE_DEFINITION = re.compile(
    r"^\s*(?:CREATE(?:\s+OR\s+ALTER)?|ALTER)\s+(?:PROC(?:EDURE)?|FUNCTION|TRIGGER)\b",
    re.IGNORECASE,
)


class SchemaTransferError(RuntimeError):
    """Schema extraction or deployment failed."""


class SchemaStagingError(SchemaTransferError, StagingBudgetError):
    """The schema tool's private staging exceeds the operator's configured budget."""


class SchemaMemoryError(SchemaTransferError, StagingBudgetError):
    """A schema script cannot be processed safely inside the configured memory budget."""


def _driver() -> str:
    drivers = [d for d in pyodbc.drivers() if "ODBC Driver" in d and "SQL Server" in d]
    if not drivers:
        raise SchemaTransferError(
            "No Microsoft ODBC Driver for SQL Server is installed; cannot transfer T-SQL schema"
        )
    return sorted(drivers)[-1]


def is_transient(error: pyodbc.Error) -> bool:
    """Whether a failure is worth waiting out rather than giving up on.

    A SQL analytics endpoint is created asynchronously and stays unreachable for minutes, so
    almost every connection failure here is the endpoint not being ready. Authentication and
    permission failures are not, and retrying those just turns a clear error into a ten
    minute hang.
    """
    state = str(error.args[0]) if error.args else ""
    if state.startswith(_TRANSIENT_SQLSTATES):
        return True
    text = str(error).lower()
    return any(message in text for message in _TRANSIENT_MESSAGES)


def _server_with_port(server: str) -> str:
    """Normalise a server address to ``host,port``.

    Endpoints differ: a warehouse or SQL analytics endpoint reports a bare host, while a
    Fabric SQL database's ``serverFqdn`` already carries ``,1433``. Appending unconditionally
    produced ``host,1433,1433``, which ODBC cannot parse.
    """
    address = server.strip()
    host, separator, _port = address.partition(",")
    return address if separator else f"{host},1433"


def connect(
    server: str,
    database: str,
    tokens: TokenProvider,
    *,
    attempts: int = CONNECT_ATTEMPTS,
    on_progress: Callable[[str], None] | None = None,
) -> pyodbc.Connection:
    """Open a connection, waiting out the endpoint if it is not answering yet.

    Every caller goes through here, so the waiting belongs here rather than in each of them.
    Failures are raised as ``SchemaTransferError`` so that a caller which already treats a
    schema transfer as best effort does not have to know about pyodbc as well.
    """
    connection_string = (
        f"Driver={{{_driver()}}};Server={_server_with_port(server)};Database={database};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )

    last_error: pyodbc.Error | None = None
    for attempt in range(1, attempts + 1):
        # The token is fetched per attempt: waiting out a cold endpoint can outlast it.
        token = sql_access_token_struct(tokens.sql_token())
        try:
            return pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token})
        except pyodbc.Error as error:
            if not is_transient(error):
                raise SchemaTransferError(
                    f"Could not connect to {server}/{database}: {error}"
                ) from error
            last_error = error
            if attempt == attempts:
                break
            if on_progress and attempt % 4 == 1:
                on_progress(f"Waiting for SQL endpoint {database} to come online")
            time.sleep(CONNECT_WAIT_SECONDS)

    raise SchemaTransferError(
        f"SQL endpoint {server}/{database} never became available after {attempts} "
        f"attempt(s): {last_error}"
    )


def wait_for_database(
    server: str,
    database: str,
    tokens: TokenProvider,
    *,
    on_progress: Callable[[str], None] | None = None,
) -> None:
    """Block until the endpoint answers a query, which lags item creation by minutes."""
    with connect(server, database, tokens, on_progress=on_progress) as connection:
        connection.cursor().execute("SELECT 1").fetchall()


def extract_dacpac(
    *,
    server: str,
    database: str,
    principal: ServicePrincipal,
    output: Path,
    attempts: int = EXTRACT_ATTEMPTS,
    staging_root: Path | None = None,
    max_staging_bytes: int | None = None,
    max_disk_staging_bytes: int | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> Path:
    """Extract a database's schema, retrying a connection that does not answer in time.

    sqlpackage opens its own connection with its own timeout, so waiting for the endpoint
    over ODBC first is not enough: it proves the endpoint answers a login, not that it will
    finish a metadata read. A freshly refreshed SQL analytics endpoint regularly refuses the
    first attempt and accepts the second.
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    connection_string = (
        f"Server={server};Initial Catalog={database};Encrypt=True;TrustServerCertificate=False;"
        "Connection Timeout=60;Authentication=Active Directory Service Principal;"
        f"User Id={principal.client_id};Password={principal.client_secret}"
    )

    for attempt in range(1, attempts + 1):
        check_cancelled(cancel_requested)
        try:
            runner = _run_bounded if staging_root is not None else _run
            extra: list[str] = []
            options = {}
            if staging_root is not None:
                disk_budget = resolve_transfer_budgets(
                    max_staging_bytes=max_staging_bytes,
                    max_disk_staging_bytes=max_disk_staging_bytes,
                ).max_disk_staging_bytes
                table_temp = staging_root / ".tool-work" / "table-data"
                table_temp.mkdir(parents=True, exist_ok=True)
                extra = [
                    "/p:ExtractAllTableData=False",
                    f"/p:TempDirectoryForTableData={table_temp.resolve()}",
                ]
                options = {
                    "staging_root": staging_root, "max_disk_staging_bytes": disk_budget,
                    "cancel_requested": cancel_requested,
                }
            runner(
                [
                    SETTINGS.sqlpackage_path,
                    "/Action:Extract",
                    f"/TargetFile:{output.resolve()}",
                    f"/SourceConnectionString:{connection_string}",
                    *extra,
                ],
                what=f"sqlpackage extract of {database}",
                **options,
            )
            return output
        except SchemaStagingError:
            raise
        except SchemaTransferError as error:
            if attempt == attempts or not _is_transient_tool_failure(str(error)):
                raise
            logger.warning(
                "sqlpackage extract of %s did not connect, retrying (attempt %s/%s)",
                database,
                attempt,
                attempts,
            )
            output.unlink(missing_ok=True)
            if cancel_requested:
                until = time.monotonic() + EXTRACT_WAIT_SECONDS
                while time.monotonic() < until:
                    check_cancelled(cancel_requested)
                    time.sleep(min(0.1, max(0, until - time.monotonic())))
            else:
                time.sleep(EXTRACT_WAIT_SECONDS)

    return output


def _is_transient_tool_failure(message: str) -> bool:
    """Whether a command line tool's failure is the endpoint not being ready yet.

    Matched on the message because these are separate processes with their own error
    reporting: there is no status code to inspect, only what they printed.
    """
    text = message.lower()
    return any(phrase in text for phrase in _TRANSIENT_TOOL_MESSAGES)


def unpack_dacpac(
    dacpac: Path,
    destination: Path,
    *,
    exclude_tables: bool,
    exclude_security: bool = False,
    staging_root: Path | None = None,
    max_staging_bytes: int | None = None,
    max_disk_staging_bytes: int | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> Path:
    """Turn a DACPAC into a deployable script, optionally without table DDL.

    Lakehouse SQL analytics endpoints materialise their own tables from the delta files, so
    replaying table DDL there would conflict with the endpoint's own metadata.

    Only ``Tables`` is excluded. DacFx's ``ObjectType`` enum has no ``Schemas`` member, and
    asking for one fails the whole command during argument binding, taking the schema
    transfer with it. Custom schemas are wanted anyway, since views on the endpoint live in
    them.
    """
    destination.mkdir(parents=True, exist_ok=True)
    command = [SETTINGS.unpackdacpac_path, "unpack", str(dacpac), str(destination)]
    if exclude_tables:
        command += ["--deploy-script-exclude-object-type", "Tables"]
    if exclude_security:
        command.append("--deploy-script-ignore-permissions")
        for object_type in SECURITY_OBJECT_TYPES:
            command += ["--deploy-script-exclude-object-type", object_type]
    if staging_root is None:
        _run(command, what=f"unpackdacpac of {dacpac.name}")
    else:
        disk_budget = resolve_transfer_budgets(
            max_staging_bytes=max_staging_bytes,
            max_disk_staging_bytes=max_disk_staging_bytes,
        ).max_disk_staging_bytes
        _run_bounded(
            command, what=f"unpackdacpac of {dacpac.name}", staging_root=staging_root,
            max_disk_staging_bytes=disk_budget, cancel_requested=cancel_requested,
        )

    script = destination / "Deploy.sql"
    if not script.exists():
        raise SchemaTransferError(f"unpackdacpac did not produce {script}")
    return script


def script_dacpac(
    dacpac: Path,
    output: Path,
    *,
    server: str,
    database: str,
    tokens: TokenProvider,
    exclude_tables: bool = False,
    exclude_security: bool = True,
    staging_root: Path | None = None,
    max_staging_bytes: int | None = None,
    max_disk_staging_bytes: int | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> Path:
    """Generate a target-aware DacFx schema script with optional typed security exclusions.

    When enabled, DacFx exclusions cover all forms of GRANT/role membership and
    AUTHORIZATION. Opaque pre/post deployment scripts are never executed in this path.
    https://learn.microsoft.com/sql/tools/sqlpackage/sqlpackage-script
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    excluded = list(SECURITY_OBJECT_TYPES) if exclude_security else []
    if exclude_tables:
        excluded.append("Tables")
    command = [
        SETTINGS.sqlpackage_path, "/Action:Script", f"/SourceFile:{dacpac.resolve()}",
        f"/OutputPath:{output.resolve()}", f"/TargetServerName:{server}",
        f"/TargetDatabaseName:{database}", f"/AccessToken:{tokens.sql_token()}",
        "/p:CreateNewDatabase=False", "/p:DropObjectsNotInSource=False",
        "/p:ScriptDatabaseOptions=False", "/p:IgnorePreDeployScript=True",
        "/p:IgnorePostDeployScript=True",
    ]
    if excluded:
        command.append(f"/p:ExcludeObjectTypes={';'.join(excluded)}")
    if exclude_security:
        command.extend([
            "/p:IgnorePermissions=True", "/p:IgnoreRoleMembership=True", "/p:IgnoreAuthorizer=True",
        ])
    if staging_root is None:
        _run(command, what=f"sqlpackage script of {dacpac.name}")
    else:
        disk_budget = resolve_transfer_budgets(
            max_staging_bytes=max_staging_bytes,
            max_disk_staging_bytes=max_disk_staging_bytes,
        ).max_disk_staging_bytes
        _run_bounded(
            command, what=f"sqlpackage script of {dacpac.name}", staging_root=staging_root,
            max_disk_staging_bytes=disk_budget, cancel_requested=cancel_requested,
        )
    if not output.exists():
        raise SchemaTransferError(f"sqlpackage did not produce {output}")
    return output


def rewrite_schema_script(
    script: str,
    *,
    id_map: Mapping[str, str],
    source_identifiers: Collection[str] = (),
    max_memory_bytes: int | None = None,
) -> str:
    """Rebind known identities, refusing an incomplete rewrite before any SQL executes.

    Replacement is single-pass and longest-first, just like item definitions. When checking
    the result, complete target endpoints protect source-ID substrings legitimately retained
    in their names, but a shorter target value must not hide a longer unresolved source ID.
    SQL security is excluded by DacFx before this step, never by editing SQL statements.
    """
    memory_budget = (
        resolve_memory_budget(max_memory_bytes=max_memory_bytes)
        if max_memory_bytes is not None
        else None
    )
    if memory_budget is not None:
        _check_text_memory(script, memory_budget, "SQL schema rewrite input")
    rewrite = build_rewriter(id_map)
    rebound = rewrite(script) if rewrite else script
    if memory_budget is not None:
        _check_text_memory(rebound, memory_budget, "Rewritten SQL schema script")
    known = {value.casefold(): value for value in source_identifiers if value}
    if not known:
        return rebound

    marker = f"\x00{uuid.uuid4().hex}:"
    markers = {identifier: f"{marker}{index}\x00" for index, identifier in enumerate(known.values())}
    targets = {
        value: "\x00"
        for key, value in id_map.items()
        if key and value and key.casefold() != value.casefold() and value.casefold() not in known
    }
    inspect = build_rewriter({**markers, **targets})
    inspected = inspect(rebound)
    if memory_budget is not None:
        _check_text_memory(inspected, memory_budget, "SQL schema source-reference scan")
    unresolved = sorted(key for key, token in markers.items() if token in inspected)
    if unresolved:
        raise SchemaTransferError(
            f"SQL schema still references source identities: {', '.join(unresolved)}. "
            "Migrate those dependencies and supply their destination mappings before retrying; "
            "no schema batches were executed."
        )
    return rebound


def _staging_size(root: Path) -> int:
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_symlink():
                raise SchemaStagingError(f"Cannot bound schema staging through symbolic link '{path}'.")
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            # SqlPackage removes temporary files while the monitor is enumerating them.
            continue
    return total


def _check_staging(root: Path, maximum: int, *, additional: int = 0) -> None:
    used = _staging_size(root) + additional
    if used > maximum:
        raise SchemaStagingError(
            f"SQL schema staging requires {used} bytes, above the {maximum}-byte staging budget "
            "for disk artifacts. Schema transfer stopped; increase max_disk_staging_bytes or "
            "reduce the schema before retrying. This is an estimated staging check, not a "
            "filesystem quota."
        )


def _check_schema_memory(required: int, maximum: int, what: str) -> None:
    check_budget(maximum, "max_memory_bytes")
    if required > maximum:
        raise SchemaMemoryError(
            f"{what} needs an estimated {required} memory bytes, above the "
            f"{maximum}-byte memory budget. Increase max_memory_bytes or reduce the schema; "
            "raising the disk staging budget does not raise this in-memory limit."
        )


def _check_text_memory(text: str, maximum: int, what: str) -> None:
    _check_schema_memory(sys.getsizeof(text), maximum, what)


def _read_text_bounded(path: Path, *, encoding: str, max_memory_bytes: int, what: str) -> str:
    size = path.stat().st_size
    _check_schema_memory(size, max_memory_bytes, what)
    text = path.read_text(encoding=encoding)
    _check_text_memory(text, max_memory_bytes, f"{what} decoded text")
    return text


def _stage_member(path: Path, root: Path) -> None:
    if not path.resolve().is_relative_to(root.resolve()):
        raise SchemaStagingError(f"Schema artifact '{path}' is outside its bounded staging directory.")


@contextmanager
def _schema_staging(scratch_dir: Path, bounded: bool):
    if not bounded:
        yield scratch_dir
        return
    root = scratch_dir / f"schema-{uuid.uuid4().hex}"
    root.mkdir(mode=0o700, parents=True)
    try:
        yield root
    finally:
        try:
            shutil.rmtree(root)
        except OSError as error:
            raise SchemaTransferError(
                f"Could not remove private SQL schema staging '{root}': {error}. "
                "Remove that directory before retrying."
            ) from error


def _strip_sqlcmd_header(script: str) -> str:
    match = _HEADER_END.search(script)
    return script[match.end() :] if match else script


def resolve_sqlcmd(script: str, *, max_memory_bytes: int | None = None) -> str:
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
    if max_memory_bytes is not None:
        characters = len(script)
        width = 1 if script.isascii() and all(value.isascii() for value in variables.values()) else 4
        for match in _SQLCMD_VARIABLE.finditer(script):
            characters += len(variables.get(match.group(1), match.group(0))) - len(match.group(0))
        _check_schema_memory(
            sys.getsizeof("") + characters * width, max_memory_bytes, "SQLCMD expansion",
        )
    return _SQLCMD_VARIABLE.sub(lambda m: variables.get(m.group(1), m.group(0)), script)


def _sql_guard_text(batch: str) -> tuple[str, str]:
    """Remove comments and mask quoted tokens for guard detection, not execution."""
    text: list[str] = []
    code: list[str] = []
    index = 0
    while index < len(batch):
        if batch.startswith("--", index):
            end = batch.find("\n", index + 2)
            index = len(batch) if end < 0 else end
            text.append(" ")
            code.append(" ")
        elif batch.startswith("/*", index):
            depth = 1
            index += 2
            while index < len(batch) and depth:
                if batch.startswith("/*", index):
                    depth += 1
                    index += 2
                elif batch.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                raise SchemaTransferError("Unterminated SQL block comment; correct the deployment script.")
            text.append(" ")
            code.append(" ")
        elif batch[index] in "'\"[":
            start = index
            closer = "]" if batch[index] == "[" else batch[index]
            index += 1
            while index < len(batch):
                if batch[index] != closer:
                    index += 1
                elif index + 1 < len(batch) and batch[index + 1] == closer:
                    index += 2
                else:
                    index += 1
                    break
            else:
                raise SchemaTransferError("Unterminated SQL quoted token; correct the deployment script.")
            text.append(batch[start:index])
            code.append("?")
        else:
            text.append(batch[index])
            code.append(batch[index])
            index += 1
    return "".join(text), "".join(code)


def _is_noise(batch: str) -> bool:
    """Whether a batch is deployment scaffolding rather than schema.

    Removing the SQLCMD directives and USE statements can leave a batch holding only the
    comment that introduced them, which is not worth a round trip and reads as a failure if
    the endpoint rejects it.
    """
    without_comments, code = _sql_guard_text(batch)
    if not without_comments.strip():
        return True
    if not _NOEXEC.search(code) or _MODULE_DEFINITION.match(code):
        return False
    if (
        _SQLCMD_GUARD.fullmatch(without_comments.strip())
        or re.fullmatch(r"\s*SET\s+NOEXEC\s+ON\s*;?\s*", code, re.IGNORECASE)
    ):
        return True
    raise SchemaTransferError(
        "Unrecognized executable SET NOEXEC ON outside the SQLCMD guard. Remove or correct "
        "that statement before deploying; discarding its batch could lose schema, and "
        "executing it could disable subsequent schema batches."
    )


def _batch_candidates(script: str) -> Iterator[str]:
    start = 0
    for match in _BATCH_SEPARATOR.finditer(script):
        yield script[start : match.start()]
        start = match.end()
    yield script[start:]


def _batches(script: str, *, max_memory_bytes: int | None = None) -> list[str]:
    memory_budget = (
        resolve_memory_budget(max_memory_bytes=max_memory_bytes)
        if max_memory_bytes is not None
        else None
    )
    retained = sys.getsizeof([]) + (sys.getsizeof(script) if memory_budget is not None else 0)
    if memory_budget is not None:
        _check_schema_memory(retained, memory_budget, "SQL schema batch input")
    batches: list[str] = []
    for candidate in _batch_candidates(script):
        batch = candidate.strip()
        if not batch or _is_noise(batch):
            continue
        if memory_budget is not None:
            retained += sys.getsizeof(batch) + 8
            _check_schema_memory(retained, memory_budget, "SQL schema batch list")
        batches.append(batch)
    return batches


def _already_exists(error: pyodbc.Error) -> bool:
    """Whether a batch failed only because the object it creates is already there.

    Deploying into a lakehouse SQL analytics endpoint replays DDL for schemas the endpoint
    derives from OneLake itself, so these are expected rather than problems.
    """
    state = str(error.args[0]) if error.args else ""
    if state == "42S01":
        return True
    text = str(error).lower()
    return "already exists" in text or "there is already an object named" in text


def apply_script(
    script_path: Path,
    *,
    server: str,
    database: str,
    tokens: TokenProvider,
    max_staging_bytes: int | None = None,
    max_memory_bytes: int | None = None,
    cancel_requested: Callable[[], bool] | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> list[str]:
    """Execute a deployment script batch by batch, collecting per-batch failures."""
    memory_budget = resolve_memory_budget(
        max_staging_bytes=max_staging_bytes,
        max_memory_bytes=max_memory_bytes,
    )
    script = _read_text_bounded(
        script_path,
        encoding="utf-8-sig",
        max_memory_bytes=memory_budget,
        what=f"SQL schema script '{script_path.name}'",
    )
    script = _strip_sqlcmd_header(script)
    _check_text_memory(script, memory_budget, "SQL schema script after header trim")
    script = resolve_sqlcmd(script, max_memory_bytes=memory_budget)
    _check_text_memory(script, memory_budget, "SQL schema script after SQLCMD expansion")
    batches = _batches(script, max_memory_bytes=memory_budget)
    if not batches:
        return []

    warnings: list[str] = []
    with connect(server, database, tokens, on_progress=on_progress) as connection:
        connection.autocommit = True
        cursor = connection.cursor()
        for index, batch in enumerate(batches, start=1):
            check_cancelled(cancel_requested)
            try:
                cursor.execute(batch)
            except pyodbc.Error as error:
                if _already_exists(error):
                    # A schema-enabled lakehouse derives its schemas from OneLake, so the
                    # deploy script recreating them is expected and not worth reporting.
                    logger.debug("Skipping batch %s, the object already exists", index)
                    continue
                summary = " ".join(batch.split())[:120]
                warnings.append(f"Batch {index} failed ({error}): {summary}")
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
    target_tokens: TokenProvider | None = None,
    target_database: str | None = None,
    id_map: Mapping[str, str] | None = None,
    source_identifiers: Collection[str] = (),
    max_staging_bytes: int | None = None,
    max_memory_bytes: int | None = None,
    max_disk_staging_bytes: int | None = None,
    exclude_security: bool | None = None,
    cancel_requested: Callable[[], bool] | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> list[str]:
    """Copy the T-SQL schema of ``database`` from one endpoint to another.

    ``source_type`` is ``"Lakehouse"``, ``"Warehouse"`` or ``"SQLDatabase"``; lakehouse
    endpoints skip table objects because the endpoint derives those from OneLake itself.
    Supplying ``target_tokens`` selects target-aware DacFx scripting, rather than replaying
    an opaque Fabric SQLDatabase definition. Security is excluded by default for paired
    credentials; an explicit ``exclude_security=False`` preserves same-tenant security handling.
    ``database`` is the source SQL catalog, not its Fabric item ID. ``target_database`` may
    differ. ``id_map`` and ``source_identifiers`` cover the other migrating items/endpoints.
    Schema transfers clean their private package/script/tool staging on every exit. Tool
    output and redirected temporary files count toward ``max_disk_staging_bytes``; SQL text
    reads, SQLCMD expansion, rewrite checks and batch materialization count toward
    ``max_memory_bytes``. ``max_staging_bytes`` remains a compatibility fallback for both
    when supplied. These are estimated process checks, not filesystem or OS memory quotas.
    """
    # Both ends are waited for. The target may still be provisioning, and a source SQL
    # analytics endpoint can be cold enough that sqlpackage's own connection attempt times
    # out before it has answered once.
    check_cancelled(cancel_requested)
    budgets = resolve_transfer_budgets(
        max_staging_bytes=max_staging_bytes,
        max_memory_bytes=max_memory_bytes,
        max_disk_staging_bytes=max_disk_staging_bytes,
    )
    memory_budget = budgets.max_memory_bytes
    disk_budget = budgets.max_disk_staging_bytes
    if exclude_security is None:
        exclude_security = target_tokens is not None
    target_aware_script = target_tokens is not None or exclude_security
    destination_tokens = target_tokens or tokens
    destination_database = target_database or database
    wait_for_database(source_server, database, tokens, on_progress=on_progress)
    check_cancelled(cancel_requested)
    wait_for_database(
        target_server, destination_database, destination_tokens, on_progress=on_progress,
    )

    with _schema_staging(scratch_dir, True) as staging:
        dacpac = staging / "schema.dacpac"
        unpacked = staging / "script"
        tool_options = {
            "staging_root": staging, "max_disk_staging_bytes": disk_budget,
            "cancel_requested": cancel_requested,
        }

        if on_progress:
            on_progress(f"Extracting schema from {database}")
        extract_dacpac(
            server=source_server, database=database, principal=principal, output=dacpac, **tool_options,
        )

        check_cancelled(cancel_requested)
        _check_staging(staging, disk_budget)
        if target_aware_script:
            script = script_dacpac(
                dacpac, unpacked / "Deploy.sql", server=target_server, database=destination_database,
                tokens=destination_tokens, exclude_tables=source_type == "Lakehouse",
                exclude_security=exclude_security, **tool_options,
            )
        else:
            script = unpack_dacpac(
                dacpac, unpacked, exclude_tables=source_type == "Lakehouse", **tool_options,
            )
        _stage_member(script, staging)
        _check_staging(staging, disk_budget)

        if target_tokens is not None or id_map is not None or source_identifiers:
            replacements = dict(id_map or {})
            known = set(source_identifiers)
            for old, new in (
                (source_server, target_server),
                (source_server.partition(",")[0], target_server.partition(",")[0]),
                (database, destination_database),
            ):
                if old and new and old.casefold() != new.casefold():
                    existing = next(
                        (value for key, value in replacements.items() if key.casefold() == old.casefold()),
                        None,
                    )
                    if existing and existing.casefold() != new.casefold():
                        raise SchemaTransferError(
                            f"The SQL mapping for '{old}' conflicts with destination '{new}'. "
                            "Correct the endpoint/catalog mapping before retrying; "
                            "no schema batches were executed."
                        )
                    replacements[old] = new
                    known.add(old)
            text = _read_text_bounded(
                script,
                encoding="utf-8-sig",
                max_memory_bytes=memory_budget,
                what=f"SQL schema script '{script.name}'",
            )
            rebound = rewrite_schema_script(
                text,
                id_map=replacements,
                source_identifiers=known,
                max_memory_bytes=memory_budget,
            )
            if rebound != text:
                _check_text_memory(rebound, memory_budget, "Rewritten SQL schema script")
                encoded = rebound.encode("utf-8")
                _check_schema_memory(len(encoded), memory_budget, "Encoded rewritten SQL schema script")
                _check_staging(
                    staging, disk_budget,
                    additional=max(0, len(encoded) - script.stat().st_size),
                )
                script.write_bytes(encoded)

        check_cancelled(cancel_requested)
        _check_staging(staging, disk_budget)
        if on_progress:
            on_progress(f"Applying schema to {database}")
        return apply_script(
            script,
            server=target_server,
            database=destination_database,
            tokens=destination_tokens,
            max_memory_bytes=memory_budget,
            on_progress=on_progress,
            **({"cancel_requested": cancel_requested} if cancel_requested else {}),
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


def _stop_tool(process: subprocess.Popen, environment: dict[str, str]) -> None:
    if process.poll() is None:
        try:
            if os.name == "nt":
                # Windows launchers can own the real tool as a child. Killing only the
                # launcher leaves that child writing files after cancellation.
                shell = shutil.which("pwsh") or shutil.which("powershell")
                if not shell:
                    raise SchemaTransferError("PowerShell is required to stop the owned schema process tree.")
                stop = (
                    "$ErrorActionPreference='Stop'; "
                    "function Stop-OwnedTree([int]$processId) { "
                    'Get-CimInstance Win32_Process -Filter "ParentProcessId = $processId" | '
                    "ForEach-Object { Stop-OwnedTree $_.ProcessId }; "
                    "Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue }; "
                    f"Stop-OwnedTree {process.pid}; exit 0"
                )
                try:
                    result = subprocess.run(
                        [shell, "-NoProfile", "-NonInteractive", "-Command", stop],
                        capture_output=True, text=True, check=False, timeout=20, env=environment,
                    )
                finally:
                    if process.poll() is None:
                        process.kill()
                if result.returncode:
                    raise SchemaTransferError(
                        f"Could not stop schema process tree {process.pid}: {result.stderr.strip()}"
                    )
            else:
                os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait()


def _tool_output(path: Path) -> str:
    with path.open("rb") as stream:
        stream.seek(max(0, path.stat().st_size - 1500))
        return stream.read(1500).decode("utf-8", errors="replace").strip()


def _run_bounded(
    command: list[str],
    *,
    what: str,
    staging_root: Path,
    max_staging_bytes: int | None = None,
    max_disk_staging_bytes: int | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> None:
    """Monitor all owned files while SqlPackage runs, retaining the service's own error.

    TMP/TEMP/TMPDIR redirect SqlPackage's documented temporary-file location. The CLI home,
    single-file bundle extraction and host tracing are also brought inside the same budget.
    This monitor reports overshoot; a hard OS-enforced limit requires a filesystem quota.
    https://learn.microsoft.com/sql/tools/sqlpackage/sqlpackage#temporary-files
    https://learn.microsoft.com/dotnet/core/tools/dotnet-environment-variables
    """
    disk_budget = resolve_transfer_budgets(
        max_staging_bytes=max_staging_bytes,
        max_disk_staging_bytes=max_disk_staging_bytes,
    ).max_disk_staging_bytes
    check_budget(disk_budget, "max_disk_staging_bytes")
    check_cancelled(cancel_requested)
    root = staging_root.resolve()
    work = root / ".tool-work"
    work.mkdir(mode=0o700, parents=True, exist_ok=True)
    _check_staging(root, disk_budget)
    environment = {
        **os.environ,
        **dict.fromkeys(("TMP", "TEMP", "TMPDIR", "DOTNET_CLI_HOME"), str(work)),
        "DOTNET_BUNDLE_EXTRACT_BASE_DIR": str(work / "bundles"),
        "DOTNET_HOST_TRACEFILE": str(work / "host-trace.log"),
        "COREHOST_TRACEFILE": str(work / "host-trace.log"),
        "DOTNET_CLI_WORKLOAD_UPDATE_NOTIFY_DISABLE": "true",
        "DOTNET_CLI_TELEMETRY_OPTOUT": "true",
        "DACFX_TELEMETRY_OPTOUT": "true",
    }
    executable = shutil.which(command[0])
    if executable:
        command = [str(Path(executable).resolve()), *command[1:]]
    output = root / f".tool-output-{uuid.uuid4().hex}.log"
    with output.open("wb") as log:
        try:
            process = subprocess.Popen(
                command, cwd=root, env=environment, stdin=subprocess.DEVNULL,
                stdout=log, stderr=subprocess.STDOUT, start_new_session=os.name != "nt",
            )
        except FileNotFoundError as error:
            raise SchemaTransferError(
                f"{what} could not run because '{command[0]}' is not installed in this runtime. "
                "T-SQL schema transfer needs sqlpackage."
            ) from error
        try:
            while True:
                check_cancelled(cancel_requested)
                _check_staging(root, disk_budget)
                if process.poll() is not None:
                    break
                time.sleep(0.05)
            _check_staging(root, disk_budget)
            check_cancelled(cancel_requested)
        except SchemaStagingError as error:
            _stop_tool(process, environment)
            detail = _tool_output(output)
            if detail:
                raise SchemaStagingError(f"{error} Tool output: {detail}") from error
            raise
        finally:
            _stop_tool(process, environment)
    if process.returncode:
        raise SchemaTransferError(
            f"{what} failed with exit code {process.returncode}: {_tool_output(output)}"
        )


def _run(command: list[str], *, what: str) -> None:
    logger.debug("Running %s", command[0])
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
    except FileNotFoundError as error:
        raise SchemaTransferError(
            f"{what} could not run because '{command[0]}' is not installed in this runtime. "
            "T-SQL schema transfer needs sqlpackage and unpackdacpac."
        ) from error
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()[-1500:]
        raise SchemaTransferError(f"{what} failed with exit code {result.returncode}: {detail}")


__all__ = [
    "SECURITY_OBJECT_TYPES",
    "SchemaMemoryError",
    "SchemaStagingError",
    "SchemaTransferError",
    "apply_script",
    "connect",
    "extract_dacpac",
    "is_transient",
    "list_base_tables",
    "rewrite_schema_script",
    "script_dacpac",
    "transfer_schema",
    "unpack_dacpac",
    "wait_for_database",
]
