"""Pre-disaster SQL portable data exports and source-offline restoration.

Fabric documents data-bearing DACPAC extract/publish, including typed exclusions:
https://learn.microsoft.com/fabric/database/sql/sqlpackage#extract-and-publish-portability
Raw BACPAC Import lacks permission exclusions. ModelFilePath surgery is deliberately
not used: Learn reserves it for troubleshooting and warns of unintended data loss.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
import zipfile
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from xml.etree import ElementTree

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.protection import (
    Cancel,
    ConsistencyEvidence,
    DataArtifact,
    DataEndpoint,
    DataIdentity,
    ProtectedLocation,
    ProtectionAssessment,
    ProtectionError,
    ProtectionLimits,
    ProtectionManifest,
    capture_directory,
    digest_file,
    evidence_ref,
    operation_cancel,
    require_independent,
    staged_artifacts,
    validate_manifest,
)
from fabshuffle.config import SETTINGS
from fabshuffle.transfer.common import StagingBudgetError, check_cancelled
from fabshuffle.transfer.sqlschema import SECURITY_OBJECT_TYPES, connect

# No user SQL, deployment contributor, connection string or executable path is accepted
# from a protection descriptor. These options cannot be overridden by the caller.
PUBLISH_OPTIONS = (
    f"/p:ExcludeObjectTypes={';'.join(SECURITY_OBJECT_TYPES)};"
    "DatabaseTriggers;ServerTriggers;ExternalDataSources;ExternalTables;ExternalStreams;"
    "ExternalStreamingJobs;Assemblies;AssemblyFiles;ExternalLibraries;ExternalLanguages",
    "/p:IgnorePermissions=True",
    "/p:IgnoreRoleMembership=True",
    "/p:IgnoreAuthorizer=True",
    "/p:IgnorePreDeployScript=True",
    "/p:IgnorePostDeployScript=True",
    "/p:CreateNewDatabase=False",
    "/p:DropObjectsNotInSource=False",
    "/p:ScriptDatabaseOptions=False",
    "/p:BlockOnPossibleDataLoss=True",
    "/p:VerifyDeployment=True",
    "/p:TreatVerificationErrorsAsWarnings=False",
)


@dataclass(frozen=True, slots=True)
class SqlTableRows:
    schema: str
    table: str
    rows: int

    def __post_init__(self) -> None:
        if not self.schema or not self.table or type(self.rows) is not int or self.rows < 0:
            raise ValueError("SQL table evidence requires schema, table and nonnegative row count.")


@dataclass(frozen=True, slots=True)
class SqlProtection:
    manifest: ProtectionManifest
    tables: tuple[SqlTableRows, ...]
    schema_approval_ref: str


def _ident(value: str) -> str:
    return "[" + value.replace("]", "]]") + "]"


def _table_rows(
    endpoint: DataEndpoint, tokens: TokenProvider, limits: ProtectionLimits, cancel: Cancel
) -> tuple[SqlTableRows, ...]:
    with closing(connect(endpoint.sql_server, endpoint.database, tokens, attempts=1)) as connection:
        cursor = connection.cursor()
        cursor.timeout = limits.timeout_seconds
        if (
            cursor.execute("SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION')").fetchone()[0]
            != 1
        ):
            raise ProtectionError(
                "Grant the recovery identity VIEW DEFINITION for this database before inventory; "
                "a permission-filtered table list cannot establish complete data protection."
            )
        cursor.execute(
            "SELECT s.name, t.name FROM sys.tables t "
            "JOIN sys.schemas s ON s.schema_id = t.schema_id "
            "WHERE t.is_ms_shipped = 0 ORDER BY s.name, t.name"
        )
        names = cursor.fetchmany(limits.max_manifest_entries + 1)
        if len(names) > limits.max_manifest_entries:
            raise StagingBudgetError("SQL table inventory exceeds max_manifest_entries.")
        result = []
        for schema, table in names:
            check_cancelled(cancel)
            rows = cursor.execute(f"SELECT COUNT_BIG(*) FROM {_ident(schema)}.{_ident(table)}").fetchone()[0]
            result.append(SqlTableRows(schema, table, int(rows)))
        return tuple(result)


def _require_empty_target(target: DataEndpoint, tokens: TokenProvider, limits: ProtectionLimits) -> None:
    with closing(connect(target.sql_server, target.database, tokens, attempts=1)) as connection:
        cursor = connection.cursor()
        cursor.timeout = limits.timeout_seconds
        if (
            cursor.execute("SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION')").fetchone()[0]
            != 1
        ):
            raise ProtectionError(
                "Grant VIEW DEFINITION to the recovery identity before checking target emptiness."
            )
        # Includes views/procedures/triggers, not merely tables with zero rows.
        if cursor.execute("SELECT COUNT_BIG(*) FROM sys.objects WHERE is_ms_shipped = 0").fetchone()[0]:
            raise ProtectionError(
                "SQL target has user-defined objects; supply a newly created empty database."
            )


def _inspect_package(path: Path, limits: ProtectionLimits, source: DataEndpoint) -> None:
    """Inspect only bounded XML; never extract or execute arbitrary ZIP members."""
    with zipfile.ZipFile(path) as package:
        members = package.infolist()
        if len(members) > limits.max_manifest_entries:
            raise StagingBudgetError("SQL package has too many members.")
        names: set[str] = set()
        total = 0
        for member in members:
            logical = PurePosixPath(member.filename)
            if (
                logical.is_absolute()
                or ".." in logical.parts
                or "\\" in member.filename
                or ":" in member.filename
                or member.filename.casefold() in names
                or member.flag_bits & 1
            ):
                raise ProtectionError("SQL package contains unsafe, duplicate or encrypted members.")
            names.add(member.filename.casefold())
            total += member.file_size
            if total > limits.max_disk_bytes:
                raise StagingBudgetError("Expanded SQL package exceeds max_disk_bytes.")
        if "model.xml" not in names:
            raise ProtectionError("SQL portable package has no model.xml.")
        member = package.getinfo("model.xml")
        if member.file_size > limits.max_record_bytes:
            raise StagingBudgetError("SQL model XML exceeds max_record_bytes.")
        model = package.read(member)
        # Decode before parsing: byte-pattern checks alone miss UTF-16 declarations.
        text = model.decode("utf-8-sig")
        if "\x00" in text or "<!DOCTYPE" in text.upper() or "<!ENTITY" in text.upper():
            raise ProtectionError("SQL model XML may not contain entity declarations.")
        root = ElementTree.fromstring(text)
        for element in root.iter():
            kind = element.get("Type", "")
            if any(
                term in kind
                for term in (
                    "DmlTrigger",
                    "DdlTrigger",
                    "Assembly",
                    "ExternalData",
                    "ExternalTable",
                    "Synonym",
                    "LinkedServer",
                    "ExternalStream",
                    "RemoteService",
                    "SqlQueue",
                )
            ):
                raise ProtectionError(
                    f"SQL model contains {kind}; provide a reviewed self-contained export without "
                    "triggers, external bindings or active service objects before standby restore."
                )
        text = text.casefold()
        if any(
            value.casefold() in text
            for value in (
                source.sql_server,
                source.identity.workspace_id,
                source.identity.item_id,
            )
        ):
            raise ProtectionError(
                "SQL model retains source references; supply a self-contained reviewed export."
            )


def _run_sqlpackage(
    arguments: list[str], *, token: str, staging: Path, limits: ProtectionLimits, cancel: Cancel
) -> None:
    """Linux-only bounded process, no raw output file and no credentials in errors.

    The SDK/SqlPackage itself can buffer internally. Disk is monitored across its
    private temp tree; output is bounded before decoding. Container resource limits
    remain necessary for a hard process-wide memory/disk quota.
    """
    if os.name != "posix":
        raise ProtectionError("Run SQL protection inside the supported Linux Docker runtime.")
    environment = dict(os.environ, TMPDIR=str(staging), TMP=str(staging), TEMP=str(staging))
    start = time.monotonic()
    output = bytearray()
    with subprocess.Popen(
        [
            SETTINGS.sqlpackage_path,
            *arguments,
            f"/AccessToken:{token}",
            "/Diagnostics:False",
            "/MaxParallelism:1",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=staging,
        env=environment,
        start_new_session=True,
    ) as process:
        assert process.stdout is not None
        os.set_blocking(process.stdout.fileno(), False)
        try:
            while True:
                check_cancelled(cancel)
                if time.monotonic() - start > limits.timeout_seconds:
                    raise ProtectionError(
                        "SqlPackage exceeded timeout_seconds; inspect the stopped target before retry."
                    )
                disk = sum(p.stat().st_size for p in staging.rglob("*") if p.is_file())
                if disk > limits.max_disk_bytes:
                    raise StagingBudgetError("SqlPackage staging exceeds max_disk_bytes.")
                try:
                    chunk = os.read(process.stdout.fileno(), 65536)
                except BlockingIOError:
                    chunk = None
                if chunk:
                    output.extend(chunk)
                    if len(output) > limits.max_record_bytes:
                        raise StagingBudgetError("SqlPackage output exceeds max_record_bytes.")
                if process.poll() is not None and chunk == b"":
                    break
                time.sleep(0.02)
            if process.returncode:
                detail = output.decode("utf-8", errors="replace").replace(token, "[redacted]")
                raise ProtectionError(
                    f"SqlPackage failed with exit code {process.returncode}: {detail.strip()}"
                )
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
    check_cancelled(cancel)


def capture_sql(
    *,
    source: DataEndpoint,
    tokens: TokenProvider,
    location: ProtectedLocation,
    consistency: ConsistencyEvidence,
    schema_approval_ref: str,
    limits: ProtectionLimits = ProtectionLimits(),
    cancel: Cancel = None,
) -> SqlProtection:
    """Capture while healthy/frozen. Caller archives reviewed ACL intentions separately."""
    cancel = operation_cancel(limits, cancel)
    evidence_ref(schema_approval_ref)
    started = datetime.now(UTC)
    consistency.require(started)
    check_cancelled(cancel)
    before = _table_rows(source, tokens, limits, cancel)
    with capture_directory(location) as (staging, name):
        output = staging / "data.dacpac"
        _run_sqlpackage(
            [
                "/Action:Extract",
                f"/SourceServerName:{source.sql_server}",
                f"/SourceDatabaseName:{source.database}",
                f"/TargetFile:{output}",
                "/p:ExtractAllTableData=True",
                "/p:ExtractReferencedServerScopedElements=False",
                "/p:IgnorePermissions=True",
                "/p:IgnoreUserLoginMappings=True",
                "/p:VerifyExtraction=True",
                f"/p:TempDirectoryForTableData={staging}",
            ],
            token=tokens.sql_token(),
            staging=staging,
            limits=limits,
            cancel=cancel,
        )
        _inspect_package(output, limits, source)
        if _table_rows(source, tokens, limits, cancel) != before:
            raise ProtectionError(
                "SQL row counts changed during capture; quiesce writes and repeat the export."
            )
        digest, size = digest_file(output, limits, cancel)
        completed = datetime.now(UTC)
        consistency.require(completed)
        artifact = DataArtifact(f"{name}/data.dacpac", digest, size)
        manifest = ProtectionManifest(
            "sql-dacpac",
            source,
            started,
            completed,
            consistency,
            location.storage_region,
            location.approval_ref,
            (artifact,),
        )
        check_cancelled(cancel)
    return SqlProtection(manifest, before, schema_approval_ref)


def restore_sql(
    protection: SqlProtection,
    *,
    source: DataIdentity,
    target: DataEndpoint,
    tokens: TokenProvider,
    location: ProtectedLocation,
    scratch: Path,
    max_age: timedelta,
    target_approval_ref: str,
    limits: ProtectionLimits = ProtectionLimits(),
    cancel: Cancel = None,
) -> ProtectionAssessment:
    """Restore to a precreated empty restricted target, with no source service client."""
    cancel = operation_cancel(limits, cancel)
    evidence_ref(target_approval_ref)
    evidence_ref(protection.schema_approval_ref)
    manifest = protection.manifest
    require_independent(manifest.source, target)
    status = validate_manifest(
        manifest,
        source=source,
        location=location,
        max_age=max_age,
        limits=limits,
        cancel=cancel,
    )
    if status.state != "protected":
        return status
    if manifest.provider == "sql-bacpac":
        return ProtectionAssessment(
            "deferred",
            warnings=(
                "Raw BACPAC import cannot exclude captured grants. Supply a pre-disaster data-bearing "
                "DACPAC for security-filtered publish; do not grant source users access to the standby.",
            ),
        )
    if manifest.provider != "sql-dacpac" or len(manifest.artifacts) != 1:
        raise ProtectionError("SQL restore requires exactly one data-bearing DACPAC.")
    if len(protection.tables) > limits.max_manifest_entries:
        raise StagingBudgetError("SQL table inventory exceeds max_manifest_entries.")
    if len({(t.schema, t.table) for t in protection.tables}) != len(protection.tables):
        raise ProtectionError("Duplicate table in SQL protection descriptor.")
    if not manifest.artifacts[0].path.endswith(".dacpac"):
        raise ProtectionError("SQL portable data input must have the .dacpac type.")
    with staged_artifacts(manifest, location, scratch, limits, cancel) as staging:
        package = staging / "data.dacpac"
        (staging / "0").rename(package)
        _inspect_package(package, limits, manifest.source)
        _require_empty_target(target, tokens, limits)
        check_cancelled(cancel)
        _run_sqlpackage(
            [
                "/Action:Publish",
                f"/SourceFile:{package}",
                f"/TargetServerName:{target.sql_server}",
                f"/TargetDatabaseName:{target.database}",
                *PUBLISH_OPTIONS,
            ],
            token=tokens.sql_token(),
            staging=staging,
            limits=limits,
            cancel=cancel,
        )
        if _table_rows(target, tokens, limits, cancel) != protection.tables:
            raise ProtectionError("SQL target table counts do not match the capture; keep recovery disabled.")
    return ProtectionAssessment(
        "restored_stopped",
        data_ready=True,
        target=target.identity,
        warnings=(
            "Portable SQL data is restored, not transaction-log/PITR recovery. "
            "Keep application writers stopped; "
            "review deferred SQL permissions and object ownership before Enable recovery.",
        ),
    )
