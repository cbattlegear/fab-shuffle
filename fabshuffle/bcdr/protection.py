"""Optional workload data protection, independent of the metadata Warehouse.

Capture returns a complete, secret-free descriptor for the caller to persist in its
Warehouse generation. Files here are optional BUSINESS DATA inputs, not a recovery
metadata archive. Restore never discovers or reads the source workload.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import stat
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from fabshuffle.transfer.common import StagingBudgetError, check_budget, check_cancelled

Provider = Literal["sql-dacpac", "sql-bacpac", "cosmos-logical", "kql-prepared"]
Cancel = Callable[[], bool] | None


class ProtectionError(RuntimeError):
    """Protection was not completed; callers must not mark the item data-ready."""


def evidence_ref(value: str) -> str:
    """Evidence is an opaque approval/run ID, never free text or a credential URL."""
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value):
        raise ValueError("Supply an opaque evidence ID (letters, digits, '-' or '_'), not secrets or a URL.")
    return value


def utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Protection timestamps must include a timezone.")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class DataIdentity:
    tenant_id: str
    workspace_id: str
    item_id: str

    def __post_init__(self) -> None:
        for name in ("tenant_id", "workspace_id", "item_id"):
            object.__setattr__(self, name, str(UUID(getattr(self, name))))


@dataclass(frozen=True, slots=True)
class DataEndpoint:
    identity: DataIdentity
    endpoint: str
    database: str

    def __post_init__(self) -> None:
        if not self.database or any(c in self.database for c in "\r\n;{}\x00"):
            raise ValueError("Supply an explicit database name without connection-string delimiters.")
        address = urlsplit(self.endpoint)
        if (
            address.scheme != "https"
            or not address.hostname
            or address.username
            or address.password
            or address.query
            or address.fragment
            or address.path not in ("", "/")
            or address.port not in (None, 443)
        ):
            raise ValueError("Supply a credential-free HTTPS endpoint, without paths, query or fragment.")
        if not re.fullmatch(r"[A-Za-z0-9.-]+", address.hostname):
            raise ValueError("The endpoint must be a DNS hostname.")
        object.__setattr__(self, "endpoint", f"https://{address.hostname.lower()}")

    @property
    def sql_server(self) -> str:
        return urlsplit(self.endpoint).hostname or ""


@dataclass(frozen=True, slots=True)
class ConsistencyEvidence:
    reference: str
    verified_at: datetime
    valid_until: datetime
    writes_quiesced: bool
    ttl_quiesced: bool = False

    def require(self, now: datetime, *, cosmos: bool = False) -> None:
        evidence_ref(self.reference)
        if not utc(self.verified_at) <= utc(now) <= utc(self.valid_until):
            raise ProtectionError("Renew the quiescence evidence so it covers the entire capture.")
        if self.writes_quiesced is not True or (cosmos and self.ttl_quiesced is not True):
            raise ProtectionError("Quiesce source writes and, for Cosmos, TTL expiration before capture.")


@dataclass(frozen=True, slots=True)
class ProtectionLimits:
    max_disk_bytes: int = 8 * 1024**3
    max_record_bytes: int = 8 * 1024**2
    max_manifest_entries: int = 10000
    timeout_seconds: int = 3600

    def __post_init__(self) -> None:
        for name in ("max_disk_bytes", "max_record_bytes", "max_manifest_entries", "timeout_seconds"):
            check_budget(getattr(self, name), name)


def operation_cancel(limits: ProtectionLimits, cancel: Cancel) -> Callable[[], bool]:
    """Check the overall streaming deadline between bounded SDK requests."""
    deadline = time.monotonic() + limits.timeout_seconds

    def check() -> bool:
        check_cancelled(cancel)
        if time.monotonic() > deadline:
            raise ProtectionError("Data protection exceeded timeout_seconds; keep the target unready.")
        return False

    return check


@dataclass(frozen=True, slots=True)
class ProtectedLocation:
    root: Path
    source_region: str
    storage_region: str
    approval_ref: str

    def __post_init__(self) -> None:
        evidence_ref(self.approval_ref)
        for region in (self.source_region, self.storage_region):
            if not re.fullmatch(r"[a-z0-9-]{1,64}", region):
                raise ValueError("Use canonical region IDs.")
        if self.source_region == self.storage_region:
            raise ProtectionError(
                "Store optional data exports in an approved region outside the source region."
            )
        if not self.root.is_absolute() or not self.root.is_dir() or self.root.is_symlink():
            raise ValueError("The approved protected-data root must be an existing absolute directory.")


@dataclass(frozen=True, slots=True)
class DataArtifact:
    path: str
    sha256: str
    size_bytes: int
    records: int | None = None

    def __post_init__(self) -> None:
        logical_path(self.path)
        if not re.fullmatch(r"[0-9a-f]{64}", self.sha256):
            raise ValueError("Expected a lowercase SHA-256 digest.")
        if type(self.size_bytes) is not int:
            raise ValueError("Artifact size must be a nonnegative integer.")
        for value in (self.size_bytes, self.records):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError("Artifact size/count must be a nonnegative integer.")


@dataclass(frozen=True, slots=True)
class ProtectionManifest:
    provider: Provider
    source: DataEndpoint
    captured_at: datetime
    completed_at: datetime
    consistency: ConsistencyEvidence
    storage_region: str
    storage_approval_ref: str
    artifacts: tuple[DataArtifact, ...]
    schema_version: int = 1
    complete: bool = True


@dataclass(frozen=True, slots=True)
class ProtectionAssessment:
    state: Literal["protected", "protection_missing", "protection_stale", "deferred", "restored_stopped"]
    data_ready: bool = False
    warnings: tuple[str, ...] = ()
    target: DataIdentity | None = None
    # Data providers cannot approve workspace ACL replay, fencing or business cutover.
    ready_for_cutover: bool = False


def missing_protection(item_name: str, item_type: str) -> ProtectionAssessment:
    remedies = {
        "SQLDatabase": "Supply a pre-disaster off-region data-bearing DACPAC export. Fabric automatic "
        "backups cannot restore across workspaces or regions.",
        "CosmosDBDatabase": "Supply a pre-disaster consistent logical document export; the analytical "
        "OneLake copy is not a transactional restore input.",
        "KQLDatabase": "Supply an explicitly mapped independent regional data standby and access evidence; "
        "a cloned empty schema is not recovered data.",
    }
    remedy = remedies.get(item_type, "Configure a workload-specific protected data input.")
    return ProtectionAssessment("protection_missing", warnings=(f"{item_name}: {remedy}",))


def require_independent(source: DataEndpoint, target: DataEndpoint) -> None:
    if source.identity.tenant_id != target.identity.tenant_id:
        raise ProtectionError("This BCDR provider supports same-tenant recovery only.")
    if (
        source.identity.workspace_id == target.identity.workspace_id
        or source.identity.item_id == target.identity.item_id
        or (source.endpoint == target.endpoint and source.database.casefold() == target.database.casefold())
    ):
        raise ProtectionError(
            "Use an independently created target in another workspace, not the source database."
        )


def logical_path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value
        or "\\" in value
        or ":" in value
        or "\x00" in value
        or path.is_absolute()
        or any(part in ("", ".", "..") for part in value.split("/"))
    ):
        raise ValueError("Artifact paths must be relative, canonical paths within the approved data root.")
    return path


def artifact_path(root: Path, value: str) -> Path:
    relative = logical_path(value)
    path = root
    for part in relative.parts:
        path /= part
        if path.is_symlink():
            raise ProtectionError("Artifact paths may not traverse symbolic links.")
    if not path.resolve().is_relative_to(root.resolve()) or not stat.S_ISREG(path.stat().st_mode):
        raise ProtectionError("Artifact must be a regular file inside the approved protected-data root.")
    return path


def digest_file(path: Path, limits: ProtectionLimits, cancel: Cancel = None) -> tuple[str, int]:
    digest, size = hashlib.sha256(), 0
    with path.open("rb") as reader:
        while True:
            check_cancelled(cancel)
            chunk = reader.read(min(1024**2, limits.max_record_bytes))
            if not chunk:
                break
            size += len(chunk)
            if size > limits.max_disk_bytes:
                raise StagingBudgetError("The data artifact exceeds max_disk_bytes.")
            digest.update(chunk)
    return digest.hexdigest(), size


def validate_manifest(
    manifest: ProtectionManifest,
    *,
    source: DataIdentity,
    location: ProtectedLocation,
    max_age: timedelta,
    limits: ProtectionLimits = ProtectionLimits(),
    cancel: Cancel = None,
    now: datetime | None = None,
) -> ProtectionAssessment:
    check_cancelled(cancel)
    current = utc(now or datetime.now(UTC))
    if max_age <= timedelta(0):
        raise ValueError("max_age must be positive.")
    if manifest.schema_version != 1 or manifest.complete is not True or manifest.source.identity != source:
        raise ProtectionError(
            "Refuse incomplete, unsupported-version or wrong-source protection descriptors."
        )
    if manifest.provider not in ("sql-dacpac", "sql-bacpac", "cosmos-logical", "kql-prepared"):
        raise ProtectionError("Unsupported protection provider.")
    if (
        manifest.storage_region != location.storage_region
        or manifest.storage_approval_ref != location.approval_ref
    ):
        raise ProtectionError("The artifact location does not match its approved protection descriptor.")
    start, end = utc(manifest.captured_at), utc(manifest.completed_at)
    if not start <= end <= current:
        raise ProtectionError("Protection capture times are inconsistent or in the future.")
    manifest.consistency.require(start, cosmos=manifest.provider == "cosmos-logical")
    manifest.consistency.require(end, cosmos=manifest.provider == "cosmos-logical")
    if not 0 < len(manifest.artifacts) <= limits.max_manifest_entries:
        raise ProtectionError("Supply a complete bounded artifact inventory.")
    paths: set[str] = set()
    total = 0
    for artifact in manifest.artifacts:
        if artifact.path.casefold() in paths:
            raise ProtectionError("Duplicate artifact path in protection descriptor.")
        paths.add(artifact.path.casefold())
        total += artifact.size_bytes
        if total > limits.max_disk_bytes:
            raise StagingBudgetError("The protection input exceeds max_disk_bytes.")
        actual = digest_file(artifact_path(location.root, artifact.path), limits, cancel)
        if actual != (artifact.sha256, artifact.size_bytes):
            raise ProtectionError("Artifact checksum or size mismatch; supply the complete original export.")
    if current - start > max_age:
        return ProtectionAssessment(
            "protection_stale",
            warnings=("The data capture exceeds max_age; supply a fresher approved export.",),
        )
    return ProtectionAssessment("protected")


@contextmanager
def capture_directory(location: ProtectedLocation) -> Iterator[tuple[Path, str]]:
    """Only a successfully finished data directory becomes externally visible."""
    name = uuid4().hex
    partial = location.root / f".{name}.partial"
    partial.mkdir(mode=0o700)
    try:
        yield partial, name
        os.rename(partial, location.root / name)
    finally:
        if partial.exists():
            shutil.rmtree(partial)


@contextmanager
def staged_artifacts(
    manifest: ProtectionManifest,
    location: ProtectedLocation,
    scratch: Path,
    limits: ProtectionLimits,
    cancel: Cancel,
) -> Iterator[Path]:
    """Pin verified bytes in private staging before making any destination mutation."""
    with tempfile.TemporaryDirectory(prefix="bcdr-data-", dir=scratch) as directory:
        staging = Path(directory)
        total = 0
        for index, artifact in enumerate(manifest.artifacts):
            source = artifact_path(location.root, artifact.path)
            digest, size = hashlib.sha256(), 0
            with source.open("rb") as reader, (staging / str(index)).open("xb") as writer:
                while True:
                    check_cancelled(cancel)
                    chunk = reader.read(min(1024**2, limits.max_record_bytes))
                    if not chunk:
                        break
                    size += len(chunk)
                    total += len(chunk)
                    if total > limits.max_disk_bytes:
                        raise StagingBudgetError("Protection restore staging exceeds max_disk_bytes.")
                    writer.write(chunk)
                    digest.update(chunk)
            if (digest.hexdigest(), size) != (artifact.sha256, artifact.size_bytes):
                raise ProtectionError("Artifact changed while staging; no destination data was written.")
        yield staging
