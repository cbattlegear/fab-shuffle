"""Independent byte recovery from explicitly qualified, pinned global OneLake data.

This is not a Delta snapshot engine. Existing Delta preflight checks structural
continuity/reference safety; it does not prove an application's active-file set or
transactional consistency. Only the approved global storage plane can be read.
https://learn.microsoft.com/fabric/onelake/onelake-disaster-recovery
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal, Self
from urllib.parse import quote

import httpx
from pydantic import AfterValidator, AwareDatetime, Field, model_validator

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.contracts import Digest, LogicalPath, Record, logical_path
from fabshuffle.bcdr.protection import (
    Cancel,
    ConsistencyEvidence,
    DataIdentity,
    ProtectionError,
    ProtectionLimits,
    evidence_ref,
    operation_cancel,
)
from fabshuffle.transfer import delta, files
from fabshuffle.transfer.common import StagingBudgetError, check_cancelled

GLOBAL = "https://onelake.dfs.fabric.microsoft.com"
EvidenceRef = Annotated[str, AfterValidator(evidence_ref)]


def _data_path(value: str) -> str:
    logical_path(value)
    if value.split("/", 1)[0] not in ("Tables", "Files"):
        raise ValueError("Pinned OneLake data must be within Tables/ or Files/.")
    return value


DataPath = Annotated[LogicalPath, AfterValidator(_data_path)]


class OneLakeFile(Record):
    path: DataPath
    size_bytes: Annotated[int, Field(strict=True, ge=0)]
    sha256: Digest
    etag: Annotated[str, Field(pattern=r'^"[^"\r\n]{1,200}"$')]

    @model_validator(mode="after")
    def file_below_root(self) -> Self:
        if "/" not in self.path:
            raise ValueError("Managed OneLake roots cannot be file entries.")
        return self


class LakehouseProtection(Record):
    source: DataIdentity
    captured_at: AwareDatetime
    completed_at: AwareDatetime
    consistency: ConsistencyEvidence
    storage_read_approval_ref: EvidenceRef
    snapshot_qualification_ref: EvidenceRef
    source_region: Annotated[str, Field(pattern=r"^[a-z0-9-]{1,64}$")]
    recovery_region: Annotated[str, Field(pattern=r"^[a-z0-9-]{1,64}$")]
    files: tuple[OneLakeFile, ...]
    directories: tuple[DataPath, ...]
    source_paths_verified_local: Literal[True]
    input_kind: Literal["qualified_onelake_snapshot"] = "qualified_onelake_snapshot"

    @model_validator(mode="after")
    def exact_inventory(self) -> Self:
        if self.source_region == self.recovery_region:
            raise ValueError("Independent Lakehouse recovery requires a different approved recovery region.")
        if not self.captured_at <= self.completed_at:
            raise ValueError("Lakehouse capture timestamps are inconsistent.")
        self.consistency.require(self.captured_at)
        self.consistency.require(self.completed_at)
        paths = [value.path for value in self.files]
        if len(paths) != len(set(paths)) or len(self.directories) != len(set(self.directories)):
            raise ValueError("Pinned OneLake inventory contains duplicate paths.")
        if set(paths) & set(self.directories) or not {"Tables", "Files"} <= set(self.directories):
            raise ValueError("Pin distinct file/directory paths and both managed Lakehouse roots.")
        for path in (*paths, *self.directories):
            for parent in PurePosixPath(path).parents:
                if str(parent) != "." and str(parent) not in self.directories:
                    raise ValueError("Pin every parent directory of the OneLake file inventory.")
        return self


class LakehouseCopyReceipt(Record):
    completed_at: AwareDatetime
    copied_files: Annotated[int, Field(ge=0)]
    copied_bytes: Annotated[int, Field(ge=0)]
    byte_copy_complete: Literal[True] = True
    data_ready: Literal[False] = False
    endpoint_ready: Literal[False] = False
    warnings: tuple[str, ...]


def validate_lakehouse(
    protection: LakehouseProtection,
    *,
    source: DataIdentity,
    max_age: timedelta,
    limits: ProtectionLimits,
    now: datetime | None = None,
) -> None:
    current = now or datetime.now(UTC)
    if protection.source != source:
        raise ProtectionError("Lakehouse protection input belongs to a different captured source.")
    if max_age <= timedelta(0) or protection.completed_at > current:
        raise ProtectionError("Lakehouse recovery point or max_age is invalid.")
    if current - protection.captured_at > max_age:
        raise ProtectionError(
            "Pinned Lakehouse snapshot exceeds max_age; provide a fresher qualified data input."
        )
    if len(protection.files) + len(protection.directories) > limits.max_manifest_entries:
        raise StagingBudgetError("Pinned OneLake inventory exceeds max_manifest_entries.")
    retained = len(protection.model_dump_json().encode("utf-8"))
    if retained > limits.max_record_bytes:
        raise StagingBudgetError("Pinned OneLake inventory exceeds max_record_bytes.")


def _root(identity: DataIdentity) -> str:
    return f"{GLOBAL}/{identity.workspace_id}/{identity.item_id}"


def _inventory(
    client: httpx.Client,
    identity: DataIdentity,
    tokens: TokenProvider,
    limits: ProtectionLimits,
    cancel: Cancel,
) -> tuple[dict[str, dict], set[str]]:
    observed: dict[str, dict] = {}
    directories = {"Tables", "Files"}
    pending = ["Tables", "Files"]
    filesystem = f"{GLOBAL}/{identity.workspace_id}"
    while pending:
        directory = pending.pop()
        for entry in files._listed_paths(
            client, filesystem, f"{identity.item_id}/{directory}", tokens, cancel
        ):
            local = str(entry["name"]).removeprefix(f"{identity.item_id}/")
            _data_path(local)
            if local in observed or local in directories:
                raise ProtectionError("OneLake returned duplicate or cyclic inventory entries.")
            if str(entry.get("isDirectory", False)).lower() == "true":
                directories.add(local)
                pending.append(local)
            else:
                observed[local] = entry
            if len(observed) + len(directories) > limits.max_manifest_entries:
                raise StagingBudgetError("OneLake inventory exceeds max_manifest_entries.")
    return observed, directories


def _hash_file(
    client: httpx.Client,
    identity: DataIdentity,
    entry: dict,
    tokens: TokenProvider,
    limits: ProtectionLimits,
    cancel: Cancel,
) -> tuple[str, int, str]:
    url = f"{GLOBAL}/{identity.workspace_id}/{quote(str(entry['name']), safe='/')}"
    size, etag = files._file_properties(client, url, entry, tokens)
    digest = hashlib.sha256()
    for chunk in files._read_chunks(
        client,
        url,
        tokens,
        size,
        etag,
        max(1, min(4 * 1024**2, limits.max_record_bytes // 2)),
        cancel,
    ):
        digest.update(chunk)
    return digest.hexdigest(), size, etag


class _PinnedClient(httpx.Client):
    """Scope every request, including those made inside shared Delta/copy helpers."""

    def __init__(
        self,
        protection: LakehouseProtection,
        target: DataIdentity,
        limits: ProtectionLimits,
        cancel: Cancel,
        **kwargs,
    ):
        super().__init__(timeout=120, follow_redirects=False, **kwargs)
        self.protection, self.target, self.limits, self.cancel = protection, target, limits, cancel
        self.pinned = {value.path: value for value in protection.files}
        self.owned_files: set[str] = set()
        self.owned_directories: set[str] = set()
        self.writes_allowed = False

    def send(self, request: httpx.Request, *, stream=False, **kwargs) -> httpx.Response:
        check_cancelled(self.cancel)
        url = request.url
        if (
            url.scheme != "https"
            or url.username
            or url.password
            or url.fragment
            or url.host != "onelake.dfs.fabric.microsoft.com"
            or url.port not in (None, 443)
        ):
            raise ProtectionError("Lakehouse recovery can only use the approved global OneLake endpoint.")
        segments = url.path.strip("/").split("/")
        source_side = segments[0] == self.protection.source.workspace_id
        identity = self.protection.source if source_side else self.target
        if segments[0] != identity.workspace_id:
            raise ProtectionError("Lakehouse transport tried to access an unapproved workspace.")
        listing = len(segments) == 1 and request.method == "GET"
        full = url.params.get("directory", "") if listing else "/".join(segments[1:])
        if not full.startswith(identity.item_id + "/"):
            raise ProtectionError("Lakehouse transport tried to access an unapproved item.")
        local = full[len(identity.item_id) + 1 :]
        if local not in self.pinned and local not in self.protection.directories:
            raise ProtectionError("OneLake returned a path outside the complete pinned snapshot.")
        if listing:
            if (
                local not in self.protection.directories
                or url.params.get("resource") != "filesystem"
                or url.params.get("recursive") != "false"
                or set(url.params) - {"resource", "directory", "recursive", "maxResults", "continuation"}
            ):
                raise ProtectionError("Only bounded pinned OneLake directory listings are allowed.")
        elif request.method in ("GET", "HEAD") and url.params:
            raise ProtectionError(
                "OneLake file reads may not override the approved transport with query parameters."
            )
        if source_side:
            if request.method not in ("GET", "HEAD"):
                raise ProtectionError("Source OneLake data mutations are forbidden.")
            if local in self.pinned and not listing:
                request.headers["If-Match"] = self.pinned[local].etag
        elif request.method not in ("GET", "HEAD"):
            if not self.writes_allowed:
                raise ProtectionError("Complete every source/target preflight before writing OneLake data.")
            if request.method == "PUT":
                if local in ("Tables", "Files"):
                    raise ProtectionError("Do not replace managed Lakehouse root directories.")
                if str(PurePosixPath(local).parent) not in self.owned_directories:
                    raise ProtectionError(
                        "Create OneLake data only beneath directories owned by this operation."
                    )
                directory = request.url.params.get("resource") == "directory"
                if set(url.params) != {"resource"} or url.params["resource"] not in ("file", "directory"):
                    raise ProtectionError("Only conditional OneLake file/directory creates are allowed.")
                if directory != (local in self.protection.directories):
                    raise ProtectionError("OneLake target resource type differs from the pinned inventory.")
                request.headers["If-None-Match"] = "*"
            elif request.method != "PATCH" or local not in self.owned_files:
                raise ProtectionError("Append/flush only files created by this exact recovery operation.")
            elif url.params.get("action") not in ("append", "flush") or set(url.params) - {
                "action",
                "position",
                "close",
            }:
                raise ProtectionError("Only appending/flushing created OneLake data is permitted.")
        kwargs["follow_redirects"] = False
        response = super().send(request, stream=True, **kwargs)
        retained = False
        try:
            rejected = response.headers.get("x-ms-rejected-headers", "").lower()
            if "if-match" in rejected or "if-none-match" in rejected:
                raise ProtectionError(
                    "OneLake did not honor required conditional integrity/no-overwrite headers."
                )
            if (
                source_side
                and local in self.pinned
                and response.is_success
                and response.headers.get("etag") != self.pinned[local].etag
            ):
                raise ProtectionError("Pinned OneLake source file version changed during recovery.")
            if not source_side and request.method == "PUT" and response.is_success:
                if request.url.params.get("resource") == "file":
                    self.owned_files.add(local)
                else:
                    self.owned_directories.add(local)
            # Listings/errors/control responses must not bypass the memory budget.
            if not stream or not response.is_success:
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=min(65536, self.limits.max_record_bytes)):
                    if len(content) + len(chunk) > self.limits.max_record_bytes:
                        raise StagingBudgetError("OneLake response exceeds max_record_bytes.")
                    content.extend(chunk)
                headers = dict(response.headers)
                response.close()
                return httpx.Response(
                    response.status_code, headers=headers, content=bytes(content), request=request
                )
            retained = True
            return response
        finally:
            if not retained:
                response.close()


def capture_lakehouse(
    *,
    source: DataIdentity,
    tokens: TokenProvider,
    consistency: ConsistencyEvidence,
    storage_read_approval_ref: str,
    snapshot_qualification_ref: str,
    source_region: str,
    recovery_region: str,
    source_paths_verified_local: Literal[True],
    scratch: Path,
    limits: ProtectionLimits = ProtectionLimits(),
    cancel: Cancel = None,
) -> LakehouseProtection:
    """Capture storage pins while healthy; caller must qualify native/local snapshot provenance."""
    started = datetime.now(UTC)
    consistency.require(started)
    evidence_ref(storage_read_approval_ref)
    evidence_ref(snapshot_qualification_ref)
    cancel = operation_cancel(limits, cancel)
    check_cancelled(cancel)
    pinned: list[OneLakeFile] = []
    with httpx.Client(timeout=120, follow_redirects=False) as client:
        observed, directories = _inventory(client, source, tokens, limits, cancel)
        for path, entry in sorted(observed.items()):
            checksum, size, etag = _hash_file(client, source, entry, tokens, limits, cancel)
            pinned.append(OneLakeFile(path=path, sha256=checksum, size_bytes=size, etag=etag))
        for root in ("Tables", "Files"):
            delta.preflight(
                client,
                f"{GLOBAL}/{source.workspace_id}",
                f"{source.item_id}/{root}",
                tokens,
                max_memory_bytes=limits.max_record_bytes,
                max_disk_staging_bytes=limits.max_disk_bytes,
                scratch_dir=scratch,
                is_excluded=lambda _: False,
                on_progress=None,
                cancel_requested=cancel,
                strict=root == "Tables",
            )
        final, final_dirs = _inventory(client, source, tokens, limits, cancel)
        if set(final) != set(observed) or final_dirs != directories:
            raise ProtectionError(
                "OneLake inventory changed during capture; quiesce source writes and retry."
            )
        for value in pinned:
            entry = dict(final[value.path], etag=value.etag, _validated_size=value.size_bytes)
            files._file_properties(
                client,
                f"{_root(source)}/{quote(value.path, safe='/')}",
                entry,
                tokens,
            )
    completed = datetime.now(UTC)
    consistency.require(completed)
    result = LakehouseProtection(
        source=source,
        captured_at=started,
        completed_at=completed,
        consistency=consistency,
        storage_read_approval_ref=storage_read_approval_ref,
        snapshot_qualification_ref=snapshot_qualification_ref,
        source_region=source_region,
        recovery_region=recovery_region,
        files=tuple(pinned),
        directories=tuple(sorted(directories)),
        source_paths_verified_local=source_paths_verified_local,
    )
    validate_lakehouse(result, source=source, max_age=timedelta(days=1), limits=limits)
    return result


def restore_lakehouse(
    protection: LakehouseProtection,
    *,
    source: DataIdentity,
    target: DataIdentity,
    tokens: TokenProvider,
    scratch: Path,
    max_age: timedelta,
    target_approval_ref: str,
    limits: ProtectionLimits = ProtectionLimits(),
    cancel: Cancel = None,
) -> LakehouseCopyReceipt:
    """Copy exact verified bytes into a fresh destination, never assert engine readiness."""
    evidence_ref(target_approval_ref)
    validate_lakehouse(protection, source=source, max_age=max_age, limits=limits)
    if (
        target.tenant_id != source.tenant_id
        or target.workspace_id == source.workspace_id
        or target.item_id == source.item_id
    ):
        raise ProtectionError(
            "Independent Lakehouse recovery requires a distinct same-tenant target workspace/item."
        )
    cancel = operation_cancel(limits, cancel)
    check_cancelled(cancel)
    with _PinnedClient(protection, target, limits, cancel) as client:
        source_files, source_dirs = _inventory(client, source, tokens, limits, cancel)
        if set(source_files) != {f.path for f in protection.files} or source_dirs != set(
            protection.directories
        ):
            raise ProtectionError("OneLake source inventory no longer matches the complete pinned snapshot.")
        for expected in protection.files:
            observed = _hash_file(client, source, source_files[expected.path], tokens, limits, cancel)
            if observed != (expected.sha256, expected.size_bytes, expected.etag):
                raise ProtectionError(
                    "Pinned OneLake source checksum/size/ETag does not match; no data was written."
                )
        target_files, target_dirs = _inventory(client, target, tokens, limits, cancel)
        if target_files or target_dirs != {"Tables", "Files"}:
            raise ProtectionError(
                "Use a fresh owned Lakehouse with empty Tables/Files roots; remove precreated user/schema "
                "directories under an approved destination-only preparation before copying."
            )
        client.owned_directories = target_dirs
        for root in ("Tables", "Files"):
            delta.preflight(
                client,
                f"{GLOBAL}/{source.workspace_id}",
                f"{source.item_id}/{root}",
                tokens,
                max_memory_bytes=limits.max_record_bytes,
                max_disk_staging_bytes=limits.max_disk_bytes,
                scratch_dir=scratch,
                is_excluded=lambda _: False,
                on_progress=None,
                cancel_requested=cancel,
                strict=root == "Tables",
            )
        client.writes_allowed = True
        for root in ("Tables", "Files"):
            files.copy_tree_streaming(
                source_path=f"{_root(source)}/{root}",
                target_path=f"{_root(target)}/{root}",
                tokens=tokens,
                target_tokens=tokens,
                max_memory_bytes=limits.max_record_bytes,
                max_disk_staging_bytes=limits.max_disk_bytes,
                scratch_dir=scratch,
                kind="lakehouse" if root == "Tables" else "files",
                cancel_requested=cancel,
                transport_client=client,
            )
        restored, restored_dirs = _inventory(client, target, tokens, limits, cancel)
        if set(restored) != set(source_files) or restored_dirs != source_dirs:
            raise ProtectionError("Destination file inventory differs after copying; keep recovery disabled.")
        for expected in protection.files:
            checksum, size, _etag = _hash_file(
                client, target, restored[expected.path], tokens, limits, cancel
            )
            if (checksum, size) != (expected.sha256, expected.size_bytes):
                raise ProtectionError(
                    "Destination OneLake checksum/size differs; keep the partial target unready."
                )
        final_files, final_dirs = _inventory(client, source, tokens, limits, cancel)
        if set(final_files) != set(source_files) or final_dirs != source_dirs:
            raise ProtectionError(
                "Source inventory changed during copy; do not claim a stable recovery input."
            )
        check_cancelled(cancel)
    return LakehouseCopyReceipt(
        completed_at=datetime.now(UTC),
        copied_files=len(protection.files),
        copied_bytes=sum(value.size_bytes for value in protection.files),
        warnings=(
            "Pinned Lakehouse bytes copied and destination hashes match. Delta preflight checks references "
            "and checkpoint continuity only, not the complete active-file set or application consistency.",
            "Keep recovery writers stopped. Validate the destination Delta engine, SQL endpoint "
            "and application "
            "queries with fresh identity-bound evidence before declaring this Lakehouse data ready.",
            "Approved global OneLake reads depend on Microsoft-managed failover and retained source storage; "
            "no RPO, native snapshot or source log repair is implied.",
        ),
    )
