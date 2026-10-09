"""Consistent, bounded logical document exports, never analytical Delta reconstruction.

Source writes and TTL must already be quiesced. Restore creates documents only in
precreated empty containers; no upserts, grants, container mutations or source reads.
TTL remains disabled until the coordinator has reconciled expiration semantics.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

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
    evidence_ref,
    operation_cancel,
    require_independent,
    staged_artifacts,
    validate_manifest,
)
from fabshuffle.transfer.common import StagingBudgetError, check_cancelled
from fabshuffle.transfer.cosmos import _client as cosmos_client
from fabshuffle.transfer.cosmos import strip_system_properties

_PARTITION_PATH = re.compile(r'(/[^/"\\\x00]+)+')


@dataclass(frozen=True, slots=True)
class CosmosContainer:
    name: str
    partition_paths: tuple[str, ...]
    partition_kind: str
    partition_version: int | None
    default_ttl: int | None

    def __post_init__(self) -> None:
        if (
            not self.name
            or not self.partition_paths
            or self.partition_kind not in ("Hash", "MultiHash")
            or any(not _PARTITION_PATH.fullmatch(p) for p in self.partition_paths)
        ):
            raise ValueError("Cosmos container metadata must include supported explicit partition-key paths.")
        if self.default_ttl is not None and (
            type(self.default_ttl) is not int or (self.default_ttl != -1 and self.default_ttl <= 0)
        ):
            raise ValueError("Cosmos container metadata has invalid default TTL.")


@dataclass(frozen=True, slots=True)
class CosmosProtection:
    manifest: ProtectionManifest
    containers: tuple[CosmosContainer, ...]


def _container(properties: Mapping[str, Any]) -> CosmosContainer:
    name = properties.get("id")
    key = properties.get("partitionKey", {})
    paths = key.get("paths", [])
    kind = key.get("kind")
    if (
        not isinstance(name, str)
        or not name
        or not paths
        or not isinstance(paths, list)
        or kind not in ("Hash", "MultiHash")
        or any(not isinstance(p, str) or not _PARTITION_PATH.fullmatch(p) for p in paths)
    ):
        raise ProtectionError(
            "Supply container IDs and supported explicit partition-key paths before export."
        )
    ttl = properties.get("defaultTtl")
    if ttl is not None and (type(ttl) is not int or (ttl != -1 and ttl <= 0)):
        raise ProtectionError("Container has invalid defaultTtl.")
    version = key.get("version")
    if version is not None and type(version) is not int:
        raise ProtectionError("Container partition-key version must be an integer.")
    return CosmosContainer(name, tuple(paths), kind, version, ttl)


def _inventory(database: Any, limits: ProtectionLimits, cancel: Cancel) -> tuple[CosmosContainer, ...]:
    containers: list[CosmosContainer] = []
    names: set[str] = set()
    for properties in database.list_containers():
        check_cancelled(cancel)
        container = _container(properties)
        if container.name in names:
            raise ProtectionError("Cosmos returned a duplicate container ID.")
        if len(containers) >= limits.max_manifest_entries:
            raise StagingBudgetError("Cosmos container inventory exceeds max_manifest_entries.")
        containers.append(container)
        names.add(container.name)
    return tuple(sorted(containers, key=lambda c: c.name))


def _validate_document(document: Any, container: CosmosContainer) -> None:
    if not isinstance(document, dict) or not isinstance(document.get("id"), str) or not document["id"]:
        raise ProtectionError(f"Container '{container.name}' has a document without a valid string id.")
    for path in container.partition_paths:
        value: Any = document
        for segment in path[1:].split("/"):
            if not isinstance(value, dict) or segment not in value:
                raise ProtectionError(f"Document in '{container.name}' lacks partition-key path '{path}'.")
            value = value[segment]
        if isinstance(value, (dict, list)):
            raise ProtectionError(f"Document in '{container.name}' has a non-scalar partition key.")
    ttl = document.get("ttl")
    if ttl is not None and (type(ttl) is not int or (ttl != -1 and ttl <= 0)):
        raise ProtectionError(f"Document in '{container.name}' has an invalid TTL value.")
    if container.default_ttl is not None and (ttl if ttl is not None else container.default_ttl) != -1:
        raise ProtectionError(
            f"Container '{container.name}' still has expiring data. Disable source TTL expiration "
            "under an approved maintenance procedure before repeating the capture."
        )


def _document_bytes(document: Any, container: CosmosContainer, limits: ProtectionLimits) -> bytes:
    _validate_document(document, container)
    # System properties remain in the protected export, including _ts for later TTL
    # reconciliation. Only the destination writes strip Cosmos-owned properties.
    payload = json.dumps(document, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    if len(payload) + 1 > limits.max_record_bytes:
        raise StagingBudgetError(f"Document in '{container.name}' exceeds max_record_bytes.")
    return payload + b"\n"


def capture_cosmos(
    *,
    source: DataEndpoint,
    tokens: TokenProvider,
    location: ProtectedLocation,
    consistency: ConsistencyEvidence,
    limits: ProtectionLimits = ProtectionLimits(),
    cancel: Cancel = None,
) -> CosmosProtection:
    """Publish a complete descriptor only after every container has streamed successfully."""
    cancel = operation_cancel(limits, cancel)
    started = datetime.now(UTC)
    consistency.require(started, cosmos=True)
    check_cancelled(cancel)
    with cosmos_client(source.endpoint, tokens) as client:
        database = client.get_database_client(source.database)
        containers = _inventory(database, limits, cancel)
        if not containers:
            raise ProtectionError(
                "Cosmos has no containers; capture definitions separately, not a data-ready export."
            )
        artifacts: list[DataArtifact] = []
        total = 0
        with capture_directory(location) as (staging, name):
            for index, container in enumerate(containers):
                reader = database.get_container_client(container.name)
                if _container(reader.read()) != container:
                    raise ProtectionError("Cosmos container configuration changed before capture.")
                if container.default_ttl not in (None, -1):
                    raise ProtectionError(
                        f"Disable TTL expiration on '{container.name}' before capture; "
                        "writes alone are not quiescence."
                    )
                filename = f"{index}.jsonl"
                count, size, digest = 0, 0, hashlib.sha256()
                with (staging / filename).open("xb") as writer:
                    for document in reader.query_items(
                        "SELECT * FROM c",
                        enable_cross_partition_query=True,
                        max_item_count=1,
                    ):
                        check_cancelled(cancel)
                        payload = _document_bytes(document, container, limits)
                        total += len(payload)
                        if total > limits.max_disk_bytes:
                            raise StagingBudgetError("Cosmos logical export exceeds max_disk_bytes.")
                        writer.write(payload)
                        digest.update(payload)
                        size += len(payload)
                        count += 1
                    writer.flush()
                    os.fsync(writer.fileno())
                artifacts.append(DataArtifact(f"{name}/{filename}", digest.hexdigest(), size, count))
            if _inventory(database, limits, cancel) != containers:
                raise ProtectionError(
                    "Cosmos container inventory changed during capture; quiesce DDL and retry."
                )
            completed = datetime.now(UTC)
            consistency.require(completed, cosmos=True)
            check_cancelled(cancel)
            manifest = ProtectionManifest(
                "cosmos-logical",
                source,
                started,
                completed,
                consistency,
                location.storage_region,
                location.approval_ref,
                tuple(artifacts),
            )
    return CosmosProtection(manifest, containers)


def _reject_constant(value: str) -> None:
    raise ProtectionError(f"Invalid JSON numeric constant '{value}' in Cosmos export.")


def _finite_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ProtectionError("Cosmos JSON number is outside the supported finite numeric range.")
    return number


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProtectionError("Duplicate JSON property in Cosmos data input.")
        result[key] = value
    return result


def _documents(
    path: Path, container: CosmosContainer, limits: ProtectionLimits, cancel: Cancel
) -> Iterator[dict[str, Any]]:
    with path.open("rb") as reader:
        while True:
            check_cancelled(cancel)
            line = reader.readline(limits.max_record_bytes + 1)
            if not line:
                break
            if len(line) > limits.max_record_bytes:
                raise StagingBudgetError("Cosmos document exceeds max_record_bytes.")
            if not line.endswith(b"\n"):
                raise ProtectionError("Cosmos export ends with an incomplete JSONL record.")
            document = json.loads(
                line,
                parse_constant=_reject_constant,
                parse_float=_finite_float,
                object_pairs_hook=_unique_object,
            )
            _validate_document(document, container)
            yield document


def restore_cosmos(
    protection: CosmosProtection,
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
    """Import logical documents, leaving TTL/security and failback reconciliation deferred."""
    cancel = operation_cancel(limits, cancel)
    evidence_ref(target_approval_ref)
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
    if manifest.provider != "cosmos-logical" or len(manifest.artifacts) != len(protection.containers):
        raise ProtectionError("Cosmos restore requires one data artifact per captured container.")
    if len({c.name for c in protection.containers}) != len(protection.containers):
        raise ProtectionError("Duplicate Cosmos container descriptor.")
    with staged_artifacts(manifest, location, scratch, limits, cancel) as staging:
        # Validate ALL records before contacting the destination, not as writes happen.
        for index, (container, artifact) in enumerate(
            zip(protection.containers, manifest.artifacts, strict=True)
        ):
            if not artifact.path.endswith(".jsonl") or artifact.records is None:
                raise ProtectionError("Cosmos requires JSONL artifacts with explicit document counts.")
            count = sum(1 for _ in _documents(staging / str(index), container, limits, cancel))
            if count != artifact.records:
                raise ProtectionError("Cosmos document count differs from the complete export descriptor.")
        with cosmos_client(target.endpoint, tokens) as client:
            database = client.get_database_client(target.database)
            actual = _inventory(database, limits, cancel)
            if {c.name for c in actual} != {c.name for c in protection.containers}:
                raise ProtectionError("Precreate exactly the captured Cosmos containers in the target.")
            for expected, found in zip(protection.containers, actual, strict=True):
                if (
                    expected.name,
                    expected.partition_paths,
                    expected.partition_kind,
                    expected.partition_version,
                ) != (
                    found.name,
                    found.partition_paths,
                    found.partition_kind,
                    found.partition_version,
                ):
                    raise ProtectionError(
                        "Target Cosmos container partition-key identity differs from the capture."
                    )
                if found.default_ttl is not None:
                    raise ProtectionError(
                        f"Turn TTL OFF on target container '{found.name}' before restore; "
                        "-1 still enables item TTL."
                    )
                reader = database.get_container_client(found.name)
                if (
                    next(
                        iter(
                            reader.query_items(
                                "SELECT TOP 1 c.id FROM c",
                                enable_cross_partition_query=True,
                                max_item_count=1,
                            )
                        ),
                        None,
                    )
                    is not None
                ):
                    raise ProtectionError(
                        f"Target container '{found.name}' is not empty. Use a clean standby; "
                        "blind upserts cannot reconcile deletions or failback."
                    )
            for index, container in enumerate(protection.containers):
                writer = database.get_container_client(container.name)
                for document in _documents(staging / str(index), container, limits, cancel):
                    # create_item fails on duplicate IDs within a partition rather than silently
                    # merging snapshots or making a partially restored target appear current.
                    writer.create_item(strip_system_properties(document))
            check_cancelled(cancel)
    return ProtectionAssessment(
        "restored_stopped",
        data_ready=True,
        target=target.identity,
        warnings=(
            "Keep Cosmos writers stopped and TTL OFF. Reconcile original _ts/TTL deadlines "
            "before Enable recovery; "
            "restored system timestamps are new and enabling TTL blindly can extend retention.",
            "Before failback, reconcile DR writes, deletes and TTL expiration explicitly; "
            "this provider never "
            "merges databases with blind upserts or copies ACLs.",
        ),
    )
