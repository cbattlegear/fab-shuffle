"""Bounded, read-only Delta reference inspection; never rewrite transaction history.

Protocol: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
V2 pointer fields: spark/src/main/scala/org/apache/spark/sql/delta/LastCheckpointInfo.scala
"""

from __future__ import annotations

import json
import re
import tempfile
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from fabshuffle.auth import TokenProvider
from fabshuffle.transfer.common import StagingBudgetError, check_cancelled
from fabshuffle.transfer.files import DeltaValidationRequired, _file_properties, _listed_paths, _read_chunks

FEATURES = {
    "appendOnly",
    "invariants",
    "checkConstraints",
    "generatedColumns",
    "identityColumns",
    "changeDataFeed",
    "columnMapping",
    "deletionVectors",
    "timestampNtz",
    "v2Checkpoint",
    "rowTracking",
    "domainMetadata",
    "clustering",
    "inCommitTimestamp",
    "vacuumProtocolCheck",
    "typeWidening",
    "typeWidening-preview",
    "variantType",
    "variantType-preview",
}
FILE_FIELDS = {
    "path",
    "partitionValues",
    "size",
    "modificationTime",
    "dataChange",
    "stats",
    "tags",
    "deletionVector",
    "baseRowId",
    "defaultRowCommitVersion",
    "clusteringProvider",
    "deletionTimestamp",
    "extendedFileMetadata",
    "stats_parsed",
    "partitionValues_parsed",
}
ACTION_FIELDS = {
    "add": FILE_FIELDS,
    "remove": FILE_FIELDS,
    "cdc": {"path", "partitionValues", "size", "dataChange", "tags"},
    "sidecar": {"path", "sizeInBytes", "modificationTime", "tags"},
    "protocol": {"minReaderVersion", "minWriterVersion", "readerFeatures", "writerFeatures"},
    "metaData": {
        "id",
        "name",
        "description",
        "format",
        "schemaString",
        "partitionColumns",
        "createdTime",
        "configuration",
    },
    "txn": {"appId", "version", "lastUpdated"},
    "domainMetadata": {"domain", "configuration", "removed"},
    "checkpointMetadata": {"version", "tags"},
}
PROJECTED = [
    "add.path",
    "add.deletionVector",
    "remove.path",
    "remove.deletionVector",
    "cdc.path",
    "sidecar",
    "protocol",
    "metaData",
    "domainMetadata",
    "checkpointMetadata",
    "txn",
]
Z85 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"


def _fail(where: str, reason: str) -> None:
    raise DeltaValidationRequired(
        f"Delta preflight refused '{where}': {reason}. Materialize a self-contained Delta snapshot "
        "with supported relative references, or migrate this item manually. "
        "No destination writes were attempted."
    )


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _mapping(value: Any, where: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        try:
            return _unique(value)
        except (ValueError, TypeError):
            pass
    _fail(where, "unrecognized metadata map")


def _fields(value: Any, allowed: set[str], where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(where, "expected a structured Delta action")
    unknown = set(value) - allowed
    if unknown:
        _fail(where, f"unsupported action fields {', '.join(sorted(unknown))}")
    return value


def relative_path(value: Any, where: str) -> str:
    """Reject schemes, rooted paths, traversal and ambiguous nested URI encoding."""
    if not isinstance(value, str) or not value:
        _fail(where, "missing file path")
    if re.search(r"%(?![0-9a-fA-F]{2})", value):
        _fail(where, "invalid URI percent encoding")
    decoded = value
    first = unquote(value, errors="strict")
    for _ in range(8):
        if (
            urlsplit(decoded).scheme
            or decoded.startswith(("/", "\\"))
            or "\\" in decoded
            or any(ord(char) < 32 for char in decoded)
            or any(part in {"..", ""} for part in decoded.split("/"))
        ):
            _fail(where, "absolute/external or parent-escape file reference")
        next_value = unquote(decoded, errors="strict")
        if next_value == decoded:
            break
        decoded = next_value
    else:
        _fail(where, "ambiguous repeatedly URI-encoded path")
    if urlsplit(value).query or urlsplit(value).fragment:
        _fail(where, "file reference contains a query or fragment")
    return first


@dataclass
class Snapshot:
    budget: int
    # Tables/ roots hold nothing but managed tables, so any file outside a discovered one is
    # unexpected. Files/ roots legitimately mix ordinary content with zero or more unmanaged
    # Delta tables (for example a notebook's ``df.write.format("delta").save("Files/...")``),
    # so only files actually inside a discovered table's ``_delta_log`` are validated there.
    strict: bool = True
    files: dict[str, tuple[str, int]] = field(default_factory=dict)
    tables: dict[str, set[str]] = field(default_factory=dict)
    pointers: set[str] = field(default_factory=set)
    checkpoints: set[tuple[str, int, int]] = field(default_factory=set)
    retained_bytes: int = 0

    def remember_pointer(self, name: str) -> None:
        if name not in self.pointers:
            self.retained_bytes += len(name.encode("utf-8")) + 128
            if self.retained_bytes > self.budget:
                raise StagingBudgetError(
                    f"Delta reference inventory exceeds {self.budget} bytes; increase the budget."
                )
            self.pointers.add(name)

    def remember_checkpoint(self, log: str, version: int, parts: int) -> None:
        descriptor = (log, version, parts)
        if descriptor not in self.checkpoints:
            self.retained_bytes += len(log.encode("utf-8")) + 128
            if self.retained_bytes > self.budget:
                raise StagingBudgetError(f"Delta checkpoint inventory exceeds {self.budget} bytes.")
            self.checkpoints.add(descriptor)

    def pin(self, entry: dict[str, Any]) -> dict[str, Any]:
        name = str(entry["name"])
        if not any(name.startswith(table + "/") for table in self.tables):
            if not self.strict:
                # An ordinary Files/ entry that is not part of any discovered Delta table;
                # nothing here was inspected, and nothing here needs to be.
                return entry
            raise DeltaValidationRequired(
                f"'{name}' is outside a validated Delta table; migrate it manually."
            )
        if "/_delta_log/" not in name:
            return entry
        if name not in self.files:
            raise DeltaValidationRequired(
                f"Uninspected Delta metadata '{name}' appeared after preflight; keep writes frozen and retry."
            )
        etag, size = self.files[name]
        listed = entry.get("etag")
        if listed and str(listed).strip('"') != etag.strip('"'):
            raise DeltaValidationRequired(f"Delta metadata '{name}' changed after preflight; retry frozen.")
        return {**entry, "etag": etag, "_validated_size": size}


class Inspector:
    def __init__(self, snapshot: Snapshot, table: str, where: str, excluded: Callable[[str], bool]):
        self.snapshot, self.table, self.where, self.excluded = snapshot, table, where, excluded

    def path(self, value: Any, *, sidecar: bool = False, checkpoint: bool = False) -> None:
        path = relative_path(value, self.where)
        if sidecar:
            for prefix in ("_delta_log/_sidecars/", "_sidecars/"):
                if path.startswith(prefix):
                    path = path[len(prefix) :]
                    break
            if "/" in path or not path.endswith(".parquet"):
                _fail(self.where, "sidecar must be a Parquet file in this table's _delta_log/_sidecars")
            name = f"{self.table}/_delta_log/_sidecars/{path}"
        elif checkpoint:
            if "/" in path or ".checkpoint." not in path:
                _fail(self.where, "unsupported V2 checkpoint pointer")
            name = f"{self.table}/_delta_log/{path}"
        else:
            name = f"{self.table}/{path}"
        if self.excluded(name):
            _fail(self.where, "a file reference depends on an excluded shortcut/path")
        if sidecar or checkpoint:
            self.snapshot.remember_pointer(name)

    def vector(self, value: Any) -> None:
        value = _fields(
            value, {"storageType", "pathOrInlineDv", "offset", "sizeInBytes", "cardinality"}, self.where
        )
        storage, path = value.get("storageType"), value.get("pathOrInlineDv")
        if not isinstance(path, str) or not path:
            _fail(self.where, "malformed deletion vector descriptor")
        if storage == "i":
            if len(path) % 5 or any(char not in Z85 for char in path):
                _fail(self.where, "malformed inline deletion vector encoding")
        elif storage == "u":
            if len(path) < 20 or any(char not in Z85 for char in path[-20:]):
                _fail(self.where, "malformed UUID-relative deletion vector")
            # Z85 UUID characters include ':' and '/'; only the preceding prefix is a path.
            prefix = path[:-20]
            raw = bytearray()
            for index in range(0, 20, 5):
                number = 0
                for character in path[-20:][index : index + 5]:
                    number = number * 85 + Z85.index(character)
                if number > 0xFFFFFFFF:
                    _fail(self.where, "invalid UUID-relative deletion vector encoding")
                raw.extend(number.to_bytes(4, "big"))
            filename = f"deletion_vector_{uuid.UUID(bytes=bytes(raw))}.bin"
            self.path((prefix.rstrip("/") + "/" if prefix else "") + filename)
        else:
            _fail(self.where, f"absolute or unsupported deletion-vector storage type {storage!r}")

    def action(self, row: Any) -> None:
        if not isinstance(row, dict) or not any(value is not None for value in row.values()):
            _fail(self.where, "empty or malformed Delta action")
        for name, value in row.items():
            if value is None:
                continue
            if name == "commitInfo":
                continue  # Provenance is arbitrary application data, not a file-location instruction.
            if name not in ACTION_FIELDS:
                _fail(self.where, f"unsupported Delta action {name!r}")
            value = _fields(value, ACTION_FIELDS[name], self.where)
            if name in {"add", "remove", "cdc", "sidecar"}:
                self.path(value.get("path"), sidecar=name == "sidecar")
                if value.get("deletionVector") is not None:
                    self.vector(value["deletionVector"])
            elif name == "protocol":
                reader, writer = value.get("minReaderVersion"), value.get("minWriterVersion")
                if (
                    type(reader) is not int
                    or type(writer) is not int
                    or not 1 <= reader <= 3
                    or not 1 <= writer <= 7
                ):
                    _fail(self.where, "unsupported Delta protocol version")
                for field_name in ("readerFeatures", "writerFeatures"):
                    features = value.get(field_name)
                    features = [] if features is None else features
                    if not isinstance(features, list) or any(feature not in FEATURES for feature in features):
                        _fail(
                            self.where,
                            f"unsupported {field_name}; catalog/unknown path features need manual migration",
                        )
                self.snapshot.tables[self.table].add("protocol")
            elif name == "metaData":
                fmt = _fields(value.get("format"), {"provider", "options"}, self.where)
                if fmt.get("provider") != "parquet" or _mapping(fmt.get("options"), self.where):
                    _fail(self.where, "only ordinary Parquet Delta format/options have been inspected")
                for key, setting in _mapping(value.get("configuration"), self.where).items():
                    if key.startswith(
                        ("delta.coordinatedCommits.", "delta.catalog", "delta.universalFormat.")
                    ) and setting not in (None, "", "false"):
                        _fail(self.where, f"unsupported catalog/external metadata configuration {key}")
                    if (
                        key.startswith("delta.feature.")
                        and key.removeprefix("delta.feature.") not in FEATURES
                    ):
                        _fail(self.where, f"unsupported table feature configuration {key}")
                self.snapshot.tables[self.table].add("metaData")
            elif name == "domainMetadata":
                if value.get("domain") not in {"delta.rowTracking", "delta.clustering"}:
                    _fail(self.where, "unknown metadata domain cannot be certified as path-independent")
                config = json.loads(value.get("configuration", ""), object_pairs_hook=_unique)
                allowed = (
                    {"rowIdHighWaterMark"}
                    if value["domain"] == "delta.rowTracking"
                    else {"clusteringColumns"}
                )
                _fields(config, allowed, self.where)

    def last_checkpoint(self, value: Any) -> None:
        value = _fields(
            value,
            {
                "version",
                "size",
                "parts",
                "sizeInBytes",
                "numOfAddFiles",
                "checkpointSchema",
                "tags",
                "checksum",
                "v2Checkpoint",
                "checkpointType",
                "amtCheckpoint",
            },
            self.where,
        )
        if value.get("checkpointType") is not None or value.get("amtCheckpoint") is not None:
            _fail(self.where, "adaptive/unknown checkpoint types need manual migration")
        version, parts = value.get("version"), value.get("parts")
        parts = 1 if parts is None else parts
        if type(version) is not int or version < 0 or type(parts) is not int or parts < 1:
            _fail(self.where, "invalid last-checkpoint version/part count")
        v2 = value.get("v2Checkpoint")
        if v2 is not None:
            v2 = _fields(
                v2, {"path", "sizeInBytes", "modificationTime", "nonFileActions", "sidecarFiles"}, self.where
            )
            self.path(v2.get("path"), checkpoint=True)
            for action in v2.get("nonFileActions") or []:
                self.action(action)
            for sidecar in v2.get("sidecarFiles") or []:
                self.action({"sidecar": sidecar})
        else:
            # Record an O(1) descriptor here; validate parts after the bounded inventory is complete.
            self.snapshot.remember_checkpoint(f"{self.table}/_delta_log", version, parts)

    def checksum(self, value: Any) -> None:
        value = _fields(
            value,
            {
                "txnId",
                "tableSizeBytes",
                "numFiles",
                "numMetadata",
                "numProtocol",
                "inCommitTimestampOpt",
                "setTransactions",
                "domainMetadata",
                "metadata",
                "protocol",
                "fileSizeHistogram",
                "allFiles",
                "numDeletedRecordsOpt",
                "numDeletionVectorsOpt",
                "deletedRecordCountsHistogramOpt",
            },
            self.where,
        )
        for name, field_name in (("metaData", "metadata"), ("protocol", "protocol")):
            if value.get(field_name) is not None:
                self.action({name: value[field_name]})
        for action in value.get("allFiles") or []:
            self.action({"add": action})
        for action in value.get("domainMetadata") or []:
            self.action({"domainMetadata": action})


def _json_records(chunks: Iterator[bytes], budget: int, where: str) -> Iterator[Any]:
    pending = bytearray()
    for chunk in chunks:
        while chunk:
            prefix, separator, chunk = chunk.partition(b"\n")
            if len(pending) + len(prefix) > budget:
                raise StagingBudgetError(
                    f"Delta JSON record in '{where}' exceeds {budget} bytes; raise the budget."
                )
            pending.extend(prefix)
            if separator:
                if pending.strip():
                    yield json.loads(pending, object_pairs_hook=_unique)
                pending.clear()
    if pending.strip():
        yield json.loads(pending, object_pairs_hook=_unique)


def _parquet(path: Path, inspector: Inspector, budget: int, cancelled: Callable[[], bool] | None) -> None:
    # Own the file handle even if Arrow's constructor fails on a corrupt footer.
    with path.open("rb") as input_file, pq.ParquetFile(
        input_file,
        memory_map=False,
        pre_buffer=False,
        thrift_string_size_limit=budget,
        thrift_container_size_limit=budget,
    ) as parquet:
        for field_info in parquet.schema_arrow:
            if field_info.name not in ACTION_FIELDS or not pa.types.is_struct(field_info.type):
                _fail(inspector.where, f"unsupported checkpoint action column {field_info.name!r}")
            _fields(
                dict.fromkeys(child.name for child in field_info.type),
                ACTION_FIELDS[field_info.name],
                inspector.where,
            )
        selected = [name for name in PROJECTED if name.split(".")[0] in parquet.schema_arrow.names]
        for index in range(parquet.metadata.num_row_groups):
            check_cancelled(cancelled)
            group = parquet.metadata.row_group(index)
            uncompressed = 0
            for column_index in range(group.num_columns):
                column = group.column(column_index)
                if column.file_path:
                    _fail(inspector.where, "checkpoint has external Parquet column-chunk references")
                if column.total_uncompressed_size < 0:
                    _fail(inspector.where, "checkpoint column has an unknown decompressed size")
                if any(
                    column.path_in_schema == name or column.path_in_schema.startswith(name + ".")
                    for name in selected
                ):
                    uncompressed += column.total_uncompressed_size
            if uncompressed > budget:
                raise StagingBudgetError(
                    f"Projected Delta checkpoint row group requires {uncompressed} bytes, above {budget}; "
                    "increase the budget or produce smaller checkpoint row groups."
                )
            for batch in parquet.iter_batches(
                batch_size=max(1, min(128, budget // 4096)),
                row_groups=[index],
                columns=selected,
                use_threads=False,
            ):
                check_cancelled(cancelled)
                if batch.nbytes > budget:
                    raise StagingBudgetError(f"Decoded Delta checkpoint batch exceeds {budget} bytes.")
                for row in batch.to_pylist():
                    inspector.action(row)


def preflight(
    client: httpx.Client,
    filesystem: str,
    root: str,
    tokens: TokenProvider,
    *,
    max_staging_bytes: int,
    scratch_dir: Path | None,
    is_excluded: Callable[[str], bool],
    on_progress: Callable[[str], None] | None,
    cancel_requested: Callable[[], bool] | None,
    strict: bool = True,
) -> Snapshot:
    """Discover every Delta table under ``root`` and validate its commit log references.

    A table is discovered generically, by encountering a ``_delta_log`` directory while
    walking; ``root`` itself need not already be a single table. When ``strict`` is true
    (a Tables/ root, which Fabric restricts to managed tables only) every file under
    ``root`` must belong to a discovered table. When false (a Files/ root, which legitimately
    mixes ordinary content with zero or more unmanaged Delta tables) files outside any
    discovered table are left for the caller to copy unvalidated, and only files actually
    inside a discovered table's ``_delta_log`` are inspected for escaping/unsafe references.
    """
    snapshot = Snapshot(budget=max_staging_bytes, strict=strict)
    budget = max_staging_bytes

    def walk(directory: str) -> Iterator[dict[str, Any]]:
        for entry in _listed_paths(client, filesystem, directory, tokens, cancel_requested):
            name = str(entry["name"])
            if is_excluded(name):
                continue
            if str(entry.get("isDirectory", False)).lower() == "true":
                if "/_delta_log/" in name and not name.endswith("/_delta_log/_sidecars"):
                    _fail(
                        name,
                        "unsupported metadata directory (catalog-managed/staged commits are not portable)",
                    )
                yield from walk(name)
            else:
                yield entry

    parent = scratch_dir or Path.cwd()
    parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="delta-preflight-", dir=parent) as directory:
        staged = Path(directory) / "checkpoint.parquet"
        for entry in walk(root):
            check_cancelled(cancel_requested)
            name = str(entry["name"])
            if "/_delta_log/" not in name:
                continue
            table, relative = name.split("/_delta_log/", 1)
            if not (table == root or table.startswith(root + "/")):
                _fail(name, "select the whole Delta table, not just a metadata/partition subtree")
            snapshot.tables.setdefault(table, set())
            url = f"{filesystem}/{quote(name, safe='/')}"
            size, etag = _file_properties(client, url, entry, tokens)
            snapshot.files[name] = etag, size
            snapshot.retained_bytes += len(name.encode()) + len(etag.encode()) + 256
            if snapshot.retained_bytes > budget:
                raise StagingBudgetError(
                    f"Delta metadata inventory exceeds {budget} bytes; increase the budget."
                )
            if on_progress:
                on_progress(f"Validating Delta references in {name}")
            inspector = Inspector(snapshot, table, name, is_excluded)
            chunks = _read_chunks(
                client,
                url,
                tokens,
                size,
                etag,
                max(1, min(budget // 2, 1024 * 1024)),
                cancel_requested,
            )
            try:
                if relative.endswith(".parquet"):
                    if not (
                        relative.startswith("_sidecars/")
                        or re.fullmatch(r"\d{20}\.checkpoint(?:\.[^.]+)*\.parquet", relative)
                    ):
                        _fail(name, "unrecognized checkpoint/sidecar file")
                    if size > budget:
                        raise StagingBudgetError(
                            f"Delta checkpoint '{name}' needs {size} staging bytes, above {budget}; "
                            "increase the budget or produce smaller checkpoints."
                        )
                    try:
                        written = 0
                        with staged.open("wb") as output:
                            for chunk in chunks:
                                if written + len(chunk) > budget:
                                    raise StagingBudgetError(
                                        "Delta checkpoint download would exceed its staging budget."
                                    )
                                output.write(chunk)
                                written += len(chunk)
                        _parquet(staged, inspector, budget, cancel_requested)
                    finally:
                        staged.unlink(missing_ok=True)
                else:
                    if not (
                        relative == "_last_checkpoint"
                        or re.fullmatch(r"\d{20}\.crc", relative)
                        or re.fullmatch(r"\d{20}(?:\.\d{20}\.compacted|\.checkpoint\.[^.]+)?\.json", relative)
                    ):
                        _fail(name, "unrecognized Delta metadata file")
                    records = 0
                    for value in _json_records(chunks, budget, name):
                        check_cancelled(cancel_requested)
                        records += 1
                        if relative == "_last_checkpoint":
                            inspector.last_checkpoint(value)
                        elif relative.endswith(".crc"):
                            inspector.checksum(value)
                        else:
                            inspector.action(value)
                    if not records or (relative == "_last_checkpoint" and records != 1):
                        _fail(name, "empty/malformed Delta metadata")
            except (ValueError, TypeError, pa.ArrowException) as exc:
                _fail(name, f"malformed or unsupported metadata: {exc}")
    for table, actions in snapshot.tables.items():
        if not {"protocol", "metaData"} <= actions:
            _fail(table, "no inspected protocol/metadata actions; checkpoint/log history is incomplete")
    for pointer in snapshot.pointers:
        check_cancelled(cancel_requested)
        if pointer not in snapshot.files:
            _fail(pointer, "referenced checkpoint/sidecar was not inspected (missing or excluded)")
    for log, version, parts in snapshot.checkpoints:
        check_cancelled(cancel_requested)
        if parts > len(snapshot.files):
            _fail(log, "last-checkpoint parts are missing")
        expected = (
            [f"{log}/{version:020}.checkpoint.parquet"]
            if parts == 1
            else (
                f"{log}/{version:020}.checkpoint.{part:010}.{parts:010}.parquet"
                for part in range(1, parts + 1)
            )
        )
        for name in expected:
            if name not in snapshot.files:
                _fail(name, "referenced checkpoint/sidecar was not inspected (missing or excluded)")
    observed: set[str] = set()
    for entry in walk(root):
        snapshot.pin(entry)
        if str(entry["name"]) in snapshot.files:
            observed.add(str(entry["name"]))
    if observed != snapshot.files.keys():
        _fail(root, "Delta metadata changed during preflight; keep source writers stopped")
    return snapshot
