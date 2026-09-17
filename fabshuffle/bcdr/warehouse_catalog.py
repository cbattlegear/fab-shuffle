"""Authoritative recovery catalog over the Fabric Warehouse TDS endpoint.

Service contract verified against Microsoft Learn:
* fabric/data-warehouse/{connectivity,service-principals,data-types}
* fabric/data-warehouse/{transactions,table-constraints}

All writes update the existing singleton fence in the same transaction. Snapshot
write conflicts, not NOT ENFORCED keys, serialize publishers and mode transitions.
There is deliberately no automatic ownership expiry or REST retry after ambiguity.
"""

from __future__ import annotations

import json
import re
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from uuid import UUID, uuid5

import pyodbc
from pydantic import BaseModel, ValidationError

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.catalog import (
    AmbiguousCommit,
    CapturedGeneration,
    CatalogConflict,
    RecoveryCatalog,
)
from fabshuffle.bcdr.contracts import (
    AppliedItem,
    CaptureSnapshot,
    CatalogDocument,
    CatalogState,
    ControllerLease,
    OperationRecord,
    OperationState,
    RecoveryMode,
    RecoverySet,
    canonical_id,
    canonical_json,
    digest,
    reject_embedded_secrets,
)
from fabshuffle.bcdr.payloads import (
    CHUNK_BYTES,
    CapturedPayload,
    IntegrityError,
    PayloadChunk,
    chunk_count,
    join_payload,
    split_payload,
)
from fabshuffle.transfer.sqlschema import connect

_RECORD_TABLES = {
    "workspaces": "source_workspaces",
    "items": "source_items",
    "dependencies": "reference_edges",
    "desired_acls": "desired_acls",
    "protections": "protection_inputs",
}
_DDL = (
    "CREATE SCHEMA bcdr",
    """CREATE TABLE bcdr.control (
        singleton int NOT NULL, schema_version int NOT NULL, recovery_set_id varchar(36) NOT NULL,
        config varbinary(max) NOT NULL, controller_id varchar(36) NULL, epoch bigint NOT NULL,
        revision bigint NOT NULL, mode varchar(32) NOT NULL, current_generation_id varchar(36) NULL
    )""",
    """CREATE TABLE bcdr.generations (
        generation_id varchar(36) NOT NULL, status varchar(16) NOT NULL,
        manifest_sha256 varchar(64) NOT NULL, payload_count bigint NOT NULL
    )""",
    """CREATE TABLE bcdr.payloads (
        generation_id varchar(36) NOT NULL, payload_id varchar(36) NOT NULL,
        info varbinary(max) NOT NULL
    )""",
    """CREATE TABLE bcdr.payload_chunks (
        generation_id varchar(36) NOT NULL, payload_id varchar(36) NOT NULL,
        ordinal int NOT NULL, byte_length int NOT NULL, sha256 varchar(64) NOT NULL,
        data varbinary(max) NOT NULL
    )""",
    *(
        f"""CREATE TABLE bcdr.{table} (
            generation_id varchar(36) NOT NULL, record_key varchar(128) NOT NULL,
            ordinal int NOT NULL, payload_id varchar(36) NOT NULL
        )"""
        for table in _RECORD_TABLES.values()
    ),
    """CREATE TABLE bcdr.applied_items (
        source_key varchar(128) NOT NULL, document varbinary(max) NOT NULL
    )""",
    """CREATE TABLE bcdr.operations (
        operation_id varchar(36) NOT NULL, state varchar(16) NOT NULL, document varbinary(max) NOT NULL
    )""",
    """CREATE TABLE bcdr.operation_events (
        operation_id varchar(36) NOT NULL, ordinal int NOT NULL, document varbinary(max) NOT NULL
    )""",
    """CREATE TABLE bcdr.mode_transitions (
        operation_id varchar(36) NOT NULL, document varbinary(max) NOT NULL
    )""",
    """CREATE TABLE bcdr.coordinator_records (
        namespace varchar(64) NOT NULL, key_hash varchar(64) NOT NULL,
        revision bigint NOT NULL, document varbinary(max) NOT NULL
    )""",
    """CREATE TABLE bcdr.coordinator_history (
        namespace varchar(64) NOT NULL, key_hash varchar(64) NOT NULL,
        revision bigint NOT NULL, document varbinary(max) NOT NULL
    )""",
)
_TRANSITIONS = {
    RecoveryMode.STANDBY: {RecoveryMode.SYNCING, RecoveryMode.ENABLING_RECOVERY, RecoveryMode.PARKING},
    RecoveryMode.SYNCING: {RecoveryMode.STANDBY},
    RecoveryMode.PARKING: {RecoveryMode.STANDBY},
    RecoveryMode.ENABLING_RECOVERY: {RecoveryMode.ACTIVE_RECOVERY},
    RecoveryMode.ACTIVE_RECOVERY: {RecoveryMode.FAILING_BACK},
    RecoveryMode.FAILING_BACK: {RecoveryMode.REARMING, RecoveryMode.ACTIVE_RECOVERY},
    RecoveryMode.REARMING: {RecoveryMode.STANDBY},
}
_BUSINESS_MODES = {
    RecoveryMode.SYNCING, RecoveryMode.ENABLING_RECOVERY, RecoveryMode.ACTIVE_RECOVERY,
    RecoveryMode.FAILING_BACK, RecoveryMode.REARMING,
}
_PENDING = {OperationState.INTENT, OperationState.RUNNING, OperationState.AMBIGUOUS}


def _small_document(value: BaseModel | dict) -> bytes:
    data = canonical_json(value)
    if len(data) > CHUNK_BYTES:
        raise ValueError("Operational record exceeds 1 MiB; store large captured content in payload chunks")
    reject_embedded_secrets(data)
    return data


def _one(rows: Sequence, label: str):
    if len(rows) != 1:
        raise IntegrityError(f"Expected one {label}, found {len(rows)}; reconcile the catalog")
    return rows[0]


def _conflict(error: pyodbc.Error) -> bool:
    # Only documented, transaction-aborted Warehouse conflicts are safe to retry.
    return bool(re.search(r"\b(?:24556|24706)\b", str(error)))


class WarehouseCatalog(RecoveryCatalog):
    """One catalog Warehouse and one explicitly authorized controller per recovery set.

    ``connection_factory`` is a DB-API connection seam for tests and deployment wiring;
    production callers should use ``from_endpoint``. Connections must start with
    autocommit disabled. Every connection is closed, including failed reads.
    """

    def __init__(
        self, connection_factory: Callable[[], pyodbc.Connection], recovery_set: RecoverySet,
        *, conflict_attempts: int = 3, sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not 1 <= conflict_attempts <= 5:
            raise ValueError("Use between one and five bounded SQL conflict attempts")
        self._connect = connection_factory
        self.recovery_set = recovery_set
        self._attempts = conflict_attempts
        self._sleep = sleep
        self._mutex = threading.RLock()

    @classmethod
    def from_endpoint(
        cls, server: str, database: str, tokens: TokenProvider, recovery_set: RecoverySet,
    ) -> WarehouseCatalog:
        # The legacy connector constructs an ODBC string. Validate its inputs here
        # rather than changing migration connection behavior.
        if not re.fullmatch(
            r"[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.datawarehouse\.fabric\.microsoft\.com", server,
        ):
            raise ValueError("Use the returned public Fabric Warehouse SQL hostname, without URL or options")
        if not database or any(char in database for char in ";{}\r\n\x00") or database.casefold() == "master":
            raise ValueError("Specify a safe explicit control Warehouse catalog, not master")
        if canonical_id(tokens.principal.tenant_id) != recovery_set.tenant_id:
            raise ValueError("The SQL token provider belongs to a different recovery tenant")
        return cls(lambda: connect(server, database, tokens), recovery_set)

    @contextmanager
    def _connection(self) -> Iterator[pyodbc.Connection]:
        connection = self._connect()
        try:
            if connection.autocommit:
                raise ValueError("Warehouse catalog requires autocommit=False")
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        """Create a fresh, versioned schema or validate the existing singleton.

        CREATE SCHEMA is the initial serialization point. A racing initializer fails
        explicitly; it cannot insert a second singleton using an unenforced key.
        """
        with self._mutex, self._connection() as connection:
            cursor = connection.cursor()
            exists = cursor.execute(
                "SELECT schema_id FROM sys.schemas WHERE name = ?", "bcdr",
            ).fetchall()
            if exists:
                state = self._read_state(cursor)
                self._read_config(cursor)
                if state.recovery_set_id != self.recovery_set.recovery_set_id:
                    raise IntegrityError("This Warehouse belongs to another recovery set")
                return
            try:
                for statement in _DDL:
                    cursor.execute(statement)
                cursor.execute(
                    "INSERT INTO bcdr.control VALUES (1, 1, ?, ?, NULL, 0, 0, ?, NULL)",
                    self.recovery_set.recovery_set_id, _small_document(self.recovery_set),
                    RecoveryMode.STANDBY.value,
                )
            except (pyodbc.Error, ValueError):
                connection.rollback()
                raise
            self._commit(connection)

    @staticmethod
    def _commit(connection: pyodbc.Connection) -> None:
        try:
            connection.commit()
        except pyodbc.Error as error:
            if _conflict(error):
                raise
            raise AmbiguousCommit(
                f"Warehouse commit acknowledgement failed: {error}. Re-read durable state before retrying."
            ) from error

    def _read_config(self, cursor) -> RecoverySet:
        row = _one(cursor.execute("SELECT config FROM bcdr.control").fetchall(), "catalog configuration")
        config = RecoverySet.model_validate_json(bytes(row[0]))
        if config != self.recovery_set:
            raise IntegrityError("Recovery configuration differs from the authoritative Warehouse")
        return config

    def _read_state(self, cursor) -> CatalogState:
        row = _one(cursor.execute(
            "SELECT singleton, schema_version, recovery_set_id, controller_id, epoch, revision, "
            "mode, current_generation_id FROM bcdr.control",
        ).fetchall(), "catalog control row")
        if row[0] != 1 or row[1] != 1 or row[2] != self.recovery_set.recovery_set_id:
            raise IntegrityError("Wrong catalog schema version or recovery identity")
        return CatalogState(
            recovery_set_id=row[2], controller_id=row[3], epoch=row[4], revision=row[5],
            mode=row[6], current_generation_id=row[7],
        )

    def state(self) -> CatalogState:
        with self._connection() as connection:
            cursor = connection.cursor()
            self._read_config(cursor)
            return self._read_state(cursor)

    def _transaction(
        self, lease: ControllerLease | None, action: Callable,
        *, allowed_modes: set[RecoveryMode] | None = None,
    ):
        with self._mutex:
            for attempt in range(self._attempts):
                try:
                    with self._connection() as connection:
                        cursor = connection.cursor()
                        try:
                            state = self._read_state(cursor)
                            self._read_config(cursor)
                            if lease is not None and (
                                lease.recovery_set_id != state.recovery_set_id
                                or lease.controller_id != state.controller_id or lease.epoch != state.epoch
                            ):
                                raise CatalogConflict("Controller ownership changed; stop new mutations")
                            if allowed_modes is not None and state.mode not in allowed_modes:
                                raise CatalogConflict(f"Catalog mode {state.mode} forbids this mutation")
                            cursor.execute(
                                "UPDATE bcdr.control SET revision = revision + 1 WHERE singleton = 1 "
                                "AND revision = ? AND epoch = ?", state.revision, state.epoch,
                            )
                            changed = self._read_state(cursor)
                            if changed.revision != state.revision + 1:
                                raise CatalogConflict("Catalog fence did not change the expected state")
                            result = action(cursor, state)
                        except (pyodbc.Error, ValueError, CatalogConflict):
                            connection.rollback()
                            raise
                        self._commit(connection)
                        return result
                except pyodbc.Error as error:
                    if not _conflict(error) or attempt + 1 == self._attempts:
                        raise
                    self._sleep(0.1 * (2 ** attempt))
        raise CatalogConflict("Warehouse conflict retry budget exhausted")

    def acquire_controller(self, controller_id: str) -> ControllerLease:
        controller_id = canonical_id(controller_id)

        def acquire(cursor, state):
            if state.controller_id is not None:
                raise CatalogConflict(
                    f"Recovery set is owned by controller {state.controller_id}; reconcile before takeover"
                )
            cursor.execute(
                "UPDATE bcdr.control SET controller_id = ?, epoch = ? WHERE singleton = 1",
                controller_id, state.epoch + 1,
            )
            return ControllerLease(
                recovery_set_id=state.recovery_set_id, controller_id=controller_id, epoch=state.epoch + 1,
            )

        return self._transaction(None, acquire)

    def takeover_controller(
        self, controller_id: str, *, expected_controller_id: str, expected_epoch: int,
        fencing_evidence: str, operation_id: str,
    ) -> ControllerLease:
        """Operator-fenced replacement, never a lease-timeout guess about a live writer."""
        controller_id, expected_controller_id = map(canonical_id, (controller_id, expected_controller_id))
        operation_id = canonical_id(operation_id)
        if not fencing_evidence.strip():
            raise ValueError("Record evidence that the previous controller is stopped before takeover")

        def takeover(cursor, state):
            if state.controller_id != expected_controller_id or state.epoch != expected_epoch:
                raise CatalogConflict("Controller ownership changed before the approved takeover")
            if cursor.execute(
                "SELECT operation_id FROM bcdr.mode_transitions WHERE operation_id = ?", operation_id,
            ).fetchall():
                raise CatalogConflict("Takeover operation already exists; re-read its committed state")
            cursor.execute(
                "INSERT INTO bcdr.mode_transitions VALUES (?, ?)", operation_id, _small_document({
                    "operation_id": operation_id, "previous_controller_id": expected_controller_id,
                    "controller_id": controller_id, "epoch": state.epoch + 1,
                    "fencing_evidence": fencing_evidence,
                }),
            )
            cursor.execute(
                "UPDATE bcdr.control SET controller_id = ?, epoch = ? WHERE singleton = 1",
                controller_id, state.epoch + 1,
            )
            return ControllerLease(
                recovery_set_id=state.recovery_set_id, controller_id=controller_id, epoch=state.epoch + 1,
            )

        return self._transaction(None, takeover)

    def release_controller(self, lease: ControllerLease) -> None:
        def release(cursor, state):
            self._require_drained(cursor)
            cursor.execute("UPDATE bcdr.control SET controller_id = NULL WHERE singleton = 1")

        self._transaction(lease, release, allowed_modes={RecoveryMode.STANDBY})

    def transition_mode(
        self, lease: ControllerLease, expected: RecoveryMode, desired: RecoveryMode, operation_id: str,
    ) -> CatalogState:
        operation_id = canonical_id(operation_id)

        def transition(cursor, state):
            if state.mode != expected or desired not in _TRANSITIONS[expected]:
                raise CatalogConflict(f"Recovery mode cannot transition from {state.mode} to {desired}")
            self._require_drained(cursor)
            if cursor.execute(
                "SELECT operation_id FROM bcdr.mode_transitions WHERE operation_id = ?", operation_id,
            ).fetchall():
                raise CatalogConflict("Mode operation already exists; reconcile its committed outcome")
            document = {
                "operation_id": operation_id, "controller_id": lease.controller_id, "epoch": lease.epoch,
                "from_mode": expected.value, "to_mode": desired.value,
                "recorded_at": datetime.now(UTC).isoformat(),
            }
            cursor.execute(
                "INSERT INTO bcdr.mode_transitions VALUES (?, ?)", operation_id, _small_document(document),
            )
            cursor.execute("UPDATE bcdr.control SET mode = ? WHERE singleton = 1", desired.value)
            return self._read_state(cursor)

        return self._transaction(lease, transition)

    @staticmethod
    def _record_key(collection: str, record) -> str:
        if collection in {"workspaces", "items"}:
            return record.identity.key
        return getattr(record, {"dependencies": "edge_id", "desired_acls": "acl_id",
                                "protections": "protection_id"}[collection])

    def _documents(
        self, snapshot: CaptureSnapshot, payloads: Sequence[CapturedPayload],
    ) -> list[tuple[dict, bytes]]:
        documents = []
        generation_uuid = UUID(snapshot.generation_id)
        header = snapshot.model_dump(mode="json", exclude={*_RECORD_TABLES, "payloads"})

        def document(namespace, key, ordinal, data, payload_id, descriptor=None):
            reject_embedded_secrets(data)
            info = {
                "payload_id": payload_id, "namespace": namespace, "key": key, "ordinal": ordinal,
                "byte_length": len(data), "sha256": digest(data), "chunk_count": chunk_count(len(data)),
                "descriptor": descriptor,
            }
            _small_document(info)
            documents.append((info, data))

        document("header", "header", 0, canonical_json(header), str(uuid5(generation_uuid, "header")))
        for collection in _RECORD_TABLES:
            for ordinal, record in enumerate(getattr(snapshot, collection)):
                key = self._record_key(collection, record)
                document(collection, key, ordinal, canonical_json(record),
                         str(uuid5(generation_uuid, f"{collection}:{key}")))
        supplied = {payload.descriptor.payload_id: payload for payload in payloads}
        if len(supplied) != len(payloads) or set(supplied) != {part.payload_id for part in snapshot.payloads}:
            raise IntegrityError("Supply exactly one payload for every captured descriptor")
        for ordinal, descriptor in enumerate(snapshot.payloads):
            payload = supplied[descriptor.payload_id]
            if payload.descriptor != descriptor:
                raise IntegrityError("Payload descriptor differs from the captured inventory")
            payload.validate()
            document("artifact", descriptor.path, ordinal, payload.data, descriptor.payload_id,
                     descriptor.model_dump(mode="json"))
        ids = [info["payload_id"] for info, _ in documents]
        if len(ids) != len(set(ids)):
            raise IntegrityError("Metadata and artifact payload identities collide")
        return documents

    @staticmethod
    def _manifest_hash(infos: list[dict]) -> str:
        # Each length-delimited descriptor hash binds identity, encoding, purpose,
        # ordering and content. Sorting does not discard duplicate detection.
        return digest(b"".join(
            bytes.fromhex(digest(canonical_json(info)))
            for info in sorted(infos, key=lambda info: info["payload_id"])
        ))

    def stage_generation(
        self, lease: ControllerLease, snapshot: CaptureSnapshot, payloads: Sequence[CapturedPayload],
    ) -> None:
        """Stage bounded commits. Interrupted generations never replace the good pointer.

        Partial snapshots may be staged for diagnosis, but publication rejects them.
        An interrupted/ambiguous stage is inspected with ``inspect_generation``;
        capture again under a new generation ID rather than appending guessed chunks.
        """
        snapshot = CaptureSnapshot.model_validate_json(canonical_json(snapshot))
        if snapshot.recovery_set_id != self.recovery_set.recovery_set_id:
            raise ValueError("Capture belongs to another recovery set")
        documents = self._documents(snapshot, payloads)
        manifest = self._manifest_hash([info for info, _ in documents])

        def begin(cursor, state):
            if cursor.execute(
                "SELECT status FROM bcdr.generations WHERE generation_id = ?", snapshot.generation_id,
            ).fetchall():
                raise CatalogConflict("Generation already exists; inspect it instead of overwriting capture")
            cursor.execute(
                "INSERT INTO bcdr.generations VALUES (?, ?, ?, ?)",
                snapshot.generation_id, "staging", manifest, len(documents),
            )

        allowed = {RecoveryMode.SYNCING, RecoveryMode.FAILING_BACK}
        self._transaction(lease, begin, allowed_modes=allowed)
        for info, data in documents:
            def put_info(cursor, state, info=info):
                cursor.execute("INSERT INTO bcdr.payloads VALUES (?, ?, ?)",
                               snapshot.generation_id, info["payload_id"], _small_document(info))
                if info["namespace"] in _RECORD_TABLES:
                    cursor.execute(
                        f"INSERT INTO bcdr.{_RECORD_TABLES[info['namespace']]} VALUES (?, ?, ?, ?)",
                        snapshot.generation_id, info["key"], info["ordinal"], info["payload_id"],
                    )

            self._transaction(lease, put_info, allowed_modes=allowed)
            for chunk in split_payload(data):
                def put_chunk(cursor, state, chunk=chunk, info=info):
                    cursor.execute(
                        "INSERT INTO bcdr.payload_chunks VALUES (?, ?, ?, ?, ?, ?)",
                        snapshot.generation_id, info["payload_id"], chunk.ordinal,
                        chunk.byte_length, chunk.sha256, chunk.data,
                    )

                self._transaction(lease, put_chunk, allowed_modes=allowed)
        self.inspect_generation(snapshot.generation_id)

        def seal(cursor, state):
            row = _one(cursor.execute(
                "SELECT status, manifest_sha256, payload_count FROM bcdr.generations WHERE generation_id = ?",
                snapshot.generation_id,
            ).fetchall(), "staged generation")
            if tuple(row) != ("staging", manifest, len(documents)):
                raise CatalogConflict("Staged generation changed before sealing")
            cursor.execute("UPDATE bcdr.generations SET status = ? WHERE generation_id = ?",
                           "sealed", snapshot.generation_id)

        self._transaction(lease, seal, allowed_modes=allowed)

    def inspect_generation(self, generation_id: str) -> CapturedGeneration:
        """Validate complete staged content without treating it as a published recovery point."""
        with self._connection() as connection:
            self._read_state(connection.cursor())
            return self._load(connection.cursor(), canonical_id(generation_id), require_complete=False)

    def load_generation(self, generation_id: str | None = None) -> CapturedGeneration:
        with self._connection() as connection:
            cursor = connection.cursor()
            self._read_config(cursor)
            state = self._read_state(cursor)
            generation_id = canonical_id(generation_id) if generation_id else state.current_generation_id
            if generation_id is None:
                raise IntegrityError("No complete captured generation has been published")
            result = self._load(cursor, generation_id, require_complete=True)
            result.snapshot.require_publishable(self.recovery_set)
            return result

    def _load(self, cursor, generation_id: str, *, require_complete: bool) -> CapturedGeneration:
        generation = _one(cursor.execute(
            "SELECT status, manifest_sha256, payload_count FROM bcdr.generations WHERE generation_id = ?",
            generation_id,
        ).fetchall(), "capture generation")
        if require_complete and generation[0] != "complete":
            raise IntegrityError("Generation is not published complete")
        rows = cursor.execute(
            "SELECT payload_id, info FROM bcdr.payloads WHERE generation_id = ?", generation_id,
        ).fetchall()
        if len(rows) != generation[2] or len({row[0] for row in rows}) != len(rows):
            raise IntegrityError("Incomplete or duplicate payload inventory")
        infos = [json.loads(bytes(row[1])) for row in rows]
        if any(info.get("payload_id") != row[0] for info, row in zip(infos, rows, strict=True)):
            raise IntegrityError("Payload identity differs from its descriptor")
        if self._manifest_hash(infos) != generation[1]:
            raise IntegrityError("Generation manifest hash mismatch")
        header = None
        records: dict[str, list[tuple[int, dict]]] = {collection: [] for collection in _RECORD_TABLES}
        artifacts = []
        for info in infos:
            if set(info) != {
                "payload_id", "namespace", "key", "ordinal", "byte_length",
                "sha256", "chunk_count", "descriptor",
            }:
                raise IntegrityError("Unknown payload descriptor schema")
            chunks = cursor.execute(
                "SELECT ordinal, byte_length, sha256, data FROM bcdr.payload_chunks "
                "WHERE generation_id = ? AND payload_id = ?", generation_id, info["payload_id"],
            ).fetchall()
            data = join_payload(
                (PayloadChunk(row[0], row[1], row[2], bytes(row[3])) for row in chunks),
                byte_length=info["byte_length"], sha256=info["sha256"], count=info["chunk_count"],
            )
            reject_embedded_secrets(data)
            namespace = info["namespace"]
            if namespace == "header":
                if header is not None or info["ordinal"] != 0 or info["key"] != "header":
                    raise IntegrityError("Invalid or duplicate generation header")
                header = json.loads(data)
            elif namespace in _RECORD_TABLES:
                records[namespace].append((info["ordinal"], json.loads(data)))
            elif namespace == "artifact":
                from fabshuffle.bcdr.contracts import PayloadDescriptor

                descriptor = PayloadDescriptor.model_validate(info["descriptor"])
                if descriptor.payload_id != info["payload_id"]:
                    raise IntegrityError("Artifact identity differs from its stored descriptor")
                payload = CapturedPayload(descriptor, data)
                payload.validate()
                artifacts.append((info["ordinal"], payload))
            else:
                raise IntegrityError("Unknown metadata namespace")
        if header is None:
            raise IntegrityError("Generation header is missing")
        for collection, entries in records.items():
            entries.sort(key=lambda entry: entry[0])
            if [entry[0] for entry in entries] != list(range(len(entries))):
                raise IntegrityError("Duplicate or incomplete metadata ordering")
            header[collection] = [entry[1] for entry in entries]
            indexed = cursor.execute(
                f"SELECT record_key, ordinal, payload_id FROM bcdr.{_RECORD_TABLES[collection]} "
                "WHERE generation_id = ?", generation_id,
            ).fetchall()
            expected = sorted(
                (info["key"], info["ordinal"], info["payload_id"])
                for info in infos if info["namespace"] == collection
            )
            if sorted(tuple(row) for row in indexed) != expected:
                raise IntegrityError(f"Corrupt or duplicate {collection} index")
        artifacts.sort(key=lambda entry: entry[0])
        if [entry[0] for entry in artifacts] != list(range(len(artifacts))):
            raise IntegrityError("Duplicate or incomplete artifact ordering")
        header["payloads"] = [payload.descriptor.model_dump(mode="json") for _, payload in artifacts]
        try:
            snapshot = CaptureSnapshot.model_validate(header)
        except ValidationError as error:
            raise IntegrityError(f"Invalid capture schema: {error}") from error
        if (
            snapshot.generation_id != generation_id
            or snapshot.recovery_set_id != self.recovery_set.recovery_set_id
        ):
            raise IntegrityError("Stored generation identity mismatch")
        # Detect orphan chunks as well as missing per-payload chunks.
        total = _one(cursor.execute(
            "SELECT COUNT(*) FROM bcdr.payload_chunks WHERE generation_id = ?", generation_id,
        ).fetchall(), "chunk count")[0]
        if total != sum(info["chunk_count"] for info in infos):
            raise IntegrityError("Unexpected orphan or duplicate chunks in generation")
        return CapturedGeneration(snapshot, tuple(payload for _, payload in artifacts))

    def publish_generation(
        self, lease: ControllerLease, generation_id: str, *, expected_current: str | None,
    ) -> CapturedGeneration:
        generation_id = canonical_id(generation_id)
        expected_current = canonical_id(expected_current) if expected_current is not None else None
        result = self.inspect_generation(generation_id)
        result.snapshot.require_publishable(self.recovery_set)

        def publish(cursor, state):
            if state.current_generation_id != expected_current:
                raise CatalogConflict("The current capture changed; inspect it before publishing")
            if result.snapshot.parent_generation_id != expected_current:
                raise CatalogConflict("Capture parent must match the expected current generation")
            row = _one(cursor.execute(
                "SELECT status, manifest_sha256, payload_count FROM bcdr.generations WHERE generation_id = ?",
                generation_id,
            ).fetchall(), "sealed generation")
            if row[0] != "sealed":
                raise CatalogConflict("Only a sealed, unpublished generation may be published")
            cursor.execute(
                "UPDATE bcdr.generations SET status = ? WHERE generation_id = ?", "complete", generation_id,
            )
            cursor.execute(
                "UPDATE bcdr.control SET current_generation_id = ? WHERE singleton = 1", generation_id,
            )

        self._transaction(lease, publish, allowed_modes={RecoveryMode.SYNCING, RecoveryMode.FAILING_BACK})
        return result

    def _operations(self, cursor) -> tuple[OperationRecord, ...]:
        rows = cursor.execute("SELECT operation_id, state, document FROM bcdr.operations").fetchall()
        if len(rows) != len({row[0] for row in rows}):
            raise IntegrityError("Duplicate operation identities")
        result = tuple(OperationRecord.model_validate_json(bytes(row[2])) for row in rows)
        if any(op.operation_id != row[0] or op.state != row[1] for op, row in zip(result, rows, strict=True)):
            raise IntegrityError("Operation index differs from its persisted record")
        return result

    def operations(self) -> tuple[OperationRecord, ...]:
        with self._connection() as connection:
            cursor = connection.cursor()
            self._read_state(cursor)
            return self._operations(cursor)

    def pending_operations(self) -> tuple[OperationRecord, ...]:
        return tuple(op for op in self.operations() if op.state in _PENDING)

    def operation_history(self, operation_id: str) -> tuple[OperationRecord, ...]:
        operation_id = canonical_id(operation_id)
        with self._connection() as connection:
            cursor = connection.cursor()
            self._read_state(cursor)
            rows = cursor.execute(
                "SELECT ordinal, document FROM bcdr.operation_events WHERE operation_id = ? ORDER BY ordinal",
                operation_id,
            ).fetchall()
            if [row[0] for row in rows] != list(range(len(rows))):
                raise IntegrityError("Duplicate or incomplete operation history")
            result = tuple(OperationRecord.model_validate_json(bytes(row[1])) for row in rows)
            if any(event.operation_id != operation_id for event in result):
                raise IntegrityError("Operation history identity mismatch")
            return result

    def _require_drained(self, cursor) -> None:
        if any(op.state in _PENDING for op in self._operations(cursor)):
            raise CatalogConflict("Reconcile or settle every pending operation before changing recovery mode")

    def begin_operation(self, lease: ControllerLease, operation: OperationRecord) -> None:
        if operation.state != OperationState.INTENT:
            raise ValueError("Persist an intent before issuing a service mutation")
        document = _small_document(operation)

        def begin(cursor, state):
            if any(op.operation_id == operation.operation_id for op in self._operations(cursor)):
                raise CatalogConflict("Operation exists; reconcile before repeating its service mutation")
            if operation.generation_id is not None:
                row = _one(cursor.execute(
                    "SELECT status FROM bcdr.generations WHERE generation_id = ?", operation.generation_id,
                ).fetchall(), "operation capture generation")
                if row[0] != "complete":
                    raise IntegrityError("Business mutations require a published capture")
            cursor.execute(
                "INSERT INTO bcdr.operations VALUES (?, ?, ?)",
                operation.operation_id, operation.state.value, document,
            )
            cursor.execute(
                "INSERT INTO bcdr.operation_events VALUES (?, ?, ?)", operation.operation_id, 0, document,
            )

        self._transaction(lease, begin, allowed_modes=_BUSINESS_MODES)

    def record_operation(self, lease: ControllerLease, operation: OperationRecord) -> None:
        document = _small_document(operation)

        def update(cursor, state):
            previous = _one(
                [op for op in self._operations(cursor) if op.operation_id == operation.operation_id],
                "operation intent",
            )
            if previous.state not in _PENDING:
                if previous == operation:
                    return
                raise CatalogConflict("Settled operation outcomes are immutable; start a new attempt")
            if operation.state == OperationState.INTENT:
                raise CatalogConflict("Do not revert an observed operation to an unissued intent")
            for field in ("kind", "generation_id", "source", "capacity_id", "ownership_evidence"):
                if getattr(previous, field) != getattr(operation, field):
                    raise CatalogConflict(f"Operation {field} differs from its durable intent")
            if previous.target is not None and operation.target != previous.target:
                raise CatalogConflict("Operation target identity changed")
            if operation.recorded_at < previous.recorded_at:
                raise CatalogConflict("Operation observation precedes its previous durable record")
            history = cursor.execute(
                "SELECT ordinal FROM bcdr.operation_events WHERE operation_id = ? ORDER BY ordinal",
                operation.operation_id,
            ).fetchall()
            if not history or [row[0] for row in history] != list(range(len(history))):
                raise IntegrityError("Duplicate or incomplete operation history")
            cursor.execute(
                "INSERT INTO bcdr.operation_events VALUES (?, ?, ?)",
                operation.operation_id, len(history), document,
            )
            cursor.execute(
                "UPDATE bcdr.operations SET state = ?, document = ? WHERE operation_id = ?",
                operation.state.value, document, operation.operation_id,
            )

        self._transaction(lease, update)

    def record_applied(self, lease: ControllerLease, applied: AppliedItem) -> None:
        document = _small_document(applied)
        generation = self.load_generation(applied.capture_generation_id)
        if applied.source.key not in {item.identity.key for item in generation.snapshot.items}:
            raise ValueError("Applied source is absent from its captured generation")
        if applied.target.workspace_id == self.recovery_set.control_workspace.workspace_id:
            raise ValueError("The control workspace cannot be a business recovery target")

        def record(cursor, state):
            operation = _one(
                [op for op in self._operations(cursor) if op.operation_id == applied.operation_id],
                "applied operation",
            )
            if (
                operation.state != OperationState.SUCCEEDED or operation.source != applied.source
                or operation.target != applied.target
                or operation.generation_id != applied.capture_generation_id
            ):
                raise CatalogConflict("Applied state requires a successful matching operation outcome")
            rows = cursor.execute(
                "SELECT document FROM bcdr.applied_items WHERE source_key = ?", applied.source.key,
            ).fetchall()
            if len(rows) > 1:
                raise IntegrityError("Duplicate applied source mapping")
            if rows:
                cursor.execute("UPDATE bcdr.applied_items SET document = ? WHERE source_key = ?",
                               document, applied.source.key)
            else:
                cursor.execute("INSERT INTO bcdr.applied_items VALUES (?, ?)", applied.source.key, document)

        self._transaction(lease, record, allowed_modes=_BUSINESS_MODES)

    def applied_items(self) -> tuple[AppliedItem, ...]:
        with self._connection() as connection:
            cursor = connection.cursor()
            self._read_state(cursor)
            rows = cursor.execute("SELECT source_key, document FROM bcdr.applied_items").fetchall()
            if len(rows) != len({row[0] for row in rows}):
                raise IntegrityError("Duplicate applied source identities")
            records = tuple(AppliedItem.model_validate_json(bytes(row[1])) for row in rows)
            if any(record.source.key != row[0] for record, row in zip(records, rows, strict=True)):
                raise IntegrityError("Applied index differs from stored identity")
            return records

    @staticmethod
    def _record_address(namespace: str, key: str) -> str:
        CatalogDocument(namespace=namespace, key=key, revision=1, document={})
        if len(key.encode("utf-8")) > 2048 or any(ord(char) < 32 for char in key):
            raise ValueError("Catalog record keys must be printable and at most 2048 UTF-8 bytes")
        return digest(key.encode("utf-8"))

    def _get_record(self, cursor, namespace: str, key: str) -> CatalogDocument | None:
        key_hash = self._record_address(namespace, key)
        rows = cursor.execute(
            "SELECT revision, document FROM bcdr.coordinator_records WHERE namespace = ? AND key_hash = ?",
            namespace, key_hash,
        ).fetchall()
        if not rows:
            return None
        row = _one(rows, "coordinator record")
        record = CatalogDocument.model_validate_json(bytes(row[1]))
        if record.namespace != namespace or record.key != key or record.revision != row[0]:
            raise IntegrityError("Coordinator record identity/revision mismatch")
        return record

    def get_record(self, namespace: str, key: str) -> CatalogDocument | None:
        with self._connection() as connection:
            cursor = connection.cursor()
            self._read_state(cursor)
            return self._get_record(cursor, namespace, key)

    def list_records(self, namespace: str) -> tuple[CatalogDocument, ...]:
        self._record_address(namespace, "namespace-validation")
        with self._connection() as connection:
            cursor = connection.cursor()
            self._read_state(cursor)
            rows = cursor.execute(
                "SELECT key_hash, revision, document FROM bcdr.coordinator_records WHERE namespace = ?",
                namespace,
            ).fetchall()
            if len(rows) != len({row[0] for row in rows}):
                raise IntegrityError("Duplicate coordinator record keys")
            records = tuple(CatalogDocument.model_validate_json(bytes(row[2])) for row in rows)
            if any(
                record.namespace != namespace or digest(record.key.encode("utf-8")) != row[0]
                or record.revision != row[1]
                for record, row in zip(records, rows, strict=True)
            ):
                raise IntegrityError("Coordinator record index mismatch")
            return records

    def put_record(
        self, lease: ControllerLease, namespace: str, key: str, document: dict,
        *, expected_revision: int | None,
    ) -> CatalogDocument:
        key_hash = self._record_address(namespace, key)
        if expected_revision is not None and expected_revision < 1:
            raise ValueError("Expected revision must be positive, or None to create")
        result = CatalogDocument(
            namespace=namespace, key=key, revision=(expected_revision or 0) + 1, document=document,
        )
        encoded = _small_document(result)

        def put(cursor, state):
            previous = self._get_record(cursor, namespace, key)
            if (previous.revision if previous else None) != expected_revision:
                raise CatalogConflict("Coordinator record changed; re-read before updating")
            cursor.execute(
                "INSERT INTO bcdr.coordinator_history VALUES (?, ?, ?, ?)",
                namespace, key_hash, result.revision, encoded,
            )
            if previous is None:
                cursor.execute(
                    "INSERT INTO bcdr.coordinator_records VALUES (?, ?, ?, ?)",
                    namespace, key_hash, result.revision, encoded,
                )
            else:
                cursor.execute(
                    "UPDATE bcdr.coordinator_records SET revision = ?, document = ? "
                    "WHERE namespace = ? AND key_hash = ?", result.revision, encoded, namespace, key_hash,
                )
            return result

        return self._transaction(
            lease, put, allowed_modes=set(RecoveryMode) - {RecoveryMode.PARKING},
        )
