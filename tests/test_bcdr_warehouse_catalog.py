"""DB-API SQL fixtures test persistence/fault handling, not live Fabric qualification.

SQLite is a test-only storage harness. Warehouse table-conflict errors are injected
explicitly; SQLite locking/constraint behavior is never used to claim Fabric support.
"""

import re
import sqlite3
from datetime import UTC, datetime
from unittest.mock import Mock

import pyodbc
import pytest

from fabshuffle.bcdr.catalog import AmbiguousCommit, CatalogConflict
from fabshuffle.bcdr.contracts import (
    AppliedItem,
    ItemIdentity,
    OperationRecord,
    OperationState,
    PayloadDescriptor,
    PayloadPurpose,
    RecoveryMode,
    RecoveryOutcome,
    digest,
)
from fabshuffle.bcdr.payloads import CapturedPayload, IntegrityError, chunk_count
from fabshuffle.bcdr.warehouse_catalog import _DDL, WarehouseCatalog
from tests.test_bcdr_contracts import guid, recovery_set, snapshot


class SqlHarness:
    def __init__(self, path):
        self.path = path
        self.connections = []
        self.fail_statement = None
        self.commit_fault = None
        self.sql = []

    def connect(self):
        connection = TestConnection(self)
        self.connections.append(connection)
        return connection

    def raw(self, sql, parameters=()):
        with sqlite3.connect(self.path) as connection:
            return connection.execute(sql.replace("bcdr.", "bcdr_"), parameters).fetchall()


class TestConnection:
    __test__ = False
    autocommit = False

    def __init__(self, harness):
        self.harness = harness
        self.db = sqlite3.connect(harness.path)
        self.db.execute("BEGIN")
        self.closed = False

    def cursor(self):
        return TestCursor(self)

    def commit(self):
        fault = self.harness.commit_fault
        self.harness.commit_fault = None
        if fault == "lost_after_commit":
            self.db.commit()
            raise pyodbc.OperationalError("08S01", "commit acknowledgement lost")
        if fault == "lost_before_commit":
            raise pyodbc.OperationalError("08S01", "commit connection lost")
        if fault == "conflict":
            self.db.rollback()
            raise pyodbc.OperationalError("40001", "(24556) Snapshot isolation transaction aborted")
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    def close(self):
        self.db.close()
        self.closed = True


class TestCursor:
    __test__ = False

    def __init__(self, connection):
        self.connection = connection
        self.result = []

    def execute(self, sql, *params):
        harness = self.connection.harness
        harness.sql.append((sql, params))
        if harness.fail_statement and harness.fail_statement in sql:
            harness.fail_statement = None
            raise pyodbc.ProgrammingError("42000", "injected statement failure")
        if sql == "CREATE SCHEMA bcdr":
            sql = "CREATE TABLE bcdr_schema_marker (value int)"
        elif "FROM sys.schemas" in sql:
            sql = "SELECT 1 FROM sqlite_master WHERE name = 'bcdr_schema_marker'"
            params = ()
        sql = sql.replace("bcdr.", "bcdr_")
        sql = re.sub(r"varbinary\(max\)", "BLOB", sql, flags=re.I)
        cursor = self.connection.db.execute(sql, params)
        self.result = cursor.fetchall() if cursor.description else []
        return self

    def fetchall(self):
        return self.result


@pytest.fixture
def catalog(tmp_path):
    harness = SqlHarness(tmp_path / "fixture.db")
    config = recovery_set()
    catalog = WarehouseCatalog(harness.connect, config, sleep=Mock())
    catalog.initialize()
    lease = catalog.acquire_controller(guid())
    catalog.transition_mode(lease, RecoveryMode.STANDBY, RecoveryMode.SYNCING, guid())
    return catalog, harness, lease


def publish(catalog, lease, *, parent=None, data=None):
    capture = snapshot(catalog.recovery_set, parent=parent)
    payloads = ()
    if data is not None:
        descriptor = PayloadDescriptor(
            payload_id=guid(), owner=capture.items[0].identity, path="definition/model.json",
            purpose=PayloadPurpose.DEFINITION, media_type="application/json", encoding="utf-8",
            byte_length=len(data), sha256=digest(data), chunk_count=chunk_count(len(data)),
        )
        capture = capture.model_copy(update={
            "items": (capture.items[0].model_copy(update={"payload_ids": (descriptor.payload_id,)}),),
            "payloads": (descriptor,),
        })
        payloads = (CapturedPayload(descriptor, data),)
    catalog.stage_generation(lease, capture, payloads)
    catalog.publish_generation(lease, capture.generation_id, expected_current=parent)
    return capture


def operation(capture, state=OperationState.INTENT, **updates):
    fields = dict(
        operation_id=guid(), kind="create_item", state=state, recorded_at=datetime.now(UTC),
        generation_id=capture.generation_id, source=capture.items[0].identity,
        ownership_evidence="explicit create intent under authorized recovery set",
    )
    fields.update(updates)
    return OperationRecord(**fields)


def test_real_sql_schema_avoids_unsupported_or_unenforced_locking_assumptions():
    ddl = "\n".join(_DDL).lower()
    assert "varbinary(max)" in ddl
    for unsupported in ("nvarchar", "primary key", "unique", "savepoint", "sp_getapplock", "identity("):
        assert unsupported not in ddl


def test_publish_large_payload_preserves_exact_bytes_and_old_generation(catalog):
    catalog, harness, lease = catalog
    first = publish(catalog, lease)
    data = ('{"model":"' + "\u96ea\U0001f680" * 2_400_000 + '"}').encode()
    assert len(data) > 16 * 1024 * 1024
    second = publish(catalog, lease, parent=first.generation_id, data=data)
    restored = catalog.load_generation()
    assert restored.snapshot == second
    assert restored.payload(second.payloads[0].payload_id).data == data
    assert catalog.load_generation(first.generation_id).snapshot == first
    assert catalog.applied_items() == ()
    assert all(connection.closed for connection in harness.connections)


@pytest.mark.parametrize("failure", ["statement", "lost_before_commit", "lost_after_commit"])
def test_publish_failure_reconciles_durable_pointer_without_repeating_mutation(catalog, failure):
    catalog, harness, lease = catalog
    first = publish(catalog, lease)
    next_capture = snapshot(catalog.recovery_set, parent=first.generation_id)
    catalog.stage_generation(lease, next_capture, ())
    if failure == "statement":
        harness.fail_statement = "SET current_generation_id"
        expected_error = pyodbc.Error
    else:
        harness.commit_fault = failure
        expected_error = AmbiguousCommit
    with pytest.raises(expected_error):
        catalog.publish_generation(lease, next_capture.generation_id, expected_current=first.generation_id)
    assert catalog.load_generation(first.generation_id).snapshot == first
    expected = next_capture if failure == "lost_after_commit" else first
    assert catalog.load_generation().snapshot == expected
    status = harness.raw(
        "SELECT status FROM bcdr.generations WHERE generation_id = ?", (next_capture.generation_id,),
    )[0][0]
    assert status == ("complete" if failure == "lost_after_commit" else "sealed")


def test_interrupted_staging_never_replaces_published_point(catalog):
    catalog, harness, lease = catalog
    first = publish(catalog, lease)
    next_capture = snapshot(catalog.recovery_set, parent=first.generation_id)
    harness.fail_statement = "INSERT INTO bcdr.payload_chunks"
    with pytest.raises(pyodbc.Error):
        catalog.stage_generation(lease, next_capture, ())
    with pytest.raises(IntegrityError):
        catalog.inspect_generation(next_capture.generation_id)
    with pytest.raises(IntegrityError):
        catalog.load_generation(next_capture.generation_id)
    assert catalog.load_generation().snapshot == first
    with pytest.raises(CatalogConflict, match="already exists"):
        catalog.stage_generation(lease, next_capture, ())


@pytest.mark.parametrize(
    "corruption", ["chunk", "duplicate_chunk", "descriptor", "orphan", "index", "generation"],
)
def test_corrupt_duplicate_and_incomplete_generation_rejected(catalog, corruption):
    catalog, harness, lease = catalog
    capture = publish(catalog, lease)
    if corruption == "chunk":
        harness.raw("UPDATE bcdr.payload_chunks SET data = ?", (b"bad",))
    elif corruption == "duplicate_chunk":
        harness.raw("INSERT INTO bcdr.payload_chunks SELECT * FROM bcdr.payload_chunks LIMIT 1")
    elif corruption == "descriptor":
        harness.raw("UPDATE bcdr.payloads SET info = ?", (b"{}",))
    elif corruption == "orphan":
        harness.raw(
            "INSERT INTO bcdr.payload_chunks VALUES (?, ?, 0, 0, ?, ?)",
            (capture.generation_id, guid(), digest(b""), b""),
        )
    elif corruption == "index":
        harness.raw("INSERT INTO bcdr.source_items SELECT * FROM bcdr.source_items")
    else:
        harness.raw("INSERT INTO bcdr.generations SELECT * FROM bcdr.generations")
    with pytest.raises(IntegrityError):
        catalog.load_generation()


def test_incomplete_metadata_cannot_be_published(catalog):
    catalog, _harness, lease = catalog
    capture = snapshot(catalog.recovery_set).model_copy(update={"inventory_complete": False})
    catalog.stage_generation(lease, capture, ())
    with pytest.raises(ValueError, match="inventory"):
        catalog.publish_generation(lease, capture.generation_id, expected_current=None)
    assert catalog.state().current_generation_id is None
    assert catalog.generations()[0].status == "sealed"


def test_sql_conflict_retries_are_bounded_and_never_report_default_success(catalog):
    catalog, harness, lease = catalog
    harness.commit_fault = "conflict"
    result = catalog.transition_mode(lease, RecoveryMode.SYNCING, RecoveryMode.STANDBY, guid())
    assert result.mode == RecoveryMode.STANDBY
    catalog._sleep.assert_called_once()
    assert catalog.state().mode == RecoveryMode.STANDBY
    assert harness.raw("SELECT COUNT(*) FROM bcdr.mode_transitions")[0][0] == 2
    harness.fail_statement = "SET revision"
    with pytest.raises(pyodbc.Error, match="injected statement failure"):
        catalog.transition_mode(lease, RecoveryMode.STANDBY, RecoveryMode.ENABLING_RECOVERY, guid())
    assert catalog.state().mode == RecoveryMode.STANDBY


def test_mode_race_and_controller_fencing_fail_closed(catalog):
    catalog, _harness, lease = catalog
    with pytest.raises(CatalogConflict, match="owned"):
        catalog.acquire_controller(guid())
    catalog.transition_mode(lease, RecoveryMode.SYNCING, RecoveryMode.STANDBY, guid())
    catalog.transition_mode(lease, RecoveryMode.STANDBY, RecoveryMode.ENABLING_RECOVERY, guid())
    with pytest.raises(CatalogConflict):
        catalog.transition_mode(lease, RecoveryMode.STANDBY, RecoveryMode.SYNCING, guid())
    with pytest.raises(CatalogConflict):
        catalog.stage_generation(lease, snapshot(catalog.recovery_set), ())
    replacement = catalog.takeover_controller(
        guid(), expected_controller_id=lease.controller_id, expected_epoch=lease.epoch,
        fencing_evidence="operator stopped old process and verified no controller jobs", operation_id=guid(),
    )
    assert replacement.epoch == lease.epoch + 1
    with pytest.raises(CatalogConflict, match="ownership"):
        catalog.transition_mode(lease, RecoveryMode.ENABLING_RECOVERY, RecoveryMode.ACTIVE_RECOVERY, guid())


def test_pending_operation_prevents_mode_change_and_has_immutable_intent_history(catalog):
    catalog, _harness, lease = catalog
    capture = publish(catalog, lease)
    intent = operation(capture)
    catalog.begin_operation(lease, intent)
    with pytest.raises(CatalogConflict, match="Reconcile"):
        catalog.transition_mode(lease, RecoveryMode.SYNCING, RecoveryMode.STANDBY, guid())
    ambiguous = intent.model_copy(update={"state": OperationState.AMBIGUOUS, "message": "Service timeout"})
    catalog.record_operation(lease, ambiguous)
    assert catalog.pending_operations() == (ambiguous,)
    with pytest.raises(CatalogConflict, match="exists"):
        catalog.begin_operation(lease, intent)
    failed = ambiguous.model_copy(update={"state": OperationState.FAILED, "error_code": "ActualServiceCode",
                                         "message": "Actual service explanation"})
    catalog.record_operation(lease, failed)
    assert catalog.operation_history(intent.operation_id) == (intent, ambiguous, failed)
    assert not catalog.pending_operations()
    catalog.transition_mode(lease, RecoveryMode.SYNCING, RecoveryMode.STANDBY, guid())
    catalog.transition_mode(lease, RecoveryMode.STANDBY, RecoveryMode.PARKING, guid())
    with pytest.raises(CatalogConflict):
        catalog.begin_operation(lease, operation(capture))
    with pytest.raises(CatalogConflict):
        catalog.transition_mode(lease, RecoveryMode.PARKING, RecoveryMode.ENABLING_RECOVERY, guid())


def test_applied_generation_is_separate_and_requires_matching_successful_outcome(catalog):
    catalog, _harness, lease = catalog
    capture = publish(catalog, lease)
    target = ItemIdentity(tenant_id=catalog.recovery_set.tenant_id, workspace_id=guid(), item_id=guid())
    intent = operation(capture, target=target)
    catalog.begin_operation(lease, intent)
    applied = AppliedItem(
        source=intent.source, target=target, capture_generation_id=capture.generation_id,
        applied_at=datetime.now(UTC), definition_sha256="0" * 64, properties_sha256="1" * 64,
        target_observed_sha256="2" * 64, outcome=RecoveryOutcome.RESTORED_STOPPED,
        operation_id=intent.operation_id,
    )
    with pytest.raises(CatalogConflict, match="successful matching"):
        catalog.record_applied(lease, applied)
    catalog.record_operation(lease, intent.model_copy(update={"state": OperationState.SUCCEEDED}))
    catalog.record_applied(lease, applied)
    next_capture = publish(catalog, lease, parent=capture.generation_id)
    assert catalog.state().current_generation_id == next_capture.generation_id
    assert catalog.applied_items() == (applied,)


def test_catalog_outage_cannot_authorize_intent_and_duplicate_singleton_is_rejected(catalog):
    catalog, harness, lease = catalog
    capture = publish(catalog, lease)
    harness.fail_statement = "SELECT singleton"
    with pytest.raises(pyodbc.Error):
        catalog.begin_operation(lease, operation(capture))
    assert not catalog.operations()
    harness.raw("INSERT INTO bcdr.control SELECT * FROM bcdr.control")
    with pytest.raises(IntegrityError, match="found 2"):
        catalog.state()


def test_endpoint_rejects_odbc_option_injection_before_getting_any_token():
    config = recovery_set()
    tokens = Mock()
    for server, database in [
        ("evil.example.com", "catalog"),
        ("example.datawarehouse.fabric.microsoft.com", "catalog;TrustServerCertificate=yes"),
        ("example.datawarehouse.fabric.microsoft.com", "master"),
    ]:
        with pytest.raises(ValueError):
            WarehouseCatalog.from_endpoint(server, database, tokens, config)
    tokens.sql_token.assert_not_called()


def test_open_existing_catalog_loads_configuration_from_sql_not_bootstrap(catalog, monkeypatch):
    catalog, harness, _lease = catalog
    tokens = Mock()
    tokens.principal.tenant_id = catalog.recovery_set.tenant_id
    connector = Mock(side_effect=harness.connect)
    monkeypatch.setattr("fabshuffle.bcdr.warehouse_catalog.connect", lambda *_args: connector())
    opened = WarehouseCatalog.open_from_endpoint(
        "control.datawarehouse.fabric.microsoft.com", catalog.recovery_set.control_warehouse.item_id,
        tokens, expected_recovery_set_id=catalog.recovery_set.recovery_set_id,
    )
    assert opened.recovery_set == catalog.recovery_set
    assert opened.state() == catalog.state()
    assert all(connection.closed for connection in harness.connections)
    with pytest.raises(IntegrityError, match="bootstrap"):
        WarehouseCatalog.open_from_endpoint(
            "control.datawarehouse.fabric.microsoft.com", catalog.recovery_set.control_warehouse.item_id,
            tokens, expected_recovery_set_id=guid(),
        )
    with pytest.raises(IntegrityError, match="bootstrap"):
        WarehouseCatalog.open_from_endpoint(
            "control.datawarehouse.fabric.microsoft.com", guid(), tokens,
            expected_recovery_set_id=catalog.recovery_set.recovery_set_id,
        )


def test_coordinator_records_have_cas_revision_and_durable_history(catalog):
    catalog, harness, lease = catalog
    assert catalog.get_record("writer-epochs", "business-a") is None
    first = catalog.put_record(lease, "writer-epochs", "business-a", {"epoch": 1}, expected_revision=None)
    assert first.revision == 1
    assert catalog.get_record("writer-epochs", "business-a") == first
    with pytest.raises(CatalogConflict, match="changed"):
        catalog.put_record(lease, "writer-epochs", "business-a", {"epoch": 99}, expected_revision=None)
    second = catalog.put_record(lease, "writer-epochs", "business-a", {"epoch": 2}, expected_revision=1)
    assert second.revision == 2
    assert catalog.list_records("writer-epochs") == (second,)
    assert harness.raw("SELECT revision FROM bcdr.coordinator_history ORDER BY revision") == [(1,), (2,)]
    catalog.transition_mode(lease, RecoveryMode.SYNCING, RecoveryMode.STANDBY, guid())
    catalog.transition_mode(lease, RecoveryMode.STANDBY, RecoveryMode.PARKING, guid())
    with pytest.raises(CatalogConflict):
        catalog.put_record(lease, "writer-epochs", "business-a", {"epoch": 3}, expected_revision=2)


def test_sql_conflict_exhaustion_preserves_state_and_original_service_error(catalog, monkeypatch):
    catalog, _harness, lease = catalog

    def conflict(connection):
        connection.db.rollback()
        raise pyodbc.OperationalError("40001", "(24706) Service snapshot conflict text")

    monkeypatch.setattr(TestConnection, "commit", conflict)
    with pytest.raises(pyodbc.Error, match=r"24706.*Service snapshot conflict text"):
        catalog.transition_mode(lease, RecoveryMode.SYNCING, RecoveryMode.STANDBY, guid())
    assert catalog._sleep.call_count == 2
    assert catalog.state().mode == RecoveryMode.SYNCING


def test_generation_listing_keeps_published_and_partial_points_distinct(catalog):
    catalog, harness, lease = catalog
    first = publish(catalog, lease)
    partial = snapshot(catalog.recovery_set, parent=first.generation_id)
    harness.fail_statement = "INSERT INTO bcdr.payload_chunks"
    with pytest.raises(pyodbc.Error):
        catalog.stage_generation(lease, partial, ())
    assert {row.generation_id: row.status for row in catalog.generations()} == {
        first.generation_id: "complete", partial.generation_id: "staging",
    }


def test_failback_capture_uses_recovery_scope_without_changing_source_authority(catalog):
    catalog, _harness, lease = catalog
    original = publish(catalog, lease)
    for expected, desired in (
        (RecoveryMode.SYNCING, RecoveryMode.STANDBY),
        (RecoveryMode.STANDBY, RecoveryMode.ENABLING_RECOVERY),
        (RecoveryMode.ENABLING_RECOVERY, RecoveryMode.ACTIVE_RECOVERY),
        (RecoveryMode.ACTIVE_RECOVERY, RecoveryMode.FAILING_BACK),
    ):
        catalog.transition_mode(lease, expected, desired, guid())
    recovery = snapshot(catalog.recovery_set, parent=original.generation_id)
    recovery = recovery.model_copy(update={
        "capture_kind": "recovery",
        "workspaces": (recovery.workspaces[0].model_copy(update={
            "capacity_id": catalog.recovery_set.target_capacity_ids[0],
        }),),
    })
    catalog.stage_failback_generation(lease, recovery, ())
    with pytest.raises(ValueError, match="publish_failback_generation"):
        catalog.publish_generation(lease, recovery.generation_id, expected_current=original.generation_id)
    catalog.publish_failback_generation(
        lease, recovery.generation_id, failover_generation_id=original.generation_id,
    )
    assert catalog.load_generation(recovery.generation_id).snapshot == recovery
    assert catalog.load_generation().snapshot == original
    assert catalog.state().current_generation_id == original.generation_id
    with pytest.raises(CatalogConflict, match="mode"):
        catalog.stage_generation(lease, snapshot(catalog.recovery_set), ())


def test_failback_capture_does_not_allow_unrelated_or_control_workspace(catalog):
    catalog, _harness, _lease = catalog
    recovery = snapshot(catalog.recovery_set).model_copy(update={"capture_kind": "recovery"})
    with pytest.raises(ValueError, match="out-of-scope"):
        recovery.require_publishable(catalog.recovery_set)
    control = recovery.workspaces[0].model_copy(update={
        "identity": catalog.recovery_set.control_workspace,
        "capacity_id": catalog.recovery_set.target_capacity_ids[0],
    })
    with pytest.raises(ValueError, match="control workspace"):
        recovery.model_copy(update={"workspaces": (control,)}).require_publishable(catalog.recovery_set)


@pytest.mark.parametrize("target_kind", ["control", "foreign", "source"])
def test_invalid_business_target_rejected_before_intent_and_on_late_observation(catalog, target_kind):
    catalog, _harness, lease = catalog
    capture = publish(catalog, lease)
    target = {
        "control": catalog.recovery_set.control_warehouse,
        "foreign": ItemIdentity(tenant_id=guid(), workspace_id=guid(), item_id=guid()),
        "source": capture.items[0].identity.model_copy(update={"item_id": guid()}),
    }[target_kind]
    intent = operation(capture, target=target)
    with pytest.raises(ValueError):
        catalog.begin_operation(lease, intent)
    assert not catalog.pending_operations()
    intent = operation(capture)
    catalog.begin_operation(lease, intent)
    with pytest.raises(ValueError):
        catalog.record_operation(
            lease, intent.model_copy(update={"target": target, "state": OperationState.SUCCEEDED}),
        )
    assert catalog.pending_operations() == (intent,)


def test_parking_intents_are_scoped_and_final_outcome_reconciles_after_resume(catalog):
    catalog, _harness, lease = catalog
    catalog.transition_mode(lease, RecoveryMode.SYNCING, RecoveryMode.STANDBY, guid())
    catalog.transition_mode(lease, RecoveryMode.STANDBY, RecoveryMode.PARKING, guid())
    intent = OperationRecord(
        operation_id=guid(), kind="suspend_capacity", state=OperationState.INTENT,
        recorded_at=datetime.now(UTC), capacity_id=catalog.recovery_set.target_capacity_ids[0],
        ownership_evidence="explicit dedicated recovery capacity; serving inventory drained",
    )
    with pytest.raises(ValueError, match="configured recovery capacity"):
        catalog.begin_parking_operation(lease, intent.model_copy(update={"capacity_id": guid()}))
    catalog.begin_parking_operation(lease, intent)
    with pytest.raises(CatalogConflict, match="Reconcile"):
        catalog.transition_mode(lease, RecoveryMode.PARKING, RecoveryMode.STANDBY, guid())
    # The bootstrap controller resumes SQL first, then supplies its persisted ARM observation.
    catalog.record_operation(lease, intent.model_copy(update={"state": OperationState.SUCCEEDED}))
    catalog.transition_mode(lease, RecoveryMode.PARKING, RecoveryMode.STANDBY, guid())
    assert catalog.state().mode == RecoveryMode.STANDBY


def test_drained_controller_can_release_between_active_recovery_requests(catalog):
    catalog, _harness, lease = catalog
    for expected, desired in (
        (RecoveryMode.SYNCING, RecoveryMode.STANDBY),
        (RecoveryMode.STANDBY, RecoveryMode.ENABLING_RECOVERY),
        (RecoveryMode.ENABLING_RECOVERY, RecoveryMode.ACTIVE_RECOVERY),
    ):
        catalog.transition_mode(lease, expected, desired, guid())
    catalog.release_controller(lease)
    assert catalog.state().controller_id is None
    assert catalog.state().mode == RecoveryMode.ACTIVE_RECOVERY
    replacement = catalog.acquire_controller(guid())
    assert replacement.epoch == lease.epoch + 1
    with pytest.raises(CatalogConflict):
        catalog.stage_generation(replacement, snapshot(catalog.recovery_set), ())
    with pytest.raises(CatalogConflict):
        catalog.transition_mode(replacement, RecoveryMode.ACTIVE_RECOVERY, RecoveryMode.PARKING, guid())
