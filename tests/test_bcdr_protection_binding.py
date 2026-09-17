"""Production provider binding, immutable configuration and durable mutation boundaries."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import pytest
from pydantic import ValidationError

from fabshuffle.bcdr import protection_binding as binding
from fabshuffle.bcdr import protection_cosmos as cosmos
from fabshuffle.bcdr import protection_kql as kql
from fabshuffle.bcdr.catalog import CapturedGeneration, CatalogConflict, CatalogError
from fabshuffle.bcdr.contracts import (
    CaptureSnapshot,
    CatalogDocument,
    ControllerLease,
    ItemIdentity,
    ItemRecord,
    OperationRecord,
    OperationState,
    RecoveryMode,
    WorkspaceIdentity,
    WorkspaceRecord,
    canonical_json,
    digest,
)
from fabshuffle.bcdr.protection import ProtectedLocation, ProtectionAssessment, ProtectionError
from fabshuffle.run import CancelledError
from tests.test_bcdr_protection import (
    DOCUMENTS,
    DST,
    EVIDENCE,
    LIMITS,
    NOW,
    SRC,
    TOKENS,
    FakeContainer,
    FakeCosmos,
)
from tests.test_bcdr_protection import location as location
from tests.test_bcdr_protection import prepared as prepared
from tests.test_bcdr_protection import sql_capture as sql_capture

SOURCE = ItemIdentity(
    tenant_id=SRC.identity.tenant_id, workspace_id=SRC.identity.workspace_id, item_id=SRC.identity.item_id
)
TARGET = ItemIdentity(
    tenant_id=DST.identity.tenant_id, workspace_id=DST.identity.workspace_id, item_id=DST.identity.item_id
)


class Catalog:
    def __init__(self):
        self.records = {}
        self.journal = {}
        self.available = True

    def check(self):
        if not self.available:
            raise CatalogError("Warehouse unavailable")

    def get_record(self, namespace, key):
        self.check()
        return self.records.get((namespace, key))

    def put_record(self, lease, namespace, key, document, *, expected_revision):
        self.check()
        previous = self.records.get((namespace, key))
        if (previous.revision if previous else None) != expected_revision:
            raise CatalogConflict("Configuration revision changed")
        record = CatalogDocument(
            namespace=namespace, key=key, document=document, revision=previous.revision + 1 if previous else 1
        )
        self.records[namespace, key] = record
        return record

    def pending_operations(self):
        self.check()
        return [op for op in self.journal.values() if op.state != OperationState.SUCCEEDED]

    def record_operation(self, lease, operation):
        self.check()
        self.journal[operation.operation_id] = operation


class Runtime:
    """Strict test double for the coordinator-owned DurableRuntime protocol."""

    def __init__(self):
        self.catalog = Catalog()
        self.lease = ControllerLease(recovery_set_id=str(uuid4()), controller_id=str(uuid4()), epoch=1)
        self.mode = RecoveryMode.STANDBY
        self.current_operation = None
        self.cancelled = False
        self.effects = 0
        self.fences = 0

    def fence(self):
        self.fences += 1
        self.catalog.check()
        if self.cancelled:
            raise CancelledError("Operator cancelled")

    def require_lease(self):
        return self.lease

    def effect(self, kind, key, action, *, generation_id, source, target):
        self.fence()
        identifier = str(uuid5(UUID(self.lease.recovery_set_id), f"{kind}:{key}"))
        previous = self.catalog.journal.get(identifier)
        if previous:
            assert previous.state == OperationState.SUCCEEDED
            return json.loads(previous.message)
        self.effects += 1
        operation = OperationRecord(
            operation_id=identifier,
            kind=kind,
            state=OperationState.INTENT,
            recorded_at=datetime.now(UTC),
            generation_id=generation_id,
            source=source,
            target=target,
            ownership_evidence="test-controller",
        )
        self.catalog.record_operation(self.lease, operation)
        self.current_operation = operation
        try:
            result = action()
        finally:
            self.current_operation = None
        self.catalog.record_operation(
            self.lease,
            operation.model_copy(
                update={
                    "state": OperationState.SUCCEEDED,
                    "message": canonical_json(result).decode(),
                }
            ),
        )
        return result


def generation(record, *, item_type="CosmosDBDatabase"):
    workspace = WorkspaceRecord(
        identity=WorkspaceIdentity(tenant_id=SOURCE.tenant_id, workspace_id=SOURCE.workspace_id),
        capacity_id=str(uuid4()),
        display_name="Source",
        captured_at=NOW,
        inventory_complete=True,
    )
    item = ItemRecord(
        identity=SOURCE,
        item_type=item_type,
        display_name="Orders",
        captured_at=NOW,
        api_version="v1",
        capture_complete=True,
    )
    snapshot = CaptureSnapshot(
        recovery_set_id=str(uuid4()),
        generation_id=str(uuid4()),
        captured_at=NOW,
        workspaces=(workspace,),
        items=(item,),
        protections=(record,) if record else (),
        inventory_complete=True,
    )
    return CapturedGeneration(snapshot, ()), item


class Fabric:
    def __init__(self, runtime, *, item_type="CosmosDBDatabase"):
        self.runtime = runtime
        self.calls = []
        self.response = {
            "id": TARGET.item_id,
            "workspaceId": TARGET.workspace_id,
            "type": item_type,
            "properties": {"serverFqdn": DST.sql_server, "databaseName": DST.database},
        }

    def get(self, path):
        assert self.runtime.current_operation is not None, "Commit intent before provider service access."
        assert TARGET.workspace_id in path and SOURCE.workspace_id not in path
        self.runtime.fence()
        self.calls.append(path)
        return self.response


@pytest.fixture
def configured(monkeypatch, tmp_path):
    root = tmp_path / "exports"
    root.mkdir()
    location = ProtectedLocation(root, "eastus", "westus", "storage-approval")
    monkeypatch.setattr(cosmos, "cosmos_client", lambda *_: FakeCosmos(FakeContainer(documents=DOCUMENTS)))
    descriptor = cosmos.capture_cosmos(
        source=SRC,
        tokens=TOKENS,
        location=location,
        consistency=EVIDENCE,
        limits=LIMITS,
    )
    configuration = binding.ProtectionConfiguration(
        provider="cosmos",
        descriptor=descriptor,
        storage=binding.ProtectionStorage(
            source_region="eastus", storage_region="westus", approval_ref="storage-approval"
        ),
        max_age_seconds=86400,
        target_approval_ref="target-approval",
    )
    request = binding.ConfigureProtectionRequest(source=SOURCE, configuration=configuration)
    runtime = Runtime()
    record = binding.configure_protection(runtime, request, protected_root=root, limits=LIMITS)
    return SimpleNamespace(runtime=runtime, request=request, record=record, root=root, descriptor=descriptor)


def binder(configured, tmp_path, *, root=True):
    client = Fabric(configured.runtime)
    recovery = binding.build_data_recovery(
        client=client,
        tokens=TOKENS,
        protected_root=configured.root if root else None,
        scratch=tmp_path,
        limits=LIMITS,
    )
    return recovery, client


def test_configuration_is_pinned_and_roundtrips(configured):
    config = configured.request.configuration
    reference = digest(canonical_json(config))
    assert configured.record.artifact_reference == configured.record.sha256 == reference
    row = configured.runtime.catalog.get_record("protection-config", reference)
    assert binding.ProtectionConfiguration.model_validate(row.document) == config
    captured, item = generation(configured.record)
    assert binding.capture_protection_records((item,), configured.runtime.catalog) == (configured.record,)
    assert not binding.owns_schema(captured, item, configured.runtime.catalog)


@pytest.mark.parametrize(
    "change",
    [
        {"protected_root": "/etc"},
        {"provider": "python"},
        {"callback": "os.system"},
        {"max_age_seconds": True},
        {"max_age_seconds": 0},
    ],
)
def test_request_forbids_arbitrary_paths_callbacks_and_bad_types(configured, change):
    body = configured.request.configuration.model_dump(mode="json") | change
    with pytest.raises(ValidationError):
        binding.ProtectionConfiguration.model_validate(body)


def test_nested_descriptor_rejects_unknown_secret_fields(configured):
    body = configured.request.configuration.model_dump(mode="json")
    body["descriptor"]["manifest"]["password"] = "DO_NOT_LOG_THIS_VALUE"
    with pytest.raises(ValidationError) as failure:
        binding.ProtectionConfiguration.model_validate(body)
    assert "DO_NOT_LOG_THIS_VALUE" not in str(failure.value)


def test_configuration_rejects_wrong_source_provider_and_region(configured):
    with pytest.raises(ValidationError):
        binding.ConfigureProtectionRequest(source=TARGET, configuration=configured.request.configuration)
    body = configured.request.configuration.model_dump(mode="json")
    with pytest.raises(ValidationError):
        binding.ProtectionConfiguration.model_validate(body | {"provider": "sql"})
    body["storage"]["storage_region"] = "eastus"
    with pytest.raises(ValidationError):
        binding.ProtectionConfiguration.model_validate(body)


def test_configuration_cas_and_active_recovery_restriction(configured):
    runtime = configured.runtime
    with pytest.raises(CatalogConflict):
        binding.configure_protection(runtime, configured.request, protected_root=configured.root)
    updated = configured.request.model_copy(update={"expected_revision": 1})
    binding.configure_protection(runtime, updated, protected_root=configured.root)
    assert runtime.catalog.get_record("protection-selection", SOURCE.key).revision == 2
    runtime.mode = RecoveryMode.ACTIVE_RECOVERY
    with pytest.raises(ProtectionError, match="standby"):
        binding.configure_protection(runtime, updated, protected_root=configured.root)


def test_selection_updates_do_not_retarget_an_existing_generation(configured):
    old = configured.request.configuration
    updated = old.model_copy(update={"max_age_seconds": 43200})
    new_record = binding.configure_protection(
        configured.runtime,
        binding.ConfigureProtectionRequest(source=SOURCE, configuration=updated, expected_revision=1),
        protected_root=configured.root,
    )
    assert new_record.artifact_reference != configured.record.artifact_reference
    assert binding._load_config(configured.record, configured.runtime.catalog) == old
    _, item = generation(configured.record)
    assert binding.capture_protection_records((item,), configured.runtime.catalog) == (new_record,)


def test_generation_rejects_changed_config_and_mismatched_kind(configured, tmp_path):
    runtime = configured.runtime
    reference = configured.record.artifact_reference
    row = runtime.catalog.records["protection-config", reference]
    runtime.catalog.records["protection-config", reference] = row.model_copy(
        update={
            "document": row.document | {"max_age_seconds": 1},
        }
    )
    recovery, client = binder(configured, tmp_path)
    captured, item = generation(configured.record)
    with pytest.raises(ProtectionError, match="integrity"):
        recovery.restore(captured, item, TARGET, runtime)
    assert not client.calls and runtime.effects == 0


def test_restore_uses_real_cosmos_provider_with_target_only_rest_and_durable_intent(
    configured,
    monkeypatch,
    tmp_path,
):
    runtime = configured.runtime

    class Target(FakeContainer):
        def create_item(self, body):
            assert runtime.current_operation is not None
            assert runtime.current_operation.kind == "data-restore"
            assert runtime.current_operation.source == SOURCE and runtime.current_operation.target == TARGET
            assert runtime.fences > len(self.writes)
            return super().create_item(body)

    target = Target()

    def destination_only(endpoint, _):
        assert endpoint == DST.endpoint
        return FakeCosmos(target)

    monkeypatch.setattr(cosmos, "cosmos_client", destination_only)
    recovery, client = binder(configured, tmp_path)
    captured, item = generation(configured.record)
    ready, warnings = recovery.restore(captured, item, TARGET, runtime)
    assert ready and len(target.writes) == 2
    assert client.calls == [f"workspaces/{TARGET.workspace_id}/cosmosDbDatabases/{TARGET.item_id}"]
    operation = next(iter(runtime.catalog.journal.values()))
    assert (
        operation.state == OperationState.SUCCEEDED
        and operation.generation_id == captured.snapshot.generation_id
    )
    assert json.loads(operation.message)["ready_for_cutover"] is False
    assert "TTL OFF" in warnings[0]
    ready, warnings = recovery.restore(captured, item, TARGET, runtime)
    assert not ready and "previously" in warnings[-1]
    assert len(target.writes) == 2 and runtime.effects == 1


@pytest.mark.parametrize("failure", ["catalog", "cancel"])
def test_provider_stops_between_writes_on_catalog_loss_or_cancellation(
    configured,
    monkeypatch,
    tmp_path,
    failure,
):
    runtime = configured.runtime

    class Target(FakeContainer):
        def create_item(self, body):
            result = super().create_item(body)
            if failure == "catalog":
                runtime.catalog.available = False
            else:
                runtime.cancelled = True
            return result

    target = Target()
    monkeypatch.setattr(cosmos, "cosmos_client", lambda *_: FakeCosmos(target))
    recovery, client = binder(configured, tmp_path)
    captured, item = generation(configured.record)
    with pytest.raises(CatalogError if failure == "catalog" else CancelledError):
        recovery.restore(captured, item, TARGET, runtime)
    assert len(target.writes) == 1 and len(client.calls) == 1
    assert next(iter(runtime.catalog.journal.values())).state == OperationState.INTENT


def test_partial_provider_failure_is_ambiguous_named_and_redacted(configured, monkeypatch, tmp_path):
    def failure(*args, **kwargs):
        kwargs["cancel"]()
        raise ProtectionError("ServiceCode42: rejected access_token=DO_NOT_LOG")

    monkeypatch.setattr(binding, "restore_cosmos", failure)
    recovery, _ = binder(configured, tmp_path)
    captured, item = generation(configured.record)
    ready, warnings = recovery.restore(captured, item, TARGET, configured.runtime)
    assert not ready and "Orders" in warnings[0] and "ServiceCode42" in warnings[0]
    assert "DO_NOT_LOG" not in warnings[0]
    operation = next(iter(configured.runtime.catalog.journal.values()))
    assert operation.state == OperationState.AMBIGUOUS and "DO_NOT_LOG" not in operation.message
    with pytest.raises(CatalogConflict, match="pending"):
        recovery.restore(captured, item, TARGET, configured.runtime)


def test_destination_identity_is_not_adopted_by_name(configured, tmp_path):
    recovery, client = binder(configured, tmp_path)
    client.response["id"] = SOURCE.item_id
    captured, item = generation(configured.record)
    ready, warnings = recovery.restore(captured, item, TARGET, configured.runtime)
    assert not ready and "target identity" in warnings[0]
    assert next(iter(configured.runtime.catalog.journal.values())).state == OperationState.AMBIGUOUS


def test_missing_root_or_input_does_not_start_operation(configured, tmp_path):
    recovery, client = binder(configured, tmp_path, root=False)
    captured, item = generation(configured.record)
    ready, warnings = recovery.restore(captured, item, TARGET, configured.runtime)
    assert not ready and "server" in warnings[0]
    captured, item = generation(None)
    ready, warnings = recovery.restore(captured, item, TARGET, configured.runtime)
    assert not ready and "Orders" in warnings[0]
    assert not client.calls and configured.runtime.effects == 0


def test_corrupt_data_is_per_item_unready_without_pending_mutation(configured, tmp_path):
    artifact = configured.descriptor.manifest.artifacts[0]
    path = configured.root / artifact.path
    path.write_bytes(b"corrupt")
    recovery, client = binder(configured, tmp_path)
    captured, item = generation(configured.record)
    ready, warnings = recovery.restore(captured, item, TARGET, configured.runtime)
    assert not ready and "checksum" in warnings[0]
    assert not client.calls and not configured.runtime.catalog.pending_operations()


def test_binder_refuses_wrong_provider_success_target(configured, monkeypatch, tmp_path):
    monkeypatch.setattr(
        binding,
        "restore_cosmos",
        lambda *_a, **_k: ProtectionAssessment(
            "restored_stopped",
            data_ready=True,
            target=SRC.identity,
        ),
    )
    recovery, _ = binder(configured, tmp_path)
    captured, item = generation(configured.record)
    ready, warnings = recovery.restore(captured, item, TARGET, configured.runtime)
    assert not ready and "different target" in warnings[0]


def test_configuring_with_missing_catalog_does_not_publish_selection(configured):
    configured.runtime.catalog.available = False
    with pytest.raises(CatalogError):
        binding.configure_protection(
            configured.runtime,
            configured.request,
            protected_root=configured.root,
        )


def test_config_and_capture_helpers_do_not_need_source_client(configured):
    _, item = generation(configured.record)
    records = binding.capture_protection_records((item,), configured.runtime.catalog)
    assert records[0].item == SOURCE
    other = item.model_copy(update={"identity": TARGET})
    assert binding.capture_protection_records((other,), configured.runtime.catalog) == ()


def test_sql_owns_schema_and_discovers_explicit_target_catalog(sql_capture, location, monkeypatch, tmp_path):
    descriptor = sql_capture[0]
    configuration = binding.ProtectionConfiguration(
        provider="sql",
        descriptor=descriptor,
        storage=binding.ProtectionStorage(
            source_region=location.source_region,
            storage_region=location.storage_region,
            approval_ref=location.approval_ref,
        ),
        max_age_seconds=86400,
        target_approval_ref="target-approval",
    )
    runtime = Runtime()
    record = binding.configure_protection(
        runtime,
        binding.ConfigureProtectionRequest(source=SOURCE, configuration=configuration),
        protected_root=location.root,
        limits=LIMITS,
    )
    captured, item = generation(record, item_type="SQLDatabase")
    assert binding.owns_schema(captured, item, runtime.catalog)
    client = Fabric(runtime, item_type="SQLDatabase")
    client.response["properties"]["serverFqdn"] += ",1433"
    calls = []

    def restore(value, **kwargs):
        assert runtime.current_operation is not None
        assert value == descriptor
        kwargs["cancel"]()
        calls.append(kwargs)
        return ProtectionAssessment("restored_stopped", data_ready=True, target=DST.identity)

    monkeypatch.setattr(binding, "restore_sql", restore)
    recovery = binding.build_data_recovery(
        client=client,
        tokens=TOKENS,
        protected_root=location.root,
        scratch=tmp_path,
        limits=LIMITS,
    )
    ready, _ = recovery.restore(captured, item, TARGET, runtime)
    assert ready and calls[0]["target"] == DST
    assert client.calls == [f"workspaces/{TARGET.workspace_id}/sqlDatabases/{TARGET.item_id}"]


def test_default_kql_binding_defers_unresolved_opaque_inputs(prepared, tmp_path):
    runtime = Runtime()
    configuration = binding.ProtectionConfiguration(
        provider="kql",
        descriptor=prepared,
        max_age_seconds=86400,
        target_approval_ref=prepared.target_approval_ref,
    )
    record = binding.configure_protection(
        runtime,
        binding.ConfigureProtectionRequest(source=SOURCE, configuration=configuration),
        protected_root=None,
    )
    captured, item = generation(record, item_type="KQLDatabase")
    client = Fabric(runtime)
    recovery = binding.build_data_recovery(
        client=client, tokens=TOKENS, protected_root=None, scratch=tmp_path
    )
    ready, warnings = recovery.restore(captured, item, TARGET, runtime)
    assert not ready and "eventhub-2" in warnings[-1] and "Orders" in warnings[-1]
    assert not client.calls and not runtime.effects


class KqlFabric:
    def __init__(self, runtime):
        self.runtime = runtime
        self.calls = []
        self.region = "West US"
        self.database_type = "ReadWrite"
        self.shortcut_values = []
        self.query_uri = DST.endpoint
        self.name = DST.database
        self.identity = TARGET
        self.failure = None

    def tenant_id(self):
        return TARGET.tenant_id

    def get(self, path):
        self.runtime.fence()
        assert self.runtime.current_operation is not None
        assert SOURCE.workspace_id not in path, "No source control-plane read at outage."
        self.calls.append(path)
        if self.failure:
            raise self.failure
        if "/kqlDatabases/" not in path:
            return {
                "id": TARGET.workspace_id,
                "capacityRegion": self.region,
                "capacityAssignmentProgress": "Completed",
            }
        return {
            "id": self.identity.item_id,
            "workspaceId": self.identity.workspace_id,
            "type": "KQLDatabase",
            "displayName": self.name,
            "properties": {
                "queryServiceUri": self.query_uri,
                "databaseType": self.database_type,
            },
        }

    def list_all(self, path):
        self.runtime.fence()
        assert path == f"workspaces/{TARGET.workspace_id}/items/{TARGET.item_id}/shortcuts"
        self.calls.append(path)
        return self.shortcut_values


class KqlClient:
    def __init__(self, runtime):
        self.runtime = runtime
        self.rows = 12
        self.physical = True
        self.queries = []
        self.closed = 0
        self.after_query = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed += 1

    def execute_mgmt(self, database, command, *, properties):
        assert self.runtime.current_operation is not None
        assert database == DST.database
        assert command == '.show table ["events"] details | project TableName'
        assert properties.get_option("request_readonly_hardline", None) is True
        return SimpleNamespace(primary_results=[[{"TableName": "events"}] if self.physical else []])

    def execute_query(self, database, command, *, properties):
        assert database == DST.database and command == '["events"] | count'
        for option in (
            "request_readonly_hardline",
            "request_remote_entities_disabled",
            "request_external_data_disabled",
            "request_external_table_disabled",
            "request_callout_disabled",
            "request_sandboxed_execution_disabled",
            "request_block_row_level_security",
            "request_impersonation_disabled",
        ):
            assert properties.get_option(option, None) is True
        self.queries.append(command)
        if self.after_query:
            self.after_query()
        return SimpleNamespace(primary_results=[[[self.rows]]])


@pytest.fixture
def materialized(prepared, tmp_path, monkeypatch):
    runtime = Runtime()
    descriptor = replace(prepared, inputs=(kql.KqlInput("materialized-events", "input-access-check"),))
    configuration = binding.ProtectionConfiguration(
        provider="kql",
        descriptor=descriptor,
        max_age_seconds=86400,
        target_approval_ref=descriptor.target_approval_ref,
        kql_materialized_inputs=(
            binding.KqlMaterializedInput(
                resource_ref="materialized-events",
                item=TARGET,
                tables=descriptor.tables,
            ),
        ),
    )
    request = binding.ConfigureProtectionRequest(source=SOURCE, configuration=configuration)
    record = binding.configure_protection(runtime, request, protected_root=None)
    captured, item = generation(record, item_type="KQLDatabase")
    fabric = KqlFabric(runtime)
    sdk = KqlClient(runtime)

    def client(endpoint, _principal):
        assert endpoint == DST.endpoint, "Never query a primary-supplied or user-arbitrary endpoint."
        runtime.fence()
        return sdk

    monkeypatch.setattr(kql, "kql_client", client)
    recovery = binding.build_data_recovery(
        client=fabric,
        tokens=TOKENS,
        protected_root=None,
        scratch=tmp_path,
        limits=LIMITS,
    )
    return SimpleNamespace(
        runtime=runtime,
        request=request,
        record=record,
        generation=captured,
        item=item,
        fabric=fabric,
        sdk=sdk,
        recovery=recovery,
    )


def test_materialized_kql_executes_authenticated_destination_only_data_checks(materialized):
    case = materialized
    assert binding.prepared_target(case.generation, case.item, case.runtime.catalog) == TARGET
    assert case.record.qualification.value == "unverified", "Nomination is not metadata qualification."
    ready, warnings = case.recovery.restore(case.generation, case.item, TARGET, case.runtime)
    assert ready and len(case.sdk.queries) == 2
    assert all(op.kind == "data-validate" for op in case.runtime.catalog.journal.values())
    assert all(
        json.loads(op.message)["ready_for_cutover"] is False for op in case.runtime.catalog.journal.values()
    )
    assert any("does not certify" in warning for warning in warnings)
    assert not case.runtime.catalog.pending_operations()


def test_materialized_kql_rechecks_data_instead_of_replaying_old_ready_result(materialized):
    case = materialized
    assert case.recovery.restore(case.generation, case.item, TARGET, case.runtime)[0]
    case.sdk.rows = 0
    ready, warnings = case.recovery.restore(case.generation, case.item, TARGET, case.runtime)
    assert not ready and any("row-count" in warning for warning in warnings)
    assert case.runtime.effects == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("region", "East US"),
        ("region", ""),
        ("database_type", "Shortcut"),
        ("shortcut_values", [{"name": "events"}]),
        ("identity", SOURCE),
        ("query_uri", SRC.endpoint),
        ("query_uri", "https://different.example.test"),
        ("name", "different-db"),
    ],
)
def test_materialized_kql_refuses_wrong_region_identity_shortcuts_or_endpoint(materialized, field, value):
    case = materialized
    setattr(case.fabric, field, value)
    ready, warnings = case.recovery.restore(case.generation, case.item, TARGET, case.runtime)
    assert not ready and warnings and not case.sdk.queries
    assert not case.runtime.catalog.pending_operations(), (
        "Read-only rejection must not block unrelated groups."
    )


def test_materialized_kql_refuses_functions_and_empty_cloned_schema(materialized):
    case = materialized
    case.sdk.physical = False
    ready, warnings = case.recovery.restore(case.generation, case.item, TARGET, case.runtime)
    assert not ready and any("physical table" in warning for warning in warnings)
    assert not case.sdk.queries


@pytest.mark.parametrize("failure", ["catalog", "cancel"])
def test_materialized_kql_fences_between_authenticated_input_and_target_reads(materialized, failure):
    case = materialized

    def interrupt():
        if failure == "catalog":
            case.runtime.catalog.available = False
        else:
            case.runtime.cancelled = True

    case.sdk.after_query = interrupt
    with pytest.raises(CatalogError if failure == "catalog" else CancelledError):
        case.recovery.restore(case.generation, case.item, TARGET, case.runtime)
    assert len(case.sdk.queries) == 1


def test_materialized_kql_preserves_service_error_and_redacts_credentials(materialized):
    from fabshuffle.fabric.client import FabricApiError

    case = materialized
    case.fabric.failure = FabricApiError(
        "GET",
        "target",
        403,
        '{"errorCode":"KqlAccessDenied","message":"token=DO_NOT_LOG"}',
    )
    ready, warnings = case.recovery.restore(case.generation, case.item, TARGET, case.runtime)
    assert not ready and "KqlAccessDenied" in warnings[0] and "DO_NOT_LOG" not in warnings[0]


@pytest.mark.parametrize("change", ["missing", "duplicate", "source", "wrong-tenant", "url"])
def test_materialized_kql_bindings_are_exact_typed_and_non_source(materialized, change):
    body = materialized.request.configuration.model_dump(mode="json")
    row = body["kql_materialized_inputs"][0]
    if change == "missing":
        row["resource_ref"] = "not-the-declared-input"
    elif change == "duplicate":
        body["kql_materialized_inputs"].append(dict(row))
    elif change == "source":
        row["item"] = SOURCE.model_dump(mode="json")
    elif change == "wrong-tenant":
        row["item"]["tenant_id"] = str(uuid4())
    else:
        row["endpoint"] = "https://arbitrary.example.test"
    with pytest.raises(ValidationError):
        binding.ProtectionConfiguration.model_validate(body)


def test_materialized_kql_does_not_claim_live_ingestion_from_table_access(materialized):
    case = materialized
    descriptor = replace(
        case.request.configuration.descriptor, continuous=True, independent_active_compute=True
    )
    configuration = case.request.configuration.model_copy(update={"descriptor": descriptor})
    request = binding.ConfigureProtectionRequest(
        source=SOURCE, configuration=configuration, expected_revision=1
    )
    record = binding.configure_protection(case.runtime, request, protected_root=None)
    captured, item = generation(record, item_type="KQLDatabase")
    assert binding.prepared_target(captured, item, case.runtime.catalog) is None
    ready, warnings = case.recovery.restore(captured, item, TARGET, case.runtime)
    assert not ready and any("no executable default binding" in warning for warning in warnings)
    assert not case.fabric.calls and not case.sdk.queries


def test_old_config_without_additive_binding_field_keeps_its_pinned_digest(configured):
    row = configured.runtime.catalog.get_record("protection-config", configured.record.artifact_reference)
    old = dict(row.document)
    old.pop("kql_materialized_inputs")
    reference = digest(canonical_json(old))
    configured.runtime.catalog.put_record(
        configured.runtime.lease,
        "protection-config",
        reference,
        old,
        expected_revision=None,
    )
    record = configured.record.model_copy(update={"artifact_reference": reference, "sha256": reference})
    assert binding._load_config(record, configured.runtime.catalog).kql_materialized_inputs == ()
