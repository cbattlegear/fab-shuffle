from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from fabshuffle.bcdr.contracts import (
    AppliedItem,
    CaptureSnapshot,
    DesiredAcl,
    ItemIdentity,
    ItemRecord,
    Principal,
    Qualification,
    RecoveryDataBinding,
    RecoverySet,
    StandbyAccessPolicy,
    WorkspaceIdentity,
    WorkspaceRecord,
)
from fabshuffle.bcdr.replica import (
    ReplicaAccessEvidence,
    ReplicaAttachmentError,
    attach_recovery_data,
    binding_digest,
    captured_recovery_paths,
    validate_attachment,
    validate_attachment_selection,
)
from fabshuffle.fabric.client import FabricApiError, FabricClient, FabricError, FabricTransportError

TENANT = "a0000000-0000-0000-0000-000000000001"
SOURCE_WS = "b0000000-0000-0000-0000-000000000001"
TARGET_WS = "c0000000-0000-0000-0000-000000000001"
CONTROL_WS = "b0000000-0000-0000-0000-000000000009"
SOURCE_ID = "d0000000-0000-0000-0000-000000000001"
TARGET_ID = "e0000000-0000-0000-0000-000000000001"
SOURCE_CAPACITY = "f0000000-0000-0000-0000-000000000001"
TARGET_CAPACITY = "f0000000-0000-0000-0000-000000000002"
GENERATION = "a0000000-0000-0000-0000-000000000002"
SET_ID = "a0000000-0000-0000-0000-000000000003"
OPERATION = "a0000000-0000-0000-0000-000000000004"
RUNTIME_ID = "a0000000-0000-0000-0000-000000000005"
EVIDENCE_ID = "a0000000-0000-0000-0000-000000000006"
SPN_ID = "a0000000-0000-0000-0000-000000000007"
OTHER_ID = "a0000000-0000-0000-0000-000000000008"
TABLE_PATH = "Tables/sales/physical-order-storage"


@dataclass
class Case:
    generation: CaptureSnapshot
    source: ItemRecord
    binding: RecoveryDataBinding
    target_mapping: AppliedItem
    recovery_set: RecoverySet
    access_evidence: ReplicaAccessEvidence
    shortcut_path: str
    shortcut_name: str

    def kwargs(self):
        return {
            "generation": self.generation,
            "source": self.source,
            "binding": self.binding,
            "target_mapping": self.target_mapping,
            "recovery_set": self.recovery_set,
            "access_evidence": self.access_evidence,
            "shortcut_path": self.shortcut_path,
            "shortcut_name": self.shortcut_name,
        }

    def select(self, path, shortcut_path, shortcut_name):
        self.binding = self.binding.model_copy(update={"source_path": path})
        self.access_evidence = self.access_evidence.model_copy(
            update={"binding_sha256": binding_digest(self.binding)},
        )
        self.shortcut_path, self.shortcut_name = shortcut_path, shortcut_name
        return self

    def replace_source(self, source):
        self.source = source
        self.generation = self.generation.model_copy(update={"items": (source,)})


def make_case(*, schema_enabled=True, files=None, tables=True):
    now = datetime.now(UTC)
    captured_at = now - timedelta(hours=1)
    source_identity = ItemIdentity(tenant_id=TENANT, workspace_id=SOURCE_WS, item_id=SOURCE_ID)
    consumer = ItemIdentity(tenant_id=TENANT, workspace_id=TARGET_WS, item_id=TARGET_ID)
    control = WorkspaceIdentity(tenant_id=TENANT, workspace_id=CONTROL_WS)
    path = TABLE_PATH if schema_enabled else "Tables/physical-order-storage"
    metadata = {
        "schema_enabled": schema_enabled,
        "schemas": [{"name": "sales" if schema_enabled else "dbo"}],
        "tables": [
            {
                "name": "Orders",
                "schema_name": "sales" if schema_enabled else "dbo",
                "data_source_format": "DELTA",
                "storage_location": f"https://onelake.dfs.fabric.microsoft.com/{SOURCE_WS}/{SOURCE_ID}/{path}",
            }
        ]
        if tables
        else [],
        "files_inventory": files if files is not None else [],
        "shortcuts": [],
    }
    source = ItemRecord(
        identity=source_identity,
        item_type="Lakehouse",
        display_name="Orders lake",
        captured_at=captured_at,
        api_version="v1",
        capture_complete=True,
        properties={"bcdr": metadata, **({"defaultSchema": "dbo"} if schema_enabled else {})},
    )
    recovery_set = RecoverySet(
        recovery_set_id=SET_ID,
        tenant_id=TENANT,
        control_workspace=control,
        control_warehouse=ItemIdentity(tenant_id=TENANT, workspace_id=CONTROL_WS, item_id=OTHER_ID),
        access_policy=StandbyAccessPolicy(
            recovery_spn=Principal(
                tenant_id=TENANT,
                object_id=SPN_ID,
                kind="ServicePrincipal",
            )
        ),
        source_capacity_ids=(SOURCE_CAPACITY,),
        target_capacity_ids=(TARGET_CAPACITY,),
    )
    generation = CaptureSnapshot(
        recovery_set_id=SET_ID,
        generation_id=GENERATION,
        captured_at=captured_at,
        workspaces=(
            WorkspaceRecord(
                identity=WorkspaceIdentity(tenant_id=TENANT, workspace_id=SOURCE_WS),
                capacity_id=SOURCE_CAPACITY,
                display_name="Source",
                captured_at=captured_at,
                inventory_complete=True,
            ),
        ),
        items=(source,),
        inventory_complete=True,
    )
    binding = RecoveryDataBinding(
        generation_id=GENERATION,
        source=source_identity,
        source_path=path,
        consumer=consumer,
        strategy="temporary_continuity",
        qualification="verified",
        read_only_verified=True,
        retention_acknowledged=True,
        evidence="qualification:caller-read-only",
    )
    mapping = AppliedItem(
        source=source_identity,
        target=consumer,
        capture_generation_id=GENERATION,
        applied_at=captured_at,
        definition_sha256="0" * 64,
        properties_sha256="0" * 64,
        target_observed_sha256="0" * 64,
        outcome="partial",
        operation_id=OPERATION,
    )
    access_evidence = ReplicaAccessEvidence(
        qualification_id=EVIDENCE_ID,
        binding_sha256=binding_digest(binding),
        principal=Principal(tenant_id=TENANT, object_id=RUNTIME_ID, kind="User"),
        verified_at=now - timedelta(minutes=5),
        valid_until=now + timedelta(minutes=30),
        enforcement_reference=binding.evidence,
    )
    return Case(
        generation,
        source,
        binding,
        mapping,
        recovery_set,
        access_evidence,
        "Tables/sales" if schema_enabled else "Tables",
        "Orders",
    )


class Tokens:
    def tenant_id(self):
        return TENANT

    def fabric_token(self):
        return "test-token"


class Destination:
    def __init__(self, case):
        self.case = case
        self.requests = []
        self.shortcuts = []
        self.guard_calls = 0
        self.on_create = None
        self.on_list = None
        self.read_override = None
        self.target_schema = case.source.properties["bcdr"]["schema_enabled"]
        self.target_capacity = TARGET_CAPACITY
        self.client = FabricClient(Tokens(), transport=httpx.MockTransport(self.respond))

    def guard(self):
        self.guard_calls += 1

    def desired(self):
        return {
            "path": self.case.shortcut_path,
            "name": self.case.shortcut_name,
            "target": {
                "oneLake": {
                    "workspaceId": SOURCE_WS,
                    "itemId": SOURCE_ID,
                    "path": self.case.binding.source_path,
                }
            },
        }

    def respond(self, request):
        self.requests.append(request)
        assert f"/workspaces/{SOURCE_WS}" not in request.url.path, "Source compute/control-plane read"
        assert "/getDefinition" not in request.url.path, "No source or destination definition reads needed"
        assert request.url.path.startswith(f"/v1/workspaces/{TARGET_WS}"), request.url
        if request.method == "GET" and request.url.path.endswith(f"/workspaces/{TARGET_WS}"):
            return httpx.Response(200, json={"id": TARGET_WS, "capacityId": self.target_capacity})
        if request.method == "GET" and "/lakehouses/" in request.url.path:
            return httpx.Response(
                200,
                json={
                    "id": TARGET_ID,
                    "workspaceId": TARGET_WS,
                    "type": "Lakehouse",
                    "displayName": "Standby",
                    "properties": {"defaultSchema": "dbo"} if self.target_schema else {},
                },
            )
        if request.method == "GET" and request.url.path.endswith("/shortcuts"):
            return (
                self.on_list(request) if self.on_list else httpx.Response(200, json={"value": self.shortcuts})
            )
        if request.method == "GET" and "/shortcuts/" in request.url.path:
            if self.read_override:
                return self.read_override(request)
            return httpx.Response(200, json=self.shortcuts[0])
        assert request.method == "POST" and request.url.path.endswith("/shortcuts")
        assert list(request.url.params.multi_items()) == [("shortcutConflictPolicy", "Abort")]
        assert self.guard_calls > 0
        document = json.loads(request.content)
        assert document == self.desired()
        if self.on_create:
            return self.on_create(request, document)
        self.shortcuts.append(document)
        return httpx.Response(201, json=document)

    def apply(self):
        return attach_recovery_data(self.client, **self.case.kwargs(), mutation_guard=self.guard)

    @property
    def posts(self):
        return [request for request in self.requests if request.method == "POST"]


@pytest.mark.parametrize("schema_enabled", [False, True])
def test_real_create_retains_source_ids_only_inside_exact_shortcut_target(schema_enabled):
    case = make_case(schema_enabled=schema_enabled)
    destination = Destination(case)
    result = destination.apply()
    assert result.changed and result.attached and result.attachment_verified
    assert result.outcome == "temporary_attached"
    assert result.data_ready is False and result.endpoint_ready is False
    assert result.shortcut == destination.desired()
    assert destination.guard_calls == 1 and len(destination.posts) == 1
    assert any("not enforced or measured" in diagnostic for diagnostic in result.diagnostics)
    assert all(request.method in {"GET", "POST"} for request in destination.requests)
    assert not any(
        "sql" in request.url.path or "jobs" in request.url.path for request in destination.requests
    )


def test_exact_existing_shortcut_is_reused_without_mutation():
    case = make_case()
    destination = Destination(case)
    existing = destination.desired()
    existing["target"]["type"] = "OneLake"
    existing["target"]["oneLake"]["workspaceId"] = SOURCE_WS.upper()
    existing["target"]["oneLake"]["itemId"] = SOURCE_ID.upper()
    destination.shortcuts = [existing]
    result = destination.apply()
    assert result.attachment_verified and not result.changed
    assert destination.posts == [] and destination.guard_calls == 0


@pytest.mark.parametrize(
    "change",
    [
        {"itemId": OTHER_ID},
        {"path": "Tables/wrong"},
        {"connectionId": OTHER_ID},
    ],
)
def test_conflicting_target_never_overwritten(change):
    case = make_case()
    destination = Destination(case)
    existing = destination.desired()
    existing["target"]["oneLake"].update(change)
    destination.shortcuts = [existing]
    with pytest.raises(ReplicaAttachmentError, match="not overwritten"):
        destination.apply()
    assert destination.posts == []


def test_execution_bearing_shortcut_metadata_is_not_ignored():
    destination = Destination(make_case())
    existing = destination.desired()
    existing["transform"] = {"type": "parquet"}
    destination.shortcuts = [existing]
    with pytest.raises(ReplicaAttachmentError, match="not overwritten"):
        destination.apply()
    assert not destination.posts


def test_duplicate_shortcut_inventory_is_not_adopted():
    destination = Destination(make_case())
    destination.shortcuts = [destination.desired(), destination.desired()]
    with pytest.raises(ReplicaAttachmentError, match="ambiguous"):
        destination.apply()
    assert not destination.posts


def test_never_create_inside_another_shortcut():
    files = [
        {"name": f"{SOURCE_ID}/Files/raw", "isDirectory": True},
        {"name": f"{SOURCE_ID}/Files/raw/child", "isDirectory": True},
    ]
    case = make_case(files=files).select("Files/raw/child", "Files/raw", "child")
    destination = Destination(case)
    destination.shortcuts = [
        {
            "path": "Files",
            "name": "raw",
            "target": {
                "oneLake": {"workspaceId": SOURCE_WS, "itemId": SOURCE_ID, "path": "Files/other"},
            },
        }
    ]
    with pytest.raises(ReplicaAttachmentError, match="overlaps"):
        destination.apply()
    assert not destination.posts


def test_409_race_requires_exact_destination_get_and_preserves_service_message():
    destination = Destination(make_case())

    def racing_create(request, document):
        destination.shortcuts = [document]
        return httpx.Response(
            409, json={"errorCode": "ShortcutAlreadyExists", "message": "Concurrent create"}
        )

    destination.on_create = racing_create
    result = destination.apply()
    assert result.attached and result.changed is False
    assert "ShortcutAlreadyExists" in result.diagnostics[0] and "Concurrent create" in result.diagnostics[0]
    assert len(destination.posts) == 1


def test_409_with_different_shortcut_preserves_original_conflict():
    destination = Destination(make_case())

    def racing_create(request, document):
        document["target"]["oneLake"]["itemId"] = OTHER_ID
        destination.shortcuts = [document]
        return httpx.Response(409, json={"errorCode": "Occupied", "message": "Do not replace"})

    destination.on_create = racing_create
    with pytest.raises(FabricApiError) as caught:
        destination.apply()
    assert caught.value.error_code == "Occupied"
    assert len(destination.posts) == 1


def test_lost_create_response_is_not_retried_but_next_invocation_reconciles():
    destination = Destination(make_case())

    def lost_response(request, document):
        destination.shortcuts = [document]
        raise httpx.ReadTimeout("response lost", request=request)

    destination.on_create = lost_response
    with pytest.raises(FabricTransportError):
        destination.apply()
    assert len(destination.posts) == 1
    result = destination.apply()
    assert result.attached and not result.changed and len(destination.posts) == 1


@pytest.mark.parametrize("status", [400, 401, 403, 404, 500])
def test_create_service_error_is_not_flattened_or_retried(status):
    destination = Destination(make_case())
    destination.on_create = lambda *args: httpx.Response(
        status,
        json={"errorCode": "ServiceSaid", "message": "Keep this diagnostic"},
    )
    with pytest.raises(FabricApiError) as caught:
        destination.apply()
    assert caught.value.error_code == "ServiceSaid"
    assert caught.value.detail == "Keep this diagnostic"
    assert len(destination.posts) == 1


def test_malformed_destination_inventory_cannot_be_assumed_empty():
    destination = Destination(make_case())
    destination.on_list = lambda _: httpx.Response(200, json={})
    with pytest.raises(FabricError, match="collection"):
        destination.apply()
    assert not destination.posts


def test_readback_mismatch_is_not_success():
    destination = Destination(make_case())
    wrong = destination.desired()
    wrong["target"]["oneLake"]["path"] = "Tables/other"
    destination.read_override = lambda _: httpx.Response(200, json=wrong)
    with pytest.raises(ReplicaAttachmentError, match="did not confirm"):
        destination.apply()
    assert len(destination.posts) == 1


def test_live_mutation_guard_can_stop_the_effect():
    destination = Destination(make_case())

    def fenced():
        raise RuntimeError("Controller lost its lease")

    with pytest.raises(RuntimeError, match="lost its lease"):
        attach_recovery_data(destination.client, **destination.case.kwargs(), mutation_guard=fenced)
    assert not destination.posts


@pytest.mark.parametrize(
    "field,value",
    [
        ("generation_id", OTHER_ID),
        ("read_only_verified", False),
        ("retention_acknowledged", False),
        ("qualification", Qualification.UNVERIFIED),
        ("source_path", "Tables"),
        ("source_path", "Files"),
    ],
)
def test_unqualified_or_widened_bindings_never_reach_destination(field, value):
    case = make_case()
    case.binding = case.binding.model_copy(update={field: value})
    destination = Destination(case)
    with pytest.raises((ValueError, ReplicaAttachmentError)):
        destination.apply()
    assert destination.requests == []


@pytest.mark.parametrize(
    "update",
    [
        {"binding_sha256": "f" * 64},
        {"valid_until": datetime.now(UTC) - timedelta(minutes=1)},
        {"verified_at": datetime.now(UTC) + timedelta(minutes=1)},
        {"verified_at": datetime.now(UTC) - timedelta(days=1)},
        {"access_mode": "delegated_owner"},
        {"enforcement_reference": "different proof"},
    ],
)
def test_current_exact_caller_mode_access_evidence_is_mandatory(update):
    case = make_case()
    case.access_evidence = case.access_evidence.model_copy(update=update)
    destination = Destination(case)
    with pytest.raises((ValueError, ReplicaAttachmentError)):
        destination.apply()
    assert not destination.requests


@pytest.mark.parametrize("permission,blocked", [("Admin", True), ("Contributor", True), ("Viewer", False)])
def test_known_captured_write_permissions_contradict_read_only_qualification(permission, blocked):
    case = make_case()
    acl = DesiredAcl(
        acl_id=OTHER_ID,
        scope="workspace",
        workspace=WorkspaceIdentity(tenant_id=TENANT, workspace_id=SOURCE_WS),
        principal=case.access_evidence.principal,
        permission=permission,
        provenance="captured ACL",
    )
    case.generation = case.generation.model_copy(update={"desired_acls": (acl,)})
    if blocked:
        with pytest.raises(ReplicaAttachmentError, match="captured"):
            validate_attachment(**case.kwargs())
    else:
        validate_attachment(**case.kwargs())


def test_stale_or_unowned_target_mapping_is_rejected():
    case = make_case()
    case.target_mapping = case.target_mapping.model_copy(update={"capture_generation_id": OTHER_ID})
    destination = Destination(case)
    with pytest.raises(ReplicaAttachmentError, match="owned"):
        destination.apply()
    assert destination.requests == []


def test_target_source_workspace_or_control_workspace_is_never_mutated():
    for workspace in (SOURCE_WS, CONTROL_WS):
        case = make_case()
        consumer = case.binding.consumer.model_copy(update={"workspace_id": workspace})
        case.binding = case.binding.model_copy(update={"consumer": consumer})
        case.target_mapping = case.target_mapping.model_copy(update={"target": consumer})
        case.access_evidence = case.access_evidence.model_copy(
            update={"binding_sha256": binding_digest(case.binding)},
        )
        destination = Destination(case)
        with pytest.raises((ValueError, ReplicaAttachmentError)):
            destination.apply()
        assert destination.requests == []


def test_wrong_capacity_and_schema_mode_are_gated():
    destination = Destination(make_case())
    destination.target_capacity = SOURCE_CAPACITY
    with pytest.raises(ReplicaAttachmentError, match="recovery capacity"):
        destination.apply()
    assert not destination.posts
    destination.target_capacity = TARGET_CAPACITY
    destination.target_schema = False
    with pytest.raises(ReplicaAttachmentError, match="schema mode"):
        destination.apply()
    assert not destination.posts


def test_binding_uses_actual_storage_path_but_preserves_logical_table_name():
    case = make_case()
    assert case.binding.source_path.endswith("physical-order-storage")
    validate_attachment(**case.kwargs())
    case.shortcut_name = "physical-order-storage"
    with pytest.raises(ReplicaAttachmentError, match="logical schema/name"):
        validate_attachment(**case.kwargs())


def test_source_storage_path_cannot_point_to_external_or_other_item_data():
    case = make_case()
    properties = copy.deepcopy(case.source.properties)
    properties["bcdr"]["tables"][0]["storage_location"] = (
        f"https://onelake.dfs.fabric.microsoft.com/{SOURCE_WS}/{OTHER_ID}/{TABLE_PATH}"
    )
    case.replace_source(case.source.model_copy(update={"properties": properties}))
    with pytest.raises(ReplicaAttachmentError, match="different source item"):
        validate_attachment(**case.kwargs())


def test_non_delta_and_chained_shortcut_targets_are_not_native_attachments():
    case = make_case()
    properties = copy.deepcopy(case.source.properties)
    properties["bcdr"]["tables"][0]["data_source_format"] = "PARQUET"
    case.replace_source(case.source.model_copy(update={"properties": properties}))
    with pytest.raises(ReplicaAttachmentError, match="Delta format"):
        validate_attachment(**case.kwargs())
    properties["bcdr"]["tables"][0]["data_source_format"] = "DELTA"
    properties["bcdr"]["shortcuts"] = [{"path": "Tables/sales", "name": "physical-order-storage"}]
    case.replace_source(case.source.model_copy(update={"properties": properties}))
    with pytest.raises(ReplicaAttachmentError, match="chained/external"):
        validate_attachment(**case.kwargs())
    properties["bcdr"]["shortcuts"] = [{"path": "Tables/sales", "name": "Orders"}]
    case.replace_source(case.source.model_copy(update={"properties": properties}))
    with pytest.raises(ReplicaAttachmentError, match="not qualified native"):
        validate_attachment(**case.kwargs())


def test_files_directory_attachment_preserves_all_child_paths():
    files = [
        {"name": f"{SOURCE_ID}/Files/raw", "isDirectory": True},
        {"name": f"{SOURCE_ID}/Files/raw/a.csv"},
        {"name": f"{SOURCE_ID}/Files/raw/sub/b.json"},
    ]
    case = make_case(files=files, tables=False).select("Files/raw", "Files", "raw")
    destination = Destination(case)
    result = destination.apply()
    coverage = validate_attachment_selection(
        case.generation,
        case.source,
        [(case.binding, case.shortcut_path, case.shortcut_name)],
    )
    assert result.attached and coverage.complete
    assert coverage.required_paths == coverage.covered_paths == ("Files/raw",)
    assert result.data_ready is False


def test_partial_files_subtree_cannot_claim_full_source_coverage():
    files = [{"name": f"{SOURCE_ID}/Files/raw/a/data.json"}, {"name": f"{SOURCE_ID}/Files/raw/b/data.json"}]
    case = make_case(files=files, tables=False).select("Files/raw/a", "Files/raw", "a")
    coverage = validate_attachment_selection(
        case.generation,
        case.source,
        [(case.binding, case.shortcut_path, case.shortcut_name)],
    )
    assert not coverage.complete and coverage.missing_paths == ("Files/raw",)


def test_loose_root_files_remain_missing_and_cannot_be_renamed_under_an_alias():
    files = [{"name": f"{SOURCE_ID}/Files/root.csv"}, {"name": f"{SOURCE_ID}/Files/raw/a.csv"}]
    case = make_case(files=files, tables=False).select("Files/raw", "Files", "raw")
    coverage = validate_attachment_selection(
        case.generation,
        case.source,
        [(case.binding, case.shortcut_path, case.shortcut_name)],
    )
    assert coverage.missing_paths == ("Files/root.csv",) and not coverage.complete
    case.select("Files/root.csv", "Files", "root.csv")
    with pytest.raises(ReplicaAttachmentError, match="loose files"):
        validate_attachment(**case.kwargs())
    case.select("Files/raw", "Files", "recovered")
    with pytest.raises(ReplicaAttachmentError, match="alias"):
        validate_attachment(**case.kwargs())


def test_empty_manifest_and_empty_selection_never_mean_ready():
    case = make_case(tables=False)
    assert captured_recovery_paths(case.source) == ()
    assert not validate_attachment_selection(case.generation, case.source, []).complete


def test_duplicate_and_overlapping_batch_entries_are_rejected_before_effects():
    case = make_case()
    entry = (case.binding, case.shortcut_path, case.shortcut_name)
    with pytest.raises(ReplicaAttachmentError, match="duplicate"):
        validate_attachment_selection(case.generation, case.source, [entry, entry])


def test_warehouse_cannot_be_reported_as_writable_recovery_through_shortcuts():
    case = make_case()
    case.replace_source(case.source.model_copy(update={"item_type": "Warehouse"}))
    with pytest.raises(ReplicaAttachmentError, match="writable Warehouse"):
        validate_attachment(**case.kwargs())
