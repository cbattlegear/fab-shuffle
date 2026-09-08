"""Combined helpers keep source reads and destination writes on explicit clients."""

from __future__ import annotations

import json
from contextlib import ExitStack
from types import SimpleNamespace

import httpx
import pytest

from fabshuffle.fabric import airflow, analytics, shortcuts, spark, sqldatabases, workspaces
from fabshuffle.fabric.client import SETTINGS, FabricApiError, FabricClient, FabricError, FabricTransportError
from fabshuffle.fabric.definitions import decode_json_part, part
from fabshuffle.lifecycle import Disposition, EvidenceState, Lifecycle

SOURCE_WS = "source-workspace"
TARGET_WS = "target-workspace"
SOURCE_ID = "source-item"
TARGET_ID = "target-item"
ITEM = {"id": SOURCE_ID, "displayName": "Migrated item"}
ID_MAP = {SOURCE_WS: TARGET_WS, "source-store": "target-store"}
PARTS = [
    part(".platform", {"logicalId": SOURCE_ID}),
    part("definition.json", {"workspaceId": SOURCE_WS, "itemId": "source-store"}),
]
REWRITTEN = [part("definition.json", {"workspaceId": TARGET_WS, "itemId": "target-store"})]
SOURCE_EXPORT = f"workspaces/{SOURCE_WS}/items/{SOURCE_ID}/getDefinition"
TARGET_CREATE = f"workspaces/{TARGET_WS}/items"
TARGET_UPDATE = f"workspaces/{TARGET_WS}/items/{TARGET_ID}/updateDefinition"


class Endpoint:
    def __init__(self, tenant, routes):
        self.tenant = tenant
        self.routes = routes
        self.calls = []

    def __call__(self, request):
        assert request.headers["Authorization"] == f"Bearer {self.tenant}"
        self.calls.append(request)
        key = (request.method, request.url.path.removeprefix("/v1/"))
        if key not in self.routes:
            pytest.fail(f"Unexpected {self.tenant} request: {key}")
        response = self.routes[key]
        if isinstance(response, Exception):
            raise response
        return response if isinstance(response, httpx.Response) else httpx.Response(200, json=response)

    @property
    def paths(self):
        return [(request.method, request.url.path.removeprefix("/v1/")) for request in self.calls]

    @property
    def bodies(self):
        return [json.loads(request.content) for request in self.calls if request.content]


@pytest.fixture
def pair():
    with ExitStack() as stack:
        def make_pair(source_routes=None, target_routes=None):
            endpoints = [Endpoint("source", source_routes or {}), Endpoint("target", target_routes or {})]
            clients = [
                stack.enter_context(FabricClient(
                    SimpleNamespace(fabric_token=lambda tenant=endpoint.tenant: tenant),
                    transport=httpx.MockTransport(endpoint),
                ))
                for endpoint in endpoints
            ]
            return (*clients, *endpoints)

        yield make_pair


def operation(result):
    return {
        ("GET", "operations/shared-operation"): {"status": "Succeeded"},
        ("GET", "operations/shared-operation/result"): result,
    }


def pending():
    return httpx.Response(202, headers={"x-ms-operation-id": "shared-operation", "Retry-After": "0"})


@pytest.mark.parametrize("item_type", ["Notebook", airflow.APACHE_AIRFLOW_JOB])
@pytest.mark.parametrize("adopted", [False, True])
@pytest.mark.parametrize("preloaded", [False, True])
def test_definition_export_and_destination_lro_use_their_own_clients(pair, item_type, adopted, preloaded):
    source_routes = {} if preloaded else {
        ("POST", SOURCE_EXPORT): pending(),
        **operation({"definition": {"parts": PARTS}}),
    }
    target_path = TARGET_UPDATE if adopted else TARGET_CREATE
    source, target, source_endpoint, target_endpoint = pair(source_routes, {
        ("GET", f"workspaces/{TARGET_WS}"): {"id": TARGET_WS},
        ("POST", target_path): pending(),
        **operation({"id": TARGET_ID}),
    })
    lifecycle = Lifecycle(attempt_id="attempt", source_workspace=SOURCE_WS).item(
        SOURCE_ID, ITEM["displayName"], item_type,
    )

    result = analytics.migrate_definition_item(
        source, target_client=target, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        cross_tenant=True,
        item=ITEM, item_type=item_type, id_map=ID_MAP, parts=PARTS if preloaded else None,
        target_id=TARGET_ID if adopted else None, lifecycle=lifecycle,
    )

    assert result.target_id == TARGET_ID
    assert result.parts == tuple(REWRITTEN)
    assert target_endpoint.bodies[0]["definition"]["parts"] == REWRITTEN
    assert source_endpoint.paths == ([] if preloaded else [
        ("POST", SOURCE_EXPORT), ("GET", "operations/shared-operation"),
        ("GET", "operations/shared-operation/result"),
    ])
    assert target_endpoint.paths == [
        ("GET", f"workspaces/{TARGET_WS}"),
        ("POST", target_path), ("GET", "operations/shared-operation"),
        ("GET", "operations/shared-operation/result"),
    ]
    outcome = lifecycle.owner.get(SOURCE_ID)
    assert outcome.targetId == TARGET_ID
    assert outcome.disposition == (Disposition.REFRESHED if adopted else Disposition.CREATED)
    assert outcome.steps["definition"].state == EvidenceState.SUCCEEDED


@pytest.mark.parametrize("adopted", [False, True])
def test_batch_passes_target_client_and_keeps_preloaded_source_definition(pair, adopted):
    target_path = TARGET_UPDATE if adopted else TARGET_CREATE
    source, target, source_endpoint, target_endpoint = pair({}, {
        ("GET", f"workspaces/{TARGET_WS}"): {"id": TARGET_WS},
        ("POST", target_path): {"id": TARGET_ID},
    })
    id_map = dict(ID_MAP)

    migrated, warnings = analytics.migrate_items(
        source, target_client=target, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        cross_tenant=True,
        items=[ITEM], item_type="Notebook", id_map=id_map, parts_by_id={SOURCE_ID: PARTS},
        existing_targets={SOURCE_ID: TARGET_ID} if adopted else None,
    )

    assert not warnings
    assert migrated[0].target_id == id_map[SOURCE_ID] == TARGET_ID
    assert not source_endpoint.calls
    assert target_endpoint.paths == [("GET", f"workspaces/{TARGET_WS}"), ("POST", target_path)]


@pytest.mark.parametrize("preloaded", [False, True])
def test_batch_failure_checks_reference_access_as_target_and_keeps_service_error(pair, preloaded):
    definition = [part("definition.json", {"workspaceId": "other-workspace"})]
    source, target, source_endpoint, target_endpoint = pair(
        {} if preloaded else {("POST", SOURCE_EXPORT): {"definition": {"parts": definition}}},
        {
            ("POST", TARGET_CREATE): httpx.Response(
                400, json={"errorCode": "UnknownError", "message": "Target could not bind source"},
            ),
            ("GET", "workspaces/other-workspace"): httpx.Response(403, json={"errorCode": "Forbidden"}),
        },
    )

    migrated, warnings = analytics.migrate_items(
        source, target_client=target, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        items=[ITEM], item_type="Notebook", id_map=dict(ID_MAP),
        parts_by_id={SOURCE_ID: definition} if preloaded else None,
        strict_references=False,
    )

    assert not migrated
    assert "UnknownError Target could not bind source" in warnings[0]
    assert "other-workspace" in warnings[0]
    assert source_endpoint.paths == ([] if preloaded else [("POST", SOURCE_EXPORT)] * 2)
    assert target_endpoint.paths == [("POST", TARGET_CREATE), ("GET", "workspaces/other-workspace")]


@pytest.mark.parametrize("adopted", [False, True])
def test_dangling_endpoint_refuses_cross_tenant_create_and_update(pair, adopted):
    source, target, source_endpoint, target_endpoint = pair()
    with pytest.raises(analytics.StrandedReference, match="Missing warehouse"):
        analytics.migrate_definition_item(
            source, target_client=target, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
            cross_tenant=True,
            item=ITEM, item_type="Notebook", id_map=ID_MAP,
            target_id=TARGET_ID if adopted else None,
            parts=[part("notebook.py", "source-endpoint.fabric.microsoft.com")],
            source_items={"warehouse": {
                "type": "Warehouse", "displayName": "Missing warehouse",
                "properties": {"connectionString": "source-endpoint.fabric.microsoft.com"},
            }},
        )
    assert not source_endpoint.calls and not target_endpoint.calls


def test_folder_clone_reads_and_adopts_target_parent_before_creating_children(pair):
    source, target, source_endpoint, target_endpoint = pair({
        ("GET", f"workspaces/{SOURCE_WS}/folders"): {"value": [
            {"id": "old-parent", "displayName": "Parent"},
            {"id": "old-child", "displayName": "Child", "parentFolderId": "old-parent"},
        ]},
    }, {
        ("GET", f"workspaces/{TARGET_WS}/folders"): {"value": [
            {"id": "new-parent", "displayName": "Parent"},
        ]},
        ("POST", f"workspaces/{TARGET_WS}/folders"): {"id": "new-child"},
    })

    assert workspaces.clone_folder_tree(source, SOURCE_WS, TARGET_WS, target_client=target) == {
        "old-parent": "new-parent", "old-child": "new-child",
    }
    assert source_endpoint.paths == [("GET", f"workspaces/{SOURCE_WS}/folders")]
    assert target_endpoint.bodies == [{"displayName": "Child", "parentFolderId": "new-parent"}]


def test_pool_resume_reads_source_and_verifies_then_replaces_missing_target(pair):
    pools = [
        {"id": "old-retained", "name": "Retained", "type": "Workspace"},
        {"id": "old-missing", "name": "Missing", "type": "Workspace"},
    ]
    source, target, source_endpoint, target_endpoint = pair({
        ("GET", f"workspaces/{SOURCE_WS}/spark/pools"): {"value": pools},
    }, {
        ("GET", f"workspaces/{TARGET_WS}/spark/pools"): {"value": [{"id": "retained"}]},
        ("POST", f"workspaces/{TARGET_WS}/spark/pools"): {"id": "replacement"},
    })
    missing, mapped = [], []

    result = spark.copy_pools(
        source, SOURCE_WS, TARGET_WS, target_client=target,
        prior_map={"old-retained": "retained", "old-missing": "deleted"},
        on_missing=missing.append, on_mapped=lambda old, new: mapped.append((old, new)),
    )

    assert result == ({"old-retained": "retained", "old-missing": "replacement"}, ["Missing"], [])
    assert missing == ["old-missing"]
    assert mapped == [("old-retained", "retained"), ("old-missing", "replacement")]
    assert source_endpoint.paths == [("GET", f"workspaces/{SOURCE_WS}/spark/pools")]
    assert target_endpoint.bodies == [{"name": "Missing"}]


def test_preloaded_pools_still_verify_retained_destination_with_target_client(pair):
    source, target, source_endpoint, target_endpoint = pair({}, {
        ("GET", f"workspaces/{TARGET_WS}/spark/pools"): httpx.Response(
            403, json={"errorCode": "TargetDenied", "message": "Cannot list target pools"},
        ),
    })
    with pytest.raises(FabricApiError, match="TargetDenied"):
        spark.copy_pools(
            source, SOURCE_WS, TARGET_WS, target_client=target,
            pools=[{"id": "old-pool", "name": "Pool"}], prior_map={"old-pool": "retained"},
        )
    assert not source_endpoint.calls
    assert target_endpoint.paths == [("GET", f"workspaces/{TARGET_WS}/spark/pools")]


@pytest.fixture(params=["items", "kqlDatabases"])
def shortcut_kind(request):
    return request.param


def test_shortcuts_read_source_and_create_rebound_targets_with_lifecycle(pair, shortcut_kind):
    source_path = f"workspaces/{SOURCE_WS}/{shortcut_kind}/{SOURCE_ID}/shortcuts"
    target_path = f"workspaces/{TARGET_WS}/{shortcut_kind}/{TARGET_ID}/shortcuts"
    source, target, source_endpoint, target_endpoint = pair({
        ("GET", source_path): {"value": [
            {"path": "Tables", "name": "Internal", "target": {
                "oneLake": {"workspaceId": SOURCE_WS, "itemId": "source-store", "path": "Tables/T"},
            }},
            {"path": "Files", "name": "External", "target": {
                "adlsGen2": {"connectionId": "source-connection", "location": "https://storage"},
            }},
        ]},
    }, {
        ("GET", f"workspaces/{TARGET_WS}"): {"id": TARGET_WS},
        ("POST", target_path): {},
    })
    copy = shortcuts.copy_shortcuts if shortcut_kind == "items" else shortcuts.copy_table_shortcuts
    lifecycle = Lifecycle(attempt_id="attempt", source_workspace=SOURCE_WS).item(
        SOURCE_ID, "Store", "Lakehouse",
    )
    lifecycle.resolve(TARGET_ID, TARGET_WS, Disposition.CREATED)

    result = copy(
        source, SOURCE_WS, SOURCE_ID, TARGET_WS, TARGET_ID,
        {**ID_MAP, "source-connection": "target-connection"}, target_client=target,
        cross_tenant=True,
        source_items={"source-connection": {"type": "Connection", "displayName": "External source"}},
        lifecycle=lifecycle,
    )

    assert result == (2, [])
    assert source_endpoint.paths == [("GET", source_path)]
    internal, external = target_endpoint.bodies
    assert internal["target"]["oneLake"]["workspaceId"] == TARGET_WS
    assert internal["target"]["oneLake"]["itemId"] == "target-store"
    assert external["target"]["adlsGen2"]["connectionId"] == "target-connection"
    assert lifecycle.owner.get(SOURCE_ID).steps["shortcuts"].state == EvidenceState.SUCCEEDED


def test_shortcut_target_failure_preserves_service_evidence(pair, shortcut_kind):
    source_path = f"workspaces/{SOURCE_WS}/{shortcut_kind}/{SOURCE_ID}/shortcuts"
    target_path = f"workspaces/{TARGET_WS}/{shortcut_kind}/{TARGET_ID}/shortcuts"
    source, target, _, _ = pair({
        ("GET", source_path): {"value": [{"path": "Files", "name": "Broken", "target": {
            "adlsGen2": {"connectionId": "source-connection", "location": "https://storage"},
        }}]},
    }, {("POST", target_path): httpx.Response(
        403, json={"errorCode": "TargetConnectionDenied", "message": "Grant access to target connection"},
    )})
    copy = shortcuts.copy_shortcuts if shortcut_kind == "items" else shortcuts.copy_table_shortcuts
    lifecycle = Lifecycle(attempt_id="attempt", source_workspace=SOURCE_WS).item(
        SOURCE_ID, "Store", "Lakehouse",
    )

    count, warnings = copy(
        source, SOURCE_WS, SOURCE_ID, TARGET_WS, TARGET_ID,
        {**ID_MAP, "source-connection": "target-connection"}, target_client=target, lifecycle=lifecycle,
        cross_tenant=True,
    )

    assert count == 0
    assert "TargetConnectionDenied Grant access to target connection" in warnings[0]
    evidence = lifecycle.owner.get(SOURCE_ID).steps["shortcuts"]
    assert evidence.state == EvidenceState.FAILED
    assert evidence.errorCode == "TargetConnectionDenied"
    assert evidence.message == "Grant access to target connection"


def test_preloaded_kql_shortcuts_never_read_with_target_client(pair):
    target_path = f"workspaces/{TARGET_WS}/kqlDatabases/{TARGET_ID}/shortcuts"
    source, target, source_endpoint, target_endpoint = pair({}, {
        ("GET", f"workspaces/{TARGET_WS}"): {"id": TARGET_WS},
        ("POST", target_path): {},
    })

    assert shortcuts.copy_table_shortcuts(
        source, SOURCE_WS, SOURCE_ID, TARGET_WS, TARGET_ID, ID_MAP, target_client=target,
        cross_tenant=True,
        shortcuts=[{"name": "Table", "target": {
            "oneLake": {"workspaceId": SOURCE_WS, "itemId": "source-store", "path": "Tables/Table"},
        }}],
    ) == (1, [])
    assert not source_endpoint.calls
    assert target_endpoint.paths == [("GET", f"workspaces/{TARGET_WS}"), ("POST", target_path)]


@pytest.mark.parametrize("preloaded", [False, True])
def test_airflow_files_use_source_preflight_and_target_upload(pair, preloaded):
    source_path = f"workspaces/{SOURCE_WS}/apacheAirflowJobs/{SOURCE_ID}/files"
    target_path = f"workspaces/{TARGET_WS}/apacheAirflowJobs/{TARGET_ID}/files/dags/main.py"
    source, target, source_endpoint, target_endpoint = pair({} if preloaded else {
        ("GET", source_path): {"value": [{"filePath": "dags/main.py", "sizeInBytes": 30}]},
        ("GET", f"{source_path}/dags/main.py"): httpx.Response(200, content=f"workspace = '{SOURCE_WS}'"),
    }, {("PUT", target_path): httpx.Response(204)})

    assert airflow.copy_files(
        source, target_client=target, source_workspace_id=SOURCE_WS, source_job_id=SOURCE_ID,
        cross_tenant=True,
        target_workspace_id=TARGET_WS, target_job_id=TARGET_ID, job_name="Job",
        id_map=ID_MAP, source_items={},
        prepared_files=[("dags/main.py", f"workspace = '{TARGET_WS}'".encode())] if preloaded else None,
    ) == (1, [])

    assert source_endpoint.paths == ([] if preloaded else [
        ("GET", source_path), ("GET", f"{source_path}/dags/main.py"),
    ])
    assert target_endpoint.paths == [("PUT", target_path)]
    assert target_endpoint.calls[0].content == f"workspace = '{TARGET_WS}'".encode()
    assert target_endpoint.calls[0].url.params["beta"] == "true"


def test_sql_schema_exports_source_and_updates_destination_lro_without_interpreting_dacpac(pair):
    dacpac = part("sqldb.dacpac", b"\x00opaque-schema\xff")
    target_path = f"workspaces/{TARGET_WS}/sqlDatabases/{TARGET_ID}/updateDefinition"
    source, target, source_endpoint, target_endpoint = pair({
        ("POST", SOURCE_EXPORT): pending(),
        **operation({"definition": {"parts": [PARTS[0], dacpac]}}),
    }, {("POST", target_path): pending(), **operation({})})

    sqldatabases.copy_schema(
        source, target_client=target, source_workspace_id=SOURCE_WS, source_id=SOURCE_ID,
        target_workspace_id=TARGET_WS, target_id=TARGET_ID,
    )

    assert source_endpoint.calls[0].url.params["format"] == "dacpac"
    assert target_endpoint.bodies == [{"definition": {"format": "dacpac", "parts": [dacpac]}}]
    assert source_endpoint.paths[1:] == target_endpoint.paths[1:] == [
        ("GET", "operations/shared-operation"), ("GET", "operations/shared-operation/result"),
    ]


def test_default_client_remains_usable_for_both_workspaces(pair):
    source, _, source_endpoint, target_endpoint = pair({
        ("POST", SOURCE_EXPORT): {"definition": {"parts": PARTS}},
        ("POST", TARGET_CREATE): {"id": TARGET_ID},
    })

    migrated, warnings = analytics.migrate_items(
        source, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        items=[ITEM], item_type="Notebook", id_map=dict(ID_MAP),
    )

    assert not warnings and migrated[0].target_id == TARGET_ID
    assert source_endpoint.paths == [("POST", SOURCE_EXPORT), ("POST", TARGET_CREATE)]
    assert not target_endpoint.calls


SOURCE_CONNECTION = "aaaaaaaa-1111-2222-3333-444444444444"
TARGET_CONNECTION = "bbbbbbbb-1111-2222-3333-444444444444"


@pytest.mark.parametrize("connection_key", ["connection", "connectionId", "dataConnectionId"])
@pytest.mark.parametrize("replacement", [None, "", SOURCE_CONNECTION.upper()])
@pytest.mark.parametrize("adopted", [False, True])
def test_unlisted_source_connection_requires_a_nonidentity_mapping_before_write(
    pair, connection_key, replacement, adopted,
):
    source, target, source_endpoint, target_endpoint = pair()
    id_map = {} if replacement is None else {SOURCE_CONNECTION: replacement}
    lifecycle = Lifecycle(attempt_id="attempt", source_workspace=SOURCE_WS).item(
        SOURCE_ID, ITEM["displayName"], "DataPipeline",
    )

    with pytest.raises(analytics.StrandedReference, match=SOURCE_CONNECTION):
        analytics.migrate_definition_item(
            source, target_client=target, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
            cross_tenant=True,
            item=ITEM, item_type="DataPipeline", id_map=id_map,
            parts=[part("pipeline.json", {"externalReferences": {connection_key: SOURCE_CONNECTION}})],
            target_id=TARGET_ID if adopted else None, lifecycle=lifecycle,
        )

    assert not source_endpoint.calls and not target_endpoint.calls
    outcome = lifecycle.owner.get(SOURCE_ID)
    assert outcome.steps["rebind"].state == EvidenceState.FAILED
    assert any(SOURCE_CONNECTION in reason for reason in outcome.unresolvedReferences)


def test_unlisted_mapped_connection_rewrites_case_insensitively(pair):
    source, target, source_endpoint, target_endpoint = pair({}, {
        ("POST", TARGET_CREATE): {"id": TARGET_ID},
    })
    result = analytics.migrate_definition_item(
        source, target_client=target, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        cross_tenant=True,
        item=ITEM, item_type="DataPipeline", id_map={SOURCE_CONNECTION.upper(): TARGET_CONNECTION},
        parts=[part("pipeline.json", {"connectionId": SOURCE_CONNECTION})],
    )

    assert result.target_id == TARGET_ID
    assert not source_endpoint.calls
    assert target_endpoint.bodies[0]["definition"]["parts"] == [
        part("pipeline.json", {"connectionId": TARGET_CONNECTION}),
    ]


@pytest.mark.parametrize("status", [400, 403, 404, 500])
@pytest.mark.parametrize("adopted", [False, True])
def test_external_workspace_access_is_verified_as_destination_before_any_write(
    pair, monkeypatch, status, adopted,
):
    monkeypatch.setattr(SETTINGS, "max_retries", 1)
    source, target, source_endpoint, target_endpoint = pair({
        ("GET", "workspaces/source-external"): {"id": "source-external"},
    }, {
        ("GET", "workspaces/source-external"): httpx.Response(
            status, json={
                "errorCode": "DestinationAccessUnknown", "message": "Cannot read referenced workspace",
            },
        ),
    })
    lifecycle = Lifecycle(attempt_id="attempt", source_workspace=SOURCE_WS).item(
        SOURCE_ID, ITEM["displayName"], "Notebook",
    )
    with pytest.raises(FabricApiError) as raised:
        analytics.migrate_definition_item(
            source, target_client=target, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
            cross_tenant=True,
            item=ITEM, item_type="Notebook", id_map=ID_MAP, lifecycle=lifecycle,
            target_id=TARGET_ID if adopted else None,
            parts=[part("definition.json", {"workspaceId": "source-external"})],
        )

    assert raised.value.error_code == "DestinationAccessUnknown"
    assert raised.value.detail == "Cannot read referenced workspace"
    assert not source_endpoint.calls
    assert target_endpoint.paths == [("GET", "workspaces/source-external")]
    assert lifecycle.owner.get(SOURCE_ID).steps["rebind"].errorCode == "DestinationAccessUnknown"


def test_every_rewritten_workspace_is_checked_with_destination_credentials(pair):
    expected_workspaces = sorted([TARGET_WS, "retained-external", "mapped-external"])
    source, target, source_endpoint, target_endpoint = pair({}, {
        **{("GET", f"workspaces/{workspace}"): {"id": workspace} for workspace in expected_workspaces},
        ("POST", TARGET_CREATE): {"id": TARGET_ID},
    })
    analytics.migrate_definition_item(
        source, target_client=target, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        cross_tenant=True,
        item=ITEM, item_type="Notebook", id_map={**ID_MAP, "old-external": "mapped-external"},
        parts=[part("definition.json", {
            "workspaceId": SOURCE_WS,
            "inputs": [{"workspaceId": "retained-external"}, {"sourceWorkspaceId": "old-external"}],
        })],
    )

    assert not source_endpoint.calls
    assert target_endpoint.paths == [
        *(("GET", f"workspaces/{workspace}") for workspace in expected_workspaces),
        ("POST", TARGET_CREATE),
    ]


def test_inconclusive_destination_workspace_read_cannot_authorize_create(pair, monkeypatch):
    monkeypatch.setattr(SETTINGS, "max_retries", 1)
    source, target, source_endpoint, target_endpoint = pair({}, {
        ("GET", "workspaces/external"): httpx.ReadTimeout("Destination read timed out"),
    })
    with pytest.raises(FabricTransportError, match="Destination read timed out"):
        analytics.migrate_definition_item(
            source, target_client=target, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
            cross_tenant=True,
            item=ITEM, item_type="Notebook", id_map={},
            parts=[part("definition.json", {"workspaceId": "external"})],
        )
    assert not source_endpoint.calls
    assert target_endpoint.paths == [("GET", "workspaces/external")]


def test_source_workspace_reference_is_rejected_even_if_destination_could_read_it(pair):
    source, target, source_endpoint, target_endpoint = pair({}, {
        ("GET", f"workspaces/{SOURCE_WS}"): {"id": SOURCE_WS},
    })
    with pytest.raises(analytics.StrandedReference, match="source workspace"):
        analytics.migrate_definition_item(
            source, target_client=target, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
            cross_tenant=True,
            item=ITEM, item_type="Notebook", id_map={},
            parts=[part("definition.json", {"workspaceId": SOURCE_WS})],
        )
    assert not source_endpoint.calls and not target_endpoint.calls


def test_explicit_same_client_preserves_legacy_reference_behavior(pair):
    source, _, source_endpoint, _ = pair({("POST", TARGET_CREATE): {"id": TARGET_ID}})
    analytics.migrate_definition_item(
        source, target_client=source, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        item=ITEM, item_type="Notebook", id_map={},
        parts=[part("definition.json", {"connectionId": SOURCE_CONNECTION, "workspaceId": "external"})],
    )
    assert source_endpoint.paths == [("POST", TARGET_CREATE)]


def test_strict_references_can_be_requested_for_one_client(pair):
    source, _, source_endpoint, _ = pair()
    migrated, warnings = analytics.migrate_items(
        source, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        items=[ITEM], item_type="Notebook", id_map={}, strict_references=True,
        parts_by_id={SOURCE_ID: [part("definition.json", {"connectionId": SOURCE_CONNECTION})]},
    )
    assert not migrated and SOURCE_CONNECTION in warnings[0]
    assert not source_endpoint.calls


def test_hidden_shortcut_connection_cannot_pass_on_source_visibility(pair, shortcut_kind):
    source_path = f"workspaces/{SOURCE_WS}/{shortcut_kind}/{SOURCE_ID}/shortcuts"
    source, target, _, target_endpoint = pair({
        ("GET", source_path): {"value": [{"path": "Files", "name": "Unlisted", "target": {
            "adlsGen2": {"connectionId": SOURCE_CONNECTION, "location": "https://storage"},
        }}]},
    })
    copy = shortcuts.copy_shortcuts if shortcut_kind == "items" else shortcuts.copy_table_shortcuts
    lifecycle = Lifecycle(attempt_id="attempt", source_workspace=SOURCE_WS).item(
        SOURCE_ID, "Store", "Lakehouse",
    )

    count, warnings = copy(
        source, SOURCE_WS, SOURCE_ID, TARGET_WS, TARGET_ID, ID_MAP, target_client=target, lifecycle=lifecycle,
        cross_tenant=True,
    )

    assert count == 0
    assert SOURCE_CONNECTION in warnings[0] and "destination mapping" in warnings[0]
    assert not target_endpoint.calls
    assert lifecycle.owner.get(SOURCE_ID).steps["shortcuts"].state == EvidenceState.FAILED
    assert any(SOURCE_CONNECTION in reason for reason in lifecycle.owner.get(SOURCE_ID).unresolvedReferences)


def test_external_shortcut_workspace_is_verified_as_target_before_create(pair, shortcut_kind):
    source_path = f"workspaces/{SOURCE_WS}/{shortcut_kind}/{SOURCE_ID}/shortcuts"
    source, target, _, target_endpoint = pair({
        ("GET", source_path): {"value": [{"path": "Tables", "name": "External", "target": {
            "oneLake": {"workspaceId": "external", "itemId": "outside-item", "path": "Tables/Table"},
        }}]},
    }, {
        ("GET", "workspaces/external"): httpx.Response(
            403, json={"errorCode": "TargetForbidden", "message": "No destination access"},
        ),
    })
    copy = shortcuts.copy_shortcuts if shortcut_kind == "items" else shortcuts.copy_table_shortcuts

    count, warnings = copy(
        source, SOURCE_WS, SOURCE_ID, TARGET_WS, TARGET_ID, ID_MAP, target_client=target, cross_tenant=True,
    )

    assert count == 0 and "TargetForbidden No destination access" in warnings[0]
    assert target_endpoint.paths == [("GET", "workspaces/external")]


@pytest.mark.parametrize("mapped", [False, True])
def test_airflow_preflight_checks_hidden_connections_before_parent_creates_job(pair, mapped):
    source_path = f"workspaces/{SOURCE_WS}/apacheAirflowJobs/{SOURCE_ID}/files"
    content = json.dumps({"connectionId": SOURCE_CONNECTION, "workspaceId": SOURCE_WS})
    source, target, source_endpoint, target_endpoint = pair({
        ("GET", source_path): {"value": [{"filePath": "config.json"}]},
        ("GET", f"{source_path}/config.json"): httpx.Response(200, content=content),
    }, {("GET", f"workspaces/{TARGET_WS}"): {"id": TARGET_WS}})
    id_map = {**ID_MAP, **({SOURCE_CONNECTION: TARGET_CONNECTION} if mapped else {})}

    if mapped:
        files = airflow.preflight_files(
            source, target_client=target, source_workspace_id=SOURCE_WS, source_job_id=SOURCE_ID,
            cross_tenant=True,
            job_name="Job", id_map=id_map, source_items={},
        )
        assert json.loads(files[0][1]) == {"connectionId": TARGET_CONNECTION, "workspaceId": TARGET_WS}
        assert target_endpoint.paths == [("GET", f"workspaces/{TARGET_WS}")]
    else:
        with pytest.raises(analytics.StrandedReference, match=SOURCE_CONNECTION):
            airflow.preflight_files(
                source, target_client=target, source_workspace_id=SOURCE_WS, source_job_id=SOURCE_ID,
                cross_tenant=True,
                job_name="Job", id_map=id_map, source_items={},
            )
        assert not target_endpoint.calls
    assert source_endpoint.paths == [("GET", source_path), ("GET", f"{source_path}/config.json")]


def test_prepared_airflow_files_with_unmapped_connections_fail_before_any_upload(pair):
    source, target, source_endpoint, target_endpoint = pair()
    count, warnings = airflow.copy_files(
        source, target_client=target, source_workspace_id=SOURCE_WS, source_job_id=SOURCE_ID,
        cross_tenant=True,
        target_workspace_id=TARGET_WS, target_job_id=TARGET_ID, job_name="Job",
        prepared_files=[
            ("dags/main.py", b"pass"),
            ("config.json", json.dumps({"connectionId": SOURCE_CONNECTION}).encode()),
        ],
    )
    assert count == 0
    assert SOURCE_CONNECTION in warnings[0] and "destination mapping" in warnings[0]
    assert not source_endpoint.calls and not target_endpoint.calls


def test_prepared_airflow_file_connections_and_workspaces_are_verified_before_upload(pair):
    target_path = f"workspaces/{TARGET_WS}/apacheAirflowJobs/{TARGET_ID}/files/config.json"
    content = json.dumps({"connectionId": TARGET_CONNECTION, "workspaceId": TARGET_WS}).encode()
    source, target, source_endpoint, target_endpoint = pair({}, {
        ("GET", f"workspaces/{TARGET_WS}"): {"id": TARGET_WS},
        ("PUT", target_path): httpx.Response(204),
    })
    result = airflow.copy_files(
        source, target_client=target, source_workspace_id=SOURCE_WS, source_job_id=SOURCE_ID,
        cross_tenant=True,
        target_workspace_id=TARGET_WS, target_job_id=TARGET_ID, job_name="Job",
        prepared_files=[("config.json", content)], id_map={SOURCE_CONNECTION: TARGET_CONNECTION},
    )
    assert result == (1, [])
    assert not source_endpoint.calls
    assert target_endpoint.paths == [("GET", f"workspaces/{TARGET_WS}"), ("PUT", target_path)]


@pytest.mark.parametrize("adopted", [False, True])
def test_separate_clients_in_same_tenant_can_reuse_external_definition_connections(pair, adopted):
    target_path = TARGET_UPDATE if adopted else TARGET_CREATE
    source, target, source_endpoint, target_endpoint = pair({}, {("POST", target_path): {"id": TARGET_ID}})
    parts = [part("pipeline.json", {"connectionId": SOURCE_CONNECTION, "workspaceId": "external"})]
    migrated, warnings = analytics.migrate_items(
        source, target_client=target, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        items=[ITEM], item_type="DataPipeline", id_map={}, parts_by_id={SOURCE_ID: parts},
        existing_targets={SOURCE_ID: TARGET_ID} if adopted else None,
    )
    assert not warnings and migrated[0].target_id == TARGET_ID
    assert not source_endpoint.calls
    assert target_endpoint.paths == [("POST", target_path)]
    assert target_endpoint.bodies[0]["definition"]["parts"] == parts


def test_separate_clients_in_same_tenant_can_reuse_external_shortcut_connections(pair, shortcut_kind):
    source_path = f"workspaces/{SOURCE_WS}/{shortcut_kind}/{SOURCE_ID}/shortcuts"
    target_path = f"workspaces/{TARGET_WS}/{shortcut_kind}/{TARGET_ID}/shortcuts"
    source, target, source_endpoint, target_endpoint = pair({
        ("GET", source_path): {"value": [{"path": "Files", "name": "Shared", "target": {
            "adlsGen2": {"connectionId": SOURCE_CONNECTION, "location": "https://storage"},
        }}]},
    }, {("POST", target_path): {}})
    copy = shortcuts.copy_shortcuts if shortcut_kind == "items" else shortcuts.copy_table_shortcuts

    assert copy(source, SOURCE_WS, SOURCE_ID, TARGET_WS, TARGET_ID, {}, target_client=target) == (1, [])

    assert source_endpoint.paths == [("GET", source_path)]
    assert target_endpoint.paths == [("POST", target_path)]
    assert target_endpoint.bodies[0]["target"]["adlsGen2"]["connectionId"] == SOURCE_CONNECTION


@pytest.mark.parametrize("preloaded", [False, True])
def test_separate_clients_in_same_tenant_can_reuse_airflow_connections(pair, preloaded):
    source_path = f"workspaces/{SOURCE_WS}/apacheAirflowJobs/{SOURCE_ID}/files"
    target_path = f"workspaces/{TARGET_WS}/apacheAirflowJobs/{TARGET_ID}/files/config.json"
    content = json.dumps({"connectionId": SOURCE_CONNECTION, "workspaceId": "external"}).encode()
    source, target, source_endpoint, target_endpoint = pair({} if preloaded else {
        ("GET", source_path): {"value": [{"filePath": "config.json"}]},
        ("GET", f"{source_path}/config.json"): httpx.Response(200, content=content),
    }, {("PUT", target_path): httpx.Response(204)})

    assert airflow.copy_files(
        source, target_client=target, source_workspace_id=SOURCE_WS, source_job_id=SOURCE_ID,
        target_workspace_id=TARGET_WS, target_job_id=TARGET_ID, job_name="Job",
        prepared_files=[("config.json", content)] if preloaded else None, id_map={}, source_items={},
    ) == (1, [])

    assert source_endpoint.paths == ([] if preloaded else [
        ("GET", source_path), ("GET", f"{source_path}/config.json"),
    ])
    assert target_endpoint.paths == [("PUT", target_path)]
    assert target_endpoint.calls[0].content == content


def test_cross_tenant_flag_does_not_allow_reference_checks_to_be_disabled(pair):
    source, target, source_endpoint, target_endpoint = pair()
    with pytest.raises(analytics.StrandedReference, match=SOURCE_CONNECTION):
        analytics.migrate_definition_item(
            source, target_client=target, cross_tenant=True, strict_references=False,
            source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
            item=ITEM, item_type="Notebook", id_map={},
            parts=[part("definition.json", {"connectionId": SOURCE_CONNECTION})],
        )
    assert not source_endpoint.calls and not target_endpoint.calls


@pytest.mark.parametrize("key", ["itemId", "parentEventhouseItemId", "notebookId"])
def test_shared_guard_discovers_unlisted_source_item_references(pair, key):
    _, target, _, target_endpoint = pair()
    with pytest.raises(analytics.StrandedReference, match="unmapped-item"):
        analytics.validate_cross_tenant_references(
            [part("properties.json", {key: "unmapped-item"})],
            source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS, id_map=ID_MAP,
            target_client=target,
        )
    assert not target_endpoint.calls


@pytest.mark.parametrize("key", ["connection", "connectionId", "dataConnectionId"])
def test_shared_guard_discovers_hidden_connections_without_source_inventory(pair, key):
    _, target, _, target_endpoint = pair()
    with pytest.raises(analytics.StrandedReference, match=SOURCE_CONNECTION):
        analytics.validate_cross_tenant_references(
            [part("snowflake.json", {"properties": {key: SOURCE_CONNECTION}})],
            source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS, id_map=ID_MAP,
            target_client=target,
        )
    assert not target_endpoint.calls


def test_shared_guard_checks_source_workspace_and_item_pairs_before_rewriting(pair):
    _, target, _, target_endpoint = pair()
    with pytest.raises(analytics.StrandedReference, match="unmapped-item"):
        analytics.validate_cross_tenant_references(
            [part("shortcut.json", {"target": {"oneLake": {
                "workspaceId": SOURCE_WS, "itemId": "unmapped-item",
            }}})],
            source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS, id_map=ID_MAP,
            target_client=target,
        )
    assert not target_endpoint.calls


def test_shared_guard_uses_known_endpoint_aliases_in_non_json_parts(pair):
    _, target, _, target_endpoint = pair()
    with pytest.raises(analytics.StrandedReference, match="Source warehouse"):
        analytics.validate_cross_tenant_references(
            [part("dag.py", "connect('old-endpoint.fabric.microsoft.com')")],
            source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS, id_map=ID_MAP,
            target_client=target, source_items={"warehouse": {
                "id": "warehouse", "type": "Warehouse", "displayName": "Source warehouse",
                "properties": {"connectionString": "old-endpoint.fabric.microsoft.com"},
            }},
        )
    assert not target_endpoint.calls


def test_shared_guard_preserves_source_parts_and_checks_only_rewritten_workspaces(pair):
    _, target, _, target_endpoint = pair({}, {
        ("GET", f"workspaces/{TARGET_WS}"): {"id": TARGET_WS},
        ("GET", "workspaces/external"): {"id": "external"},
    })
    parts = [
        part(".platform", {"workspaceId": SOURCE_WS, "itemId": "discarded-logical-id"}),
        part("definition.json", {
            "workspaceId": SOURCE_WS, "itemId": "source-store", "connectionId": SOURCE_CONNECTION,
            "external": {"workspaceId": "external", "itemId": "external-item"},
        }),
    ]
    original = json.dumps(parts)

    analytics.validate_cross_tenant_references(
        parts, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        id_map={**ID_MAP, SOURCE_CONNECTION: TARGET_CONNECTION}, target_client=target,
    )

    assert json.dumps(parts) == original
    assert target_endpoint.paths == [("GET", "workspaces/external"), ("GET", f"workspaces/{TARGET_WS}")]


def test_shared_guard_rejects_inconsistent_destination_workspace_mapping(pair):
    _, target, _, target_endpoint = pair()
    with pytest.raises(FabricError, match="must map to destination workspace"):
        analytics.validate_cross_tenant_references(
            [part("definition.json", {"workspaceId": SOURCE_WS})],
            source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
            id_map={SOURCE_WS: "wrong-destination"}, target_client=target,
        )
    assert not target_endpoint.calls


def test_shared_guard_preserves_failed_destination_workspace_read(pair):
    _, target, _, target_endpoint = pair({}, {
        ("GET", f"workspaces/{TARGET_WS}"): httpx.Response(
            403, json={"errorCode": "DestinationDenied", "message": "Cannot read the new workspace"},
        ),
    })
    with pytest.raises(FabricApiError) as raised:
        analytics.validate_cross_tenant_references(
            PARTS, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
            id_map=ID_MAP, target_client=target,
        )
    assert raised.value.error_code == "DestinationDenied"
    assert raised.value.detail == "Cannot read the new workspace"
    assert target_endpoint.paths == [("GET", f"workspaces/{TARGET_WS}")]


def model_parts(*, members=None, data_sources=None):
    role = {
        "name": "Region filter",
        "modelPermission": "read",
        "tablePermissions": [{"name": "Sales", "filterExpression": "[Region] = USERPRINCIPALNAME()"}],
    }
    if members is not None:
        role["members"] = members
    model = {"roles": [role], "tables": []}
    if data_sources is not None:
        model["dataSources"] = data_sources
    return [part("model.bim", {"compatibilityLevel": 1702, "model": model})]


@pytest.mark.parametrize("adopted", [False, True])
@pytest.mark.parametrize("members", [None, []])
def test_cross_tenant_model_preserves_rls_when_no_principal_members_are_present(pair, adopted, members):
    parts = model_parts(members=members)
    target_path = TARGET_UPDATE if adopted else TARGET_CREATE
    source, target, source_endpoint, target_endpoint = pair({}, {("POST", target_path): {"id": TARGET_ID}})
    lifecycle = Lifecycle(attempt_id="attempt", source_workspace=SOURCE_WS).item(
        SOURCE_ID, "Model", "SemanticModel",
    )
    result = analytics.migrate_definition_item(
        source, target_client=target, cross_tenant=True, source_workspace_id=SOURCE_WS,
        target_workspace_id=TARGET_WS, item=ITEM, item_type="SemanticModel", id_map={},
        parts=parts, target_id=TARGET_ID if adopted else None, lifecycle=lifecycle,
    )
    assert result.parts == tuple(parts)
    assert result.warnings == ()
    assert not source_endpoint.calls
    assert target_endpoint.bodies[0]["definition"] == {"format": "TMSL", "parts": parts}
    assert lifecycle.owner.get(SOURCE_ID).steps["identity"].state == EvidenceState.SUCCEEDED


@pytest.mark.parametrize("adopted", [False, True])
def test_cross_tenant_model_omits_member_assignments_without_modifying_rls(pair, adopted):
    parts = model_parts(members=[{"memberName": "source-user", "memberId": "source-principal"}])
    original = json.dumps(parts)
    target_path = TARGET_UPDATE if adopted else TARGET_CREATE
    source, target, source_endpoint, target_endpoint = pair({}, {("POST", target_path): {"id": TARGET_ID}})
    lifecycle = Lifecycle(attempt_id="attempt", source_workspace=SOURCE_WS).item(
        SOURCE_ID, "Model", "SemanticModel",
    )
    result = analytics.migrate_definition_item(
        source, target_client=target, cross_tenant=True, source_workspace_id=SOURCE_WS,
        target_workspace_id=TARGET_WS, item=ITEM, item_type="SemanticModel", id_map={},
        parts=parts, target_id=TARGET_ID if adopted else None, lifecycle=lifecycle,
    )
    sent = decode_json_part(target_endpoint.bodies[0]["definition"]["parts"][0]["payload"])
    role = sent["model"]["roles"][0]
    assert "members" not in role
    assert role["tablePermissions"] == [{
        "name": "Sales", "filterExpression": "[Region] = USERPRINCIPALNAME()",
    }]
    assert any("memberships" in warning for warning in result.warnings)
    assert all("source-user" not in warning for warning in result.warnings)
    assert json.dumps(parts) == original
    assert not source_endpoint.calls
    assert lifecycle.owner.get(SOURCE_ID).steps["identity"].state == EvidenceState.SUCCEEDED


def test_kql_schema_principal_commands_are_not_imported():
    with pytest.raises(analytics.IdentityBindingError, match="role assignments"):
        analytics.validate_cross_tenant_identities([
            part("DatabaseSchema.kql", ".add database Orders admins ('aadapp=source-principal')"),
        ], item_type="KQLDatabase")


def test_partial_external_workspace_mapping_refuses_unmapped_item_in_that_workspace(pair):
    source, target, _, target_endpoint = pair()
    with pytest.raises(analytics.StrandedReference, match="unmapped-notebook"):
        analytics.migrate_definition_item(
            source, target_client=target, cross_tenant=True,
            source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
            item=ITEM, item_type="DataPipeline",
            id_map={SOURCE_WS: TARGET_WS, "external-source": "external-target", "mapped-model": "new-model"},
            parts=[part("pipeline-content.json", {
                "workspaceId": "external-source", "notebookId": "unmapped-notebook",
            })],
        )
    assert not target_endpoint.calls


def test_report_external_model_id_requires_a_mapping_even_without_workspace_field(pair):
    source, target, _, target_endpoint = pair()
    with pytest.raises(analytics.StrandedReference, match="external-model"):
        analytics.migrate_definition_item(
            source, target_client=target, cross_tenant=True,
            source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
            item=ITEM, item_type="Report", id_map={SOURCE_WS: TARGET_WS},
            parts=[part("definition.pbir", {
                "datasetReference": {"byConnection": {"connectionString": "semanticmodelid=external-model"}},
            })],
        )
    assert not target_endpoint.calls


def test_cross_tenant_model_export_requests_inspectable_tmsl_from_source(pair):
    parts = model_parts()
    source, target, source_endpoint, target_endpoint = pair({
        ("POST", SOURCE_EXPORT): {"definition": {"parts": parts}},
    }, {("POST", TARGET_CREATE): {"id": TARGET_ID}})
    result = analytics.migrate_definition_item(
        source, target_client=target, cross_tenant=True, source_workspace_id=SOURCE_WS,
        target_workspace_id=TARGET_WS, item=ITEM, item_type="SemanticModel", id_map={},
    )
    assert source_endpoint.calls[0].url.params["format"] == "TMSL"
    assert target_endpoint.bodies[0]["definition"]["format"] == "TMSL"
    assert result.parts == tuple(parts)


def test_same_tenant_model_keeps_existing_membership_behavior_with_distinct_clients(pair):
    parts = model_parts(members=[{"memberName": "existing-user", "memberId": "existing-principal"}])
    source, target, _, target_endpoint = pair({}, {("POST", TARGET_CREATE): {"id": TARGET_ID}})
    result = analytics.migrate_definition_item(
        source, target_client=target, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        item=ITEM, item_type="SemanticModel", id_map={}, parts=parts,
    )
    assert result.parts == tuple(parts)
    assert target_endpoint.bodies[0]["definition"]["parts"] == parts


@pytest.mark.parametrize(
    "opaque_path", ["definition/roles/RLS.tmdl", "model.abf", "sqldb.dacpac", "model.pbix"],
)
def test_opaque_security_formats_are_refused_rather_than_filtered_or_verified(pair, opaque_path):
    _, target, _, target_endpoint = pair()
    opaque = [part(opaque_path, b"opaque security and RLS bytes")]
    with pytest.raises(analytics.IdentityBindingError, match="cannot be inspected safely"):
        analytics.validate_cross_tenant_references(
            opaque, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS, id_map={},
            target_client=target,
        )
    assert not target_endpoint.calls


@pytest.mark.parametrize("model", [
    {"model": {"roles": {}}},
    {"model": {"roles": [{"name": "Role", "members": "opaque"}]}},
    {"model": {"roles": [None]}},
    {"model": None},
])
def test_unknown_model_role_shapes_are_not_marked_verified(model):
    with pytest.raises(analytics.IdentityBindingError, match="uninspectable"):
        analytics.validate_cross_tenant_identities([part("model.bim", model)], item_type="SemanticModel")


@pytest.mark.parametrize("payload", [
    {"credentialDetails": {"credentials": {"credentialType": "WorkspaceIdentity"}}},
    {"credentialDetails": {"credentials": {
        "credentialType": "ServicePrincipal", "servicePrincipalClientId": "source-app",
        "tenantId": "source-tenant",
    }}},
    {"workspaceIdentity": {"id": "source-identity"}},
    {"authentication": {"servicePrincipalId": "source-principal"}},
    {"authentication": {"type": "ServicePrincipal", "username": "source-app"}},
    {"authentication": {"type": "WorkspaceIdentity"}},
    {"roleAssignments": [{"principal": {"type": "Group", "id": "source-group"}, "role": "Viewer"}]},
])
def test_structured_source_identity_bindings_refuse_manual_definition_paths(pair, payload):
    _, target, _, target_endpoint = pair()
    with pytest.raises(analytics.IdentityBindingError, match="destination"):
        analytics.validate_cross_tenant_references(
            [part("configuration.json", payload)], source_workspace_id=SOURCE_WS,
            target_workspace_id=TARGET_WS, id_map={}, target_client=target,
        )
    assert not target_endpoint.calls


@pytest.mark.parametrize("source_config", [
    {"type": "provider", "account": "source-account", "impersonationMode": "impersonateAccount"},
    {"type": "provider", "password": "source-password"},
    {"type": "structured", "credential": {"Username": "source-account", "AuthenticationKind": "Windows"}},
])
def test_model_data_source_credentials_are_not_replayed(pair, source_config):
    _, target, _, target_endpoint = pair()
    with pytest.raises(analytics.IdentityBindingError, match="configure the destination connection"):
        analytics.validate_cross_tenant_references(
            model_parts(data_sources=[{"name": "Source", **source_config}]),
            source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS, id_map={},
            target_client=target, item_type="SemanticModel",
        )
    assert not target_endpoint.calls


def test_identity_refusal_does_not_log_sensitive_definition_payload(pair, caplog):
    secret = "sensitive-source-value-must-not-be-logged"
    parts = [part("pipeline.json", {"credentialDetails": {"credentials": {
        "credentialType": "ServicePrincipal", "servicePrincipalSecret": secret,
    }}})]
    source, target, source_endpoint, target_endpoint = pair()
    migrated, warnings = analytics.migrate_items(
        source, target_client=target, cross_tenant=True, source_workspace_id=SOURCE_WS,
        target_workspace_id=TARGET_WS, items=[ITEM], item_type="DataPipeline", id_map={},
        parts_by_id={SOURCE_ID: parts},
    )
    assert not migrated and "credential binding" in warnings[0]
    assert secret not in caplog.text and secret not in " ".join(warnings)
    assert not source_endpoint.calls and not target_endpoint.calls


def test_runtime_identity_in_code_is_unknown_without_regex_filtering(pair):
    parts = [part("notebook.py", "print('members, principalId, workspaceIdentity are data here')")]
    source, target, _, target_endpoint = pair({}, {("POST", TARGET_CREATE): {"id": TARGET_ID}})
    lifecycle = Lifecycle(attempt_id="attempt", source_workspace=SOURCE_WS).item(
        SOURCE_ID, "Code", "Notebook",
    )
    result = analytics.migrate_definition_item(
        source, target_client=target, cross_tenant=True, source_workspace_id=SOURCE_WS,
        target_workspace_id=TARGET_WS, item=ITEM, item_type="Notebook", id_map={},
        parts=parts, lifecycle=lifecycle,
    )
    assert result.parts == tuple(parts)
    assert "not verified" in result.warnings[0]
    assert target_endpoint.bodies[0]["definition"]["parts"] == parts
    assert lifecycle.owner.get(SOURCE_ID).steps["identity"].state == EvidenceState.UNKNOWN
    assert "identity" in lifecycle.owner.get(SOURCE_ID).required


def test_nonsecurity_members_metadata_is_preserved():
    parts = [part("notebook-metadata.json", {"display": {"members": ["sample-value"]}})]
    assert analytics.validate_cross_tenant_identities(parts) == []
    assert parts == [part("notebook-metadata.json", {"display": {"members": ["sample-value"]}})]


def test_airflow_identity_bindings_are_refused_before_upload(pair):
    content = json.dumps({"workspaceIdentity": {"id": "source-identity"}}).encode()
    source, target, source_endpoint, target_endpoint = pair()
    copied, warnings = airflow.copy_files(
        source, target_client=target, cross_tenant=True, source_workspace_id=SOURCE_WS,
        source_job_id=SOURCE_ID,
        target_workspace_id=TARGET_WS, target_job_id=TARGET_ID, job_name="Airflow",
        prepared_files=[("config.json", content)],
    )
    assert copied == 0 and "workspace identity" in warnings[0]
    assert not source_endpoint.calls and not target_endpoint.calls
