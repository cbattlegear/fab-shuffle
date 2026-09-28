"""Exercise captured inputs through migration helpers and the real recovery coordinator."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from fabshuffle.bcdr import adapters
from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.capture import ItemCapture, _dependencies, capture_item, capture_workspaces, make_payload
from fabshuffle.bcdr.catalog import CapturedGeneration
from fabshuffle.bcdr.contracts import ItemIdentity, PayloadPurpose, RecoveryMode
from fabshuffle.bcdr.coordinator import RecoveryCoordinator
from fabshuffle.bcdr.service import ContinueDrTestRequest, EndDrTestRequest, PlanRequest, StartDrTestRequest
from fabshuffle.fabric import analytics, migration_refs, spark
from fabshuffle.fabric.client import FabricApiError, FabricError
from fabshuffle.fabric.definitions import decode_json_part, part
from tests.test_bcdr_adapters import (
    DEPENDENCY,
    REPLACEMENT,
    SOURCE,
    TARGET,
    TENANT,
    Destination,
    apply,
    captured,
)
from tests.test_bcdr_capture import Readers, SourceClient, recovery_set
from tests.test_bcdr_contracts import guid
from tests.test_bcdr_coordinator import lakehouse_setup as lakehouse_setup
from tests.test_bcdr_coordinator import now, proofs
from tests.test_bcdr_coordinator import system as system
from tests.test_bcdr_coordinator import (
    test_independent_lakehouse_real_copy_finalizes_metadata_then_requires_new_proof as first_copy,
)


@pytest.mark.parametrize("root", [
    "https://onelake.dfs.fabric.microsoft.com/Production%20Workspace/Orders.Lakehouse/",
    "https://onelake.blob.fabric.microsoft.com/Production Workspace/Orders.Lakehouse/",
    "abfss://Production%20Workspace@onelake.dfs.fabric.microsoft.com/Orders.Lakehouse/",
    f"https://onelake.dfs.fabric.microsoft.com/{SOURCE}/{DEPENDENCY}/",
])
def test_named_and_guid_onelake_roots_use_migration_aliases(root):
    uri = root + "Tables/orders"
    item, payloads = captured("Notebook", {
        "nbformat": 4, "nbformat_minor": 5, "metadata": {},
        "cells": [{"cell_type": "code", "metadata": {}, "outputs": [], "execution_count": None,
                   "source": [f'data = spark.read.format("delta").load("{uri}")']}],
    }, path="notebook-content.ipynb")
    item = item.model_copy(update={"definition_format": "ipynb"})
    lakehouse = item.model_copy(update={
        "identity": item.identity.model_copy(update={"item_id": DEPENDENCY}),
        "item_type": "Lakehouse", "display_name": "Orders", "payload_ids": (),
    })
    client = Destination()
    result = apply(
        item, payloads, client, source_items=[item, lakehouse],
        item_mappings={(TENANT, SOURCE, DEPENDENCY): ItemIdentity(
            tenant_id=TENANT, workspace_id=TARGET, item_id=REPLACEMENT,
        )},
        source_workspace_names={(TENANT, SOURCE): "Production Workspace"},
    )
    assert result.metadata_applied
    text = json.dumps(decode_json_part(client.definition[0]["payload"]))
    assert uri not in text and TARGET in text and REPLACEMENT in text
    mappings = migration_refs.onelake_aliases(SOURCE, "Production Workspace", {
        "id": DEPENDENCY, "displayName": "Orders", "type": "Lakehouse",
    }, TARGET, REPLACEMENT)
    assert mappings[root] in text


def test_named_onelake_dependency_is_captured_and_unmapped_root_cannot_pass():
    uri = "abfss://Production@onelake.dfs.fabric.microsoft.com/Orders.Lakehouse/Tables/orders"
    item, payloads = captured("Notebook", {"path": uri})
    lake = item.model_copy(update={"identity": item.identity.model_copy(update={"item_id": DEPENDENCY}),
                                  "item_type": "Lakehouse", "display_name": "Orders", "payload_ids": ()})
    edges = _dependencies((ItemCapture(item, payloads), ItemCapture(lake, ())),
                          workspace_names={SOURCE: "Production"})
    assert any(edge.consumer == item.identity and edge.prerequisite == lake.identity for edge in edges)
    client = Destination()
    result = apply(item, payloads, client, source_items=[item, lake])
    assert not result.metadata_applied and not client.mutations
    assert any("OneLake" in reason for reason in result.diagnostics)


@pytest.mark.parametrize("uri", [
    "https://onelake.dfs.fabric.microsoft.com/Production/Orders.Lakehouse",
    "abfss://Production@onelake.dfs.fabric.microsoft.com/Orders.Lakehouse",
])
def test_unqualified_onelake_root_without_trailing_slash_is_refused(uri):
    item, payloads = captured("Notebook", {"path": uri})
    client = Destination()
    result = apply(item, payloads, client)
    assert not result.metadata_applied and not client.mutations
    assert any("OneLake" in reason for reason in result.diagnostics)


def test_conflicting_named_onelake_aliases_are_not_guessed():
    item, payloads = captured("Notebook", {"path": (
        "https://onelake.dfs.fabric.microsoft.com/Production/Orders.Lakehouse/Tables/orders"
    )})
    lakes = [
        item.model_copy(update={
            "identity": item.identity.model_copy(update={"item_id": guid()}),
            "item_type": "Lakehouse", "display_name": "Orders", "payload_ids": (),
        }) for _ in range(2)
    ]
    client = Destination()
    result = apply(
        item, payloads, client, source_items=[item, *lakes],
        source_workspace_names={(TENANT, SOURCE): "Production"},
        item_mappings={
            (TENANT, SOURCE, lake.identity.item_id): ItemIdentity(
                tenant_id=TENANT, workspace_id=TARGET, item_id=guid(),
            ) for lake in lakes
        },
    )
    assert not result.metadata_applied and not client.mutations
    assert any("Conflicting qualified mappings" in reason for reason in result.diagnostics)


def test_kql_consumer_gets_coordinator_query_and_ingestion_uri_mappings():
    uri, new_uri = "https://source.example.kusto.windows.net", "https://target.example.kusto.windows.net"
    database, _ = captured("KQLDatabase", {})
    database = database.model_copy(update={
        "identity": database.identity.model_copy(update={"item_id": DEPENDENCY}),
        "properties": {"queryServiceUri": uri, "ingestionServiceUri": uri.replace("https://", "https://ingest-")},
        "display_name": "KQL source", "payload_ids": (),
    })
    target = ItemIdentity(tenant_id=TENANT, workspace_id=TARGET, item_id=REPLACEMENT)
    reader = Mock()
    reader.get.return_value = {"id": REPLACEMENT, "type": "KQLDatabase", "properties": {
        "queryServiceUri": new_uri, "ingestionServiceUri": new_uri.replace("https://", "https://ingest-"),
    }}
    catalog = Mock()
    catalog.list_records.return_value = ()
    pairs = RecoveryCoordinator._endpoint_pairs(
        SimpleNamespace(destination=reader, catalog=catalog),
        SimpleNamespace(snapshot=SimpleNamespace(items=(database,))),
        {database.identity.key: SimpleNamespace(target=target)},
    )
    assert {row.endpoint_kind for row, _ in pairs} == {"kql_query_uri", "kql_ingestion_uri"}
    item, payloads = captured("KQLQueryset", {"clusterUri": uri, "database": DEPENDENCY})
    client = Destination()
    result = apply(item, payloads, client, source_items=[item, database], endpoint_mappings=pairs,
                   item_mappings={(TENANT, SOURCE, DEPENDENCY): target})
    assert result.metadata_applied
    assert new_uri in json.dumps(decode_json_part(client.definition[0]["payload"]))


def test_bypath_report_dependency_orders_model_before_report(system):
    source = system.captured.items[0]
    model_id = source.identity.model_copy(update={"item_id": "ffffffff-ffff-ffff-ffff-ffffffffffff"})
    report_id = source.identity.model_copy(update={"item_id": "00000000-0000-0000-0000-000000000001"})
    model_part = make_payload(model_id, "model.bim", b'{"model":{"roles":[]}}', PayloadPurpose.DEFINITION)
    report_part = make_payload(report_id, "definition.pbir",
                               b'{"datasetReference":{"byPath":{"path":"../Sales.SemanticModel"}}}',
                               PayloadPurpose.DEFINITION)
    model = source.model_copy(update={
        "identity": model_id, "item_type": "SemanticModel",
        "display_name": "Sales", "payload_ids": (model_part.descriptor.payload_id,),
    })
    report = source.model_copy(update={"identity": report_id, "item_type": "Report",
                                       "payload_ids": (report_part.descriptor.payload_id,)})
    edges = _dependencies((ItemCapture(model, (model_part,)), ItemCapture(report, (report_part,))))
    assert any(edge.consumer == report_id and edge.prerequisite == model_id for edge in edges)
    generation = CapturedGeneration(system.captured.model_copy(update={
        "items": (model, report), "dependencies": edges,
        "payloads": (model_part.descriptor, report_part.descriptor),
    }), (model_part, report_part))
    plan = system.c._plan(generation, PlanRequest(capacity_routes=system.request.capacity_routes))
    assert plan.executable_order.index(("create", *model_id.key.split("/"))) < (
        plan.executable_order.index(("create", *report_id.key.split("/")))
    )


@pytest.mark.parametrize("reference", [
    {"byPath": "../Sales.SemanticModel"},
    {"byPath": {"path": 42}},
    {"byPath": {"path": "../Sales.SemanticModel"}, "byConnection": {"connectionString": "model"}},
])
def test_malformed_or_competing_pbir_bindings_are_explicit_failures(reference):
    models = [{"id": guid(), "workspaceId": SOURCE, "type": "SemanticModel", "displayName": "Sales"}]
    with pytest.raises(FabricError):
        analytics.report_model_source(
            [part("definition.pbir", {"datasetReference": reference})], models, SOURCE,
        )


def test_ambiguous_pbir_model_names_are_not_guessed():
    parts = [part("definition.pbir", {"datasetReference": {"byPath": {"path": "../Sales.SemanticModel"}}})]
    models = [
        {"id": guid(), "workspaceId": SOURCE, "type": "SemanticModel", "displayName": "Sales"}
        for _ in range(2)
    ]
    with pytest.raises(FabricError, match="exact captured semantic model"):
        analytics.report_model_source(parts, models, SOURCE)


@pytest.mark.parametrize("metadata", [
    "not-json", [], {"formatVersion": analytics.CICD_DATAFLOW_FORMAT_VERSION},
])
def test_bad_or_supported_dataflow_metadata_cannot_be_labeled_unsupported(metadata):
    definition = {"parts": [part(analytics.QUERY_METADATA_PART, metadata)]}
    if isinstance(metadata, dict):
        parts, reason = analytics.classify_dataflow_definition(
            definition, {"displayName": "Orders"}, strict=True,
        )
        assert parts and reason is None
    else:
        with pytest.raises(FabricError, match="query metadata"):
            analytics.classify_dataflow_definition(definition, {"displayName": "Orders"}, strict=True)


@pytest.mark.parametrize("version", ["legacy", None])
def test_dataflow_subtype_uses_the_migration_classifier_without_stopping_capture(version):
    parts = [part(analytics.QUERY_METADATA_PART, {"formatVersion": version})] if version else [
        part("mashup.pq", "let source = 1 in source"),
    ]
    client = SourceClient("Dataflow", parts)
    _, reason = analytics.classify_dataflow(client, SOURCE, {"id": guid(), "displayName": "Orders"})
    capture = capture_workspaces(client, recovery_set(), tokens=object(), readers=Readers())
    capture.snapshot.require_publishable(recovery_set())
    item = capture.snapshot.items[0]
    assert item.properties["bcdr"]["inventory_only"] and not item.capture_complete
    assert reason == item.properties["bcdr"]["unsupported_reason"]
    assert not any("connections" in path for _, path, _ in client.calls)


@pytest.mark.parametrize("status,code", [
    (403, "InsufficientPrivileges"), (404, "NotFound"), (400, "UnknownError"),
])
def test_dataflow_inconclusive_service_failure_is_not_a_subtype_exclusion(status, code):
    item, _ = captured("Dataflow")
    client = Mock()
    error = FabricApiError(
        "POST", "getDefinition", status, json.dumps({"errorCode": code, "message": "Inspect me"}),
    )
    client.post.side_effect = error
    with pytest.raises(FabricApiError) as observed:
        capture_item(client, item.identity, "Dataflow", readers=Mock())
    assert observed.value is error
    client.get.assert_not_called()


def test_definite_unsupported_dataflow_operation_remains_inventory_only():
    client = SourceClient("Dataflow")
    client.post = Mock(side_effect=FabricApiError(
        "POST", "getDefinition", 400,
        '{"errorCode":"OperationNotSupportedForItem","message":"Not a CI/CD item"}',
    ))
    capture = capture_workspaces(client, recovery_set(), tokens=object(), readers=Readers())
    capture.snapshot.require_publishable(recovery_set())
    assert capture.snapshot.items[0].properties["bcdr"]["inventory_only"]


def test_captured_custom_pools_and_settings_precede_environment_and_are_reused(system, monkeypatch):
    pool = {"id": guid(), "name": "Captured pool", "type": "Workspace", "nodeFamily": "MemoryOptimized",
            "nodeSize": "Small", "autoScale": {"enabled": True, "minNodeCount": 1, "maxNodeCount": 2},
            "dynamicExecutorAllocation": {"enabled": True, "minExecutors": 1, "maxExecutors": 2}}
    settings = {"pool": {"defaultPool": {"id": pool["id"], "name": pool["name"], "type": "Workspace"}},
                "environment": {"name": "A notebook"}, "automaticLog": {"enabled": False}}
    original_capture = system.c.capture
    original_handler, original_apply = system.estate.handler, system.c.apply
    pools, target_settings = {}, {}
    creations, mapped = [], []

    def capture(*args, **kwargs):
        generation = original_capture(*args, **kwargs)
        workspace = generation.snapshot.workspaces[0].model_copy(update={
            "properties": {"bcdr": {"spark_pools": [pool], "spark_settings": settings}},
        })
        environment = generation.snapshot.items[0].model_copy(update={
            "item_type": "Environment", "properties": {"bcdr": {"environment": {
                "staging_compute": {"instancePool": {"id": pool["id"], "name": pool["name"]}},
            }}},
        })
        return CapturedGeneration(generation.snapshot.model_copy(update={
            "workspaces": (workspace,), "items": (environment,),
        }), ())

    def handler(request):
        path = request.url.path.removeprefix("/v1/")
        assert not path.startswith(f"workspaces/{system.captured.workspaces[0].identity.workspace_id}/")
        if path.endswith("/spark/pools"):
            if request.method == "POST":
                value = {"id": guid(), "type": "Workspace", **json.loads(request.content)}
                pools[value["id"]] = value
                creations.append(value)
                return httpx.Response(201, json=value)
            return httpx.Response(200, json={"value": list(pools.values())})
        if path.endswith("/spark/settings"):
            if request.method == "PATCH":
                target_settings.update(json.loads(request.content))
            return httpx.Response(200, json=target_settings)
        if "/environments/" in path:
            return httpx.Response(200, json=system.estate.items[path.split("/")[-1]])
        return original_handler(request)

    def apply_item(client, item, payloads, **kwargs):
        endpoints = kwargs["endpoint_mappings"]
        assert len(creations) == 1
        assert target_settings["pool"]["defaultPool"]["id"] == creations[0]["id"]
        assert [(entry.endpoint_id, value) for entry, value in endpoints] == [
            (pool["id"], creations[0]["id"]),
        ]
        compute = adapters._environment_preflight(client, item, [], kwargs["target_workspace"], dict(
            (entry.endpoint_id, value) for entry, value in endpoints
        ))
        assert compute["instancePool"]["name"] == "Captured pool"
        assert compute["customLivePoolSupport"] == "Disabled"
        mapped.append(compute)
        return original_apply(client, item, payloads, **kwargs)

    monkeypatch.setattr(system.c.destination.client._http, "_transport", httpx.MockTransport(handler))
    system.c.capture, system.c.apply = capture, apply_item
    spy = Mock(wraps=spark.copy_pools)
    monkeypatch.setattr(spark, "copy_pools", spy)
    first = system.service.synchronize(system.request)
    assert first.groups[0].metadata_applied and mapped
    assert target_settings["environment"] == {"name": "A notebook"}
    assert len(system.catalog.list_records("spark-pools")) == 1
    second = system.service.synchronize(system.request)
    assert second.groups[0].metadata_applied
    assert len(creations) == 1 and spy.call_count == 1
    pools[creations[0]["id"]]["nodeSize"] = "Large"
    before = len(mapped)
    with pytest.raises(RecoveryBlocked, match="drifted"):
        system.service.synchronize(system.request)
    assert len(mapped) == before and len(creations) == 1


def test_second_protected_lakehouse_test_verifies_retained_copy_without_writing(
    system, lakehouse_setup, tmp_path, monkeypatch,
):
    source, tokens = system.c.source, system.c.source_tokens
    first_copy(system, lakehouse_setup, tmp_path, monkeypatch, owners_only=True)
    record = system.c.runtime.get("lifecycle", "dr-test")
    system.service.end_dr_test(EndDrTestRequest(test_id=record["test_id"], park=False))
    system.c.source, system.c.source_tokens = source, tokens
    original = system.c.capture

    def capture(*args, **kwargs):
        prior = original(*args, **kwargs)
        return CapturedGeneration(prior.snapshot.model_copy(update={
            "generation_id": guid(), "parent_generation_id": kwargs.get("parent_generation_id"),
        }), prior.payloads)

    system.c.capture = capture
    synced = system.service.synchronize(system.request)
    lake, _ = lakehouse_setup
    written, calls = len(lake.writes), len(lake.calls)
    tested = system.service.start_dr_test(StartDrTestRequest(
        generation_id=synced.generation_id, group_ids=(synced.groups[0].group_id,),
    ))
    assert tested.mode == RecoveryMode.TESTING and not system.catalog.pending_operations()
    assert len(lake.writes) == written
    assert all(system.captured.workspaces[0].identity.workspace_id not in request.url.path
               for request in lake.calls[calls:])
    evidence = tuple(
        row.model_copy(update={"observed_at": now()}) for row in proofs(system, synced.generation_id)
    )
    done = system.service.continue_dr_test(ContinueDrTestRequest(
        test_id=tested.details["dr_test"]["test_id"], readiness=evidence,
    ))
    assert done.details["dr_test"]["results"][0]["outcome"] == "passed"
    assert len(lake.writes) == written and not system.catalog.pending_operations()
    assert any(row.kind == "lakehouse-copy-verify" for row in system.catalog.operations())
    original_data = bytes(lake.target["Files/input.txt"])
    lake.target["Files/input.txt"] = b"unapproved change"
    with pytest.raises(RecoveryBlocked, match="Revalidate the retained data copy"):
        system.service.continue_dr_test(ContinueDrTestRequest(
            test_id=tested.details["dr_test"]["test_id"], readiness=evidence,
        ))
    assert not system.catalog.pending_operations() and len(lake.writes) == written
    lake.target["Files/input.txt"] = original_data
    revalidated = system.service.continue_dr_test(ContinueDrTestRequest(
        test_id=tested.details["dr_test"]["test_id"],
    ))
    assert revalidated.details["dr_test"]["results"][0]["outcome"] == "not_tested"
    fresh = tuple(
        row.model_copy(update={"observed_at": now()}) for row in proofs(system, synced.generation_id)
    )
    approved = system.service.continue_dr_test(ContinueDrTestRequest(
        test_id=tested.details["dr_test"]["test_id"], readiness=fresh,
    ))
    assert approved.details["dr_test"]["results"][0]["outcome"] == "passed"
    assert len(lake.writes) == written
