"""Picking up a lakehouse's SQL analytics endpoint.

Fabric creates the endpoint alongside the lakehouse, asynchronously, so the item read back
straight after creation usually reports no endpoint at all. Recording it only at that moment
left the endpoint's ids out of the map entirely, which sent semantic models reading from the
workspace being migrated away from, and later made them look as though the endpoint had
failed to migrate.
"""

from __future__ import annotations

import pytest

from fabshuffle import orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric import analytics
from fabshuffle.fabric.analytics import dangling_references
from fabshuffle.fabric.definitions import decode_payload, part
from fabshuffle.run import MigrationRun

SOURCE_ENDPOINT_ID = "ep-source"
TARGET_ENDPOINT_ID = "ep-target"
SOURCE_CONNECTION = "src.datawarehouse.fabric.microsoft.com"
TARGET_CONNECTION = "dst.datawarehouse.fabric.microsoft.com"


def make_ctx():
    plan = orchestrator.MigrationPlan(
        capacity_id="cap",
        capacity_name="F64",
        capacity_region="westus",
        source_workspace_id="ws-source",
        source_workspace_name="src",
        target_workspace_name="dst",
    )
    return orchestrator._Context(
        client=object(),
        tokens=object(),
        principal=ServicePrincipal("t", "c", "s"),
        plan=plan,
        run=MigrationRun(source_workspace_name="src", capacity_name="F64"),
        scratch_dir=None,
    )


SOURCE = {"id": SOURCE_ENDPOINT_ID, "connectionString": SOURCE_CONNECTION}
READY = {"id": TARGET_ENDPOINT_ID, "connectionString": TARGET_CONNECTION}


def test_both_identifiers_are_recorded_when_the_endpoint_exists():
    ctx = make_ctx()
    orchestrator._map_sql_endpoint(ctx, SOURCE, READY)

    # A semantic model names its endpoint by connection string in one place and by item id in
    # another, so missing either leaves it pointing at the original.
    assert ctx.id_map[SOURCE_ENDPOINT_ID] == TARGET_ENDPOINT_ID
    assert ctx.id_map[SOURCE_CONNECTION] == TARGET_CONNECTION


def test_an_endpoint_that_has_not_appeared_yet_records_nothing():
    ctx = make_ctx()
    orchestrator._map_sql_endpoint(ctx, SOURCE, {})

    assert ctx.id_map == {}


def test_a_later_read_fills_in_what_the_first_could_not():
    """The lakehouse phase calls this at creation and the shortcut phase calls it again."""
    ctx = make_ctx()
    orchestrator._map_sql_endpoint(ctx, SOURCE, {})
    assert ctx.id_map == {}

    orchestrator._map_sql_endpoint(ctx, SOURCE, READY)
    assert ctx.id_map[SOURCE_ENDPOINT_ID] == TARGET_ENDPOINT_ID


def test_a_partly_provisioned_endpoint_records_what_it_has():
    ctx = make_ctx()
    orchestrator._map_sql_endpoint(ctx, SOURCE, {"connectionString": TARGET_CONNECTION})

    assert ctx.id_map == {SOURCE_CONNECTION: TARGET_CONNECTION}


# ------------------------------------------------------- not a stranded item


def test_an_unmapped_sql_endpoint_is_reported():
    source_items = {
        SOURCE_ENDPOINT_ID: {
            "id": SOURCE_ENDPOINT_ID,
            "displayName": "CloneTest",
            "type": "SQLEndpoint",
        }
    }
    parts = [part("model.bim", f'{{"expression": "Sql.Database(\\"x\\", \\"{SOURCE_ENDPOINT_ID}\\")"}}')]

    assert dangling_references(parts, {}, source_items) == ["SQLEndpoint 'CloneTest'"]


def test_a_real_item_that_did_not_migrate_is_still_reported():
    source_items = {
        "lh-1": {"id": "lh-1", "displayName": "Bronze", "type": "Lakehouse"},
    }
    parts = [part("content.json", '{"artifactId": "lh-1"}')]

    assert dangling_references(parts, {}, source_items) == ["Lakehouse 'Bronze'"]


def test_an_unmapped_mirrored_warehouse_is_reported_too():
    source_items = {
        "mw-1": {"id": "mw-1", "displayName": "Mirror", "type": "MirroredWarehouse"},
    }
    parts = [part("content.json", '{"artifactId": "mw-1"}')]

    assert dangling_references(parts, {}, source_items) == ["MirroredWarehouse 'Mirror'"]


@pytest.mark.parametrize("mapping", [
    {},
    {SOURCE_ENDPOINT_ID: TARGET_ENDPOINT_ID},
    {SOURCE_CONNECTION: TARGET_CONNECTION},
    {SOURCE_ENDPOINT_ID: SOURCE_ENDPOINT_ID},
])
def test_semantic_model_creation_refuses_missing_or_partial_endpoint_mapping(mapping):
    ctx = make_ctx()
    ctx.source_items["lakehouse"] = {
        "id": "lakehouse", "displayName": "Bronze", "type": "Lakehouse",
        "properties": {"sqlEndpointProperties": SOURCE},
    }
    sent = []

    class Client:
        def post(self, *args, **kwargs):
            sent.append(kwargs)
            return {"id": "model-new"}

    parts = [part("model.bim", f'Sql.Database("{SOURCE_CONNECTION}", "{SOURCE_ENDPOINT_ID}")')]
    migrated, warnings = analytics.migrate_items(
        Client(), source_workspace_id="ws-source", target_workspace_id="ws-target",
        items=[{"id": "model", "displayName": "Sales"}], item_type="SemanticModel",
        parts_by_id={"model": parts}, id_map=dict(mapping), source_items=ctx.source_items,
    )
    assert not migrated and not sent
    assert "Bronze" in warnings[0]
    assert "Migrate what it needs" in warnings[0]


def test_semantic_model_with_complete_endpoint_mapping_is_rebound():
    ctx = make_ctx()
    orchestrator._map_sql_endpoint(ctx, SOURCE, READY)
    sent = []

    class Client:
        def post(self, _path, json=None, **kwargs):
            sent.append(json)
            return {"id": "model-new"}

    result = analytics.migrate_definition_item(
        Client(), source_workspace_id="ws-source", target_workspace_id="ws-target",
        item={"id": "model", "displayName": "Sales"}, item_type="SemanticModel",
        parts=[part("model.bim", f'Sql.Database("{SOURCE_CONNECTION}", "{SOURCE_ENDPOINT_ID}")')],
        id_map=ctx.id_map, source_items=ctx.source_items,
    )
    assert result.rebound_parts == 1
    text = decode_payload(sent[0]["definition"]["parts"][0]["payload"]).decode()
    assert SOURCE_CONNECTION not in text and SOURCE_ENDPOINT_ID not in text
    assert TARGET_CONNECTION in text and TARGET_ENDPOINT_ID in text


def test_only_actually_referenced_identifiers_need_mapping():
    ctx = make_ctx()
    orchestrator._map_sql_endpoint(ctx, SOURCE, {"connectionString": TARGET_CONNECTION})
    assert dangling_references(
        [part("model.bim", SOURCE_CONNECTION)], ctx.id_map, ctx.source_items
    ) == []
    assert dangling_references(
        [part("model.bim", "external.example.com")], {}, ctx.source_items
    ) == []
    assert dangling_references(
        [part("model.bim", SOURCE_ENDPOINT_ID)], {}, ctx.source_items,
        ignore=(SOURCE_ENDPOINT_ID,),
    ) == []


def test_a_shared_server_requires_a_concrete_mapping_even_when_the_address_is_unchanged():
    ctx = make_ctx()
    orchestrator._map_sql_endpoint(
        ctx, SOURCE, {"connectionString": SOURCE_CONNECTION, "id": TARGET_ENDPOINT_ID},
    )
    parts = [part("model.bim", f'Sql.Database("{SOURCE_CONNECTION}", "{SOURCE_ENDPOINT_ID}")')]
    assert dangling_references(parts, ctx.id_map, ctx.source_items) == []
    del ctx.id_map[SOURCE_CONNECTION]
    assert dangling_references(parts, ctx.id_map, ctx.source_items)
