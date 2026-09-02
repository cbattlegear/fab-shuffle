"""Migrating a Cosmos DB database.

The containers ride along in the item definition, which holds no Fabric ids at all, so the
interesting part is the documents: those have no Fabric API and go over the Cosmos data
plane instead.
"""

from __future__ import annotations

import json

import pytest

from fabshuffle import orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric import cosmosdb
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import decode_payload, part
from fabshuffle.run import MigrationRun
from fabshuffle.transfer import cosmos

SOURCE_WS = "ws-source"
TARGET_WS = "ws-target"
SOURCE_DB = "cosmos-source"
TARGET_DB = "cosmos-target"

DEFINITION = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/CosmosDB/definition/CosmosDB/2.0.0/schema.json",
    "containers": [
        {
            "options": {"autoscaleSettings": {"maxThroughput": 4000}},
            "resource": {"id": "orders", "partitionKey": {"paths": ["/customerId"], "kind": "Hash"}},
        }
    ],
}

SOURCE_ITEM = {
    "id": SOURCE_DB,
    "displayName": "CosmosTestItem",
    "type": "CosmosDBDatabase",
    "properties": {
        "serverFqdn": "src.xyz.cosmos.fabric.microsoft.com",
        "databaseName": "CosmosTestItem",
    },
}
TARGET_ITEM = {
    "id": TARGET_DB,
    "displayName": "CosmosTestItem",
    "properties": {
        "serverFqdn": "dst.xyz.cosmos.fabric.microsoft.com",
        "databaseName": "CosmosTestItem",
    },
}


class FakeClient:
    def __init__(self, create_fails: bool = False) -> None:
        self.posts: list[tuple[str, dict]] = []
        self.create_fails = create_fails

    def list_all(self, path, params=None, value_key="value"):
        if path == f"workspaces/{SOURCE_WS}/cosmosDbDatabases":
            return [SOURCE_ITEM]
        return []

    def get(self, path, params=None):
        if path == f"workspaces/{TARGET_WS}/cosmosDbDatabases/{TARGET_DB}":
            return TARGET_ITEM
        raise AssertionError(f"unexpected GET {path}")

    def post(self, path, json=None, params=None, wait=True):
        self.posts.append((path, json or {}))
        if path.endswith("/getDefinition"):
            return {"definition": {"parts": [part("definition.json", DEFINITION), part(".platform", "{}")]}}
        if path.endswith("/items"):
            if self.create_fails:
                raise FabricApiError("POST", path, 400, '{"errorCode":"Nope","message":"no"}')
            return {"id": TARGET_DB}
        return {}

    def delete(self, path, params=None):
        return None


def make_ctx(client, monkeypatch, copied=None):
    calls: list[dict] = []

    def copy_documents(**kwargs):
        calls.append(kwargs)
        return copied if copied is not None else []

    monkeypatch.setattr(orchestrator.cosmos_transfer, "copy_documents", copy_documents)

    plan = orchestrator.MigrationPlan(
        capacity_id="cap",
        capacity_name="F64",
        capacity_region="westus",
        source_workspace_id=SOURCE_WS,
        source_workspace_name="src",
        target_workspace_name="dst",
    )
    ctx = orchestrator._Context(
        client=client,
        tokens=object(),
        principal=ServicePrincipal("t", "c", "s"),
        plan=plan,
        run=MigrationRun(source_workspace_name="src", capacity_name="F64"),
        scratch_dir=None,
    )
    ctx.target_workspace_id = TARGET_WS
    ctx.run.start_step("sqldatabases", "Migrating SQL and Cosmos databases")
    return ctx, calls


# ------------------------------------------------------------------ the item


def test_the_container_definition_is_copied_across_untouched(monkeypatch):
    client = FakeClient()
    ctx, _ = make_ctx(client, monkeypatch)
    migrated, warnings = orchestrator._migrate_cosmos_databases(ctx)

    assert (migrated, warnings) == (1, [])
    _, body = next(p for p in client.posts if p[0].endswith("/items"))
    sent = json.loads(
        decode_payload(
            next(p for p in body["definition"]["parts"] if p["path"] == "definition.json")["payload"]
        )
    )
    # Nothing in a Cosmos definition references a Fabric item, so nothing is rewritten.
    assert sent == DEFINITION


def test_the_source_platform_part_is_not_carried_over(monkeypatch):
    client = FakeClient()
    ctx, _ = make_ctx(client, monkeypatch)
    orchestrator._migrate_cosmos_databases(ctx)

    _, body = next(p for p in client.posts if p[0].endswith("/items"))
    assert all(p["path"] != ".platform" for p in body["definition"]["parts"])


def test_a_database_that_cannot_be_created_is_reported_and_the_run_goes_on(monkeypatch):
    ctx, calls = make_ctx(FakeClient(create_fails=True), monkeypatch)
    migrated, warnings = orchestrator._migrate_cosmos_databases(ctx)

    assert migrated == 0
    assert any("CosmosTestItem" in w for w in warnings)
    assert calls == []


# ------------------------------------------------------------- the documents


def test_documents_are_copied_between_the_two_endpoints(monkeypatch):
    ctx, calls = make_ctx(FakeClient(), monkeypatch)
    orchestrator._migrate_cosmos_databases(ctx)

    assert len(calls) == 1
    assert calls[0]["source_endpoint"] == "https://src.xyz.cosmos.fabric.microsoft.com:443/"
    assert calls[0]["target_endpoint"] == "https://dst.xyz.cosmos.fabric.microsoft.com:443/"
    assert calls[0]["source_database"] == "CosmosTestItem"


def test_no_documents_are_copied_when_the_plan_says_not_to(monkeypatch):
    ctx, calls = make_ctx(FakeClient(), monkeypatch)
    ctx.plan.include_data = False
    migrated, warnings = orchestrator._migrate_cosmos_databases(ctx)

    assert (migrated, warnings, calls) == (1, [], [])


def test_a_document_copy_failure_still_leaves_the_containers(monkeypatch):
    client = FakeClient()
    ctx, _ = make_ctx(client, monkeypatch)
    monkeypatch.setattr(
        orchestrator.cosmos_transfer,
        "copy_documents",
        lambda **k: (_ for _ in ()).throw(cosmos.CosmosTransferError("access was denied")),
    )
    migrated, warnings = orchestrator._migrate_cosmos_databases(ctx)

    assert migrated == 1
    assert any("containers are there" in w for w in warnings)


def test_per_container_warnings_name_their_database(monkeypatch):
    ctx, _ = make_ctx(FakeClient(), monkeypatch, copied=["Documents in container 'orders' did not copy"])
    _, warnings = orchestrator._migrate_cosmos_databases(ctx)

    assert warnings == [
        "Cosmos DB database 'CosmosTestItem': Documents in container 'orders' did not copy"
    ]


# ------------------------------------------------------------ transfer layer


def test_cosmos_owned_properties_are_not_written_back():
    document = {
        "id": "1",
        "customerId": "c-9",
        "total": 12,
        "_rid": "x",
        "_self": "y",
        "_etag": "z",
        "_attachments": "a",
        "_ts": 1,
    }
    # id and the partition key are data; everything Cosmos generated describes where the
    # document lived and is rejected on write.
    assert cosmos.strip_system_properties(document) == {
        "id": "1",
        "customerId": "c-9",
        "total": 12,
    }


def test_a_document_with_no_system_properties_is_unchanged():
    assert cosmos.strip_system_properties({"id": "1"}) == {"id": "1"}


class FakeCosmosError(Exception):
    def __init__(self, status_code, message) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(message)


def test_a_denied_read_explains_that_the_data_plane_is_authorised_separately():
    message = cosmos._describe(FakeCosmosError(403, "Forbidden\nActivityId: abc"))

    assert "data plane separately from the item" in message
    # The SDK's multi-line activity trace is noise in an operator-facing warning.
    assert "ActivityId" not in message


def test_another_failure_repeats_what_the_service_said():
    assert cosmos._describe(FakeCosmosError(429, "Too many requests")) == "HTTP 429: Too many requests"


# ------------------------------------------------------------------ endpoint


@pytest.mark.parametrize(
    ("fqdn", "expected"),
    [
        ("host.cosmos.fabric.microsoft.com", "https://host.cosmos.fabric.microsoft.com:443/"),
        ("https://host.cosmos.fabric.microsoft.com:443/", "https://host.cosmos.fabric.microsoft.com:443/"),
        ("", ""),
    ],
)
def test_the_endpoint_url_is_built_from_the_bare_host(fqdn, expected):
    assert cosmosdb.endpoint_url({"properties": {"serverFqdn": fqdn}}) == expected
