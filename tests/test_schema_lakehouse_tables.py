"""Listing the tables of a schema-enabled lakehouse.

The lakehouse tables API rejects them outright:

    GET .../lakehouses/{id}/tables -> 400 UnsupportedOperationForSchemasEnabledLakehouse

so they are enumerated over TDS through the SQL analytics endpoint instead.
"""

from __future__ import annotations

import pytest

from fabshuffle import orchestrator
from fabshuffle.fabric import data_stores
from fabshuffle.fabric.client import FabricApiError

SOURCE_WS = "ws-source"
LAKEHOUSE_ID = "lh-1"
ENDPOINT = "abc.datawarehouse.fabric.microsoft.com"

SCHEMA_LAKEHOUSE = {
    "id": LAKEHOUSE_ID,
    "displayName": "CloneTest",
    "properties": {
        "defaultSchema": "dbo",
        "sqlEndpointProperties": {"connectionString": ENDPOINT, "id": "ep-1"},
    },
}
CLASSIC_LAKEHOUSE = {
    "id": LAKEHOUSE_ID,
    "displayName": "Plain",
    "properties": {"sqlEndpointProperties": {"connectionString": ENDPOINT, "id": "ep-1"}},
}


class FakeClient:
    """Rejects the tables API the way Fabric does for a schema-enabled lakehouse."""

    def __init__(self, shortcuts_=None) -> None:
        self.shortcuts = shortcuts_ or []
        self.tables_called = False

    def list_all(self, path, params=None, value_key="value"):
        if path.endswith("/tables"):
            self.tables_called = True
            raise FabricApiError(
                "GET",
                path,
                400,
                '{"errorCode":"UnsupportedOperationForSchemasEnabledLakehouse",'
                '"message":"The operation is not supported for Lakehouse with schemas enabled."}',
            )
        if path.endswith("/shortcuts"):
            return self.shortcuts
        return []


def make_ctx(client, monkeypatch, tables):
    monkeypatch.setattr(orchestrator.sqlschema, "list_base_tables", lambda *a, **k: tables)

    plan = orchestrator.MigrationPlan(
        capacity_id="cap",
        capacity_name="F64",
        capacity_region="westus",
        source_workspace_id=SOURCE_WS,
        source_workspace_name="src",
        target_workspace_name="dst",
    )
    return orchestrator._Context(
        client=client,
        tokens=object(),
        principal=object(),
        plan=plan,
        run=orchestrator.MigrationRun(source_workspace_name="src", capacity_name="F64"),
        scratch_dir=None,
    )


def test_a_schema_enabled_lakehouse_falls_back_to_the_sql_endpoint(monkeypatch):
    client = FakeClient()
    ctx = make_ctx(client, monkeypatch, [("year_2017", "green_tripdata"), ("dbo", "Orders")])

    refs = orchestrator._lakehouse_tables(ctx, SCHEMA_LAKEHOUSE, schema_enabled=True)

    # The tables API is never called, because it cannot answer for this lakehouse.
    assert client.tables_called is False
    assert {(r.schema, r.name) for r in refs} == {
        ("year_2017", "green_tripdata"),
        ("dbo", "Orders"),
    }


def test_shortcuts_are_excluded_from_the_fallback(monkeypatch):
    # A shortcut appears as a table on the SQL endpoint, but its data belongs to the target.
    client = FakeClient(
        shortcuts_=[{"path": "Tables/dbo", "name": "SharedOrders"}]
    )
    ctx = make_ctx(client, monkeypatch, [("dbo", "Orders"), ("dbo", "SharedOrders")])

    refs = orchestrator._lakehouse_tables(ctx, SCHEMA_LAKEHOUSE, schema_enabled=True)

    assert {(r.schema, r.name) for r in refs} == {("dbo", "Orders")}


def test_shortcut_exclusion_ignores_case(monkeypatch):
    client = FakeClient(shortcuts_=[{"path": "Tables/DBO", "name": "sharedorders"}])
    ctx = make_ctx(client, monkeypatch, [("dbo", "SharedOrders")])

    assert orchestrator._lakehouse_tables(ctx, SCHEMA_LAKEHOUSE, schema_enabled=True) == []


def test_a_lakehouse_with_no_sql_endpoint_is_reported(monkeypatch):
    client = FakeClient()
    ctx = make_ctx(client, monkeypatch, [])
    lakehouse = {"id": LAKEHOUSE_ID, "displayName": "Broken", "properties": {}}

    with pytest.raises(orchestrator.sqlschema.SchemaTransferError, match="SQL analytics endpoint"):
        orchestrator._lakehouse_tables(ctx, lakehouse, schema_enabled=True)


def test_a_classic_lakehouse_still_uses_the_tables_api(monkeypatch):
    called = {}

    def managed_tables(client, ws, lh, *, schema_enabled):
        called["schema_enabled"] = schema_enabled
        return [data_stores.TableRef(name="Orders")]

    monkeypatch.setattr(orchestrator.data_stores, "managed_tables", managed_tables)
    ctx = make_ctx(FakeClient(), monkeypatch, [])

    refs = orchestrator._lakehouse_tables(ctx, CLASSIC_LAKEHOUSE, schema_enabled=False)

    assert called == {"schema_enabled": False}
    assert [r.name for r in refs] == ["Orders"]


def test_shortcuts_are_excluded_from_a_classic_lakehouse_too(monkeypatch):
    """The regression: a shortcut is indistinguishable from a table in the tables API.

    ``TableType`` is only ever ``Managed`` or ``External``, so a shortcut to a delta table is
    reported as ``Managed`` like any other. Copying it duplicated the data into the target and
    left a real table sitting on the name, so the shortcut phase then failed with a conflict
    against our own copy.
    """
    client = FakeClient(shortcuts_=[{"path": "Tables", "name": "random_table"}])
    monkeypatch.setattr(
        orchestrator.data_stores,
        "managed_tables",
        lambda *a, **k: [
            data_stores.TableRef(name="Orders"),
            data_stores.TableRef(name="random_table"),
        ],
    )
    ctx = make_ctx(client, monkeypatch, [])

    refs = orchestrator._lakehouse_tables(ctx, CLASSIC_LAKEHOUSE, schema_enabled=False)

    assert [r.name for r in refs] == ["Orders"]


def test_a_files_shortcut_does_not_exclude_a_table_of_the_same_name(monkeypatch):
    client = FakeClient(shortcuts_=[{"path": "Files/landing", "name": "Orders"}])
    monkeypatch.setattr(
        orchestrator.data_stores,
        "managed_tables",
        lambda *a, **k: [data_stores.TableRef(name="Orders")],
    )
    ctx = make_ctx(client, monkeypatch, [])

    assert [r.name for r in orchestrator._lakehouse_tables(ctx, CLASSIC_LAKEHOUSE, schema_enabled=False)] == [
        "Orders"
    ]


def test_a_schema_shortcut_does_not_exclude_the_same_name_in_another_schema(monkeypatch):
    client = FakeClient(shortcuts_=[{"path": "Tables/shared", "name": "Orders"}])
    ctx = make_ctx(client, monkeypatch, [("dbo", "Orders"), ("shared", "Orders")])

    refs = orchestrator._lakehouse_tables(ctx, SCHEMA_LAKEHOUSE, schema_enabled=True)

    assert {(r.schema, r.name) for r in refs} == {("dbo", "Orders")}


def test_a_leading_slash_on_the_shortcut_path_still_matches(monkeypatch):
    client = FakeClient(shortcuts_=[{"path": "/Tables/", "name": "random_table"}])
    monkeypatch.setattr(
        orchestrator.data_stores,
        "managed_tables",
        lambda *a, **k: [data_stores.TableRef(name="random_table")],
    )
    ctx = make_ctx(client, monkeypatch, [])

    assert orchestrator._lakehouse_tables(ctx, CLASSIC_LAKEHOUSE, schema_enabled=False) == []
