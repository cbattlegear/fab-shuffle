"""Fabric auto-creates a child KQL database when an eventhouse is created.

A source database with that name therefore already exists in the target and must be updated
in place; creating it fails with 409 ItemDisplayNameAlreadyInUse.
"""

from __future__ import annotations

import pytest

from fabshuffle.fabric import eventhouses
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import part

PARTS = [
    part("DatabaseProperties.json", {"databaseType": "ReadWrite", "parentEventhouseItemId": "eh-new"}),
    part("DatabaseSchema.kql", ".create-merge table Events (ts:datetime)"),
    part(".platform", {"metadata": {"type": "KQLDatabase", "displayName": "ForzaEvents"}}),
]

CONFLICT_BODY = (
    '{"errorCode":"ItemDisplayNameAlreadyInUse","message":"Requested \'ForzaEvents\' is already in use"}'
)


class FakeClient:
    def __init__(self, *, conflict_on_create: bool = False, databases: list[dict] | None = None) -> None:
        self.conflict_on_create = conflict_on_create
        self.databases = databases or []
        self.created: list[dict] = []
        self.updated: list[tuple[str, list[dict]]] = []

    def post(self, path, json=None, params=None, wait=True):
        if path.endswith("/updateDefinition"):
            item_id = path.split("/items/")[1].split("/")[0]
            self.updated.append((item_id, json["definition"]["parts"]))
            return {}
        if path.endswith("/kqlDatabases"):
            if self.conflict_on_create:
                raise FabricApiError("POST", path, 409, CONFLICT_BODY)
            self.created.append(json)
            return {"id": "kql-new"}
        raise AssertionError(f"unexpected POST {path}")

    def get(self, path, params=None):
        for database in self.databases:
            if path.endswith(f"/kqlDatabases/{database['id']}"):
                return database
        raise FabricApiError("GET", path, 404, "not found")

    def list_all(self, path, params=None, value_key="value"):
        return self.databases


DEFAULT_DB = {"id": "kql-default", "displayName": "ForzaEvents", "type": "KQLDatabase"}


def test_default_database_is_adopted_instead_of_recreated():
    client = FakeClient()
    target, adopted = eventhouses.create_or_adopt_kql_database(
        client,
        "ws",
        "ForzaEvents",
        parts=PARTS,
        existing={"ForzaEvents": DEFAULT_DB},
    )

    assert adopted is True
    assert target["id"] == "kql-default"
    assert client.created == []

    item_id, sent_parts = client.updated[0]
    assert item_id == "kql-default"
    # updateDefinition only accepts the platform file with updateMetadata=true.
    assert all(p["path"] != ".platform" for p in sent_parts)
    assert {p["path"] for p in sent_parts} == {"DatabaseProperties.json", "DatabaseSchema.kql"}


def test_a_database_with_no_name_clash_is_created_normally():
    client = FakeClient()
    target, adopted = eventhouses.create_or_adopt_kql_database(
        client,
        "ws",
        "OtherDatabase",
        parts=PARTS,
        existing={"ForzaEvents": DEFAULT_DB},
    )

    assert adopted is False
    assert target["id"] == "kql-new"
    assert client.updated == []
    assert client.created[0]["displayName"] == "OtherDatabase"


def test_conflict_is_recovered_when_the_default_appears_late():
    # The child database can be provisioned just after the eventhouse, so the up-front
    # lookup can miss it and the create still races into a 409.
    client = FakeClient(conflict_on_create=True, databases=[DEFAULT_DB])
    target, adopted = eventhouses.create_or_adopt_kql_database(
        client, "ws", "ForzaEvents", parts=PARTS, existing={}
    )

    assert adopted is True
    assert target["id"] == "kql-default"
    assert client.updated[0][0] == "kql-default"


def test_conflict_with_no_matching_database_still_raises():
    client = FakeClient(conflict_on_create=True, databases=[])
    with pytest.raises(FabricApiError) as error:
        eventhouses.create_or_adopt_kql_database(client, "ws", "ForzaEvents", parts=PARTS, existing={})
    assert error.value.status_code == 409


def test_other_errors_are_not_swallowed():
    class Failing(FakeClient):
        def post(self, path, json=None, params=None, wait=True):
            raise FabricApiError("POST", path, 403, "forbidden")

    with pytest.raises(FabricApiError) as error:
        eventhouses.create_or_adopt_kql_database(Failing(), "ws", "X", parts=PARTS, existing={})
    assert error.value.status_code == 403


def test_eventhouse_databases_maps_names_to_databases():
    client = FakeClient(
        databases=[
            DEFAULT_DB,
            {"id": "kql-2", "displayName": "Telemetry", "type": "KQLDatabase"},
        ]
    )
    eventhouse = {"properties": {"databasesItemIds": ["kql-default", "kql-2", "missing"]}}

    databases = eventhouses.eventhouse_databases(client, "ws", eventhouse)

    assert set(databases) == {"ForzaEvents", "Telemetry"}
    # A database that cannot be read is skipped rather than failing the migration.
    assert databases["ForzaEvents"]["id"] == "kql-default"
