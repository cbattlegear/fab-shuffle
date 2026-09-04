"""Naming the item a definition needed but did not get.

A reference inside a definition is usually a workspace id beside an item id. The workspace id
is always in the map, so it is always rewritten. If the item beside it is not, the pair
becomes the *new* workspace and the *old* item: something that was never in that workspace.

Fabric's answer to that is not always legible. Two Copy Jobs pointing at a lakehouse in
another workspace came back with nothing but UnknownError.
"""

from __future__ import annotations

import json

import pytest

from fabshuffle.fabric.analytics import dangling_references, describe_failure
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import part

SOURCE_WS = "348428fd-3410-45ff-9d25-db66fa34f37d"
MIGRATED_LAKEHOUSE = "f721e011-2e0e-40ab-88f5-70aba80920ae"
STRANDED_LAKEHOUSE = "bab00672-2f3c-4320-a09e-f47fcfcb1ab6"
OTHER_WS = "8d10604d-6100-4238-83a0-d54616c246fe"
OTHER_ITEM = "7aec1cee-020f-4554-8c80-a5fe1512f657"

SOURCE_ITEMS = {
    MIGRATED_LAKEHOUSE: {"id": MIGRATED_LAKEHOUSE, "displayName": "SchemaHouse", "type": "Lakehouse"},
    STRANDED_LAKEHOUSE: {"id": STRANDED_LAKEHOUSE, "displayName": "ShortcutCopyTest", "type": "Lakehouse"},
}
ID_MAP = {SOURCE_WS: "ws-new", MIGRATED_LAKEHOUSE: "lh-new"}


def copy_job(source_item, destination_workspace=OTHER_WS, destination_item=OTHER_ITEM):
    content = {
        "properties": {
            "source": {
                "connectionSettings": {
                    "typeProperties": {"workspaceId": SOURCE_WS, "artifactId": source_item}
                }
            },
            "destination": {
                "connectionSettings": {
                    "typeProperties": {
                        "workspaceId": destination_workspace,
                        "artifactId": destination_item,
                    }
                }
            },
        }
    }
    return [part("copyjob-content.json", json.dumps(content))]


# ------------------------------------------------------------- the detection


def test_an_item_that_did_not_migrate_is_named():
    found = dangling_references(copy_job(STRANDED_LAKEHOUSE), ID_MAP, SOURCE_ITEMS)

    assert found == ["Lakehouse 'ShortcutCopyTest'"]


def test_an_item_that_did_migrate_is_not_named():
    assert dangling_references(copy_job(MIGRATED_LAKEHOUSE), ID_MAP, SOURCE_ITEMS) == []


def test_an_item_in_another_workspace_is_not_our_business():
    """That workspace is not moving, so the reference still resolves and must be left alone."""
    found = dangling_references(copy_job(MIGRATED_LAKEHOUSE), ID_MAP, SOURCE_ITEMS)

    assert found == []


def test_several_missing_items_are_all_named_in_a_fixed_order():
    items = {
        **SOURCE_ITEMS,
        "zzz": {"id": "zzz", "displayName": "Alpha", "type": "Warehouse"},
    }
    parts = copy_job(STRANDED_LAKEHOUSE, destination_item="zzz")
    found = dangling_references(parts, ID_MAP, items)

    assert found == ["Warehouse 'Alpha'", "Lakehouse 'ShortcutCopyTest'"]


def test_matching_ignores_case():
    parts = copy_job(STRANDED_LAKEHOUSE.upper())

    assert dangling_references(parts, ID_MAP, SOURCE_ITEMS) == ["Lakehouse 'ShortcutCopyTest'"]


def test_nothing_is_reported_without_a_source_item_list():
    assert dangling_references(copy_job(STRANDED_LAKEHOUSE), ID_MAP, {}) == []


def test_a_binary_part_is_skipped_rather_than_crashing():
    parts = [{"path": "sqldb.dacpac", "payload": "AAECAw=="}]

    assert dangling_references(parts, ID_MAP, SOURCE_ITEMS) == []


def test_an_item_is_never_a_reason_not_to_create_itself():
    """Several types carry their own id in their own definition.

    It is not in the map yet precisely because this is the call that would put it there, so
    reading that as a missing dependency refused every one of them: "SemanticModel 'CloneTest'
    depends on SemanticModel 'CloneTest'".
    """
    from fabshuffle.fabric.analytics import migrate_definition_item

    model_id = "sm-self"
    items = {model_id: {"id": model_id, "displayName": "CloneTest", "type": "SemanticModel"}}
    parts = [part("model.bim", json.dumps({"model": {"id": model_id}}))]

    class Client:
        def post(self, path, json=None, params=None, wait=True):
            return {"id": "sm-new"}

    result = migrate_definition_item(
        Client(),
        source_workspace_id=SOURCE_WS,
        target_workspace_id="ws-new",
        item={"id": model_id, "displayName": "CloneTest"},
        item_type="SemanticModel",
        id_map={SOURCE_WS: "ws-new"},
        parts=parts,
        source_items=items,
    )

    assert result.target_id == "sm-new"


def test_a_self_reference_does_not_hide_a_real_one():
    model_id = "sm-self"
    items = {
        model_id: {"id": model_id, "displayName": "CloneTest", "type": "SemanticModel"},
        "lh-1": {"id": "lh-1", "displayName": "Bronze", "type": "Lakehouse"},
    }
    parts = [part("model.bim", json.dumps({"id": model_id, "reads": "lh-1"}))]

    assert dangling_references(parts, {}, items, ignore=(model_id,)) == ["Lakehouse 'Bronze'"]


# ------------------------------------------------------------- the refusal


def test_an_item_needing_something_that_did_not_migrate_is_refused():
    """It must not be created. Half rewritten it points at nothing; left alone it points at
    the workspace being migrated away from, and breaks the day that is deleted."""
    from fabshuffle.fabric.analytics import StrandedReference, migrate_definition_item

    class Client:
        def post(self, path, json=None, params=None, wait=True):
            raise AssertionError("nothing should have been created")

    with pytest.raises(StrandedReference) as raised:
        migrate_definition_item(
            Client(),
            source_workspace_id=SOURCE_WS,
            target_workspace_id="ws-new",
            item={"id": "cj-1", "displayName": "TestCopyCrossWorkspace"},
            item_type="CopyJob",
            id_map=ID_MAP,
            parts=copy_job(STRANDED_LAKEHOUSE),
            source_items=SOURCE_ITEMS,
        )

    assert raised.value.needed == ["Lakehouse 'ShortcutCopyTest'"]


def test_an_item_whose_references_all_migrated_is_created():
    from fabshuffle.fabric.analytics import migrate_definition_item

    class Client:
        def post(self, path, json=None, params=None, wait=True):
            return {"id": "new-1"}

    result = migrate_definition_item(
        Client(),
        source_workspace_id=SOURCE_WS,
        target_workspace_id="ws-new",
        item={"id": "cj-1", "displayName": "SchemaLakeCopyTest"},
        item_type="CopyJob",
        id_map=ID_MAP,
        parts=copy_job(MIGRATED_LAKEHOUSE),
        source_items=SOURCE_ITEMS,
    )

    assert result.target_id == "new-1"


# ------------------------------------------------------------- the message


def test_a_connection_failure_still_reads_as_a_connection_failure():
    error = FabricApiError("POST", "url", 400, '{"errorCode":"DataSourcesValidationError"}')
    message = describe_failure("Eventstream", "Meshtastic", error)

    assert "Manage Connections and Gateways" in message


def test_an_unreadable_failure_repeats_what_the_service_said():
    error = FabricApiError("POST", "url", 400, '{"errorCode":"UnknownError","message":"An error occurred"}')

    assert "UnknownError An error occurred" in describe_failure("CopyJob", "Nightly", error)
