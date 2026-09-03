"""Naming the item a definition needed but did not get.

A reference inside a definition is usually a workspace id beside an item id. The workspace id
is always in the map, so it is always rewritten. If the item beside it is not, the pair
becomes the *new* workspace and the *old* item: something that was never in that workspace.

Fabric's answer to that is not always legible. Two Copy Jobs pointing at a lakehouse in
another workspace came back with nothing but UnknownError.
"""

from __future__ import annotations

import json

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


# ------------------------------------------------------------- the message


def test_the_failure_says_which_item_was_missing():
    error = FabricApiError("POST", "url", 400, '{"errorCode":"UnknownError","message":"An error occurred"}')
    message = describe_failure("CopyJob", "TestCopyCrossWorkspace", error, needed=["Lakehouse 'X'"])

    assert "it refers to Lakehouse 'X', which did not migrate" in message
    assert "Migrate those first" in message
    # The service's own words are still repeated, in case our reading is wrong.
    assert "UnknownError" in message


def test_without_a_missing_item_the_message_is_unchanged():
    error = FabricApiError("POST", "url", 400, '{"errorCode":"UnknownError","message":"An error occurred"}')
    message = describe_failure("CopyJob", "Nightly", error)

    assert "refers to" not in message
    assert "UnknownError An error occurred" in message


def test_a_connection_failure_still_reads_as_a_connection_failure():
    error = FabricApiError("POST", "url", 400, '{"errorCode":"DataSourcesValidationError"}')
    message = describe_failure("Eventstream", "Meshtastic", error)

    assert "Manage Connections and Gateways" in message
