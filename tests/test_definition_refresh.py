"""Refreshing a retained target uses the same safety checks as its first creation."""

import pytest

from fabshuffle.fabric import analytics, special_items
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import decode_json_part, part

SOURCE_WS = "aaaabbbb-1111-2222-3333-444455556666"
TARGET_WS = "bbbbcccc-1111-2222-3333-444455556666"
SOURCE_ITEM = {"id": "source-job", "displayName": "Spark job"}


class Client:
    def __init__(self, error=None):
        self.sent = []
        self.error = error

    def post(self, path, json=None, **kwargs):
        self.sent.append((path, json))
        if self.error:
            raise self.error
        return {"id": "new-job"}


def test_spark_path_warning_is_derived_from_the_original_definition():
    client = Client()
    path = f"abfss://{SOURCE_WS}@onelake.dfs.fabric.microsoft.com/lakehouse/Files/main.py"
    migrated, warnings = analytics.migrate_items(
        client, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        items=[SOURCE_ITEM], item_type="SparkJobDefinition",
        id_map={SOURCE_WS: TARGET_WS},
        parts_by_id={SOURCE_ITEM["id"]: [
            part(special_items.SPARK_JOB_PAYLOAD_PART, {"executableFile": path}),
        ]},
    )
    assert len(migrated) == 1
    assert any("path inside the workspace" in warning for warning in warnings)
    sent = client.sent[0][1]["definition"]["parts"][0]
    assert TARGET_WS in decode_json_part(sent["payload"])["executableFile"]


def test_retained_target_is_updated_in_place_after_rebinding():
    client = Client()
    migrated, warnings = analytics.migrate_items(
        client, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
        items=[SOURCE_ITEM], item_type="Notebook",
        id_map={SOURCE_WS: TARGET_WS}, existing_targets={SOURCE_ITEM["id"]: "retained"},
        parts_by_id={SOURCE_ITEM["id"]: [part("notebook-content.py", SOURCE_WS)]},
    )
    assert warnings == [] and migrated[0].target_id == "retained"
    assert [path for path, _ in client.sent] == [
        f"workspaces/{TARGET_WS}/items/retained/updateDefinition",
    ]
    assert client.sent[0][1]["definition"]["parts"][0] == part("notebook-content.py", TARGET_WS)


def test_retained_target_is_not_updated_with_an_unmapped_dependency():
    client = Client()
    with pytest.raises(analytics.StrandedReference, match="Missing store"):
        analytics.migrate_definition_item(
            client, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
            item=SOURCE_ITEM, item_type="Notebook", id_map={SOURCE_WS: TARGET_WS},
            parts=[part("notebook-content.py", "unmapped-store")],
            source_items={"unmapped-store": {"type": "Lakehouse", "displayName": "Missing store"}},
            target_id="retained",
        )
    assert not client.sent


def test_unsupported_definition_update_retains_the_service_error():
    error = FabricApiError(
        "POST", "updateDefinition", 400,
        '{"errorCode":"OperationNotSupportedForItem","message":"Update not supported here"}',
    )
    client = Client(error)
    with pytest.raises(FabricApiError) as raised:
        analytics.migrate_definition_item(
            client, source_workspace_id=SOURCE_WS, target_workspace_id=TARGET_WS,
            item=SOURCE_ITEM, item_type="Notebook", id_map={},
            parts=[part("notebook-content.py", "pass")], target_id="retained",
        )
    assert raised.value is error
    assert len(client.sent) == 1
