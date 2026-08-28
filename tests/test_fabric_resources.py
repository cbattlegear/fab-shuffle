from __future__ import annotations

import base64
import json

from fabshuffle.fabric import copyjobs, eventhouses, shortcuts
from fabshuffle.fabric.data_stores import TableRef, _schema_from_location, is_schema_enabled
from fabshuffle.fabric.definitions import decode_json_part, find_part, part, platform_part

# ------------------------------------------------------------------ definitions


def test_platform_part_round_trips():
    encoded = platform_part("CopyJob", "CopyJob_Lakehouse_sales")
    decoded = decode_json_part(encoded["payload"])
    assert decoded["metadata"] == {"type": "CopyJob", "displayName": "CopyJob_Lakehouse_sales"}
    assert encoded["payloadType"] == "InlineBase64"


def test_part_encodes_json_as_base64():
    encoded = part("copyjob-content.json", {"a": 1})
    assert json.loads(base64.b64decode(encoded["payload"])) == {"a": 1}


# --------------------------------------------------------------------- copyjob


def test_lakehouse_copy_job_includes_schema_for_schema_enabled_tables():
    content = copyjobs.build_lakehouse_copy_job(
        source_workspace_id="sw",
        source_item_id="si",
        target_workspace_id="tw",
        target_item_id="ti",
        tables=[TableRef("green_tripdata", "year_2017"), TableRef("plain")],
    )

    assert content["properties"]["source"]["connectionSettings"]["typeProperties"]["artifactId"] == "si"
    assert content["properties"]["destination"]["connectionSettings"]["typeProperties"]["workspaceId"] == "tw"

    first, second = content["activities"]
    assert first["properties"]["source"]["datasetSettings"] == {
        "table": "green_tripdata",
        "schema": "year_2017",
    }
    # A classic lakehouse table must not carry a schema key at all.
    assert second["properties"]["source"]["datasetSettings"] == {"table": "plain"}
    assert first["id"] != second["id"]


def test_lakehouse_activities_overwrite_without_staging():
    content = copyjobs.build_lakehouse_copy_job(
        source_workspace_id="sw",
        source_item_id="si",
        target_workspace_id="tw",
        target_item_id="ti",
        tables=[TableRef("t")],
    )
    properties = content["activities"][0]["properties"]
    assert properties["destination"]["writeBehavior"] == "Overwrite"
    assert properties["enableStaging"] is False


def test_warehouse_copy_job_autocreates_and_stages():
    content = copyjobs.build_warehouse_copy_job(
        source_workspace_id="sw",
        source_item_id="si",
        source_endpoint="src.datawarehouse.fabric.microsoft.com",
        target_workspace_id="tw",
        target_item_id="ti",
        target_endpoint="dst.datawarehouse.fabric.microsoft.com",
        tables=[TableRef("Orders", "dbo")],
    )
    connection = content["properties"]["source"]["connectionSettings"]["typeProperties"]
    assert connection["endPoint"] == "src.datawarehouse.fabric.microsoft.com"

    properties = content["activities"][0]["properties"]
    assert properties["destination"]["tableOption"] == "autoCreate"
    assert properties["enableStaging"] is True


# ------------------------------------------------------------------- lakehouse


def test_schema_detection_and_location_parsing():
    assert is_schema_enabled({"properties": {"defaultSchema": "dbo"}}) is True
    assert is_schema_enabled({"properties": {}}) is False

    location = "abfss://ws@onelake.dfs.fabric.microsoft.com/item/Tables/sales/Orders"
    assert _schema_from_location(location) == "sales"
    assert _schema_from_location("abfss://ws@host/item/Tables/Orders") is None


# ------------------------------------------------------------------- shortcuts


def test_onelake_shortcut_target_is_remapped():
    shortcut = {
        "path": "Tables",
        "name": "shared",
        "target": {
            "type": "OneLake",
            "oneLake": {"workspaceId": "old-ws", "itemId": "old-item", "path": "Tables/x"},
        },
    }
    remapped = shortcuts.remap_shortcut_target(shortcut, {"old-ws": "new-ws", "old-item": "new-item"})

    assert remapped["target"]["oneLake"]["workspaceId"] == "new-ws"
    assert remapped["target"]["oneLake"]["itemId"] == "new-item"
    assert remapped["target"]["oneLake"]["path"] == "Tables/x"
    # The discriminator is read-only and rejected by the create API.
    assert "type" not in remapped["target"]


def test_external_shortcut_target_is_preserved():
    shortcut = {
        "path": "Files",
        "name": "landing",
        "target": {
            "type": "AdlsGen2",
            "adlsGen2": {
                "connectionId": "conn-1",
                "location": "https://x.dfs.core.windows.net",
                "subpath": "/raw",
            },
        },
    }
    remapped = shortcuts.remap_shortcut_target(shortcut, {"conn-1": "should-not-be-used"})
    assert remapped["target"]["adlsGen2"]["connectionId"] == "conn-1"


# ------------------------------------------------------------------ eventhouse


def test_kql_definition_is_retargeted_to_the_new_eventhouse():
    parts = [
        part("DatabaseProperties.json", {"databaseType": "ReadWrite", "parentEventhouseItemId": "old-eh"}),
        part("DatabaseSchema.kql", ".create-merge table T (a:string)"),
    ]
    updated = eventhouses.retarget_database_definition(parts, "new-eh")

    properties = decode_json_part(find_part(updated, "DatabaseProperties.json")["payload"])
    assert properties["parentEventhouseItemId"] == "new-eh"
    assert properties["databaseType"] == "ReadWrite"
    # The schema part must survive untouched.
    assert find_part(updated, "DatabaseSchema.kql")["payload"] == parts[1]["payload"]


def test_shortcut_payload_requires_a_known_source():
    database = {"properties": {"databaseType": "Shortcut"}}
    assert eventhouses.shortcut_creation_payload(database, "eh") is None

    database = {
        "properties": {
            "databaseType": "Shortcut",
            "sourceClusterUri": "https://adx.kusto.windows.net",
            "sourceDatabaseName": "Telemetry",
        }
    }
    payload = eventhouses.shortcut_creation_payload(database, "eh")
    assert payload == {
        "databaseType": "Shortcut",
        "parentEventhouseItemId": "eh",
        "sourceClusterUri": "https://adx.kusto.windows.net",
        "sourceDatabaseName": "Telemetry",
    }
