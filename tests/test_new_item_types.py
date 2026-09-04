"""The item types added from the support plan, exercised through their phases.

These check the wiring rather than the transforms, which have their own tests: that each type
is listed as supported, migrated in a phase that runs after whatever it reads, and reported
with the warning its plan entry called for.
"""

from __future__ import annotations

from fabshuffle.fabric import analytics
from fabshuffle.fabric import special_items as special
from fabshuffle.fabric.definitions import decode_json_part, find_part
from fabshuffle.fabric.support import REBUILT_TYPES, assess_workspace

NEW_TYPES = [
    "GraphQLApi",
    "Map",
    "Reflex",
    "SparkJobDefinition",
    "VariableLibrary",
    "MountedDataFactory",
    "GraphModel",
    "GraphQuerySet",
    "MirroredAzureDatabricksCatalog",
    "SnowflakeDatabase",
]


def test_the_new_types_are_no_longer_reported_as_unsupported() -> None:
    assessment = assess_workspace(
        [{"displayName": name, "type": name, "id": name} for name in NEW_TYPES]
    )

    assert assessment.unsupported == []
    assert {item["type"] for item in assessment.migrated} == set(NEW_TYPES)


def test_every_new_type_is_in_the_support_matrix() -> None:
    assert set(NEW_TYPES) <= REBUILT_TYPES


class Client:
    """Serves list, export and create for a single item type."""

    def __init__(self, item_type: str, parts: list[dict]) -> None:
        self.item_type = item_type
        self.parts = parts
        self.created: list[dict] = []
        self.export_params: list[dict | None] = []

    def list_all(self, path, params=None, value_key="value"):
        if path.endswith("/items"):
            return [{"id": "src-1", "displayName": "thing", "type": self.item_type}]
        return []

    def get(self, path, params=None):
        return {}

    def post(self, path, json=None, params=None, wait=True):
        if path.endswith("/getDefinition"):
            self.export_params.append(params)
            return {"definition": {"parts": self.parts}}
        self.created.append(json or {})
        return {"id": "tgt-1"}


def migrate(item_type: str, parts: list[dict]) -> tuple[Client, list[str]]:
    client = Client(item_type, parts)
    _, warnings = analytics.migrate_items(
        client,
        source_workspace_id="ws-source",
        target_workspace_id="ws-target",
        items=[{"id": "src-1", "displayName": "thing", "type": item_type}],
        item_type=item_type,
        id_map={},
    )
    return client, warnings


def test_a_spark_job_is_exported_in_the_format_that_carries_its_code() -> None:
    """V1 shares a payload schema and a filename with V2 but omits the Main/ and Libs/ parts."""
    parts = [special.encode_part(special.SPARK_JOB_PAYLOAD_PART, {"executableFile": "main.py"})]

    client, _ = migrate("SparkJobDefinition", parts)

    assert client.export_params[0] == {"format": "SparkJobDefinitionV2"}
    assert client.created[0]["definition"]["format"] == "SparkJobDefinitionV2"


def test_other_types_are_exported_in_the_default_format() -> None:
    client, _ = migrate("Map", [special.encode_part("map.json", {"dataSources": []})])

    assert client.export_params[0] is None


def test_a_spark_job_that_runs_a_jar_is_reported() -> None:
    parts = [special.encode_part(special.SPARK_JOB_PAYLOAD_PART, {"executableFile": "app.jar"})]

    _, warnings = migrate("SparkJobDefinition", parts)

    assert any(".jar" in warning for warning in warnings)


def test_a_catalog_is_created_with_syncing_off_and_says_so() -> None:
    parts = [
        special.encode_part(
            special.ADB_CATALOG_PART,
            {"catalogName": "c", "autoSync": "Enabled", "mirroringMode": "Full"},
        )
    ]

    client, warnings = migrate("MirroredAzureDatabricksCatalog", parts)

    sent = find_part(client.created[0]["definition"]["parts"], special.ADB_CATALOG_PART)
    assert decode_json_part(sent["payload"])["autoSync"] == "Disabled"
    assert any("automatic sync was turned off" in warning for warning in warnings)


def test_a_reflex_is_created_with_its_rules_off_and_says_so() -> None:
    entities = [
        {
            "uniqueIdentifier": "r1",
            "type": "timeSeriesView-v1",
            "payload": {"definition": {"type": "Rule", "settings": {"shouldRun": True}}},
        }
    ]
    parts = [special.encode_part(special.REFLEX_ENTITIES_PART, entities)]

    client, warnings = migrate("Reflex", parts)

    sent = find_part(client.created[0]["definition"]["parts"], special.REFLEX_ENTITIES_PART)
    updated = decode_json_part(sent["payload"])
    assert updated[0]["payload"]["definition"]["settings"]["shouldRun"] is False
    assert any("switched off for the move" in warning for warning in warnings)


def test_a_quiet_item_produces_no_warning() -> None:
    _, warnings = migrate("VariableLibrary", [special.encode_part("variables.json", {})])

    assert warnings == []
