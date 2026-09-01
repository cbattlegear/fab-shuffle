"""Items that need more than a definition round trip.

Each of these was decided in the item support plan. The shapes below come from the Learn
definition articles for each type, so a change in those shapes should fail here first.
"""

from __future__ import annotations

from fabshuffle.fabric import special_items as special
from fabshuffle.fabric.definitions import decode_json_part, find_part


def parts_with(path: str, content) -> list[dict[str, str]]:
    return [special.encode_part(path, content), special.encode_part(".platform", {"metadata": {}})]


# ------------------------------------------------------------------- Snowflake


def test_the_payload_carries_the_two_fields_create_wants() -> None:
    item = {"id": "s1", "displayName": "Sales"}
    parts = parts_with(
        special.SNOWFLAKE_PROPERTIES_PART,
        {"snowflakeDatabaseName": "ExampleDatabase", "connectionId": "conn-1"},
    )

    assert special.snowflake_creation_payload(item, parts) == {
        "snowflakeDatabaseName": "ExampleDatabase",
        "connectionId": "conn-1",
    }


def test_item_properties_win_over_the_exported_definition() -> None:
    # The live item is authoritative; the definition is only a fallback.
    item = {"properties": {"snowflakeDatabaseName": "Live", "connectionId": "conn-live"}}
    parts = parts_with(
        special.SNOWFLAKE_PROPERTIES_PART,
        {"snowflakeDatabaseName": "Stale", "connectionId": "conn-stale"},
    )

    assert special.snowflake_creation_payload(item, parts)["snowflakeDatabaseName"] == "Live"


def test_the_connection_is_carried_across_untouched() -> None:
    # Connections are tenant scoped, so the copy follows the same one.
    item = {"properties": {"snowflakeDatabaseName": "S", "connectionId": "conn-1"}}

    assert special.snowflake_creation_payload(item, [])["connectionId"] == "conn-1"


def test_nothing_is_built_without_both_fields() -> None:
    assert special.snowflake_creation_payload({"properties": {"connectionId": "c"}}, []) is None
    assert special.snowflake_creation_payload({"properties": {"snowflakeDatabaseName": "s"}}, []) is None


# ----------------------------------------------- mirrored Databricks catalog

CATALOG = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/x/schema.json",
    "catalogName": "catalogName",
    "databricksWorkspaceConnectionId": "4eb6b767-e786-45ed-b7cf-d25023e52222",
    "autoSync": "Enabled",
    "mirroringMode": "Partial",
    "mirrorConfiguration": {"schemas": [{"name": "schema_3", "mirroringMode": "Full"}]},
}


def test_a_catalog_arrives_with_syncing_off() -> None:
    parts, was = special.disable_catalog_autosync(parts_with(special.ADB_CATALOG_PART, CATALOG))

    assert was == "Enabled"
    content = decode_json_part(find_part(parts, special.ADB_CATALOG_PART)["payload"])
    assert content["autoSync"] == "Disabled"


def test_the_rest_of_the_catalog_is_untouched() -> None:
    parts, _ = special.disable_catalog_autosync(parts_with(special.ADB_CATALOG_PART, CATALOG))
    content = decode_json_part(find_part(parts, special.ADB_CATALOG_PART)["payload"])

    assert content["catalogName"] == "catalogName"
    assert content["mirroringMode"] == "Partial"
    assert content["mirrorConfiguration"] == CATALOG["mirrorConfiguration"]
    # The connection is outward pointing and stays exactly as it was.
    assert content["databricksWorkspaceConnectionId"] == CATALOG["databricksWorkspaceConnectionId"]


def test_an_already_disabled_catalog_is_left_alone() -> None:
    parts = parts_with(special.ADB_CATALOG_PART, {**CATALOG, "autoSync": "Disabled"})

    unchanged, was = special.disable_catalog_autosync(parts)

    assert was == "Disabled"
    assert unchanged is parts


# ----------------------------------------------------------------------- Reflex


def rule(identifier: str, *, should_run: bool) -> dict:
    return {
        "uniqueIdentifier": identifier,
        "type": "timeSeriesView-v1",
        "payload": {
            "name": "Too hot for medicine",
            "parentObject": {"targetUniqueIdentifier": "33dd33dd-ee44-ff55-aa66-77bb77bb77bb"},
            "definition": {
                "type": "Rule",
                "instance": "{}",
                "settings": {"shouldRun": should_run, "shouldApplyRuleOnUpdate": False},
            },
        },
    }


CONTAINER = {
    "uniqueIdentifier": "00aa00aa-bb11-cc22-dd33-44ee44ee44ee",
    "type": "container-v1",
    "payload": {"name": "Package delivery sample", "type": "samples"},
}


def test_every_running_rule_is_switched_off() -> None:
    entities = [CONTAINER, rule("r1", should_run=True), rule("r2", should_run=True)]

    parts, running = special.disable_reflex_rules(
        parts_with(special.REFLEX_ENTITIES_PART, entities)
    )

    assert running == 2
    updated = decode_json_part(find_part(parts, special.REFLEX_ENTITIES_PART)["payload"])
    assert all(
        entity["payload"]["definition"]["settings"]["shouldRun"] is False
        for entity in updated
        if entity["type"] == "timeSeriesView-v1"
    )


def test_the_entity_ids_are_left_exactly_as_they_are() -> None:
    """They wire the Reflex to itself; remapping them would break it."""
    entities = [CONTAINER, rule("r1", should_run=True)]

    parts, _ = special.disable_reflex_rules(parts_with(special.REFLEX_ENTITIES_PART, entities))
    updated = decode_json_part(find_part(parts, special.REFLEX_ENTITIES_PART)["payload"])

    assert updated[0]["uniqueIdentifier"] == CONTAINER["uniqueIdentifier"]
    assert updated[1]["payload"]["parentObject"]["targetUniqueIdentifier"] == (
        "33dd33dd-ee44-ff55-aa66-77bb77bb77bb"
    )


def test_a_reflex_with_no_running_rules_is_not_rewritten() -> None:
    parts = parts_with(special.REFLEX_ENTITIES_PART, [CONTAINER, rule("r1", should_run=False)])

    unchanged, running = special.disable_reflex_rules(parts)

    assert running == 0
    assert unchanged is parts


def test_a_rule_with_no_settings_still_gets_switched_off() -> None:
    bare = {
        "uniqueIdentifier": "r1",
        "type": "timeSeriesView-v1",
        "payload": {"definition": {"type": "Rule", "instance": "{}"}},
    }

    parts, running = special.disable_reflex_rules(parts_with(special.REFLEX_ENTITIES_PART, [bare]))

    assert running == 1
    updated = decode_json_part(find_part(parts, special.REFLEX_ENTITIES_PART)["payload"])
    assert updated[0]["payload"]["definition"]["settings"]["shouldRun"] is False


def test_non_rule_views_are_not_touched() -> None:
    attribute = {
        "uniqueIdentifier": "a1",
        "type": "timeSeriesView-v1",
        "payload": {"definition": {"type": "Attribute", "instance": "{}"}},
    }

    parts, running = special.disable_reflex_rules(
        parts_with(special.REFLEX_ENTITIES_PART, [attribute])
    )

    assert running == 0
    assert parts[0] == special.encode_part(special.REFLEX_ENTITIES_PART, [attribute])


# ------------------------------------------------------- Spark job definition

SOURCE_WORKSPACE = "9e4b0e5d-3952-44df-9ac8-2503775e0425"


def spark_payload(**overrides) -> dict:
    return {
        "executableFile": "main.py",
        "defaultLakehouseArtifactId": "",
        "mainClass": "",
        "additionalLakehouseIds": [],
        "retryPolicy": None,
        "commandLineArguments": "",
        "additionalLibraryUris": [],
        "language": "Python",
        "environmentArtifactId": None,
        **overrides,
    }


def test_a_python_job_with_inline_files_needs_no_warning() -> None:
    parts = parts_with(special.SPARK_JOB_PAYLOAD_PART, spark_payload())

    assert special.spark_job_warnings(parts, SOURCE_WORKSPACE) == []


def test_a_jar_job_is_reported() -> None:
    parts = parts_with(
        special.SPARK_JOB_PAYLOAD_PART,
        spark_payload(executableFile="abfss://x@onelake.dfs.fabric.microsoft.com/y/Files/app.jar"),
    )

    warnings = special.spark_job_warnings(parts, SOURCE_WORKSPACE)

    assert any(".jar" in warning for warning in warnings)


def test_a_library_jar_is_reported_too() -> None:
    parts = parts_with(
        special.SPARK_JOB_PAYLOAD_PART, spark_payload(additionalLibraryUris=["helper.jar"])
    )

    assert any(".jar" in warning for warning in special.spark_job_warnings(parts, SOURCE_WORKSPACE))


def test_a_path_into_the_migrating_workspace_is_reported() -> None:
    """The rewriter repoints it at the new workspace, where nothing ever wrote the file."""
    parts = parts_with(
        special.SPARK_JOB_PAYLOAD_PART,
        spark_payload(
            executableFile=(
                f"abfss://{SOURCE_WORKSPACE}@onelake.dfs.fabric.microsoft.com/it/Files/main.py"
            )
        ),
    )

    warnings = special.spark_job_warnings(parts, SOURCE_WORKSPACE)

    assert any("does not exist" in warning for warning in warnings)


def test_a_path_into_another_workspace_is_fine() -> None:
    # That workspace is not moving, so the path keeps working.
    parts = parts_with(
        special.SPARK_JOB_PAYLOAD_PART,
        spark_payload(
            executableFile="abfss://someone-else@onelake.dfs.fabric.microsoft.com/it/Files/main.py"
        ),
    )

    assert special.spark_job_warnings(parts, SOURCE_WORKSPACE) == []


def test_a_definition_without_the_payload_part_says_nothing() -> None:
    assert special.spark_job_warnings([], SOURCE_WORKSPACE) == []
