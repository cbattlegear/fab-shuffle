"""Notebooks, environments, and dataflows.

Only Dataflow Gen2 (CI/CD) items work with the item definition APIs. A Gen1 dataflow or a
classic Gen2 has to be reported rather than half-migrated, and Fabric documents that
filtering the item list by dataflow type does not return reliable information, so each one is
classified by probing its definition.
"""

from __future__ import annotations

import json

from fabshuffle.fabric import analytics
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import decode_payload, is_text_part, part, rewrite_parts

DATAFLOW = {"id": "df-1", "displayName": "Nightly", "type": "Dataflow"}

CICD_METADATA = {"formatVersion": "202502", "name": "Nightly", "connections": []}
LEGACY_METADATA = {"formatVersion": "202106", "name": "Nightly"}


class FakeClient:
    def __init__(self, definition=None, *, status: int | None = None) -> None:
        self.definition = definition
        self.status = status

    def post(self, path, json=None, params=None, wait=True):
        if self.status is not None:
            raise FabricApiError("POST", path, self.status, "nope")
        return {"definition": self.definition}


def definition_with(metadata) -> dict:
    return {
        "parts": [
            part("queryMetadata.json", metadata),
            part("mashup.pq", 'section Section1; shared q = Lakehouse.Contents([]);'),
        ]
    }


# ------------------------------------------------------------- dataflow gates


def test_a_cicd_dataflow_is_migrated():
    parts, reason = analytics.classify_dataflow(
        FakeClient(definition_with(CICD_METADATA)), "ws", DATAFLOW
    )
    assert reason is None
    assert [p["path"] for p in parts] == ["queryMetadata.json", "mashup.pq"]


def test_a_dataflow_without_definition_support_is_reported():
    # Gen1 dataflows do not support the definition APIs at all.
    parts, reason = analytics.classify_dataflow(FakeClient(status=400), "ws", DATAFLOW)

    assert parts is None
    assert "Gen1 dataflow or a classic Gen2" in reason
    assert "upgrade wizard or Save As" in reason


def test_a_dataflow_with_the_wrong_format_version_is_reported():
    parts, reason = analytics.classify_dataflow(
        FakeClient(definition_with(LEGACY_METADATA)), "ws", DATAFLOW
    )

    assert parts is None
    assert "202106" in reason and "not a Dataflow Gen2 (CI/CD)" in reason


def test_a_dataflow_with_no_metadata_part_is_reported():
    parts, reason = analytics.classify_dataflow(
        FakeClient({"parts": [part("mashup.pq", "section Section1;")]}), "ws", DATAFLOW
    )

    assert parts is None
    assert "returned no queryMetadata.json" in reason


def test_dataflow_lakehouse_references_are_rewritten():
    # mashup.pq carries the workspace and lakehouse GUIDs the dataflow reads from.
    mashup = (
        'section Section1; shared q = let Source = Lakehouse.Contents([]),\n'
        '#"Nav 1" = Source{[workspaceId = "old-ws"]}[Data],\n'
        '#"Nav 2" = #"Nav 1"{[lakehouseId = "old-lh"]}[Data] in #"Nav 2";'
    )
    rewritten, changed = rewrite_parts(
        [part("mashup.pq", mashup)], {"old-ws": "new-ws", "old-lh": "new-lh"}
    )

    assert changed == 1
    text = decode_payload(rewritten[0]["payload"]).decode()
    assert "new-ws" in text and "new-lh" in text
    assert "old-ws" not in text and "old-lh" not in text


# ------------------------------------------------------------- notebook parts


def test_notebook_content_is_treated_as_rewritable():
    # A notebook binds its default lakehouse in the content file, not in JSON.
    for path in ("notebook-content.py", "notebook-content.sql", "artifact.content.ipynb"):
        assert is_text_part(path) is True


def test_notebook_default_lakehouse_is_rewritten():
    content = (
        "# Fabric notebook source\n"
        "# META {\n"
        '# META   "dependencies": {"lakehouse": {"default_lakehouse": "old-lh",\n'
        '# META   "default_lakehouse_workspace_id": "old-ws"}}\n'
        "# META }\n"
    )
    rewritten, changed = rewrite_parts(
        [part("notebook-content.py", content)], {"old-lh": "new-lh", "old-ws": "new-ws"}
    )

    assert changed == 1
    text = decode_payload(rewritten[0]["payload"]).decode()
    assert "new-lh" in text and "new-ws" in text


def test_packaged_libraries_are_left_alone():
    # Wheels and jars are binary; rewriting inside them would corrupt them.
    assert is_text_part("Libraries/CustomLibraries/pkg.whl") is False
    assert is_text_part("Libraries/CustomLibraries/lib.jar") is False


# ------------------------------------------------------- environment warnings


def test_a_custom_spark_pool_that_did_not_transfer_is_reported():
    compute = (
        "enable_native_execution_engine: false\n"
        "instance_pool_id: 655fc33c-2712-45a3-864a-b2a00429a8aa\n"
        "driver_cores: 4\n"
    )
    warnings = analytics.environment_warnings("Prod", [part("Setting/Sparkcompute.yml", compute)])

    assert len(warnings) == 1
    assert "655fc33c" in warnings[0]
    assert "was not recreated" in warnings[0]


def test_a_recreated_spark_pool_is_not_reported():
    # Pools are recreated before environments migrate, so the id has already been rewritten.
    pool_id = "999fc33c-2712-45a3-864a-b2a00429a8aa"
    compute = f"instance_pool_id: {pool_id}\ndriver_cores: 4\n"
    warnings = analytics.environment_warnings(
        "Prod", [part("Setting/Sparkcompute.yml", compute)], known_pool_ids={pool_id}
    )
    assert warnings == []


def test_a_starter_pool_environment_is_not_reported():
    compute = "instance_pool_id: null\ndriver_cores: 4\n"
    assert analytics.environment_warnings("Prod", [part("Setting/Sparkcompute.yml", compute)]) == []


def test_an_environment_with_no_compute_settings_is_not_reported():
    libraries = part("Libraries/PublicLibraries/environment.yml", "dependencies:\n  - scipy==0.0.1\n")
    assert analytics.environment_warnings("Prod", [libraries]) == []


# ------------------------------------------------------------ connection ids


def test_a_dataflow_embedded_connection_blob_is_not_mistaken_for_a_connection_id():
    from fabshuffle.fabric import connections

    # A dataflow stores connectionId as an embedded JSON document, not a GUID.
    metadata = {
        "connections": [
            {
                "path": "Lakehouse",
                "kind": "Lakehouse",
                "connectionId": json.dumps({"ClusterId": "b1b1b1b1", "DatasourceId": "c2c2c2c2"}),
            }
        ]
    }
    assert connections.referenced_connection_ids([part("queryMetadata.json", metadata)]) == set()


def test_a_real_connection_id_is_still_found():
    from fabshuffle.fabric import connections

    guid = "dc2cf8ff-abfa-4413-8244-ee10160ed37f"
    payload = {"target": {"adlsGen2": {"connectionId": guid}}}
    assert connections.referenced_connection_ids([part("shortcuts.json", payload)]) == {guid}
