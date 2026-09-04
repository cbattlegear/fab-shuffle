"""Connections are reused, not recreated.

They are tenant scoped, so the same id resolves from the new workspace, and the API never
returns credentials so a faithful copy is impossible. What matters is reporting the ones that
will not work once the item has moved.
"""

from __future__ import annotations

from fabshuffle.fabric import connections
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import part

CLOUD = "11111111-1111-1111-1111-111111111111"
PERSONAL = "22222222-2222-2222-2222-222222222222"
ONPREM = "33333333-3333-3333-3333-333333333333"
VNET = "44444444-4444-4444-4444-444444444444"
UNKNOWN = "55555555-5555-5555-5555-555555555555"

KNOWN = {
    CLOUD: {"id": CLOUD, "displayName": "Contoso SQL", "connectivityType": "ShareableCloud"},
    PERSONAL: {"id": PERSONAL, "displayName": "My Files", "connectivityType": "PersonalCloud"},
    ONPREM: {"id": ONPREM, "displayName": "HQ Gateway", "connectivityType": "OnPremisesGateway"},
    VNET: {"id": VNET, "displayName": "VNet Gateway", "connectivityType": "VirtualNetworkGateway"},
}


# ------------------------------------------------------------------ extraction


def test_pipeline_external_references_are_found():
    pipeline = {
        "properties": {
            "activities": [
                {
                    "name": "Copy",
                    "typeProperties": {
                        "source": {"externalReferences": {"connection": CLOUD}},
                        "sink": {"datasetSettings": {"externalReferences": {"connection": ONPREM}}},
                    },
                }
            ]
        }
    }
    found = connections.referenced_connection_ids([part("pipeline-content.json", pipeline)])
    assert found == {CLOUD, ONPREM}


def test_copy_job_connection_settings_are_found():
    job = {
        "properties": {
            "destination": {
                "connectionSettings": {
                    "type": "DataWarehouse",
                    "externalReferences": {"connection": CLOUD},
                }
            }
        }
    }
    assert connections.referenced_connection_ids([part("copyjob-content.json", job)]) == {CLOUD}


def test_shortcut_style_connection_ids_are_found():
    payload = {"target": {"adlsGen2": {"connectionId": ONPREM, "location": "https://x"}}}
    assert connections.referenced_connection_ids([part("shortcuts.metadata.json", payload)]) == {ONPREM}


def test_binary_and_non_json_parts_are_skipped():
    parts = [
        {"path": "StaticResources/logo.jpg", "payload": "///9j/4AAQ", "payloadType": "InlineBase64"},
        part("definition/tables/sales.tmdl", "table Sales"),
    ]
    assert connections.referenced_connection_ids(parts) == set()


def test_items_with_no_connections_report_none():
    assert connections.referenced_connection_ids([part("pipeline-content.json", {"a": 1})]) == set()


# ------------------------------------------------------------------ checking


def test_a_shareable_cloud_connection_is_fine():
    assert connections.check("'Nightly'", [CLOUD], KNOWN) == []


def test_an_invisible_connection_is_reported():
    issues = connections.check("'Nightly'", [UNKNOWN], KNOWN)
    assert len(issues) == 1
    assert "cannot see" in issues[0].message()
    assert UNKNOWN in issues[0].message()


def test_a_personal_connection_is_reported():
    issues = connections.check("'Nightly'", [PERSONAL], KNOWN)
    assert len(issues) == 1
    assert "cannot be shared" in issues[0].message()
    assert "My Files" in issues[0].message()


def test_a_vnet_gateway_names_the_target_region():
    issues = connections.check("'Nightly'", [VNET], KNOWN, target_region="westus")
    assert len(issues) == 1
    assert "stays in its original region" in issues[0].message()
    assert "westus" in issues[0].message()


def test_an_on_premises_gateway_is_reported_without_a_region_claim():
    issues = connections.check("'Nightly'", [ONPREM], KNOWN, target_region="westus")
    assert len(issues) == 1
    assert "on-premises data gateway" in issues[0].message()
    assert "westus" not in issues[0].message()


def test_every_problem_connection_is_reported():
    issues = connections.check("'Nightly'", [CLOUD, PERSONAL, VNET, UNKNOWN], KNOWN)
    assert len(issues) == 3


# ------------------------------------------------------------------- listing


def test_missing_connection_permission_is_not_fatal():
    class Denied:
        def list_all(self, path, params=None, value_key="value"):
            raise FabricApiError("GET", path, 403, "forbidden")

    assert connections.list_connections(Denied()) == []


def test_other_errors_still_raise():
    class Broken:
        def list_all(self, path, params=None, value_key="value"):
            raise FabricApiError("GET", path, 500, "boom")

    try:
        connections.list_connections(Broken())
    except FabricApiError as error:
        assert error.status_code == 500
    else:
        raise AssertionError("a server error should not be swallowed")
