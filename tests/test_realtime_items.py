"""Eventstreams, KQL querysets, KQL dashboards, and mirrored databases.

All four bind other items by id, so what matters is that the id map rewrites every shape
they use, and that the connection extractor recognises the keys they bind connections with.
"""

from __future__ import annotations

import json

from fabshuffle.fabric import connections, data_stores
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import decode_payload, part, rewrite_parts
from fabshuffle.fabric.support import Strategy, assess_workspace

OLD_WS = "aaaaaaaa-1111-1111-1111-111111111111"
NEW_WS = "bbbbbbbb-2222-2222-2222-222222222222"
OLD_ITEM = "cccccccc-3333-3333-3333-333333333333"
NEW_ITEM = "dddddddd-4444-4444-4444-444444444444"
OLD_CLUSTER = "https://trd-old.z5.kusto.fabric.microsoft.com"
NEW_CLUSTER = "https://trd-new.z5.kusto.fabric.microsoft.com"
CONNECTION = "eeeeeeee-5555-5555-5555-555555555555"

ID_MAP = {OLD_WS: NEW_WS, OLD_ITEM: NEW_ITEM, OLD_CLUSTER: NEW_CLUSTER}


# ---------------------------------------------------------------- eventstream


EVENTSTREAM = {
    "sources": [
        {
            "name": "myEventHub",
            "type": "AzureEventHub",
            "properties": {"dataConnectionId": CONNECTION, "consumerGroupName": "$Default"},
        }
    ],
    "destinations": [
        {
            "name": "myLakehouse",
            "type": "Lakehouse",
            "properties": {"workspaceId": OLD_WS, "itemId": OLD_ITEM, "deltaTable": "newTable"},
        }
    ],
    "streams": [],
    "operators": [],
}


def test_eventstream_destination_is_repointed():
    rewritten, changed = rewrite_parts([part("eventstream.json", EVENTSTREAM)], ID_MAP)

    assert changed == 1
    topology = json.loads(decode_payload(rewritten[0]["payload"]))
    destination = topology["destinations"][0]["properties"]
    assert destination["workspaceId"] == NEW_WS
    assert destination["itemId"] == NEW_ITEM
    # The connection is tenant scoped, so it is deliberately left alone.
    assert topology["sources"][0]["properties"]["dataConnectionId"] == CONNECTION


def test_eventstream_source_connection_is_found():
    # An eventstream spells the binding dataConnectionId, unlike a pipeline.
    found = connections.referenced_connection_ids([part("eventstream.json", EVENTSTREAM)])
    assert found == {CONNECTION}


# ------------------------------------------------------------- KQL queryset


def test_queryset_cluster_uri_is_repointed():
    queryset = {
        "queryset": {
            "version": "1.0.0",
            "dataSources": [
                {"id": "ds-1", "clusterUri": OLD_CLUSTER, "databaseName": "Telemetry"}
            ],
            "tabs": [{"id": "t1", "content": "StormEvents | count", "dataSourceId": "ds-1"}],
        }
    }
    rewritten, changed = rewrite_parts([part("RealTimeQueryset.json", queryset)], ID_MAP)

    assert changed == 1
    document = json.loads(decode_payload(rewritten[0]["payload"]))
    source = document["queryset"]["dataSources"][0]
    assert source["clusterUri"] == NEW_CLUSTER
    # The database keeps its name, so the reference still resolves.
    assert source["databaseName"] == "Telemetry"


# ------------------------------------------------------------ KQL dashboard


def test_dashboard_data_source_is_repointed():
    dashboard = {
        "schema_version": "52",
        "title": "Ops",
        "dataSources": [
            {"id": "ds-1", "clusterUri": OLD_CLUSTER, "database": "Telemetry", "kind": "kusto-trident"}
        ],
        "tiles": [],
        "pages": [],
    }
    rewritten, changed = rewrite_parts([part("RealTimeDashboard.json", dashboard)], ID_MAP)

    assert changed == 1
    document = json.loads(decode_payload(rewritten[0]["payload"]))
    assert document["dataSources"][0]["clusterUri"] == NEW_CLUSTER


# -------------------------------------------------------- mirrored database


MIRRORING = {
    "properties": {
        "source": {
            "type": "Snowflake",
            "typeProperties": {
                "connection": CONNECTION,
                "database": "test",
                "externalStorages": [
                    {"type": "AmazonS3", "typeProperties": {"connection": CONNECTION}}
                ],
            },
        },
        "target": {"type": "MountedRelationalDatabase", "typeProperties": {"format": "Delta"}},
    }
}


def test_mirrored_database_source_connection_is_found():
    # A mirrored database binds a bare `connection` under typeProperties.
    found = connections.referenced_connection_ids([part("mirroring.json", MIRRORING)])
    assert found == {CONNECTION}


def test_a_landing_zone_lakehouse_is_repointed():
    sap = {
        "properties": {
            "source": {
                "type": "SAP",
                "typeProperties": {
                    "subType": "Datasphere",
                    "landingZone": {
                        "type": "Lakehouse",
                        "typeProperties": {
                            "connection": CONNECTION,
                            "workspaceId": OLD_WS,
                            "artifactId": OLD_ITEM,
                            "rootFolder": "Files/test",
                        },
                    },
                },
            }
        }
    }
    rewritten, changed = rewrite_parts([part("mirroring.json", sap)], ID_MAP)

    assert changed == 1
    document = json.loads(decode_payload(rewritten[0]["payload"]))
    zone = document["properties"]["source"]["typeProperties"]["landingZone"]["typeProperties"]
    assert zone["workspaceId"] == NEW_WS and zone["artifactId"] == NEW_ITEM
    assert zone["connection"] == CONNECTION


class StatusClient:
    def __init__(self, status: str | None = None, *, code: int | None = None) -> None:
        self.status = status
        self.code = code

    def post(self, path, json=None, params=None, wait=True):
        if self.code is not None:
            raise FabricApiError("POST", path, self.code, "nope")
        return {"status": self.status}

    def list_all(self, path, params=None, value_key="value"):
        return []


def test_mirroring_status_is_read():
    assert data_stores.mirroring_status(StatusClient("Running"), "ws", "db") == "Running"


def test_an_unreadable_mirroring_status_is_not_fatal():
    # A mirror that was never started returns an error rather than a status.
    assert data_stores.mirroring_status(StatusClient(code=400), "ws", "db") is None


def test_unreadable_mirrored_databases_are_not_fatal():
    class Denied:
        def list_all(self, path, params=None, value_key="value"):
            raise FabricApiError("GET", path, 403, "forbidden")

    assert data_stores.list_mirrored_databases(Denied(), "ws") == []


# ------------------------------------------------------------- assessment


def test_the_new_types_are_migrated_rather_than_reported():
    assessment = assess_workspace(
        [
            {"displayName": "stream", "type": "Eventstream"},
            {"displayName": "queries", "type": "KQLQueryset"},
            {"displayName": "ops", "type": "KQLDashboard"},
            {"displayName": "mirror", "type": "MirroredDatabase"},
            {"displayName": "churn", "type": "MLModel"},
        ]
    )
    assert assessment.strategy is Strategy.REBUILD
    # Only the ML model is left behind now.
    assert assessment.unsupported_types == ["MLModel"]
