"""KQL databases expose their table shortcuts, so they can be migrated properly.

Two things follow. The shortcut tables must be kept out of the cross-cluster data copy,
because their data belongs to the shortcut target. And the shortcuts themselves must be
recreated once the items they point at exist.
"""

from __future__ import annotations

from typing import ClassVar

from fabshuffle.fabric import shortcuts
from fabshuffle.fabric.client import FabricApiError

OLD_WS = "11111111-1111-1111-1111-111111111111"
NEW_WS = "22222222-2222-2222-2222-222222222222"
OLD_ITEM = "33333333-3333-3333-3333-333333333333"
NEW_ITEM = "44444444-4444-4444-4444-444444444444"
ID_MAP = {OLD_WS: NEW_WS, OLD_ITEM: NEW_ITEM}

ONELAKE_SHORTCUT = {
    "name": "EmployeesShortcut",
    "enableQueryAcceleration": False,
    "target": {
        "type": "OneLake",
        "oneLake": {"itemId": OLD_ITEM, "workspaceId": OLD_WS, "path": "Tables/Employees"},
    },
}
ACCELERATED_SHORTCUT = {
    "name": "PricesShortcut",
    "enableQueryAcceleration": True,
    "target": {
        "type": "OneLake",
        "oneLake": {"itemId": OLD_ITEM, "workspaceId": OLD_WS, "path": "Tables/Prices"},
    },
}
S3_SHORTCUT = {
    "name": "AmazonS3TableShortcut",
    "enableQueryAcceleration": False,
    "target": {
        "type": "AmazonS3",
        "amazonS3": {
            "connectionId": "dc2cf8ff-abfa-4413-8244-ee10160ed37f",
            "location": "s3://bucket",
            "subpath": "/",
        },
    },
}


class FakeClient:
    def __init__(self, listed: list[dict] | None = None, *, fail_names: set[str] | None = None) -> None:
        self.listed = listed or []
        self.fail_names = fail_names or set()
        self.posted: list[tuple[str, dict]] = []

    def list_all(self, path, params=None, value_key="value"):
        return self.listed

    def post(self, path, json=None, params=None, wait=True):
        if json["name"] in self.fail_names:
            raise FabricApiError("POST", path, 400, "FailureToReserveTableShortcutName")
        self.posted.append((path, json))
        return {"name": json["name"]}


def test_shortcut_names_are_collected_for_exclusion():
    client = FakeClient([ONELAKE_SHORTCUT, S3_SHORTCUT])
    assert shortcuts.table_shortcut_names(client, "ws", "db") == {
        "EmployeesShortcut",
        "AmazonS3TableShortcut",
    }


def test_missing_shortcut_endpoint_is_not_fatal():
    class NoEndpoint:
        def list_all(self, path, params=None, value_key="value"):
            raise FabricApiError("GET", path, 404, "not found")

    assert shortcuts.list_table_shortcuts(NoEndpoint(), "ws", "db") == []


def test_onelake_targets_are_remapped_and_acceleration_preserved():
    client = FakeClient()
    created, warnings = shortcuts.copy_table_shortcuts(
        client, "ws-src", "db-src", NEW_WS, "db-new", ID_MAP,
        shortcuts=[ONELAKE_SHORTCUT, ACCELERATED_SHORTCUT],
    )

    assert created == 2 and warnings == []
    path, body = client.posted[0]
    assert path == f"workspaces/{NEW_WS}/kqlDatabases/db-new/shortcuts"
    assert body["target"]["oneLake"]["itemId"] == NEW_ITEM
    assert body["target"]["oneLake"]["workspaceId"] == NEW_WS
    assert body["target"]["oneLake"]["path"] == "Tables/Employees"
    # The create payload takes exactly one target and no discriminator.
    assert "type" not in body["target"]

    assert client.posted[0][1]["enableQueryAcceleration"] is False
    assert client.posted[1][1]["enableQueryAcceleration"] is True


def test_external_targets_are_copied_verbatim():
    client = FakeClient()
    shortcuts.copy_table_shortcuts(
        client, "ws-src", "db-src", NEW_WS, "db-new", ID_MAP, shortcuts=[S3_SHORTCUT]
    )

    _, body = client.posted[0]
    assert body["target"]["amazonS3"]["connectionId"] == "dc2cf8ff-abfa-4413-8244-ee10160ed37f"
    assert "oneLake" not in body["target"]


def test_a_failed_shortcut_warns_without_stopping_the_others():
    client = FakeClient(fail_names={"EmployeesShortcut"})
    created, warnings = shortcuts.copy_table_shortcuts(
        client, "ws-src", "db-src", NEW_WS, "db-new", ID_MAP,
        shortcuts=[ONELAKE_SHORTCUT, ACCELERATED_SHORTCUT],
    )

    assert created == 1
    assert len(warnings) == 1 and "EmployeesShortcut" in warnings[0]


def test_unrecognised_target_is_skipped_with_a_warning():
    client = FakeClient()
    created, warnings = shortcuts.copy_table_shortcuts(
        client, "ws-src", "db-src", NEW_WS, "db-new", ID_MAP,
        shortcuts=[{"name": "Mystery", "target": {"type": "SomethingNew"}}],
    )

    assert created == 0
    assert len(warnings) == 1 and "Mystery" in warnings[0]


# ---------------------------------------------------------- copy exclusion


def test_shortcut_tables_are_excluded_from_the_data_copy(monkeypatch):
    from fabshuffle.transfer import kql

    class Row(dict):
        pass

    class FakeResponse:
        primary_results: ClassVar[list] = [
            [Row(TableName="RealTable"), Row(TableName="EmployeesShortcut"), Row(TableName="$system")]
        ]

    class FakeKusto:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

        def execute_mgmt(self, database, command, properties=None):
            return FakeResponse()

    monkeypatch.setattr(kql, "_client", lambda uri, principal: FakeKusto())

    tables = kql.list_tables("https://cluster", "db", None, exclude={"EmployeesShortcut"})

    # The shortcut is excluded, and so are Kusto's own system tables.
    assert tables == ["RealTable"]


def test_exclusion_ignores_case(monkeypatch):
    from fabshuffle.transfer import kql

    class FakeResponse:
        primary_results: ClassVar[list] = [[{"TableName": "EmployeesShortcut"}]]

    class FakeKusto:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

        def execute_mgmt(self, database, command, properties=None):
            return FakeResponse()

    monkeypatch.setattr(kql, "_client", lambda uri, principal: FakeKusto())

    assert kql.list_tables("https://c", "db", None, exclude={"employeesshortcut"}) == []
