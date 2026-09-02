"""A shortcut pointing at something that did not migrate.

This is the case that produced a bare 400 and a message telling the operator to recreate the
shortcut by hand, with no way to know what it pointed at. The workspace id remaps and the
item id does not, so Fabric is asked for an item that was never in that workspace.
"""

from __future__ import annotations

from fabshuffle.fabric import shortcuts

SOURCE_WS = "ws-source"
TARGET_WS = "ws-target"
LAKEHOUSE = "lh-source"
MIRRORED = "mirror-source"
WAREHOUSE = "wh-source"
OTHER_WS = "ws-elsewhere"

SOURCE_ITEMS = {
    MIRRORED: {"id": MIRRORED, "displayName": "BattleCabbageReplTest", "type": "MirroredDatabase"},
    WAREHOUSE: {"id": WAREHOUSE, "displayName": "CloneWarehouseTest", "type": "Warehouse"},
}

# The mirrored database failed to migrate, so it is absent from the map.
ID_MAP = {SOURCE_WS: TARGET_WS, LAKEHOUSE: "lh-new", WAREHOUSE: "wh-new"}


def onelake(workspace_id, item_id, path="Tables/dbo/movies"):
    return {
        "name": "dbo_movies",
        "path": "Tables",
        "target": {
            "type": "OneLake",
            "oneLake": {"workspaceId": workspace_id, "itemId": item_id, "path": path},
        },
    }


class FakeClient:
    def __init__(self, shortcuts_) -> None:
        self.shortcuts = shortcuts_
        self.created: list[dict] = []

    def list_all(self, path, params=None, value_key="value"):
        return self.shortcuts

    def post(self, path, json=None, params=None, wait=True):
        self.created.append(json or {})
        return {}


def copy(shortcuts_):
    client = FakeClient(shortcuts_)
    created, warnings = shortcuts.copy_shortcuts(
        client,
        SOURCE_WS,
        LAKEHOUSE,
        TARGET_WS,
        "lh-new",
        ID_MAP,
        source_items=SOURCE_ITEMS,
    )
    return client, created, warnings


# ------------------------------------------------------------- the detection


def test_a_shortcut_into_an_item_that_did_not_migrate_is_not_attempted():
    client, created, warnings = copy([onelake(SOURCE_WS, MIRRORED)])

    # Sending it would ask Fabric for an item that was never in the new workspace, which
    # comes back as an unreadable 400.
    assert client.created == []
    assert created == 0
    assert len(warnings) == 1


def test_the_warning_names_the_item_it_needed():
    _, _, warnings = copy([onelake(SOURCE_WS, MIRRORED)])

    assert "MirroredDatabase 'BattleCabbageReplTest'" in warnings[0]
    assert "did not migrate" in warnings[0]
    assert "Migrate that item, then recreate the shortcut" in warnings[0]


def test_an_unnamed_item_falls_back_to_its_id():
    client = FakeClient([onelake(SOURCE_WS, "ghost-item")])
    _, warnings = shortcuts.copy_shortcuts(
        client, SOURCE_WS, LAKEHOUSE, TARGET_WS, "lh-new", ID_MAP, source_items=SOURCE_ITEMS
    )

    assert "the item ghost-item" in warnings[0]


def test_a_shortcut_into_an_item_that_did_migrate_is_created():
    client, created, warnings = copy([onelake(SOURCE_WS, WAREHOUSE)])

    assert (created, warnings) == (1, [])
    assert client.created[0]["target"]["oneLake"]["itemId"] == "wh-new"
    assert client.created[0]["target"]["oneLake"]["workspaceId"] == TARGET_WS


def test_a_shortcut_into_another_workspace_is_left_exactly_as_it_is():
    """That workspace is not moving, so the reference still resolves and must not be touched."""
    client, created, warnings = copy([onelake(OTHER_WS, "some-item")])

    assert (created, warnings) == (1, [])
    assert client.created[0]["target"]["oneLake"] == {
        "workspaceId": OTHER_WS,
        "itemId": "some-item",
        "path": "Tables/dbo/movies",
    }


def test_an_external_target_is_never_treated_as_unmigrated():
    external = {
        "name": "adventureworks",
        "path": "Tables",
        "target": {"type": "AdlsGen2", "adlsGen2": {"connectionId": "c-1", "location": "x", "subpath": "y"}},
    }
    client, created, warnings = copy([external])

    assert (created, warnings) == (1, [])
    assert client.created[0]["target"] == {
        "adlsGen2": {"connectionId": "c-1", "location": "x", "subpath": "y"}
    }


# ------------------------------------------------------------ the predicate


def test_the_check_reads_the_original_ids_not_the_remapped_ones():
    assert shortcuts.unmigrated_target(onelake(SOURCE_WS, MIRRORED), ID_MAP, SOURCE_WS) == MIRRORED
    assert shortcuts.unmigrated_target(onelake(SOURCE_WS, WAREHOUSE), ID_MAP, SOURCE_WS) is None
    assert shortcuts.unmigrated_target(onelake(OTHER_WS, "any"), ID_MAP, SOURCE_WS) is None


def test_a_target_with_no_item_id_is_not_reported():
    assert shortcuts.unmigrated_target(onelake(SOURCE_WS, ""), ID_MAP, SOURCE_WS) is None


def test_a_shortcut_with_no_target_at_all_is_not_reported():
    assert shortcuts.unmigrated_target({"name": "x"}, ID_MAP, SOURCE_WS) is None


# --------------------------------------------------------- KQL table shortcuts


def test_kql_table_shortcuts_get_the_same_treatment():
    client = FakeClient([])
    created, warnings = shortcuts.copy_table_shortcuts(
        client,
        SOURCE_WS,
        "kql-source",
        TARGET_WS,
        "kql-new",
        ID_MAP,
        shortcuts=[onelake(SOURCE_WS, MIRRORED)],
        source_items=SOURCE_ITEMS,
    )

    assert (created, client.created) == (0, [])
    assert "KQL table shortcut" in warnings[0]
    assert "BattleCabbageReplTest" in warnings[0]
