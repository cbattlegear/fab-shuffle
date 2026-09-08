"""A shortcut pointing at something that did not migrate.

This is the case that produced a bare 400 and a message telling the operator to recreate the
shortcut by hand, with no way to know what it pointed at. The workspace id remaps and the
item id does not, so Fabric is asked for an item that was never in that workspace.
"""

from __future__ import annotations

import pytest

from fabshuffle.fabric import shortcuts
from fabshuffle.fabric.client import FabricApiError

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


# ------------------------------------------------- a target with no data yet

# The case that produced the wrong diagnosis. The mirrored database *did* migrate, so it is
# in the id map and the shortcut is remapped correctly. It fails because we deliberately
# leave mirroring stopped, so nothing has replicated into it and there is nothing to point at.

MIGRATED_MIRROR = "mirror-migrated"
DORMANT = {
    MIGRATED_MIRROR: (
        "arrives with its mirroring not started, so no data has replicated into it yet. "
        "Start mirroring in the new workspace."
    )
}
FULL_MAP = {**ID_MAP, MIGRATED_MIRROR: "mirror-new"}
FULL_ITEMS = {
    **SOURCE_ITEMS,
    MIGRATED_MIRROR: {
        "id": MIGRATED_MIRROR,
        "displayName": "BattleCabbageReplTest",
        "type": "MirroredDatabase",
    },
}


class RefusingClient(FakeClient):
    def post(self, path, json=None, params=None, wait=True):
        raise FabricApiError(
            "POST", path, 400, '{"errorCode":"InvalidPath","message":"path not found"}'
        )


def copy_against_dormant(dormant=DORMANT):
    client = RefusingClient([onelake(SOURCE_WS, MIGRATED_MIRROR)])
    created, warnings = shortcuts.copy_shortcuts(
        client,
        SOURCE_WS,
        LAKEHOUSE,
        TARGET_WS,
        "lh-new",
        FULL_MAP,
        source_items=FULL_ITEMS,
        dormant=dormant,
    )
    return created, warnings


def test_a_failure_against_an_item_with_no_data_yet_explains_why():
    _, warnings = copy_against_dormant()

    assert "MirroredDatabase 'BattleCabbageReplTest'" in warnings[0]
    # Not "did not migrate": it did, which is what made the first diagnosis wrong.
    assert "did not migrate" not in warnings[0]
    assert "no data has replicated into it yet" in warnings[0]
    assert "Start mirroring in the new workspace. Do that, then recreate this shortcut" in warnings[0]


def test_that_explanation_still_quotes_the_service():
    _, warnings = copy_against_dormant()

    # We explain our side. Whether Fabric refuses for that reason is its business to state.
    assert "The service said: InvalidPath path not found." in warnings[0]


def test_the_attempt_is_still_made():
    """Whether Fabric allows a shortcut into an empty item is not ours to assume."""
    client = RefusingClient([onelake(SOURCE_WS, MIGRATED_MIRROR)])
    try:
        shortcuts.copy_shortcuts(
            client, SOURCE_WS, LAKEHOUSE, TARGET_WS, "lh-new", FULL_MAP, dormant=DORMANT
        )
    except FabricApiError:  # pragma: no cover - would be a regression
        raise AssertionError("the failure should have been caught and explained") from None


def test_without_that_context_the_generic_message_is_used():
    _, warnings = copy_against_dormant(dormant={})

    assert "InvalidPath path not found" in warnings[0]
    assert "recreate this shortcut" not in warnings[0]


def test_the_item_a_shortcut_points_at_is_read_before_remapping():
    assert shortcuts.onelake_item_id(onelake(SOURCE_WS, MIGRATED_MIRROR)) == MIGRATED_MIRROR
    assert shortcuts.onelake_item_id({"name": "x"}) == ""


GUID_WS = "aaaaBBBB-1234-5678-90ab-cdef12345678"
GUID_ITEM = "bbbbCCCC-1234-5678-90ab-cdef12345678"
GUID_TARGET = "ddddEEEE-1234-5678-90ab-cdef12345678"


@pytest.mark.parametrize("copy_fn", [shortcuts.copy_shortcuts, shortcuts.copy_table_shortcuts])
@pytest.mark.parametrize("workspace", [GUID_WS.lower(), GUID_WS.upper(), GUID_WS])
@pytest.mark.parametrize("item", [GUID_ITEM.lower(), GUID_ITEM.upper(), GUID_ITEM])
@pytest.mark.parametrize("present", [True, False])
def test_guid_casing_cannot_bypass_rebinding_or_refusal(copy_fn, workspace, item, present):
    original = onelake(workspace, item, path="Tables/CaseSensitive/SomeTable")
    client = FakeClient([original])
    id_map = {GUID_WS.lower(): GUID_TARGET, "Tables/CaseSensitive/SomeTable": "wrong"}
    if present:
        id_map[GUID_ITEM.lower()] = "item-new"
    count, warnings = copy_fn(
        client, GUID_WS.lower(), "source", GUID_TARGET, "target", id_map,
        source_items={GUID_ITEM.lower(): {"type": "Lakehouse", "displayName": "Bronze"}},
    )
    if present:
        assert (count, warnings) == (1, [])
        assert client.created[0]["target"]["oneLake"] == {
            "workspaceId": GUID_TARGET, "itemId": "item-new",
            "path": "Tables/CaseSensitive/SomeTable",
        }
    else:
        assert count == 0 and not client.created
        assert "Lakehouse 'Bronze'" in warnings[0]


@pytest.mark.parametrize("copy_fn", [shortcuts.copy_shortcuts, shortcuts.copy_table_shortcuts])
def test_external_workspace_stays_unchanged_even_with_an_item_mapping(copy_fn):
    original = onelake(GUID_TARGET.upper(), GUID_ITEM.upper())
    client = FakeClient([original])
    count, warnings = copy_fn(
        client, GUID_WS, "source", "new-ws", "new-item",
        {GUID_WS.lower(): "new-ws", GUID_ITEM.lower(): "new-item"},
    )
    assert (count, warnings) == (1, [])
    assert client.created[0]["target"]["oneLake"] == original["target"]["oneLake"]


@pytest.mark.parametrize("copy_fn", [shortcuts.copy_shortcuts, shortcuts.copy_table_shortcuts])
def test_mixed_case_dormant_target_has_the_named_diagnostic(copy_fn):
    client = RefusingClient([onelake(GUID_WS.upper(), GUID_ITEM.upper())])
    count, warnings = copy_fn(
        client, GUID_WS.lower(), "source", "new-ws", "new-item",
        {GUID_WS.lower(): "new-ws", GUID_ITEM.lower(): "new-item"},
        source_items={GUID_ITEM.lower(): {"type": "MirroredDatabase", "displayName": "Mirror"}},
        dormant={GUID_ITEM.lower(): "has not started. Start replication."},
    )
    assert count == 0
    assert "Mirror" in warnings[0] and "Start replication" in warnings[0]
    assert "InvalidPath path not found" in warnings[0]


@pytest.mark.parametrize("copy_fn", [shortcuts.copy_shortcuts, shortcuts.copy_table_shortcuts])
@pytest.mark.parametrize("mapped", [False, True])
def test_source_bound_connection_shortcuts_require_a_replacement(copy_fn, mapped):
    original = {
        "name": "bound", "path": "Tables",
        "target": {"adlsGen2": {"connectionId": GUID_ITEM.upper(), "path": "CaseSensitive"}},
    }
    client = FakeClient([original])
    count, warnings = copy_fn(
        client, GUID_WS, "source", "new-ws", "new-item",
        {GUID_ITEM.lower(): "new-connection"} if mapped else {},
        source_items={GUID_ITEM.lower(): {"type": "Connection", "displayName": "Source store"}},
    )
    if mapped:
        assert count == 1 and not warnings
        assert client.created[0]["target"]["adlsGen2"]["connectionId"] == "new-connection"
        assert client.created[0]["target"]["adlsGen2"]["path"] == "CaseSensitive"
    else:
        assert count == 0 and not client.created
        assert "Source store" in warnings[0] and "retry" in warnings[0]


@pytest.mark.parametrize("copy_fn", [shortcuts.copy_shortcuts, shortcuts.copy_table_shortcuts])
def test_a_mapped_item_without_its_workspace_mapping_is_not_copied(copy_fn):
    client = FakeClient([onelake(GUID_WS, GUID_ITEM)])
    count, warnings = copy_fn(
        client, GUID_WS.lower(), "source", "new-ws", "target", {GUID_ITEM.lower(): "new-item"},
    )
    assert count == 0 and not client.created
    assert warnings
