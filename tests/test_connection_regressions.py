"""Regression tests unrelated to automatic connection replacement.

Two bugs were confirmed from an attached log of repeated warnings, from a version of this
tool that used to recreate connections automatically (see ``test_connections.py`` for why
that no longer happens):

* Several of the messages built while reporting an unrecreatable connection quoted
  ``source.get('displayName')`` directly, with no fallback, so an unnamed connection was
  reported as ``Connection 'None'``. ``display_name`` still backs every connection-naming
  message in the tool today, so its fallback behaviour remains covered here.
* A OneLake shortcut target's ``connectionId`` (used for delegated cross-tenant shortcuts) was
  never rewritten by ``_remap_target``, unlike every other shortcut target type, so a shortcut
  with a valid destination mapping still tried to create against the stale source connection.
"""

from __future__ import annotations

from fabshuffle.fabric import connections, shortcuts

# ------------------------------------------------------------- display_name fallback


def test_display_name_falls_back_to_id_when_unnamed():
    assert connections.display_name({"id": "conn-1", "displayName": None}) == "conn-1"
    assert connections.display_name({"id": "conn-1"}) == "conn-1"


def test_display_name_prefers_the_declared_name():
    assert connections.display_name({"id": "conn-1", "displayName": "Bronze SQL"}) == "Bronze SQL"


# ------------------------------------------------------- shortcut oneLake connectionId remap


def test_onelake_shortcut_connection_id_is_remapped_when_workspace_is_mapped():
    shortcut = {
        "path": "Tables",
        "name": "shared",
        "target": {
            "type": "OneLake",
            "oneLake": {
                "workspaceId": "old-ws", "itemId": "old-item", "connectionId": "old-conn",
                "path": "Tables/x",
            },
        },
    }
    remapped = shortcuts.remap_shortcut_target(
        shortcut, {"old-ws": "new-ws", "old-item": "new-item", "old-conn": "new-conn"},
    )

    assert remapped["target"]["oneLake"]["workspaceId"] == "new-ws"
    assert remapped["target"]["oneLake"]["itemId"] == "new-item"
    assert remapped["target"]["oneLake"]["connectionId"] == "new-conn"


def test_onelake_shortcut_connection_id_is_remapped_even_when_workspace_is_not_mapped():
    """A delegated cross-tenant OneLake shortcut can carry a connectionId that needs a
    replacement even when its workspaceId/itemId are left as-is (the early-return path that
    previously skipped connectionId entirely)."""
    shortcut = {
        "path": "Tables",
        "name": "shared",
        "target": {
            "type": "OneLake",
            "oneLake": {
                "workspaceId": "untouched-ws", "itemId": "untouched-item",
                "connectionId": "old-conn", "path": "Tables/x",
            },
        },
    }
    remapped = shortcuts.remap_shortcut_target(shortcut, {"old-conn": "new-conn"})

    assert remapped["target"]["oneLake"]["workspaceId"] == "untouched-ws"
    assert remapped["target"]["oneLake"]["itemId"] == "untouched-item"
    assert remapped["target"]["oneLake"]["connectionId"] == "new-conn"


def test_onelake_shortcut_connection_id_without_a_mapping_is_preserved():
    shortcut = {
        "path": "Tables",
        "name": "shared",
        "target": {
            "type": "OneLake",
            "oneLake": {"workspaceId": "old-ws", "connectionId": "conn-1", "path": "Tables/x"},
        },
    }
    remapped = shortcuts.remap_shortcut_target(shortcut, {"old-ws": "new-ws"})
    assert remapped["target"]["oneLake"]["connectionId"] == "conn-1"
