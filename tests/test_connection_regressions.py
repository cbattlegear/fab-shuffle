"""Regression tests for a live migration's connection-replacement warnings.

Three bugs were confirmed from an attached log of repeated warnings:

* ``can_recreate`` returned the same "recreated by hand against its gateway" reason for every
  connectivity type it refuses, including ``PersonalCloud`` - which has no gateway at all and,
  per Fabric's connectivity types, "cannot be shared with others". The operator was told to do
  something that does not apply to what they actually have.
* Several of the messages built around that reason quoted ``source.get('displayName')``
  directly, with no fallback, so an unnamed connection was reported as ``Connection 'None'``.
* A OneLake shortcut target's ``connectionId`` (used for delegated cross-tenant shortcuts) was
  never rewritten by ``_remap_target``, unlike every other shortcut target type, so a shortcut
  with a valid destination mapping still tried to create against the stale source connection.
"""

from __future__ import annotations

from dataclasses import replace

from test_connection_replacement import SQL_METADATA, connection
from test_rebuild_ordering import (
    LAKEHOUSE,
    PRINCIPAL,
    SOURCE_ENDPOINT,
    SOURCE_WS,
    TARGET_ENDPOINT,
    TARGET_WS,
    FakeFabric,
    make_plan,
)
from test_source_connection_migration import CONNECTION, rebuild
from test_source_connection_migration import fabric as fabric

from fabshuffle import orchestrator
from fabshuffle.fabric import connections, shortcuts
from fabshuffle.run import MigrationRun

SOURCE_TENANT = "11111111-1111-1111-1111-111111111111"
TARGET_TENANT = "22222222-2222-2222-2222-222222222222"


# ------------------------------------------------------------- can_recreate wording


def test_a_personal_cloud_connection_is_refused_without_claiming_a_gateway():
    personal = connection(connectivityType="PersonalCloud")
    reason = connections.can_recreate(personal, SQL_METADATA)
    assert reason is not None
    assert "gateway" not in reason
    assert "personal cloud connection" in reason
    assert "cannot be shared" in reason


def test_a_gateway_connection_still_names_its_gateway():
    # Locks in that special-casing PersonalCloud left the true gateway wording untouched.
    gateway = connection(connectivityType="OnPremisesGateway")
    reason = connections.can_recreate(gateway, SQL_METADATA)
    assert reason and "against its gateway" in reason


def test_an_unrecognised_connectivity_type_does_not_claim_a_gateway_either():
    odd = connection(connectivityType="SomeFutureConnectivityType")
    reason = connections.can_recreate(odd, SQL_METADATA)
    assert reason is not None
    assert "gateway" not in reason


# ------------------------------------------------------------- display_name fallback


def test_display_name_falls_back_to_id_when_unnamed():
    assert connections.display_name({"id": "conn-1", "displayName": None}) == "conn-1"
    assert connections.display_name({"id": "conn-1"}) == "conn-1"


def test_display_name_prefers_the_declared_name():
    assert connections.display_name({"id": "conn-1", "displayName": "Bronze SQL"}) == "Bronze SQL"


# ------------------------------------------------------- _migrate_connections warnings


def test_an_unnamed_personal_cloud_source_gets_an_accurate_warning(fabric):
    """End-to-end through _migrate_connections: the exact warning an operator would read."""
    fabric.source_connection["connectivityType"] = "PersonalCloud"
    fabric.source_connection["displayName"] = None
    run = rebuild(fabric)

    assert not any(kind in {"DataPipeline", "Eventstream"} for kind, _, _ in fabric.created)
    warnings = run.summary["warnings"]
    assert not fabric.connection_creates

    # A source connection whose path targets this workspace is also reported, separately, by
    # the inward "will need to be replaced by hand" dependency check - filter to the actual
    # _migrate_connections replacement warning this test is about.
    matching = [w for w in warnings if "personal cloud connection" in w]
    assert matching, warnings
    warning = matching[0]
    # The id stands in for the missing name; nothing renders as the literal string None.
    assert f"Connection '{CONNECTION}'" in warning
    assert "None" not in warning
    assert "against its gateway" not in warning
    assert "cannot be shared" in warning
    # A PersonalCloud connection cannot be created explicitly, so the fix must not describe
    # trying to recreate one with that connectivity type.
    assert "connectivity 'PersonalCloud'" not in warning
    assert "shareable cloud connection" in warning


def test_cross_tenant_personal_cloud_still_gets_the_accurate_instruction():
    """The cross-tenant branch (no supplied mapping) builds the same instruction text; it must
    not carry the gateway claim or the "create with connectivity 'PersonalCloud'" claim either.
    """
    fake = FakeFabric()
    source = connection(
        id=CONNECTION, displayName=None, connectivityType="PersonalCloud",
        connectionDetails={"type": "SQL", "path": f"{SOURCE_ENDPOINT};{LAKEHOUSE}"},
    )
    plan = replace(
        make_plan(), source_tenant_id=SOURCE_TENANT, target_tenant_id=TARGET_TENANT,
        source_client_id="src-client", target_client_id="dst-client",
    )
    ctx = orchestrator._Context(
        client=fake, tokens=object(), principal=PRINCIPAL, plan=plan,
        run=MigrationRun(source_workspace_name="src", capacity_name="F64"), scratch_dir=None,
        target_workspace_id=TARGET_WS,
        source_items={CONNECTION: {**source, "type": "Connection"}},
        id_map={SOURCE_WS: TARGET_WS, SOURCE_ENDPOINT: TARGET_ENDPOINT, LAKEHOUSE: "lh-new"},
    )

    orchestrator._migrate_connections(ctx)

    matching = [w for w in ctx.warnings if "explicit destination connection mapping" in w]
    assert matching, ctx.warnings
    warning = matching[0]
    assert f"Connection '{CONNECTION}'" in warning
    assert "None" not in warning
    assert "against its gateway" not in warning
    assert "connectivity 'PersonalCloud'" not in warning
    assert "shareable cloud connection" in warning


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
