"""A rebuild never creates or adopts a connection; it only uses what the operator maps.

Automatic same-tenant connection recreation was deliberately removed (see ``test_connections.py``
and the "Connections" section of the README for the policy). A connection whose path points back
into the workspace being migrated is never created, adopted by name, or adopted by a coincidental
path match. If the operator supplies an explicit ``connection_mappings`` entry naming an existing,
verified destination connection, that mapping is honoured; otherwise the connection is left
unresolved and anything that depends on it - a Data pipeline, an Eventstream, a mirrored database
- is refused through the same generic dangling-reference guard used for every other cross-item
dependency, not a bespoke connection-phase warning.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from test_rebuild_ordering import (
    LAKEHOUSE,
    MODEL,
    MODEL_BIM,
    PRINCIPAL,
    REPORT,
    REPORT_PBIR,
    SOURCE_ENDPOINT,
    SOURCE_WS,
    TARGET_ENDPOINT,
    TARGET_WS,
    FakeFabric,
    StubPowerBi,
    make_plan,
)

from fabshuffle import journal, orchestrator
from fabshuffle.config import SETTINGS
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import decode_json_part, part
from fabshuffle.fabric.support import assess_workspace
from fabshuffle.run import MigrationRun, RunStatus

CONNECTION = "aaaabbbb-1111-2222-3333-444455556666"
REPLACEMENT = "bbbbcccc-1111-2222-3333-444455556666"
EXTERNAL = "ccccdddd-1111-2222-3333-444455556666"
CONSUMERS = [
    {"id": "pipeline-source", "type": "DataPipeline", "displayName": "Daily pipeline"},
    {"id": "stream-source", "type": "Eventstream", "displayName": "Live events"},
]


def connection(**overrides):
    base = {
        "id": "conn-old",
        "displayName": "Bronze SQL",
        "connectivityType": "ShareableCloud",
        "privacyLevel": "Organizational",
        "connectionDetails": {"type": "SQL", "path": "old.datawarehouse.fabric.microsoft.com;bronze"},
        "credentialDetails": {
            "credentialType": "WorkspaceIdentity",
            "singleSignOnType": "None",
            "connectionEncryption": "Encrypted",
            "skipTestConnection": False,
        },
    }
    base.update(overrides)
    return base


class Fabric(FakeFabric):
    def __init__(self):
        super().__init__()
        self.source_connection = connection(
            id=CONNECTION,
            connectionDetails={"type": "SQL", "path": f"{SOURCE_ENDPOINT};{LAKEHOUSE}"},
        )
        self.tenant_connections = [
            self.source_connection,
            connection(id=EXTERNAL, connectionDetails={"type": "SQL", "path": "external.example.com;other"}),
        ]
        self.unreadable = set()
        self.early_items = []
        self.definitions[MODEL] = [part("model.bim", MODEL_BIM)]
        self.definitions[REPORT] = [part("definition.pbir", REPORT_PBIR)]
        for item in CONSUMERS:
            self.definitions[item["id"]] = [part("content.json", {
                "connectionId": CONNECTION.upper(),
                "externalReferences": {"connection": EXTERNAL},
            })]

    def list_all(self, path, params=None, value_key="value"):
        if path == "connections":
            return self.tenant_connections
        if path == f"workspaces/{SOURCE_WS}/mirroredDatabases":
            return self.early_items
        items = super().list_all(path, params, value_key)
        if path == f"workspaces/{SOURCE_WS}/items":
            return [*items, *CONSUMERS, *self.early_items]
        return items

    def get(self, path, params=None):
        if path.startswith("connections/"):
            connection_id = path.split("/")[1]
            if connection_id not in self.unreadable:
                for candidate in self.tenant_connections:
                    if candidate["id"] == connection_id:
                        return candidate
            raise FabricApiError("GET", path, 403, '{"errorCode":"Denied","message":"share replacement"}')
        return super().get(path, params)


@pytest.fixture
def fabric(monkeypatch):
    fake = Fabric()
    monkeypatch.setattr(orchestrator, "FabricClient", fake)
    monkeypatch.setattr(orchestrator, "TokenProvider", lambda principal: object())
    monkeypatch.setattr(orchestrator.workspaces, "clone_folder_tree", lambda *a: {})
    monkeypatch.setattr(orchestrator.workspaces, "list_role_assignments", lambda *a: [])
    monkeypatch.setattr(orchestrator.workspaces, "copy_role_assignments", lambda *a, **k: [])
    monkeypatch.setattr(orchestrator.sqlschema, "transfer_schema", lambda **k: [])
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", StubPowerBi())
    return fake


def rebuild(fabric, prior=None, **plan_overrides):
    run = MigrationRun(source_workspace_name="bronze-ws", capacity_name="F64")
    plan = replace(make_plan(), include_data=False, include_files=False, **plan_overrides)
    orchestrator.run_migration(run, PRINCIPAL, plan, cleanup=False, prior=prior)
    assert run.status == RunStatus.SUCCEEDED, run.error
    return run


# --------------------------------------------------------- no mapping, no creation, no adoption


def test_a_source_bound_connection_is_never_created_or_adopted_without_a_mapping(fabric):
    run = rebuild(fabric)

    assert not any(kind == "Connection" for kind, _, _ in fabric.created)
    # Nothing to bind the connection to, so the consumers that need it are refused rather
    # than created against a stale or absent reference.
    assert not any(kind in {"DataPipeline", "Eventstream"} for kind, _, _ in fabric.created)
    warnings = run.summary["warnings"]
    assert any("Daily pipeline" in warning and "Bronze SQL" in warning for warning in warnings)
    assert any("Live events" in warning and "Bronze SQL" in warning for warning in warnings)
    assert CONNECTION not in journal.read(SETTINGS.journal_for(run.id)).id_map


def test_a_plausible_destination_connection_is_never_adopted_without_an_explicit_mapping(fabric):
    """A destination connection that happens to share the migrated target's path must not be
    treated as a replacement just because it looks like one; only a supplied mapping does."""
    fabric.tenant_connections.append(connection(
        id=REPLACEMENT, displayName="Bronze SQL",
        connectionDetails={"type": "SQL", "path": f"{TARGET_ENDPOINT};lh-new"},
    ))
    rebuild(fabric)
    assert not any(kind in {"DataPipeline", "Eventstream"} for kind, _, _ in fabric.created)


def test_a_connection_referenced_but_not_pointing_into_the_workspace_is_left_alone(fabric):
    """``EXTERNAL`` is bound by both consumers but its path never names this workspace, so it
    is a standard external connection: reused unchanged, nothing for the operator to resolve."""
    fabric.tenant_connections.append(connection(
        id=REPLACEMENT, displayName="Bronze SQL (dest)",
        connectionDetails={"type": "SQL", "path": f"{TARGET_ENDPOINT};lh-new"},
    ))
    run = rebuild(fabric, connection_mappings={CONNECTION: REPLACEMENT})
    assert EXTERNAL not in journal.read(SETTINGS.journal_for(run.id)).id_map
    assert not any("external.example.com" in warning for warning in run.summary["warnings"])


# ------------------------------------------------------------------- explicit mapping honoured


def test_an_explicit_connection_mapping_is_validated_and_used_to_bind_consumers(fabric):
    fabric.tenant_connections.append(connection(
        id=REPLACEMENT, displayName="Bronze SQL (dest)",
        connectionDetails={"type": "SQL", "path": f"{TARGET_ENDPOINT};lh-new"},
    ))
    run = rebuild(fabric, connection_mappings={CONNECTION: REPLACEMENT})

    assert not any(kind == "Connection" for kind, _, _ in fabric.created)
    for kind, _, target_id in fabric.created:
        if kind in {"DataPipeline", "Eventstream"}:
            payload = decode_json_part(fabric.definitions[target_id][0]["payload"])
            assert payload["connectionId"] == REPLACEMENT
            assert payload["externalReferences"]["connection"] == EXTERNAL
    assert {kind for kind, _, _ in fabric.created} >= {"DataPipeline", "Eventstream"}
    assert journal.read(SETTINGS.journal_for(run.id)).id_map[CONNECTION] == REPLACEMENT


def test_explicit_same_tenant_external_mapping_is_not_silently_ignored(fabric):
    external_target = "ddddaaaa-1111-2222-3333-444455556666"
    fabric.tenant_connections.extend([
        connection(id=REPLACEMENT, connectionDetails={"type": "SQL", "path": f"{TARGET_ENDPOINT};lh-new"}),
        connection(
            id=external_target, connectionDetails={"type": "SQL", "path": "external.example.com;other"},
        ),
    ])
    run = rebuild(fabric, connection_mappings={CONNECTION: REPLACEMENT, EXTERNAL: external_target})
    assert run.status is RunStatus.SUCCEEDED
    assert not any(kind == "Connection" for kind, _, _ in fabric.created)
    consumers = [
        target_id for kind, _, target_id in fabric.created if kind in {"DataPipeline", "Eventstream"}
    ]
    assert consumers
    for target_id in consumers:
        payload = decode_json_part(fabric.definitions[target_id][0]["payload"])
        assert payload["externalReferences"]["connection"] == external_target


def test_resume_preserves_a_previously_validated_mapping(fabric):
    fabric.tenant_connections.append(connection(
        id=REPLACEMENT, displayName="Bronze SQL (dest)",
        connectionDetails={"type": "SQL", "path": f"{TARGET_ENDPOINT};lh-new"},
    ))
    first = rebuild(fabric, connection_mappings={CONNECTION: REPLACEMENT})
    prior = journal.read(SETTINGS.journal_for(first.id))
    second = rebuild(fabric, prior=prior, connection_mappings={CONNECTION: REPLACEMENT})

    assert not any(kind == "Connection" for kind, _, _ in fabric.created)
    assert journal.read(SETTINGS.journal_for(second.id)).id_map[CONNECTION] == REPLACEMENT


def test_removed_connection_mapping_invalidates_the_prior_binding_on_retry(fabric):
    """Removing a mapping between attempts tombstones it hard enough to refuse a resume that
    would otherwise leave an already-created consumer bound to a connection nobody vouches
    for any more, rather than silently reconciling it as if nothing had changed."""
    fabric.tenant_connections.append(connection(
        id=REPLACEMENT, displayName="Bronze SQL (dest)",
        connectionDetails={"type": "SQL", "path": f"{TARGET_ENDPOINT};lh-new"},
    ))
    first = rebuild(fabric, connection_mappings={CONNECTION: REPLACEMENT})
    assert journal.read(SETTINGS.journal_for(first.id)).id_map[CONNECTION] == REPLACEMENT

    prior = journal.read(SETTINGS.journal_for(first.id))
    second = MigrationRun(source_workspace_name="bronze-ws", capacity_name="F64")
    plan = replace(make_plan(), include_data=False, include_files=False)
    orchestrator.run_migration(second, PRINCIPAL, plan, cleanup=False, prior=prior)

    # The Eventstream this mapping used to bind was already created last time; with the
    # mapping gone it cannot be silently rebound, so the resume itself is refused rather
    # than quietly finishing with the stale binding intact.
    assert second.status == RunStatus.FAILED
    assert "Live events" in second.error and "Bronze SQL" in second.error
    assert CONNECTION not in journal.read(SETTINGS.journal_for(second.id)).id_map


@pytest.mark.parametrize("mismatch", ["type", "connectivityType"])
def test_a_mapping_to_a_connection_that_no_longer_matches_the_migrated_target_is_refused(fabric, mismatch):
    replacement = connection(
        id=REPLACEMENT, displayName="Bronze SQL (dest)",
        connectionDetails={"type": "SQL", "path": f"{TARGET_ENDPOINT};lh-new"},
    )
    if mismatch == "type":
        replacement["connectionDetails"]["type"] = "Web"
    else:
        replacement["connectivityType"] = "PersonalCloud"
    fabric.tenant_connections.append(replacement)

    run = rebuild(fabric, connection_mappings={CONNECTION: REPLACEMENT})
    assert not any(kind in {"DataPipeline", "Eventstream"} for kind, _, _ in fabric.created)
    assert CONNECTION not in journal.read(SETTINGS.journal_for(run.id)).id_map
    assert any("no longer matches its migrated target" in warning for warning in run.summary["warnings"])


def test_a_mapping_to_an_unreadable_destination_connection_is_refused(fabric):
    fabric.unreadable.add(REPLACEMENT)
    run = rebuild(fabric, connection_mappings={CONNECTION: REPLACEMENT})

    assert not any(kind in {"DataPipeline", "Eventstream"} for kind, _, _ in fabric.created)
    assert CONNECTION not in journal.read(SETTINGS.journal_for(run.id)).id_map
    assert any(
        "Denied" in warning and "share replacement" in warning for warning in run.summary["warnings"]
    )


# ------------------------------------------------------------------------- early mirror consumer


def test_early_mirror_consumer_refuses_an_unmapped_connection(fabric):
    mirror = {"id": "mirror", "displayName": "Internal mirror", "type": "MirroredDatabase"}
    fabric.early_items = [mirror]
    fabric.definitions["mirror"] = [part("mirroring.json", {"connection": CONNECTION})]

    run = rebuild(fabric)
    assert not any(kind == "MirroredDatabase" for kind, _, _ in fabric.created)
    assert any(
        "Internal mirror" in warning and "Bronze SQL" in warning for warning in run.summary["warnings"]
    )
    assert "mirror" not in journal.read(SETTINGS.journal_for(run.id)).id_map


def test_connection_detection_does_not_require_the_relations_api(fabric, monkeypatch):
    from fabshuffle.fabric import relations

    monkeypatch.setattr(
        relations, "build_graph", lambda *a, **k: relations.DependencyGraph(available=False),
    )
    rebuild(fabric)
    assert not any(kind in {"DataPipeline", "Eventstream"} for kind, _, _ in fabric.created)


# ---------------------------------------------------------------- shortcuts bind connections too


def test_a_connection_referenced_only_by_a_shortcut_is_still_detected():
    """A shortcut's target carries its own ``connectionId``, separate from an item's own
    definition parts, so it must be picked up even when nothing else in the workspace names
    the connection directly."""
    lakehouse = {"id": LAKEHOUSE, "displayName": "bronze", "type": "Lakehouse"}
    source_connection = connection(
        id=CONNECTION, connectionDetails={"type": "SQL", "path": f"{SOURCE_ENDPOINT};{LAKEHOUSE}"},
    )

    class Client:
        def list_all(self, path, params=None, value_key="value"):
            if path == "connections":
                return [source_connection]
            if path == f"workspaces/{SOURCE_WS}/lakehouses":
                return []
            if path == f"workspaces/{SOURCE_WS}/items/{LAKEHOUSE}/shortcuts":
                return [{
                    "name": "ext", "path": "Files/ext",
                    "target": {"adlsGen2": {"connectionId": CONNECTION}},
                }]
            raise AssertionError(f"unexpected list {path}")

    ctx = orchestrator._Context(
        client=Client(), tokens=object(), principal=PRINCIPAL, plan=make_plan(),
        run=MigrationRun(source_workspace_name="bronze-ws", capacity_name="F64"), scratch_dir=None,
        target_workspace_id=TARGET_WS,
        source_items={LAKEHOUSE: dict(lakehouse)},
        id_map={SOURCE_WS: TARGET_WS, LAKEHOUSE: "lh-new"},
        assessment=assess_workspace([lakehouse], force_rebuild=True),
    )
    orchestrator._load_source_references(ctx)

    # Detected and tracked (available for the operator to map), but still not resolved: a
    # shortcut reference does not imply a mapping any more than an item definition does.
    assert ctx.source_items[CONNECTION]["type"] == "Connection"
    assert CONNECTION not in ctx.id_map


def test_a_connection_pointing_at_the_workspace_but_referenced_by_nothing_is_left_alone():
    """Fab Shuffle never scans every tenant connection for one that merely points at the
    workspace; only connections its selected items and shortcuts actually reference matter."""
    lakehouse = {"id": LAKEHOUSE, "displayName": "bronze", "type": "Lakehouse"}
    unreferenced = "dddddddd-1111-2222-3333-444455556666"
    stray = connection(
        id=unreferenced, displayName="Unrelated",
        connectionDetails={"type": "SQL", "path": f"{SOURCE_ENDPOINT};{LAKEHOUSE}"},
    )

    class Client:
        def list_all(self, path, params=None, value_key="value"):
            if path == "connections":
                return [stray]
            if path == f"workspaces/{SOURCE_WS}/lakehouses":
                return []
            if path == f"workspaces/{SOURCE_WS}/items/{LAKEHOUSE}/shortcuts":
                return []
            raise AssertionError(f"unexpected list {path}")

    ctx = orchestrator._Context(
        client=Client(), tokens=object(), principal=PRINCIPAL, plan=make_plan(),
        run=MigrationRun(source_workspace_name="bronze-ws", capacity_name="F64"), scratch_dir=None,
        target_workspace_id=TARGET_WS,
        source_items={LAKEHOUSE: dict(lakehouse)},
        id_map={SOURCE_WS: TARGET_WS, LAKEHOUSE: "lh-new"},
        assessment=assess_workspace([lakehouse], force_rebuild=True),
    )
    orchestrator._load_source_references(ctx)

    assert unreferenced not in ctx.source_items
    assert unreferenced not in ctx.id_map
    assert not ctx.warnings
