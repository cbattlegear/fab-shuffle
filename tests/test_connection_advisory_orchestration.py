"""The connection advisory phase, wired into a real (fake-backed) migration run.

Exercises the actual orchestrator pipeline rather than the module in isolation, so the phase
placement (after every id-map-building phase, before permissions), the best-effort scan on a
failed or cancelled run, the REASSIGN skip, and inheritance across a resume are all covered
against the same fake Fabric the rest of the rebuild-ordering suite uses.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from test_rebuild_ordering import (
    MODEL,
    MODEL_BIM,
    PRINCIPAL,
    REPORT,
    REPORT_PBIR,
    SOURCE_WS,
    TARGET_WS,
    FakeFabric,
    StubPowerBi,
    attempt,
    dies_at,
    make_plan,
)

from fabshuffle import journal, orchestrator
from fabshuffle.config import SETTINGS
from fabshuffle.fabric.definitions import part
from fabshuffle.fabric.support import Strategy
from fabshuffle.run import MigrationRun, RunStatus

MATCHING_CONNECTION = {
    "id": "conn-source-match", "displayName": "Bronze SQL", "connectivityType": "ShareableCloud",
    "connectionDetails": {"type": "Web", "path": f"https://example.com/{SOURCE_WS}/thing"},
}
UNRELATED_CONNECTION = {
    "id": "conn-unrelated", "displayName": "Other tenant thing", "connectivityType": "ShareableCloud",
    "connectionDetails": {"type": "Web", "path": "https://totally-unrelated.example.com/data"},
}


class ConnectionsFabric(FakeFabric):
    def __init__(self, connections=None):
        super().__init__()
        self.tenant_connections = connections if connections is not None else []

    def list_all(self, path, params=None, value_key="value"):
        if path == "connections":
            return self.tenant_connections
        return super().list_all(path, params, value_key)


@pytest.fixture
def fabric(monkeypatch):
    fake = ConnectionsFabric([MATCHING_CONNECTION, UNRELATED_CONNECTION])
    fake.definitions[MODEL] = [part("model.bim", MODEL_BIM), part(".platform", "{}")]
    fake.definitions[REPORT] = [part("definition.pbir", REPORT_PBIR), part(".platform", "{}")]

    monkeypatch.setattr(orchestrator, "FabricClient", fake)
    monkeypatch.setattr(orchestrator, "TokenProvider", lambda principal: object())
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", StubPowerBi())

    monkeypatch.setattr(orchestrator.workspaces, "clone_folder_tree", lambda c, s, t: {})
    monkeypatch.setattr(orchestrator.workspaces, "list_role_assignments", lambda c, w: [])
    monkeypatch.setattr(orchestrator.workspaces, "copy_role_assignments", lambda *a, **k: [])
    monkeypatch.setattr(orchestrator.shortcuts, "copy_shortcuts", lambda *a, **k: (0, []))
    monkeypatch.setattr(orchestrator.file_transfer, "copy_files", lambda **k: None)
    monkeypatch.setattr(orchestrator.sqlschema, "transfer_schema", lambda **k: [])
    return fake


def run(fabric: FakeFabric) -> MigrationRun:
    migration = MigrationRun(source_workspace_name="bronze-ws", capacity_name="F64")
    orchestrator.run_migration(migration, PRINCIPAL, make_plan(), cleanup=False)
    return migration


# --------------------------------------------------------------------- normal path


def test_the_phase_runs_after_reflexes_and_before_permissions(fabric):
    migration = run(fabric)
    assert migration.status == RunStatus.SUCCEEDED, migration.error
    ids = [step["id"] for step in migration.snapshot()["steps"]]
    assert ids.index("reflexes") < ids.index("connectionadvisory") < ids.index("permissions")


def test_a_matching_tenant_connection_is_recorded_an_unrelated_one_is_not(fabric):
    migration = run(fabric)
    assert migration.status == RunStatus.SUCCEEDED, migration.error

    advisory = migration.connection_advisory
    assert advisory is not None
    assert advisory["scanState"] == "complete"
    ids = [entry["connectionId"] for entry in advisory["connections"]]
    assert ids == ["conn-source-match"]
    assert advisory["attemptId"] == migration.id
    assert advisory["sourceWorkspaceId"] == SOURCE_WS


def test_the_scan_is_persisted_to_the_journal(fabric):
    migration = run(fabric)
    replay = journal.read(SETTINGS.journal_for(migration.id))
    assert replay.connection_advisory is not None
    assert replay.connection_advisory["connections"][0]["connectionId"] == "conn-source-match"


def test_the_expected_new_path_reflects_the_migrated_target_workspace(fabric):
    migration = run(fabric)
    advisory = migration.connection_advisory
    entry = advisory["connections"][0]
    assert entry["expectedNewPath"] == f"https://example.com/{TARGET_WS}/thing"


# ---------------------------------------------------------------------- reassign


def test_reassign_never_scans_and_never_records_anything(fabric):
    migration = MigrationRun(source_workspace_name="bronze-ws", capacity_name="F64")
    plan = replace(make_plan(), strategy=Strategy.REASSIGN)
    orchestrator.run_migration(migration, PRINCIPAL, plan, cleanup=False)
    assert migration.status == RunStatus.SUCCEEDED, migration.error
    assert migration.connection_advisory is None
    assert "connectionadvisory" not in [step["id"] for step in migration.snapshot()["steps"]]


# ----------------------------------------------------------- best effort on failure


def test_a_run_that_fails_earlier_does_not_start_another_network_scan(fabric):
    with dies_at("engineering"):
        first = attempt(make_plan())
    assert first.status == RunStatus.FAILED
    steps = {step["id"]: step for step in first.snapshot()["steps"]}
    assert "connectionadvisory" not in steps
    assert first.connection_advisory is None


def test_a_best_effort_scan_failure_does_not_change_the_run_status(fabric, monkeypatch):
    def explode(*args, **kwargs):
        raise RuntimeError("the tenant connections listing is unreachable")

    monkeypatch.setattr(orchestrator.connection_advisory, "scan_source_connections", explode)
    with dies_at("engineering"):
        first = attempt(make_plan())
    assert first.status == RunStatus.FAILED
    assert first.error and "engineering" not in first.error.lower()
    assert first.connection_advisory is None


def test_a_cancelled_run_does_not_start_another_network_scan(fabric):

    def cancelled(_ctx):
        raise orchestrator.CancelledError("operator cancelled the migration")

    original = orchestrator._REBUILD_PHASES
    orchestrator._REBUILD_PHASES = tuple(
        (name, cancelled if name == "engineering" else fn) for name, fn in original
    )
    try:
        migration = MigrationRun(source_workspace_name="bronze-ws", capacity_name="F64")
        orchestrator.run_migration(migration, PRINCIPAL, make_plan(), cleanup=False)
    finally:
        orchestrator._REBUILD_PHASES = original

    assert migration.status == RunStatus.CANCELLED
    assert migration.connection_advisory is None
    assert "connectionadvisory" not in [step["id"] for step in migration.snapshot()["steps"]]


# -------------------------------------------------------------------------- resume


def test_a_resumed_attempt_refreshes_the_scan_with_the_new_attempt_id(fabric):
    with dies_at("permissions"):
        first = attempt(make_plan())
    assert first.status == RunStatus.FAILED
    assert first.connection_advisory["attemptId"] == first.id

    replay = journal.read(SETTINGS.journal_for(first.id))
    second = attempt(orchestrator.plan_from_journal(replay), prior=replay)
    assert second.status == RunStatus.SUCCEEDED, second.error
    assert second.connection_advisory["attemptId"] == second.id
    assert second.connection_advisory["attemptId"] != first.id


def test_a_resumed_attempt_that_fails_early_retains_the_previous_scan_as_stale(fabric):
    with dies_at("permissions"):
        first = attempt(make_plan())
    replay = journal.read(SETTINGS.journal_for(first.id))

    with dies_at("orchestration"):
        second = attempt(orchestrator.plan_from_journal(replay), prior=replay)
    assert second.status == RunStatus.FAILED
    assert second.connection_advisory is not None
    assert second.connection_advisory["attemptId"] == first.id
