from __future__ import annotations

from fabshuffle.orchestrator import default_target_name
from fabshuffle.run import MigrationRun, RunStatus, StepStatus
from fabshuffle.transfer.sqlschema import _batches, _strip_sqlcmd_header


def test_target_name_uses_capacity_region():
    assert default_target_name("Sales", "westeurope") == "Sales-westeurope"
    assert default_target_name("Sales", "") == "Sales-copy"


def test_run_tracks_step_lifecycle():
    run = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    run.mark_running()
    run.start_step("lakehouses", "Migrating lakehouses")
    run.update_step("lakehouses", "Creating lakehouse 'bronze'")
    run.finish_step("lakehouses", StepStatus.SUCCEEDED, "Migrated 1 lakehouse", ["files skipped"])
    run.mark_finished(RunStatus.SUCCEEDED)

    snapshot = run.snapshot()
    assert snapshot["status"] == "succeeded"
    step = snapshot["steps"][0]
    assert step["status"] == "succeeded"
    assert step["detail"] == "Migrated 1 lakehouse"
    assert step["warnings"] == ["files skipped"]
    assert step["startedAt"] and step["finishedAt"]


def test_subscriber_receives_snapshots_and_a_close_sentinel():
    run = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    subscriber = run.subscribe()
    assert subscriber.get_nowait()["status"] == "pending"

    run.start_step("a", "Step A")
    assert subscriber.get_nowait()["steps"][0]["status"] == "running"

    run.mark_finished(RunStatus.SUCCEEDED)
    # One snapshot for the status change, then the sentinel that closes the SSE stream.
    assert subscriber.get_nowait()["status"] == "succeeded"
    assert subscriber.get_nowait() is None


def test_cancellation_is_observable():
    run = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    assert run.cancelled is False
    run.raise_if_cancelled()

    run.cancel()
    assert run.cancelled is True
    try:
        run.raise_if_cancelled()
    except Exception as error:
        assert "cancelled" in str(error).lower()
    else:
        raise AssertionError("raise_if_cancelled should have raised")


def test_sqlcmd_header_is_stripped_before_batching():
    script = """:setvar DatabaseName "Sales"
PRINT N'Preamble';
GO
CREATE VIEW dbo.V AS SELECT 1 AS x;
GO
CREATE PROCEDURE dbo.P AS SELECT 1;
GO
"""
    batches = _batches(_strip_sqlcmd_header(script))
    assert len(batches) == 2
    assert batches[0].startswith("CREATE VIEW")
    assert batches[1].startswith("CREATE PROCEDURE")
