from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

import pytest

from fabshuffle.__main__ import configure_logging
from fabshuffle.fabric.client import _log_route
from fabshuffle.lifecycle import Lifecycle
from fabshuffle.orchestrator import default_target_name
from fabshuffle.run import MigrationRun, RunStatus, StepStatus
from fabshuffle.transfer.sqlschema import _batches, _log_tool_output, _strip_sqlcmd_header


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


def test_activity_tracks_parallel_items_and_keeps_latest_snapshot_bounded():
    run = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    run._secrets = ("private-value",)
    run.start_step("dependencies", "Checking dependencies")
    subscriber = run.subscribe()
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(
            lambda n: run.item_activity(str(n), f"item {n} private-value", "Notebook", "Reading", "running"),
            range(60),
        ))
    snapshot = run.snapshot()["steps"][0]
    assert len(snapshot["activity"]) == 50
    assert snapshot["activityOmitted"] == 10
    assert "private-value" not in str(snapshot)
    first = snapshot["activity"][0]["id"]
    for i in range(10):
        run.item_activity(first, "item", "Notebook", f"action {i}", "running")
    run.mark_finished(RunStatus.SUCCEEDED)
    events = []
    while not subscriber.empty():
        events.append(subscriber.get_nowait())
    assert events[-1] is None
    assert any(event and event["status"] == "succeeded" for event in events)
    history = run.snapshot()["steps"][0]["activity"][0]["actions"]
    assert len(history) == 5 and history[-1]["action"] == "action 9"


def test_lifecycle_operations_publish_without_changing_evidence():
    run = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    run.start_step("items", "Migrating items")
    lifecycle = Lifecycle(attempt_id=run.id, activity=run.item_activity)
    item = lifecycle.item("one", "Report", "Report")
    with item.operation("definition", "Definition created."):
        assert run.snapshot()["steps"][0]["activity"][0]["actions"][-1]["state"] == "running"
    assert run.snapshot()["steps"][0]["activity"][0]["actions"][-1]["state"] == "succeeded"


def test_log_modes_and_safe_tool_output(caplog):
    root = logging.getLogger()
    activity = logging.getLogger("fabshuffle.activity")
    previous_root, previous_activity = root.level, activity.level
    try:
        configure_logging("basic")
        assert not activity.isEnabledFor(logging.INFO)
        configure_logging("verbose")
        assert activity.isEnabledFor(logging.INFO)
        assert not logging.getLogger("fabshuffle.transfer.sqlschema").isEnabledFor(logging.DEBUG)
        configure_logging("debug")
        assert logging.getLogger("fabshuffle.transfer.sqlschema").isEnabledFor(logging.DEBUG)
        assert not logging.getLogger("httpx").isEnabledFor(logging.INFO)
        _log_tool_output("Initializing deployment (Start)\npassword=secret\n")
        assert "Initializing deployment (Start)" in caplog.text
        assert "password=secret" not in caplog.text
        with pytest.raises(ValueError, match="FAB_SHUFFLE_LOG_LEVEL"):
            configure_logging("invalid")
        route = _log_route("https://example.test/v1/workspaces/private/items/secret?token=hidden")
        assert route == "v1/workspaces/{resource}/items/{resource}"
    finally:
        root.setLevel(previous_root)
        activity.setLevel(previous_activity)


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
