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
    history = next(
        entry["actions"] for entry in run.snapshot()["steps"][0]["activity"] if entry["id"] == first
    )
    assert len(history) == 5 and history[-1]["action"] == "action 9"


def test_activity_window_shows_later_items_and_prefers_retaining_active_ones():
    run = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    run.start_step("dependencies", "Checking dependencies")
    run.item_activity("active", "still running", "Notebook", "Reading", "running")
    for i in range(60):
        run.item_activity(str(i), f"item {i}", "Notebook", "Reading", "running")
        run.item_activity(str(i), f"item {i}", "Notebook", "Read", "succeeded")
    run.item_activity("later", "current", "Notebook", "Reading", "running")
    snapshot = run.snapshot()["steps"][0]
    ids = {entry["id"] for entry in snapshot["activity"]}
    assert {"active", "later", "59"} <= ids
    assert "0" not in ids
    assert snapshot["activityOmitted"] == 12
    run.item_activity("later", "current", "Notebook", "Still reading", "running")
    assert run.snapshot()["steps"][0]["activityOmitted"] == 12


def test_lifecycle_operations_publish_without_changing_evidence():
    run = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    run.start_step("items", "Migrating items")
    lifecycle = Lifecycle(attempt_id=run.id, activity=run.item_activity)
    item = lifecycle.item("one", "Report", "Report")
    with item.operation("definition", "Definition created."):
        assert run.snapshot()["steps"][0]["activity"][0]["actions"][-1]["state"] == "running"
    assert run.snapshot()["steps"][0]["activity"][0]["actions"][-1]["state"] == "succeeded"


def test_activity_keeps_item_active_when_parallel_operation_finishes():
    run = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    run.start_step("items", "Migrating items")
    run.item_activity("one", "Report", "Report", "Data started", "running", "data")
    run.item_activity("one", "Report", "Report", "Schema started", "running", "schema")
    run.item_activity("one", "Report", "Report", "Schema done", "succeeded", "schema")
    for i in range(50):
        run.item_activity(str(i), str(i), "Report", "Done", "succeeded")
    entries = run.snapshot()["steps"][0]["activity"]
    assert next(item for item in entries if item["id"] == "one")["active"]
    run.item_activity("one", "Report", "Report", "Data done", "succeeded", "data")
    entries = run.snapshot()["steps"][0]["activity"]
    assert not next(item for item in entries if item["id"] == "one")["active"]


def test_log_modes_and_safe_tool_output(caplog):
    names = ("", "fabshuffle", "fabshuffle.stage", "fabshuffle.activity",
             "httpx", "httpcore", "urllib3", "azure", "msal", "uvicorn.access")
    previous = {name: logging.getLogger(name).level for name in names}
    activity = logging.getLogger("fabshuffle.activity")
    item_log = logging.getLogger("fabshuffle.orchestrator")
    try:
        configure_logging("basic")
        assert not activity.isEnabledFor(logging.INFO)
        item_log.info("Per-item detail")
        item_log.warning("A real warning")
        logging.getLogger("fabshuffle.stage").info("Stage progress")
        run = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
        run.start_step("lakehouse", "Migrating lakehouses")
        run.update_step("lakehouse", "Creating lakehouse 'bronze'")
        assert "Per-item detail" not in caplog.text
        assert "Creating lakehouse 'bronze'" not in caplog.text
        assert "A real warning" in caplog.text
        assert "Stage progress" in caplog.text
        configure_logging("verbose")
        assert activity.isEnabledFor(logging.INFO)
        item_log.info("Per-item detail")
        run.update_step("lakehouse", "Creating lakehouse 'bronze'")
        assert "Per-item detail" in caplog.text
        assert "Creating lakehouse 'bronze'" in caplog.text
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
        for name, level in previous.items():
            logging.getLogger(name).setLevel(level)


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
