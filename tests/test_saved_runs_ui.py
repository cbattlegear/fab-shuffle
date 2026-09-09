"""Run saved-migration browser logic with controlled requests and no live resources."""

import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("scenario", [
    "confirmation_cancel",
    "ignore_refresh",
    "ignore_failure",
    "full_restart_fresh_state",
    "restart_forces_rebuild",
    "ordinary_preview_not_forced",
    "start_over_clears_forced_rebuild",
    "restart_no_destination",
    "pending_and_blocked",
    "restart_failure_retry",
    "restart_failure_refresh_failure",
    "stale_session_success",
    "stale_session_failure",
    "logout_during_action",
    "stale_discovery_after_restart",
    "stale_preview_after_restart",
    "stale_workspace_discovery",
    "stale_resume_after_restart",
    "stale_connections_after_restart",
    "missing_source_reselection",
    "resume_locks_row",
    "saved_review_back",
])
def test_saved_runs_ui(scenario):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed for wizard checks")
    result = subprocess.run(
        [node, str(Path(__file__).with_name("saved_runs_ui_checks.js")), scenario],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
