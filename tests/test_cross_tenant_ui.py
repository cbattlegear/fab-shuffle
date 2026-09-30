"""Exercise the real paired wizard without installing a browser or build tool."""

import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("scenario", [
    "login", "freeze_and_review", "mappings_and_execution",
    "stale_and_failed_assessment", "paired_resume",
    "edit_mappings_before_retry",
    "same_tenant_pair_requires_freeze",
    "mirror_start_option", "same_tenant_mirror_retry",
    "standby_login_hides_other_tenant",
])
def test_cross_tenant_ui(scenario):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed for wizard checks")
    result = subprocess.run(
        [node, str(Path(__file__).with_name("cross_tenant_ui_checks.js")), scenario],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
