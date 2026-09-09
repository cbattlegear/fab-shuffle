"""Exercise the actual wizard JavaScript without adding a browser or build dependency."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

NODE = shutil.which("node")


@pytest.mark.skipif(NODE is None, reason="Node is needed to exercise the plain JavaScript UI")
@pytest.mark.parametrize(
    "scenario",
    [
        "throttle",
        "terminal",
        "run_races",
        "filters",
        "safe_text_and_focus",
        "connection_advisories_render",
        "connection_advisories_states",
        "connection_advisories_personal_and_retired",
        "connection_advisories_safe_text",
        "connection_lookup_script_flow",
        "connection_lookup_races",
        "errors_and_empty",
        "export",
        "sign_out",
        "hidden_until_halted",
        "active_again",
        "target_is_not_spinner",
    ],
)
def test_readiness_ui(scenario: str) -> None:
    result = subprocess.run(
        [NODE, str(Path(__file__).with_name("readiness_ui_checks.js")), scenario],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
