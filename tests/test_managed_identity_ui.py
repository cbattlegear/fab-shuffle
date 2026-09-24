"""Run the real authentication controls through the existing Linux Node harness."""

import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("scenario", [
    "availability_and_selection",
    "managed_login_is_secretless",
    "local_and_failed_options_cannot_enable_identity",
    "expired_easyauth_is_actionable",
])
def test_managed_identity_ui(scenario):
    node = shutil.which("node")
    assert node, "Use the Linux Docker test target, which includes Node."
    result = subprocess.run(
        [node, str(Path(__file__).with_name("managed_identity_ui_checks.js")), scenario],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_authentication_form_preserves_hidden_and_mobile_constraints():
    styles = (
        Path(__file__).parents[1] / "fabshuffle" / "web" / "static" / "styles.css"
    ).read_text(encoding="utf-8")
    assert "#tenant-mode[hidden] { display: none; }" in styles
    assert '.form select, .form input:not([type="checkbox"]) { width: 100%; min-width: 0; }' in styles
