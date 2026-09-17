"""Exercise the shipped BCDR JavaScript using the existing Node-in-Docker test pattern."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("scenario", [
    "open_is_read_only", "recovery_login_skips_source_discovery", "controls_pin_backend_ids",
    "partial_is_not_ready", "service_errors_and_pending_controls",
    "stale_session_never_renders_old_results", "schema_controls_are_native_not_json",
    "signout_clears_sensitive_results", "optional_object_is_omitted_unless_selected",
    "signout_ignores_late_errors",
    "capacity_state_is_not_inferred_from_mode",
    "returning_to_migration_cannot_discover_for_a_new_session",
    "reconciliation_pins_controller_not_writer_epoch", "reconciliation_confirmation_uses_exact_action",
    "readiness_pins_issuer_generation_and_writer", "qualification_is_not_generated_by_defaults",
    "missing_readiness_context_never_uses_lineage_generation",
    "temporary_attachment_retains_source_and_is_not_ready",
])
def test_bcdr_ui(scenario):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed for wizard checks")
    result = subprocess.run(
        [node, str(Path(__file__).with_name("bcdr_ui_checks.js")), scenario],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_every_shipped_schema_has_native_controls(tmp_path):
    from fabshuffle.web.bcdr import COMMANDS

    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed for wizard checks")
    schemas = tmp_path / "schemas.json"
    schemas.write_text(json.dumps([
        {
            "name": name, "label": label, "description": description, "confirmation": confirmation,
            "schema": model.model_json_schema(),
        }
        for name, label, model, description, confirmation in COMMANDS if model is not None
    ]), encoding="utf-8")
    result = subprocess.run(
        [node, str(Path(__file__).with_name("bcdr_ui_checks.js")), "shipped_schemas", str(schemas)],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
