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
    "discovery_feedback_and_preserved_selections", "discovery_empty_partial_and_failed",
    "discovery_signout_ignores_late_result", "existing_workspace_picker_uses_recovery_names",
    "workspace_picker_rejects_missing_ambiguous_and_stale_choices",
    "named_choices_reject_missing_resources", "discovery_keeps_pending_operation_choice",
    "recovery_capacity_names_submit_exact_pair_and_catalog",
    "capacity_match_refresh_preserves_only_same_identity_approvals",
    "capacity_routes_are_named_and_unmatched_choices_are_disabled",
    "feedback_stays_with_the_action_and_marks_only_its_fields",
    "native_validation_is_reported_at_the_action",
    "warehouse_setup_prefills_saved_name_and_uses_selected_exact_item",
    "warehouse_list_errors_and_workspace_changes_do_not_select_stale_items",
    "warehouse_list_signout_drops_late_choices",
    "journeys_are_separate_and_navigation_never_reads_the_source",
    "initial_metadata_sync_offers_schedule_despite_data_gaps",
    "test_state_blocks_setup_and_incident_controls_without_claiming_production",
    "schedule_handoff_is_downloadable_not_a_deployment_claim",
    "normal_standby_setup_only_selects_workspace_scope",
    "standby_name_rule_invalidates_stale_selection_preview",
    "standby_prerequisites_belong_in_settings_not_the_selection_form",
    "late_workspace_choices_do_not_populate_an_incident",
    "destination_settings_load_without_source_discovery",
    "settings_load_is_not_lost_behind_pending_workspace_discovery",
    "ambiguous_workspace_names_are_not_selected_by_guessing",
    "scope_loading_errors_never_leave_an_enabled_noop_review_button",
    "late_settings_error_does_not_disable_a_loaded_workspace_selection",
    "activity_observation_identifies_the_local_worker_without_catalog_requests",
    "workspace_loading_and_preview_show_and_clear_activity_indicators",
    "retry_saved_sync_is_offered_without_rebuilding_its_selection",
    "legacy_resume_requires_review_and_sends_an_explicit_resume_flag",
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
