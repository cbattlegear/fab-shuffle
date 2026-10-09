from __future__ import annotations

from unittest.mock import Mock

import pytest

from fabshuffle.bcdr import workflow
from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.contracts import RecoveryMode
from fabshuffle.bcdr.service import StandbyDefaultsRequest, StandbySelectionRequest
from tests.test_bcdr_contracts import guid
from tests.test_bcdr_coordinator import system as system


@pytest.fixture
def scope(system, monkeypatch):
    workspace = system.captured.workspaces[0]
    entries = [{
        "id": workspace.identity.workspace_id, "displayName": workspace.display_name,
        "type": "Workspace", "capacityId": system.config.source_capacity_ids[0],
        "capacityRegion": "East US",
    }, {
        "id": guid(), "displayName": "Outside source scope", "type": "Workspace", "capacityId": guid(),
    }]
    source_read = Mock(return_value=entries)
    monkeypatch.setattr(workflow.workspaces, "list_workspaces", source_read)
    monkeypatch.setattr(workflow.workspaces, "list_capacities", lambda _: [
        {"id": identifier, "displayName": f"Recovery {index}", "region": "West US"}
        for index, identifier in enumerate(system.config.target_capacity_ids)
    ])
    system.scope_entries = entries
    system.source_read = source_read
    return system


def test_workspace_scope_reuses_environment_and_runs_manual_sync(scope):
    options = scope.service.standby_scope_options().details["standby_scope"]
    assert len(options["workspaces"]) == 1
    request = StandbySelectionRequest(workspace_ids=(options["workspaces"][0]["id"],))
    preview = scope.service.preview_standby_selection(request).details["standby_preview"]
    assert len(preview["workspaces"]) == 1 and not scope.captures
    result = scope.service.create_standby(request.model_copy(update={
        "expected_configuration": preview["configuration"],
    }))
    assert result.details["sync_summary"]["kind"] == "manual"
    assert scope.c.runtime.get("lifecycle", "last-scheduled-sync") is None
    saved = scope.c.runtime.get("plans", result.generation_id)
    assert saved["include_workspace_ids"] == list(request.workspace_ids)
    assert not saved["keywords"] and saved["capture"] and saved["park"]
    assert saved["capacity_routes"][0]["target_capacity_id"] == scope.config.target_capacity_ids[0]


def test_name_rule_is_casefolded_and_preserved_for_future_sync(scope):
    request = StandbySelectionRequest(selection_mode="pattern", name_pattern=" SOURCE ")
    preview = scope.service.preview_standby_selection(request).details["standby_preview"]
    result = scope.service.create_standby(request.model_copy(update={
        "expected_configuration": preview["configuration"],
    }))
    saved = scope.c.runtime.get("plans", result.generation_id)
    assert saved["keywords"] == ["source"] and not saved["include_workspace_ids"]
    assert len(scope.captures) == 1


def test_pattern_preview_uses_same_unicode_casefold_as_planner(scope):
    scope.scope_entries[0]["displayName"] = "Straße reports"
    result = scope.service.preview_standby_selection(
        StandbySelectionRequest(selection_mode="pattern", name_pattern="STRASSE"),
    )
    assert result.details["standby_preview"]["workspaces"][0]["displayName"] == "Straße reports"


@pytest.mark.parametrize("fields", [
    {}, {"selection_mode": "pattern", "name_pattern": "  "},
    {"selection_mode": "pattern", "name_pattern": "Source", "workspace_ids": [guid()]},
    {"workspace_ids": [guid()], "name_pattern": "Source"},
])
def test_empty_or_mixed_scope_is_never_implicit_select_all(fields):
    with pytest.raises(ValueError):
        StandbySelectionRequest.model_validate(fields)


def test_unavailable_workspaces_empty_matches_and_stale_review_never_capture(scope):
    with pytest.raises(RecoveryBlocked, match="unavailable"):
        scope.service.preview_standby_selection(StandbySelectionRequest(workspace_ids=(guid(),)))
    with pytest.raises(RecoveryBlocked, match="No workspaces"):
        scope.service.preview_standby_selection(
            StandbySelectionRequest(selection_mode="pattern", name_pattern="no matching workspaces"),
        )
    with pytest.raises(RecoveryBlocked, match="changed"):
        scope.service.create_standby(StandbySelectionRequest(
            workspace_ids=(scope.scope_entries[0]["id"],), expected_configuration="0" * 64,
        ))
    assert not scope.captures


@pytest.mark.parametrize("system", [{"catalog_separate": True}], indirect=True)
def test_ambiguous_destination_requires_explicit_one_time_default(scope):
    request = StandbySelectionRequest(workspace_ids=(scope.scope_entries[0]["id"],))
    with pytest.raises(RecoveryBlocked, match="default recovery capacity"):
        scope.service.preview_standby_selection(request)
    selected = scope.config.target_capacity_ids[1]
    scope.service.configure_standby_defaults(StandbyDefaultsRequest(target_capacity_id=selected))
    assert not scope.captures and not scope.estate.calls
    preview = scope.service.preview_standby_selection(request).details["standby_preview"]
    result = scope.service.create_standby(request.model_copy(update={
        "expected_configuration": preview["configuration"],
    }))
    routes = scope.c.runtime.get("plans", result.generation_id)["capacity_routes"]
    assert routes[0]["target_capacity_id"] == selected


@pytest.mark.parametrize("system", [{"catalog_separate": True}], indirect=True)
def test_default_does_not_override_an_existing_approved_route(scope):
    scope.service.synchronize(scope.request)
    scope.service.configure_standby_defaults(StandbyDefaultsRequest(
        target_capacity_id=scope.config.target_capacity_ids[1],
    ))
    request = StandbySelectionRequest(workspace_ids=(scope.scope_entries[0]["id"],))
    _, recipe, _, _ = workflow._selection_recipe(scope.c, request)
    assert recipe.capacity_routes == scope.request.capacity_routes


def test_settings_read_is_source_free_and_active_test_blocks_scope_discovery(scope):
    scope.c.source = None
    scope.service.standby_scope_options(include_workspaces=False)
    scope.source_read.assert_not_called()
    scope.harness.raw("UPDATE bcdr.control SET mode = ?", (RecoveryMode.TESTING.value,))
    with pytest.raises(RecoveryBlocked):
        scope.service.standby_scope_options()
    scope.source_read.assert_not_called()
    with pytest.raises(RecoveryBlocked):
        scope.service.configure_standby_defaults(StandbyDefaultsRequest(target_capacity_id=guid()))


def test_default_cannot_authorize_an_unconfigured_capacity(scope):
    with pytest.raises(RecoveryBlocked, match="already authorized"):
        scope.service.configure_standby_defaults(StandbyDefaultsRequest(target_capacity_id=guid()))
    assert scope.c.runtime.get("lifecycle", "standby-defaults") is None
