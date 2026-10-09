from unittest.mock import Mock

import pytest

from fabshuffle.bcdr.capture import capture_item
from fabshuffle.bcdr.catalog import CapturedGeneration
from fabshuffle.bcdr.contracts import DependencyEdge, Qualification
from fabshuffle.fabric.support import assess_workspace
from tests.test_bcdr_contracts import guid
from tests.test_bcdr_coordinator import system as system


@pytest.mark.parametrize("dependent", [False, True])
def test_shared_exclusion_keeps_identity_without_blocking_unrelated_creation(system, dependent):
    supported = system.captured.items[0]
    identity = supported.identity.model_copy(update={"item_id": guid()})
    raw = {"id": identity.item_id, "type": "MLModel", "displayName": "Excluded trained model"}
    client = Mock()
    skipped = capture_item(client, identity, raw["type"], inventory_item=raw, readers=Mock())
    assert not client.mock_calls
    edges = (
        DependencyEdge(
            edge_id=guid(), consumer=supported.identity, prerequisite=identity, phase="bind",
            qualification=Qualification.DOCUMENTED, provenance="Exact source reference",
            detail="The notebook references the excluded model by its exact item identity",
        ),
    ) if dependent else ()
    snapshot = system.captured.model_copy(update={
        "items": (*system.captured.items, skipped.record), "dependencies": edges,
    })
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(snapshot, ())
    result = system.service.synchronize(system.request)
    captured = system.catalog.load_generation(result.generation_id).snapshot
    unsupported = next(item for item in captured.items if item.identity == identity)
    assert not unsupported.capture_complete
    assert unsupported.properties["bcdr"]["inventory_only"]
    expected = assess_workspace([raw], force_rebuild=True, require_stopped=True).unsupported[0].reason
    assert any("Excluded trained model" in warning and expected in warning for warning in result.warnings)
    assert all(item.get("type") != "MLModel" for item in system.estate.items.values())
    if dependent:
        assert result.groups[0].blockers
        assert not result.groups[0].metadata_applied
        assert not system.estate.items
    else:
        assert result.groups[0].metadata_applied
        assert len(system.estate.items) == 1
        assert next(iter(system.estate.items.values()))["type"] == "Notebook"
