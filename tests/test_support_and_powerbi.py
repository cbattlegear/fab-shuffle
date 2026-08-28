from __future__ import annotations

import httpx
import pytest

from fabshuffle.fabric import powerbi
from fabshuffle.fabric.support import (
    Strategy,
    assess_workspace,
    supports_large_semantic_models,
)
from tests.test_client import StubTokens


def items(*specs: tuple[str, str]) -> list[dict[str, str]]:
    return [{"displayName": name, "type": item_type} for name, item_type in specs]


# ------------------------------------------------------------------ assessment


def test_power_bi_only_workspace_is_reassignable():
    assessment = assess_workspace(
        items(("Sales", "Report"), ("Sales Model", "SemanticModel"), ("Exec", "Dashboard"))
    )
    assert assessment.strategy is Strategy.REASSIGN
    # Reassignment carries everything, so nothing is reported as left behind.
    assert assessment.unsupported == []


def test_any_fabric_item_forces_a_rebuild():
    assessment = assess_workspace(items(("Sales", "Report"), ("bronze", "Lakehouse")))
    assert assessment.strategy is Strategy.REBUILD


def test_rebuild_warns_about_every_item_it_leaves_behind():
    assessment = assess_workspace(
        items(
            ("bronze", "Lakehouse"),
            ("Nightly load", "DataPipeline"),
            ("Exec", "Dashboard"),
            ("Sales", "Report"),
        )
    )
    assert assessment.strategy is Strategy.REBUILD
    # Reports are rebuilt and rebound, so only the pipeline and dashboard are left behind.
    assert assessment.unsupported_types == ["Dashboard", "DataPipeline"]

    by_name = {item.name: item for item in assessment.unsupported}
    assert "does not migrate this item type yet" in by_name["Nightly load"].reason
    assert "cannot recreate this Power BI item type yet" in by_name["Exec"].reason
    assert by_name["Nightly load"].message().startswith("DataPipeline 'Nightly load' was not migrated")


def test_derived_items_are_neither_migrated_nor_reported():
    assessment = assess_workspace(items(("bronze", "Lakehouse"), ("bronze", "SQLEndpoint")))
    assert assessment.unsupported == []


def test_unknown_fabric_types_still_force_a_rebuild():
    # An item type Fabric adds after this release must not be mistaken for Power BI content.
    assessment = assess_workspace(items(("Something", "BrandNewItemType")))
    assert assessment.strategy is Strategy.REBUILD
    assert assessment.unsupported_types == ["BrandNewItemType"]


def test_large_model_region_support():
    assert supports_large_semantic_models("westeurope") is True
    assert supports_large_semantic_models("West Europe") is True
    assert supports_large_semantic_models("nowhereland") is False


# --------------------------------------------------------------------- Power BI


def make_pbi(handler) -> powerbi.PowerBiClient:
    return powerbi.PowerBiClient(StubTokens(), transport=httpx.MockTransport(handler))


def test_list_semantic_models_reads_storage_mode():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer stub-token"
        return httpx.Response(
            200,
            json={
                "value": [
                    {"id": "1", "name": "Big", "targetStorageMode": "PremiumFiles"},
                    {"id": "2", "name": "Small", "targetStorageMode": "Abf"},
                    {"id": "3", "name": "Unknown"},
                ]
            },
        )

    with make_pbi(handler) as pbi:
        models = pbi.list_semantic_models("ws")

    assert [m.is_large for m in models] == [True, False, False]
    # A model with no reported mode behaves as small rather than blocking the move.
    assert models[2].storage_mode == powerbi.SMALL


def test_push_models_are_not_convertible():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "value": [
                    {
                        "id": "1",
                        "name": "Streaming",
                        "targetStorageMode": "PremiumFiles",
                        "ContentProviderType": "RealTimeInPushMode",
                    }
                ]
            },
        )

    with make_pbi(handler) as pbi:
        assert pbi.list_semantic_models("ws")[0].convertible is False


def test_convert_patches_then_waits_for_the_new_mode(monkeypatch):
    monkeypatch.setattr(powerbi.time, "sleep", lambda _: None)
    state = {"mode": "PremiumFiles", "patches": 0, "polls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PATCH":
            state["patches"] += 1
            assert request.read() == b'{"targetStorageMode":"Abf"}'
            return httpx.Response(200)
        state["polls"] += 1
        # The conversion is asynchronous, so the first poll still reports the old mode.
        if state["polls"] > 1:
            state["mode"] = "Abf"
        return httpx.Response(
            200,
            json={"value": [{"id": "1", "name": "Big", "targetStorageMode": state["mode"]}]},
        )

    model = powerbi.SemanticModel(id="1", name="Big", storage_mode="PremiumFiles", content_provider="")
    with make_pbi(handler) as pbi:
        pbi.convert("ws", model, powerbi.SMALL)

    assert state["patches"] == 1
    assert state["polls"] == 2


def test_convert_times_out_if_the_mode_never_changes(monkeypatch):
    monkeypatch.setattr(powerbi.time, "sleep", lambda _: None)
    monkeypatch.setattr(powerbi, "CONVERSION_TIMEOUT_SECONDS", 0)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PATCH":
            return httpx.Response(200)
        return httpx.Response(
            200, json={"value": [{"id": "1", "name": "Big", "targetStorageMode": "PremiumFiles"}]}
        )

    model = powerbi.SemanticModel(id="1", name="Big", storage_mode="PremiumFiles", content_provider="")
    with make_pbi(handler) as pbi:
        with pytest.raises(powerbi.PowerBiError, match="still PremiumFiles"):
            pbi.convert("ws", model, powerbi.SMALL)


def test_failed_request_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="not the dataset owner")

    with make_pbi(handler) as pbi:
        with pytest.raises(powerbi.PowerBiError, match="403"):
            pbi.list_semantic_models("ws")
