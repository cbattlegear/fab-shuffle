from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric import powerbi
from fabshuffle.web import app as web
from tests.test_web_api import StubTokens, auth

CAPACITY = {"id": "cap-1", "displayName": "F64", "region": "West Europe"}
WORKSPACE = {"id": "ws-1", "displayName": "Sales"}


@pytest.fixture
def client() -> TestClient:
    return TestClient(web.app)


@pytest.fixture
def session_id():
    session = web.SESSIONS.create(ServicePrincipal("tenant", "client", "secret"), StubTokens())
    yield session.id
    web.SESSIONS.drop(session.id)


class FakeFabric:
    """Serves just the reads the preview endpoint performs."""

    def __init__(self, items: list[dict[str, str]]) -> None:
        self.items = items

    def __call__(self, _tokens) -> FakeFabric:
        return self

    def __enter__(self) -> FakeFabric:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def get(self, path, params=None):
        if path.startswith("capacities/"):
            return CAPACITY
        if path.startswith("workspaces/"):
            return WORKSPACE
        raise AssertionError(f"unexpected GET {path}")

    def list_all(self, path, params=None, value_key="value"):
        if path.endswith("/items"):
            return self.items
        return []


def install(monkeypatch, items, models=None):
    fabric = FakeFabric(items)
    monkeypatch.setattr(web, "FabricClient", fabric)
    monkeypatch.setattr("fabshuffle.orchestrator.FabricClient", fabric)
    monkeypatch.setattr("fabshuffle.orchestrator.list_items", lambda client, ws: items)
    monkeypatch.setattr(web, "list_items", lambda client, ws: items)
    monkeypatch.setattr(
        web.data_stores,
        "list_lakehouses",
        lambda c, w: [i for i in items if i["type"] == "Lakehouse"],
    )
    monkeypatch.setattr(web.data_stores, "list_warehouses", lambda c, w: [])
    monkeypatch.setattr(web.eventhouses, "list_eventhouses", lambda c, w: [])

    class FakePbi:
        def __call__(self, _tokens):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

        def list_semantic_models(self, workspace_id):
            return models or []

    monkeypatch.setattr(web, "PowerBiClient", FakePbi())


def preview(client, session_id):
    response = client.get(
        "/api/preview",
        params={"capacity_id": "cap-1", "source_workspace_id": "ws-1"},
        headers=auth(session_id),
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_preview_recommends_reassignment_for_power_bi_only(client, session_id, monkeypatch):
    install(
        monkeypatch,
        [{"displayName": "Sales", "type": "Report"}, {"displayName": "Model", "type": "SemanticModel"}],
        models=[powerbi.SemanticModel("1", "Model", powerbi.LARGE, "")],
    )
    result = preview(client, session_id)

    assert result["strategy"] == "reassign"
    assert result["largeSemanticModels"] == [{"id": "1", "name": "Model"}]
    assert result["blockers"] == []
    assert result["unsupported"] == []
    # Reassignment keeps the workspace, so the name must not be rewritten.
    assert result["targetWorkspaceName"] == "Sales"


def test_preview_blocks_reassignment_into_an_unsupported_region(client, session_id, monkeypatch):
    install(
        monkeypatch,
        [{"displayName": "Sales", "type": "Report"}],
        models=[powerbi.SemanticModel("1", "Model", powerbi.LARGE, "")],
    )
    monkeypatch.setitem(CAPACITY, "region", "Nowhere Land")
    try:
        result = preview(client, session_id)
    finally:
        CAPACITY["region"] = "West Europe"

    assert result["strategy"] == "reassign"
    assert any("does not support large semantic model storage" in b for b in result["blockers"])


def test_preview_lists_unsupported_items_for_a_rebuild(client, session_id, monkeypatch):
    install(
        monkeypatch,
        [
            {"displayName": "bronze", "type": "Lakehouse"},
            {"displayName": "Nightly", "type": "MLModel"},
            {"displayName": "Exec", "type": "Dashboard"},
            {"displayName": "Sales", "type": "Report"},
        ],
    )
    result = preview(client, session_id)

    assert result["strategy"] == "rebuild"
    assert result["targetWorkspaceName"] == "Sales-westeurope"
    assert result["counts"]["lakehouses"] == 1
    # The report is migrated and rebound, so it is not reported as left behind.
    assert result["unsupportedItemTypes"] == ["Dashboard", "MLModel"]

    names = {item["name"]: item for item in result["unsupported"]}
    assert set(names) == {"Nightly", "Exec"}
    assert "Power BI item type" in names["Exec"]["reason"]
