from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric import powerbi, relations
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


def dependencies(client, session_id):
    response = client.get(
        "/api/preview/dependencies",
        params={"source_workspace_id": "ws-1"},
        headers=auth(session_id),
    )
    assert response.status_code == 200, response.text
    return response.json()["dependencies"]


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
    # A dashboard has no definition to read, which is worth saying rather than "not yet".
    assert "no way to read a dashboard's definition" in names["Exec"]["reason"]


def test_dependency_problems_are_reported_before_the_run_starts(client, session_id, monkeypatch):
    """The check is read only, so there is no reason to make the operator start a run for it."""
    install(monkeypatch, [{"id": "lh", "displayName": "bronze", "type": "Lakehouse"}])

    graph = relations.DependencyGraph(
        dependencies={"lh": {"outside"}},
        items={
            "lh": {"id": "lh", "displayName": "bronze", "type": "Lakehouse", "workspaceId": "ws-1"},
            "outside": {
                "id": "outside",
                "displayName": "shared",
                "type": "Warehouse",
                "workspaceId": "ws-other",
            },
        },
        workspaces={"ws-other": "Finance"},
    )
    monkeypatch.setattr(relations, "build_graph", lambda *a, **k: graph)
    monkeypatch.setattr("fabshuffle.orchestrator.connection_prerequisites", lambda *a, **k: [])

    problems = dependencies(client, session_id)

    assert problems
    assert any("Finance" in message for message in problems)


def test_the_review_says_so_when_dependencies_cannot_be_checked(client, session_id, monkeypatch):
    install(monkeypatch, [{"id": "lh", "displayName": "bronze", "type": "Lakehouse"}])
    monkeypatch.setattr(
        relations,
        "build_graph",
        lambda *a, **k: relations.DependencyGraph(available=False),
    )

    assert dependencies(client, session_id) == [
        "The relations API is unavailable to this service principal, so dependencies "
        "between items could not be checked."
    ]


def test_a_clean_workspace_reports_no_dependency_problems(client, session_id, monkeypatch):
    install(monkeypatch, [{"id": "lh", "displayName": "bronze", "type": "Lakehouse"}])
    monkeypatch.setattr(
        relations,
        "build_graph",
        lambda *a, **k: relations.DependencyGraph(dependencies={"lh": set()}),
    )
    monkeypatch.setattr("fabshuffle.orchestrator.connection_prerequisites", lambda *a, **k: [])

    assert dependencies(client, session_id) == []


def test_the_preview_does_not_wait_for_the_dependency_check(client, session_id, monkeypatch):
    """The two are separate requests so the review screen can appear before the slow one lands."""
    install(monkeypatch, [{"id": "lh", "displayName": "bronze", "type": "Lakehouse"}])

    def explode(*_args, **_kwargs):
        raise AssertionError("the preview must not run the dependency check")

    monkeypatch.setattr(relations, "build_graph", explode)

    assert "dependencies" not in preview(client, session_id)


def test_a_reassignment_skips_the_dependency_check_entirely(client, session_id, monkeypatch):
    # Nothing is rebuilt, so no reference has to be rewritten and nothing can be left dangling.
    install(monkeypatch, [{"id": "r", "displayName": "Sales", "type": "Report"}])

    def explode(*_args, **_kwargs):
        raise AssertionError("a reassignment has no references to check")

    monkeypatch.setattr(relations, "build_graph", explode)

    assert dependencies(client, session_id) == []
