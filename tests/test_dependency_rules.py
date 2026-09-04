"""Dependency rules derived from a real run against a test workspace.

The relations APIs report several types under different internal names than the item APIs
do, and Fabric is inconsistent about how it capitalises its own system items. Both caused
noise in a real run's output, and both are reproduced here.
"""

from __future__ import annotations

from fabshuffle.fabric import relations
from fabshuffle.fabric.items import is_monitoring_item, is_system_item
from fabshuffle.fabric.support import is_derived_type, normalise_type

SOURCE_WS = "ws-source"
MODEL = "sm-1"
ENDPOINT = "ep-1"
KQL_DB = "kdb-1"
EVENTHOUSE = "eh-1"


class FakeClient:
    def __init__(self, responses: dict[str, dict]) -> None:
        self.responses = responses

    def get(self, path, params=None):
        item_id = path.split("/items/")[1].split("/")[0]
        return self.responses.get(item_id, {"items": [], "relations": [], "workspaces": []})


def upstream(items, edges):
    return {"items": items, "relations": edges, "workspaces": []}


def edge(item_id, depends_on):
    return {"itemId": item_id, "dependentOnItemId": depends_on, "relationType": "Datasource"}


# ------------------------------------------------------- relations type names


def test_relation_type_names_are_normalised_to_item_api_names():
    # The relations APIs return their own internal spellings.
    assert normalise_type("Model") == "SemanticModel"
    assert normalise_type("SqlAnalyticsEndpoint") == "SQLEndpoint"
    assert normalise_type("KustoEventHouse") == "Eventhouse"
    # An unknown type is passed through rather than dropped.
    assert normalise_type("SomethingNew") == "SomethingNew"


def test_the_relations_spelling_of_a_sql_endpoint_counts_as_derived():
    assert is_derived_type("SqlAnalyticsEndpoint") is True
    assert is_derived_type("SQLEndpoint") is True
    assert is_derived_type("sqlanalyticsendpoint") is True
    assert is_derived_type("Lakehouse") is False


# ------------------------------------------------------------ system items


def test_monitoring_items_are_matched_however_fabric_capitalises_them():
    # Fabric returns "Monitoring KQL database", not "Monitoring KQL Database".
    for name in ("Monitoring KQL database", "Monitoring KQL Database", "Monitoring Eventhouse"):
        assert is_monitoring_item({"displayName": name}) is True, name
        assert is_system_item({"displayName": name}) is True, name


def test_staging_items_are_still_system_items():
    assert is_system_item({"displayName": "DataflowsStagingLakehouse"}) is True
    assert is_monitoring_item({"displayName": "DataflowsStagingLakehouse"}) is False


def test_a_normal_item_is_neither():
    assert is_system_item({"displayName": "Sales"}) is False
    assert is_monitoring_item({"displayName": "Sales"}) is False


# ------------------------------------------------------------- the real run


def test_a_semantic_model_on_its_own_sql_endpoint_is_not_reported():
    """Reproduces: Model 'CloneTest' depends on SqlAnalyticsEndpoint 'CloneTest'.

    The endpoint is created with its lakehouse, so it arrives on its own.
    """
    client = FakeClient(
        {
            MODEL: upstream(
                [
                    {
                        "id": ENDPOINT,
                        "type": "SqlAnalyticsEndpoint",
                        "displayName": "CloneTest",
                        "workspaceId": SOURCE_WS,
                    }
                ],
                [edge(MODEL, ENDPOINT)],
            )
        }
    )
    graph = relations.build_graph(
        client, SOURCE_WS, [{"id": MODEL, "type": "SemanticModel", "displayName": "CloneTest"}]
    )
    issues = relations.analyse(graph, migrated_ids={MODEL}, source_workspace_id=SOURCE_WS)

    assert issues == []


def test_the_monitoring_eventhouse_dependency_is_not_reported():
    """Reproduces: KQLDatabase 'Monitoring KQL database' depends on KustoEventHouse."""
    client = FakeClient(
        {
            KQL_DB: upstream(
                [
                    {
                        "id": EVENTHOUSE,
                        "type": "KustoEventHouse",
                        "displayName": "Monitoring Eventhouse",
                        "workspaceId": SOURCE_WS,
                    }
                ],
                [edge(KQL_DB, EVENTHOUSE)],
            )
        }
    )
    graph = relations.build_graph(
        client,
        SOURCE_WS,
        [{"id": KQL_DB, "type": "KQLDatabase", "displayName": "Monitoring KQL database"}],
    )
    issues = relations.analyse(graph, migrated_ids={KQL_DB}, source_workspace_id=SOURCE_WS)

    assert issues == []


def test_a_genuine_missing_dependency_is_still_reported():
    # The new rules must not silence a real problem.
    client = FakeClient(
        {
            MODEL: upstream(
                [
                    {
                        "id": "df-1",
                        "type": "Dataflow",
                        "displayName": "Nightly",
                        "workspaceId": SOURCE_WS,
                    }
                ],
                [edge(MODEL, "df-1")],
            )
        }
    )
    graph = relations.build_graph(
        client, SOURCE_WS, [{"id": MODEL, "type": "Model", "displayName": "Sales"}]
    )
    issues = relations.analyse(graph, migrated_ids={MODEL}, source_workspace_id=SOURCE_WS)

    assert len(issues) == 1
    # The message uses the item API name, matching what the portal shows.
    assert issues[0].message().startswith("SemanticModel 'Sales' depends on Dataflow 'Nightly'")
