"""Counting what a migration will move, for the review screen.

The count comes out of the assessment rather than from asking Fabric for each type in turn.
That costs nothing, cannot disagree with what the run then does, and means a newly supported
item type is counted the moment it is supported instead of when someone remembers to add it
to the preview.
"""

from __future__ import annotations

import pytest

from fabshuffle.fabric.support import (
    DISPLAY_ORDER,
    REBUILT_TYPES,
    Strategy,
    WorkspaceAssessment,
    assess_workspace,
    type_label,
)


def counts(items):
    return assess_workspace(items).migrated_counts()


def test_every_supported_type_is_counted_not_just_the_data_stores():
    result = counts(
        [
            {"displayName": "bronze", "type": "Lakehouse"},
            {"displayName": "Nightly", "type": "DataPipeline"},
            {"displayName": "Explore", "type": "Notebook"},
            {"displayName": "Sales", "type": "Report"},
        ]
    )

    assert {entry["type"] for entry in result} == {"Lakehouse", "Notebook", "Report", "DataPipeline"}
    assert all(entry["count"] == 1 for entry in result)


def test_items_of_the_same_type_are_added_up():
    result = counts([{"displayName": f"lh{n}", "type": "Lakehouse"} for n in range(4)])

    assert result == [{"type": "Lakehouse", "label": "Lakehouses", "count": 4}]


def test_the_order_follows_the_order_the_migration_creates_things_in():
    result = counts(
        [
            {"displayName": "Sales", "type": "Report"},
            {"displayName": "Nightly", "type": "DataPipeline"},
            {"displayName": "bronze", "type": "Lakehouse"},
            {"displayName": "events", "type": "Eventhouse"},
        ]
    )

    # Data stores first, then what reads them, then what orchestrates that: the same order
    # the operator watches the run happen in.
    assert [entry["type"] for entry in result] == [
        "Lakehouse",
        "Eventhouse",
        "Report",
        "DataPipeline",
    ]


def test_an_unsupported_item_is_not_counted_as_moving():
    result = counts(
        [
            {"displayName": "bronze", "type": "Lakehouse"},
            {"displayName": "Exec", "type": "Dashboard"},
        ]
    )

    assert [entry["type"] for entry in result] == ["Lakehouse"]


def test_a_derived_item_is_not_counted_either():
    # A SQL analytics endpoint arrives with its lakehouse, so counting it would double up.
    result = counts(
        [
            {"displayName": "bronze", "type": "Lakehouse"},
            {"displayName": "bronze", "type": "SQLEndpoint"},
        ]
    )

    assert [entry["type"] for entry in result] == ["Lakehouse"]


def test_a_type_missing_from_the_display_order_still_appears():
    """A newly supported type must be visible before anyone decides where it sorts."""
    result = assess_workspace(
        [{"displayName": "x", "type": "Lakehouse"}, {"displayName": "y", "type": "Warehouse"}]
    ).migrated_counts()
    assert [entry["type"] for entry in result] == ["Lakehouse", "Warehouse"]

    # Simulate one that nobody has ranked yet by checking the fallback directly.
    assessment = WorkspaceAssessment(
        Strategy.REBUILD,
        migrated=[
            {"displayName": "a", "type": "Lakehouse"},
            {"displayName": "b", "type": "SomeFutureThing"},
        ],
    )
    assert [entry["type"] for entry in assessment.migrated_counts()] == [
        "Lakehouse",
        "SomeFutureThing",
    ]


def test_the_total_matches_the_sum_of_the_parts():
    items = [
        {"displayName": "a", "type": "Lakehouse"},
        {"displayName": "b", "type": "Lakehouse"},
        {"displayName": "c", "type": "Notebook"},
        {"displayName": "d", "type": "Dashboard"},
    ]
    assessment = assess_workspace(items)

    assert assessment.migrated_total == 3
    assert sum(entry["count"] for entry in assessment.migrated_counts()) == 3


def test_a_reassignment_has_nothing_to_count():
    # Reassignment moves the workspace itself, so no item is recreated.
    assessment = assess_workspace([{"displayName": "Sales", "type": "Report"}])

    assert assessment.migrated_counts() == []
    assert assessment.migrated_total == 0


# ------------------------------------------------------------------- labels


@pytest.mark.parametrize(
    ("item_type", "expected"),
    [
        ("Lakehouse", "Lakehouses"),
        ("Notebook", "Notebooks"),
        ("SemanticModel", "Semantic models"),
        ("SparkJobDefinition", "Spark job definitions"),
        ("SQLDatabase", "SQL databases"),
        ("CosmosDBDatabase", "Cosmos DB databases"),
        ("GraphQLApi", "GraphQL APIs"),
        ("ApacheAirflowJob", "Apache Airflow jobs"),
        ("CopyJob", "Copy Jobs"),
        ("Reflex", "Activator items"),
        ("KQLQueryset", "KQL querysets"),
    ],
)
def test_type_names_are_turned_into_readable_plurals(item_type, expected):
    assert type_label(item_type) == expected


def test_an_unknown_type_is_still_given_a_readable_name():
    assert type_label("SomeFutureThing") == "Some future things"


def test_a_name_ending_in_y_is_pluralised_properly():
    assert type_label("Ontology") == "Ontologies"


# ------------------------------------------------------------ the two lists


def test_every_rebuilt_type_has_a_place_in_the_display_order():
    """Otherwise a supported type sorts to the end of the review screen by accident."""
    missing = REBUILT_TYPES - set(DISPLAY_ORDER)
    assert not missing, f"add these to DISPLAY_ORDER: {sorted(missing)}"


def test_the_display_order_does_not_promise_types_we_cannot_move():
    extra = set(DISPLAY_ORDER) - REBUILT_TYPES
    assert not extra, f"these are not migrated, so remove them: {sorted(extra)}"
