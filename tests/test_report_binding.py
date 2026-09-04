"""Deciding whether a report that rewrote nothing is actually a problem.

A report names its semantic model in ``definition.pbir`` either by GUID, which has to be
repointed, or by relative path, which Fabric resolves inside the new workspace on its own.
Treating "nothing changed" as a failure warned about every report of the second kind, which
is the normal shape for a report stored beside its model.
"""

from __future__ import annotations

import json

from fabshuffle.fabric.analytics import report_binding_warning
from fabshuffle.fabric.definitions import part

MODEL = "sm-source"
ID_MAP = {MODEL: "sm-new"}


def pbir(reference):
    return [part("definition.pbir", json.dumps({"version": "4.0", "datasetReference": reference}))]


def by_connection(model_id):
    return pbir({"byConnection": {"connectionString": f"semanticmodelid={model_id}"}})


# ------------------------------------------------------------------- by path


def test_a_report_bound_by_path_is_not_a_problem():
    """The path is relative to the report, so it follows it into the new workspace."""
    parts = pbir({"byPath": {"path": "../Sales.SemanticModel"}})

    assert report_binding_warning("TestReport", parts, ID_MAP) is None


def test_a_path_binding_is_not_reported_even_with_an_empty_map():
    parts = pbir({"byPath": {"path": "../Sales.SemanticModel"}})

    assert report_binding_warning("TestReport", parts, {}) is None


# ------------------------------------------------------------- by connection


def test_a_model_that_did_migrate_is_not_reported():
    # Nothing changed only because the rewrite had already been applied.
    assert report_binding_warning("Sales", by_connection(MODEL), ID_MAP) is None


def test_a_model_that_did_not_migrate_is_named():
    message = report_binding_warning("Sales", by_connection("sm-elsewhere"), ID_MAP)

    assert "sm-elsewhere" in message
    assert "still reads from the original workspace" in message


def test_the_model_id_is_found_among_other_connection_properties():
    parts = pbir(
        {
            "byConnection": {
                "connectionString": "datasource=x;semanticmodelid=sm-elsewhere;initial catalog=y"
            }
        }
    )

    assert "sm-elsewhere" in report_binding_warning("Sales", parts, ID_MAP)


def test_a_connection_naming_no_model_is_called_out_rather_than_ignored():
    parts = pbir({"byConnection": {"connectionString": "datasource=something"}})
    message = report_binding_warning("Sales", parts, ID_MAP)

    assert "does not name a semantic model in a way we recognise" in message


# ---------------------------------------------------------------- edge cases


def test_a_report_without_a_pbir_is_reported_as_uncheckable():
    message = report_binding_warning("Sales", [part("report.json", "{}")], ID_MAP)

    assert "has no definition.pbir" in message


def test_an_unreadable_pbir_is_reported_rather_than_crashing():
    message = report_binding_warning(
        "Sales", [part("definition.pbir", "not json at all")], ID_MAP
    )

    assert "could not be read" in message


def test_a_pbir_with_no_dataset_reference_is_called_out():
    parts = [part("definition.pbir", json.dumps({"version": "4.0"}))]

    assert "does not name a semantic model" in report_binding_warning("Sales", parts, ID_MAP)
