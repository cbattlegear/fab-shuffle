"""Fixes from the first end-to-end run.

The run completed every phase, so what is left is quality of the reporting and a few things
that should not have been reported at all:

* seven items failed with a bare "HTTP 400. Recreate it manually", which says nothing that
  could be acted on and hid seven different causes;
* a warehouse schema was lost because a freshly created warehouse answers a login with 28000
  and "the database was not found", which reads like a permission problem;
* six CREATE SCHEMA batches were reported against a schema-enabled lakehouse whose schemas
  Fabric derives from OneLake, so they already existed;
* Monitoring_Eventstream was migrated and failed with 401, when it is a workspace monitoring
  item and should never have been attempted.
"""

from __future__ import annotations

import pyodbc
import pytest

from fabshuffle.fabric import analytics
from fabshuffle.fabric.client import FabricApiError, OperationFailed, OperationTimeout
from fabshuffle.fabric.items import is_monitoring_item, is_system_item
from fabshuffle.fabric.support import assess_workspace, normalise_type
from fabshuffle.transfer import sqlschema

# What Fabric actually returns when a pipeline's bindings cannot be resolved.
PIPELINE_400 = (
    '{"requestId":"1","errorCode":"ItemDisplayNameAlreadyInUse",'
    '"message":"Requested \'copyjob1\' is already in use","isRetriable":false}'
)


def test_a_rejected_request_repeats_what_the_service_said() -> None:
    """A bare status hid seven different causes behind one sentence."""
    error = FabricApiError("POST", "/items", 400, PIPELINE_400)

    message = analytics.describe_failure("CopyJob", "copyjob1", error)

    assert "ItemDisplayNameAlreadyInUse" in message
    assert "already in use" in message
    assert "check its data source bindings" not in message


def test_the_status_is_still_shown() -> None:
    error = FabricApiError("POST", "/items", 400, PIPELINE_400)

    assert "HTTP 400" in analytics.describe_failure("CopyJob", "copyjob1", error)


def test_an_unparseable_body_falls_back_to_the_old_advice() -> None:
    error = FabricApiError("POST", "/items", 400, "<html>gateway error</html>")

    message = analytics.describe_failure("DataPipeline", "p", error)

    assert "HTTP 400" in message
    assert "Recreate it manually" in message


def test_a_nested_error_body_is_still_read() -> None:
    error = FabricApiError("POST", "/items", 400, '{"error":{"errorCode":"Nested","message":"m"}}')

    assert "Nested" in analytics.describe_failure("CopyJob", "j", error)


def test_the_connection_case_still_gets_its_specific_advice() -> None:
    # It now matches on the code alone, so it works whichever way the failure arrived.
    for error in (
        OperationFailed("op", "Failed", {"errorCode": "DataSourcesValidationError", "message": "x"}),
        FabricApiError("POST", "/items", 400, '{"errorCode":"DataSourcesValidationError"}'),
    ):
        message = analytics.describe_failure("Eventstream", "Meshtastic", error)
        assert "Manage Connections and Gateways" in message


def test_a_timeout_still_says_it_might_arrive() -> None:
    message = analytics.describe_failure("Notebook", "nb", OperationTimeout("slow"))

    assert "still being created" in message


# ------------------------------------------------------- warehouse login timing

WAREHOUSE_NOT_READY = pyodbc.Error(
    "28000",
    "[28000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Login failed for user "
    "'<token-identified principal>'.Reason: Authentication was successful, but the database "
    "was not found or you have insufficient permissions to connect to it. (18456)",
)
REAL_LOGIN_FAILURE = pyodbc.Error("28000", "[28000] Login failed for user 'someone'. (18456)")


def test_a_warehouse_that_is_not_ready_yet_is_waited_out() -> None:
    """It reads like a permission problem and is really the endpoint catching up."""
    assert sqlschema.is_transient(WAREHOUSE_NOT_READY)


def test_a_genuine_login_failure_is_still_not_retried() -> None:
    assert not sqlschema.is_transient(REAL_LOGIN_FAILURE)


# ------------------------------------------------------------- schema already there

ALREADY_EXISTS = pyodbc.ProgrammingError(
    "42S01", "[42S01] There is already an object named 'year_2017' in the database."
)


def test_recreating_a_schema_that_exists_is_not_a_warning(monkeypatch, tmp_path) -> None:
    """A schema-enabled lakehouse derives its schemas from OneLake, so the DDL is redundant."""
    executed: list[str] = []

    class Cursor:
        def execute(self, batch: str) -> None:
            executed.append(batch)
            if batch.upper().startswith("CREATE SCHEMA"):
                raise ALREADY_EXISTS

    class Connection:
        autocommit = False

        def cursor(self):
            return Cursor()

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

    monkeypatch.setattr(sqlschema, "connect", lambda *a, **k: Connection())

    script = tmp_path / "Deploy.sql"
    script.write_text(
        "CREATE SCHEMA [year_2017] AUTHORIZATION [dbo];\nGO\n"
        "CREATE VIEW [year_2017].[V] AS SELECT 1 AS x;\nGO\n",
        encoding="utf-8",
    )

    warnings = sqlschema.apply_script(script, server="s", database="d", tokens=object())

    assert warnings == []
    # The view after it still ran, so nothing was skipped beyond the redundant batch.
    assert any("CREATE VIEW" in batch for batch in executed)


def test_a_real_batch_failure_is_still_reported(monkeypatch, tmp_path) -> None:
    class Cursor:
        def execute(self, _batch: str) -> None:
            raise pyodbc.ProgrammingError("42000", "[42000] Incorrect syntax near 'FROM'")

    class Connection:
        autocommit = False

        def cursor(self):
            return Cursor()

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

    monkeypatch.setattr(sqlschema, "connect", lambda *a, **k: Connection())

    script = tmp_path / "Deploy.sql"
    script.write_text("CREATE VIEW [dbo].[Broken] AS SELECT FROM;\n", encoding="utf-8")

    assert sqlschema.apply_script(script, server="s", database="d", tokens=object()) != []


@pytest.mark.parametrize(
    "error",
    [
        ALREADY_EXISTS,
        pyodbc.ProgrammingError("42000", "The schema 'x' already exists in the database"),
    ],
)
def test_already_exists_shapes_are_recognised(error) -> None:
    assert sqlschema._already_exists(error)


# --------------------------------------------------------- monitoring eventstream


def test_the_monitoring_eventstream_is_left_alone() -> None:
    """It comes from turning workspace monitoring on, so creating it returns 401."""
    item = {"displayName": "Monitoring_Eventstream", "type": "Eventstream"}

    assert is_system_item(item)
    assert is_monitoring_item(item)


def test_an_ordinary_eventstream_is_still_migrated() -> None:
    item = {"displayName": "Meshtastic", "type": "Eventstream"}

    assert not is_system_item(item)
    assert not is_monitoring_item(item)


def test_the_monitoring_eventstream_is_not_reported_as_unsupported() -> None:
    assessment = assess_workspace(
        [
            {"id": "1", "displayName": "Monitoring_Eventstream", "type": "Eventstream"},
            {"id": "2", "displayName": "Meshtastic", "type": "Eventstream"},
        ]
    )

    # assess_workspace is given an already-filtered list in the orchestrator, but a caller
    # that passes everything should not be told the monitoring one was left behind.
    assert [item["displayName"] for item in assessment.migrated] == [
        "Monitoring_Eventstream",
        "Meshtastic",
    ]


# ------------------------------------------------------------- relations naming


def test_a_fabric_sql_database_is_named_consistently() -> None:
    """The relations API calls it SQLDbNative; the item API calls it SQLDatabase."""
    assert normalise_type("SQLDbNative") == "SQLDatabase"
    assert normalise_type("sqldbnative") == "SQLDatabase"
