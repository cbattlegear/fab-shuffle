"""Reading what a SQL endpoint refresh actually did.

Refresh Sql Endpoint Metadata is a long running operation, so the client waits for it. But
settling is not succeeding: the result carries a status per table, and the call reports
success while individual tables failed to sync.

A table that did not sync is invisible on the endpoint, so the schema deploy that follows
fails on whatever reads it, naming the view rather than the table:

    Batch 4 failed (42S02): CREATE VIEW random_view AS SELECT * FROM random_table

with nothing anywhere pointing at random_table.
"""

from __future__ import annotations

from fabshuffle.fabric.data_stores import sync_failures


def table(name, status, error=None):
    entry = {
        "tableName": name,
        "status": status,
        "lastSuccessfulSyncDateTime": "2026-06-08T08:32:53Z",
    }
    if error:
        entry["error"] = error
    return entry


def test_a_table_that_failed_to_sync_is_named():
    response = {
        "value": [
            table("Orders", "Success"),
            table(
                "random_table",
                "Failure",
                {"errorCode": "DeltaTableNotFound", "message": "Delta table not found."},
            ),
        ]
    }

    failures = sync_failures(response)

    assert len(failures) == 1
    assert "random_table" in failures[0]
    assert "DeltaTableNotFound" in failures[0]
    # The point of saying it at all: the deploy failure names the view, not the table.
    assert "such as a view, will not deploy" in failures[0]


def test_a_refresh_where_everything_worked_says_nothing():
    assert sync_failures({"value": [table("Orders", "Success")]}) == []


def test_not_run_counts_as_not_synced():
    """It says in as many words that the operation did not run, so it is worth asking again."""
    from fabshuffle.fabric.data_stores import unsynced_tables

    assert len(unsynced_tables({"value": [table("Orders", "NotRun")]})) == 1
    assert unsynced_tables({"value": [table("Orders", "Success")]}) == []


def test_a_table_that_never_ran_is_reported_differently_from_one_that_failed():
    failures = sync_failures({"value": [table("Orders", "NotRun")]})

    assert "was not synced" in failures[0]
    # It may simply have been current already, so the wording does not claim a failure.
    assert "may already have been up to date" in failures[0]


def test_a_failure_without_an_error_still_names_the_table():
    failures = sync_failures({"value": [table("Orders", "Failure")]})

    assert "Orders" in failures[0]


def test_a_schema_qualified_name_is_kept():
    response = {"value": [table("sales.Orders", "Failure", {"errorCode": "X", "message": "y"})]}

    assert "sales.Orders" in sync_failures(response)[0]


def test_a_refresh_that_was_skipped_reports_nothing():
    # What refresh_sql_endpoint_metadata returns when the endpoint refused the call.
    assert sync_failures({"status": "Skipped", "reason": "..."}) == []


def test_an_empty_response_reports_nothing():
    assert sync_failures({}) == []


def test_several_failures_are_all_reported():
    response = {
        "value": [
            table("a", "Failure", {"errorCode": "X", "message": "one"}),
            table("b", "Success"),
            table("c", "Failure", {"errorCode": "Y", "message": "two"}),
        ]
    }

    assert len(sync_failures(response)) == 2


# ------------------------------------------------------------------ retrying


class FakeClient:
    """Returns a scripted sequence of refresh results, one per call."""

    def __init__(self, responses) -> None:
        self.responses = list(responses)
        self.calls = 0

    def post(self, path, json=None, params=None, wait=True):
        self.calls += 1
        index = min(self.calls - 1, len(self.responses) - 1)
        return self.responses[index]


def refresh(client, **kwargs):
    from fabshuffle.fabric.data_stores import refresh_sql_endpoint_metadata

    return refresh_sql_endpoint_metadata(client, "ws", "ep", **kwargs)


def test_a_refresh_where_everything_synced_is_not_repeated(monkeypatch):
    monkeypatch.setattr("fabshuffle.fabric.data_stores.time.sleep", lambda _s: None)
    client = FakeClient([{"value": [table("Orders", "Success")]}])

    refresh(client)

    assert client.calls == 1


def test_a_table_that_did_not_sync_is_asked_for_again(monkeypatch):
    monkeypatch.setattr("fabshuffle.fabric.data_stores.time.sleep", lambda _s: None)
    client = FakeClient(
        [
            {"value": [table("Orders", "Failure", {"errorCode": "X", "message": "y"})]},
            {"value": [table("Orders", "Success")]},
        ]
    )

    result = refresh(client)

    assert client.calls == 2
    assert sync_failures(result) == []


def test_retrying_stops_rather_than_going_round_forever(monkeypatch):
    """Every attempt failing must end, or a flaky endpoint stalls the whole migration."""
    monkeypatch.setattr("fabshuffle.fabric.data_stores.time.sleep", lambda _s: None)
    client = FakeClient([{"value": [table("Orders", "NotRun")]}])

    result = refresh(client, attempts=3)

    assert client.calls == 3
    assert len(sync_failures(result)) == 1


def test_the_last_result_is_the_one_reported(monkeypatch):
    monkeypatch.setattr("fabshuffle.fabric.data_stores.time.sleep", lambda _s: None)
    client = FakeClient(
        [
            {"value": [table("a", "Failure", {"errorCode": "X", "message": "first"})]},
            {"value": [table("a", "Failure", {"errorCode": "Y", "message": "second"})]},
        ]
    )

    assert "second" in sync_failures(refresh(client, attempts=2))[0]


def test_progress_says_it_is_trying_again(monkeypatch):
    monkeypatch.setattr("fabshuffle.fabric.data_stores.time.sleep", lambda _s: None)
    seen = []
    client = FakeClient([{"value": [table("Orders", "NotRun")]}])

    refresh(client, attempts=2, on_progress=seen.append)

    assert "refreshing again (attempt 1 of 2)" in seen[0]


def test_an_endpoint_that_refuses_the_call_is_not_retried_to_death(monkeypatch):
    """A 404 is an answer, not flakiness."""
    from fabshuffle.fabric.client import FabricApiError

    monkeypatch.setattr("fabshuffle.fabric.data_stores.time.sleep", lambda _s: None)

    class Refusing:
        calls = 0

        def post(self, path, json=None, params=None, wait=True):
            Refusing.calls += 1
            raise FabricApiError("POST", path, 404, "{}")

    result = refresh(Refusing())

    assert result["status"] == "Skipped"
    assert Refusing.calls == 1
    assert sync_failures(result) == []
