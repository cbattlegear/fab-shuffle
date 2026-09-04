"""One item failing must not end the migration.

Creating an item can fail two ways. The request can be rejected, raising FabricApiError, or
it can be accepted and the long running operation behind it fail later, raising
OperationFailed. Those were unrelated exception types, and the per-item handler only caught
the first, so an eventstream whose source connection the service principal could not reach
took the whole run down with it -- along with every phase that had not happened yet.
"""

from __future__ import annotations

import pytest

from fabshuffle.fabric import analytics
from fabshuffle.fabric.client import (
    FabricApiError,
    FabricError,
    OperationFailed,
    OperationTimeout,
)

DATA_SOURCES_ERROR = {
    "errorCode": "DataSourcesValidationError",
    "message": (
        "Validation errors found for some nodes: new-source: The cloud connection used by "
        "the source is missing or inaccessible."
    ),
    "isRetriable": False,
}


def test_every_fabric_failure_shares_a_base() -> None:
    """So a caller handling one item at a time can catch the lot."""
    assert issubclass(FabricApiError, FabricError)
    assert issubclass(OperationFailed, FabricError)
    assert issubclass(OperationTimeout, FabricError)


def test_operation_failed_keeps_the_error_code_and_message() -> None:
    error = OperationFailed("op-1", "Failed", DATA_SOURCES_ERROR)

    assert error.error_code == "DataSourcesValidationError"
    assert "cloud connection" in error.detail
    assert error.operation_id == "op-1"
    assert error.status == "Failed"


def test_operation_failed_survives_an_absent_error_body() -> None:
    error = OperationFailed("op-1", "Cancelled", None)

    assert error.error_code == ""
    assert error.detail == ""
    assert "ended as Cancelled" in str(error)


def test_the_connection_failure_says_what_to_actually_do() -> None:
    message = analytics.describe_failure(
        "Eventstream", "orders", OperationFailed("op-1", "Failed", DATA_SOURCES_ERROR)
    )

    assert "connection this service principal cannot reach" in message
    assert "Manage Connections and Gateways" in message
    assert "tenant wide" in message


def test_another_operation_failure_repeats_the_service_message() -> None:
    error = OperationFailed("op-1", "Failed", {"errorCode": "Whatever", "message": "Too big"})

    assert "Too big" in analytics.describe_failure("Eventstream", "orders", error)


def test_a_timeout_says_the_item_might_still_arrive() -> None:
    message = analytics.describe_failure("Eventstream", "orders", OperationTimeout("slow"))

    assert "still being created" in message
    assert "in case it arrived late" in message


def test_a_rejected_request_still_reports_its_status() -> None:
    error = FabricApiError("POST", "/items", 400, "bad")

    assert "HTTP 400" in analytics.describe_failure("Notebook", "nb", error)


class Client:
    """Fails the first item, succeeds the second."""

    def __init__(self, error: Exception) -> None:
        self.error = error
        self.created: list[str] = []

    def post(self, path, json=None, params=None, wait=True):
        if path.endswith("/getDefinition"):
            return {"definition": {"parts": []}}
        name = (json or {}).get("displayName", "")
        if name == "broken":
            raise self.error
        self.created.append(name)
        return {"id": f"new-{name}"}


ITEMS = [
    {"id": "a", "displayName": "broken", "type": "Eventstream"},
    {"id": "b", "displayName": "healthy", "type": "Eventstream"},
]


@pytest.mark.parametrize(
    "error",
    [
        OperationFailed("op-1", "Failed", DATA_SOURCES_ERROR),
        OperationTimeout("took too long"),
        FabricApiError("POST", "/items", 400, "bad"),
    ],
)
def test_a_failing_item_does_not_stop_the_batch(error: Exception) -> None:
    client = Client(error)
    id_map: dict[str, str] = {}

    migrated, warnings = analytics.migrate_items(
        client,
        source_workspace_id="ws-source",
        target_workspace_id="ws-target",
        items=ITEMS,
        item_type="Eventstream",
        id_map=id_map,
    )

    # The healthy item after it still migrated, and the failure is a warning not a crash.
    assert client.created == ["healthy"]
    assert [item.name for item in migrated] == ["healthy"]
    assert len(warnings) == 1
    assert "broken" in warnings[0]
    assert id_map["b"] == "new-healthy"


def test_the_real_eventstream_failure_is_reported_not_raised() -> None:
    """Reproduces the run that died in the realtime phase."""
    client = Client(OperationFailed("cfd70a20", "Failed", DATA_SOURCES_ERROR))

    _, warnings = analytics.migrate_items(
        client,
        source_workspace_id="ws-source",
        target_workspace_id="ws-target",
        items=[ITEMS[0]],
        item_type="Eventstream",
        id_map={},
    )

    assert len(warnings) == 1
    assert "Manage Connections and Gateways" in warnings[0]
