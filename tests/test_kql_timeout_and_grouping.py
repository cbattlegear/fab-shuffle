from __future__ import annotations

from datetime import timedelta

import pytest
from azure.kusto.data import ClientRequestProperties
from azure.kusto.data.client_base import ExecuteRequestParams
from azure.kusto.data.client_details import ClientDetails

from fabshuffle.fabric.support import Strategy, assess_workspace
from fabshuffle.transfer import kql


def build_params(timeout_option) -> ExecuteRequestParams:
    """Exercise the SDK path that does `(timeout or default) + client_server_delta`."""
    properties = ClientRequestProperties()
    properties.set_option(ClientRequestProperties.request_timeout_option_name, timeout_option)

    return ExecuteRequestParams(
        payload=None,
        json_payload={},
        request_headers={},
        client_request_id_prefix="test;",
        properties=properties,
        timeout=timedelta(minutes=4),
        mgmt_default_timeout=timedelta(hours=1),
        client_server_delta=timedelta(seconds=30),
        client_details=ClientDetails(None, None),
    )


def test_server_timeout_must_be_a_timedelta():
    """The SDK adds a client/server delta to this option, so a string raises TypeError."""
    assert isinstance(kql.INGEST_TIMEOUT, timedelta)


def test_kusto_accepts_our_timeout():
    params = build_params(kql.INGEST_TIMEOUT)
    assert params.timeout == kql.INGEST_TIMEOUT + timedelta(seconds=30)


def test_a_string_timeout_would_have_failed():
    """Guards the regression: passing "01:00:00" is exactly what the previous code did."""
    with pytest.raises(TypeError, match="timedelta"):
        build_params("01:00:00")


def test_unsupported_items_are_grouped_by_type():
    items = [
        {"displayName": "bronze", "type": "Lakehouse"},
        {"displayName": "AlertAgain", "type": "Dashboard"},
        {"displayName": "AlertOnce", "type": "Dashboard"},
        {"displayName": "BattleCabbageReplTest", "type": "MirroredDatabase"},
    ]
    assessment = assess_workspace(items)
    assert assessment.strategy is Strategy.REBUILD

    messages = assessment.grouped_messages()

    # One line per type, not one per item.
    assert len(messages) == 2
    assert messages[0].startswith("Dashboard (2) not migrated: 'AlertAgain', 'AlertOnce'.")
    assert messages[1].startswith("MirroredDatabase (1) not migrated: 'BattleCabbageReplTest'.")
    assert all(message.endswith(".") for message in messages)


def test_grouped_messages_are_empty_when_everything_is_supported():
    assessment = assess_workspace([{"displayName": "bronze", "type": "Lakehouse"}])
    assert assessment.grouped_messages() == []
