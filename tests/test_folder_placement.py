"""Items must land in the folder they came from.

The folder tree was recreated correctly and its source-to-target ids recorded, but the
eventhouse and KQL database creates never passed folderId, so those items landed in the
workspace root while everything else was placed properly. Silently, because a workspace with
folders still ends up with all its folders.
"""

from __future__ import annotations

import contextlib

from fabshuffle.fabric import eventhouses
from fabshuffle.fabric.client import FabricApiError


class Client:
    def __init__(self, *, reject_folder: bool = False) -> None:
        self.reject_folder = reject_folder
        self.posted: list[dict] = []

    def post(self, path, json=None, params=None, wait=True):
        self.posted.append(json)
        if self.reject_folder and "folderId" in json:
            raise FabricApiError("POST", path, 400, '{"errorCode":"InvalidRequest"}')
        return {"id": "new-1"}


def test_an_eventhouse_is_created_in_its_folder() -> None:
    client = Client()

    eventhouses.create_eventhouse(client, "ws", "Telemetry", folder_id="folder-target")

    assert client.posted[0]["folderId"] == "folder-target"


def test_an_eventhouse_with_no_folder_sends_none() -> None:
    client = Client()

    eventhouses.create_eventhouse(client, "ws", "Telemetry")

    assert "folderId" not in client.posted[0]


def test_a_kql_database_is_created_in_its_folder() -> None:
    client = Client()

    eventhouses.create_kql_database(
        client, "ws", "Events", creation_payload={"databaseType": "ReadWrite"}, folder_id="f-1"
    )

    assert client.posted[0]["folderId"] == "f-1"


def test_a_rejected_folder_costs_the_folder_not_the_database() -> None:
    """A KQL database belongs to its eventhouse, so Fabric may refuse to place it."""
    client = Client(reject_folder=True)

    result = eventhouses.create_kql_database(
        client, "ws", "Events", creation_payload={"databaseType": "ReadWrite"}, folder_id="f-1"
    )

    assert result == {"id": "new-1"}
    assert len(client.posted) == 2
    assert "folderId" not in client.posted[1]


def test_a_real_failure_is_not_swallowed_by_the_folder_retry() -> None:
    class AlwaysFails:
        def post(self, path, json=None, params=None, wait=True):
            raise FabricApiError("POST", path, 409, "ItemDisplayNameAlreadyInUse")

    try:
        eventhouses.create_kql_database(
            AlwaysFails(), "ws", "Events", creation_payload={}, folder_id="f-1"
        )
    except FabricApiError as error:
        assert error.status_code == 409
    else:  # pragma: no cover - the call must raise
        raise AssertionError("a 409 must not be retried away")


def test_the_retry_only_happens_when_a_folder_was_asked_for() -> None:
    class AlwaysFails:
        def __init__(self) -> None:
            self.calls = 0

        def post(self, path, json=None, params=None, wait=True):
            self.calls += 1
            raise FabricApiError("POST", path, 400, "bad")

    client = AlwaysFails()
    with contextlib.suppress(FabricApiError):
        eventhouses.create_kql_database(client, "ws", "Events", creation_payload={})

    assert client.calls == 1
