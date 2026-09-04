"""Default semantic models, and references to workspaces that are gone.

Every lakehouse and warehouse comes with a semantic model named after it, created by Fabric.
We do not recreate those — the target has its own — but it is a different item with a
different id, and reports bind to it by id.

Separately, an item naming a workspace that no longer exists cannot be created, and Fabric
says only "UnknownError" about it.
"""

from __future__ import annotations

import json

from fabshuffle import orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric.analytics import (
    describe_failure,
    referenced_workspaces,
    unreachable_workspaces,
)
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import part
from fabshuffle.run import MigrationRun

SOURCE_WS = "ws-source"
TARGET_WS = "ws-target"
GONE_WS = "8d10604d-6100-4238-83a0-d54616c246fe"

SOURCE_DEFAULT = "sm-source-default"
TARGET_DEFAULT = "sm-target-default"


class FakeClient:
    def __init__(self, target_models=None, reachable=()) -> None:
        self.target_models = target_models if target_models is not None else [
            {"id": TARGET_DEFAULT, "displayName": "CloneTest", "type": "SemanticModel"}
        ]
        self.reachable = set(reachable)
        self.asked: list[str] = []

    def list_all(self, path, params=None, value_key="value"):
        if path == f"workspaces/{TARGET_WS}/items":
            return self.target_models
        return []

    def get(self, path, params=None):
        workspace_id = path.split("/")[-1]
        self.asked.append(workspace_id)
        if workspace_id in self.reachable:
            return {"id": workspace_id, "displayName": "Somewhere"}
        raise FabricApiError("GET", path, 404, '{"errorCode":"WorkspaceNotFound"}')


def make_ctx(client):
    plan = orchestrator.MigrationPlan(
        capacity_id="cap",
        capacity_name="F64",
        capacity_region="westus",
        source_workspace_id=SOURCE_WS,
        source_workspace_name="src",
        target_workspace_name="dst",
    )
    ctx = orchestrator._Context(
        client=client,
        tokens=object(),
        principal=ServicePrincipal("t", "c", "s"),
        plan=plan,
        run=MigrationRun(source_workspace_name="src", capacity_name="F64"),
        scratch_dir=None,
    )
    ctx.target_workspace_id = TARGET_WS
    ctx.run.start_step("analytics", "Migrating semantic models and reports")
    return ctx


SKIPPED = [{"id": SOURCE_DEFAULT, "displayName": "CloneTest", "type": "SemanticModel"}]


# ------------------------------------------------------- foreign workspaces


def copy_job(destination_workspace):
    content = {
        "properties": {
            "source": {"connectionSettings": {"typeProperties": {"workspaceId": SOURCE_WS}}},
            "destination": {
                "connectionSettings": {"typeProperties": {"workspaceId": destination_workspace}}
            },
        }
    }
    return [part("copyjob-content.json", json.dumps(content))]


def test_workspaces_are_read_from_the_keys_that_name_one():
    assert referenced_workspaces(copy_job(GONE_WS)) == {SOURCE_WS, GONE_WS}


def test_an_id_that_is_not_a_workspace_is_not_collected():
    parts = [part("content.json", json.dumps({"artifactId": "lh-1", "itemId": "lh-2"}))]

    assert referenced_workspaces(parts) == set()


def test_nested_references_are_found():
    parts = [part("content.json", json.dumps({"a": [{"b": {"workspaceId": GONE_WS}}]}))]

    assert referenced_workspaces(parts) == {GONE_WS}


def test_a_binary_part_is_skipped():
    assert referenced_workspaces([{"path": "sqldb.dacpac", "payload": "AAECAw=="}]) == set()


def test_a_workspace_we_cannot_read_is_reported():
    client = FakeClient()

    assert unreachable_workspaces(client, [GONE_WS]) == [GONE_WS]


def test_a_workspace_we_can_read_is_not():
    client = FakeClient(reachable=[GONE_WS])

    assert unreachable_workspaces(client, [GONE_WS]) == []


def test_the_failure_names_the_workspace_rather_than_the_unknown_error():
    error = FabricApiError(
        "POST", "url", 400, '{"errorCode":"UnknownError","message":"An error occurred"}'
    )
    message = describe_failure("CopyJob", "TestCopyCrossWorkspace", error, unreachable=[GONE_WS])

    assert GONE_WS in message
    assert "cannot read" in message
    assert "deleted or access was never granted" in message
    # Still repeated, in case our reading of it is wrong.
    assert "UnknownError" in message


def test_without_an_unreachable_workspace_the_message_is_unchanged():
    error = FabricApiError(
        "POST", "url", 400, '{"errorCode":"UnknownError","message":"An error occurred"}'
    )
    message = describe_failure("CopyJob", "Nightly", error)

    assert "refers to workspace" not in message
    assert "UnknownError An error occurred" in message
