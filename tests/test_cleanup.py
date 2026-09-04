"""Cleanup must not try to delete items individually.

A scratch workspace contains a lakehouse, and Fabric derives a SQL analytics endpoint from
it. That endpoint rejects a direct delete with OperationNotSupportedForItem, which used to
abort cleanup and strand the workspace. Deleting the workspace removes its items anyway.
"""

from __future__ import annotations

from fabshuffle import orchestrator
from fabshuffle.fabric import workspaces
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.run import MigrationRun, StepStatus

SCRATCH_ID = "5beba7d5-6064-49a7-b27b-4a7e71484761"


class FakeClient:
    def __init__(self, *, fail_workspace_delete: bool = False) -> None:
        self.fail_workspace_delete = fail_workspace_delete
        self.deleted: list[str] = []

    def delete(self, path, params=None):
        if "/items/" in path:
            # Deleting a derived item such as a SQL analytics endpoint is never supported.
            raise FabricApiError("DELETE", path, 400, '{"errorCode":"OperationNotSupportedForItem"}')
        if self.fail_workspace_delete:
            raise FabricApiError("DELETE", path, 403, "forbidden")
        self.deleted.append(path)

    def list_all(self, path, params=None, value_key="value"):
        raise AssertionError("cleanup must not enumerate items")


def make_run() -> MigrationRun:
    run = MigrationRun(source_workspace_name="Sales", capacity_name="F64")
    run.scratch_workspace = {"id": SCRATCH_ID, "displayName": "fab-shuffle-scratch-abc123"}
    return run


def test_cleanup_deletes_the_workspace_without_touching_items(tmp_path):
    client = FakeClient()
    run = make_run()

    warnings = orchestrator.cleanup_run(run, client, tmp_path)

    assert warnings == []
    assert client.deleted == [f"workspaces/{SCRATCH_ID}"]
    assert run.scratch_workspace is None
    assert run.cleanup_done is True

    step = run.snapshot()["steps"][0]
    assert step["id"] == "cleanup"
    assert step["status"] == StepStatus.SUCCEEDED


def test_cleanup_reports_a_failed_workspace_delete_without_raising(tmp_path):
    run = make_run()
    warnings = orchestrator.cleanup_run(run, FakeClient(fail_workspace_delete=True), tmp_path)

    assert len(warnings) == 1 and SCRATCH_ID in warnings[0]
    # The workspace is still tracked so the operator can retry.
    assert run.scratch_workspace is not None
    assert run.cleanup_done is False
    assert run.snapshot()["steps"][0]["status"] == StepStatus.FAILED


def test_cleanup_removes_local_staging(tmp_path):
    staging = tmp_path / "run-scratch"
    staging.mkdir()
    (staging / "dump.dacpac").write_text("x")

    run = make_run()
    orchestrator.cleanup_run(run, FakeClient(), staging)

    assert not staging.exists()


# --------------------------------------------------------- leftover discovery


class WorkspaceListClient:
    def __init__(self, names: list[str], *, undeletable: set[str] | None = None) -> None:
        self.names = names
        self.undeletable = undeletable or set()
        self.deleted: list[str] = []

    def list_all(self, path, params=None, value_key="value"):
        return [{"id": f"id-{name}", "displayName": name} for name in self.names]

    def delete(self, path, params=None):
        workspace_id = path.rsplit("/", 1)[-1]
        if workspace_id in self.undeletable:
            raise FabricApiError("DELETE", path, 403, "forbidden")
        self.deleted.append(workspace_id)


def test_only_fab_shuffle_scratch_workspaces_are_listed():
    client = WorkspaceListClient(
        ["Sales", "fab-shuffle-scratch-abc123", "fab-shuffle-scratch-def456", "Marketing"]
    )
    found = workspaces.list_scratch_workspaces(client)

    assert [w["displayName"] for w in found] == [
        "fab-shuffle-scratch-abc123",
        "fab-shuffle-scratch-def456",
    ]


def test_scratch_cleanup_reports_partial_failures():
    client = WorkspaceListClient(
        ["fab-shuffle-scratch-aaa", "fab-shuffle-scratch-bbb"],
        undeletable={"id-fab-shuffle-scratch-bbb"},
    )
    deleted, warnings = workspaces.delete_scratch_workspaces(client)

    assert deleted == 1
    assert len(warnings) == 1 and "fab-shuffle-scratch-bbb" in warnings[0]


def test_generated_scratch_names_are_discoverable():
    name = workspaces.scratch_workspace_name()
    assert name.startswith(workspaces.SCRATCH_WORKSPACE_PREFIX)
    assert workspaces.scratch_workspace_name() != name
