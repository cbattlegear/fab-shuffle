"""Workspace access must not depend on the migration reaching its final phase.

A workspace this service principal creates is invisible to everyone else until its
permissions are copied, so a run that fails earlier would strand it: nobody could open or
delete it from the portal.
"""

from __future__ import annotations

import pytest

from fabshuffle import orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric import workspaces
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.support import Strategy
from fabshuffle.run import MigrationRun

PRINCIPAL = ServicePrincipal("tenant", "client", "secret")

ADMIN = {"principal": {"id": "user-1", "type": "User", "displayName": "Cam"}, "role": "Admin"}
MEMBER = {"principal": {"id": "user-2", "type": "User", "displayName": "Sam"}, "role": "Member"}
VIEWER = {"principal": {"id": "grp-1", "type": "Group", "displayName": "Analysts"}, "role": "Viewer"}


class RoleClient:
    def __init__(self, *, conflict_ids: set[str] | None = None, fail_ids: set[str] | None = None) -> None:
        self.granted: list[tuple[str, str, str]] = []
        self.conflict_ids = conflict_ids or set()
        self.fail_ids = fail_ids or set()

    def post(self, path, json=None, params=None, wait=True):
        workspace_id = path.split("/")[1]
        principal_id = json["principal"]["id"]
        if principal_id in self.conflict_ids:
            raise FabricApiError("POST", path, 409, "already exists")
        if principal_id in self.fail_ids:
            raise FabricApiError("POST", path, 400, "bad principal")
        self.granted.append((workspace_id, principal_id, json["role"]))
        return {}


def test_only_the_requested_roles_are_copied():
    client = RoleClient()
    warnings = workspaces.copy_role_assignments(
        client, [ADMIN, MEMBER, VIEWER], "ws-target", roles={"Admin"}
    )

    assert warnings == []
    assert client.granted == [("ws-target", "user-1", "Admin")]


def test_an_existing_assignment_is_not_reported_as_a_problem():
    # Admins are granted up front and again in the final pass, so 409 is expected.
    client = RoleClient(conflict_ids={"user-1"})
    warnings = workspaces.copy_role_assignments(client, [ADMIN, MEMBER], "ws-target")

    assert warnings == []
    assert client.granted == [("ws-target", "user-2", "Member")]


def test_a_failed_grant_is_reported_but_does_not_stop_the_rest():
    client = RoleClient(fail_ids={"user-1"})
    warnings = workspaces.copy_role_assignments(client, [ADMIN, MEMBER], "ws-target")

    assert len(warnings) == 1 and "Cam" in warnings[0]
    assert client.granted == [("ws-target", "user-2", "Member")]


# ------------------------------------------------------- orchestrator wiring


class FakeFabric:
    """Fails immediately after the workspace phase, like a run that dies early."""

    def __init__(self) -> None:
        self.created_workspaces: list[dict] = []
        self.granted: list[tuple[str, str, str]] = []

    def __call__(self, _tokens):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return None

    def post(self, path, json=None, params=None, wait=True):
        if path == "workspaces":
            self.created_workspaces.append(json)
            index = len(self.created_workspaces)
            return {"id": "ws-target" if index == 1 else "ws-scratch"}
        if path.endswith("/roleAssignments"):
            self.granted.append((path.split("/")[1], json["principal"]["id"], json["role"]))
            return {}
        if path.endswith("/lakehouses"):
            return {"id": "lh-hold"}
        raise AssertionError(f"unexpected POST {path}")

    def list_all(self, path, params=None, value_key="value"):
        if path.endswith("/roleAssignments"):
            return [ADMIN, MEMBER, VIEWER]
        if path.endswith("/items"):
            return []
        if path.endswith("/eventhouses"):
            raise FabricApiError("GET", path, 500, "boom")  # abort right after the workspace phase
        return []


@pytest.fixture
def fabric(monkeypatch) -> FakeFabric:
    fake = FakeFabric()
    monkeypatch.setattr(orchestrator, "FabricClient", fake)
    monkeypatch.setattr(orchestrator, "TokenProvider", lambda principal: object())
    monkeypatch.setattr(orchestrator.workspaces, "clone_folder_tree", lambda c, s, t: {})
    return fake


def test_admins_are_granted_before_any_content_is_migrated(fabric):
    run = MigrationRun(source_workspace_name="MirrorGeoTest", capacity_name="F64")
    plan = orchestrator.MigrationPlan(
        capacity_id="cap-1",
        capacity_name="F64",
        capacity_region="westus",
        source_workspace_id="ws-source",
        source_workspace_name="MirrorGeoTest",
        target_workspace_name="MirrorGeoTest-westus",
        strategy=Strategy.REBUILD,
    )
    orchestrator.run_migration(run, PRINCIPAL, plan, cleanup=False)

    # The run failed, but the workspaces it made are still reachable by a human.
    assert run.status.value == "failed"
    assert ("ws-target", "user-1", "Admin") in fabric.granted
    assert ("ws-scratch", "user-1", "Admin") in fabric.granted

    # Only admins at this point; everyone else waits for the permissions phase.
    assert all(role == "Admin" for _, _, role in fabric.granted)


def test_created_workspaces_say_where_they_came_from(fabric):
    run = MigrationRun(source_workspace_name="MirrorGeoTest", capacity_name="F64")
    plan = orchestrator.MigrationPlan(
        capacity_id="cap-1",
        capacity_name="F64",
        capacity_region="westus",
        source_workspace_id="ws-source",
        source_workspace_name="MirrorGeoTest",
        target_workspace_name="MirrorGeoTest-westus",
        strategy=Strategy.REBUILD,
    )
    orchestrator.run_migration(run, PRINCIPAL, plan, cleanup=False)

    target, scratch = fabric.created_workspaces
    assert "Fab Shuffle" in target["description"] and "MirrorGeoTest" in target["description"]
    assert "Safe to delete" in scratch["description"]
