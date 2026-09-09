"""A workspace whose capacity assignment names no capacity.

``GET /workspaces/{id}`` documents ``capacityId`` as part of ``WorkspaceInfo`` and
``capacityAssignmentProgress: "Completed"`` as "last capacity assignment operation was
completed successfully" (Microsoft Learn's Get Workspace reference), but it does not document
what a missing ``capacityId`` alongside a completed assignment means. A tenant whose trial
capacity had expired showed exactly this shape for three workspaces, and ``assignToCapacity``
on each answered ``400 AssignWorkspaceToCapacityFailed`` - a plausible explanation (a deleted
or expired-trial capacity), not a documented or otherwise confirmed one. This module only
tests the shape detection; the reassign path treats a match as a hint to offer alongside an
actual assignment failure, never as proof and never as a reason to refuse anything up front.
"""

from __future__ import annotations

from fabshuffle.fabric import workspaces

# Shape verified against a live tenant, September 2026.
LIVE = {
    "id": "97bdfaae",
    "displayName": "FabricPOCPortal",
    "type": "Workspace",
    "capacityId": "fe27c7e1-7fbf-4401-9813-ba8e3c9487e7",
    "capacityRegion": "Central US",
    "oneLakeEndpoints": {
        "blobEndpoint": "https://centralus-onelake.blob.fabric.microsoft.com",
        "dfsEndpoint": "https://centralus-onelake.dfs.fabric.microsoft.com",
    },
    "capacityAssignmentProgress": "Completed",
}

MISSING_CAPACITY = {
    "id": "e5437fd8",
    "displayName": "Acquisition Analytics Demo",
    "type": "Workspace",
    "capacityAssignmentProgress": "Completed",
}

NEVER_ASSIGNED = {
    "id": "0000",
    "displayName": "Fresh",
    "type": "Workspace",
    "capacityAssignmentProgress": "NotStarted",
}


def test_a_workspace_on_a_live_capacity_is_resolved():
    assert workspaces.unresolved_capacity_assignment(LIVE) is False


def test_a_completed_assignment_naming_no_capacity_is_unresolved():
    assert workspaces.unresolved_capacity_assignment(MISSING_CAPACITY) is True


def test_a_workspace_that_was_never_assigned_is_not_unresolved():
    assert workspaces.unresolved_capacity_assignment(NEVER_ASSIGNED) is False


def test_a_workspace_with_no_assignment_field_at_all_is_not_unresolved():
    assert workspaces.unresolved_capacity_assignment({"id": "x", "displayName": "y"}) is False
