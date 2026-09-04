"""A workspace left behind when its capacity was deleted.

``GET /workspaces/{id}`` drops ``capacityId``, ``capacityRegion`` and ``oneLakeEndpoints``
once the capacity behind an assignment has gone, while still reporting the assignment as
``Completed``. That combination is the only signal Fabric gives, and it is what tells the
reassign path why ``assignToCapacity`` was refused.
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

STRANDED = {
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


def test_a_workspace_on_a_live_capacity_is_not_stranded():
    assert workspaces.stranded_on_deleted_capacity(LIVE) is False


def test_a_completed_assignment_naming_no_capacity_is_stranded():
    assert workspaces.stranded_on_deleted_capacity(STRANDED) is True


def test_a_workspace_that_was_never_assigned_is_not_stranded():
    assert workspaces.stranded_on_deleted_capacity(NEVER_ASSIGNED) is False


def test_a_workspace_with_no_assignment_field_at_all_is_not_stranded():
    assert workspaces.stranded_on_deleted_capacity({"id": "x", "displayName": "y"}) is False
