"""Sharing a replacement connection, and telling the operator what to share.

Two things a real run needed:

* five items were refused because they bind connections the service principal cannot see. The
  API can grant that access, but only to a caller who already holds Owner on the connection,
  which is exactly what is missing -- so the review screen has to explain the portal steps
  instead of doing it.
* a replacement connection is a brand new connection, so it starts out visible only to
  whoever created it. Everyone who could use the original has to be added again, which is the
  manual work the replacement existed to avoid.
"""

from __future__ import annotations

from fabshuffle import orchestrator
from fabshuffle.fabric import connections
from fabshuffle.fabric.client import FabricApiError

SPN = "b6c172e2-9209-4337-a91b-fa0849bbde5b"

ASSIGNMENTS = [
    {
        "id": "1",
        "role": "Owner",
        "principal": {"id": "user-1", "type": "User", "displayName": "Ada"},
    },
    {
        "id": "2",
        "role": "User",
        "principal": {"id": "group-1", "type": "Group", "displayName": "Analysts"},
    },
    {
        "id": "3",
        "role": "Owner",
        "principal": {
            "id": "spn-object-id",
            "type": "ServicePrincipal",
            "servicePrincipalDetails": {"aadAppId": SPN},
        },
    },
]


class Client:
    def __init__(self, assignments=ASSIGNMENTS, *, fail_for: set[str] | None = None) -> None:
        self.assignments = assignments
        self.fail_for = fail_for or set()
        self.granted: list[tuple[str, str, str]] = []

    def list_all(self, path, params=None, value_key="value"):
        if self.assignments is None:
            raise FabricApiError("GET", path, 403, "forbidden")
        return self.assignments

    def post(self, path, json=None, params=None, wait=True):
        connection_id = path.split("/")[1]
        principal = json["principal"]
        if principal["id"] in self.fail_for:
            raise FabricApiError("POST", path, 403, "forbidden")
        self.granted.append((connection_id, principal["id"], json["role"]))
        return {"id": principal["id"]}


def test_the_original_users_are_given_the_replacement() -> None:
    client = Client()

    copied, warnings = connections.copy_role_assignments(
        client, source_connection_id="old", target_connection_id="new", client_id=SPN
    )

    assert copied == 2
    assert warnings == []
    assert ("new", "user-1", "Owner") in client.granted
    assert ("new", "group-1", "User") in client.granted


def test_our_own_assignment_is_not_replayed() -> None:
    # Creating the connection already made this service principal its owner.
    client = Client()

    connections.copy_role_assignments(
        client, source_connection_id="old", target_connection_id="new", client_id=SPN
    )

    assert not any(principal == "spn-object-id" for _, principal, _ in client.granted)


def test_a_principal_that_cannot_be_added_is_named() -> None:
    client = Client(fail_for={"group-1"})

    copied, warnings = connections.copy_role_assignments(
        client, source_connection_id="old", target_connection_id="new", client_id=SPN
    )

    assert copied == 1
    assert "Analysts" in warnings[0]
    assert "by hand" in warnings[0]


def test_unreadable_assignments_are_reported_not_silently_dropped() -> None:
    """Otherwise the replacement is quietly private to the service principal."""
    client = Client(assignments=None)

    copied, warnings = connections.copy_role_assignments(
        client, source_connection_id="old", target_connection_id="new", client_id=SPN
    )

    assert copied == 0
    assert "could not be read" in warnings[0]
    assert "Share it with whoever used the original" in warnings[0]


def test_an_assignment_missing_its_principal_is_skipped() -> None:
    client = Client([{"id": "1", "role": "User", "principal": {}}])

    copied, warnings = connections.copy_role_assignments(
        client, source_connection_id="old", target_connection_id="new", client_id=SPN
    )

    assert copied == 0
    assert warnings == []


# --------------------------------------------------------------- portal steps


def test_the_instructions_ask_for_user_not_owner() -> None:
    """User is enough to bind a connection, so asking for Owner would over-grant."""
    steps = " ".join(orchestrator.portal_instructions(SPN))

    assert "User role" in steps
    assert "Owner is not needed" in steps
    assert SPN in steps


def test_the_instructions_end_by_pointing_at_the_recheck() -> None:
    assert "Re-check" in orchestrator.portal_instructions(SPN)[-1]


def test_a_blocked_connection_names_what_needs_it() -> None:
    entry = orchestrator.ConnectionAccess(
        connection_id="c-1", used_by=("CopyJob 'copyjob1'", "DataPipeline 'p'")
    )

    assert entry.as_dict() == {
        "connectionId": "c-1",
        "connectionName": "",
        "label": "c-1",
        "usedBy": ["CopyJob 'copyjob1'", "DataPipeline 'p'"],
    }
    assert "copyjob1" in entry.message()
    assert "cannot see it" in entry.message()


def test_a_named_connection_shows_its_name_rather_than_its_id() -> None:
    entry = orchestrator.ConnectionAccess(
        connection_id="c-2", connection_name="Sales SQL", used_by=("CopyJob 'j'",)
    )

    assert entry.label == "Sales SQL"
    assert "Sales SQL" in entry.message()


def test_connections_pointing_inward_are_a_count_not_a_list_each() -> None:
    """They are the customer's job to repoint, so one line is enough."""
    prerequisites = [
        connections.ConnectionPrerequisite(
            connection_id="a",
            connection_name="Warehouse conn",
            path="p",
            matched="m",
            credential_type="unknown",
            connectivity_type="ShareableCloud",
            manageable=False,
        ),
        connections.ConnectionPrerequisite(
            connection_id="b",
            connection_name="",
            path="p2",
            matched="m2",
            credential_type="unknown",
            connectivity_type="ShareableCloud",
            manageable=False,
        ),
    ]

    summary = orchestrator.inward_connection_summary(prerequisites)

    assert summary.startswith("2 connection(s) point at items in this workspace")
    assert "Warehouse conn" in summary
    assert "b" in summary


def test_access_problems_are_not_repeated_in_the_dependency_warnings() -> None:
    """They have their own section; listing them twice showed the same lines under two heads."""
    report = orchestrator.DependencyReport(
        graph=None,
        issues=[],
        prerequisites=[],
        access=[orchestrator.ConnectionAccess(connection_id="c-1", used_by=("CopyJob 'j'",))],
    )

    assert report.messages() == []
