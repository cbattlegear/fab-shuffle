"""Connections that point into the workspace being migrated.

Fabric does not allow a connection's target to change: no Update Connection request variant
accepts `connectionDetails`. So a connection aimed at a lakehouse, warehouse, or KQL database
in the source workspace cannot be repointed at the migrated copy, and has to be replaced.
That is surfaced before anything is created.
"""

from __future__ import annotations

from fabshuffle.fabric import connections
from fabshuffle.fabric.client import FabricApiError

SPN_APP_ID = "99999999-9999-9999-9999-999999999999"
SOURCE_WS = "aaaaaaaa-0000-0000-0000-000000000000"
LAKEHOUSE_ID = "bbbbbbbb-0000-0000-0000-000000000000"
ENDPOINT = "abc123.datawarehouse.fabric.microsoft.com"

ITEMS = [
    {"id": LAKEHOUSE_ID, "type": "Lakehouse", "displayName": "bronze"},
    {"id": "cccccccc-0000-0000-0000-000000000000", "type": "Report", "displayName": "Sales"},
]

OWNER_ASSIGNMENT = {
    "role": "Owner",
    "principal": {"type": "ServicePrincipal", "servicePrincipalDetails": {"aadAppId": SPN_APP_ID}},
}
USER_ASSIGNMENT = {
    "role": "User",
    "principal": {"type": "ServicePrincipal", "servicePrincipalDetails": {"aadAppId": SPN_APP_ID}},
}


def connection(path: str, **overrides):
    base = {
        "id": "conn-1",
        "displayName": "Bronze SQL",
        "connectivityType": "ShareableCloud",
        "connectionDetails": {"type": "SQL", "path": path},
        "credentialDetails": {"credentialType": "ServicePrincipal"},
    }
    base.update(overrides)
    return base


class FakeClient:
    def __init__(self, assignments=None, *, forbidden: bool = False) -> None:
        self.assignments = assignments if assignments is not None else []
        self.forbidden = forbidden

    def list_all(self, path, params=None, value_key="value"):
        if path.endswith("/roleAssignments"):
            if self.forbidden:
                raise FabricApiError("GET", path, 403, "forbidden")
            return self.assignments
        return []


# ------------------------------------------------------------- identifiers


def test_identifiers_cover_the_workspace_items_and_endpoints():
    identifiers = connections.source_identifiers(SOURCE_WS, ITEMS, [ENDPOINT])

    assert identifiers[SOURCE_WS] == "this workspace"
    assert identifiers[LAKEHOUSE_ID] == "Lakehouse 'bronze'"
    assert identifiers[ENDPOINT] == ENDPOINT


# ---------------------------------------------------------------- scanning


def scan(client, candidates):
    return connections.scan_prerequisites(
        client,
        identifiers=connections.source_identifiers(SOURCE_WS, ITEMS, [ENDPOINT]),
        client_id=SPN_APP_ID,
        known=candidates,
    )


def test_a_connection_pointing_at_a_workspace_item_is_reported():
    found = scan(FakeClient([OWNER_ASSIGNMENT]), [connection(f"{ENDPOINT};bronze")])

    assert len(found) == 1
    assert found[0].matched == ENDPOINT
    assert found[0].manageable is True
    assert "does not allow a connection's target to be changed" in found[0].message()


def test_a_connection_matching_an_item_guid_is_reported():
    found = scan(FakeClient([OWNER_ASSIGNMENT]), [connection(f"https://onelake/{LAKEHOUSE_ID}/Tables")])
    assert found[0].matched == "Lakehouse 'bronze'"


def test_matching_ignores_case():
    found = scan(FakeClient([OWNER_ASSIGNMENT]), [connection(ENDPOINT.upper())])
    assert len(found) == 1


def test_unrelated_connections_are_left_alone():
    assert scan(FakeClient(), [connection("contoso.database.windows.net;sales")]) == []


def test_connections_with_no_path_are_skipped():
    assert scan(FakeClient(), [connection("")]) == []


def test_missing_owner_role_is_called_out_with_what_to_do():
    found = scan(FakeClient([USER_ASSIGNMENT]), [connection(ENDPOINT)])

    assert found[0].manageable is False
    assert "no Owner role" in found[0].message()


def test_unreadable_role_assignments_count_as_not_manageable():
    found = scan(FakeClient(forbidden=True), [connection(ENDPOINT)])
    assert found[0].manageable is False


def test_the_replacement_message_names_the_credential_type():
    found = scan(
        FakeClient([OWNER_ASSIGNMENT]),
        [connection(ENDPOINT, credentialDetails={"credentialType": "WorkspaceIdentity"})],
    )
    assert "WorkspaceIdentity connection" in found[0].message()


# ------------------------------------------------------------------- roles


def test_owner_is_detected_by_app_id():
    assert connections.is_owned_by([OWNER_ASSIGNMENT], SPN_APP_ID) is True


def test_lesser_roles_are_not_ownership():
    # User and UserWithReshare can use a connection but not manage it.
    assert connections.is_owned_by([USER_ASSIGNMENT], SPN_APP_ID) is False


def test_another_principals_ownership_does_not_count():
    other = {
        "role": "Owner",
        "principal": {"type": "ServicePrincipal", "servicePrincipalDetails": {"aadAppId": "other"}},
    }
    assert connections.is_owned_by([other], SPN_APP_ID) is False


def test_no_assignments_is_not_ownership():
    assert connections.is_owned_by(None, SPN_APP_ID) is False
    assert connections.is_owned_by([], SPN_APP_ID) is False
