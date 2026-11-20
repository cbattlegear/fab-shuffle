import pytest

from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric.client import FabricApiError, FabricError
from fabshuffle.fabric.sql_connections import ensure_connection
from tests.test_sql_database_migration import SUPPORTED_SQL


def test_connection_error_surfaces_service_code_without_secret():
    class Client:
        def list_all(self, path):
            return [SUPPORTED_SQL] if path.endswith("supportedConnectionTypes") else []

        def post(self, path, **kwargs):
            raise FabricApiError(
                "POST", path, 400, '{"errorCode":"InvalidCredential","message":"secret-value"}'
            )

    recorded = []
    with pytest.raises(FabricError, match=r"InvalidCredential: \[redacted\]") as caught:
        ensure_connection(
            Client(), ServicePrincipal("tenant", "client", "secret-value"),
            name="owned", server="server", database="db", recorded_id=None,
            record=recorded.append,
        )
    assert caught.value.__context__ is None
    assert recorded == ["", None]


def test_uncertain_create_recovers_only_a_unique_matching_owned_connection():
    class Client:
        def __init__(self):
            self.matches = []

        def list_all(self, path):
            assert path == "connections"
            return self.matches

    client = Client()
    recorded = []
    args = {
        "name": "owned", "server": "server", "database": "db", "recorded_id": "",
        "record": recorded.append,
    }
    principal = ServicePrincipal("tenant", "client", "secret-value")
    with pytest.raises(FabricError, match="will not create a duplicate"):
        ensure_connection(client, principal, **args)
    cid = "00000000-0000-0000-0000-000000000001"
    connection = {
        "id": cid, "displayName": "owned", "connectivityType": "ShareableCloud",
        "myRole": "User", "credentialDetails": {"credentialType": "ServicePrincipal"},
        "connectionDetails": {"type": "SQL", "path": "server;db"},
    }
    client.matches = [connection]
    with pytest.raises(FabricError, match="does not match"):
        ensure_connection(client, principal, **args)
    assert not recorded
    connection["myRole"] = "Owner"
    assert ensure_connection(client, principal, **args) == cid
    assert recorded == [cid]
