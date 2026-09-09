"""Tenant-wide connection advisory scan: matching, redaction, and scan-state honesty.

The scan looks for a tenant-visible connection whose path still names the source workspace,
one of its items, or one of its data-store endpoints - reporting only, never a precondition
for anything the migration itself does. See ``fabshuffle.fabric.connection_advisory`` for the
policy this is testing against.
"""

from __future__ import annotations

import httpx
import pytest

from fabshuffle.fabric import connection_advisory as ca
from fabshuffle.fabric.client import FabricApiError, FabricTransportError
from fabshuffle.run import CancelledError

SOURCE_WS = "11111111-1111-1111-1111-111111111111"
TARGET_WS = "99999999-9999-9999-9999-999999999999"
LAKEHOUSE = "22222222-2222-2222-2222-222222222222"
TARGET_LAKEHOUSE = "88888888-8888-8888-8888-888888888888"
WAREHOUSE = "33333333-3333-3333-3333-333333333333"


class FakeClient:
    """A minimal Fabric client the scan can read from, with per-path failures."""

    def __init__(
        self, *, connections=None, items=None, lakehouses=None, warehouses=None,
        sql_databases=None, mirrored=None, eventhouses=None, cosmos=None, list_errors=None,
    ):
        self.connections = connections if connections is not None else []
        self.items = items if items is not None else []
        self.lakehouses = lakehouses or []
        self.warehouses = warehouses or []
        self.sql_databases = sql_databases or []
        self.mirrored = mirrored or []
        self.eventhouses = eventhouses or []
        self.cosmos = cosmos or []
        self.list_errors = list_errors or {}

    def list_all(self, path, params=None, value_key="value"):
        for suffix, error in self.list_errors.items():
            if path.endswith(suffix):
                raise error
        if path == "connections":
            return self.connections
        if path.endswith("/items"):
            return self.items
        if path.endswith("/lakehouses"):
            return self.lakehouses
        if path.endswith("/warehouses"):
            return self.warehouses
        if path.endswith("/sqlDatabases"):
            return self.sql_databases
        if path.endswith("/mirroredDatabases"):
            return self.mirrored
        if path.endswith("/eventhouses"):
            return self.eventhouses
        if path.endswith("/cosmosDbDatabases"):
            return self.cosmos
        return []


def connection(id_, path, *, connectivity="ShareableCloud", ctype="Web", name="My Connection"):
    return {
        "id": id_, "displayName": name, "connectivityType": connectivity,
        "connectionDetails": {"type": ctype, "path": path},
    }


# ------------------------------------------------------------------- redaction


def test_query_string_is_dropped():
    assert ca.redact_path("https://x.example.com/data?sig=abc123&api-version=1") == \
        "https://x.example.com/data"


def test_userinfo_is_dropped():
    assert ca.redact_path("https://user:s3cr3t@x.example.com/data") == "https://x.example.com/data"


def test_key_value_secrets_outside_a_query_string_are_still_redacted():
    # safe_text catches these even without a leading '?'.
    assert "[redacted]" in ca.redact_path("server;password=hunter2;catalog")


def test_empty_and_none_like_paths_pass_through():
    assert ca.redact_path("") == ""


# ------------------------------------------------------------------ matching


def test_a_connection_naming_the_source_workspace_guid_is_found():
    client = FakeClient(connections=[connection("conn-1", f"https://example.com/{SOURCE_WS}/thing")])
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert scan.scan_state == ca.SCAN_COMPLETE
    assert [c.connection_id for c in scan.connections] == ["conn-1"]
    assert scan.connections[0].matched_source_items == ()


def test_a_connection_naming_a_source_item_guid_is_attributed_to_that_item():
    client = FakeClient(
        items=[{"id": LAKEHOUSE, "displayName": "bronze", "type": "Lakehouse"}],
        connections=[connection("conn-1", f"https://example.com/items/{LAKEHOUSE}")],
    )
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert scan.connections[0].matched_source_items == (LAKEHOUSE,)


def test_a_connection_naming_a_complete_onelake_root_is_found():
    client = FakeClient(
        items=[{"id": LAKEHOUSE, "displayName": "bronze", "type": "Lakehouse"}],
        connections=[connection(
            "conn-1",
            f"https://onelake.dfs.fabric.microsoft.com/{SOURCE_WS}/{LAKEHOUSE}/Tables/sales",
            ctype="OneLake",
        )],
    )
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert scan.connections[0].matched_source_items == (LAKEHOUSE,)


def test_a_connection_naming_a_source_sql_endpoint_is_found():
    endpoint = "src.datawarehouse.fabric.microsoft.com"
    client = FakeClient(
        lakehouses=[{
            "id": LAKEHOUSE, "displayName": "bronze",
            "properties": {"sqlEndpointProperties": {"connectionString": endpoint}},
        }],
        connections=[connection("conn-1", f"{endpoint};bronze", ctype="SQL")],
    )
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert scan.connections[0].matched_source_items == (LAKEHOUSE,)


def test_a_bare_catalog_name_alone_is_never_enough_to_match():
    """The catalog name here ('bronze') is a source item's display name, but nothing in the
    scan's signature set is built from display names alone - only ids, OneLake roots and
    endpoints - so a connection naming an unrelated server with the same catalog word must
    not be reported."""
    client = FakeClient(
        items=[{"id": LAKEHOUSE, "displayName": "bronze", "type": "Lakehouse"}],
        connections=[connection("conn-1", "totally-unrelated.example.com;bronze", ctype="SQL")],
    )
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert scan.connections == ()


def test_a_guid_embedded_without_a_boundary_does_not_match():
    """A GUID immediately followed by more identifier characters, with no delimiter, is not
    a genuine reference: it is a coincidental prefix of something longer."""
    client = FakeClient(connections=[connection("conn-1", f"https://example.com/{SOURCE_WS}extra/thing")])
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert scan.connections == ()


def test_a_connection_with_no_path_is_ignored():
    client = FakeClient(connections=[{
        "id": "conn-1", "displayName": "No path", "connectivityType": "ShareableCloud",
        "connectionDetails": {"type": "Web"},
    }])
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert scan.connections == ()


def test_an_unrelated_connection_is_not_reported():
    client = FakeClient(connections=[connection("conn-1", "https://totally-unrelated.example.com/data")])
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert scan.connections == ()
    assert "No tenant-visible connection" in scan.message


def test_original_source_connection_stays_listed_even_when_a_replacement_was_mapped():
    """A connection that has an operator-supplied replacement is not hidden: its own path
    still points at the source, and something outside this migration might still read
    through it."""
    client = FakeClient(connections=[connection("conn-old", f"https://example.com/{SOURCE_WS}/thing")])
    scan = ca.scan_source_connections(
        client, source_workspace_id=SOURCE_WS,
        id_map={"conn-old": "conn-new"},
    )
    assert [c.connection_id for c in scan.connections] == ["conn-old"]


# --------------------------------------------------------------- name/id fallback


def test_an_unnamed_connection_reports_its_id_never_the_word_none():
    client = FakeClient(connections=[{
        "id": "conn-1", "displayName": None, "connectivityType": "ShareableCloud",
        "connectionDetails": {"type": "Web", "path": f"https://example.com/{SOURCE_WS}"},
    }])
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert scan.connections[0].connection_name == "conn-1"
    assert "None" not in scan.connections[0].connection_name
    assert scan.connections[0].connection_id == "conn-1"


# --------------------------------------------------------------- expected new path


def test_expected_new_path_is_filled_only_once_the_id_map_resolves_it():
    client = FakeClient(connections=[connection("conn-1", f"https://example.com/{SOURCE_WS}/thing")])

    unresolved = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS, id_map={})
    assert unresolved.connections[0].expected_new_path == ""
    assert "expectedNewPath" not in unresolved.connections[0].as_dict()

    resolved = ca.scan_source_connections(
        client, source_workspace_id=SOURCE_WS, id_map={SOURCE_WS: TARGET_WS},
    )
    assert resolved.connections[0].expected_new_path == f"https://example.com/{TARGET_WS}/thing"
    assert resolved.connections[0].as_dict()["expectedNewPath"] == f"https://example.com/{TARGET_WS}/thing"


def test_expected_new_path_never_reveals_a_redacted_query_string():
    client = FakeClient(connections=[connection(
        "conn-1", f"https://example.com/{SOURCE_WS}/thing?sig=shouldnotappear",
    )])
    scan = ca.scan_source_connections(
        client, source_workspace_id=SOURCE_WS, id_map={SOURCE_WS: TARGET_WS},
    )
    assert "shouldnotappear" not in scan.connections[0].expected_new_path
    assert "shouldnotappear" not in scan.connections[0].path


def test_partially_rewritten_destination_path_is_not_suggested():
    client = FakeClient(
        items=[{"id": LAKEHOUSE, "displayName": "bronze", "type": "Lakehouse"}],
        connections=[connection("conn-1", f"https://example.com/{SOURCE_WS}/{LAKEHOUSE}")],
    )
    scan = ca.scan_source_connections(
        client, source_workspace_id=SOURCE_WS, id_map={SOURCE_WS: TARGET_WS},
    )
    assert scan.connections[0].expected_new_path == ""


def test_an_endpoint_inside_another_hostname_is_not_a_match():
    endpoint = "source.datawarehouse.fabric.microsoft.com"
    client = FakeClient(
        lakehouses=[{"id": LAKEHOUSE, "properties": {
            "sqlEndpointProperties": {"connectionString": endpoint},
        }}],
        connections=[connection("conn-1", f"https://{endpoint}.unrelated.test/path")],
    )
    assert not ca.scan_source_connections(client, source_workspace_id=SOURCE_WS).connections


def test_query_values_are_not_treated_as_connection_targets():
    client = FakeClient(connections=[connection(
        "conn-1", f"https://example.com/path?sig={SOURCE_WS}",
    )])
    assert not ca.scan_source_connections(client, source_workspace_id=SOURCE_WS).connections


def test_a_transport_failure_is_incomplete_not_a_migration_failure():
    error = FabricTransportError("GET", "connections", 2, httpx.ReadTimeout("read timed out"))
    client = FakeClient(list_errors={"connections": error})
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert scan.scan_state == ca.SCAN_INCOMPLETE
    assert "read timed out" in scan.message
    assert "application permission" not in scan.action
    assert scan.error_code == ""


def test_cancelled_scan_does_not_begin_network_reads():
    class NoReads(FakeClient):
        def list_all(self, *args, **kwargs):
            pytest.fail("Cancelled advisory scan must not read Fabric")

    def cancel():
        raise CancelledError("operator cancelled")

    with pytest.raises(CancelledError):
        ca.scan_source_connections(NoReads(), source_workspace_id=SOURCE_WS, check_cancel=cancel)


# ----------------------------------------------------------------- scan state


def test_a_permission_denied_connections_listing_is_reported_as_incomplete_not_empty():
    """The whole point: an inaccessible tenant listing must never look like a clean scan
    that simply found nothing."""
    error = FabricApiError("GET", "connections", 403, '{"errorCode":"Denied","message":"no access"}')

    class Denied(FakeClient):
        def list_all(self, path, params=None, value_key="value"):
            if path == "connections":
                raise error
            return super().list_all(path, params, value_key)

    scan = ca.scan_source_connections(Denied(), source_workspace_id=SOURCE_WS)
    assert scan.scan_state == ca.SCAN_INCOMPLETE
    assert scan.connections == ()
    assert scan.error_code == "Denied"
    assert "Denied" in scan.message or "no access" in scan.message
    assert scan.action


def test_a_partial_signature_read_failure_still_scans_what_it_can():
    error = FabricApiError("GET", "workspaces/x/warehouses", 403, '{"errorCode":"Denied","message":"nope"}')
    client = FakeClient(
        items=[{"id": LAKEHOUSE, "displayName": "bronze", "type": "Lakehouse"}],
        connections=[connection("conn-1", f"https://example.com/items/{LAKEHOUSE}")],
        list_errors={"/warehouses": error},
    )
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert scan.scan_state == ca.SCAN_INCOMPLETE
    assert "warehouses" in scan.message
    assert "Denied: nope" in scan.message
    assert scan.action
    # Still found what it could read.
    assert scan.connections[0].connection_id == "conn-1"


def test_a_failed_item_listing_is_a_limitation_not_a_crash():
    error = FabricApiError("GET", "workspaces/x/items", 500, '{"errorCode":"Boom","message":"down"}')
    client = FakeClient(
        connections=[connection("conn-1", f"https://example.com/{SOURCE_WS}")],
        list_errors={"/items": error},
    )
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert scan.scan_state == ca.SCAN_INCOMPLETE
    # The bare workspace GUID signature does not depend on the item listing, so it is
    # still found.
    assert scan.connections[0].connection_id == "conn-1"


def test_no_matches_and_a_complete_scan_is_reported_as_reviewed_not_unknown():
    client = FakeClient(connections=[])
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert scan.scan_state == ca.SCAN_COMPLETE
    assert scan.connections == ()


# ------------------------------------------------------------- section_for_report


def test_reassign_strategy_is_not_applicable_regardless_of_any_payload():
    payload = {"scanState": "complete", "connections": []}
    section = ca.section_for_report(payload, run_id="r1", strategy="reassign")
    assert section["scanState"] == ca.SCAN_NOT_APPLICABLE
    assert section["connections"] == []


def test_a_missing_scan_is_unknown_not_a_false_green():
    section = ca.section_for_report(None, run_id="r1", strategy="rebuild")
    assert section["scanState"] == ca.SCAN_UNKNOWN
    assert section["connections"] == []


def test_a_scan_from_the_current_attempt_is_unchanged():
    payload = {
        "matcherVersion": ca.MATCHER_VERSION,
        "scanState": "complete", "connections": [], "attemptId": "r1", "message": "fine",
    }
    section = ca.section_for_report(payload, run_id="r1", strategy="rebuild")
    assert section["scanState"] == "complete"
    assert "attemptId" not in section


def test_a_scan_from_an_earlier_attempt_is_marked_stale():
    payload = {
        "matcherVersion": ca.MATCHER_VERSION,
        "scanState": "complete", "connections": [{"connectionId": "conn-1"}],
        "attemptId": "old-attempt", "message": "1 tenant-visible connection(s)...",
    }
    section = ca.section_for_report(payload, run_id="new-attempt", strategy="rebuild")
    assert section["scanState"] == ca.SCAN_STALE
    assert "earlier attempt" in section["message"]
    assert section["connections"] == [{"connectionId": "conn-1"}]


def test_retired_matcher_snapshot_is_withheld_without_changing_the_journal_payload():
    payload = {
        "scanState": "complete", "attemptId": "r1",
        "connections": [{"connectionId": "c", "expectedNewPath": "other-server;wrong-database"}],
    }
    section = ca.section_for_report(payload, run_id="r1", strategy="rebuild")
    assert section["scanState"] == ca.SCAN_STALE
    assert section["connections"] == []
    assert "retired identifier-only matcher" in section["message"]
    assert "not revalidated" in section["message"]
    assert "lookup script" in section["action"]
    assert payload["connections"][0]["expectedNewPath"] == "other-server;wrong-database"


def sql_source_fixture(connections):
    return FakeClient(
        items=[{"id": LAKEHOUSE, "type": "Lakehouse", "displayName": "bronze"},
               {"id": WAREHOUSE, "type": "SQLEndpoint", "displayName": "bronze"}],
        lakehouses=[{
            "id": LAKEHOUSE, "displayName": "bronze",
            "properties": {"sqlEndpointProperties": {
                "id": WAREHOUSE, "connectionString": "source.datawarehouse.fabric.microsoft.com",
            }},
        }],
        connections=connections,
    )


def test_database_guid_repeated_on_five_other_servers_does_not_target_this_workspace():
    """Reproduces the live report: SQL endpoint GUIDs matched on other workspaces' hosts."""
    rows = [
        connection(f"conn-{i}-{catalog}", f"other-{i}.datawarehouse.fabric.microsoft.com;{catalog}",
                   ctype="SQL", connectivity="PersonalCloud", name=None)
        for i in range(5) for catalog in (WAREHOUSE, LAKEHOUSE)
    ]
    scan = ca.scan_source_connections(sql_source_fixture(rows), source_workspace_id=SOURCE_WS)
    assert scan.scan_state == "complete"
    assert scan.connections == ()


@pytest.mark.parametrize("connectivity", ["PersonalCloud", "ShareableCloud", "Automatic"])
@pytest.mark.parametrize("catalog", ["bronze", WAREHOUSE, WAREHOUSE.upper()])
def test_confirmed_sql_pairs_remain_visible_regardless_of_connection_kind(connectivity, catalog):
    row = connection("c", f"source.datawarehouse.fabric.microsoft.com;{catalog}",
                     ctype="SQL", connectivity=connectivity)
    scan = ca.scan_source_connections(sql_source_fixture([row]), source_workspace_id=SOURCE_WS)
    [found] = scan.connections
    assert found.matched_source_items == (LAKEHOUSE,)
    payload = found.as_dict()
    assert payload["matchBasis"] == "sql_server_database"
    assert payload["usageState"] == "not_checked"
    if connectivity == "PersonalCloud":
        assert "does not prove default-model ownership" in payload["action"]
    if connectivity == "Automatic":
        assert "Implicit/SSO" in payload["action"]


@pytest.mark.parametrize("catalog", ["other", LAKEHOUSE, SOURCE_WS, "bronze-more", "Bronze"])
def test_sql_server_alone_and_non_catalog_item_ids_are_not_enough(catalog):
    row = connection("c", f"source.datawarehouse.fabric.microsoft.com;{catalog}", ctype="SQL")
    scan = ca.scan_source_connections(sql_source_fixture([row]), source_workspace_id=SOURCE_WS)
    assert scan.connections == ()


@pytest.mark.parametrize("path", ["", "source.datawarehouse.fabric.microsoft.com",
                                "source.datawarehouse.fabric.microsoft.com;",
                                "source.datawarehouse.fabric.microsoft.com;bronze;extra"])
def test_unparseable_sql_paths_are_incomplete_not_generic_guid_matches(path):
    row = connection("c", path, ctype="SQL")
    scan = ca.scan_source_connections(sql_source_fixture([row]), source_workspace_id=SOURCE_WS)
    assert scan.scan_state == "incomplete"
    assert scan.connections == ()
    assert "connection c" in scan.message


def test_unknown_sql_style_connector_cannot_bypass_pair_matching():
    row = connection("c", f"other.datawarehouse.fabric.microsoft.com;{WAREHOUSE}", ctype="NewSqlType")
    scan = ca.scan_source_connections(sql_source_fixture([row]), source_workspace_id=SOURCE_WS)
    assert not scan.connections


def test_sql_destination_suggestion_requires_exact_mappings_for_both_coordinates():
    row = connection("c", f"source.datawarehouse.fabric.microsoft.com;{WAREHOUSE}", ctype="SQL")
    for mappings in (
        {WAREHOUSE: TARGET_LAKEHOUSE},
        {"source.datawarehouse.fabric.microsoft.com": "target.datawarehouse.fabric.microsoft.com"},
    ):
        scan = ca.scan_source_connections(
            sql_source_fixture([row]), source_workspace_id=SOURCE_WS, id_map=mappings,
        )
        assert scan.connections[0].expected_new_path == ""
    scan = ca.scan_source_connections(
        sql_source_fixture([row]), source_workspace_id=SOURCE_WS,
        id_map={WAREHOUSE: TARGET_LAKEHOUSE,
                "source.datawarehouse.fabric.microsoft.com": "target.datawarehouse.fabric.microsoft.com"},
    )
    assert scan.connections[0].expected_new_path == \
        f"target.datawarehouse.fabric.microsoft.com;{TARGET_LAKEHOUSE}"


def test_sql_database_uses_service_database_name_not_display_name_or_item_id():
    rows = [connection(f"c-{i}", f"server.database.windows.net;{db}", ctype="SQL")
            for i, db in enumerate(["actual-catalog", "Display", LAKEHOUSE])]
    client = FakeClient(sql_databases=[{
        "id": LAKEHOUSE, "displayName": "Display",
        "properties": {"serverFqdn": "tcp:SERVER.database.windows.net,1433",
                       "databaseName": "actual-catalog"},
    }], connections=rows)
    scan = ca.scan_source_connections(client, source_workspace_id=SOURCE_WS)
    assert [c.connection_id for c in scan.connections] == ["c-0"]


def test_matching_server_with_unknown_catalog_is_unverified_not_a_clean_scan():
    row = connection("c", "source.datawarehouse.fabric.microsoft.com;master", ctype="SQL")
    scan = ca.scan_source_connections(sql_source_fixture([row]), source_workspace_id=SOURCE_WS)
    assert scan.scan_state == "incomplete"
    assert not scan.connections
    assert "SQL server matches, but the database" in scan.message


def test_sql_destination_cannot_be_another_source_store_server():
    row = connection("c", f"source.datawarehouse.fabric.microsoft.com;{WAREHOUSE}", ctype="SQL")
    client = sql_source_fixture([row])
    client.sql_databases = [{
        "id": "other-source-store", "properties": {
            "serverFqdn": "other-source.database.windows.net", "databaseName": "Original",
        },
    }]
    scan = ca.scan_source_connections(
        client, source_workspace_id=SOURCE_WS, id_map={
            "source.datawarehouse.fabric.microsoft.com": "other-source.database.windows.net",
            WAREHOUSE: TARGET_LAKEHOUSE,
        },
    )
    assert scan.connections[0].expected_new_path == ""
