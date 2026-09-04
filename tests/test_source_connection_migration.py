"""A full rebuild must replace internal connections before importing their consumers."""

from dataclasses import replace

import pytest
from test_connection_replacement import SQL_METADATA, connection
from test_rebuild_ordering import (
    LAKEHOUSE,
    MODEL,
    MODEL_BIM,
    PRINCIPAL,
    REPORT,
    REPORT_PBIR,
    SOURCE_ENDPOINT,
    SOURCE_WS,
    TARGET_ENDPOINT,
    TARGET_WS,
    FakeFabric,
    make_plan,
)

from fabshuffle import journal, orchestrator
from fabshuffle.config import SETTINGS
from fabshuffle.fabric import connections
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import decode_json_part, part
from fabshuffle.run import MigrationRun, RunStatus

CONNECTION = "aaaabbbb-1111-2222-3333-444455556666"
REPLACEMENT = "bbbbcccc-1111-2222-3333-444455556666"
EXTERNAL = "ccccdddd-1111-2222-3333-444455556666"
CONSUMERS = [
    {"id": "pipeline-source", "type": "DataPipeline", "displayName": "Daily pipeline"},
    {"id": "stream-source", "type": "Eventstream", "displayName": "Live events"},
]


class Fabric(FakeFabric):
    def __init__(self):
        super().__init__()
        self.source_connection = connection(
            id=CONNECTION,
            connectionDetails={"type": "SQL", "path": f"{SOURCE_ENDPOINT};{LAKEHOUSE}"},
        )
        self.tenant_connections = [
            self.source_connection,
            connection(id=EXTERNAL, connectionDetails={"type": "SQL", "path": "external.example.com;other"}),
        ]
        self.connection_creates = []
        self.connection_error = None
        self.unreadable = set()
        self.early_items = []
        self.definitions[MODEL] = [part("model.bim", MODEL_BIM)]
        self.definitions[REPORT] = [part("definition.pbir", REPORT_PBIR)]
        for item in CONSUMERS:
            self.definitions[item["id"]] = [part("content.json", {
                "connectionId": CONNECTION.upper(),
                "externalReferences": {"connection": EXTERNAL},
            })]

    def list_all(self, path, params=None, value_key="value"):
        if path == "connections":
            return self.tenant_connections
        if path == "connections/supportedConnectionTypes":
            return [SQL_METADATA]
        if path == f"workspaces/{SOURCE_WS}/mirroredDatabases":
            return self.early_items
        items = super().list_all(path, params, value_key)
        if path == f"workspaces/{SOURCE_WS}/items":
            return [*items, *CONSUMERS, *self.early_items]
        if path == f"workspaces/{TARGET_WS}/items":
            return [item for item in items if item["type"] != "Connection"]
        return items

    def get(self, path, params=None):
        if path.startswith("connections/"):
            connection_id = path.split("/")[1]
            if connection_id not in self.unreadable:
                for candidate in self.tenant_connections:
                    if candidate["id"] == connection_id:
                        return candidate
            raise FabricApiError("GET", path, 403, '{"errorCode":"Denied","message":"share replacement"}')
        return super().get(path, params)

    def post(self, path, json=None, params=None, wait=True):
        if path == "connections":
            self.connection_creates.append(json)
            if self.connection_error:
                raise self.connection_error
            candidate = {
                **json, "id": REPLACEMENT,
                "connectionDetails": {
                    "type": json["connectionDetails"]["type"],
                    "path": ";".join(p["value"] for p in json["connectionDetails"]["parameters"]),
                },
            }
            self.tenant_connections.append(candidate)
            self.created.append(("Connection", json["displayName"], REPLACEMENT))
            return candidate
        return super().post(path, json, params, wait)


@pytest.fixture
def fabric(monkeypatch):
    fake = Fabric()
    monkeypatch.setattr(orchestrator, "FabricClient", fake)
    monkeypatch.setattr(orchestrator, "TokenProvider", lambda principal: object())
    monkeypatch.setattr(orchestrator.workspaces, "clone_folder_tree", lambda *a: {})
    monkeypatch.setattr(orchestrator.workspaces, "list_role_assignments", lambda *a: [])
    monkeypatch.setattr(orchestrator.workspaces, "copy_role_assignments", lambda *a, **k: [])
    monkeypatch.setattr(orchestrator.sqlschema, "transfer_schema", lambda **k: [])
    return fake


def rebuild(fabric, prior=None):
    run = MigrationRun(source_workspace_name="bronze-ws", capacity_name="F64")
    plan = replace(make_plan(), include_data=False, include_files=False)
    orchestrator.run_migration(run, PRINCIPAL, plan, cleanup=False, prior=prior)
    assert run.status == RunStatus.SUCCEEDED, run.error
    return run


def test_connection_is_created_and_journaled_before_pipeline_and_eventstream(fabric):
    run = rebuild(fabric)
    order = [kind for kind, _, _ in fabric.created]
    assert order.index("Lakehouse") < order.index("Connection") < order.index("Eventstream")
    assert order.index("Connection") < order.index("DataPipeline")
    for kind, _, target_id in fabric.created:
        if kind in {"DataPipeline", "Eventstream"}:
            payload = decode_json_part(fabric.definitions[target_id][0]["payload"])
            assert payload["connectionId"] == REPLACEMENT
            assert payload["externalReferences"]["connection"] == EXTERNAL
    assert len(fabric.connection_creates) == 1
    assert fabric.connection_creates[0]["connectionDetails"]["parameters"] == [
        {"name": "server", "dataType": "Text", "value": TARGET_ENDPOINT},
        {"name": "database", "dataType": "Text", "value": "lh-new"},
    ]
    assert journal.read(SETTINGS.journal_for(run.id)).id_map[CONNECTION] == REPLACEMENT


@pytest.mark.parametrize("reason", ["secrets", "create-fails", "unmapped-store"])
def test_unresolved_source_connection_prevents_dependent_creates(fabric, reason):
    if reason == "secrets":
        fabric.source_connection["credentialDetails"] = {"credentialType": "Basic"}
    elif reason == "create-fails":
        fabric.connection_error = FabricApiError(
            "POST", "connections", 400, '{"errorCode":"InvalidCredentials","message":"grant identity"}',
        )
    else:
        fabric.source_connection["connectionDetails"]["path"] = f"{SOURCE_ENDPOINT};{MODEL}"
    run = rebuild(fabric)
    assert not any(kind in {"DataPipeline", "Eventstream"} for kind, _, _ in fabric.created)
    warnings = run.summary["warnings"]
    assert any("Bronze SQL" in warning for warning in warnings)
    if reason == "create-fails":
        assert any("InvalidCredentials" in warning and "grant identity" in warning for warning in warnings)
    elif reason == "secrets":
        assert not fabric.connection_creates
        assert any("enter its credentials" in warning and TARGET_ENDPOINT in warning for warning in warnings)


def test_retry_adopts_an_operator_created_replacement_without_copying_secrets(fabric):
    fabric.source_connection["credentialDetails"] = {"credentialType": "Basic"}
    first = rebuild(fabric)
    candidate = connection(
        id=REPLACEMENT, displayName=connections.replacement_name(fabric.source_connection, TARGET_WS),
        connectionDetails={"type": "SQL", "path": f"{TARGET_ENDPOINT};lh-new"},
        credentialDetails={"credentialType": "Basic"},
    )
    fabric.tenant_connections.append(candidate)
    second = rebuild(fabric, journal.read(SETTINGS.journal_for(first.id)))
    assert not fabric.connection_creates
    assert any(kind == "DataPipeline" for kind, _, _ in fabric.created)
    assert journal.read(SETTINGS.journal_for(second.id)).id_map[CONNECTION] == REPLACEMENT


def test_retry_verifies_and_reuses_a_prior_replacement(fabric):
    first = rebuild(fabric)
    rebuild(fabric, journal.read(SETTINGS.journal_for(first.id)))
    assert len(fabric.connection_creates) == 1


def test_an_inaccessible_prior_replacement_is_not_duplicated_or_used_for_new_consumers(fabric):
    first = rebuild(fabric)
    prior = journal.read(SETTINGS.journal_for(first.id))
    # A prior consumer that was never created must not reuse an unverifiable connection.
    for item in CONSUMERS:
        prior.id_map.pop(item["id"], None)
    fabric.created = [entry for entry in fabric.created if entry[0] not in {"DataPipeline", "Eventstream"}]
    fabric.unreadable.add(REPLACEMENT)
    second = rebuild(fabric, prior)
    assert len(fabric.connection_creates) == 1
    assert not any(kind in {"DataPipeline", "Eventstream"} for kind, _, _ in fabric.created)
    assert any(
        "Denied" in warning and "share replacement" in warning
        for warning in second.summary["warnings"]
    )


def test_name_alone_never_adopts_a_source_bound_connection(fabric):
    fabric.source_connection["credentialDetails"] = {"credentialType": "Basic"}
    fabric.tenant_connections.append(connection(
        id=REPLACEMENT, displayName=connections.replacement_name(fabric.source_connection, TARGET_WS),
        connectionDetails={"type": "SQL", "path": f"{SOURCE_ENDPOINT};{LAKEHOUSE}"},
    ))
    rebuild(fabric)
    assert not any(kind in {"DataPipeline", "Eventstream"} for kind, _, _ in fabric.created)


def test_early_mirror_consumer_refuses_a_connection_not_yet_replaced(fabric):
    mirror = {"id": "mirror", "displayName": "Internal mirror", "type": "MirroredDatabase"}
    fabric.early_items = [mirror]
    fabric.definitions["mirror"] = [part("mirroring.json", {"connection": CONNECTION})]
    run = rebuild(fabric)
    assert not any(kind == "MirroredDatabase" for kind, _, _ in fabric.created)
    assert any(
        "Internal mirror" in warning and "Bronze SQL" in warning
        for warning in run.summary["warnings"]
    )
    assert "mirror" not in journal.read(SETTINGS.journal_for(run.id)).id_map
    assert len(fabric.connection_creates) == 1


def test_connection_detection_does_not_require_the_relations_api(fabric, monkeypatch):
    from fabshuffle.fabric import relations

    monkeypatch.setattr(
        relations, "build_graph", lambda *a, **k: relations.DependencyGraph(available=False),
    )
    fabric.source_connection["credentialDetails"] = {"credentialType": "Basic"}
    rebuild(fabric)
    assert not any(kind in {"DataPipeline", "Eventstream"} for kind, _, _ in fabric.created)


@pytest.mark.parametrize("mismatch", ["type", "connectivityType"])
def test_manual_adoption_requires_connector_and_connectivity_identity(fabric, mismatch):
    fabric.source_connection["credentialDetails"] = {"credentialType": "Basic"}
    replacement = connection(
        id=REPLACEMENT, displayName=connections.replacement_name(fabric.source_connection, TARGET_WS),
        connectionDetails={"type": "SQL", "path": f"{TARGET_ENDPOINT};lh-new"},
    )
    if mismatch == "type":
        replacement["connectionDetails"]["type"] = "Web"
    else:
        replacement["connectivityType"] = "PersonalCloud"
    fabric.tenant_connections.append(replacement)
    rebuild(fabric)
    assert not any(kind in {"DataPipeline", "Eventstream"} for kind, _, _ in fabric.created)
