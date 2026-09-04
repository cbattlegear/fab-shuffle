"""End-to-end ordering test for the rebuild path.

Drives the orchestrator against an in-memory Fabric so the phase order, and the id map that
each phase feeds the next, are exercised together rather than unit by unit.
"""

from __future__ import annotations

import json

import pytest

from fabshuffle import journal, orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.config import SETTINGS
from fabshuffle.fabric.definitions import decode_payload, part
from fabshuffle.fabric.support import Strategy
from fabshuffle.run import MigrationRun, RunStatus

PRINCIPAL = ServicePrincipal("tenant", "client", "secret")

SOURCE_WS = "ws-source"
TARGET_WS = "ws-target"
LAKEHOUSE = "lh-source"
MODEL = "sm-source"
REPORT = "rp-source"
SOURCE_ENDPOINT = "src.datawarehouse.fabric.microsoft.com"
TARGET_ENDPOINT = "dst.datawarehouse.fabric.microsoft.com"

MODEL_BIM = json.dumps(
    {
        "model": {
            "expressions": [
                {
                    "name": "DatabaseQuery",
                    "expression": f'Sql.Database("{SOURCE_ENDPOINT}", "{LAKEHOUSE}")',
                }
            ]
        }
    }
)
REPORT_PBIR = json.dumps(
    {"version": "4.0", "datasetReference": {"byConnection": {"connectionString": f"semanticmodelid={MODEL}"}}}
)


class FakeFabric:
    """Records every create call in order and serves the reads each phase makes."""

    def __init__(self, relations: dict[str, list[dict]] | None = None) -> None:
        self.created: list[tuple[str, str, str]] = []
        self.definitions: dict[str, list[dict]] = {}
        self.next_id = 0
        # source item id -> upstream relation edges
        self.relations = relations or {}

    # -- lifecycle used by the orchestrator -------------------------------------------

    def __call__(self, _tokens) -> FakeFabric:
        return self

    def __enter__(self) -> FakeFabric:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def _mint(self, prefix: str) -> str:
        self.next_id += 1
        return f"{prefix}-{self.next_id}"

    # -- reads -----------------------------------------------------------------------

    def get(self, path, params=None):
        if path.endswith("/relations/upstream") or path.endswith("/relations/downstream"):
            item_id = path.split("/items/")[1].split("/")[0]
            edges = self.relations.get(item_id, []) if path.endswith("/upstream") else []
            return {"items": [], "relations": edges, "workspaces": []}
        if path.endswith("/spark/settings"):
            return {}
        if path == f"workspaces/{TARGET_WS}/lakehouses/lh-new":
            return {
                "id": "lh-new",
                "displayName": "bronze",
                "properties": {
                    "oneLakeFilesPath": "https://onelake/target/Files",
                    "sqlEndpointProperties": {
                        "id": "ep-new",
                        "connectionString": TARGET_ENDPOINT,
                        "provisioningStatus": "Success",
                    },
                },
            }
        raise AssertionError(f"unexpected GET {path}")

    def list_all(self, path, params=None, value_key="value"):
        item_type = (params or {}).get("type")

        if path == f"workspaces/{SOURCE_WS}/items":
            items = [
                {"id": LAKEHOUSE, "displayName": "bronze", "type": "Lakehouse"},
                {"id": MODEL, "displayName": "Sales Model", "type": "SemanticModel"},
                {"id": REPORT, "displayName": "Sales", "type": "Report"},
            ]
            return [i for i in items if not item_type or i["type"] == item_type]

        if path == f"workspaces/{SOURCE_WS}/lakehouses":
            return [
                {
                    "id": LAKEHOUSE,
                    "displayName": "bronze",
                    "properties": {
                        "oneLakeFilesPath": "https://onelake/source/Files",
                        "sqlEndpointProperties": {"id": "ep-src", "connectionString": SOURCE_ENDPOINT},
                    },
                }
            ]
        if path.endswith("/tables"):
            return []
        if path.endswith("/spark/pools"):
            return []
        return []

    # -- writes ----------------------------------------------------------------------

    def post(self, path, json=None, params=None, wait=True):
        body = json or {}

        if path == "workspaces":
            return {"id": TARGET_WS if body["displayName"].startswith("bronze-ws") else "ws-scratch"}
        if path.endswith("/lakehouses"):
            if TARGET_WS in path:
                self.created.append(("Lakehouse", body["displayName"], "lh-new"))
                return {"id": "lh-new"}
            return {"id": "lh-scratch"}
        if path.endswith("/getDefinition"):
            item_id = path.split("/items/")[1].split("/")[0]
            return {"definition": {"parts": self.definitions[item_id]}}
        if path.endswith("/items"):
            new_id = self._mint(body["type"].lower())
            self.created.append((body["type"], body["displayName"], new_id))
            self.definitions[new_id] = body["definition"]["parts"]
            return {"id": new_id}
        if path.endswith("/refreshMetadata"):
            return {}
        return {}

    def delete(self, path, params=None):
        return None


@pytest.fixture
def fabric(monkeypatch) -> FakeFabric:
    fake = FakeFabric()
    fake.definitions[MODEL] = [part("model.bim", MODEL_BIM), part(".platform", "{}")]
    fake.definitions[REPORT] = [part("definition.pbir", REPORT_PBIR), part(".platform", "{}")]

    monkeypatch.setattr(orchestrator, "FabricClient", fake)
    monkeypatch.setattr(orchestrator, "TokenProvider", lambda principal: object())

    # Everything that leaves the process is stubbed; this test is about ordering and rebinding.
    monkeypatch.setattr(orchestrator.workspaces, "clone_folder_tree", lambda c, s, t: {})
    monkeypatch.setattr(orchestrator.workspaces, "list_role_assignments", lambda c, w: [])
    monkeypatch.setattr(orchestrator.workspaces, "copy_role_assignments", lambda *a, **k: [])
    monkeypatch.setattr(orchestrator.shortcuts, "copy_shortcuts", lambda *a, **k: (0, []))
    monkeypatch.setattr(orchestrator.file_transfer, "copy_files", lambda **k: None)
    monkeypatch.setattr(orchestrator.sqlschema, "transfer_schema", lambda **k: [])
    return fake


def make_plan() -> orchestrator.MigrationPlan:
    return orchestrator.MigrationPlan(
        capacity_id="cap-1",
        capacity_name="F64",
        capacity_region="westeurope",
        source_workspace_id=SOURCE_WS,
        source_workspace_name="bronze-ws",
        target_workspace_name="bronze-ws-westeurope",
        strategy=Strategy.REBUILD,
    )


def run(fabric: FakeFabric) -> MigrationRun:
    migration = MigrationRun(source_workspace_name="bronze-ws", capacity_name="F64")
    orchestrator.run_migration(migration, PRINCIPAL, make_plan(), cleanup=False)
    return migration


def test_phases_run_in_dependency_order(fabric):
    migration = run(fabric)
    assert migration.status == RunStatus.SUCCEEDED, migration.error

    assert [step["id"] for step in migration.snapshot()["steps"]] == [
        "assessment",
        "dependencies",
        "workspaces",
        "eventhouses",
        "lakehouses",
        "warehouses",
        "sqldatabases",
        "mirrored",
        "shortcuts",
        "realtime",
        "engineering",
        "analytics",
        "orchestration",
        "reflexes",
        "permissions",
    ]


# ------------------------------------------------------------------- the journal


def test_a_run_writes_down_enough_to_be_picked_up(fabric):
    """The whole point of the journal: what a later attempt needs in order to skip ahead."""
    migration = run(fabric)
    replay = journal.read(SETTINGS.journal_for(migration.id))

    assert replay.plan["source_workspace_id"] == SOURCE_WS
    assert replay.target_workspace_id == TARGET_WS
    # Every phase that ran is recorded as finished, in the order it ran.
    assert replay.phases_started[:3] == ["assessment", "dependencies", "workspaces"]
    assert "analytics" in replay.phases_finished
    assert replay.status == RunStatus.SUCCEEDED.value


def test_the_journal_maps_every_item_that_was_created(fabric):
    """id_map is what later phases rewrite definitions through, so it has to survive intact.

    Nothing may be created without being written down: an item missing from the journal is one
    a resume would build a second time, under a name that is already taken.
    """
    migration = run(fabric)
    replay = journal.read(SETTINGS.journal_for(migration.id))

    recorded = set(replay.id_map.values())
    for kind, name, new_id in fabric.created:
        assert new_id in recorded, f"{kind} '{name}' was created but never recorded"

    # And the bindings the rebuild test depends on are the ones that came back.
    assert replay.id_map[LAKEHOUSE] == "lh-new"
    assert replay.id_map[SOURCE_WS] == TARGET_WS


def test_the_journal_records_endpoints_as_well_as_items(fabric):
    """A semantic model rebinds through the endpoint name, not only through the item id."""
    migration = run(fabric)
    replay = journal.read(SETTINGS.journal_for(migration.id))

    assert replay.id_map.get(SOURCE_ENDPOINT) == TARGET_ENDPOINT


def test_a_run_that_died_records_no_ending(fabric, monkeypatch):
    """A run with no ending is exactly the one worth offering back, and how it is recognised."""

    def explode(_ctx):
        raise RuntimeError("the capacity went away")

    # Patched in the phase table rather than on the module: the table holds the functions
    # themselves, taken when it was defined, so rebinding the name would have no effect.
    monkeypatch.setattr(
        orchestrator,
        "_REBUILD_PHASES",
        tuple(
            (name, explode if name == "realtime" else fn)
            for name, fn in orchestrator._REBUILD_PHASES
        ),
    )

    migration = MigrationRun(source_workspace_name="bronze-ws", capacity_name="F64")
    orchestrator.run_migration(migration, PRINCIPAL, make_plan(), cleanup=False)
    replay = journal.read(SETTINGS.journal_for(migration.id))

    assert migration.status == RunStatus.FAILED
    assert replay.interrupted
    assert not replay.status
    # What it got through is still there to be picked up.
    assert "lakehouses" in replay.phases_finished
    assert "realtime" not in replay.phases_finished
    assert replay.target_workspace_id == TARGET_WS


def test_data_items_are_created_before_the_models_that_read_them(fabric):
    run(fabric)
    order = [item_type for item_type, _, _ in fabric.created]

    # A semantic model binds to the lakehouse, and a report binds to the model.
    assert order.index("Lakehouse") < order.index("SemanticModel") < order.index("Report")


def test_semantic_model_is_rebound_to_the_new_lakehouse(fabric):
    run(fabric)
    model_id = next(new_id for kind, _, new_id in fabric.created if kind == "SemanticModel")
    payload = next(p for p in fabric.definitions[model_id] if p["path"] == "model.bim")
    text = decode_payload(payload["payload"]).decode()

    assert TARGET_ENDPOINT in text and "lh-new" in text
    assert SOURCE_ENDPOINT not in text and LAKEHOUSE not in text


def test_report_is_rebound_to_the_new_semantic_model(fabric):
    run(fabric)
    model_id = next(new_id for kind, _, new_id in fabric.created if kind == "SemanticModel")
    report_id = next(new_id for kind, _, new_id in fabric.created if kind == "Report")

    payload = next(p for p in fabric.definitions[report_id] if p["path"] == "definition.pbir")
    binding = json.loads(decode_payload(payload["payload"]))
    assert binding["datasetReference"]["byConnection"]["connectionString"] == f"semanticmodelid={model_id}"


def test_source_platform_file_is_not_carried_over(fabric):
    run(fabric)
    model_id = next(new_id for kind, _, new_id in fabric.created if kind == "SemanticModel")
    assert all(p["path"] != ".platform" for p in fabric.definitions[model_id])


def test_a_model_sharing_its_lakehouses_name_is_still_migrated(fabric, monkeypatch):
    """It used to be skipped as Fabric's auto-created default. Fabric stopped creating those
    on 5 September 2025 and decoupled the existing ones by 30 November 2025, so a model with
    that name is now just a model, and skipping it lost somebody's work."""

    def list_all(path, params=None, value_key="value"):
        if path == f"workspaces/{SOURCE_WS}/items":
            items = [
                {"id": LAKEHOUSE, "displayName": "bronze", "type": "Lakehouse"},
                {"id": MODEL, "displayName": "bronze", "type": "SemanticModel"},
            ]
            item_type = (params or {}).get("type")
            return [i for i in items if not item_type or i["type"] == item_type]
        return FakeFabric.list_all(fabric, path, params, value_key)

    monkeypatch.setattr(fabric, "list_all", list_all)
    run(fabric)

    assert any(kind == "SemanticModel" for kind, _, _ in fabric.created)


def test_a_composite_model_is_created_after_the_model_it_reads(monkeypatch):
    """The relations graph, not source order, decides how semantic models are sequenced."""
    base, composite = "sm-base", "sm-composite"

    fabric = FakeFabric(
        relations={
            composite: [
                {"itemId": composite, "dependentOnItemId": base, "relationType": "Datasource"}
            ]
        }
    )
    fabric.definitions[base] = [part("model.bim", "{}")]
    fabric.definitions[composite] = [part("model.bim", "{}")]

    def list_all(path, params=None, value_key="value"):
        if path == f"workspaces/{SOURCE_WS}/items":
            # Listed composite-first, so source order alone would create it too early.
            items = [
                {"id": composite, "displayName": "Composite", "type": "SemanticModel"},
                {"id": base, "displayName": "Base", "type": "SemanticModel"},
                {"id": LAKEHOUSE, "displayName": "bronze", "type": "Lakehouse"},
            ]
            item_type = (params or {}).get("type")
            return [i for i in items if not item_type or i["type"] == item_type]
        return FakeFabric.list_all(fabric, path, params, value_key)

    monkeypatch.setattr(fabric, "list_all", list_all)
    monkeypatch.setattr(orchestrator, "FabricClient", fabric)
    monkeypatch.setattr(orchestrator, "TokenProvider", lambda principal: object())
    monkeypatch.setattr(orchestrator.workspaces, "clone_folder_tree", lambda c, s, t: {})
    monkeypatch.setattr(orchestrator.workspaces, "list_role_assignments", lambda c, w: [])
    monkeypatch.setattr(orchestrator.workspaces, "copy_role_assignments", lambda *a, **k: [])
    monkeypatch.setattr(orchestrator.shortcuts, "copy_shortcuts", lambda *a, **k: (0, []))
    monkeypatch.setattr(orchestrator.file_transfer, "copy_files", lambda **k: None)
    monkeypatch.setattr(orchestrator.sqlschema, "transfer_schema", lambda **k: [])

    run(fabric)

    models = [name for kind, name, _ in fabric.created if kind == "SemanticModel"]
    assert models == ["Base", "Composite"]


PIPELINE = "dp-1"
CONNECTION = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
PIPELINE_CONTENT = json.dumps(
    {
        "properties": {
            "activities": [
                {
                    "name": "Load bronze",
                    "typeProperties": {
                        "sink": {
                            "datasetSettings": {
                                "linkedService": {
                                    "properties": {
                                        "typeProperties": {
                                            "workspaceId": SOURCE_WS,
                                            "artifactId": LAKEHOUSE,
                                        }
                                    }
                                }
                            },
                            "externalReferences": {"connection": CONNECTION},
                        }
                    },
                }
            ]
        }
    }
)


def test_pipelines_are_rebound_and_their_connections_checked(monkeypatch):
    fabric = FakeFabric()
    fabric.definitions[PIPELINE] = [part("pipeline-content.json", PIPELINE_CONTENT)]

    def list_all(path, params=None, value_key="value"):
        if path == "connections":
            # A personal connection cannot be shared, so it must be reported.
            return [
                {"id": CONNECTION, "displayName": "My Files", "connectivityType": "PersonalCloud"}
            ]
        if path == f"workspaces/{SOURCE_WS}/items":
            items = [
                {"id": LAKEHOUSE, "displayName": "bronze", "type": "Lakehouse"},
                {"id": PIPELINE, "displayName": "Nightly", "type": "DataPipeline"},
            ]
            item_type = (params or {}).get("type")
            return [i for i in items if not item_type or i["type"] == item_type]
        return FakeFabric.list_all(fabric, path, params, value_key)

    monkeypatch.setattr(fabric, "list_all", list_all)
    monkeypatch.setattr(orchestrator, "FabricClient", fabric)
    monkeypatch.setattr(orchestrator, "TokenProvider", lambda principal: object())
    monkeypatch.setattr(orchestrator.workspaces, "clone_folder_tree", lambda c, s, t: {})
    monkeypatch.setattr(orchestrator.workspaces, "list_role_assignments", lambda c, w: [])
    monkeypatch.setattr(orchestrator.workspaces, "copy_role_assignments", lambda *a, **k: [])
    monkeypatch.setattr(orchestrator.shortcuts, "copy_shortcuts", lambda *a, **k: (0, []))
    monkeypatch.setattr(orchestrator.file_transfer, "copy_files", lambda **k: None)
    monkeypatch.setattr(orchestrator.sqlschema, "transfer_schema", lambda **k: [])

    migration = run(fabric)
    assert migration.status == RunStatus.SUCCEEDED, migration.error

    # The pipeline was created after the lakehouse it loads.
    order = [kind for kind, _, _ in fabric.created]
    assert order.index("Lakehouse") < order.index("DataPipeline")

    pipeline_id = next(new_id for kind, _, new_id in fabric.created if kind == "DataPipeline")
    content = decode_payload(fabric.definitions[pipeline_id][0]["payload"]).decode()
    assert TARGET_WS in content and "lh-new" in content
    assert LAKEHOUSE not in content
    # The connection id is deliberately left alone: connections are tenant scoped.
    assert CONNECTION in content

    step = next(s for s in migration.snapshot()["steps"] if s["id"] == "orchestration")
    assert any("cannot be shared" in warning for warning in step["warnings"])
