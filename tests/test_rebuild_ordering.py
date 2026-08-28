"""End-to-end ordering test for the rebuild path.

Drives the orchestrator against an in-memory Fabric so the phase order, and the id map that
each phase feeds the next, are exercised together rather than unit by unit.
"""

from __future__ import annotations

import json

import pytest

from fabshuffle import orchestrator
from fabshuffle.auth import ServicePrincipal
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

    def __init__(self) -> None:
        self.created: list[tuple[str, str, str]] = []
        self.definitions: dict[str, list[dict]] = {}
        self.next_id = 0

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
        "workspaces",
        "eventhouses",
        "lakehouses",
        "warehouses",
        "shortcuts",
        "analytics",
        "permissions",
    ]


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


def test_default_semantic_model_of_a_lakehouse_is_not_recreated(fabric, monkeypatch):
    # Rename the model to match the lakehouse, making it look like the auto-created default.
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

    assert not any(kind == "SemanticModel" for kind, _, _ in fabric.created)
