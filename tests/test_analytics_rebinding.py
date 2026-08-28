from __future__ import annotations

import base64
import json

import pytest

from fabshuffle.fabric import analytics
from fabshuffle.fabric.definitions import (
    build_rewriter,
    decode_payload,
    is_text_part,
    part,
    rewrite_parts,
    strip_part,
)
from fabshuffle.fabric.support import Strategy, assess_workspace

OLD_LAKEHOUSE = "11111111-1111-1111-1111-111111111111"
NEW_LAKEHOUSE = "22222222-2222-2222-2222-222222222222"
OLD_ENDPOINT = "aaaaaaa.datawarehouse.fabric.microsoft.com"
NEW_ENDPOINT = "bbbbbbb.datawarehouse.fabric.microsoft.com"
OLD_MODEL = "33333333-3333-3333-3333-333333333333"
NEW_MODEL = "44444444-4444-4444-4444-444444444444"

ID_MAP = {
    OLD_LAKEHOUSE: NEW_LAKEHOUSE,
    OLD_ENDPOINT: NEW_ENDPOINT,
    OLD_MODEL: NEW_MODEL,
}


# ------------------------------------------------------------------- rewriting


def test_direct_lake_model_expression_is_repointed():
    model_bim = json.dumps(
        {
            "model": {
                "expressions": [
                    {
                        "name": "DatabaseQuery",
                        "expression": [
                            "let",
                            f'    database = Sql.Database("{OLD_ENDPOINT}", "{OLD_LAKEHOUSE}")',
                            "in",
                            "    database",
                        ],
                    }
                ]
            }
        }
    )
    rewritten, changed = rewrite_parts([part("model.bim", model_bim)], ID_MAP)

    assert changed == 1
    text = decode_payload(rewritten[0]["payload"]).decode()
    assert NEW_ENDPOINT in text and NEW_LAKEHOUSE in text
    assert OLD_ENDPOINT not in text and OLD_LAKEHOUSE not in text


def test_report_binding_is_repointed():
    pbir = json.dumps({"version": "4.0", "datasetReference": {"byConnection": {
        "connectionString": f"semanticmodelid={OLD_MODEL}"
    }}})
    rewritten, changed = rewrite_parts([part("definition.pbir", pbir)], ID_MAP)

    assert changed == 1
    binding = json.loads(decode_payload(rewritten[0]["payload"]))
    assert binding["datasetReference"]["byConnection"]["connectionString"] == f"semanticmodelid={NEW_MODEL}"


def test_tmdl_parts_are_rewritten():
    tmdl = f"partition Sales = entity\n\tmode: directLake\n\tsource = {OLD_LAKEHOUSE}\n"
    rewritten, changed = rewrite_parts([part("definition/tables/sales.tmdl", tmdl)], ID_MAP)

    assert changed == 1
    assert NEW_LAKEHOUSE in decode_payload(rewritten[0]["payload"]).decode()


def test_guid_casing_is_ignored():
    # Fabric is inconsistent about GUID casing between endpoints.
    text = f"reference to {OLD_LAKEHOUSE.upper()}"
    rewritten, changed = rewrite_parts([part("definition.pbir", text)], ID_MAP)

    assert changed == 1
    assert decode_payload(rewritten[0]["payload"]).decode() == f"reference to {NEW_LAKEHOUSE}"


def test_binary_parts_are_passed_through_untouched():
    logo = {
        "path": "StaticResources/RegisteredResources/logo.jpg",
        "payload": base64.b64encode(b"\xff\xd8\xff\xe0binary\x00\x80data").decode(),
        "payloadType": "InlineBase64",
    }
    rewritten, changed = rewrite_parts([logo], ID_MAP)

    assert changed == 0
    assert rewritten[0]["payload"] == logo["payload"]


def test_replacement_is_single_pass():
    """A value produced by one replacement must not be rewritten by another key."""
    chained = {"AAA": "BBB", "BBB": "CCC"}
    rewrite = build_rewriter(chained)
    assert rewrite("AAA and BBB") == "BBB and CCC"


def test_longer_keys_win_over_the_guids_inside_them():
    overlapping = {
        OLD_LAKEHOUSE: NEW_LAKEHOUSE,
        f"{OLD_LAKEHOUSE}/Tables": "SPECIFIC",
    }
    assert build_rewriter(overlapping)(f"{OLD_LAKEHOUSE}/Tables") == "SPECIFIC"


def test_identity_and_empty_mappings_are_ignored():
    assert build_rewriter({}) is None
    assert build_rewriter({"same": "same"}) is None
    assert build_rewriter({"a": ""}) is None


def test_unreferenced_definition_reports_no_change():
    rewritten, changed = rewrite_parts([part("definition.pbir", "nothing to see")], ID_MAP)
    assert changed == 0
    assert rewritten[0]["payload"] == part("definition.pbir", "nothing to see")["payload"]


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("model.bim", True),
        ("definition.pbir", True),
        ("definition.pbism", True),
        ("definition/tables/sales.tmdl", True),
        (".platform", True),
        ("definition/pages/pages.json", True),
        ("StaticResources/RegisteredResources/logo.jpg", False),
        ("StaticResources/RegisteredResources/visual.pbiviz", False),
    ],
)
def test_text_part_detection(path: str, expected: bool):
    assert is_text_part(path) is expected


def test_strip_part_removes_only_the_named_path():
    parts = [part(".platform", "{}"), part("model.bim", "{}")]
    assert [p["path"] for p in strip_part(parts, ".platform")] == ["model.bim"]


# ------------------------------------------------------------------ assessment


def test_models_and_reports_are_migrated_but_do_not_force_a_rebuild():
    pbi_only = assess_workspace(
        [
            {"displayName": "Sales", "type": "Report"},
            {"displayName": "Sales Model", "type": "SemanticModel"},
        ]
    )
    assert pbi_only.strategy is Strategy.REASSIGN

    mixed = assess_workspace(
        [
            {"displayName": "bronze", "type": "Lakehouse"},
            {"displayName": "Sales", "type": "Report"},
            {"displayName": "Sales Model", "type": "SemanticModel"},
            {"displayName": "Exec", "type": "Dashboard"},
        ]
    )
    assert mixed.strategy is Strategy.REBUILD
    # Reports and models are now rebuilt; the dashboard still is not.
    assert mixed.unsupported_types == ["Dashboard"]
    assert {item["displayName"] for item in mixed.migrated} == {"bronze", "Sales", "Sales Model"}


# -------------------------------------------------------------------- analytics


class FakeClient:
    def __init__(self, items: list[dict[str, str]]) -> None:
        self.items = items

    def list_all(self, path, params=None, value_key="value"):
        item_type = (params or {}).get("type")
        if item_type:
            return [i for i in self.items if i["type"] == item_type]
        return self.items


def test_default_semantic_models_are_identified_by_their_parent():
    client = FakeClient(
        [
            {"id": "1", "displayName": "bronze", "type": "Lakehouse"},
            {"id": "2", "displayName": "bronze", "type": "SemanticModel"},
            {"id": "3", "displayName": "dw", "type": "Warehouse"},
            {"id": "4", "displayName": "Sales Model", "type": "SemanticModel"},
        ]
    )
    assert analytics.default_semantic_model_names(client, "ws") == {"bronze", "dw"}
