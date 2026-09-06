"""Preserving large semantic model storage format across a rebuild.

A model on the large (``PremiumFiles``) format is backed by Azure Premium Files. On a
rebuild the target model is created fresh, and Fabric creates every new semantic model on
its default small (``Abf``) format, so the large format is silently lost unless it is set
back. Verified live: a source model reporting ``targetStorageMode=PremiumFiles`` produced a
target reporting ``Abf`` with no warning. The reassign path already converts large models
down and up around the move; the rebuild path never touched storage format at all.
"""

from __future__ import annotations

import pytest

from fabshuffle import orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric import analytics, powerbi
from fabshuffle.run import MigrationRun, StepStatus

PRINCIPAL = ServicePrincipal("tenant", "client", "secret")


class FakePowerBi:
    """Stands in for PowerBiClient, recording every storage-format change in order."""

    def __init__(
        self,
        source_models: list[powerbi.SemanticModel],
        *,
        fail_on_target: str | None = None,
        fail_list: bool = False,
    ) -> None:
        self.source_models = source_models
        self.fail_on_target = fail_on_target
        self.fail_list = fail_list
        self.storage_calls: list[tuple[str, str, str]] = []
        self.listed: list[str] = []

    def __call__(self, _tokens) -> FakePowerBi:
        return self

    def __enter__(self) -> FakePowerBi:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def list_semantic_models(self, workspace_id: str) -> list[powerbi.SemanticModel]:
        self.listed.append(workspace_id)
        if self.fail_list:
            raise powerbi.PowerBiError(f"GET /groups/{workspace_id}/datasets failed with HTTP 403")
        return self.source_models

    def set_storage_mode(self, workspace_id: str, model_id: str, storage_mode: str) -> None:
        if model_id == self.fail_on_target:
            raise powerbi.PowerBiError(f"HTTP 400: cannot enable large storage on {model_id}")
        self.storage_calls.append((workspace_id, model_id, storage_mode))


def large(name: str, model_id: str) -> powerbi.SemanticModel:
    return powerbi.SemanticModel(id=model_id, name=name, storage_mode=powerbi.LARGE, content_provider="")


def small(name: str, model_id: str) -> powerbi.SemanticModel:
    return powerbi.SemanticModel(id=model_id, name=name, storage_mode=powerbi.SMALL, content_provider="")


def make_ctx(
    *,
    region: str = "centralus",
    id_map: dict[str, str] | None = None,
    target_ws: str = "ws-target",
) -> orchestrator._Context:
    plan = orchestrator.MigrationPlan(
        capacity_id="cap",
        capacity_name="F64",
        capacity_region=region,
        source_workspace_id="ws-source",
        source_workspace_name="src",
        target_workspace_name="dst",
    )
    ctx = orchestrator._Context(
        client=object(),
        tokens=object(),
        principal=PRINCIPAL,
        plan=plan,
        run=MigrationRun(source_workspace_name="src", capacity_name="F64"),
        scratch_dir=None,
    )
    ctx.target_workspace_id = target_ws
    if id_map:
        ctx.id_map.update(id_map)
    ctx.run.start_step("analytics", "Migrating semantic models and reports")
    return ctx


# ------------------------------------------------------- the restore helper


def test_a_large_source_model_sets_the_target_back_to_premium_files(monkeypatch):
    fake = FakePowerBi([large("sm_sales", "sm-src")])
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", fake)

    ctx = make_ctx(id_map={"sm-src": "sm-tgt"})
    warnings = orchestrator._restore_large_semantic_models(ctx, "analytics")

    assert warnings == []
    # The source is read, and the target model is the one set to large.
    assert fake.listed == ["ws-source"]
    assert fake.storage_calls == [("ws-target", "sm-tgt", powerbi.LARGE)]


def test_a_small_source_model_is_left_alone(monkeypatch):
    fake = FakePowerBi([small("sm_ref", "sm-src")])
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", fake)

    ctx = make_ctx(id_map={"sm-src": "sm-tgt"})
    warnings = orchestrator._restore_large_semantic_models(ctx, "analytics")

    assert warnings == []
    assert fake.storage_calls == []


def test_only_the_large_targets_are_set_when_formats_are_mixed(monkeypatch):
    fake = FakePowerBi([large("Big", "big-src"), small("Little", "little-src"), large("Huge", "huge-src")])
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", fake)

    ctx = make_ctx(id_map={"big-src": "big-tgt", "little-src": "little-tgt", "huge-src": "huge-tgt"})
    warnings = orchestrator._restore_large_semantic_models(ctx, "analytics")

    assert warnings == []
    assert fake.storage_calls == [
        ("ws-target", "big-tgt", powerbi.LARGE),
        ("ws-target", "huge-tgt", powerbi.LARGE),
    ]


def test_a_large_model_that_did_not_migrate_is_skipped(monkeypatch):
    """A model missing from ``id_map`` did not migrate, and migrate_items already warned."""
    fake = FakePowerBi([large("sm_sales", "sm-src")])
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", fake)

    ctx = make_ctx(id_map={})
    warnings = orchestrator._restore_large_semantic_models(ctx, "analytics")

    assert warnings == []
    assert fake.storage_calls == []


def test_an_unsupported_target_region_warns_and_makes_no_call(monkeypatch):
    fake = FakePowerBi([large("sm_sales", "sm-src")])
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", fake)

    ctx = make_ctx(region="nowhereland", id_map={"sm-src": "sm-tgt"})
    warnings = orchestrator._restore_large_semantic_models(ctx, "analytics")

    assert fake.storage_calls == []
    assert len(warnings) == 1
    # Names the item and the region, and points at the fix.
    assert "'sm_sales'" in warnings[0]
    assert "nowhereland" in warnings[0]
    assert "does not support large semantic models" in warnings[0]
    assert "Re-run the migration" in warnings[0]


def test_a_failure_setting_the_mode_warns_but_does_not_raise(monkeypatch):
    fake = FakePowerBi([large("sm_sales", "sm-src")], fail_on_target="sm-tgt")
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", fake)

    ctx = make_ctx(id_map={"sm-src": "sm-tgt"})
    warnings = orchestrator._restore_large_semantic_models(ctx, "analytics")

    assert fake.storage_calls == []
    assert len(warnings) == 1
    assert "'sm_sales'" in warnings[0]
    assert "Re-enable large storage" in warnings[0]
    # The service's own words are repeated, not predicted.
    assert "HTTP 400: cannot enable large storage on sm-tgt" in warnings[0]


def test_a_failure_on_one_model_still_sets_the_others(monkeypatch):
    fake = FakePowerBi([large("Bad", "bad-src"), large("Good", "good-src")], fail_on_target="bad-tgt")
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", fake)

    ctx = make_ctx(id_map={"bad-src": "bad-tgt", "good-src": "good-tgt"})
    warnings = orchestrator._restore_large_semantic_models(ctx, "analytics")

    assert fake.storage_calls == [("ws-target", "good-tgt", powerbi.LARGE)]
    assert len(warnings) == 1
    assert "'Bad'" in warnings[0]


def test_failing_to_read_the_source_formats_warns_instead_of_raising(monkeypatch):
    fake = FakePowerBi([], fail_list=True)
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", fake)

    ctx = make_ctx(id_map={"sm-src": "sm-tgt"})
    warnings = orchestrator._restore_large_semantic_models(ctx, "analytics")

    assert fake.storage_calls == []
    assert len(warnings) == 1
    assert "Check each model's storage format by hand" in warnings[0]
    assert "HTTP 403" in warnings[0]


# ------------------------------------------------- wired into the rebuild phase


def _stub_analytics(monkeypatch, source_models, migrated_target="sm-tgt"):
    def fake_list_of_type(client, workspace_id, item_type):
        return source_models if item_type == analytics.SEMANTIC_MODEL else []

    def fake_migrate_items(client, *, items, item_type, id_map, **_kwargs):
        migrated = []
        for item in items:
            id_map[item["id"]] = migrated_target
            migrated.append(
                analytics.MigratedItem(
                    source_id=item["id"],
                    target_id=migrated_target,
                    name=item["displayName"],
                    rebound_parts=1,
                )
            )
        return migrated, []

    monkeypatch.setattr(orchestrator.analytics, "list_of_type", fake_list_of_type)
    monkeypatch.setattr(orchestrator.analytics, "migrate_items", fake_migrate_items)
    monkeypatch.setattr(orchestrator.relations, "topological_order", lambda ids, graph: list(ids))


def test_rebuild_phase_restores_large_format_and_succeeds(monkeypatch):
    source = [{"id": "sm-src", "displayName": "sm_sales", "type": "SemanticModel"}]
    _stub_analytics(monkeypatch, source)

    fake = FakePowerBi([large("sm_sales", "sm-src")])
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", fake)

    ctx = make_ctx(region="centralus")
    orchestrator._migrate_reports_and_models(ctx)

    assert fake.storage_calls == [("ws-target", "sm-tgt", powerbi.LARGE)]
    assert list(ctx.warnings) == []
    assert ctx.run.snapshot()["steps"][-1]["status"] == StepStatus.SUCCEEDED.value


def test_rebuild_phase_threads_the_downgrade_warning_from_an_unsupported_region(monkeypatch):
    source = [{"id": "sm-src", "displayName": "sm_sales", "type": "SemanticModel"}]
    _stub_analytics(monkeypatch, source)

    fake = FakePowerBi([large("sm_sales", "sm-src")])
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", fake)

    ctx = make_ctx(region="nowhereland")
    orchestrator._migrate_reports_and_models(ctx)

    assert fake.storage_calls == []
    assert any("does not support large semantic models" in w for w in ctx.warnings)


@pytest.mark.parametrize("region", ["centralus", "nowhereland"])
def test_no_semantic_models_never_touches_power_bi(monkeypatch, region):
    """Only reports, no models: the storage-format restore is not reached at all."""

    def fake_list_of_type(client, workspace_id, item_type):
        if item_type == analytics.REPORT:
            return [{"id": "rp-src", "displayName": "Sales", "type": "Report"}]
        return []

    def fake_migrate_items(client, *, items, item_type, id_map, **_kwargs):
        migrated = [
            analytics.MigratedItem(
                source_id=item["id"], target_id=f"{item['id']}-tgt", name=item["displayName"], rebound_parts=1
            )
            for item in items
        ]
        return migrated, []

    monkeypatch.setattr(orchestrator.analytics, "list_of_type", fake_list_of_type)
    monkeypatch.setattr(orchestrator.analytics, "migrate_items", fake_migrate_items)
    monkeypatch.setattr(orchestrator.relations, "topological_order", lambda ids, graph: list(ids))

    fake = FakePowerBi([])
    monkeypatch.setattr(orchestrator.powerbi, "PowerBiClient", fake)

    ctx = make_ctx(region=region)
    orchestrator._migrate_reports_and_models(ctx)

    # No models, so the source datasets were never even listed.
    assert fake.listed == []
    assert fake.storage_calls == []
