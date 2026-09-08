from __future__ import annotations

import json
from copy import deepcopy

import pytest

from fabshuffle import journal
from fabshuffle.config import Settings
from fabshuffle.run import MigrationRun, RunConflict, RunRegistry, RunStatus

SOURCE_TENANT = "11111111-1111-1111-1111-111111111111"
TARGET_TENANT = "22222222-2222-2222-2222-222222222222"
SOURCE_APP = "33333333-3333-3333-3333-333333333333"
TARGET_APP = "44444444-4444-4444-4444-444444444444"
IDENTITY = {
    "source_tenant_id": SOURCE_TENANT, "target_tenant_id": TARGET_TENANT,
    "source_client_id": SOURCE_APP, "target_client_id": TARGET_APP,
}
PLAN = {
    **IDENTITY, "source_workspace_id": "source-workspace",
    "target_workspace_name": "Destination", "capacity_id": "destination-capacity",
    "strategy": "rebuild", "copy_permissions": False,
}


def new_run():
    return MigrationRun(source_workspace_name="Source", capacity_name="F64")


def write_run(directory, run_id="original", plan=None):
    book = journal.Journal(directory / f"{run_id}.jsonl")
    book.run_created(PLAN if plan is None else plan, cleanup=False)
    book.workspace("target", "target-workspace", "Destination")
    book.workspace("scratch", "scratch-workspace")
    book.item("source-item", "target-item", "Lakehouse", "Data")
    book.mapping("source-connection", "target-connection", owner="source-item")
    book.data("source-item", "table", "Orders", target_id="target-item")
    return book


def job(state="submitting"):
    return {
        "workspace_id": "scratch-workspace", "copy_job_id": "copy-job",
        "instance_id": None, "item_id": "source-item", "display_name": "Data",
        "submission_state": state, "poll_not_before": "2026-09-08T13:00:00+00:00",
    }


def runtime_run(book):
    replay = journal.read(book.path)
    run = new_run()
    run.id = replay.run_id
    run.lineage_id = replay.lineage_id
    run.plan = deepcopy(replay.plan)
    run.target_workspace = {"id": replay.target_workspace_id}
    run.scratch_workspace = {"id": replay.scratch_workspace_id}
    run.mark_finished(RunStatus.FAILED)
    return run


def test_settings_isolate_ordered_pairs_and_paired_same_tenant_from_legacy(tmp_path):
    settings = Settings(scratch_root=tmp_path)
    legacy = settings.journal_for("same-run")
    paired = settings.journal_for(
        "same-run", source_tenant_id=SOURCE_TENANT, target_tenant_id=TARGET_TENANT,
    )
    reversed_pair = settings.journal_for(
        "same-run", source_tenant_id=TARGET_TENANT, target_tenant_id=SOURCE_TENANT,
    )
    same_tenant = settings.journal_for(
        "same-run", source_tenant_id=SOURCE_TENANT, target_tenant_id=SOURCE_TENANT,
    )
    assert len({legacy, paired, reversed_pair, same_tenant}) == 4
    assert legacy.parent == settings.journal_dir
    assert "journal-paired-v1" in paired.parts
    assert not paired.is_relative_to(settings.journal_dir)
    assert settings.journal_dir_for_plan(PLAN) == paired.parent
    assert settings.journal_for_plan("same-run", PLAN) == paired
    assert settings.journal_for_plan("same-run", {}) == legacy
    write_run(paired.parent)
    write_run(same_tenant.parent, plan={**PLAN, "target_tenant_id": SOURCE_TENANT})
    assert journal.list_runs(settings.journal_dir) == []
    assert len(journal.list_runs(paired.parent)) == 1
    with pytest.raises(ValueError, match="both"):
        settings.journal_dir_for(source_tenant_id=SOURCE_TENANT)


def test_staging_budget_environment_is_read_when_settings_are_created(monkeypatch):
    monkeypatch.delenv("FAB_SHUFFLE_MAX_STAGING_BYTES", raising=False)
    assert Settings().max_staging_bytes == 1024 ** 3
    monkeypatch.setenv("FAB_SHUFFLE_MAX_STAGING_BYTES", "1024")
    assert Settings().max_staging_bytes == 1024
    monkeypatch.setenv("FAB_SHUFFLE_MAX_STAGING_BYTES", "not-a-number")
    assert Settings().max_staging_bytes == 1024 ** 3


@pytest.mark.parametrize("key", IDENTITY)
@pytest.mark.parametrize("replacement", ["", "a-different-identity"])
def test_recovery_requires_all_four_original_identities(key, replacement):
    expected = {**IDENTITY, key: replacement}
    with pytest.raises(journal.TenantBindingError):
        journal.validate_tenant_binding(PLAN, **expected)
    with pytest.raises(journal.TenantBindingError):
        journal.validate_tenant_binding({**PLAN, key: replacement}, **IDENTITY)


def test_swapped_pair_and_legacy_promotion_are_refused():
    with pytest.raises(journal.TenantBindingError):
        journal.validate_tenant_binding(
            PLAN, **{**IDENTITY, "source_tenant_id": TARGET_TENANT, "target_tenant_id": SOURCE_TENANT},
        )
    with pytest.raises(journal.TenantBindingError, match="cannot be interchanged"):
        journal.validate_tenant_binding(PLAN)
    with pytest.raises(journal.TenantBindingError, match="cannot be interchanged"):
        journal.validate_tenant_binding({"source_workspace_id": "source-workspace"}, **IDENTITY)
    assert journal.validate_tenant_binding({}) is False
    same = {**IDENTITY, "target_tenant_id": SOURCE_TENANT}
    assert journal.validate_tenant_binding({**PLAN, **same}, **same) is True


def test_journal_roundtrip_preserves_owned_resources_incarnations_and_ambiguous_jobs(tmp_path):
    first = write_run(tmp_path)
    first.copy_job(job(), target_id="target-item")
    first.follower("follower", "follower-target", "leader", "parent")
    first.refresh(["source-item"])
    first.item("source-item", "replacement-item", "Lakehouse", "Data")
    first.finished("failed", "Completion unknown")
    replay = journal.read(first.path)
    assert journal.validate_replay_binding(replay, **IDENTITY)
    assert replay.tenant_binding == IDENTITY
    assert replay.data_targets[("source-item", "table", "Orders")] == "target-item"
    assert replay.id_map["source-item"] == "replacement-item"
    assert replay.mapping_owners == {"source-connection": "source-item"}
    assert replay.owned_workspace_id("scratch", tenant_id=TARGET_TENANT, client_id=TARGET_APP) == (
        "scratch-workspace"
    )
    with pytest.raises(journal.TenantBindingError, match="another destination"):
        replay.owned_workspace_id("scratch", tenant_id=SOURCE_TENANT, client_id=SOURCE_APP)

    second = journal.Journal(tmp_path / "second.jsonl")
    second.run_created(PLAN, cleanup=False, prior=replay)
    first.path.unlink()
    resumed = journal.read(second.path)
    assert journal.validate_replay_binding(resumed, **IDENTITY)
    assert resumed.lineage_id == "original"
    assert resumed.ancestors == ["original"]
    assert resumed.items == replay.items
    assert resumed.data_targets == replay.data_targets
    assert resumed.copy_jobs == replay.copy_jobs
    assert resumed.follower_bindings == replay.follower_bindings
    assert resumed.refresh_needed == replay.refresh_needed
    assert resumed.workspace_owners == replay.workspace_owners
    assert resumed.attempts[0]["error"] == "Completion unknown"

    reopened = journal.Journal(second.path)
    reopened.copy_job(job("completed"), target_id="target-item", active=False)
    assert not journal.read(second.path).copy_jobs


@pytest.mark.parametrize("field,value", [
    ("source_tenant_id", TARGET_TENANT), ("target_client_id", SOURCE_APP),
    ("source_workspace_id", "other-source"), ("target_workspace_id", "other-target"),
])
def test_foreign_resource_ownership_cannot_be_replayed_or_used_to_clear_ambiguity(tmp_path, field, value):
    book = write_run(tmp_path)
    book.copy_job(job(), target_id="target-item")
    records = [json.loads(line) for line in book.path.read_text().splitlines() if line.strip()]
    forged = deepcopy(records[-1])
    forged["active"] = False
    forged["ownership"][field] = value
    with book.path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(forged) + "\n")
    replay = journal.read(book.path)
    assert replay.copy_jobs[("source-item", "Data")]["job"]["submission_state"] == "submitting"
    with pytest.raises(journal.TenantBindingError, match="ownership"):
        journal.validate_replay_binding(replay, **IDENTITY)
    assert RunRegistry().resumable(tmp_path, **IDENTITY) == []
    assert journal.prune(tmp_path, keep=0) == 0


def test_advisory_damage_does_not_discard_strict_tenant_or_submission_ownership(tmp_path):
    book = write_run(tmp_path)
    with book.path.open("a", encoding="utf-8") as handle:
        handle.write('{"t":"outcome","item":')
    book.copy_job(job(), target_id="target-item")
    replay = journal.read(book.path)
    assert journal.validate_replay_binding(replay, **IDENTITY)
    assert replay.damaged_lines == 1
    assert replay.copy_jobs[("source-item", "Data")]["job"] == job()
    assert replay.inventory_complete is False


def test_unversioned_qualified_journal_is_not_silently_adopted(tmp_path):
    path = tmp_path / "old.jsonl"
    path.write_text(json.dumps({"t": "run", "plan": PLAN}) + "\n", encoding="utf-8")
    replay = journal.read(path)
    with pytest.raises(journal.TenantBindingError, match="supported paired ownership"):
        journal.validate_replay_binding(replay, **IDENTITY)
    run = new_run()
    with pytest.raises(journal.TenantBindingError):
        RunRegistry().admit(run, directory=tmp_path, plan=PLAN, cleanup=False, prior=replay)
    assert not (tmp_path / f"{run.id}.jsonl").exists()


def test_a_paired_header_cannot_retroactively_qualify_legacy_resources(tmp_path):
    book = journal.Journal(tmp_path / "legacy-resources.jsonl")
    book.workspace("target", "legacy-workspace")
    book.run_created(PLAN, cleanup=False)
    with pytest.raises(journal.TenantBindingError, match="precede"):
        journal.validate_replay_binding(journal.read(book.path), **IDENTITY)


@pytest.mark.parametrize("key,value", [
    ("source_tenant_id", TARGET_TENANT), ("target_tenant_id", SOURCE_TENANT),
    ("source_client_id", TARGET_APP), ("target_client_id", SOURCE_APP),
    ("source_workspace_id", "other-source"), ("capacity_id", "other-capacity"),
    ("target_workspace_name", "Other destination"), ("strategy", "reassign"),
])
def test_admission_refuses_changed_binding_or_destination_before_claiming(tmp_path, key, value):
    replay = journal.read(write_run(tmp_path).path)
    run = new_run()
    registry = RunRegistry()
    with pytest.raises(journal.TenantBindingError):
        registry.admit(
            run, directory=tmp_path, plan={**PLAN, key: value}, cleanup=False, prior=replay,
        )
    assert not run.journal_started
    assert registry.get(run.id) is None
    assert not registry._claims
    assert not (tmp_path / f"{run.id}.jsonl").exists()


def test_admission_refuses_memory_workspace_tampering_and_wrong_directory(tmp_path):
    original = journal.read(write_run(tmp_path).path)
    changed = deepcopy(original)
    changed.target_workspace_id = "foreign-target"
    with pytest.raises(journal.TenantBindingError, match="workspaces differ"):
        RunRegistry().admit(new_run(), directory=tmp_path, plan=PLAN, cleanup=False, prior=changed)
    with pytest.raises(journal.TenantBindingError, match="missing"):
        RunRegistry().admit(
            new_run(), directory=tmp_path / "other-pair", plan=PLAN, cleanup=False, prior=original,
        )


def test_admission_keeps_an_independent_durable_authorization_plan_and_single_writer(tmp_path):
    original = journal.read(write_run(tmp_path).path)
    run = new_run()
    plan = deepcopy(PLAN)
    registry = RunRegistry()
    registry.admit(run, directory=tmp_path, plan=plan, cleanup=False, prior=original)
    plan["source_client_id"] = "changed"
    assert run.plan == PLAN
    assert journal.read(tmp_path / f"{run.id}.jsonl").plan == PLAN
    assert run.scratch_workspace["id"] == "scratch-workspace"
    with pytest.raises(RunConflict):
        registry.admit(new_run(), directory=tmp_path, plan=PLAN, cleanup=False, prior=original)
    assert registry.resumable(tmp_path, **IDENTITY) == []
    assert RunRegistry().resumable(tmp_path) == []
    assert RunRegistry().resumable(tmp_path, **{**IDENTITY, "target_client_id": SOURCE_APP}) == []
    assert [r.run_id for r in RunRegistry().resumable(tmp_path, **IDENTITY)] == [run.id]


def test_cleanup_rejects_missing_wrong_and_unrecorded_ownership_before_yield(tmp_path):
    book = write_run(tmp_path)
    run = runtime_run(book)
    registry = RunRegistry()
    effects = []
    for expected in ({}, {**IDENTITY, "target_tenant_id": SOURCE_TENANT}):
        with pytest.raises(journal.TenantBindingError):
            with registry.cleanup_claim(run, directory=tmp_path, **expected):
                effects.append("deleted")
    with pytest.raises(journal.TenantBindingError, match="global"):
        with registry.cleanup_claim(directory=tmp_path, **IDENTITY):
            effects.append("deleted")
    with pytest.raises(journal.TenantBindingError, match="global"):
        with registry.cleanup_claim(directory=tmp_path):
            effects.append("deleted")
    with pytest.raises(journal.TenantBindingError, match="directory"):
        with registry.cleanup_claim(run, **IDENTITY):
            effects.append("deleted")
    run.scratch_workspace["id"] = "foreign-scratch"
    with pytest.raises(journal.TenantBindingError, match="not owned"):
        with registry.cleanup_claim(run, directory=tmp_path, **IDENTITY):
            effects.append("deleted")
    assert effects == []
    assert not registry._cleaning


def test_cleanup_claim_guards_owned_scratch_and_durable_copy_job_ambiguity(tmp_path):
    book = write_run(tmp_path)
    run = runtime_run(book)
    registry = RunRegistry()
    with registry.cleanup_claim(run, directory=tmp_path, **IDENTITY):
        with pytest.raises(RunConflict, match="cleanup"):
            registry.admit(
                new_run(), directory=tmp_path, plan=PLAN, cleanup=False, prior=journal.read(book.path),
            )
    assert not registry._cleaning
    book.copy_job(job(), target_id="target-item")
    with pytest.raises(RunConflict, match="Unfinished Copy Jobs"):
        with RunRegistry().cleanup_claim(run, directory=tmp_path, **IDENTITY):
            pytest.fail("Ambiguous submission must be reconciled before cleanup.")
    journal.Journal(book.path).copy_job(job("completed"), target_id="target-item", active=False)
    with registry.cleanup_claim(run, directory=tmp_path, **IDENTITY):
        pass


def test_same_resource_ids_in_other_tenants_do_not_conflict_or_hide_recovery(tmp_path):
    other_plan = {**PLAN, "source_tenant_id": TARGET_TENANT, "target_tenant_id": SOURCE_TENANT}
    one = write_run(tmp_path, "one")
    two = write_run(tmp_path, "two", other_plan)
    assert {r.run_id for r in journal.latest_runs(tmp_path)} == {"one", "two"}
    registry = RunRegistry()
    active = new_run()
    registry.admit(active, directory=tmp_path, plan=PLAN, cleanup=False, prior=journal.read(one.path))
    other = runtime_run(two)
    other_identity = journal.tenant_binding(other_plan)
    assert [r.run_id for r in registry.resumable(tmp_path, **other_identity)] == ["two"]
    with registry.cleanup_claim(other, directory=tmp_path, **other_identity):
        pass
    with pytest.raises(RunConflict):
        with registry.cleanup_claim(directory=tmp_path):
            pytest.fail("Global cleanup still serializes against all tenants.")


def test_copy_job_checkpoint_cannot_claim_an_unrecorded_remote_workspace(tmp_path):
    book = write_run(tmp_path)
    with pytest.raises(journal.TenantBindingError, match="not owned"):
        book.copy_job({**job(), "workspace_id": "foreign-scratch"}, target_id="target-item")
    assert not journal.read(book.path).copy_jobs


def test_cleanup_detects_shared_scratch_resource_claims_even_between_otherwise_unrelated_runs(tmp_path):
    original = runtime_run(write_run(tmp_path))
    other_book = journal.Journal(tmp_path / "other.jsonl")
    other_book.run_created({**PLAN, "target_workspace_name": "Other target"}, cleanup=False)
    other_book.workspace("target", "other-target")
    other_book.workspace("scratch", "scratch-workspace")
    other = runtime_run(other_book)
    other.mark_running()
    registry = RunRegistry()
    registry.add(other)
    with pytest.raises(RunConflict, match="finish"):
        with registry.cleanup_claim(original, directory=tmp_path, **IDENTITY):
            pytest.fail("Shared scratch ownership must serialize cleanup.")
    other_book.copy_job(job(), target_id="other-item")
    with pytest.raises(RunConflict, match="Unfinished Copy Jobs"):
        with RunRegistry().cleanup_claim(original, directory=tmp_path, **IDENTITY):
            pytest.fail("Shared scratch jobs remain unsafe after a registry restart.")


def test_legacy_journal_recovery_and_cleanup_defaults_stay_compatible(tmp_path):
    legacy = {key: value for key, value in PLAN.items() if key not in IDENTITY}
    book = write_run(tmp_path, plan=legacy)
    run = new_run()
    registry = RunRegistry()
    registry.admit(run, directory=tmp_path, plan=legacy, cleanup=False, prior=journal.read(book.path))
    run.mark_finished(RunStatus.FAILED)
    registry.release(run.id)
    assert [r.run_id for r in registry.resumable(tmp_path)] == [run.id]
    with registry.cleanup_claim(run, directory=tmp_path):
        pass
