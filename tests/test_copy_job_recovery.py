from __future__ import annotations

import pytest

from fabshuffle import journal, orchestrator
from fabshuffle.fabric import copyjobs
from fabshuffle.fabric.client import FabricApiError, FabricError
from fabshuffle.run import MigrationRun


def make_context(tmp_path, prior=None):
    book = journal.Journal(tmp_path / ("second.jsonl" if prior else "first.jsonl"))
    book.run_created({"source_workspace_id": "source"}, cleanup=False, prior=prior)
    return orchestrator._Context(
        client=object(), tokens=object(), principal=object(),
        plan=orchestrator.MigrationPlan(
            capacity_id="cap", capacity_name="F64", capacity_region="westus",
            source_workspace_id="source", source_workspace_name="source", target_workspace_name="target",
        ),
        run=MigrationRun(source_workspace_name="source", capacity_name="F64"),
        scratch_dir=tmp_path, target_workspace_id="target", journal=book,
        prior=prior, id_map={"lakehouse": "target-lakehouse"},
    )


def spec():
    return copyjobs.CopyJobSpec(
        workspace_id="scratch", display_name="copy-lakehouse", content={},
        label="Lakehouse", item_id="lakehouse",
    )


def test_lost_progress_is_durable_and_resume_polls_without_starting_again(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    creates, starts = [], []
    monkeypatch.setattr(copyjobs.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        copyjobs, "create_copy_job", lambda *a: creates.append(a) or {"id": "copy"}
    )
    monkeypatch.setattr(copyjobs, "start_copy_job", lambda *a: starts.append(a) or "instance")

    def unavailable(client, job):
        # Admission of the remote run must be durable before the first progress read.
        replay = journal.read(ctx.journal.path)
        record = replay.copy_jobs[("lakehouse", "copy-lakehouse")]
        assert record["target"] == "target-lakehouse"
        assert record["job"]["instance_id"] == "instance"
        raise FabricApiError("GET", "progress", 503, '{"errorCode":"CapacityUnavailable","message":"retry"}')

    monkeypatch.setattr(copyjobs, "_job_status", unavailable)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete):
        orchestrator._run_copy_jobs(ctx, "lakehouses", [spec()], "lakehouse")
    replay = journal.read(ctx.journal.path)
    assert "CapacityUnavailable" in replay.copy_jobs[("lakehouse", "copy-lakehouse")]["job"]["last_error"]
    assert not replay.data_is_done("lakehouse", "tables")
    assert len(creates) == len(starts) == 1

    second = make_context(tmp_path, prior=replay)
    monkeypatch.setattr(copyjobs, "_job_status", lambda *a: ("Completed", {"status": "Completed"}))
    assert orchestrator._run_copy_jobs(second, "lakehouses", [spec()], "lakehouse") == []
    final = journal.read(second.journal.path)
    assert final.copy_jobs == {}
    assert final.data_is_done("lakehouse", "tables")
    assert final.data_targets[("lakehouse", "tables", "")] == "target-lakehouse"
    assert len(creates) == len(starts) == 1


def test_unknown_submission_is_not_automatically_submitted_again(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    creates = []

    def uncertain(*args):
        creates.append(args)
        raise FabricError("Submission timed out")

    monkeypatch.setattr(copyjobs, "create_copy_job", uncertain)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete):
        orchestrator._run_copy_jobs(ctx, "lakehouses", [spec()], "lakehouse")
    replay = journal.read(ctx.journal.path)
    saved = replay.copy_jobs[("lakehouse", "copy-lakehouse")]["job"]
    assert saved["copy_job_id"] is None and saved["instance_id"] is None
    assert saved["last_error"] == "Submission timed out"
    second = make_context(tmp_path, prior=replay)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete):
        orchestrator._run_copy_jobs(second, "lakehouses", [spec()], "lakehouse")
    assert len(creates) == 1


def test_job_for_an_old_target_is_not_adopted_for_a_replacement(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    saved = {
        "workspace_id": "scratch", "copy_job_id": "copy", "instance_id": "instance",
        "item_id": "lakehouse", "display_name": "copy-lakehouse", "label": "Lakehouse",
    }
    ctx.journal.copy_job(saved, target_id="deleted-target")
    second = make_context(tmp_path, prior=journal.read(ctx.journal.path))
    monkeypatch.setattr(copyjobs, "run_copy_jobs", lambda *a, **k: pytest.fail("must not submit"))
    with pytest.raises(orchestrator.ResumeRefused, match="older target"):
        orchestrator._run_copy_jobs(second, "lakehouses", [spec()], "lakehouse")


def test_confirmed_failures_are_cleared_while_unresolved_jobs_are_preserved(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    other = copyjobs.CopyJobSpec(
        workspace_id="scratch", display_name="other", content={}, label="Other", item_id="other"
    )
    ctx.id_map["other"] = "target-other"

    def partial(client, specs, **kwargs):
        for candidate in specs:
            kwargs["on_started"](candidate, candidate.display_name, "instance")
        raise copyjobs.CopyJobBatchIncomplete(
            "progress unavailable",
            active_jobs=[copyjobs.CopyJobRun(
                workspace_id="scratch", copy_job_id="other", instance_id="instance",
                item_id="other", display_name="other", label="Other",
            )],
            not_started=[], created=[], warnings=["Lakehouse failed"],
            counts=copyjobs.CopyJobCounts(2, 0, 1, 0, 0, 1, 0),
        )

    monkeypatch.setattr(copyjobs, "run_copy_jobs", partial)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete):
        orchestrator._run_copy_jobs(ctx, "lakehouses", [spec(), other], "lakehouse")
    replay = journal.read(ctx.journal.path)
    assert set(replay.copy_jobs) == {("other", "other")}
    assert not replay.data_is_done("lakehouse", "tables")


def test_source_rename_still_adopts_its_outstanding_job(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    ctx.journal.copy_job({
        "workspace_id": "scratch", "copy_job_id": "copy", "instance_id": "instance",
        "item_id": "lakehouse", "display_name": "old-name", "label": "Old name",
    }, target_id="target-lakehouse")
    second = make_context(tmp_path, prior=journal.read(ctx.journal.path))
    monkeypatch.setattr(copyjobs.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        copyjobs, "create_copy_job", lambda *a: pytest.fail("renaming must not submit another copy"),
    )
    monkeypatch.setattr(copyjobs, "_job_status", lambda *a: ("Completed", {"status": "Completed"}))
    assert orchestrator._run_copy_jobs(second, "lakehouses", [spec()], "lakehouse") == []
    replay = journal.read(second.journal.path)
    assert replay.copy_jobs == {}
    assert replay.data_is_done("lakehouse", "tables")
