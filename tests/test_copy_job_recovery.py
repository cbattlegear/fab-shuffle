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
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_poll_seconds", 0)
    monkeypatch.setattr(
        copyjobs, "create_copy_job", lambda *a: creates.append(a) or {"id": "copy"}
    )
    monkeypatch.setattr(
        copyjobs, "_submit_copy_job",
        lambda *a: starts.append(a) or copyjobs._CopyJobSubmission("instance", 0),
    )

    def unavailable(client, job):
        # Admission of the remote run must be durable before the first progress read.
        replay = journal.read(ctx.journal.path)
        record = replay.copy_jobs[("lakehouse", "copy-lakehouse")]
        assert record["target"] == "target-lakehouse"
        assert record["job"]["instance_id"] == "instance"
        assert record["job"]["submission_state"] == "submitted"
        assert record["job"]["poll_not_before"] is not None
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
            kwargs["on_state"](copyjobs.CopyJobRun(
                workspace_id=candidate.workspace_id, copy_job_id=candidate.display_name,
                instance_id="instance", item_id=candidate.item_id,
                display_name=candidate.display_name, label=candidate.label, submission_state="submitted",
            ))
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


def test_unwritable_intent_journal_prevents_any_remote_mutation(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    ctx.journal.path = tmp_path / "missing-directory" / "attempt.jsonl"
    monkeypatch.setattr(
        copyjobs, "create_copy_job", lambda *a: pytest.fail("cannot create without durable intent"),
    )
    monkeypatch.setattr(
        copyjobs, "_submit_copy_job", lambda *a: pytest.fail("cannot submit without durable intent"),
    )
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        orchestrator._run_copy_jobs(ctx, "lakehouses", [spec()], "lakehouse")
    assert caught.value.active_jobs[0].copy_job_id is None
    assert caught.value.active_jobs[0].instance_id is None
    assert isinstance(caught.value.__cause__, OSError)


def test_accepted_job_ids_survive_a_failed_completion_checkpoint(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    created = []
    monkeypatch.setattr(copyjobs.time, "sleep", lambda _: None)
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_poll_seconds", 0)
    monkeypatch.setattr(copyjobs, "create_copy_job", lambda *a: created.append(a) or {"id": "copy"})
    monkeypatch.setattr(copyjobs, "_submit_copy_job", lambda *a: copyjobs._CopyJobSubmission("instance", 0))
    monkeypatch.setattr(copyjobs, "_job_status", lambda *a: ("Completed", {"status": "Completed"}))

    def failed_checkpoint(*args, **kwargs):
        raise OSError("data checkpoint disk full")

    monkeypatch.setattr(ctx.journal, "data", failed_checkpoint)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        orchestrator._run_copy_jobs(ctx, "lakehouses", [spec()], "lakehouse")
    assert caught.value.active_jobs[0].instance_id == "instance"
    replay = journal.read(ctx.journal.path)
    saved = replay.copy_jobs[("lakehouse", "copy-lakehouse")]["job"]
    assert saved["last_status"] == "Completed"
    assert saved["submission_state"] == "submitted"
    assert not replay.data_is_done("lakehouse", "tables")

    second = make_context(tmp_path, prior=replay)
    orchestrator._run_copy_jobs(second, "lakehouses", [spec()], "lakehouse")
    assert len(created) == 1
    final = journal.read(second.journal.path)
    assert final.copy_jobs == {}
    assert final.data_is_done("lakehouse", "tables")


def test_created_but_unsubmitted_job_is_started_without_recreation(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    ctx.journal.copy_job({
        "workspace_id": "scratch", "copy_job_id": "copy", "instance_id": None,
        "item_id": "lakehouse", "display_name": "copy-lakehouse", "label": "Lakehouse",
        "submission_state": "created", "poll_not_before": None,
    }, target_id="target-lakehouse")
    second = make_context(tmp_path, prior=journal.read(ctx.journal.path))
    monkeypatch.setattr(copyjobs.time, "sleep", lambda _: None)
    monkeypatch.setattr(copyjobs, "create_copy_job", lambda *a: pytest.fail("already created"))
    submissions = []
    monkeypatch.setattr(
        copyjobs, "_submit_copy_job",
        lambda *a: submissions.append(a) or copyjobs._CopyJobSubmission("instance", 0),
    )
    monkeypatch.setattr(copyjobs, "_job_status", lambda *a: ("Completed", {"status": "Completed"}))
    assert orchestrator._run_copy_jobs(second, "lakehouses", [spec()], "lakehouse") == []
    assert submissions == [(second.client, "scratch", "copy")]
    assert journal.read(second.journal.path).copy_jobs == {}


def test_preflight_failure_does_not_create_or_submit_a_job(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    monkeypatch.setattr(copyjobs, "create_copy_job", lambda *args: pytest.fail("must not create"))
    monkeypatch.setattr(copyjobs, "_submit_copy_job", lambda *args: pytest.fail("must not submit"))

    def reject(_spec):
        raise ValueError("destination is not empty")

    with pytest.raises(copyjobs.CopyJobBatchIncomplete, match=r"destination is not empty.*resume"):
        orchestrator._run_copy_jobs(ctx, "lakehouses", [spec()], "lakehouse", before_submit=reject)
    assert not journal.read(ctx.journal.path).copy_jobs


def test_preflight_failure_keeps_existing_unsubmitted_job(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    ctx.journal.copy_job({
        "workspace_id": "scratch", "copy_job_id": "copy", "instance_id": None,
        "item_id": "lakehouse", "display_name": "copy-lakehouse", "label": "Lakehouse",
        "submission_state": "created",
    }, target_id="target-lakehouse")
    second = make_context(tmp_path, prior=journal.read(ctx.journal.path))
    monkeypatch.setattr(copyjobs, "_submit_copy_job", lambda *args: pytest.fail("must not submit"))

    def reject(_spec):
        raise ValueError("destination is not empty")

    with pytest.raises(copyjobs.CopyJobBatchIncomplete, match=r"destination is not empty.*resume"):
        orchestrator._run_copy_jobs(second, "lakehouses", [spec()], "lakehouse", before_submit=reject)
    saved = journal.read(second.journal.path).copy_jobs[("lakehouse", "copy-lakehouse")]["job"]
    assert saved["submission_state"] == "created" and saved["instance_id"] is None


def test_failed_sql_job_records_target_bound_retry_before_clearing(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    sql_spec = copyjobs.CopyJobSpec(
        workspace_id="scratch", display_name="sql-copy", content={},
        label="SQL", item_id="lakehouse", fingerprint="definition",
    )
    monkeypatch.setattr(copyjobs.time, "sleep", lambda _: None)
    monkeypatch.setattr(copyjobs, "create_copy_job", lambda *args: {"id": "copy"})
    monkeypatch.setattr(
        copyjobs, "_submit_copy_job", lambda *args: copyjobs._CopyJobSubmission("instance", 0),
    )
    monkeypatch.setattr(copyjobs, "_job_status", lambda *args: ("Failed", {
        "status": "Failed", "failureReason": {"errorCode": "CopyFailed", "message": "failed"},
    }))
    warnings = orchestrator._run_copy_jobs(ctx, "sqldatabases", [sql_spec], "SQL database")
    replay = journal.read(ctx.journal.path)
    key = ("lakehouse", "sql_retry", "definition")
    assert key in replay.data_done
    assert replay.data_targets[key] == "target-lakehouse"
    assert not replay.copy_jobs
    assert warnings and "Failed" in warnings[0]


def test_deduped_sql_job_does_not_authorize_truncation(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    sql_spec = copyjobs.CopyJobSpec(
        workspace_id="scratch", display_name="sql-copy", content={},
        label="SQL", item_id="lakehouse", fingerprint="definition",
    )
    monkeypatch.setattr(copyjobs.time, "sleep", lambda _: None)
    monkeypatch.setattr(copyjobs, "create_copy_job", lambda *args: {"id": "copy"})
    monkeypatch.setattr(
        copyjobs, "_submit_copy_job", lambda *args: copyjobs._CopyJobSubmission("instance", 0),
    )
    monkeypatch.setattr(copyjobs, "_job_status", lambda *args: ("Deduped", {"status": "Deduped"}))
    with pytest.raises(copyjobs.CopyJobBatchIncomplete, match="Deduped"):
        orchestrator._run_copy_jobs(ctx, "sqldatabases", [sql_spec], "SQL database")
    saved = journal.read(ctx.journal.path)
    assert saved.copy_jobs
    assert ("lakehouse", "sql_retry", "definition") not in saved.data_done


def test_failed_job_is_repolled_if_terminal_checkpoint_is_interrupted(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    sql_spec = copyjobs.CopyJobSpec(
        workspace_id="scratch", display_name="sql-copy", content={},
        label="SQL", item_id="lakehouse", fingerprint="definition",
    )
    monkeypatch.setattr(copyjobs.time, "sleep", lambda _: None)
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_poll_seconds", 0)
    monkeypatch.setattr(copyjobs, "create_copy_job", lambda *args: {"id": "copy"})
    monkeypatch.setattr(
        copyjobs, "_submit_copy_job", lambda *args: copyjobs._CopyJobSubmission("instance", 0),
    )
    monkeypatch.setattr(copyjobs, "_job_status", lambda *args: ("Failed", {"status": "Failed"}))
    write = ctx.journal.copy_job

    def interrupted(job, *, target_id, active=True):
        if not active:
            raise OSError("journal unavailable")
        write(job, target_id=target_id, active=active)

    monkeypatch.setattr(ctx.journal, "copy_job", interrupted)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete, match="journal unavailable"):
        orchestrator._run_copy_jobs(ctx, "sqldatabases", [sql_spec], "SQL database")
    saved = journal.read(ctx.journal.path)
    assert saved.copy_jobs and ("lakehouse", "sql_retry", "definition") in saved.data_done
    second = make_context(tmp_path, prior=saved)
    monkeypatch.setattr(copyjobs, "create_copy_job", lambda *args: pytest.fail("must reconcile first"))
    monkeypatch.setattr(copyjobs, "_submit_copy_job", lambda *args: pytest.fail("must not resubmit"))
    assert orchestrator._run_copy_jobs(second, "sqldatabases", [sql_spec], "SQL database")
    assert not journal.read(second.journal.path).copy_jobs


def test_running_changed_definition_is_polled_but_never_checkpointed(tmp_path, monkeypatch):
    ctx = make_context(tmp_path)
    ctx.journal.copy_job({
        "workspace_id": "scratch", "copy_job_id": "copy", "instance_id": "instance",
        "item_id": "lakehouse", "display_name": "copy-lakehouse", "label": "Lakehouse",
        "submission_state": "submitted", "fingerprint": "old",
    }, target_id="target-lakehouse")
    second = make_context(tmp_path, prior=journal.read(ctx.journal.path))
    changed = copyjobs.CopyJobSpec(
        workspace_id="scratch", display_name="copy-lakehouse", content={},
        label="Lakehouse", item_id="lakehouse", fingerprint="new",
    )
    polls = []
    monkeypatch.setattr(copyjobs.time, "sleep", lambda _: None)
    monkeypatch.setattr(copyjobs, "_job_status", lambda *a: (polls.append(1) or "Completed", {}))
    with pytest.raises(copyjobs.CopyJobBatchIncomplete, match="different or unknown"):
        orchestrator._run_copy_jobs(second, "lakehouses", [changed], "lakehouse")
    replay = journal.read(second.journal.path)
    assert polls and not replay.data_is_done("lakehouse", "tables")
    assert replay.copy_jobs[("lakehouse", "copy-lakehouse")]["job"]["fingerprint"] == "old"
