"""Incomplete advisory records must not hide strict recovery intent or certify readiness."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock

import pytest

from fabshuffle import journal
from fabshuffle.lifecycle import Disposition, EvidenceState, Lifecycle, readiness_report

UNCERTAIN = "Journal contains incomplete records; previous evidence is uncertain."


def copy_job(state):
    return {
        "workspace_id": "scratch", "copy_job_id": "copy", "instance_id": None,
        "item_id": "notebook", "display_name": "copy-notebook", "label": "Notebook",
        "submission_state": state, "poll_not_before": None,
    }


def ready_outcomes(book):
    lifecycle = Lifecycle(attempt_id=book.path.stem, record=book.outcome)
    for source in ("notebook", "other"):
        item = lifecycle.item(source, source.title(), "Notebook")
        item.resolve(f"target-{source}", "target-workspace", Disposition.CREATED)
        item.step("definition", EvidenceState.SUCCEEDED, "Definition copied.")
        item.step("rebind", EvidenceState.SUCCEEDED, "References repointed.")
        item.complete()
    return lifecycle.snapshot()


def report(replay):
    return readiness_report(
        replay.outcomes, run_id=replay.run_id, lineage_id=replay.lineage_id,
        run_status=replay.status, inventory_complete=replay.inventory_complete,
    )


def test_partial_advisory_write_cannot_swallow_a_later_strict_submission_intent(
    tmp_path, monkeypatch, caplog,
):
    book = journal.Journal(tmp_path / "first.jsonl")
    book.copy_job(copy_job("created"), target_id="target-notebook")
    original_open = Path.open

    @contextmanager
    def partial_open(path, *args, **kwargs):
        with original_open(path, *args, **kwargs) as handle:
            writer = Mock(wraps=handle)

            def partial_write(text):
                handle.write(text[:len(text) // 2])
                handle.flush()
                raise OSError("Injected advisory disk-full failure")

            writer.write.side_effect = partial_write
            yield writer

    with monkeypatch.context() as patch:
        patch.setattr(Path, "open", partial_open)
        book.outcome({"sourceId": "notebook", "name": "Notebook", "itemType": "Notebook"})
    assert "Injected advisory disk-full failure" in caplog.text
    assert not book.path.read_text(encoding="utf-8").endswith("\n")

    book.copy_job(copy_job("submitting"), target_id="target-notebook")
    replay = journal.read(book.path)
    saved = replay.copy_jobs[("notebook", "copy-notebook")]
    assert saved["job"]["submission_state"] == "submitting"
    assert saved["job"]["copy_job_id"] == "copy"
    assert saved["job"]["instance_id"] is None
    assert saved["target"] == "target-notebook"
    assert replay.damaged_lines == 1
    assert replay.data_done == set()


def test_framed_strict_append_still_flushes_before_fsync(tmp_path, monkeypatch):
    book = journal.Journal(tmp_path / "first.jsonl")
    original_open = Path.open
    original_fsync = journal.os.fsync
    events = []
    written = []

    @contextmanager
    def tracked_open(path, *args, **kwargs):
        with original_open(path, *args, **kwargs) as handle:
            writer = Mock(wraps=handle)

            def write(text):
                events.append("write")
                written.append(text)
                return handle.write(text)

            def flush():
                events.append("flush")
                handle.flush()

            writer.write.side_effect = write
            writer.flush.side_effect = flush
            yield writer

    def fsync(descriptor):
        events.append("fsync")
        original_fsync(descriptor)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "open", tracked_open)
        patch.setattr(journal.os, "fsync", fsync)
        book.copy_job(copy_job("created"), target_id="target-notebook")
        book.copy_job(copy_job("submitting"), target_id="target-notebook")

    assert events == ["write", "flush", "fsync"] * 2
    assert all(text.startswith("\n") and text.endswith("\n") for text in written)
    assert all(json.loads(text)["t"] == journal.COPY_JOB for text in written)
    assert journal.read(book.path).damaged_lines == 0


@pytest.mark.parametrize("failure_point", ["write", "flush", "fsync"])
def test_strict_append_still_propagates_write_flush_and_fsync_failures(
    tmp_path, monkeypatch, failure_point,
):
    book = journal.Journal(tmp_path / "first.jsonl")
    original_open = Path.open
    error = OSError(f"Injected {failure_point} failure")

    @contextmanager
    def failing_open(path, *args, **kwargs):
        with original_open(path, *args, **kwargs) as handle:
            writer = Mock(wraps=handle)
            if failure_point in ("write", "flush"):
                getattr(writer, failure_point).side_effect = error
            yield writer

    monkeypatch.setattr(Path, "open", failing_open)
    if failure_point == "fsync":
        monkeypatch.setattr(journal.os, "fsync", Mock(side_effect=error))
    with pytest.raises(OSError) as raised:
        book.copy_job(copy_job("submitting"), target_id="target-notebook")
    assert raised.value is error


@pytest.mark.parametrize("damage_position", ["before-outcomes", "after-outcomes"])
def test_damage_invalidates_all_readiness_but_not_recovery_checkpoints_or_phase_statuses(
    tmp_path, damage_position,
):
    book = journal.Journal(tmp_path / "first.jsonl")
    book.run_created({"source_workspace_id": "source-workspace"}, cleanup=False)
    book.workspace("target", "target-workspace", "Migrated")
    book.workspace("scratch", "scratch")
    book.phase_started("definitions")
    book.item("notebook", "target-notebook", "Notebook", "Notebook")
    book.item("other", "target-other", "Notebook", "Other")
    book.mapping("old-endpoint", "new-endpoint", owner="notebook")
    book.data("notebook", "files", target_id="target-notebook")
    book.copy_job(copy_job("submitting"), target_id="target-notebook")
    book.follower("follower", "target-follower", "notebook", "parent")
    book.dormant("mirror", "Start replication manually after verification.")
    book.warning("Check Orders in the target workspace.")
    if damage_position == "before-outcomes":
        with book.path.open("a", encoding="utf-8") as handle:
            handle.write('{"t":"outcome","item":')
    original = ready_outcomes(book)
    book.inventory()
    book.phase_finished("definitions")
    book.finished("succeeded")
    if damage_position == "after-outcomes":
        with book.path.open("a", encoding="utf-8") as handle:
            handle.write('{"t":"outcome","item":')
    original_bytes = book.path.read_bytes()

    replay = journal.read(book.path)
    assert replay.damaged_lines == 1
    assert replay.inventory_complete is False
    assert set(replay.outcomes) == set(original)
    for source, outcome in replay.outcomes.items():
        assert outcome.targetId == original[source].targetId
        assert outcome.disposition == original[source].disposition
        assert set(outcome.steps) == set(outcome.required)
        assert all(evidence.state == EvidenceState.UNKNOWN for evidence in outcome.steps.values())
        assert all(evidence.reason == UNCERTAIN for evidence in outcome.steps.values())
        assert all(evidence.attemptId == "first" for evidence in outcome.steps.values())
        assert all(evidence.targetId == outcome.targetId for evidence in outcome.steps.values())
        prior = original[source].record()
        prior.pop("history")
        assert prior in outcome.history
    assert report(replay)["state"] == "unknown"
    assert {item["state"] for item in report(replay)["items"]} == {"unknown"}
    assert replay.id_map == {
        "notebook": "target-notebook", "other": "target-other", "old-endpoint": "new-endpoint",
    }
    assert replay.mapping_owners == {"old-endpoint": "notebook"}
    assert replay.items["notebook"]["target"] == "target-notebook"
    assert replay.data_done == {("notebook", "files", "")}
    assert replay.data_targets == {("notebook", "files", ""): "target-notebook"}
    assert replay.copy_jobs[("notebook", "copy-notebook")]["job"] == copy_job("submitting")
    assert replay.follower_bindings == {
        "follower": {"target": "target-follower", "leader": "notebook", "parent": "parent"},
    }
    assert replay.target_workspace_id == "target-workspace"
    assert replay.scratch_workspace_id == "scratch"
    assert replay.dormant == {"mirror": "Start replication manually after verification."}
    assert replay.warnings == ["Check Orders in the target workspace."]
    assert replay.refresh_needed == set()
    assert replay.phases_started == ["definitions"]
    assert replay.phases_finished == {"definitions"}
    assert replay.status == "succeeded"
    assert replay.error is None
    assert journal.read(book.path) == replay
    assert book.path.read_bytes() == original_bytes


@pytest.mark.parametrize("fresh_inventory", [False, True])
def test_clean_inherited_attempts_keep_uncertainty_after_the_damaged_ancestor_is_removed(
    tmp_path, fresh_inventory,
):
    first = journal.Journal(tmp_path / "first.jsonl")
    first.run_created({"source_workspace_id": "source-workspace"}, cleanup=False)
    first.item("notebook", "target-notebook", "Notebook", "Notebook")
    first.data("notebook", "files", target_id="target-notebook")
    ready_outcomes(first)
    first.inventory()
    first.finished("failed", "Run interrupted.")
    with first.path.open("a", encoding="utf-8") as handle:
        handle.write('{"t":"outcome","item":')
    prior = journal.read(first.path)

    second = journal.Journal(tmp_path / "second.jsonl")
    second.run_created(prior.plan, cleanup=False, prior=prior)
    if fresh_inventory:
        second.inventory()
    second.finished("succeeded")
    first.path.unlink()
    inherited = journal.read(second.path)
    assert inherited.damaged_lines == 0
    assert inherited.inventory_complete is fresh_inventory
    assert inherited.attempts[0]["damaged_lines"] == 1
    assert inherited.outcomes == prior.outcomes
    assert all(
        evidence.state == EvidenceState.UNKNOWN and evidence.reason == UNCERTAIN
        for outcome in inherited.outcomes.values() for evidence in outcome.steps.values()
    )
    assert report(inherited)["state"] == "unknown"
    assert inherited.data_is_done("notebook", "files")

    third = journal.Journal(tmp_path / "third.jsonl")
    third.run_created(inherited.plan, cleanup=False, prior=inherited)
    second.path.unlink()
    latest = journal.read(third.path)
    assert latest.damaged_lines == 0
    assert latest.inventory_complete is fresh_inventory
    assert latest.outcomes == inherited.outcomes
    assert latest.lineage_id == "first"
    assert latest.ancestors == ["first", "second"]
    assert report(latest)["state"] == "unknown"
    assert latest.data_targets == {("notebook", "files", ""): "target-notebook"}


@pytest.mark.parametrize("malformed", [
    None,
    [],
    {"sourceId": "notebook", "steps": {"definition": None}},
    {"sourceId": "notebook", "steps": {"definition": []}},
    {"sourceId": "notebook", "steps": []},
    {"sourceId": "notebook", "steps": [{"state": "succeeded"}]},
], ids=["null-item", "list-item", "null-step", "list-step", "empty-list-steps", "list-steps"])
def test_malformed_outcome_shapes_count_as_damage_without_crashing_recovery(tmp_path, malformed):
    book = journal.Journal(tmp_path / "first.jsonl")
    book.run_created({"source_workspace_id": "source-workspace"}, cleanup=False)
    book.item("notebook", "target-notebook", "Notebook", "Notebook")
    book.data("notebook", "files", target_id="target-notebook")
    original = ready_outcomes(book)
    book.inventory()
    book.outcome(malformed)
    book.finished("succeeded")

    replay = journal.read(book.path)
    assert replay.damaged_lines == 1
    assert replay.inventory_complete is False
    assert set(replay.outcomes) == set(original)
    assert replay.data_is_done("notebook", "files")
    assert replay.id_map["notebook"] == "target-notebook"
    assert replay.status == "succeeded"
    assert report(replay)["state"] == "unknown"
    assert all(
        evidence.state == EvidenceState.UNKNOWN and evidence.reason == UNCERTAIN
        for outcome in replay.outcomes.values() for evidence in outcome.steps.values()
    )
