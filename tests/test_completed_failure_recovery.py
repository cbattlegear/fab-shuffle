"""Runtime handoff must keep successful runs with failed work discoverable for retry."""

from fabshuffle import journal
from fabshuffle.lifecycle import Disposition, EvidenceState, Lifecycle
from fabshuffle.run import MigrationRun, RunRegistry


def saved_run(directory, state):
    run = MigrationRun(source_workspace_name="Source", capacity_name="Destination")
    book = journal.Journal(directory / f"{run.id}.jsonl")
    book.run_created({
        "source_workspace_id": "source", "source_workspace_name": "Source",
        "target_workspace_name": "Destination", "capacity_id": "capacity",
        "capacity_name": "Destination", "capacity_region": "westus",
    }, cleanup=False)
    book.workspace("target", "target")
    book.item("lakehouse", "new-lakehouse", "Lakehouse", "Bronze")
    lifecycle = Lifecycle(attempt_id=run.id, source_workspace="source", record=book.outcome)
    item = lifecycle.item("lakehouse", "Bronze", "Lakehouse")
    item.resolve("new-lakehouse", "target", Disposition.CREATED)
    item.step("files", state, "File transfer result.")
    book.finished("succeeded")
    return run, book


def test_completed_failed_work_is_offered_after_registry_restart(tmp_path):
    run, _book = saved_run(tmp_path, EvidenceState.FAILED)
    recovered = RunRegistry().resumable(tmp_path)
    assert [entry.run_id for entry in recovered] == [run.id]
    assert recovered[0].status == "succeeded"
    assert recovered[0].id_map["lakehouse"] == "new-lakehouse"


def test_clean_or_manual_only_completions_are_not_added_as_failed_retries(tmp_path):
    for state in (EvidenceState.SUCCEEDED, EvidenceState.SKIPPED, EvidenceState.UNKNOWN):
        directory = tmp_path / state.value
        saved_run(directory, state)
        assert RunRegistry().resumable(directory) == []


def test_active_attempt_is_not_offered_even_if_its_journal_has_a_final_record(tmp_path):
    run, book = saved_run(tmp_path, EvidenceState.FAILED)
    run.plan = journal.read(book.path).plan
    run.target_workspace = {"id": "target"}
    run.mark_running()
    registry = RunRegistry()
    registry.add(run)
    assert registry.resumable(tmp_path) == []
