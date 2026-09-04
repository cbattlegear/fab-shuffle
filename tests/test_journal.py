"""The append-only record that makes a migration resumable.

A rebuild runs for hours and keeps everything in memory, so a container restart or a phase
throwing orphans whatever was already built. The journal is what lets a later run pick it up,
which means the things worth pinning here are: that it survives being cut off mid-write, that
it can be written from several threads at once, and that a future build can read it.
"""

from __future__ import annotations

import json
import threading

from fabshuffle import journal


def write_sample(path):
    j = journal.Journal(path)
    j.run_created({"sourceWorkspaceId": "src", "targetWorkspaceName": "Sales-westus"}, cleanup=True)
    j.workspace("target", "tgt-1", "Sales-westus")
    j.workspace("scratch", "scratch-1")
    j.phase_started("lakehouses")
    j.item("lh-src", "lh-tgt", "Lakehouse", "CloneTest")
    j.mapping("src.datawarehouse.fabric.microsoft.com", "dst.datawarehouse.fabric.microsoft.com")
    j.data("lh-src", "table", "dbo.Orders")
    j.dormant("mirror-src", "replication was left switched off")
    j.warning("Shortcut 'x' could not be created")
    j.phase_finished("lakehouses")
    return j


# ----------------------------------------------------------------- round trip


def test_what_went_in_comes_back_out(tmp_path):
    write_sample(tmp_path / "run.jsonl")
    replay = journal.read(tmp_path / "run.jsonl")

    assert replay.plan["targetWorkspaceName"] == "Sales-westus"
    assert replay.target_workspace_id == "tgt-1"
    assert replay.target_workspace_name == "Sales-westus"
    assert replay.scratch_workspace_id == "scratch-1"
    assert replay.id_map["lh-src"] == "lh-tgt"
    assert replay.items["lh-src"]["name"] == "CloneTest"
    assert replay.items["lh-src"]["type"] == "Lakehouse"
    assert replay.dormant["mirror-src"] == "replication was left switched off"
    assert replay.warnings == ["Shortcut 'x' could not be created"]
    assert replay.phases_finished == {"lakehouses"}


def test_an_item_is_a_mapping_too(tmp_path):
    """Later phases rewrite definitions through id_map, so a created item has to land there."""
    j = journal.Journal(tmp_path / "run.jsonl")
    j.item("a", "b", "Notebook", "Ingest")

    assert journal.read(tmp_path / "run.jsonl").id_map == {"a": "b"}


def test_a_mapping_that_is_not_an_item_is_kept_apart(tmp_path):
    """id_map also holds endpoints, server names and cluster URIs, which are not items.

    They belong in id_map so definitions rewrite through them, but they must not look like
    something a resume could skip creating.
    """
    j = journal.Journal(tmp_path / "run.jsonl")
    j.mapping("src.fabric.com", "dst.fabric.com")

    replay = journal.read(tmp_path / "run.jsonl")
    assert replay.id_map == {"src.fabric.com": "dst.fabric.com"}
    assert replay.items == {}


def test_finished_data_is_remembered_per_table(tmp_path):
    j = journal.Journal(tmp_path / "run.jsonl")
    j.data("db-1", "table", "dbo.Orders")
    j.data("db-1", "table", "dbo.Customers")

    replay = journal.read(tmp_path / "run.jsonl")
    assert replay.data_is_done("db-1", "table", "dbo.Orders")
    assert replay.data_is_done("db-1", "table", "dbo.Customers")
    assert not replay.data_is_done("db-1", "table", "dbo.Invoices")
    assert not replay.data_is_done("db-2", "table", "dbo.Orders")


def test_data_without_a_key_covers_the_whole_item(tmp_path):
    """Files and documents move as one unit; there is no per-table key to record."""
    j = journal.Journal(tmp_path / "run.jsonl")
    j.data("lh-1", "files")

    assert journal.read(tmp_path / "run.jsonl").data_is_done("lh-1", "files")


# ------------------------------------------------------- surviving a crash


def test_a_line_cut_off_mid_write_is_dropped_not_fatal(tmp_path):
    """The expected shape of a crash: the process died partway through the last append."""
    path = tmp_path / "run.jsonl"
    write_sample(path)
    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"t":"item","source":"half-w')

    replay = journal.read(path)

    assert replay.damaged_lines == 1
    # Everything written before the crash is still there.
    assert replay.id_map["lh-src"] == "lh-tgt"
    assert replay.items["lh-src"]["name"] == "CloneTest"


def test_a_run_that_never_recorded_an_ending_is_interrupted(tmp_path):
    write_sample(tmp_path / "run.jsonl")
    replay = journal.read(tmp_path / "run.jsonl")

    assert replay.interrupted
    assert not replay.finished


def test_a_run_that_recorded_an_ending_is_not_interrupted(tmp_path):
    j = write_sample(tmp_path / "run.jsonl")
    j.finished("succeeded")

    replay = journal.read(tmp_path / "run.jsonl")
    assert replay.finished
    assert not replay.interrupted
    assert replay.status == "succeeded"


def test_a_failure_keeps_the_reason(tmp_path):
    j = write_sample(tmp_path / "run.jsonl")
    j.finished("failed", "the capacity went away")

    replay = journal.read(tmp_path / "run.jsonl")
    assert replay.status == "failed"
    assert replay.error == "the capacity went away"


def test_an_empty_or_missing_journal_is_not_an_error(tmp_path):
    assert journal.read(tmp_path / "nothing.jsonl").plan == {}
    (tmp_path / "empty.jsonl").write_text("", encoding="utf-8")
    assert not journal.read(tmp_path / "empty.jsonl").interrupted


def test_a_journal_that_cannot_be_written_does_not_stop_the_migration(tmp_path):
    """Losing the ability to resume is bad. Taking down a running migration for it is worse."""
    unwritable = tmp_path / "sub" / "run.jsonl"
    j = journal.Journal(unwritable)
    unwritable.parent.rmdir()

    # No raise.
    j.item("a", "b", "Notebook", "Ingest")


# -------------------------------------------------------------- compatibility


def test_a_record_kind_we_do_not_know_is_skipped(tmp_path):
    """An older build must be able to read a journal a newer one wrote."""
    path = tmp_path / "run.jsonl"
    write_sample(path)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"t": "something-new", "detail": "from a later version"}) + "\n")
        handle.write(json.dumps({"t": "item", "source": "n", "target": "m", "type": "", "name": ""}) + "\n")

    replay = journal.read(path)

    assert replay.damaged_lines == 0
    assert replay.id_map["n"] == "m"


# ------------------------------------------------------------------- threads


def test_several_threads_can_record_at_once(tmp_path):
    """Bounded pools copy schemas and files while the Copy Job poller runs beside them."""
    path = tmp_path / "run.jsonl"
    j = journal.Journal(path)

    def record(n):
        for i in range(20):
            j.item(f"s{n}-{i}", f"t{n}-{i}", "Notebook", f"N{n}-{i}")

    threads = [threading.Thread(target=record, args=(n,)) for n in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    replay = journal.read(path)
    assert replay.damaged_lines == 0
    assert len(replay.id_map) == 120


# ------------------------------------------------------- the recording containers


def test_a_recording_map_reports_every_addition(tmp_path):
    seen = []
    id_map = journal.RecordingMap(lambda k, v: seen.append((k, v)))
    id_map["a"] = "b"

    assert id_map == {"a": "b"}
    assert seen == [("a", "b")]


def test_a_recording_map_reports_an_update_too(tmp_path):
    """dict.update does not go through __setitem__, and two callers add a whole map at once.

    Getting this wrong loses the folder and Spark pool mappings, which a resume needs to place
    items and to attach environments to their pool.
    """
    seen = []
    id_map = journal.RecordingMap(lambda k, v: seen.append((k, v)))
    id_map.update({"folder-a": "folder-1", "folder-b": "folder-2"})

    assert id_map == {"folder-a": "folder-1", "folder-b": "folder-2"}
    assert sorted(seen) == [("folder-a", "folder-1"), ("folder-b", "folder-2")]


def test_a_recording_list_reports_appends_and_extends():
    seen = []
    warnings = journal.RecordingList(seen.append)
    warnings.append("one")
    warnings.extend(["two", "three"])

    assert list(warnings) == ["one", "two", "three"]
    assert seen == ["one", "two", "three"]


def test_a_recording_map_wired_to_a_journal_survives_a_round_trip(tmp_path):
    j = journal.Journal(tmp_path / "run.jsonl")
    id_map = journal.RecordingMap(j.mapping)
    id_map["lh-src"] = "lh-tgt"
    id_map.update({"src.fabric.com": "dst.fabric.com"})

    assert journal.read(tmp_path / "run.jsonl").id_map == {
        "lh-src": "lh-tgt",
        "src.fabric.com": "dst.fabric.com",
    }


def test_a_discarding_journal_writes_nothing(tmp_path):
    """A preview, or a test with nothing to resume, should not leave a file behind."""
    journal.DISCARD.item("a", "b", "Notebook", "N")
    journal.DISCARD.finished("succeeded")

    assert list(tmp_path.iterdir()) == []
    assert journal.DISCARD.path is None


# --------------------------------------------------------------- listing runs


def test_runs_are_listed_newest_first(tmp_path):
    """A run id is a random uuid, so the order has to come from the recorded time."""
    for name, when in (("aaa", "2026-01-01T00:00:00+00:00"), ("zzz", "2026-06-01T00:00:00+00:00")):
        (tmp_path / f"{name}.jsonl").write_text(
            json.dumps({"t": "run", "at": when, "plan": {}}) + "\n", encoding="utf-8"
        )

    assert [replay.run_id for replay in journal.list_runs(tmp_path)] == ["zzz", "aaa"]


def test_listing_a_directory_that_does_not_exist_is_empty(tmp_path):
    assert journal.list_runs(tmp_path / "nope") == []
