"""A paired lakehouse copy must stage Delta preflight checkpoints under the run's own scratch
directory, not the server process's current working directory.

``copy_tree_streaming`` defaults ``scratch_dir`` to ``Path.cwd()`` when the caller passes
``None`` (see ``fabshuffle/transfer/delta.py``/``files.py``), which is only safe for a
short-lived CLI invocation. A long-running server process has no meaningful "current
directory" to stage temporary checkpoint files in, and doing so would leave
``delta-preflight-*`` directories wherever the process happened to be launched from instead of
inside the run-scoped ``scratch_dir`` that ``cleanup_run`` already knows how to remove. Both
paired ``copy_tree_streaming`` call sites in the orchestrator (Tables/ and Files/) must pass an
explicit ``scratch_dir`` under ``ctx.scratch_dir`` so staging is bounded to, and cleaned up
with, the rest of the run's local artifacts.
"""

from __future__ import annotations

from pathlib import Path

from fabshuffle import orchestrator
from fabshuffle.lifecycle import CopyOutcome

SOURCE_WS = "ws-source"
LAKEHOUSE_ID = "lh-1"

LAKEHOUSE = {
    "id": LAKEHOUSE_ID,
    "displayName": "Sales",
    "properties": {
        "oneLakeTablesPath": "https://onelake.dfs.fabric.microsoft.com/ws-source/lh-1/Tables",
        "oneLakeFilesPath": "https://onelake.dfs.fabric.microsoft.com/ws-source/lh-1/Files",
    },
}
TARGET = {
    "id": "lh-1-new",
    "displayName": "Sales",
    "properties": {
        "oneLakeTablesPath": "https://onelake.dfs.fabric.microsoft.com/ws-target/lh-1-new/Tables",
        "oneLakeFilesPath": "https://onelake.dfs.fabric.microsoft.com/ws-target/lh-1-new/Files",
    },
}


class FakeClient:
    """Reports no shortcuts; nothing else in these tests calls the client."""

    def list_all(self, path, params=None, value_key="value"):
        assert path.endswith("/shortcuts")
        return []


def make_ctx(tmp_path) -> orchestrator._Context:
    plan = orchestrator.MigrationPlan(
        capacity_id="cap",
        capacity_name="F64",
        capacity_region="westus",
        source_workspace_id=SOURCE_WS,
        source_workspace_name="src",
        target_workspace_name="dst",
        source_tenant_id="11111111-1111-1111-1111-111111111111",
        target_tenant_id="22222222-2222-2222-2222-222222222222",
        write_freeze_confirmed=True,
    )
    assert plan.paired
    return orchestrator._Context(
        client=FakeClient(),
        tokens=object(),
        principal=object(),
        plan=plan,
        run=orchestrator.MigrationRun(source_workspace_name="src", capacity_name="F64"),
        scratch_dir=tmp_path,
        target_tokens=object(),
    )


def test_paired_table_copy_stages_delta_preflight_under_the_run_scratch_dir(monkeypatch, tmp_path):
    ctx = make_ctx(tmp_path)
    captured = {}

    def fake_copy_tree_streaming(**kwargs):
        captured.update(kwargs)
        return CopyOutcome(kind="lakehouse")

    monkeypatch.setattr(orchestrator.file_transfer, "copy_tree_streaming", fake_copy_tree_streaming)

    warnings = orchestrator._copy_lakehouse_tables(ctx, "data", [(LAKEHOUSE, TARGET, False)])

    assert warnings == []
    assert captured["scratch_dir"] == tmp_path / f"delta-{LAKEHOUSE_ID}"
    # Never the implicit default: a live server has no meaningful working directory to stage in.
    assert captured["scratch_dir"] != Path.cwd()
    assert captured["max_memory_bytes"] == orchestrator.SETTINGS.max_memory_bytes
    assert captured["max_disk_staging_bytes"] == orchestrator.SETTINGS.max_disk_staging_bytes
    assert "max_staging_bytes" not in captured


def test_paired_files_copy_stages_delta_preflight_under_the_run_scratch_dir(monkeypatch, tmp_path):
    ctx = make_ctx(tmp_path)
    captured = {}

    def fake_copy_tree_streaming(**kwargs):
        captured.update(kwargs)
        return CopyOutcome(kind="files")

    monkeypatch.setattr(orchestrator.file_transfer, "copy_tree_streaming", fake_copy_tree_streaming)

    warnings = orchestrator._lakehouse_file_job(ctx, LAKEHOUSE, TARGET)()

    assert warnings == []
    assert captured["scratch_dir"] == tmp_path / f"delta-files-{LAKEHOUSE_ID}"
    assert captured["scratch_dir"] != Path.cwd()
    # Distinct from the Tables/ scratch subdirectory, so a table and a file copy for the same
    # lakehouse never contend over (or race the cleanup of) the same staging directory.
    assert captured["scratch_dir"] != tmp_path / f"delta-{LAKEHOUSE_ID}"
    assert captured["max_memory_bytes"] == orchestrator.SETTINGS.max_memory_bytes
    assert captured["max_disk_staging_bytes"] == orchestrator.SETTINGS.max_disk_staging_bytes
    assert "max_staging_bytes" not in captured
