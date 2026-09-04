"""An append-only record of what a migration did, so it can be picked up again.

A rebuild can run for hours. Everything it knows lives in memory in
:class:`~fabshuffle.run.MigrationRun` and the orchestrator's context, so if the container stops
or a phase throws, the items already created in the target workspace are orphaned: there is no
record of what was built, and the only way forward is to delete the workspace and start over.

This is that record. One file per run, written as JSON Lines, appended to as the migration
goes rather than rewritten, so that a crash midway through a write costs at most the last line
and a truncated final line can be dropped on read. Replaying it rebuilds enough to carry on.

It holds ids, names and item types: the same things already on the screen. It never holds
credentials. Those stay in memory for the life of the session, which is why resuming always
asks the operator to sign in again.

The file deliberately lives outside the per-run scratch directory, because ``cleanup_run``
deletes that on success and a finished run's journal is what makes "retry the items that did
not migrate" possible.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Record kinds. Strings rather than an enum because they are written to disk and read back by
# a future version of this program, which should be able to skip a kind it does not know.
RUN = "run"
PHASE = "phase"
WORKSPACE = "workspace"
ITEM = "item"
MAPPING = "mapping"
DATA = "data"
DORMANT = "dormant"
WARNING = "warning"
FINISHED = "finished"


def _now() -> str:
    return datetime.now(UTC).isoformat()


class Journal:
    """Appends one run's records. Safe to write to from several threads.

    Bounded thread pools copy schemas and files, and the Copy Job poller runs alongside them,
    so more than one of them can finish something at the same moment.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _write(self, _kind: str, **fields: Any) -> None:
        record = {"t": _kind, "at": _now(), **fields}
        line = json.dumps(record, separators=(",", ":"), default=str)
        try:
            with self._lock, self.path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
                handle.flush()
        except OSError as error:
            # A journal that cannot be written must not take the migration down with it. The
            # run is still doing real work; it just becomes unresumable, which is where it
            # started.
            logger.warning("Could not write to the run journal at %s: %s", self.path, error)

    # ---------------------------------------------------------------- writing

    def run_created(self, plan: dict[str, Any], *, cleanup: bool) -> None:
        self._write(RUN, plan=plan, cleanup=cleanup)

    def phase_started(self, phase: str) -> None:
        self._write(PHASE, phase=phase, state="started")

    def phase_finished(self, phase: str) -> None:
        self._write(PHASE, phase=phase, state="finished")

    def workspace(self, role: str, workspace_id: str, name: str = "") -> None:
        self._write(WORKSPACE, role=role, id=workspace_id, name=name)

    def item(self, source_id: str, target_id: str, item_type: str, name: str) -> None:
        """One item created in the target workspace, and the mapping it establishes."""
        self._write(ITEM, source=source_id, target=target_id, type=item_type, name=name)

    def mapping(self, source: str, target: str) -> None:
        """An id_map entry that is not an item: an endpoint, a server name, a cluster URI."""
        self._write(MAPPING, source=source, target=target)

    def data(self, item_id: str, kind: str, key: str = "") -> None:
        """Data that has finished moving for an item, optionally one table or container."""
        self._write(DATA, item=item_id, kind=kind, key=key)

    def dormant(self, item_id: str, why: str) -> None:
        self._write(DORMANT, item=item_id, why=why)

    def warning(self, text: str) -> None:
        self._write(WARNING, text=text)

    def finished(self, status: str, error: str | None = None) -> None:
        self._write(FINISHED, status=status, error=error)


@dataclass
class Replay:
    """What a journal says happened, ready to be checked against the workspace itself."""

    run_id: str = ""
    created_at: str = ""
    plan: dict[str, Any] = field(default_factory=dict)
    cleanup: bool = True
    target_workspace_id: str = ""
    target_workspace_name: str = ""
    scratch_workspace_id: str = ""
    id_map: dict[str, str] = field(default_factory=dict)
    # Source item id -> what was created for it. A superset of the item entries in id_map,
    # carrying the type and name so a resume can say what it is skipping.
    items: dict[str, dict[str, str]] = field(default_factory=dict)
    # (item id, kind, key) for data that finished moving.
    data_done: set[tuple[str, str, str]] = field(default_factory=set)
    dormant: dict[str, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    phases_started: list[str] = field(default_factory=list)
    phases_finished: set[str] = field(default_factory=set)
    status: str = ""
    error: str | None = None
    # Lines that could not be read. A truncated last line is expected after a crash; anything
    # else is worth knowing about.
    damaged_lines: int = 0

    @property
    def finished(self) -> bool:
        return bool(self.status)

    @property
    def interrupted(self) -> bool:
        """Whether this run stopped without recording an ending."""
        return bool(self.plan) and not self.status

    def data_is_done(self, item_id: str, kind: str, key: str = "") -> bool:
        return (item_id, kind, key) in self.data_done


def read(path: Path) -> Replay:
    """Rebuild what a journal says, tolerating a file that stops mid-line."""
    replay = Replay(run_id=path.stem)
    for record in _records(path, replay):
        _apply(replay, record)
    return replay


def _records(path: Path, replay: Replay) -> Iterator[dict[str, Any]]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as error:
        logger.warning("Could not read the run journal at %s: %s", path, error)
        return
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except ValueError:
            # Expected at most once, on the final line, when the process died mid-write.
            replay.damaged_lines += 1
            continue
        if isinstance(record, dict):
            yield record


def _apply(replay: Replay, record: dict[str, Any]) -> None:
    kind = record.get("t")
    if kind == RUN:
        replay.plan = record.get("plan") or {}
        replay.cleanup = bool(record.get("cleanup", True))
        replay.created_at = str(record.get("at") or "")
    elif kind == PHASE:
        phase = str(record.get("phase") or "")
        if record.get("state") == "finished":
            replay.phases_finished.add(phase)
        elif phase not in replay.phases_started:
            replay.phases_started.append(phase)
    elif kind == WORKSPACE:
        if record.get("role") == "scratch":
            replay.scratch_workspace_id = str(record.get("id") or "")
        else:
            replay.target_workspace_id = str(record.get("id") or "")
            replay.target_workspace_name = str(record.get("name") or "")
    elif kind == ITEM:
        source = str(record.get("source") or "")
        target = str(record.get("target") or "")
        if source and target:
            replay.id_map[source] = target
            replay.items[source] = {
                "target": target,
                "type": str(record.get("type") or ""),
                "name": str(record.get("name") or ""),
            }
    elif kind == MAPPING:
        source = str(record.get("source") or "")
        target = str(record.get("target") or "")
        if source and target:
            replay.id_map[source] = target
    elif kind == DATA:
        replay.data_done.add(
            (
                str(record.get("item") or ""),
                str(record.get("kind") or ""),
                str(record.get("key") or ""),
            )
        )
    elif kind == DORMANT:
        replay.dormant[str(record.get("item") or "")] = str(record.get("why") or "")
    elif kind == WARNING:
        text = str(record.get("text") or "")
        if text:
            replay.warnings.append(text)
    elif kind == FINISHED:
        replay.status = str(record.get("status") or "")
        replay.error = record.get("error")
    # An unknown kind is ignored on purpose, so an older build can read a newer journal.


def list_runs(directory: Path) -> list[Replay]:
    """Every journal in a directory, newest first, for offering a run back to the operator.

    Sorted on the recorded timestamp rather than the file name: a run id is a random uuid and
    says nothing about when it happened.
    """
    if not directory.is_dir():
        return []
    replays = [read(path) for path in directory.glob("*.jsonl")]
    replays.sort(key=lambda replay: replay.created_at, reverse=True)
    return replays


__all__ = [
    "DATA",
    "DORMANT",
    "FINISHED",
    "ITEM",
    "MAPPING",
    "PHASE",
    "RUN",
    "WARNING",
    "WORKSPACE",
    "Journal",
    "Replay",
    "list_runs",
    "read",
]
