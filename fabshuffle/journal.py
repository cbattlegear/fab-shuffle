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
import os
import threading
from collections.abc import Iterable, Iterator
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
INVALIDATED = "invalidated"
REFRESH = "refresh"
COPY_JOB = "copy_job"
FOLLOWER = "follower"

#: How many run journals to keep. They are small, but the directory sits on a volume that
#: outlives the container and nothing else ever removes them.
KEEP_JOURNALS = 100


def _now() -> str:
    return datetime.now(UTC).isoformat()


class Journal:
    """Appends one run's records. Safe to write to from several threads.

    Bounded thread pools copy schemas and files, and the Copy Job poller runs alongside them,
    so more than one of them can finish something at the same moment.
    """

    def __init__(self, path: Path | None) -> None:
        self.path = path
        self._lock = threading.Lock()
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)

    def _write(self, _kind: str, *, strict: bool = False, **fields: Any) -> None:
        if self.path is None:
            return
        record = {"t": _kind, "at": _now(), **fields}
        line = json.dumps(record, separators=(",", ":"), default=str)
        try:
            with self._lock, self.path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as error:
            if strict:
                raise
            # A journal that cannot be written must not take the migration down with it. The
            # run is still doing real work; it just becomes unresumable, which is where it
            # started.
            logger.warning("Could not write to the run journal at %s: %s", self.path, error)

    # ---------------------------------------------------------------- writing

    def run_created(
        self, plan: dict[str, Any], *, cleanup: bool, prior: Replay | None = None
    ) -> None:
        """Persist admission and its inherited state in one record, before starting work."""
        self._write(
            RUN, strict=True, plan=plan, cleanup=cleanup,
            lineage_id=(prior.lineage_id or prior.run_id) if prior else "",
            resumed_from=prior.run_id if prior else "",
            ancestors=[*prior.ancestors, prior.run_id] if prior else [],
            inherited=_state_records(prior) if prior else [],
            attempts=[
                *prior.attempts,
                {
                    "run_id": prior.run_id, "status": prior.status, "error": prior.error,
                    "last_phase": prior.phases_started[-1] if prior.phases_started else "",
                    "damaged_lines": prior.damaged_lines,
                },
            ] if prior else [],
        )

    def phase_started(self, phase: str) -> None:
        self._write(PHASE, phase=phase, state="started")

    def phase_finished(self, phase: str) -> None:
        self._write(PHASE, phase=phase, state="finished")

    def workspace(self, role: str, workspace_id: str, name: str = "") -> None:
        self._write(WORKSPACE, role=role, id=workspace_id, name=name)

    def item(self, source_id: str, target_id: str, item_type: str, name: str) -> None:
        """One item created in the target workspace, and the mapping it establishes."""
        self._write(ITEM, source=source_id, target=target_id, type=item_type, name=name)

    def mapping(self, source: str, target: str, *, owner: str = "") -> None:
        """An id_map entry that is not an item: an endpoint, a server name, a cluster URI."""
        self._write(MAPPING, source=source, target=target, owner=owner)

    def data(self, item_id: str, kind: str, key: str = "", *, target_id: str = "") -> None:
        """Data that has finished moving for an item, optionally one table or container."""
        self._write(DATA, item=item_id, kind=kind, key=key, target=target_id)

    def invalidate(self, sources: Iterable[str], *, refresh: Iterable[str] = ()) -> None:
        self._write(INVALIDATED, sources=list(sources), refresh=list(refresh), strict=True)

    def refresh(self, sources: Iterable[str], *, required: bool = True) -> None:
        self._write(REFRESH, sources=list(sources), required=required, strict=True)

    def copy_job(self, job: dict[str, Any], *, target_id: str, active: bool = True) -> None:
        self._write(COPY_JOB, job=job, target=target_id, active=active, strict=True)

    def follower(self, source: str, target: str, leader: str, parent: str) -> None:
        self._write(FOLLOWER, source=source, target=target, leader=leader, parent=parent, strict=True)

    def dormant(self, item_id: str, why: str) -> None:
        self._write(DORMANT, item=item_id, why=why)

    def warning(self, text: str) -> None:
        self._write(WARNING, text=text)

    def finished(self, status: str, error: str | None = None) -> None:
        self._write(FINISHED, status=status, error=error)


#: A journal that records nothing, for a preview or a test that has nothing to resume.
DISCARD = Journal(None)


class RecordingMap(dict):
    """A dict that reports every addition as it is made.

    The orchestrator adds to ``id_map`` in dozens of places, and to ``dormant`` in two. Doing
    the recording here rather than beside each of them means a new call site cannot forget,
    and forgetting would not fail: it would produce a resume that quietly rebinds an item to
    something that is not there any more.
    """

    def __init__(self, record: Any, initial: Any = None) -> None:
        # Seeded through dict's own constructor, which does not go through __setitem__: a
        # resumed run starts with what the journal replayed, and writing all of that back
        # would double the file every time it was picked up.
        super().__init__(initial or {})
        self._record = record

    def __setitem__(self, key: str, value: str) -> None:
        super().__setitem__(key, value)
        self._record(key, value)

    def update(self, other: Any = (), /, **extra: Any) -> None:
        # dict.update does not go through __setitem__, and two callers use it to add a whole
        # folder or pool map at once.
        for key, value in dict(other, **extra).items():
            self[key] = value


class RecordingList(list):
    """A list that reports everything added to it. Used for the run's warnings."""

    def __init__(self, record: Any, initial: Any = None) -> None:
        super().__init__(initial or [])
        self._record = record

    def append(self, item: Any) -> None:
        super().append(item)
        self._record(item)

    def extend(self, items: Any) -> None:
        for item in items:
            self.append(item)


@dataclass
class Replay:
    """What a journal says happened, ready to be checked against the workspace itself."""

    run_id: str = ""
    lineage_id: str = ""
    resumed_from: str = ""
    ancestors: list[str] = field(default_factory=list)
    attempts: list[dict[str, Any]] = field(default_factory=list)
    created_at: str = ""
    plan: dict[str, Any] = field(default_factory=dict)
    cleanup: bool = True
    target_workspace_id: str = ""
    target_workspace_name: str = ""
    scratch_workspace_id: str = ""
    id_map: dict[str, str] = field(default_factory=dict)
    mapping_owners: dict[str, str] = field(default_factory=dict)
    refresh_needed: set[str] = field(default_factory=set)
    copy_jobs: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    follower_bindings: dict[str, dict[str, str]] = field(default_factory=dict)
    # Source item id -> what was created for it. A superset of the item entries in id_map,
    # carrying the type and name so a resume can say what it is skipping.
    items: dict[str, dict[str, str]] = field(default_factory=dict)
    # (item id, kind, key) for data that finished moving.
    data_done: set[tuple[str, str, str]] = field(default_factory=set)
    data_targets: dict[tuple[str, str, str], str] = field(default_factory=dict)
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
        """Whether this attempt is interrupted or failed and worth offering for recovery."""
        return bool(self.plan) and self.status in ("", "failed")

    def data_is_done(self, item_id: str, kind: str, key: str = "") -> bool:
        return (item_id, kind, key) in self.data_done


def _state_records(replay: Replay) -> list[dict[str, Any]]:
    """A flat snapshot: another resume must not depend on an ancestor surviving retention."""
    records: list[dict[str, Any]] = [
        {"t": WORKSPACE, "role": "target", "id": replay.target_workspace_id,
         "name": replay.target_workspace_name},
        {"t": WORKSPACE, "role": "scratch", "id": replay.scratch_workspace_id},
    ]
    records.extend(
        {"t": ITEM, "source": source, **item} for source, item in replay.items.items()
    )
    records.extend(
        {"t": MAPPING, "source": source, "target": target,
         "owner": replay.mapping_owners.get(source, "")}
        for source, target in replay.id_map.items()
    )
    records.extend(
        {"t": DATA, "item": item, "kind": kind, "key": key,
         "target": replay.data_targets.get((item, kind, key), replay.id_map.get(item, ""))}
        for item, kind, key in sorted(replay.data_done)
    )
    records.extend({"t": DORMANT, "item": item, "why": why} for item, why in replay.dormant.items())
    records.extend({"t": WARNING, "text": text} for text in replay.warnings)
    records.append({"t": REFRESH, "sources": sorted(replay.refresh_needed), "required": True})
    records.extend({"t": COPY_JOB, **record} for record in replay.copy_jobs.values())
    records.extend(
        {"t": FOLLOWER, "source": source, **binding}
        for source, binding in replay.follower_bindings.items()
    )
    return records


def read(path: Path) -> Replay:
    """Rebuild what a journal says, tolerating a file that stops mid-line."""
    replay = Replay(run_id=path.stem, lineage_id=path.stem)
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
        replay.lineage_id = str(record.get("lineage_id") or replay.run_id)
        replay.resumed_from = str(record.get("resumed_from") or "")
        replay.ancestors = list(record.get("ancestors") or [])
        replay.attempts = list(record.get("attempts") or [])
        for inherited in record.get("inherited") or []:
            _apply(replay, inherited)
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
            if record.get("owner"):
                replay.mapping_owners[source] = str(record["owner"])
    elif kind == DATA:
        key = (
            str(record.get("item") or ""),
            str(record.get("kind") or ""),
            str(record.get("key") or ""),
        )
        replay.data_done.add(key)
        replay.data_targets[key] = str(record.get("target") or replay.id_map.get(key[0], ""))
    elif kind == INVALIDATED:
        sources = set(record.get("sources") or [])
        for source in sources:
            replay.id_map.pop(source, None)
            replay.items.pop(source, None)
            replay.mapping_owners.pop(source, None)
            replay.dormant.pop(source, None)
            replay.follower_bindings.pop(source, None)
        replay.data_done = {key for key in replay.data_done if key[0] not in sources}
        replay.data_targets = {
            key: target for key, target in replay.data_targets.items() if key[0] not in sources
        }
        replay.refresh_needed.update(record.get("refresh") or [])
    elif kind == REFRESH:
        sources = set(record.get("sources") or [])
        if record.get("required", True):
            replay.refresh_needed.update(sources)
        else:
            replay.refresh_needed.difference_update(sources)
    elif kind == COPY_JOB:
        job = record.get("job") or {}
        key = (str(job.get("item_id") or ""), str(job.get("display_name") or ""))
        if record.get("active", True):
            replay.copy_jobs[key] = {
                "job": job, "target": str(record.get("target") or ""), "active": True
            }
        else:
            replay.copy_jobs.pop(key, None)
    elif kind == FOLLOWER:
        replay.follower_bindings[str(record.get("source") or "")] = {
            key: str(record.get(key) or "") for key in ("target", "leader", "parent")
        }
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


def latest_runs(directory: Path) -> list[Replay]:
    replays = list_runs(directory)
    ancestors = {ancestor for replay in replays for ancestor in replay.ancestors}
    lineages: set[str] = set()
    targets: set[str] = set()
    latest = []
    for replay in replays:
        lineage = replay.lineage_id or replay.run_id
        target = replay.target_workspace_id
        if replay.run_id in ancestors or lineage in lineages or (target and target in targets):
            continue
        lineages.add(lineage)
        if target:
            targets.add(target)
        latest.append(replay)
    return latest


def prune(directory: Path, keep: int = KEEP_JOURNALS) -> int:
    """Delete the oldest journals once there are more than ``keep``. Returns how many went.

    They are small but they are never otherwise removed, and the directory is on a volume that
    outlives the container. A finished run's journal is kept rather than deleted on success,
    because retrying the items it left behind reads the same file. The latest recoverable
    attempts and unresolved remote jobs are protected even outside the normal retention limit.
    """
    if not directory.is_dir():
        return 0
    files = sorted(directory.glob("*.jsonl"), key=lambda path: path.stat().st_mtime, reverse=True)
    protected = {
        replay.run_id for replay in latest_runs(directory) if replay.interrupted or replay.copy_jobs
    }
    removed = 0
    for path in files[keep:]:
        if path.stem in protected:
            continue
        try:
            path.unlink()
            removed += 1
        except OSError as error:
            logger.warning("Could not remove the old run journal %s: %s", path, error)
    return removed


__all__ = [
    "DATA",
    "DISCARD",
    "DORMANT",
    "FINISHED",
    "ITEM",
    "KEEP_JOURNALS",
    "MAPPING",
    "PHASE",
    "RUN",
    "WARNING",
    "WORKSPACE",
    "Journal",
    "RecordingList",
    "RecordingMap",
    "Replay",
    "latest_runs",
    "list_runs",
    "prune",
    "read",
]
