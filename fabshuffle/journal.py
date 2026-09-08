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
from collections.abc import Iterable, Iterator, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from fabshuffle.lifecycle import ItemOutcome

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
OUTCOME = "outcome"
INVENTORY = "inventory"
REFERENCE_BLOCK = "reference_block"

#: How many run journals to keep. They are small, but the directory sits on a volume that
#: outlives the container and nothing else ever removes them.
KEEP_JOURNALS = 100
PAIRED_JOURNAL_VERSION = 1
TENANT_BINDING_FIELDS = (
    "source_tenant_id", "target_tenant_id", "source_client_id", "target_client_id",
)
_OWNED_RECORDS = {WORKSPACE, ITEM, MAPPING, DATA, INVALIDATED, REFRESH, COPY_JOB, FOLLOWER, REFERENCE_BLOCK}


class TenantBindingError(ValueError):
    """Recovery cannot prove which tenant pair and applications own the recorded resources."""


def tenant_binding(plan: Mapping[str, Any]) -> dict[str, str]:
    """Return the complete paired identity, or an empty binding for legacy one-client plans."""
    binding = {}
    for key in TENANT_BINDING_FIELDS:
        value = plan.get(key, "")
        if not isinstance(value, str):
            raise TenantBindingError(f"The journal requires a string {key}.")
        binding[key] = value.strip().casefold()
    if not any(binding.values()):
        return {}
    missing = [key for key, value in binding.items() if not value]
    if missing:
        raise TenantBindingError(
            "Paired recovery requires both tenant IDs and both application IDs; missing "
            + ", ".join(missing) + ". Sign in with the original source and destination applications."
        )
    return binding


def validate_tenant_binding(
    plan: Mapping[str, Any], *, source_tenant_id: str = "", target_tenant_id: str = "",
    source_client_id: str = "", target_client_id: str = "",
) -> bool:
    """Require the same ordered identities, including paired use within a single tenant."""
    recorded = tenant_binding(plan)
    expected = tenant_binding({
        "source_tenant_id": source_tenant_id, "target_tenant_id": target_tenant_id,
        "source_client_id": source_client_id, "target_client_id": target_client_id,
    })
    if bool(recorded) != bool(expected):
        raise TenantBindingError(
            "Legacy one-principal runs and paired runs cannot be interchanged. "
            "Use this run's original source and destination sign-in mode."
        )
    for key, value in recorded.items():
        if value != expected[key]:
            raise TenantBindingError(
                f"The recorded {key} does not match the signed-in {key}. "
                "Sign in with the original source and destination tenants and applications."
            )
    return bool(recorded)


def validate_replay_binding(replay: Replay, **expected: str) -> bool:
    """Validate strict persisted ownership before consuming checkpoints or deleting resources."""
    paired = validate_tenant_binding(replay.plan, **expected)
    if replay.ownership_error:
        raise TenantBindingError(replay.ownership_error)
    if paired and (
        replay.binding_version != PAIRED_JOURNAL_VERSION
        or replay.tenant_binding != tenant_binding(replay.plan)
    ):
        raise TenantBindingError(
            "This journal has no supported paired ownership record. "
            "Do not adopt a legacy journal into a paired migration."
        )
    return paired


def validate_resume_plan(plan: Mapping[str, Any], prior: Replay) -> None:
    binding = tenant_binding(plan)
    validate_replay_binding(prior, **binding)
    if binding:
        for key in ("source_workspace_id", "capacity_id", "target_workspace_name", "strategy"):
            if plan.get(key, "") != prior.plan.get(key, ""):
                raise TenantBindingError(
                    f"The resumed {key} differs from the recorded migration. "
                    "Resume with the original source workspace and destination plan."
                )


def _removed_reference_record(plan: Mapping[str, Any], prior: Replay) -> dict[str, Any] | None:
    primary = {*prior.items, *prior.outcomes}
    for identifier in prior.id_map:
        if not prior.mapping_owners.get(identifier):
            try:
                UUID(identifier)
            except ValueError:
                continue
            primary.add(identifier)
    previous = {key.casefold() for key in (prior.plan.get("connection_mappings") or {})}
    current = {key.casefold() for key in (plan.get("connection_mappings") or {})}
    removed = {key: key for key in previous - current}
    fields = ("source_workspace_id", "source_item_id", "target_workspace_id", "target_item_id")
    before = {
        tuple(str(entry.get(key) or "").casefold() for key in fields)
        for entry in (prior.plan.get("reference_mappings") or [])
    }
    after = {
        tuple(str(entry.get(key) or "").casefold() for key in fields)
        for entry in (plan.get("reference_mappings") or [])
    }
    if before - after:
        preserved = {*primary, str(prior.plan.get("source_workspace_id") or "")}
        explicit = {value for entry in before - after for value in entry[:2]}
        removed.update({
            key: key for key in prior.id_map if key not in preserved or key.casefold() in explicit
        })
    if not removed:
        return None
    record: dict[str, Any] = {
        "t": REFERENCE_BLOCK, "references": removed, "invalidate": True,
        "refresh": sorted(primary),
    }
    if prior.tenant_binding:
        record["ownership"] = _ownership(prior)
    return record


def binding_scope(plan: Mapping[str, Any]) -> tuple[str, ...]:
    binding = tenant_binding(plan)
    return tuple(binding.get(key, "") for key in TENANT_BINDING_FIELDS)


def target_scope(plan: Mapping[str, Any], workspace_id: str) -> tuple[str, str]:
    return tenant_binding(plan).get("target_tenant_id", ""), workspace_id


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
        self._binding: dict[str, str] = {}
        self._source_workspace_id = ""
        self._workspaces: dict[str, str] = {}
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.is_file():
                replay = read(path)
                self._binding = tenant_binding(replay.plan)
                validate_replay_binding(replay, **self._binding)
                self._source_workspace_id = str(replay.plan.get("source_workspace_id") or "")
                self._workspaces = {
                    "target": replay.target_workspace_id, "scratch": replay.scratch_workspace_id,
                }

    def _write(self, _kind: str, *, strict: bool = False, **fields: Any) -> None:
        if self.path is None:
            return
        if self._binding and _kind in _OWNED_RECORDS:
            fields["ownership"] = {
                **self._binding, "source_workspace_id": self._source_workspace_id,
                "target_workspace_id": self._workspaces.get("target", ""),
                "scratch_workspace_id": self._workspaces.get("scratch", ""),
            }
            if _kind == WORKSPACE:
                fields["ownership"][f"{fields['role']}_workspace_id"] = fields["id"]
            elif _kind == COPY_JOB:
                workspace = fields["job"].get("workspace_id")
                if not workspace or workspace not in self._workspaces.values():
                    raise TenantBindingError(
                        "The Copy Job workspace is not owned by this run. "
                        "Reconcile the recorded destination scratch workspace before continuing."
                    )
        record = {"t": _kind, "at": _now(), **fields}
        line = json.dumps(record, separators=(",", ":"), default=str)
        try:
            with self._lock, self.path.open("a", encoding="utf-8") as handle:
                # A partial advisory write must not swallow the next strict recovery record.
                handle.write("\n" + line + "\n")
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
        binding = tenant_binding(plan)
        if prior is not None:
            validate_resume_plan(plan, prior)
        extra = (
            {"tenant_binding_version": PAIRED_JOURNAL_VERSION, "tenant_binding": binding}
            if binding else {}
        )
        inherited = _state_records(prior) if prior else []
        if prior is not None:
            removed = _removed_reference_record(plan, prior)
            if removed is not None:
                inherited.append(removed)
        self._write(
            RUN, strict=True, plan=plan, cleanup=cleanup,
            lineage_id=(prior.lineage_id or prior.run_id) if prior else "",
            resumed_from=prior.run_id if prior else "",
            ancestors=[*prior.ancestors, prior.run_id] if prior else [],
            inherited=inherited,
            attempts=[
                *prior.attempts,
                {
                    "run_id": prior.run_id, "status": prior.status, "error": prior.error,
                    "last_phase": prior.phases_started[-1] if prior.phases_started else "",
                    "damaged_lines": prior.damaged_lines,
                },
            ] if prior else [],
            **extra,
        )
        self._binding = binding
        self._source_workspace_id = str(plan.get("source_workspace_id") or "")
        if prior is not None:
            self._workspaces = {
                "target": prior.target_workspace_id, "scratch": prior.scratch_workspace_id,
            }

    def phase_started(self, phase: str) -> None:
        self._write(PHASE, phase=phase, state="started")

    def phase_finished(self, phase: str) -> None:
        self._write(PHASE, phase=phase, state="finished")

    def workspace(self, role: str, workspace_id: str, name: str = "") -> None:
        if self._binding and role not in ("target", "scratch"):
            raise TenantBindingError(
                "Paired journals record only target-owned target and scratch workspaces."
            )
        if self._binding and role == "target" and self._workspaces.get(role) not in (None, "", workspace_id):
            raise TenantBindingError("The recorded destination workspace cannot be replaced during recovery.")
        self._write(WORKSPACE, role=role, id=workspace_id, name=name, strict=True)
        self._workspaces[role] = workspace_id

    def item(self, source_id: str, target_id: str, item_type: str, name: str) -> None:
        """One item created in the target workspace, and the mapping it establishes."""
        self._write(ITEM, source=source_id, target=target_id, type=item_type, name=name, strict=True)

    def mapping(self, source: str, target: str, *, owner: str = "") -> None:
        """An id_map entry that is not an item: an endpoint, a server name, a cluster URI."""
        self._write(MAPPING, source=source, target=target, owner=owner, strict=True)

    def data(self, item_id: str, kind: str, key: str = "", *, target_id: str = "") -> None:
        """Data that has finished moving for an item, optionally one table or container."""
        self._write(DATA, item=item_id, kind=kind, key=key, target=target_id, strict=True)

    def invalidate(self, sources: Iterable[str], *, refresh: Iterable[str] = ()) -> None:
        self._write(INVALIDATED, sources=list(sources), refresh=list(refresh), strict=True)

    def block_references(self, references: Mapping[str, str], *, refresh: Iterable[str] = ()) -> None:
        self._write(
            REFERENCE_BLOCK, references=dict(references), invalidate=True,
            refresh=list(refresh), strict=True,
        )

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

    def outcome(self, record: dict[str, Any]) -> None:
        self._write(OUTCOME, item=record)

    def inventory(self) -> None:
        self._write(INVENTORY)


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
    tenant_binding: dict[str, str] = field(default_factory=dict)
    binding_version: int = 0
    ownership_error: str = ""
    workspace_owners: dict[str, dict[str, str]] = field(default_factory=dict)
    cleanup: bool = True
    target_workspace_id: str = ""
    target_workspace_name: str = ""
    scratch_workspace_id: str = ""
    id_map: dict[str, str] = field(default_factory=dict)
    mapping_owners: dict[str, str] = field(default_factory=dict)
    blocked_references: dict[str, str] = field(default_factory=dict)
    refresh_needed: set[str] = field(default_factory=set)
    copy_jobs: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    follower_bindings: dict[str, dict[str, str]] = field(default_factory=dict)
    outcomes: dict[str, ItemOutcome] = field(default_factory=dict)
    inventory_complete: bool = False
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

    def owned_workspace_id(self, role: str, *, tenant_id: str, client_id: str) -> str:
        """Return a workspace only when its durable destination ownership matches the caller."""
        validate_replay_binding(self, **self.tenant_binding)
        if not self.tenant_binding:
            raise TenantBindingError("Legacy journals do not prove paired workspace ownership.")
        owner = self.workspace_owners.get(role)
        if not owner:
            raise TenantBindingError(f"This run did not record ownership of a {role} workspace.")
        if (owner["tenant_id"], owner["client_id"]) != (
            tenant_id.strip().casefold(), client_id.strip().casefold(),
        ):
            raise TenantBindingError(
                f"The recorded {role} workspace belongs to another destination tenant or application."
            )
        return owner["id"]


def _ownership(replay: Replay) -> dict[str, str]:
    return {
        **replay.tenant_binding,
        "source_workspace_id": str(replay.plan.get("source_workspace_id") or ""),
        "target_workspace_id": replay.target_workspace_id,
        "scratch_workspace_id": replay.scratch_workspace_id,
    }


def _state_records(replay: Replay) -> list[dict[str, Any]]:
    """A flat snapshot: another resume must not depend on an ancestor surviving retention."""
    records: list[dict[str, Any]] = [
        {"t": WORKSPACE, "role": "target", "id": replay.target_workspace_id,
         "name": replay.target_workspace_name},
        {"t": WORKSPACE, "role": "scratch", "id": replay.scratch_workspace_id},
    ]
    if replay.blocked_references:
        records.append({
            "t": REFERENCE_BLOCK, "references": replay.blocked_references,
            "invalidate": False, "refresh": [],
        })
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
    records.extend({"t": OUTCOME, "item": item.record()} for item in replay.outcomes.values())
    if replay.inventory_complete:
        records.append({"t": INVENTORY})
    if replay.tenant_binding:
        for record in records:
            if record["t"] in _OWNED_RECORDS:
                record["ownership"] = _ownership(replay)
    return records


def read(path: Path) -> Replay:
    """Rebuild what a journal says, tolerating a file that stops mid-line."""
    replay = Replay(run_id=path.stem, lineage_id=path.stem)
    for record in _records(path, replay):
        _apply(replay, record)
    if replay.damaged_lines:
        replay.inventory_complete = False
        for outcome in replay.outcomes.values():
            outcome.invalidate(
                replay.run_id, target_lost=False,
                reason="Journal contains incomplete records; previous evidence is uncertain.",
            )
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
        plan = record.get("plan") or {}
        if not isinstance(plan, dict):
            replay.ownership_error = "The journal's migration plan is not a valid object."
            return
        try:
            binding = tenant_binding(plan)
        except TenantBindingError as error:
            replay.ownership_error = str(error)
            binding = {}
        if replay.plan and (
            replay.plan != plan or replay.tenant_binding != binding
        ):
            replay.ownership_error = "The journal contains conflicting admission identities."
            return
        if binding and not replay.plan and (
            replay.id_map or replay.target_workspace_id or replay.scratch_workspace_id
            or replay.copy_jobs or replay.data_done
        ):
            replay.ownership_error = (
                "Resource records precede this journal's paired admission. "
                "Do not adopt a legacy journal into a paired migration."
            )
        replay.plan = deepcopy(plan)
        replay.binding_version = record.get("tenant_binding_version", 0)
        replay.tenant_binding = binding
        if (binding or replay.binding_version or record.get("tenant_binding")) and (
            not binding or replay.binding_version != PAIRED_JOURNAL_VERSION
            or record.get("tenant_binding") != binding
        ):
            replay.ownership_error = (
                "The journal has no supported paired ownership record. "
                "Do not adopt a legacy journal into a paired migration."
            )
        replay.cleanup = bool(record.get("cleanup", True))
        replay.created_at = str(record.get("at") or "")
        replay.lineage_id = str(record.get("lineage_id") or replay.run_id)
        replay.resumed_from = str(record.get("resumed_from") or "")
        replay.ancestors = list(record.get("ancestors") or [])
        replay.attempts = list(record.get("attempts") or [])
        for inherited in record.get("inherited") or []:
            _apply(replay, inherited)
        return
    if kind in _OWNED_RECORDS and not _valid_ownership(replay, record):
        return
    if kind == PHASE:
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
        if replay.tenant_binding:
            replay.workspace_owners[record["role"]] = {
                "id": str(record.get("id") or ""),
                "tenant_id": replay.tenant_binding["target_tenant_id"],
                "client_id": replay.tenant_binding["target_client_id"],
            }
    elif kind == ITEM:
        source = str(record.get("source") or "")
        target = str(record.get("target") or "")
        if source and target:
            _retarget_outcome(replay, source, target)
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
            _retarget_outcome(replay, source, target)
            replay.id_map[source] = target
            if record.get("owner"):
                replay.mapping_owners[source] = str(record["owner"])
    elif kind == REFERENCE_BLOCK:
        references = record.get("references")
        if not isinstance(references, dict) or any(
            not isinstance(key, str) or not isinstance(value, str) for key, value in references.items()
        ):
            replay.ownership_error = "The journal's blocked source references cannot be read safely."
            return
        replay.blocked_references.update(references)
        if record.get("invalidate", True):
            removed = {key.casefold() for key in references}
            for key in list(replay.id_map):
                if key.casefold() in removed:
                    replay.id_map.pop(key, None)
                    replay.mapping_owners.pop(key, None)
            replay.refresh_needed.update(record.get("refresh") or [])
            for source in record.get("refresh") or []:
                if source in replay.outcomes:
                    replay.outcomes[source].invalidate(replay.run_id, target_lost=False)
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
        for source in sources | set(record.get("refresh") or []):
            if source in replay.outcomes:
                replay.outcomes[source].invalidate(replay.run_id, target_lost=source in sources)
    elif kind == REFRESH:
        sources = set(record.get("sources") or [])
        if record.get("required", True):
            replay.refresh_needed.update(sources)
            for source in sources:
                if source in replay.outcomes:
                    replay.outcomes[source].invalidate(replay.run_id, target_lost=False)
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
    elif kind == OUTCOME:
        try:
            outcome = ItemOutcome.from_record(record["item"])
        except (KeyError, TypeError, ValueError):
            replay.damaged_lines += 1
        else:
            replay.outcomes[outcome.sourceId] = outcome
    elif kind == INVENTORY:
        replay.inventory_complete = True
    # Unknown advisory kinds are ignored. Paired formats live outside legacy journal discovery.


def _valid_ownership(replay: Replay, record: dict[str, Any]) -> bool:
    owner = record.get("ownership")
    if not replay.tenant_binding and not owner:
        return True
    expected = _ownership(replay)
    kind = record["t"]
    valid = bool(replay.tenant_binding) and isinstance(owner, dict)
    if valid:
        valid = all(owner.get(key) == value for key, value in expected.items() if key not in (
            "target_workspace_id", "scratch_workspace_id",
        ))
    if valid and kind == WORKSPACE:
        role = record.get("role")
        valid = role in ("target", "scratch") and owner.get(f"{role}_workspace_id") == record.get("id")
        if role == "scratch" and replay.target_workspace_id:
            valid = valid and owner.get("target_workspace_id") == replay.target_workspace_id
        if role == "target" and replay.target_workspace_id:
            valid = valid and record.get("id") == replay.target_workspace_id
    elif valid:
        valid = all(owner.get(key) == expected[key] for key in (
            "target_workspace_id", "scratch_workspace_id",
        ))
        if kind == COPY_JOB:
            job = record.get("job") or {}
            workspace = job.get("workspace_id") if isinstance(job, dict) else ""
            valid = valid and bool(workspace) and workspace in (
                replay.target_workspace_id, replay.scratch_workspace_id,
            )
    if not valid:
        replay.ownership_error = (
            f"The journal's {kind} record has missing or mismatched tenant/workspace ownership. "
            "Reconcile the recorded resources with the original tenant pair before recovery or cleanup."
        )
    return valid


def _retarget_outcome(replay: Replay, source: str, target: str) -> None:
    outcome = replay.outcomes.get(source)
    if outcome and outcome.targetId and outcome.targetId != target:
        outcome.invalidate(replay.run_id, target_lost=True)
        outcome.targetId = target


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
    ancestors = {
        (binding_scope(replay.plan), ancestor)
        for replay in replays if not replay.ownership_error for ancestor in replay.ancestors
    }
    lineages: set[tuple[tuple[str, ...], str]] = set()
    targets: set[tuple[str, str]] = set()
    latest = []
    for replay in replays:
        if replay.ownership_error:
            latest.append(replay)
            continue
        scope = binding_scope(replay.plan)
        lineage = (scope, replay.lineage_id or replay.run_id)
        target = target_scope(replay.plan, replay.target_workspace_id)
        if (
            (scope, replay.run_id) in ancestors or lineage in lineages
            or (replay.target_workspace_id and target in targets)
        ):
            continue
        lineages.add(lineage)
        if replay.target_workspace_id:
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
        replay.run_id for replay in latest_runs(directory)
        if replay.interrupted or replay.copy_jobs or replay.ownership_error
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
    "TenantBindingError",
    "binding_scope",
    "latest_runs",
    "list_runs",
    "prune",
    "read",
    "target_scope",
    "tenant_binding",
    "validate_replay_binding",
    "validate_resume_plan",
    "validate_tenant_binding",
]
