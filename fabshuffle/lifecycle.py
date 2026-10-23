"""Advisory migration evidence, not a second run-status machine.

Adapters record observations through ItemLifecycle. Neither a mapping nor a returned
create request proves that data was copied or that an item is running. The journal keeps
these small, payload-free records independently of the recovery checkpoints.
"""

from __future__ import annotations

import copy
import re
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, replace
from enum import StrEnum
from typing import Any


class EvidenceState(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    UNKNOWN = "unknown"
    # An operator-chosen deferral that is not a failure and not done: the work is still owed
    # and is reported as a next step, never as success. Journals written before this state
    # existed simply never contain it.
    PLANNED = "planned"


class Disposition(StrEnum):
    CREATED = "created"
    ADOPTED = "adopted"
    REFRESHED = "refreshed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class LifecycleContract:
    required: tuple[str, ...] = ("target", "definition", "rebind")
    data_kind: str = ""
    files_kind: str = ""


@dataclass(frozen=True)
class CopyOutcome:
    kind: str
    empty: bool | None = None


DEFINITION = LifecycleContract()
STORES = {
    "Lakehouse": LifecycleContract(
        ("target", "data", "files", "shortcuts", "endpoint", "schema"), "tables", "files"
    ),
    "Warehouse": LifecycleContract(("target", "schema", "data"), "tables"),
    "SQLDatabase": LifecycleContract(("target", "schema", "data"), "tables"),
    "KQLDatabase": LifecycleContract(
        ("target", "definition", "rebind", "data", "shortcuts"), "kql"
    ),
    "CosmosDBDatabase": LifecycleContract(("target", "definition", "rebind", "data"), "documents"),
    "ApacheAirflowJob": LifecycleContract(
        ("target", "definition", "rebind", "files"), files_kind="airflow-files"
    ),
    "Eventhouse": LifecycleContract(("target",)),
}


def contract_for(item_type: str) -> LifecycleContract:
    return STORES.get(item_type, DEFINITION)


def safe_text(value: str) -> str:
    """Keep service words, but not bearer tokens, connection secrets or echoed payloads."""
    value = re.sub(r"(?i)\bBearer\s+\S+", "Bearer [redacted]", value)
    value = re.sub(
        r"""(?ix)(["']?(?:password|pwd|client_secret|clientSecret|access_token|accessToken|
        token|sig|accountkey|sharedaccesssignature|payload)["']?\s*[:=]\s*)
        (?:"[^"]*"|'[^']*'|[^;\s&,}]+)""",
        r"\1[redacted]", value,
    )
    value = re.sub(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b", "[redacted]", value)
    value = re.sub(
        r"""(?is)(\b(?:definition(?:\s+sent)?|payload)["']?\s*[:=]).*""",
        r"\1 [redacted]", value,
    )
    return value[:2000]


@dataclass(frozen=True)
class Evidence:
    state: EvidenceState = EvidenceState.UNKNOWN
    reason: str = "No evidence recorded."
    action: str = "Resume to collect missing evidence, or inspect this work in the target workspace."
    errorCode: str = ""
    message: str = ""
    attemptId: str = ""
    targetId: str = ""
    # For PLANNED evidence: the source id of the item whose own planned follow-up this waits on.
    waitsOn: str = ""


@dataclass
class ItemOutcome:
    sourceId: str
    name: str
    itemType: str
    sourceWorkspaceId: str = ""
    targetWorkspaceId: str = ""
    targetId: str = ""
    disposition: Disposition = Disposition.UNKNOWN
    required: list[str] = field(default_factory=list)
    steps: dict[str, Evidence] = field(default_factory=dict)
    dependencies: list[str] = field(default_factory=list)
    unresolvedReferences: list[str] = field(default_factory=list)
    referenceGroups: dict[str, dict[str, list[str]]] = field(default_factory=dict)
    history: list[dict[str, Any]] = field(default_factory=list)

    def record(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> ItemOutcome:
        # Explicit allowlist: journal extensions must never become an accidental payload export.
        if not isinstance(record, Mapping):
            raise ValueError("An item outcome must be an object")
        for key in ("required", "dependencies", "unresolvedReferences", "history"):
            values = record.get(key, [])
            if not isinstance(values, list):
                raise ValueError(f"Outcome {key} must be a list")
            if key != "history" and any(not isinstance(value, str) for value in values):
                raise ValueError(f"Outcome {key} must contain strings")
        steps = record.get("steps", {})
        groups = record.get("referenceGroups", {})
        if not isinstance(steps, Mapping) or any(
            not isinstance(value, Mapping) for value in steps.values()
        ):
            raise ValueError("Outcome steps must contain evidence objects")
        if not isinstance(groups, Mapping) or any(
            not isinstance(group, Mapping) or any(
                not isinstance(group.get(key, []), list)
                or any(not isinstance(value, str) for value in group.get(key, []))
                for key in ("dependencies", "unresolved")
            ) for group in groups.values()
        ):
            raise ValueError("Outcome reference groups must contain lists of identifiers")
        result = cls(**{
            key: safe_text(str(record.get(key) or "")) for key in (
                "sourceId", "name", "itemType", "sourceWorkspaceId", "targetWorkspaceId", "targetId"
            )
        })
        result.disposition = Disposition(record.get("disposition", "unknown"))
        result.required = [safe_text(str(step)) for step in record.get("required") or []]
        result.steps = {
            safe_text(str(key)): Evidence(
                state=EvidenceState(value.get("state", "unknown")),
                **{field: safe_text(str(value.get(field) or "")) for field in (
                    "reason", "action", "errorCode", "message", "attemptId", "targetId", "waitsOn"
                )},
            )
            for key, value in (record.get("steps") or {}).items()
        }
        result.dependencies = [safe_text(str(value)) for value in record.get("dependencies") or []]
        result.unresolvedReferences = [
            safe_text(str(value)) for value in record.get("unresolvedReferences") or []
        ]
        result.referenceGroups = {
            str(scope): {
                key: [safe_text(str(value)) for value in group.get(key) or []]
                for key in ("dependencies", "unresolved")
            }
            for scope, group in (record.get("referenceGroups") or {}).items()
        }
        for previous in record.get("history") or []:
            historical = cls.from_record({**previous, "history": []}).record()
            historical.pop("history")
            result.history.append(historical)
        return result

    def invalidate(
        self, attempt_id: str, *, target_lost: bool,
        reason: str = "Target or dependency changed; previous evidence is stale.",
    ) -> None:
        previous = self.record()
        previous.pop("history")
        self.history.append(previous)
        if target_lost:
            self.targetId = ""
            self.disposition = Disposition.UNKNOWN
        # Even unchanged targets must not inherit successful copies after their dependencies
        # changed. Retain the observations in history, not as current readiness evidence.
        self.steps = {
            step: Evidence(
                reason=reason,
                action="Resume to refresh this item, then verify any unmeasured work.",
                attemptId=attempt_id, targetId=self.targetId,
            )
            for step in self.required
        }
        self.unresolvedReferences = []
        self.dependencies = []
        self.referenceGroups = {}


class Lifecycle:
    """Thread-safe evidence sink shared by definition and store adapters."""

    def __init__(
        self, *, attempt_id: str = "", source_workspace: str = "",
        record: Callable[[dict[str, Any]], None] | None = None,
        changed: Callable[[], None] | None = None,
        initial: Mapping[str, ItemOutcome] | None = None,
        secrets: tuple[str, ...] = (),
    ) -> None:
        self.attempt_id = attempt_id
        self.source_workspace = source_workspace
        self._record = record
        self._changed = changed
        self._items = copy.deepcopy(dict(initial or {}))
        for item in self._items.values():
            if item.steps and any(step.attemptId != attempt_id for step in item.steps.values()):
                previous = item.record()
                previous.pop("history")
                item.history.append(previous)
        self._lock = threading.RLock()
        self._secrets = tuple(secret for secret in secrets if secret)

    def _text(self, value: str) -> str:
        for secret in self._secrets:
            value = value.replace(secret, "[redacted]")
        return safe_text(value)

    def _save(self, item: ItemOutcome) -> None:
        if self._record:
            self._record(item.record())
        if self._changed:
            self._changed()

    def item(
        self, source_id: str, name: str, item_type: str, *,
        contract: LifecycleContract | None = None,
    ) -> ItemLifecycle:
        contract = contract or contract_for(item_type)
        with self._lock:
            if source_id not in self._items:
                self._items[source_id] = ItemOutcome(
                    source_id, self._text(name), item_type, self.source_workspace,
                    required=list(contract.required),
                )
                self._save(self._items[source_id])
        return ItemLifecycle(self, source_id)

    def invalidate(self, sources: Iterable[str], *, target_lost: bool = False) -> None:
        with self._lock:
            for source in sources:
                if source in self._items:
                    self._items[source].invalidate(self.attempt_id, target_lost=target_lost)
                    self._save(self._items[source])

    def snapshot(self) -> dict[str, ItemOutcome]:
        with self._lock:
            return copy.deepcopy(self._items)

    def get(self, source_id: str) -> ItemOutcome:
        with self._lock:
            return copy.deepcopy(self._items[source_id])


@dataclass(frozen=True)
class ItemLifecycle:
    owner: Lifecycle
    source_id: str

    def resolve(self, target_id: str, workspace_id: str, disposition: Disposition) -> None:
        with self.owner._lock:
            item = self.owner._items[self.source_id]
            if item.targetId and item.targetId != target_id:
                item.invalidate(self.owner.attempt_id, target_lost=True)
            item.targetId = target_id
            item.targetWorkspaceId = workspace_id
            item.disposition = disposition
            item.steps = {
                step: replace(evidence, targetId=target_id)
                if not evidence.targetId and evidence.attemptId == self.owner.attempt_id else evidence
                for step, evidence in item.steps.items()
            }
            self.step("target", EvidenceState.SUCCEEDED, f"Target {disposition.value}.")

    def references(
        self, dependencies: Iterable[str], unresolved: Iterable[str] = (), *, scope: str = "definition",
    ) -> None:
        with self.owner._lock:
            item = self.owner._items[self.source_id]
            if not item.referenceGroups and (item.dependencies or item.unresolvedReferences):
                item.referenceGroups["definition"] = {
                    "dependencies": item.dependencies, "unresolved": item.unresolvedReferences,
                }
            item.referenceGroups[scope] = {
                "dependencies": sorted(set(dependencies) - {self.source_id}),
                "unresolved": [self.owner._text(value) for value in unresolved],
            }
            item.dependencies = sorted({
                value for group in item.referenceGroups.values() for value in group["dependencies"]
            })
            item.unresolvedReferences = sorted({
                value for group in item.referenceGroups.values() for value in group["unresolved"]
            })
            self.owner._save(item)

    def add_references(self, dependencies: Iterable[str], unresolved: Iterable[str] = ()) -> None:
        with self.owner._lock:
            item = self.owner._items[self.source_id]
            self.references(
                [*item.dependencies, *dependencies],
                list(dict.fromkeys([*item.unresolvedReferences, *unresolved])),
            )

    def step(
        self, step: str, state: EvidenceState, reason: str, *, action: str = "",
        error: Exception | None = None, required: bool = True, waits_on: str = "",
    ) -> None:
        with self.owner._lock:
            item = self.owner._items[self.source_id]
            if required and step not in item.required:
                item.required.append(step)
            item.steps[step] = Evidence(
                state, self.owner._text(reason), self.owner._text(action),
                self.owner._text(str(getattr(error, "error_code", "") or "")),
                self.owner._text(str(getattr(error, "detail", "") or error or "")),
                self.owner.attempt_id, item.targetId, self.owner._text(waits_on),
            )
            self.owner._save(item)

    @contextmanager
    def operation(self, step: str, success: str | None) -> Iterator[None]:
        self.step(step, EvidenceState.UNKNOWN, "Operation started; completion not recorded.")
        try:
            yield
        except Exception as error:
            # Cancellation is not evidence that a remote operation failed.
            cancelled = type(error).__name__ == "CancelledError"
            self.step(
                step, EvidenceState.UNKNOWN if cancelled else EvidenceState.FAILED,
                "Operation interrupted." if cancelled else "Operation failed.",
                error=error, action="Review the reported failure and retry this item.",
            )
            raise
        else:
            if success is not None:
                self.step(step, EvidenceState.SUCCEEDED, success)

    def complete(self) -> None:
        self.step(
            "completion", EvidenceState.SUCCEEDED,
            "Adapter returned; required work and activation are evaluated separately.", required=False,
        )

    def copied(self, outcome: CopyOutcome) -> None:
        reason = (
            f"No copyable {outcome.kind} found." if outcome.empty
            else f"{outcome.kind} copy completed; no content reconciliation was performed."
        )
        self.step(
            "files" if outcome.kind in ("files", "airflow-files") else "data",
            EvidenceState.SUCCEEDED, reason,
        )


LIMITS = [
    "Advisory only: run completion is unchanged. Ready means recorded migration obligations "
    "succeeded, not that production cutover has been verified.",
    "No row-count/hash reconciliation, live activation checks, query execution, permission "
    "parity, source-change consistency or external dependency availability checks are performed "
    "by this report. It is not permission to delete the source.",
    "Evidence describes the recorded target incarnation at migration time. Changes made outside "
    "Fab Shuffle after that observation are not detected until recovery checks run.",
    "Unmeasured work and legacy journals remain unknown. Empty enumerations mean no copyable "
    "objects were found, not proof of zero rows or byte-for-byte equality.",
    "Planned follow-ups (for example a destination mirror left stopped on purpose) are listed once "
    "under next steps. Item dependencies are recorded per item, so this report does not trace which "
    "reports, models or notebooks read a specific waiting shortcut; verify those after the follow-up.",
]


def _planned_current(
    outcomes: Mapping[str, ItemOutcome], outcome: ItemOutcome, key: str, evidence: Evidence,
) -> bool:
    """A planned follow-up is only meaningful while the planned item still says so."""
    if key == "shortcuts":
        return any(
            _planned_current(outcomes, outcome, other, step)
            for other, step in outcome.steps.items()
            if other.startswith("shortcut:") and step.state == EvidenceState.PLANNED
            and step.targetId == outcome.targetId
        )
    if not evidence.waitsOn:
        return True
    gate = outcomes.get(evidence.waitsOn)
    gate_step = gate.steps.get("activation") if gate else None
    return bool(
        gate and gate_step and gate_step.state == EvidenceState.PLANNED
        and gate_step.targetId == gate.targetId
    )


def readiness_report(
    outcomes: Mapping[str, ItemOutcome], *, run_id: str, lineage_id: str,
    run_status: str, attempts: list[dict[str, Any]] | None = None,
    inventory_complete: bool = False,
) -> dict[str, Any]:
    items: dict[str, dict[str, Any]] = {}
    ranks = {"ready": 0, "next_steps": 1, "unknown": 2, "needs_attention": 3}
    next_steps: dict[tuple[str, str], dict[str, Any]] = {}
    waiting: list[tuple[str, dict[str, Any]]] = []
    for source, outcome in outcomes.items():
        steps = {**outcome.steps, **{
            key: outcome.steps.get(key, Evidence()) for key in outcome.required
        }}
        state = "ready" if outcome.required else "unknown"
        reasons: list[str] = [] if outcome.required else ["Lifecycle requirements were not recorded."]
        actions: list[str] = []
        for key, evidence in list(steps.items()):
            if evidence.targetId != outcome.targetId:
                evidence = replace(
                    evidence, state=EvidenceState.UNKNOWN,
                    reason="Evidence belongs to another target incarnation.",
                    action="Resume or verify this work against the current target.",
                )
                steps[key] = evidence
            if key not in outcome.required:
                continue
            if evidence.state == EvidenceState.PLANNED and not _planned_current(
                outcomes, outcome, key, evidence
            ):
                evidence = replace(
                    evidence, state=EvidenceState.UNKNOWN,
                    reason="The planned follow-up this depended on has changed since it was recorded.",
                    action="Retry this item to refresh the work that was waiting.",
                )
                steps[key] = evidence
            current = evidence.state
            if current in (EvidenceState.FAILED, EvidenceState.SKIPPED):
                state = "needs_attention"
            elif current == EvidenceState.UNKNOWN and ranks[state] < ranks["unknown"]:
                state = "unknown"
            elif current == EvidenceState.PLANNED and state == "ready":
                state = "next_steps"
            if current == EvidenceState.PLANNED and key != "shortcuts":
                entry = {
                    "sourceId": source, "name": outcome.name, "itemType": outcome.itemType,
                    "step": key, "reason": evidence.reason, "action": evidence.action,
                    "errorCode": evidence.errorCode, "message": evidence.message,
                }
                if evidence.waitsOn:
                    waiting.append((evidence.waitsOn, entry))
                else:
                    next_steps[(source, key)] = {**entry, "waiting": []}
            if current != EvidenceState.SUCCEEDED:
                reasons.append(f"{key}: {evidence.reason}")
                if evidence.action:
                    actions.append(evidence.action)
        if not outcome.targetId and ranks[state] < ranks["unknown"]:
            state = "unknown"
            reasons.append("No target identity recorded.")
        if outcome.unresolvedReferences:
            state = "needs_attention"
            reasons.append("Unresolved: " + ", ".join(outcome.unresolvedReferences))
            actions.append("Migrate or replace the named dependencies, then retry this item.")
        items[source] = {
            **outcome.record(), "state": state,
            "reasons": reasons, "actions": list(dict.fromkeys(actions)),
            "steps": [{"step": key, **asdict(value)} for key, value in steps.items()],
        }
    # Monotonic propagation also handles cycles, without recursive traversal of large graphs.
    dependents: dict[str, set[str]] = {}
    for source, outcome in outcomes.items():
        for dependency in outcome.dependencies:
            dependents.setdefault(dependency, set()).add(source)
    pending = list(items)
    for dependency in dependents.keys() - items.keys():
        pending.append(dependency)
    while pending:
        dependency = pending.pop()
        state = items.get(dependency, {}).get("state", "unknown")
        for source in dependents.get(dependency, ()):
            # A planned follow-up is owed by the item itself; depending on it does not show that
            # a dependent touches the waiting work, so it is not propagated.
            if state != "next_steps" and ranks[state] > ranks[items[source]["state"]]:
                items[source]["state"] = state
                pending.append(source)
    for source, outcome in outcomes.items():
        for dependency in outcome.dependencies:
            depended = items.get(dependency)
            if not depended or depended["state"] not in ("ready", "next_steps"):
                name = depended["name"] if depended else dependency
                items[source]["reasons"].append(f"Dependency '{name}' is not ready.")
                items[source]["actions"].append(f"Review readiness for '{name}' before cutover.")
        if not items[source]["reasons"]:
            items[source]["reasons"] = ["All required migration work has recorded success."]
    for gate, entry in waiting:
        owner = next_steps.get((gate, "activation"))
        if owner is not None:
            owner["waiting"].append(entry)
    for entry in next_steps.values():
        entry["waiting"].sort(key=lambda value: (
            value["name"].casefold(), value["sourceId"], value["step"]
        ))
        if entry["waiting"]:
            entry["action"] = (
                f"{entry['action']} Then retry the migration so the {len(entry['waiting'])} "
                "waiting shortcut(s) are recreated."
            ).strip()
    counts = {state: sum(item["state"] == state for item in items.values()) for state in ranks}
    state = (
        "needs_attention" if counts["needs_attention"] else
        "unknown" if counts["unknown"] or not inventory_complete else
        "next_steps" if counts["next_steps"] else "ready"
    )
    return {
        "schemaVersion": 2, "runId": run_id, "lineageId": lineage_id, "runStatus": run_status,
        "state": state, "counts": counts, "inventoryComplete": inventory_complete,
        "nextSteps": sorted(next_steps.values(), key=lambda entry: (
            entry["name"].casefold(), entry["sourceId"], entry["step"]
        )),
        "limits": [*LIMITS, *([] if inventory_complete else [
            "Source inventory was not recorded completely; the report may omit items."
        ])],
        "attempts": attempts or [],
        "items": sorted(items.values(), key=lambda item: (
            -ranks[item["state"]], item["name"].casefold(), item["sourceId"]
        )),
    }
