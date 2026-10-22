"""A destination mirror left stopped on purpose is one next step, not a failed migration."""

from __future__ import annotations

import json

import pytest

from fabshuffle.fabric import shortcuts
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.lifecycle import (
    Disposition,
    EvidenceState,
    ItemOutcome,
    Lifecycle,
    readiness_report,
)

SOURCE_WS = "ws-source"
TARGET_WS = "ws-target"
MIRROR = "mirror-source"
LAKEHOUSE = "lakehouse-source"
SOURCE_ITEMS = {MIRROR: {"type": "MirroredDatabase", "displayName": "Orders mirror"}}
DORMANT = {MIRROR: "reports mirroring status Stopped in the destination."}


def api_error(code="BadRequest", message="Bad request.", status=400):
    return FabricApiError("POST", "shortcuts", status, json.dumps({"errorCode": code, "message": message}))


def onelake(name, item=MIRROR):
    return {
        "name": name, "path": "Tables",
        "target": {"oneLake": {"workspaceId": SOURCE_WS, "itemId": item, "path": f"Tables/{name}"}},
    }


class Client:
    def __init__(self, source, failures):
        self.source, self.failures, self.target, self.posted = list(source), failures, [], []

    def list_all(self, path):
        return list(self.source) if f"workspaces/{SOURCE_WS}/" in path else list(self.target)

    def get(self, path):
        raise api_error("ItemNotFound", "No shortcut found.", 404)

    def post(self, path, json=None, params=None):
        self.posted.append(json["name"])
        if error := self.failures.get(json["name"]):
            raise error
        self.target.append(dict(json))
        return {}


def mirror(owner, activation=EvidenceState.PLANNED):
    item = owner.item(MIRROR, "Orders mirror", "MirroredDatabase")
    item.resolve("mirror-target", TARGET_WS, Disposition.CREATED)
    item.step("activation", activation, "Not started.", action="Start the mirror.")
    item.step("definition", EvidenceState.SUCCEEDED, "Definition copied.")
    item.step("rebind", EvidenceState.SUCCEEDED, "Repointed.")
    return item


def lakehouse(owner):
    item = owner.item(LAKEHOUSE, "Lakehouse", "Lakehouse")
    item.resolve("lakehouse-target", TARGET_WS, Disposition.CREATED)
    for step in ("data", "files", "endpoint", "schema"):
        item.step(step, EvidenceState.SUCCEEDED, f"{step} done.")
    return item


def run(owner, client, **kwargs):
    return shortcuts.copy_shortcuts(
        client, SOURCE_WS, LAKEHOUSE, TARGET_WS, "lakehouse-target",
        {SOURCE_WS: TARGET_WS, MIRROR: "mirror-target"},
        source_items=SOURCE_ITEMS, lifecycle=owner.item(LAKEHOUSE, "Lakehouse", "Lakehouse"),
        dormant=DORMANT, **kwargs,
    )


def report(owner):
    return readiness_report(
        owner.snapshot(), run_id="run", lineage_id="lineage", run_status="succeeded",
        inventory_complete=True,
    )


def dependents(owner, count=15):
    for index in range(count):
        item = owner.item(f"notebook-{index}", f"Notebook {index}", "Notebook")
        item.resolve(f"notebook-target-{index}", TARGET_WS, Disposition.CREATED)
        item.step("definition", EvidenceState.SUCCEEDED, "Definition copied.")
        item.step("rebind", EvidenceState.SUCCEEDED, "Repointed.")
        item.references([LAKEHOUSE, MIRROR])


@pytest.fixture
def owner():
    return Lifecycle(attempt_id="attempt", source_workspace=SOURCE_WS)


def test_refused_shortcut_on_a_deliberately_unstarted_mirror_is_planned_with_service_words(owner):
    mirror(owner)
    lakehouse(owner)
    client = Client([onelake("A"), onelake("B")], {n: api_error("BadRequest", "Nope.") for n in "AB"})

    created, warnings = run(owner, client)

    assert created == 0 and warnings == []
    item = owner.get(LAKEHOUSE)
    step = item.steps["shortcut:Tables/A"]
    assert step.state == EvidenceState.PLANNED and step.waitsOn == MIRROR
    assert (step.errorCode, step.message) == ("BadRequest", "Nope.")
    assert "Nope." in step.reason
    assert item.steps["shortcuts"].state == EvidenceState.PLANNED
    assert item.steps["shortcuts"].errorCode == "BadRequest"


def test_unrelated_failure_alongside_planned_shortcut_still_needs_attention(owner):
    mirror(owner)
    lakehouse(owner)
    other = {"name": "Other", "path": "Files", "target": {"adlsGen2": {"connectionId": "c", "location": "x"}}}
    client = Client([onelake("A"), other], {"A": api_error(), "Other": api_error("Denied", "No.", 403)})

    _, warnings = run(owner, client)

    item = owner.get(LAKEHOUSE)
    assert item.steps["shortcut:Tables/A"].state == EvidenceState.PLANNED
    assert item.steps["shortcut:Files/Other"].state == EvidenceState.FAILED
    assert item.steps["shortcuts"].state == EvidenceState.FAILED
    assert item.steps["shortcuts"].errorCode == "Denied"
    assert len(warnings) == 1 and "Other" in warnings[0]


@pytest.mark.parametrize("activation", [EvidenceState.FAILED, EvidenceState.SUCCEEDED])
def test_requested_start_failure_or_started_mirror_keeps_shortcut_failures(owner, activation):
    mirror(owner, activation)
    lakehouse(owner)

    _, warnings = run(owner, Client([onelake("A")], {"A": api_error()}))

    assert len(warnings) == 1
    assert owner.get(LAKEHOUSE).steps["shortcut:Tables/A"].state == EvidenceState.FAILED
    assert owner.get(LAKEHOUSE).steps["shortcuts"].state == EvidenceState.FAILED


def test_planned_state_is_not_inferred_without_evidence_from_the_mirror(owner):
    lakehouse(owner)

    _, warnings = run(owner, Client([onelake("A")], {"A": api_error()}))

    assert len(warnings) == 1
    assert owner.get(LAKEHOUSE).steps["shortcut:Tables/A"].state == EvidenceState.FAILED


def test_retry_that_succeeds_replaces_the_planned_evidence(owner):
    mirror(owner)
    lakehouse(owner)
    client = Client([onelake("A")], {"A": api_error()})
    run(owner, client)
    client.failures.clear()

    run(owner, client)

    item = owner.get(LAKEHOUSE)
    assert item.steps["shortcut:Tables/A"].state == EvidenceState.SUCCEEDED
    assert item.steps["shortcuts"].state == EvidenceState.SUCCEEDED


def test_one_stopped_mirror_is_one_next_step_not_fifteen_broken_dependents(owner):
    mirror(owner)
    lakehouse(owner)
    for step in ("data", "files", "endpoint", "schema"):
        assert owner.get(LAKEHOUSE).steps[step].state == EvidenceState.SUCCEEDED
    run(owner, Client([onelake("A"), onelake("B")], {n: api_error() for n in "AB"}))
    owner.item(MIRROR, "", "").step("replication", EvidenceState.SUCCEEDED, "Checked.")
    dependents(owner)

    result = report(owner)

    assert result["counts"]["needs_attention"] == 0
    assert result["counts"]["next_steps"] == 2
    assert result["counts"]["ready"] == 15
    assert result["state"] == "next_steps"
    assert len(result["nextSteps"]) == 1
    entry = result["nextSteps"][0]
    assert entry["name"] == "Orders mirror" and entry["step"] == "activation"
    assert [w["name"] for w in entry["waiting"]] == ["Lakehouse", "Lakehouse"]
    assert {w["errorCode"] for w in entry["waiting"]} == {"BadRequest"}
    assert "recreated" in entry["action"]
    notebooks = [i for i in result["items"] if i["itemType"] == "Notebook"]
    assert all(not any("Dependency" in r for r in i["reasons"]) for i in notebooks)


def test_unrelated_shortcut_failure_still_propagates_to_dependents(owner):
    mirror(owner)
    lakehouse(owner)
    other = {"name": "Other", "path": "Files", "target": {"adlsGen2": {"connectionId": "c", "location": "x"}}}
    run(owner, Client([onelake("A"), other], {"A": api_error(), "Other": api_error("Denied", "No.", 403)}))
    dependents(owner, 2)

    result = report(owner)

    assert result["state"] == "needs_attention"
    assert result["counts"]["needs_attention"] == 3
    assert result["counts"]["next_steps"] == 1


def test_planned_work_stays_conservative_when_the_mirror_changes_or_incarnation_is_stale(owner):
    mirror(owner)
    lakehouse(owner)
    run(owner, Client([onelake("A")], {"A": api_error()}))
    owner.item(MIRROR, "", "").step("activation", EvidenceState.SUCCEEDED, "Started.")

    result = report(owner)

    states = {i["sourceId"]: i["state"] for i in result["items"]}
    assert states[LAKEHOUSE] == "unknown"
    assert result["nextSteps"] == []
    assert result["state"] == "unknown"

    # A different target incarnation of the mirror never counts as the recorded plan.
    owner = Lifecycle(attempt_id="attempt", source_workspace=SOURCE_WS)
    mirror(owner)
    lakehouse(owner)
    run(owner, Client([onelake("A")], {"A": api_error()}))
    owner.item(MIRROR, "", "").resolve("mirror-recreated", TARGET_WS, Disposition.CREATED)
    states = {i["sourceId"]: i["state"] for i in report(owner)["items"]}
    assert states[LAKEHOUSE] == "unknown"


def test_a_planned_evidence_only_report_is_never_ready(owner):
    mirror(owner)

    result = report(owner)

    assert result["state"] != "ready"
    assert result["items"][0]["state"] == "next_steps"
    assert result["nextSteps"][0]["action"] == "Start the mirror."


def test_planned_evidence_round_trips_through_the_journal_record(owner):
    mirror(owner)
    lakehouse(owner)
    run(owner, Client([onelake("A")], {"A": api_error()}))

    restored = ItemOutcome.from_record(owner.get(LAKEHOUSE).record())

    assert restored.steps["shortcut:Tables/A"].state == EvidenceState.PLANNED
    assert restored.steps["shortcut:Tables/A"].waitsOn == MIRROR


def test_old_journal_evidence_without_waits_on_loads_unchanged():
    record = ItemOutcome("a", "A", "Notebook", targetId="t").record()
    record["steps"] = {"target": {"state": "skipped", "targetId": "t"}}

    assert ItemOutcome.from_record(record).steps["target"].waitsOn == ""
