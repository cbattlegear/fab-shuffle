"""Readiness is evidence about migration obligations, not a synonym for creation."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import replace
from itertools import product

import pytest

from fabshuffle import journal
from fabshuffle.fabric.client import FabricApiError, OperationFailed
from fabshuffle.lifecycle import (
    Disposition,
    EvidenceState,
    ItemOutcome,
    Lifecycle,
    LifecycleContract,
    contract_for,
    readiness_report,
    safe_text,
)
from fabshuffle.run import CancelledError


def report(lifecycle, *, inventory_complete=True):
    return readiness_report(
        lifecycle.snapshot(), run_id=lifecycle.attempt_id, lineage_id="first",
        run_status="succeeded", inventory_complete=inventory_complete,
    )


def ready_notebook(lifecycle, source="notebook", name="Ingest"):
    item = lifecycle.item(source, name, "Notebook")
    item.resolve(f"target-{source}", "target-workspace", Disposition.CREATED)
    item.step("definition", EvidenceState.SUCCEEDED, "Definition copied.")
    item.step("rebind", EvidenceState.SUCCEEDED, "References repointed.")
    item.complete()
    return item


@pytest.mark.parametrize("disposition", [
    Disposition.CREATED, Disposition.ADOPTED, Disposition.REFRESHED,
])
@pytest.mark.parametrize("item_type", ["Notebook", "Lakehouse", "Warehouse"])
def test_a_target_and_returned_adapter_do_not_prove_usability(disposition, item_type):
    lifecycle = Lifecycle(attempt_id="first", source_workspace="source-workspace")
    item = lifecycle.item("source", "Orders", item_type)
    item.resolve("target", "target-workspace", disposition)
    item.complete()

    result = report(lifecycle)
    outcome = result["items"][0]
    assert result["state"] == outcome["state"] == "unknown"
    assert outcome["disposition"] == disposition
    assert outcome["targetId"] == "target"
    assert outcome["sourceWorkspaceId"] == "source-workspace"
    assert outcome["targetWorkspaceId"] == "target-workspace"
    assert next(step for step in outcome["steps"] if step["step"] == "target")["state"] == "succeeded"
    assert "completion" not in outcome["required"]


@pytest.mark.parametrize(
    "states", list(product(EvidenceState, repeat=3)),
    ids=lambda states: "-".join(state.value for state in states),
)
def test_required_evidence_truth_table(states):
    lifecycle = Lifecycle(attempt_id="first")
    item = lifecycle.item("source", "Ingest", "Notebook")
    item.resolve("target", "target-workspace", Disposition.CREATED)
    for step, state in zip(("target", "definition", "rebind"), states, strict=True):
        item.step(step, state, f"{step} was {state}.")
    item.complete()

    if any(state in (EvidenceState.FAILED, EvidenceState.SKIPPED) for state in states):
        expected = "needs_attention"
    elif EvidenceState.UNKNOWN in states:
        expected = "unknown"
    else:
        expected = "ready"

    result = report(lifecycle)
    assert result["state"] == result["items"][0]["state"] == expected
    assert result["counts"] == {
        state: int(state == expected) for state in ("ready", "unknown", "needs_attention")
    }
    assert result["runStatus"] == "succeeded"


@pytest.mark.parametrize(("item_type", "required"), [
    ("Notebook", {"target", "definition", "rebind"}),
    ("Lakehouse", {"target", "data", "files", "shortcuts", "endpoint", "schema"}),
    ("Warehouse", {"target", "schema", "data"}),
    ("SQLDatabase", {"target", "schema", "data"}),
    ("KQLDatabase", {"target", "definition", "rebind", "data", "shortcuts"}),
    ("CosmosDBDatabase", {"target", "definition", "rebind", "data"}),
    ("ApacheAirflowJob", {"target", "definition", "rebind", "files"}),
    ("Eventhouse", {"target"}),
])
def test_item_contracts_name_all_required_migration_obligations(item_type, required):
    assert set(contract_for(item_type).required) == required
    lifecycle = Lifecycle(attempt_id="first")
    lifecycle.item("source", "Orders", item_type)
    assert set(lifecycle.snapshot()["source"].required) == required


@pytest.mark.parametrize(("complete", "expected"), [(False, "unknown"), (True, "ready")])
def test_known_empty_inventory_is_distinct_from_unmeasured_inventory(complete, expected):
    result = report(Lifecycle(attempt_id="first"), inventory_complete=complete)
    assert result["state"] == expected
    assert result["inventoryComplete"] is complete
    assert result["items"] == []
    assert result["counts"] == {"ready": 0, "unknown": 0, "needs_attention": 0}
    assert bool(any("inventory" in limit.lower() for limit in result["limits"])) is not complete


def test_ready_observed_items_do_not_prove_an_unfinished_inventory_is_ready():
    lifecycle = Lifecycle(attempt_id="first")
    ready_notebook(lifecycle)
    result = report(lifecycle, inventory_complete=False)
    assert result["items"][0]["state"] == "ready"
    assert result["state"] == "unknown"


def test_a_missing_required_step_remains_unknown_after_adapter_completion():
    lifecycle = Lifecycle(attempt_id="first")
    item = lifecycle.item("source", "Ingest", "Notebook")
    item.resolve("target", "target-workspace", Disposition.CREATED)
    item.step("definition", EvidenceState.SUCCEEDED, "Definition copied.")
    item.complete()

    result = report(lifecycle)["items"][0]
    rebind = next(step for step in result["steps"] if step["step"] == "rebind")
    assert result["state"] == "unknown"
    assert rebind["state"] == "unknown"
    assert rebind["reason"]
    assert any("rebind" in reason for reason in result["reasons"])


@pytest.mark.parametrize("item_type", ["Notebook", "Eventhouse"])
def test_a_bare_outcome_mapping_without_evidence_is_never_ready(item_type):
    outcome = ItemOutcome("source", "Mapped item", item_type, targetId="target")
    result = readiness_report(
        {"source": outcome}, run_id="first", lineage_id="first",
        run_status="succeeded", inventory_complete=True,
    )
    assert result["state"] == result["items"][0]["state"] == "unknown"


def test_successful_observations_without_a_target_identity_cannot_be_ready():
    lifecycle = Lifecycle(attempt_id="first")
    item = lifecycle.item("source", "Ingest", "Notebook")
    for step in ("target", "definition", "rebind"):
        item.step(step, EvidenceState.SUCCEEDED, "Observed a successful operation.")
    result = report(lifecycle)["items"][0]
    assert result["state"] == "unknown"
    assert result["targetId"] == ""
    assert any("target" in reason.lower() for reason in result["reasons"])


def test_explicitly_skipped_data_needs_attention_and_an_action():
    lifecycle = Lifecycle(attempt_id="first")
    item = lifecycle.item("warehouse", "Orders", "Warehouse")
    item.resolve("target", "target-workspace", Disposition.CREATED)
    item.step("schema", EvidenceState.SUCCEEDED, "Schema copied.")
    item.step(
        "data", EvidenceState.SKIPPED, "Data copying was disabled.",
        action="Resume Orders with data copying enabled.",
    )
    item.complete()

    outcome = report(lifecycle)["items"][0]
    assert outcome["state"] == "needs_attention"
    assert "data: Data copying was disabled." in outcome["reasons"]
    assert "Resume Orders with data copying enabled." in outcome["actions"]


def test_optional_completion_is_not_an_extra_readiness_obligation():
    lifecycle = Lifecycle(attempt_id="first")
    item = ready_notebook(lifecycle)
    item.step("diagnostic", EvidenceState.FAILED, "Optional check failed.", required=False)
    assert report(lifecycle)["state"] == "ready"


def test_custom_contract_can_add_a_required_obligation():
    lifecycle = Lifecycle(attempt_id="first")
    item = lifecycle.item(
        "source", "Scheduled item", "Custom",
        contract=LifecycleContract(("target", "stopped")),
    )
    item.resolve("target", "target-workspace", Disposition.CREATED)
    assert report(lifecycle)["state"] == "unknown"
    item.step("stopped", EvidenceState.SUCCEEDED, "Created switched off.")
    assert report(lifecycle)["state"] == "ready"


def test_operation_is_unknown_until_it_returns_successfully():
    lifecycle = Lifecycle(attempt_id="first")
    item = ready_notebook(lifecycle)
    with item.operation("definition", "Definition refreshed."):
        assert report(lifecycle)["state"] == "unknown"
        assert lifecycle.snapshot()["notebook"].steps["definition"].state == EvidenceState.UNKNOWN
    assert report(lifecycle)["state"] == "ready"
    assert lifecycle.snapshot()["notebook"].steps["definition"].reason == "Definition refreshed."


@pytest.mark.parametrize("error", [
    FabricApiError("POST", "https://example.invalid/items", 403, json.dumps({
        "errorCode": "InsufficientPrivileges",
        "message": "The principal cannot write Orders.",
        "requestPayload": "not-an-exported-definition",
    })),
    OperationFailed("operation-id", "Failed", {
        "errorCode": "InsufficientPrivileges",
        "message": "The principal cannot write Orders.",
    }),
])
def test_failed_operations_preserve_service_code_and_message_and_reraise(error):
    lifecycle = Lifecycle(attempt_id="first")
    item = ready_notebook(lifecycle)
    with pytest.raises(type(error)) as raised:
        with item.operation("definition", "Definition copied."):
            raise error

    assert raised.value is error
    evidence = lifecycle.snapshot()["notebook"].steps["definition"]
    assert evidence.state == EvidenceState.FAILED
    assert evidence.errorCode == "InsufficientPrivileges"
    assert evidence.message == "The principal cannot write Orders."
    assert evidence.attemptId == "first"
    assert evidence.targetId == "target-notebook"
    assert report(lifecycle)["state"] == "needs_attention"
    assert "not-an-exported-definition" not in json.dumps(report(lifecycle))


@pytest.mark.parametrize("error_type", [CancelledError, asyncio.CancelledError])
def test_cancellation_does_not_claim_the_remote_operation_failed(error_type):
    lifecycle = Lifecycle(attempt_id="first")
    item = ready_notebook(lifecycle)
    error = error_type("Operator requested cancellation.")
    with pytest.raises(error_type) as raised:
        with item.operation("definition", "Definition copied."):
            raise error

    assert raised.value is error
    assert lifecycle.snapshot()["notebook"].steps["definition"].state == EvidenceState.UNKNOWN
    assert report(lifecycle)["state"] == "unknown"


def test_retry_success_replaces_current_failure_evidence():
    lifecycle = Lifecycle(attempt_id="first")
    item = ready_notebook(lifecycle)
    with pytest.raises(RuntimeError, match="Copy failed"):
        with item.operation("definition", "Definition copied."):
            raise RuntimeError("Copy failed")
    assert report(lifecycle)["state"] == "needs_attention"
    with item.operation("definition", "Definition copied."):
        pass
    evidence = lifecycle.snapshot()["notebook"].steps["definition"]
    assert evidence.errorCode == evidence.message == ""
    assert report(lifecycle)["state"] == "ready"


@pytest.mark.parametrize(("text", "secret"), [
    ("Authorization: Bearer bearer-secret", "bearer-secret"),
    ("Password=password-secret;", "password-secret"),
    ("pwd='pwd-secret';", "pwd-secret"),
    ('"client_secret": "client-secret"', "client-secret"),
    ('"accessToken":"access-secret"', "access-secret"),
    ("token=token-secret&other=ok", "token-secret"),
    ("https://example.invalid?sig=sas-secret&other=ok", "sas-secret"),
    ("AccountKey=account-secret;", "account-secret"),
    ('payload="encoded-definition-secret"', "encoded-definition-secret"),
    ("eyJheader.eyJbody.signature", "eyJheader.eyJbody.signature"),
])
def test_evidence_text_redacts_credentials_and_payloads_without_losing_service_words(text, secret):
    sanitized = safe_text(f"CopyDenied: Orders could not be copied. {text}")
    assert secret not in sanitized
    assert "CopyDenied: Orders could not be copied." in sanitized


def test_named_secrets_and_structured_payloads_are_removed_from_all_failure_text():
    lifecycle = Lifecycle(attempt_id="first", secrets=("configured-secret-marker",))
    item = ready_notebook(lifecycle)
    item.step(
        "definition", EvidenceState.FAILED, "Failed using configured-secret-marker.",
        action="Replace configured-secret-marker before retrying.",
        error=FabricApiError("POST", "https://example.invalid/items", 400, json.dumps({
            "errorCode": "CopyDenied token=error-code-token-marker",
            "message": "Orders could not be copied with configured-secret-marker. "
                       'payload={"parts":[{"payload":"structured-definition-marker",'
                       '"name":"nested-payload-marker"}]}',
        })),
    )
    outcome = report(lifecycle)["items"][0]
    failed = next(step for step in outcome["steps"] if step["step"] == "definition")
    assert failed["errorCode"].startswith("CopyDenied")
    assert "Orders could not be copied" in failed["message"]
    encoded = json.dumps(outcome)
    for secret in (
        "configured-secret-marker", "error-code-token-marker",
        "structured-definition-marker", "nested-payload-marker",
    ):
        assert secret not in encoded


def test_unresolved_references_name_the_item_that_must_be_migrated():
    lifecycle = Lifecycle(attempt_id="first")
    item = ready_notebook(lifecycle)
    item.references([], ["Lakehouse 'Orders' (orders-source)"])

    result = report(lifecycle)["items"][0]
    assert result["state"] == "needs_attention"
    assert any("Lakehouse 'Orders' (orders-source)" in reason for reason in result["reasons"])
    assert result["actions"]


def test_references_replace_while_add_references_preserves_existing_dependencies():
    lifecycle = Lifecycle(attempt_id="first")
    item = ready_notebook(lifecycle)
    item.references(["notebook", "old"], ["Obsolete dependency"])
    item.references(["warehouse", "warehouse", "notebook"], ["Missing Orders"])
    item.add_references(["model", "warehouse", "notebook"], ["Missing Orders", "Missing Revenue"])

    outcome = lifecycle.snapshot()["notebook"]
    assert outcome.dependencies == ["model", "warehouse"]
    assert outcome.unresolvedReferences == ["Missing Orders", "Missing Revenue"]


@pytest.mark.parametrize(("state", "expected"), [
    (EvidenceState.UNKNOWN, "unknown"),
    (EvidenceState.FAILED, "needs_attention"),
    (EvidenceState.SKIPPED, "needs_attention"),
])
def test_dependency_readiness_propagates_transitively_with_display_names(state, expected):
    lifecycle = Lifecycle(attempt_id="first")
    upstream = ready_notebook(lifecycle, "upstream", "Orders ingestion")
    middle = ready_notebook(lifecycle, "middle", "Revenue model")
    downstream = ready_notebook(lifecycle, "downstream", "Executive report")
    middle.references(["upstream"])
    downstream.references(["middle"])
    upstream.step("definition", state, "Definition did not finish.")

    results = {item["sourceId"]: item for item in report(lifecycle)["items"]}
    assert {item["state"] for item in results.values()} == {expected}
    assert "Dependency 'Orders ingestion' is not ready." in results["middle"]["reasons"]
    assert "Dependency 'Revenue model' is not ready." in results["downstream"]["reasons"]
    assert any("Orders ingestion" in action for action in results["middle"]["actions"])
    assert any("Revenue model" in action for action in results["downstream"]["actions"])


def test_a_dependency_absent_from_the_inventory_is_unknown_not_ready():
    lifecycle = Lifecycle(attempt_id="first")
    item = ready_notebook(lifecycle)
    item.references(["missing-source"])
    outcome = report(lifecycle)["items"][0]
    assert outcome["state"] == "unknown"
    assert "Dependency 'missing-source' is not ready." in outcome["reasons"]


@pytest.mark.parametrize(("state", "expected"), [
    (EvidenceState.SUCCEEDED, "ready"),
    (EvidenceState.UNKNOWN, "unknown"),
    (EvidenceState.FAILED, "needs_attention"),
])
def test_dependency_cycles_reach_a_fixed_point_without_inventing_failure(state, expected):
    lifecycle = Lifecycle(attempt_id="first")
    items = {key: ready_notebook(lifecycle, key, key.upper()) for key in ("a", "b", "c")}
    items["a"].references(["b"])
    items["b"].references(["c"])
    items["c"].references(["a"])
    items["b"].step("rebind", state, "Reference update observed.")

    result = report(lifecycle)
    assert result["state"] == expected
    assert {item["state"] for item in result["items"]} == {expected}
    assert result["counts"][expected] == 3


def test_evidence_for_another_target_is_reported_as_unknown_not_success():
    lifecycle = Lifecycle(attempt_id="first")
    ready_notebook(lifecycle)
    initial = lifecycle.snapshot()
    initial["notebook"].steps["definition"] = replace(
        initial["notebook"].steps["definition"], targetId="old-target",
    )
    lifecycle = Lifecycle(attempt_id="second", initial=initial)

    outcome = report(lifecycle)["items"][0]
    assert outcome["state"] == "unknown"
    evidence = next(step for step in outcome["steps"] if step["step"] == "definition")
    assert evidence["state"] == "unknown"
    assert any("target" in reason.lower() for reason in outcome["reasons"])


@pytest.mark.parametrize(("evidence_attempt", "expected"), [("first", "ready"), ("older", "unknown")])
def test_only_current_attempt_evidence_can_bind_to_a_newly_resolved_target(evidence_attempt, expected):
    lifecycle = Lifecycle(attempt_id=evidence_attempt)
    item = lifecycle.item("source", "Ingest", "Notebook")
    item.step("definition", EvidenceState.SUCCEEDED, "Definition submitted before target returned.")
    item.step("rebind", EvidenceState.SUCCEEDED, "References were rewritten.")
    lifecycle = Lifecycle(attempt_id="first", initial=lifecycle.snapshot())
    item = lifecycle.item("source", "Ingest", "Notebook")
    item.resolve("target", "target-workspace", Disposition.CREATED)
    assert report(lifecycle)["state"] == expected


@pytest.mark.parametrize("target_lost", [False, True])
def test_target_or_dependency_invalidation_resets_current_evidence_but_keeps_history(target_lost):
    first = Lifecycle(attempt_id="first")
    item = ready_notebook(first)
    item.references(["source-dependency"], ["Named missing dependency"])
    prior = first.snapshot()
    original = prior["notebook"].record()
    original.pop("history")
    second = Lifecycle(attempt_id="second", initial=prior)
    second.invalidate(["notebook", "not-observed"], target_lost=target_lost)

    outcome = second.snapshot()["notebook"]
    assert outcome.targetId == ("" if target_lost else "target-notebook")
    assert outcome.disposition == (Disposition.UNKNOWN if target_lost else Disposition.CREATED)
    assert set(outcome.steps) == set(outcome.required)
    assert all(evidence.state == EvidenceState.UNKNOWN for evidence in outcome.steps.values())
    assert all(evidence.attemptId == "second" for evidence in outcome.steps.values())
    assert all(evidence.targetId == outcome.targetId for evidence in outcome.steps.values())
    assert outcome.dependencies == outcome.unresolvedReferences == []
    assert original in outcome.history
    assert all("history" not in entry for entry in outcome.history)
    assert report(second)["state"] == "unknown"
    assert prior == first.snapshot()
    assert prior["notebook"].history == []


def test_resolving_a_replacement_target_invalidates_old_success_without_affecting_other_items():
    lifecycle = Lifecycle(attempt_id="first")
    item = ready_notebook(lifecycle)
    ready_notebook(lifecycle, "unrelated", "Unrelated")
    old_record = lifecycle.snapshot()["notebook"].record()
    old_record.pop("history")
    item.resolve("replacement", "target-workspace", Disposition.CREATED)

    outcome = lifecycle.snapshot()["notebook"]
    assert outcome.targetId == "replacement"
    assert outcome.steps["target"].state == EvidenceState.SUCCEEDED
    assert outcome.steps["definition"].state == EvidenceState.UNKNOWN
    assert outcome.steps["rebind"].state == EvidenceState.UNKNOWN
    assert outcome.history == [old_record]
    results = {entry["sourceId"]: entry["state"] for entry in report(lifecycle)["items"]}
    assert results == {"notebook": "unknown", "unrelated": "ready"}


def test_snapshots_and_resumed_initial_outcomes_do_not_share_mutable_state():
    first = Lifecycle(attempt_id="first")
    ready_notebook(first)
    first.invalidate(["notebook"])
    initial = first.snapshot()
    second = Lifecycle(attempt_id="second", initial=initial)
    expected = second.snapshot()
    initial["notebook"].required.append("external-mutation")
    initial["notebook"].history[0]["steps"]["definition"]["reason"] = "Externally changed"
    snapshot = second.snapshot()
    snapshot["notebook"].dependencies.append("external-dependency")
    snapshot["notebook"].history[0]["steps"]["rebind"]["reason"] = "Also externally changed"

    assert second.snapshot() == expected
    assert "external-mutation" not in first.snapshot()["notebook"].required
    assert second.snapshot()["notebook"].history[0]["steps"]["definition"]["reason"] == "Definition copied."


def test_journal_outcome_and_inventory_round_trip(tmp_path):
    book = journal.Journal(tmp_path / "first.jsonl")
    book.run_created({"source_workspace_id": "source-workspace"}, cleanup=True)
    lifecycle = Lifecycle(
        attempt_id="first", source_workspace="source-workspace", record=book.outcome,
    )
    ready_notebook(lifecycle)
    book.inventory()
    book.finished("succeeded")

    replay = journal.read(book.path)
    assert replay.outcomes == lifecycle.snapshot()
    assert replay.inventory_complete is True
    assert replay.damaged_lines == 0
    assert report(Lifecycle(initial=replay.outcomes))["state"] == "ready"


def test_journal_outcome_ignores_top_level_payload_extensions(tmp_path):
    lifecycle = Lifecycle(attempt_id="first")
    ready_notebook(lifecycle)
    record = lifecycle.snapshot()["notebook"].record()
    record["payload"] = {"parts": ["raw-definition-marker"]}
    record["access_token"] = "raw-access-token-marker"
    book = journal.Journal(tmp_path / "first.jsonl")
    book.outcome(record)

    replay = journal.read(book.path)
    assert replay.damaged_lines == 0
    assert replay.outcomes == lifecycle.snapshot()
    assert "raw-definition-marker" not in json.dumps(replay.outcomes["notebook"].record())
    assert "raw-access-token-marker" not in json.dumps(replay.outcomes["notebook"].record())


@pytest.mark.parametrize("malformed", [
    {"sourceId": "bad", "disposition": "not-a-disposition"},
    {"sourceId": "bad", "steps": {"data": {"state": "not-an-evidence-state"}}},
])
def test_bad_outcomes_preserve_previous_evidence_as_uncertain_history(tmp_path, malformed):
    book = journal.Journal(tmp_path / "first.jsonl")
    lifecycle = Lifecycle(attempt_id="first", record=book.outcome)
    ready_notebook(lifecycle)
    book.outcome(malformed)
    replay = journal.read(book.path)
    assert replay.damaged_lines == 1
    original = lifecycle.snapshot()["notebook"].record()
    original.pop("history")
    outcome = replay.outcomes["notebook"]
    assert original in outcome.history
    assert all(evidence.state == EvidenceState.UNKNOWN for evidence in outcome.steps.values())
    assert all(
        evidence.reason == "Journal contains incomplete records; previous evidence is uncertain."
        for evidence in outcome.steps.values()
    )
    assert replay.inventory_complete is False


@pytest.mark.parametrize("invalidation", ["target", "dependency", "refresh"])
def test_replayed_invalidation_preserves_history_without_reusing_stale_success(tmp_path, invalidation):
    book = journal.Journal(tmp_path / "second.jsonl")
    book.run_created({"source_workspace_id": "source-workspace"}, cleanup=True)
    book.item("notebook", "target-notebook", "Notebook", "Ingest")
    book.data("notebook", "files", target_id="target-notebook")
    lifecycle = Lifecycle(attempt_id="first", record=book.outcome)
    ready_notebook(lifecycle)
    if invalidation == "target":
        book.invalidate(["notebook"])
    elif invalidation == "dependency":
        book.invalidate([], refresh=["notebook"])
    else:
        book.refresh(["notebook"])

    replay = journal.read(book.path)
    outcome = replay.outcomes["notebook"]
    assert outcome.targetId == ("" if invalidation == "target" else "target-notebook")
    assert all(evidence.state == EvidenceState.UNKNOWN for evidence in outcome.steps.values())
    assert all(evidence.attemptId == "second" for evidence in outcome.steps.values())
    assert outcome.history[0]["steps"]["definition"]["state"] == "succeeded"
    assert outcome.history[0]["steps"]["definition"]["attemptId"] == "first"
    assert replay.data_is_done("notebook", "files") is (invalidation != "target")
    assert report(Lifecycle(initial=replay.outcomes))["state"] == "unknown"


def test_three_attempts_retain_independent_evidence_and_history_after_ancestor_pruning(tmp_path):
    plan = {"source_workspace_id": "source-workspace"}
    first_book = journal.Journal(tmp_path / "first.jsonl")
    first_book.run_created(plan, cleanup=True)
    first_book.workspace("target", "target-workspace", "Migrated")
    first_book.item("notebook", "target-notebook", "Notebook", "Ingest")
    first = Lifecycle(attempt_id="first", record=first_book.outcome)
    first_item = ready_notebook(first)
    first_item.step(
        "definition", EvidenceState.FAILED, "Definition copy failed.",
        error=OperationFailed("operation-id", "Failed", {
            "errorCode": "CopyRejected", "message": "The definition could not be copied.",
        }),
    )
    first_book.inventory()
    first_book.finished("failed", "Copy did not finish.")
    prior = journal.read(first_book.path)

    second_book = journal.Journal(tmp_path / "second.jsonl")
    second_book.run_created(plan, cleanup=True, prior=prior)
    second = Lifecycle(attempt_id="second", record=second_book.outcome, initial=prior.outcomes)
    second.invalidate(["notebook"])
    ready_notebook(second)
    second_book.finished("succeeded")
    second_replay = journal.read(second_book.path)
    assert prior.outcomes["notebook"].steps["definition"].state == EvidenceState.FAILED
    assert prior.outcomes["notebook"].history == []

    third_book = journal.Journal(tmp_path / "third.jsonl")
    third_book.run_created(plan, cleanup=True, prior=second_replay)
    for index, book in enumerate((first_book, second_book, third_book), start=1):
        os.utime(book.path, (index, index))
    assert journal.prune(tmp_path, keep=1) == 2
    assert not first_book.path.exists()
    assert not second_book.path.exists()

    replay = journal.read(third_book.path)
    assert replay.lineage_id == "first"
    assert replay.resumed_from == "second"
    assert replay.ancestors == ["first", "second"]
    assert [(attempt["run_id"], attempt["status"]) for attempt in replay.attempts] == [
        ("first", "failed"), ("second", "succeeded"),
    ]
    assert replay.inventory_complete is True
    assert replay.outcomes == second_replay.outcomes
    assert replay.id_map["notebook"] == "target-notebook"
    current = replay.outcomes["notebook"]
    assert current.steps["definition"].attemptId == "second"
    assert current.history
    assert all("history" not in entry for entry in current.history)
    previous_failure = next(
        entry["steps"]["definition"] for entry in current.history
        if entry["steps"]["definition"]["state"] == "failed"
    )
    assert previous_failure["errorCode"] == "CopyRejected"
    assert previous_failure["attemptId"] == "first"
    assert report(Lifecycle(initial=replay.outcomes))["state"] == "ready"
