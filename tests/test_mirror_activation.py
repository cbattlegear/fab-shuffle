"""Opt-in mirror starts are destination-only and require observed state on every attempt."""

from dataclasses import replace

import httpx
import pytest

from fabshuffle import journal, orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.config import SETTINGS
from fabshuffle.fabric import analytics, mirroring
from fabshuffle.fabric.client import FabricApiError, FabricError, FabricTransportError
from fabshuffle.fabric.support import Strategy
from fabshuffle.lifecycle import EvidenceState
from fabshuffle.run import CancelledError, MigrationRun
from fabshuffle.web.app import ResumeRunRequest, StartRunRequest


class MirrorClient:
    def __init__(self, states, *, start_error=None):
        self.states = list(states)
        self.calls = []
        self.start_error = start_error

    def post(self, path, **kwargs):
        self.calls.append(path)
        assert path.startswith("workspaces/target-ws/mirroredDatabases/target-db/")
        if path.endswith("/startMirroring"):
            if self.start_error:
                raise self.start_error
            return {}
        assert path.endswith("/getMirroringStatus")
        result = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        if isinstance(result, Exception):
            raise result
        return result if isinstance(result, dict) else {"status": result}

    @property
    def starts(self):
        return sum(path.endswith("/startMirroring") for path in self.calls)


@pytest.fixture
def clock(monkeypatch):
    current = [0.0]
    monkeypatch.setattr(mirroring.time, "monotonic", lambda: current[0])
    monkeypatch.setattr(mirroring.time, "sleep", lambda seconds: current.__setitem__(0, current[0] + seconds))
    monkeypatch.setattr(SETTINGS, "lro_poll_seconds", 1)
    monkeypatch.setattr(SETTINGS, "lro_timeout_seconds", 5)


def start(client, check_cancel=lambda: None):
    mirroring.ensure_running(
        client, "target-ws", "target-db", check_cancel=check_cancel, on_progress=lambda _: None,
    )


@pytest.mark.parametrize("states,starts", [
    (["Stopped", "Starting", "Running"], 1),
    (["Initializing", "Initialized", "Starting", "Running"], 1),
    (["Stopping", "Stopped", "Running"], 1),
    (["Paused", "Running"], 1),
    (["Running"], 0),
    (["Starting", "Running"], 0),
    (["NewServiceState", "Running"], 1),
])
def test_start_observes_destination_state_and_does_not_repeat_start(clock, states, starts):
    client = MirrorClient(states)
    start(client)
    assert client.starts == starts


def test_a_successful_start_response_is_not_proof_of_running(clock):
    client = MirrorClient(["Stopped", "Starting"])
    with pytest.raises(TimeoutError, match="last status: Starting"):
        start(client)
    assert client.starts == 1


def test_status_embedded_error_retains_service_code_and_message():
    client = MirrorClient([{"status": "Stopped", "error": {
        "errorCode": "SourceUnavailable", "message": "Replication source cannot be reached.",
    }}])
    with pytest.raises(FabricError, match="SourceUnavailable Replication source cannot be reached"):
        start(client)
    assert client.starts == 0


def test_status_permission_failure_is_not_swallowed():
    error = FabricApiError("POST", "getMirroringStatus", 403, '{"errorCode":"Denied","message":"no access"}')
    client = MirrorClient([error])
    with pytest.raises(FabricApiError, match="no access"):
        start(client)
    assert client.starts == 0


@pytest.mark.parametrize("result", [{}, {"status": ""}, {"status": None}])
def test_missing_status_is_unknown_not_success(result):
    client = MirrorClient([result])
    with pytest.raises(FabricError, match="no status value"):
        start(client)
    assert client.starts == 0


def test_transport_error_does_not_reissue_an_uncertain_start():
    error = FabricTransportError("POST", "startMirroring", 1, httpx.ReadTimeout("response lost"))
    client = MirrorClient(["Stopped"], start_error=error)
    with pytest.raises(FabricTransportError, match="response lost"):
        start(client)
    assert client.starts == 1


def test_cancelled_migration_never_starts_a_mirror():
    client = MirrorClient(["Stopped"])
    def cancel():
        raise CancelledError("cancelled")
    with pytest.raises(CancelledError):
        start(client, cancel)
    assert not client.calls


def test_cancellation_after_start_never_stops_the_replication(clock):
    client = MirrorClient(["Stopped"])
    def cancel_after_start():
        if client.starts:
            raise CancelledError("cancelled")
    with pytest.raises(CancelledError):
        start(client, cancel_after_start)
    assert client.starts == 1
    assert not any("stopMirroring" in path for path in client.calls)


@pytest.fixture
def context(tmp_path):
    ctx = orchestrator._Context(
        client=object(), tokens=object(), principal=ServicePrincipal("tenant", "app", "secret"),
        plan=orchestrator.MigrationPlan(
            capacity_id="cap", capacity_name="F64", capacity_region="westus",
            source_workspace_id="source-ws", source_workspace_name="Source",
            target_workspace_name="Target",
        ),
        run=MigrationRun(source_workspace_name="Source", capacity_name="F64"),
        scratch_dir=tmp_path, target_workspace_id="target-ws",
        journal=journal.Journal(tmp_path / "run.jsonl"),
    )
    ctx.source_items["source-db"] = {"id": "source-db", "type": "MirroredDatabase", "displayName": "Orders"}
    ctx.resolve_item(ctx.source_items["source-db"], "target-db", "MirroredDatabase")
    return ctx


def configure(context, states, enabled=False):
    context.plan = replace(context.plan, start_database_mirrors=enabled)
    destination = MirrorClient(states)
    context.target_client = destination
    context.dormant["source-db"] = "old stopped explanation"
    warnings = []
    orchestrator._configure_database_mirror(
        context, context.source_items["source-db"],
        analytics.MigratedItem(source_id="source-db", target_id="target-db", name="Orders", rebound_parts=0),
        warnings,
    )
    return destination, warnings, context.run.lifecycle.snapshot()["source-db"]


def test_off_by_default_only_reads_destination_status(context):
    destination, warnings, outcome = configure(context, ["Stopped"])
    assert destination.starts == 0
    assert outcome.steps["activation"].state == EvidenceState.SKIPPED
    assert "Stopped" in warnings[0]
    assert "Stopped" in context.dormant["source-db"]


def test_opt_in_records_running_but_not_data_readiness(context, clock):
    destination, warnings, outcome = configure(context, ["Stopped", "Running"], True)
    assert destination.starts == 1
    assert outcome.steps["activation"].state == EvidenceState.SUCCEEDED
    assert outcome.steps["replication"].state == EvidenceState.UNKNOWN
    assert "table availability" in warnings[0]
    assert context.dormant["source-db"] == ""
    replay = journal.read(context.journal.path)
    assert replay.dormant["source-db"] == ""
    assert replay.outcomes["source-db"].steps["activation"].targetId == "target-db"


def test_manually_started_mirror_is_not_reported_stopped_on_retry(context):
    context.prior = journal.Replay(id_map={"source-db": "target-db"})
    destination, warnings, outcome = configure(context, ["Running"], False)
    assert destination.starts == 0
    assert outcome.steps["activation"].state == EvidenceState.SUCCEEDED
    assert "reports Running" in warnings[0]
    assert context.dormant["source-db"] == ""


def test_opt_in_start_failure_is_actionable_evidence_not_whole_run_failure(context):
    error = FabricApiError("POST", "getMirroringStatus", 403,
                           '{"errorCode":"InsufficientPrivileges","message":"Service detail"}')
    _, warnings, outcome = configure(context, [error], True)
    assert outcome.steps["activation"].state == EvidenceState.FAILED
    assert outcome.steps["activation"].errorCode == "InsufficientPrivileges"
    assert "Service detail" in warnings[0]
    assert context.dormant["source-db"] == ""


def test_mirrored_phase_reconciles_activation_of_an_adopted_mirror(context, monkeypatch, clock):
    context.prior = journal.Replay(id_map={"source-db": "target-db"})
    context.plan = replace(context.plan, start_database_mirrors=True)
    context.target_client = MirrorClient(["Running"])
    context.target_client.get = lambda path: {"id": "target-db", "properties": {}}
    monkeypatch.setattr(orchestrator.data_stores, "list_mirrored_databases",
                        lambda *args: [context.source_items["source-db"]])
    monkeypatch.setattr(orchestrator.analytics, "list_of_type", lambda *args: [])
    def migrate(ctx, **kwargs):
        assert kwargs["items"] == []
        return [], []
    monkeypatch.setattr(orchestrator, "_migrate_definition_items", migrate)
    orchestrator._migrate_mirrored_databases(context)
    assert context.target_client.starts == 0
    assert context.run.lifecycle.snapshot()["source-db"].steps["activation"].state == EvidenceState.SUCCEEDED


@pytest.mark.parametrize("created", [False, True])
def test_only_successfully_created_mirrors_can_be_started(context, monkeypatch, clock, created):
    context.plan = replace(context.plan, start_database_mirrors=True)
    context.target_client = MirrorClient(["Stopped", "Running"])
    context.target_client.get = lambda path: {"id": "target-db", "properties": {}}
    monkeypatch.setattr(orchestrator.data_stores, "list_mirrored_databases",
                        lambda *args: [context.source_items["source-db"]])
    monkeypatch.setattr(orchestrator.analytics, "list_of_type", lambda *args: [])
    def migrate(ctx, **kwargs):
        assert ctx.target_client.calls == [], "No mirroring calls before definition creation"
        results = [analytics.MigratedItem("source-db", "target-db", "Orders", 0)] if created else []
        return results, [] if created else ["Definition could not be created"]
    monkeypatch.setattr(orchestrator, "_migrate_definition_items", migrate)
    orchestrator._migrate_mirrored_databases(context)
    assert context.target_client.starts == (1 if created else 0)


def test_plan_persists_opt_in_but_web_resume_requires_fresh_consent(context):
    assert not context.plan.start_database_mirrors
    enabled = replace(context.plan, start_database_mirrors=True)
    record = orchestrator._plan_record(enabled)
    assert record["start_database_mirrors"] is True
    restored = orchestrator.plan_from_journal(journal.Replay(plan=record))
    assert restored.start_database_mirrors
    assert not ResumeRunRequest().apply(restored).start_database_mirrors
    assert ResumeRunRequest(start_database_mirrors=True).apply(restored).start_database_mirrors
    assert not orchestrator.plan_from_journal(journal.Replay(
        plan={k: v for k, v in record.items() if k != "start_database_mirrors"},
    )).start_database_mirrors


@pytest.mark.parametrize("value", ["true", "false", 1, 0, None])
def test_mirror_activation_requires_explicit_boolean(value):
    for request, arguments in ((StartRunRequest, {"capacity_id": "c", "source_workspace_id": "s"}),
                               (ResumeRunRequest, {})):
        with pytest.raises(ValueError):
            request(start_database_mirrors=value, **arguments)


def test_reassignment_cannot_enable_source_mirroring(context):
    plan = replace(context.plan, strategy=Strategy.REASSIGN, start_database_mirrors=True)
    assert "only available for rebuild" in plan.execution_blocker


def test_uninitialised_mirror_times_out_without_starting(clock):
    client = MirrorClient(["Initializing"])
    with pytest.raises(TimeoutError, match="last status: Initializing"):
        start(client)
    assert client.starts == 0


def test_start_rejection_preserves_service_words():
    client = MirrorClient(["Stopped"], start_error=FabricApiError(
        "POST", "startMirroring", 400, '{"errorCode":"UnknownError","message":"The service said this"}',
    ))
    with pytest.raises(FabricApiError, match="The service said this") as error:
        start(client)
    assert error.value.error_code == "UnknownError"
    assert client.starts == 1


def test_source_workspace_is_never_activated(context):
    context.target_workspace_id = context.plan.source_workspace_id.upper()
    destination, warnings, outcome = configure(context, ["Stopped"], True)
    assert not destination.calls
    assert "source workspace" in warnings[0]
    assert outcome.steps["activation"].state == EvidenceState.FAILED
