"""Running Copy Jobs as a batch rather than one after another.

Starting a job and waiting for one are separate calls, so there is no reason to finish the
first before starting the second. This needs no threads: all of it is waiting on HTTP.

The bound matters as much as the parallelism. A Copy Job runs on the target capacity, so a
dozen at once on a small SKU is not a dozen times faster, and past a point Fabric turns the
over-subscription into a failed job rather than a slow one.
"""

from __future__ import annotations

from unittest.mock import Mock

import httpx
import pytest

from fabshuffle.fabric import copyjobs
from fabshuffle.fabric.client import (
    FabricApiError,
    FabricClient,
    FabricError,
    FabricTransportError,
    OperationFailed,
)

WS = "ws-scratch"


@pytest.fixture(autouse=True)
def _no_waiting(monkeypatch):
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_poll_seconds", 0)


class FakeClient:
    """Serves job instances, and records how many were running at the same time."""

    def __init__(
        self, *, finish_after=1, fail: set[str] | None = None, never_finish=(), retry_after=None,
    ) -> None:
        self.finish_after = finish_after
        self.fail = fail or set()
        self.never_finish = set(never_finish)
        self.started: list[str] = []
        self.polls: dict[str, int] = {}
        self.peak_in_flight = 0
        self.live: set[str] = set()
        self.next_id = 0
        self.retry_after = retry_after or {}

    def post(self, path, json=None, params=None, wait=True):
        self.next_id += 1
        name = json["displayName"]
        if name in self.fail:
            raise FabricApiError("POST", path, 400, '{"errorCode":"Nope","message":"refused"}')
        job_id = f"job-{self.next_id}"
        self.started.append(name)
        return {"id": job_id, "displayName": name}

    def request(self, method, path, expected=None, **kwargs):
        job_id = path.split("/items/")[1].split("/")[0]
        self.live.add(job_id)
        self.peak_in_flight = max(self.peak_in_flight, len(self.live))

        headers = {"Location": f"https://api/instances/inst-{job_id}"}
        if job_id in self.retry_after:
            headers["Retry-After"] = self.retry_after[job_id]
        return httpx.Response(202, headers=headers)

    def get(self, path, params=None):
        job_id = path.split("/items/")[1].split("/")[0]
        self.polls[job_id] = self.polls.get(job_id, 0) + 1

        name = f"CopyJob{job_id}"
        if job_id in self.never_finish:
            return {"status": "InProgress"}
        if self.polls[job_id] < self.finish_after:
            return {"status": "InProgress"}

        self.live.discard(job_id)
        if name in self.fail:
            return {
                "status": "Failed",
                "failureReason": {"errorCode": "SourceGone", "message": "the source went away"},
            }
        return {"status": "Completed"}


def specs(count, prefix="CopyJob_Lakehouse_"):
    return [
        copyjobs.CopyJobSpec(
            workspace_id=WS,
            display_name=f"{prefix}{n}",
            content={"properties": {}, "activities": []},
            label=f"Table data for lakehouse '{n}'",
        )
        for n in range(count)
    ]


# ------------------------------------------------------------------ the batch


def test_every_job_runs_and_is_reported_as_created():
    client = FakeClient()
    created, warnings = copyjobs.run_copy_jobs(client, specs(5))

    assert warnings == []
    assert len(created) == 5
    assert all(workspace == WS for workspace, _ in created)


def test_jobs_overlap_rather_than_waiting_for_each_other():
    client = FakeClient(finish_after=3)
    copyjobs.run_copy_jobs(client, specs(3), concurrency=3)

    # All three were in flight together; serially the peak would be one.
    assert client.peak_in_flight == 3


def test_no_more_than_the_limit_run_at_once():
    client = FakeClient(finish_after=3)
    copyjobs.run_copy_jobs(client, specs(6), concurrency=2)

    assert client.peak_in_flight == 2
    assert len(client.started) == 6


def test_an_unset_setting_falls_back_to_one_at_a_time(monkeypatch):
    """The caller normally sizes this from the capacity; on its own the runner does not guess."""
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_concurrency", 0)
    client = FakeClient(finish_after=3)
    copyjobs.run_copy_jobs(client, specs(4))

    assert client.peak_in_flight == 1


def test_an_explicit_setting_is_used_when_the_caller_gives_no_limit(monkeypatch):
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_concurrency", 2)
    client = FakeClient(finish_after=3)
    copyjobs.run_copy_jobs(client, specs(6))

    assert client.peak_in_flight == 2


def test_an_empty_batch_does_nothing():
    client = FakeClient()
    assert copyjobs.run_copy_jobs(client, []) == ([], [])
    assert client.started == []


# ------------------------------------------------------------- one going wrong


def test_a_job_that_will_not_start_does_not_stop_the_others():
    client = FakeClient(fail={"CopyJob_Lakehouse_1"})
    created, warnings = copyjobs.run_copy_jobs(client, specs(3))

    assert len(created) == 2
    assert len(warnings) == 1
    assert "lakehouse '1' did not start" in warnings[0]
    assert "refused" in warnings[0]


def test_a_job_that_fails_reports_what_the_service_said():
    client = FakeClient(fail={"CopyJobjob-2"})
    _, warnings = copyjobs.run_copy_jobs(client, specs(3))

    assert len(warnings) == 1
    assert "ended as Failed" in warnings[0]
    assert "the source went away" in warnings[0]


def test_a_job_still_running_at_the_deadline_aborts_with_all_queued_work(clock):
    client = FakeClient(never_finish={"job-1"})
    seen = []
    done = []
    batch = specs(3)

    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, batch, concurrency=1, on_progress=seen.append, on_done=done.append)

    error = caught.value
    assert error.created == [(WS, "job-1")]
    assert error.not_started == tuple(batch[1:])
    assert error.active_jobs[0].copy_job_id == "job-1"
    assert error.active_jobs[0].instance_id == "inst-job-1"
    assert error.counts == copyjobs.CopyJobCounts(3, 0, 0, 0, 1, 0, 2)
    assert all(spec.label in str(error) for spec in batch)
    assert "Do not clean up" in str(error)
    assert "batch waiting budget of 10s expired" in str(error)
    assert not any("3 of 3" in message for message in seen)
    assert seen[-1].startswith("0 of 3 copy job(s) finished")
    assert len(client.started) == 1
    assert done == []
    assert clock.now == 10


def test_a_failed_job_is_still_returned_for_cleanup():
    """It exists in the scratch workspace either way, so it still has to be removed."""
    client = FakeClient(fail={"CopyJobjob-1"})
    created, _ = copyjobs.run_copy_jobs(client, specs(1))

    assert created == [(WS, "job-1")]


# -------------------------------------------------------------------- progress


def test_progress_counts_finished_against_the_total():
    client = FakeClient()
    seen: list[str] = []
    copyjobs.run_copy_jobs(client, specs(3), concurrency=1, on_progress=seen.append)

    # A single detail line cannot name every job at once, so it counts them instead.
    assert seen[-1] == copyjobs.CopyJobCounts(3, 3, 0, 0, 0, 0, 0).summary()
    assert any("running" in message for message in seen)


def test_progress_is_silent_for_an_empty_batch():
    seen: list[str] = []
    copyjobs.run_copy_jobs(FakeClient(), [], on_progress=seen.append)

    assert seen == []


# ------------------------------------------------------- the single job path


def test_the_single_job_helper_still_works():
    """Kept for callers that genuinely have one thing to do and want to block on it."""
    client = FakeClient()
    job_id = copyjobs.run_copy_job(client, WS, "CopyJob_One", {"properties": {}})

    assert job_id == "job-1"


class Clock:
    now = 0.0

    def sleep(self, seconds):
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(copyjobs.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(copyjobs.time, "sleep", clock.sleep)
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_poll_seconds", 6)
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_timeout_seconds", 10)
    return clock


class ScriptedClient(FakeClient):
    def __init__(self, outcomes, **kwargs):
        super().__init__(**kwargs)
        self.outcomes = outcomes
        self.reads = []

    def get(self, path, params=None):
        job_id = path.split("/items/")[1].split("/")[0]
        self.reads.append(job_id)
        outcomes = self.outcomes.get(job_id)
        if not outcomes:
            return super().get(path, params)
        outcome = outcomes.pop(0)
        if isinstance(outcome, FabricError):
            raise outcome
        if outcome.get("status") in copyjobs.TERMINAL_JOB_STATES:
            self.live.discard(job_id)
        return outcome


def unreadable():
    return FabricApiError(
        "GET", "job/progress", 503, '{"errorCode":"Unavailable","message":"capacity is unavailable"}',
    )


def test_repeated_failed_progress_reads_never_release_the_slot():
    client = ScriptedClient({"job-1": [unreadable() for _ in range(3)]})
    done = []
    seen = []
    batch = specs(3)

    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, batch, concurrency=1, on_done=done.append, on_progress=seen.append)

    error = caught.value
    assert client.peak_in_flight == 1
    assert len(client.started) == 1
    assert client.reads == ["job-1"] * 3
    assert error.counts == copyjobs.CopyJobCounts(3, 0, 0, 0, 0, 1, 2)
    assert error.active_jobs[0].last_status == "Unknown"
    assert "Unavailable" in error.active_jobs[0].last_error
    assert "capacity is unavailable" in str(error)
    assert "3 consecutive reconciliation attempts" in str(error)
    assert all(spec.label in str(error) for spec in batch)
    assert "1 unknown" in seen[-1]
    assert done == []


def test_batch_deadline_also_bounds_reconciliation_and_preserves_the_last_error(clock):
    client = ScriptedClient({"job-1": [unreadable()] * 3})
    with pytest.raises(copyjobs.CopyJobBatchIncomplete, match="waiting budget") as caught:
        copyjobs.run_copy_jobs(client, specs(2), concurrency=1)
    assert client.reads == ["job-1"]
    assert caught.value.counts == copyjobs.CopyJobCounts(2, 0, 0, 0, 0, 1, 1)
    assert "Unavailable" in caught.value.active_jobs[0].last_error


def test_progress_recovers_and_resets_the_consecutive_failure_budget():
    client = ScriptedClient({
        "job-1": [
            unreadable(), unreadable(), {"status": "InProgress"},
            unreadable(), unreadable(), {"status": "Completed"},
        ],
    })
    done = []

    created, warnings = copyjobs.run_copy_jobs(client, specs(2), concurrency=1, on_done=done.append)

    assert len(created) == len(done) == 2
    assert client.peak_in_flight == 1
    assert warnings == []
    assert client.reads[:6] == ["job-1"] * 6


def test_exhausted_transport_error_is_reconciled_as_unknown():
    error = FabricTransportError("GET", "progress", 6, httpx.ReadTimeout("response lost"))
    client = ScriptedClient({"job-1": [error] * 3})
    with pytest.raises(copyjobs.CopyJobBatchIncomplete, match="ReadTimeout: response lost") as caught:
        copyjobs.run_copy_jobs(client, specs(2), concurrency=1)
    assert caught.value.counts.unknown == 1
    assert client.peak_in_flight == 1
    assert len(client.started) == 1


def test_deadline_counts_success_failure_rejection_running_and_queue_separately(clock):
    client = FakeClient(
        fail={"CopyJob_Lakehouse_0", "CopyJobjob-3"}, never_finish={"job-4"},
    )
    batch = specs(6)
    done = []
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, batch, concurrency=2, on_done=done.append)

    error = caught.value
    assert error.counts == copyjobs.CopyJobCounts(6, 1, 1, 1, 2, 0, 1)
    assert error.counts.finished == 2
    assert error.not_started == (batch[5],)
    assert done == [batch[1]]
    assert "SourceGone" in str(error)
    assert "the source went away" in str(error)
    assert error.created == [(WS, "job-2"), (WS, "job-3"), (WS, "job-4"), (WS, "job-5")]


@pytest.mark.parametrize("status", ["Completed", "Failed", "Cancelled", "Deduped"])
def test_only_confirmed_success_is_checkpointed_and_terminal_counts_are_honest(status):
    client = ScriptedClient({"job-1": [{
        "status": status,
        "failureReason": {"errorCode": "ServiceCode", "message": "service detail"},
    }]})
    done = []
    seen = []
    batch = specs(1)
    created, warnings = copyjobs.run_copy_jobs(
        client, batch, on_done=done.append, on_progress=seen.append,
    )
    success = int(status == "Completed")
    assert done == (batch if success else [])
    assert created == [(WS, "job-1")]
    assert seen[-1] == copyjobs.CopyJobCounts(1, success, 1 - success, 0, 0, 0, 0).summary()
    if not success:
        assert status in warnings[0]
        assert "ServiceCode" in warnings[0]
        assert "service detail" in warnings[0]


def test_deadline_with_only_queued_jobs_left_still_aborts(clock, monkeypatch):
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_poll_seconds", 10)
    client = FakeClient()
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, specs(3), concurrency=1)
    assert caught.value.counts == copyjobs.CopyJobCounts(3, 1, 0, 0, 0, 0, 2)
    assert caught.value.active_jobs == ()
    assert len(client.started) == 1


def test_no_submission_is_started_after_the_batch_deadline(clock, monkeypatch):
    client = FakeClient()
    create = client.post

    def slow_create(*args, **kwargs):
        clock.now += 11
        return create(*args, **kwargs)

    monkeypatch.setattr(client, "post", slow_create)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, specs(2), concurrency=1)
    assert caught.value.created == [(WS, "job-1")]
    assert caught.value.counts.not_started == 2
    assert client.live == set()


def test_zero_batch_budget_does_not_start_any_work(clock, monkeypatch):
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_timeout_seconds", 0)
    client = FakeClient()
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, specs(2))
    assert client.started == []
    assert caught.value.counts.not_started == 2


@pytest.mark.parametrize("status", [None, "NewServiceStatus"])
def test_missing_or_unrecognized_status_is_not_terminal(status, clock):
    client = ScriptedClient({"job-1": [{"status": status}] * 2})
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, specs(2), concurrency=1)
    assert caught.value.counts.unknown == 1
    assert caught.value.active_jobs[0].last_status == (status or "Unknown")
    assert len(client.started) == 1


def saved_run(spec, *, job_id="existing", instance_id="existing-instance", **kwargs):
    return copyjobs.CopyJobRun(
        workspace_id=spec.workspace_id, copy_job_id=job_id, instance_id=instance_id,
        item_id=spec.item_id, display_name=spec.display_name, label=spec.label, **kwargs,
    )


def test_resume_adopts_and_polls_without_recreating_or_restarting():
    batch = specs(2)
    client = FakeClient(finish_after=2)
    client.live.add("existing")
    started = []
    done = []

    created, warnings = copyjobs.run_copy_jobs(
        client, batch, concurrency=1, resume_jobs=[saved_run(batch[0])],
        on_started=lambda *args: started.append(args), on_done=done.append,
    )

    assert created == [(WS, "existing"), (WS, "job-1")]
    assert client.polls["existing"] == 2
    assert client.started == [batch[1].display_name]
    assert started == [(batch[1], "job-1", "inst-job-1")]
    assert done == batch
    assert client.peak_in_flight == 1
    assert warnings == []


@pytest.mark.parametrize("job_id,instance_id", [(None, None), ("existing", None)])
def test_unknown_submission_refuses_automatic_resume(job_id, instance_id):
    batch = specs(2)
    client = FakeClient()
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(
            client, batch, resume_jobs=[saved_run(batch[0], job_id=job_id, instance_id=instance_id)],
        )
    assert client.started == []
    assert caught.value.active_jobs[0].instance_id is None
    assert caught.value.not_started == (batch[1],)
    assert "reconcile it in Fabric" in str(caught.value)


def test_resume_rejects_wrong_workspace_before_starting_anything():
    batch = specs(1)
    saved = copyjobs.CopyJobRun("other-workspace", "job", "instance", "", batch[0].display_name, "old")
    client = FakeClient()
    with pytest.raises(FabricError, match="current target"):
        copyjobs.run_copy_jobs(client, batch, resume_jobs=[saved])
    assert client.started == []


def test_started_callback_runs_before_any_status_read(monkeypatch):
    client = FakeClient()
    recorded = []
    get = client.get

    def poll(*args, **kwargs):
        assert recorded == [(specs(1)[0], "job-1", "inst-job-1")]
        return get(*args, **kwargs)

    monkeypatch.setattr(client, "get", poll)
    copyjobs.run_copy_jobs(client, specs(1), on_started=lambda *args: recorded.append(args))


@pytest.mark.parametrize(
    "location", ["", "https://api", "https://api/jobs/instances?jobType=CopyJob", "https://["],
)
def test_accepted_start_without_an_instance_location_is_not_a_failed_start(location, monkeypatch):
    client = FakeClient()
    request = client.request

    def start(*args, **kwargs):
        response = request(*args, **kwargs)
        response.headers = {"Location": location}
        return response

    monkeypatch.setattr(client, "request", start)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, specs(2), concurrency=1)
    assert caught.value.active_jobs[0].copy_job_id == "job-1"
    assert caught.value.active_jobs[0].instance_id is None
    assert caught.value.counts.failed_to_start == 0
    assert len(client.started) == 1
    assert client.live == {"job-1"}


@pytest.mark.parametrize("failure", [
    FabricTransportError("POST", "instances", 1, httpx.ReadTimeout("response lost")),
    FabricApiError("POST", "instances", 503, '{"errorCode":"Unknown","message":"service unavailable"}'),
])
def test_uncertain_start_aborts_without_releasing_a_slot(failure, monkeypatch):
    client = FakeClient()

    def start(*args, **kwargs):
        client.live.add("job-1")
        raise failure

    monkeypatch.setattr(client, "request", start)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, specs(3), concurrency=1)
    assert len(client.started) == 1
    assert caught.value.counts == copyjobs.CopyJobCounts(3, 0, 0, 0, 0, 1, 2)
    assert str(failure) in str(caught.value)


def test_unknown_creation_keeps_label_and_refuses_to_assume_no_item_exists(monkeypatch):
    client = FakeClient()

    def create(*args, **kwargs):
        raise FabricTransportError("POST", "copyJobs", 1, httpx.ReadTimeout("lost create response"))

    monkeypatch.setattr(client, "post", create)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, specs(2))
    assert caught.value.created == []
    assert caught.value.active_jobs[0].copy_job_id is None
    assert caught.value.active_jobs[0].label == specs(2)[0].label
    assert caught.value.counts == copyjobs.CopyJobCounts(2, 0, 0, 0, 0, 1, 1)


@pytest.mark.parametrize("result", [{}, {"id": ""}, {"id": None}, {"id": 5}])
def test_creation_without_a_usable_id_is_reported_as_unknown(result, monkeypatch):
    client = FakeClient()
    monkeypatch.setattr(client, "post", lambda *args, **kwargs: result)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete, match="did not return an item ID") as caught:
        copyjobs.run_copy_jobs(client, specs(2), concurrency=1)
    assert caught.value.active_jobs[0].copy_job_id is None
    assert caught.value.created == []
    assert client.live == set()


def test_undefined_creation_operation_is_not_treated_as_a_confirmed_failure(monkeypatch):
    client = FakeClient()

    def create(*args, **kwargs):
        raise OperationFailed("op-1", "Undefined", {"message": "state unavailable"})

    monkeypatch.setattr(client, "post", create)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete, match="state unavailable") as caught:
        copyjobs.run_copy_jobs(client, specs(2))
    assert caught.value.counts.unknown == 1
    assert caught.value.counts.failed_to_start == 0


def test_deadline_during_a_progress_read_keeps_other_active_ids_without_more_reads(clock, monkeypatch):
    client = ScriptedClient({})
    get = client.get

    def slow_poll(*args, **kwargs):
        clock.now += 11
        return get(*args, **kwargs)

    monkeypatch.setattr(client, "get", slow_poll)
    done = []
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, specs(3), concurrency=2, on_done=done.append)
    assert client.reads == ["job-1"]
    assert caught.value.counts == copyjobs.CopyJobCounts(3, 1, 0, 0, 1, 0, 1)
    assert caught.value.active_jobs[0].instance_id == "inst-job-2"
    assert done == [specs(3)[0]]


def test_first_poll_hints_are_honored_without_delaying_other_eligible_jobs(clock, monkeypatch):
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_poll_seconds", 2)
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_timeout_seconds", 60)
    client = ScriptedClient({}, retry_after={"job-1": "20", "job-2": "2"})
    polls = []
    done = []
    get = client.get

    def poll(path, **kwargs):
        polls.append((path.split("/items/")[1].split("/")[0], clock.now))
        return get(path, **kwargs)

    monkeypatch.setattr(client, "get", poll)
    batch = specs(2)
    created, warnings = copyjobs.run_copy_jobs(client, batch, concurrency=2, on_done=done.append)

    assert len(created) == 2
    assert warnings == []
    assert polls == [("job-2", 2), ("job-1", 20)]
    assert done == [batch[1], batch[0]]


def test_first_poll_hint_beyond_deadline_does_not_trigger_an_early_get(clock):
    client = ScriptedClient({}, retry_after={"job-1": "20"})
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, specs(2), concurrency=1)
    assert client.reads == []
    assert clock.now == 10
    assert caught.value.active_jobs[0].instance_id == "inst-job-1"
    assert caught.value.counts == copyjobs.CopyJobCounts(2, 0, 0, 0, 1, 0, 1)


def test_single_job_runner_honors_its_first_poll_hint(clock, monkeypatch):
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_timeout_seconds", 30)
    client = ScriptedClient({}, retry_after={"job-1": "15"})
    get = client.get
    polls = []

    def poll(*args, **kwargs):
        polls.append(clock.now)
        return get(*args, **kwargs)

    monkeypatch.setattr(client, "get", poll)
    assert copyjobs.run_copy_job(client, WS, "CopyJob_One", {}) == "job-1"
    assert polls == [15]


def test_single_job_does_not_wait_past_its_budget_to_honor_a_poll_hint(clock):
    client = ScriptedClient({}, retry_after={"job-1": "20"})
    with pytest.raises(copyjobs.CopyJobFailed, match="Completion is unknown"):
        copyjobs.run_copy_job(client, WS, "CopyJob_One", {})
    assert client.reads == []
    assert clock.now == 10


def test_public_start_helper_still_returns_only_the_instance_id_without_waiting(clock):
    client = FakeClient(retry_after={"existing": "30"})
    assert copyjobs.start_copy_job(client, WS, "existing") == "inst-existing"
    assert clock.now == 0


@pytest.mark.parametrize("status", [403, 404, 429])
def test_error_observing_accepted_creation_is_not_a_mutation_rejection(status, monkeypatch):
    monkeypatch.setattr(copyjobs.SETTINGS, "max_retries", 2)
    calls = []
    states = []
    tokens = Mock()
    tokens.fabric_token.return_value = "offline-token"

    def handler(request):
        calls.append(request.method)
        if request.method == "POST":
            return httpx.Response(202, headers={"x-ms-operation-id": "creation-op"})
        return httpx.Response(
            status, headers={"Retry-After": "0"},
            json={"errorCode": "ObservationUnavailable", "message": "cannot read creation progress"},
        )

    with FabricClient(tokens, transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
            copyjobs.run_copy_jobs(client, specs(2), concurrency=1, on_state=states.append)

    error = caught.value
    assert calls == (["POST", "GET", "GET"] if status == 429 else ["POST", "GET"])
    assert isinstance(error.__cause__, FabricApiError)
    assert error.__cause__.method == "GET"
    assert error.counts == copyjobs.CopyJobCounts(2, 0, 0, 0, 0, 1, 1)
    assert error.active_jobs[0].copy_job_id is None
    assert error.active_jobs[0].submission_state == "unknown"
    assert "ObservationUnavailable" in str(error)
    assert "cannot read creation progress" in str(error)
    assert len(states) == 1
    assert states[0].copy_job_id is None


def test_started_checkpoint_failure_keeps_all_accepted_ids_and_never_calls_error_progress():
    client = FakeClient()
    failure = OSError("checkpoint disk is full")
    started = []

    def checkpoint(spec, job_id, instance_id):
        started.append((spec, job_id, instance_id))
        if len(started) == 2:
            raise failure

    def no_error_callback(_message):
        pytest.fail("A failing checkpoint must not trigger another callback while reporting the error")

    batch = specs(3)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(
            client, batch, concurrency=2, on_started=checkpoint, on_progress=no_error_callback,
        )

    error = caught.value
    assert error.__cause__ is failure
    assert error.created == [(WS, "job-1"), (WS, "job-2")]
    assert [(job.copy_job_id, job.instance_id) for job in error.active_jobs] == [
        ("job-1", "inst-job-1"), ("job-2", "inst-job-2"),
    ]
    assert all(job.submission_state == "submitted" for job in error.active_jobs)
    assert all(job.poll_not_before is not None for job in error.active_jobs)
    assert error.not_started == (batch[2],)
    assert error.counts == copyjobs.CopyJobCounts(3, 0, 0, 0, 2, 0, 1)
    assert all(spec.label in str(error) for spec in batch)
    assert "checkpoint disk is full" in str(error)
    assert client.live == {"job-1", "job-2"}


def test_progress_callback_failure_is_not_reinvoked_or_allowed_to_mask_metadata():
    failure = OSError("progress sink unavailable")
    calls = []

    def progress(message):
        calls.append(message)
        raise failure

    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(FakeClient(), specs(2), concurrency=1, on_progress=progress)
    assert len(calls) == 1
    assert caught.value.__cause__ is failure
    assert caught.value.active_jobs[0].instance_id == "inst-job-1"
    assert caught.value.counts == copyjobs.CopyJobCounts(2, 0, 0, 0, 1, 0, 1)


@pytest.mark.parametrize("phase", ["unknown", "created", "submitting", "submitted"])
def test_state_checkpoint_failure_stops_at_the_corresponding_remote_boundary(phase):
    failure = OSError(f"{phase} checkpoint failed")
    client = FakeClient()
    states = []
    batch = specs(2)

    def checkpoint(state):
        states.append(state)
        if state.submission_state == phase:
            raise failure

    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, batch, concurrency=1, on_state=checkpoint)

    error = caught.value
    assert error.__cause__ is failure
    assert error.active_jobs[0].submission_state == phase
    assert client.next_id == (0 if phase == "unknown" else 1)
    assert client.live == ({"job-1"} if phase == "submitted" else set())
    assert client.polls == {}
    assert error.not_started == (tuple(batch) if phase == "created" else (batch[1],))
    if phase == "created":
        assert error.counts == copyjobs.CopyJobCounts(2, 0, 0, 0, 0, 0, 2)


def test_state_checkpoints_surround_remote_boundaries_and_terminal_checkpoint_follows_on_done(monkeypatch):
    client = FakeClient()
    states = []
    done = []
    post = client.post
    request = client.request

    def create(*args, **kwargs):
        assert states[-1].submission_state == "unknown"
        assert states[-1].copy_job_id is None
        return post(*args, **kwargs)

    def submit(*args, **kwargs):
        assert states[-1].submission_state == "submitting"
        assert states[-1].copy_job_id == "job-1"
        assert states[-1].instance_id is None
        return request(*args, **kwargs)

    def checkpoint(state):
        if state.last_status == "Completed":
            assert done == specs(1)
        states.append(state)

    monkeypatch.setattr(client, "post", create)
    monkeypatch.setattr(client, "request", submit)
    copyjobs.run_copy_jobs(client, specs(1), on_state=checkpoint, on_done=done.append)

    assert [state.submission_state for state in states] == [
        "unknown", "created", "submitting", "submitted", "submitted",
    ]
    assert states[1].instance_id is None
    assert states[3].instance_id == "inst-job-1"
    assert states[3].poll_not_before is not None
    assert states[-1].last_status == "Completed"


def test_failed_success_checkpoint_retains_terminal_run_for_resume_without_recopying():
    client = FakeClient()
    failure = OSError("success checkpoint failed")
    states = []
    batch = specs(2)

    def fail_checkpoint(_spec):
        raise failure

    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(
            client, batch, concurrency=1, on_state=states.append, on_done=fail_checkpoint,
        )

    error = caught.value
    assert error.__cause__ is failure
    assert error.active_jobs[0].last_status == "Completed"
    assert error.active_jobs[0].instance_id == "inst-job-1"
    assert error.counts == copyjobs.CopyJobCounts(2, 1, 0, 0, 0, 0, 1)
    assert states[-1].last_status == "NotStarted"
    assert "ended as Completed, but checkpointing is incomplete" in str(error)
    done = []
    created, warnings = copyjobs.run_copy_jobs(
        client, batch, concurrency=1, resume_jobs=error.active_jobs, on_done=done.append,
    )
    assert created == [(WS, "job-1"), (WS, "job-2")]
    assert client.next_id == 2
    assert done == batch
    assert warnings == []


@pytest.mark.parametrize("status", ["Completed", "Failed"])
def test_terminal_state_checkpoint_failure_preserves_confirmed_result_and_remaining_work(status):
    client = ScriptedClient({"job-1": [{"status": status}]})
    failure = OSError("terminal checkpoint unavailable")
    batch = specs(2)
    done = []

    def checkpoint(state):
        if state.last_status == status:
            raise failure

    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(
            client, batch, concurrency=1, on_state=checkpoint, on_done=done.append,
        )
    error = caught.value
    assert error.__cause__ is failure
    assert error.active_jobs[0].last_status == status
    assert error.active_jobs[0].instance_id == "inst-job-1"
    assert error.not_started == (batch[1],)
    success = int(status == "Completed")
    assert error.counts == copyjobs.CopyJobCounts(2, success, 1 - success, 0, 0, 0, 1)
    assert done == ([batch[0]] if success else [])
    assert len(client.started) == 1


def test_late_creation_is_checkpointed_and_adopted_without_a_second_create(clock, monkeypatch):
    client = FakeClient()
    post = client.post
    states = []
    batch = specs(2)

    def slow_create(*args, **kwargs):
        clock.now += 11
        return post(*args, **kwargs)

    monkeypatch.setattr(client, "post", slow_create)
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, batch, concurrency=1, on_state=states.append)

    error = caught.value
    assert error.active_jobs[0].submission_state == "created"
    assert error.active_jobs[0].copy_job_id == "job-1"
    assert error.active_jobs[0].instance_id is None
    assert error.not_started == tuple(batch)
    assert error.counts == copyjobs.CopyJobCounts(2, 0, 0, 0, 0, 0, 2)
    assert states[-1] == error.active_jobs[0]
    assert client.live == set()

    monkeypatch.setattr(client, "post", post)
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_timeout_seconds", 30)
    created, warnings = copyjobs.run_copy_jobs(
        client, batch, concurrency=1, resume_jobs=error.active_jobs,
    )
    assert created == [(WS, "job-1"), (WS, "job-2")]
    assert client.next_id == 2
    assert client.polls["job-1"] == 1
    assert warnings == []


def test_write_ahead_submitting_record_refuses_duplicate_start_on_resume():
    client = FakeClient()
    batch = specs(2)
    saved = saved_run(batch[0], job_id="existing", instance_id=None, submission_state="submitting")
    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, batch, resume_jobs=[saved])
    assert client.started == []
    assert client.live == set()
    assert caught.value.active_jobs[0].copy_job_id == "existing"


def test_resume_preserves_wall_clock_first_poll_deadline_across_monotonic_origins(clock, monkeypatch):
    epoch = 1_700_000_000.0
    monkeypatch.setattr(copyjobs.time, "time", lambda: epoch + clock.now)
    client = ScriptedClient({}, retry_after={"job-1": "60"})
    batch = specs(2)
    states = []

    with pytest.raises(copyjobs.CopyJobBatchIncomplete) as caught:
        copyjobs.run_copy_jobs(client, batch, concurrency=1, on_state=states.append)
    saved = caught.value.active_jobs
    assert saved[0].poll_not_before == epoch + 60
    assert states[-1].poll_not_before == epoch + 60
    assert clock.now == 10
    assert client.reads == []

    monkeypatch.setattr(copyjobs.time, "monotonic", lambda: 5_000 + clock.now)
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_timeout_seconds", 100)
    get = client.get
    polls = []

    def poll(path, **kwargs):
        polls.append((path.split("/items/")[1].split("/")[0], copyjobs.time.time()))
        return get(path, **kwargs)

    monkeypatch.setattr(client, "get", poll)
    created, warnings = copyjobs.run_copy_jobs(client, batch, concurrency=2, resume_jobs=saved)
    assert polls == [("job-2", epoch + 16), ("job-1", epoch + 60)]
    assert created == [(WS, "job-1"), (WS, "job-2")]
    assert warnings == []
