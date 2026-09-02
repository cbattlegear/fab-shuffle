"""Running Copy Jobs as a batch rather than one after another.

Starting a job and waiting for one are separate calls, so there is no reason to finish the
first before starting the second. This needs no threads: all of it is waiting on HTTP.

The bound matters as much as the parallelism. A Copy Job runs on the target capacity, so a
dozen at once on a small SKU is not a dozen times faster, and past a point Fabric turns the
over-subscription into a failed job rather than a slow one.
"""

from __future__ import annotations

import pytest

from fabshuffle.fabric import copyjobs
from fabshuffle.fabric.client import FabricApiError

WS = "ws-scratch"


@pytest.fixture(autouse=True)
def _no_waiting(monkeypatch):
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_poll_seconds", 0)


class FakeClient:
    """Serves job instances, and records how many were running at the same time."""

    def __init__(self, *, finish_after=1, fail: set[str] | None = None, never_finish=()) -> None:
        self.finish_after = finish_after
        self.fail = fail or set()
        self.never_finish = set(never_finish)
        self.started: list[str] = []
        self.polls: dict[str, int] = {}
        self.peak_in_flight = 0
        self.live: set[str] = set()
        self.next_id = 0

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

        class Response:
            headers = {"Location": f"https://api/instances/inst-{job_id}"}  # noqa: RUF012

        return Response()

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
            return {"status": "Failed", "failureReason": {"message": "the source went away"}}
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


def test_a_job_still_running_at_the_deadline_is_reported_not_waited_on(monkeypatch):
    monkeypatch.setattr(copyjobs.SETTINGS, "copy_job_timeout_seconds", 0)
    client = FakeClient(never_finish={"job-1"})

    _, warnings = copyjobs.run_copy_jobs(client, specs(1))

    assert "still running after" in warnings[0]
    assert "before copying anything by hand" in warnings[0]


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
    assert seen[-1] == "3 of 3 copy job(s) finished"
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
