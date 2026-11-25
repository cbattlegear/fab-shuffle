"""Running blocking work on a bounded pool.

Most of a migration waits on HTTP and is handled by polling in one loop. This covers the
parts that genuinely block a thread: sqlpackage and unpackdacpac subprocesses, the schema
deploy over ODBC.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from fabshuffle import concurrency
from fabshuffle.run import CancelledError


def job(key, warnings=(), *, record=None, delay=0.0):
    def run():
        if delay:
            time.sleep(delay)
        if record is not None:
            record.append(key)
        return list(warnings)

    return concurrency.Job(key=key, run=run)


# ----------------------------------------------------------------- the basics


def test_every_job_runs():
    ran: list[str] = []
    concurrency.run_bounded([job(str(n), record=ran) for n in range(5)], limit=3)

    assert sorted(ran) == ["0", "1", "2", "3", "4"]


def test_nothing_to_do_is_not_an_error():
    assert concurrency.run_bounded([], limit=3) == []


def test_warnings_come_back_in_a_fixed_order_whatever_the_timing():
    """Two runs of the same workspace have to read the same way."""
    jobs = [
        job("charlie", ["c"], delay=0.03),
        job("alpha", ["a"], delay=0.01),
        job("bravo", ["b"], delay=0.02),
    ]
    # Sorted by key, not by which thread happened to finish first.
    assert concurrency.run_bounded(jobs, limit=3) == ["a", "b", "c"]


def test_one_job_returning_warnings_does_not_affect_the_others():
    jobs = [job("a", ["went wrong"]), job("b"), job("c")]

    assert concurrency.run_bounded(jobs, limit=2) == ["went wrong"]


# ------------------------------------------------------------------ the bound


def test_no_more_than_the_limit_run_at_once():
    live = 0
    peak = 0
    lock = threading.Lock()

    def counted():
        nonlocal live, peak
        with lock:
            live += 1
            peak = max(peak, live)
        time.sleep(0.02)
        with lock:
            live -= 1
        return []

    jobs = [concurrency.Job(key=str(n), run=counted) for n in range(8)]
    concurrency.run_bounded(jobs, limit=2)

    # sqlpackage is a few hundred megabytes a copy; eight at once is not eight times faster.
    assert peak == 2


def test_work_actually_overlaps():
    live = 0
    peak = 0
    lock = threading.Lock()

    def counted():
        nonlocal live, peak
        with lock:
            live += 1
            peak = max(peak, live)
        time.sleep(0.05)
        with lock:
            live -= 1
        return []

    concurrency.run_bounded([concurrency.Job(key=str(n), run=counted) for n in range(3)], limit=3)

    assert peak > 1


def test_a_single_job_does_not_get_a_thread():
    """One piece of work should not pay for a pool, and its stack stays readable."""
    where: list[str] = []
    concurrency.run_bounded(
        [concurrency.Job(key="only", run=lambda: where.append(threading.current_thread().name) or [])],
        limit=4,
    )

    assert where == [threading.current_thread().name]


def test_a_limit_of_one_runs_in_series():
    ran: list[str] = []
    concurrency.run_bounded([job(str(n), record=ran) for n in range(3)], limit=1)

    assert ran == ["0", "1", "2"]


# ----------------------------------------------------------------- exceptions


def test_an_unexpected_error_is_raised_once_the_pool_has_drained():
    finished: list[str] = []

    def boom():
        raise RuntimeError("sqlpackage vanished")

    jobs = [
        concurrency.Job(key="a", run=boom),
        job("b", record=finished, delay=0.02),
        job("c", record=finished, delay=0.02),
    ]
    with pytest.raises(RuntimeError, match="sqlpackage vanished"):
        concurrency.run_bounded(jobs, limit=3)

    # The others were not abandoned halfway through writing a schema.
    assert sorted(finished) == ["b", "c"]


def test_cancellation_reaches_the_caller():
    def cancelled():
        raise CancelledError("stopped")

    with pytest.raises(CancelledError):
        concurrency.run_bounded(
            [concurrency.Job(key="a", run=cancelled), job("b")], limit=2
        )


def test_the_first_error_is_the_one_raised():
    def boom(message):
        def run():
            raise RuntimeError(message)

        return run

    jobs = [concurrency.Job(key=str(n), run=boom(f"error {n}")) for n in range(3)]
    with pytest.raises(RuntimeError, match="error"):
        concurrency.run_bounded(jobs, limit=3)


# ------------------------------------------------------------------ progress


def test_progress_counts_rather_than_naming_whichever_finished_last():
    seen: list[str] = []
    concurrency.run_bounded(
        [job(str(n)) for n in range(3)], limit=2, on_progress=seen.append, noun="schema transfer"
    )

    # A single detail line cannot carry three workers' messages, so it carries a count.
    assert seen[-1] == "3 of 3 schema transfer(s) finished"
    assert seen[0] == "0 of 3 schema transfer(s) finished"


def test_progress_in_series_counts_the_same_way():
    seen: list[str] = []
    concurrency.run_bounded(
        [job(str(n)) for n in range(3)], limit=1, on_progress=seen.append, noun="file copy"
    )

    assert seen[-1] == "3 of 3 file copy(s) finished"


def test_a_lone_job_does_not_report_a_count():
    seen: list[str] = []
    concurrency.run_bounded([job("only")], limit=2, on_progress=seen.append)

    assert seen == []


# -------------------------------------------------------------------- guarded


def test_a_guarded_job_checks_before_it_starts():
    def refuse():
        raise CancelledError("stopped")

    guarded = concurrency.guarded(lambda: ["never"], before=refuse)
    with pytest.raises(CancelledError):
        guarded()


def test_a_guarded_job_runs_when_the_check_passes():
    guarded = concurrency.guarded(lambda: ["done"], before=lambda: None)

    assert guarded() == ["done"]


def test_bounded_map_refills_while_first_result_is_blocked_and_yields_in_order():
    release_first = threading.Event()
    replacement = threading.Event()
    first_started = threading.Event()

    def read(index):
        if index == 0:
            first_started.set()
            assert release_first.wait(5)
        if index >= 4:
            replacement.set()
        return index

    with ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(lambda: list(concurrency.bounded_map(read, range(10))))
        try:
            assert first_started.wait(5)
            assert replacement.wait(5)
            assert not result.done()
        finally:
            release_first.set()
        assert result.result(timeout=5) == list(range(10))


@pytest.mark.parametrize("error_type", [RuntimeError, CancelledError])
def test_bounded_map_stops_refilling_on_failure_and_drains_in_flight_reads(error_type, monkeypatch):
    full = threading.Event()
    release_error = threading.Event()
    release_others = threading.Event()
    observed_error = threading.Event()
    lock = threading.Lock()
    started = []
    finished = []
    original_wait = concurrency.wait

    def wait_for_completion(*args, **kwargs):
        done, pending = original_wait(*args, **kwargs)
        if any(future.exception() is not None for future in done):
            observed_error.set()
        return done, pending

    monkeypatch.setattr(concurrency, "wait", wait_for_completion)

    def read(index):
        with lock:
            started.append(index)
            if len(started) == 4:
                full.set()
        try:
            if index == 1:
                assert release_error.wait(5)
                raise error_type("stopped")
            assert release_others.wait(5)
            return index
        finally:
            with lock:
                finished.append(index)

    with ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(lambda: list(concurrency.bounded_map(read, range(10))))
        try:
            assert full.wait(5)
            release_error.set()
            assert observed_error.wait(5)
            assert not result.done()
        finally:
            release_others.set()
            release_error.set()
        with pytest.raises(error_type, match="stopped"):
            result.result(timeout=5)
    assert sorted(started) == sorted(finished) == [0, 1, 2, 3]
