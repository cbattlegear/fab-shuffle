"""Running a handful of blocking jobs at once.

Most of a migration is waiting on HTTP, and that is handled by submitting work and polling
it in one loop rather than by threads. A few things cannot be: sqlpackage and unpackdacpac
are subprocesses, the schema deploy is a batch at a time over ODBC, and azcopy stages files
through local disk. Those genuinely block a thread, so they get a pool.

Two rules make that safe to read afterwards.

**The limit is small and deliberate.** sqlpackage is a .NET process of a few hundred
megabytes, and azcopy tunes its own concurrency and stages whole directories locally, so
several at once compete for the same memory and disk rather than going faster.

**Output does not depend on timing.** Warnings are collected per job and sorted by a stable
key before being returned, so two runs of the same workspace read the same way, and progress
is a count rather than a line each worker overwrites.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Job:
    """One piece of blocking work, and the name it sorts and reports under."""

    key: str
    run: Callable[[], list[str]]


def run_bounded(
    jobs: Sequence[Job],
    *,
    limit: int,
    on_progress: Callable[[str], None] | None = None,
    noun: str = "job",
) -> list[str]:
    """Run ``jobs`` at most ``limit`` at a time, and return their warnings in a fixed order.

    A job reports what went wrong by returning warnings, so one failing does not cost the
    others. Anything a job actually raises is unexpected: the pool is drained first so no
    thread is left running, and then the first such error is re-raised.
    """
    if not jobs:
        return []
    if len(jobs) == 1 or limit <= 1:
        return _run_in_series(jobs, on_progress=on_progress, noun=noun)

    total = len(jobs)
    finished = 0
    results: dict[str, list[str]] = {}
    failure: BaseException | None = None

    def report() -> None:
        if on_progress:
            on_progress(f"{finished} of {total} {noun}(s) finished")

    with ThreadPoolExecutor(max_workers=min(limit, total)) as pool:
        futures: dict[Future[list[str]], Job] = {pool.submit(job.run): job for job in jobs}
        report()

        for future in as_completed(futures):
            job = futures[future]
            finished += 1
            try:
                results[job.key] = future.result()
            except BaseException as error:
                logger.exception("%s '%s' failed", noun, job.key)
                results[job.key] = []
                if failure is None:
                    failure = error
            report()

    if failure is not None:
        raise failure

    # Sorted by key rather than by whichever thread finished first, so the same workspace
    # produces the same output twice running.
    return [warning for key in sorted(results) for warning in results[key]]


def _run_in_series(
    jobs: Sequence[Job],
    *,
    on_progress: Callable[[str], None] | None,
    noun: str,
) -> list[str]:
    """Run without a pool at all, for one job or a limit of one.

    Worth keeping separate: a single job should not pay for a thread, and it keeps the
    stack readable when something goes wrong with the only piece of work there was.
    """
    results: dict[str, list[str]] = {}
    for index, job in enumerate(jobs, start=1):
        if on_progress and len(jobs) > 1:
            on_progress(f"{index - 1} of {len(jobs)} {noun}(s) finished")
        results[job.key] = job.run()
    if on_progress and len(jobs) > 1:
        on_progress(f"{len(jobs)} of {len(jobs)} {noun}(s) finished")
    return [warning for key in sorted(results) for warning in results[key]]


def guarded(run: Callable[[], list[str]], *, before: Callable[[], Any]) -> Callable[[], list[str]]:
    """Wrap a job so it checks something before it starts.

    Used for cancellation: a job that has not begun should not begin, and one already running
    finishes rather than being abandoned halfway through writing a schema.
    """

    def wrapped() -> list[str]:
        before()
        return run()

    return wrapped


__all__ = ["Job", "guarded", "run_bounded"]
