"""Copy Job construction, submission, and monitoring.

Copy Jobs are how Fab Shuffle moves table data between regions. The job definition is
built in Python (v1 kept it in ``CopyJobTemplates/*.json`` and patched it with string
replacement) and shipped as a base64 item definition.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Literal, NoReturn, ParamSpec
from urllib.parse import urlsplit

from fabshuffle.config import SETTINGS
from fabshuffle.fabric.client import (
    FabricApiError,
    FabricClient,
    FabricError,
    OperationFailed,
    _retry_after_seconds,
)
from fabshuffle.fabric.data_stores import TableRef
from fabshuffle.fabric.definitions import part, platform_part

logger = logging.getLogger(__name__)

COPY_JOB_CONTENT_PART = "copyjob-content.json"
COPY_JOB_TIMEOUT = "0.12:00:00"
MAX_STATUS_READ_FAILURES = 3

# https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/get-item-job-instance
# New statuses may be added; only these explicit terminal states release a running slot.
TERMINAL_JOB_STATES = frozenset({"Completed", "Failed", "Cancelled", "Deduped"})
SUCCESS_JOB_STATES = frozenset({"Completed"})
RUNNING_JOB_STATES = frozenset({"NotStarted", "InProgress"})
_SubmissionState = Literal["unknown", "created", "submitting", "submitted"]
_CallbackArgs = ParamSpec("_CallbackArgs")


class CopyJobFailed(RuntimeError):
    """A Copy Job finished in a non-success state."""


def _lakehouse_connection(workspace_id: str, item_id: str) -> dict[str, Any]:
    return {
        "type": "Lakehouse",
        "typeProperties": {
            "workspaceId": workspace_id,
            "artifactId": item_id,
            "rootFolder": "Tables",
        },
    }


def _warehouse_connection(workspace_id: str, item_id: str, endpoint: str) -> dict[str, Any]:
    return {
        "type": "DataWarehouse",
        "typeProperties": {
            "workspaceId": workspace_id,
            "artifactId": item_id,
            "endPoint": endpoint,
        },
    }


def _dataset_settings(table: TableRef) -> dict[str, Any]:
    settings: dict[str, Any] = {"table": table.name}
    if table.schema:
        settings["schema"] = table.schema
    return settings


def _activity(table: TableRef, *, destination_extras: dict[str, Any], enable_staging: bool) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "properties": {
            "source": {"datasetSettings": _dataset_settings(table)},
            "destination": {**destination_extras, "datasetSettings": _dataset_settings(table)},
            "enableStaging": enable_staging,
            "translator": {"type": "TabularTranslator"},
            "typeConversionSettings": {
                "typeConversion": {"allowDataTruncation": True, "treatBooleanAsNumber": False}
            },
        },
    }


def build_lakehouse_copy_job(
    *,
    source_workspace_id: str,
    source_item_id: str,
    target_workspace_id: str,
    target_item_id: str,
    tables: Iterable[TableRef],
) -> dict[str, Any]:
    activities = [
        _activity(
            table,
            destination_extras={"partitionOption": "None", "writeBehavior": "Overwrite"},
            enable_staging=False,
        )
        for table in tables
    ]
    return {
        "properties": {
            "jobMode": "Batch",
            "source": {
                "type": "LakehouseTable",
                "connectionSettings": _lakehouse_connection(source_workspace_id, source_item_id),
            },
            "destination": {
                "type": "LakehouseTable",
                "connectionSettings": _lakehouse_connection(target_workspace_id, target_item_id),
            },
            "policy": {"timeout": COPY_JOB_TIMEOUT},
        },
        "activities": activities,
    }


def build_warehouse_copy_job(
    *,
    source_workspace_id: str,
    source_item_id: str,
    source_endpoint: str,
    target_workspace_id: str,
    target_item_id: str,
    target_endpoint: str,
    tables: Iterable[TableRef],
) -> dict[str, Any]:
    activities = [
        _activity(table, destination_extras={"tableOption": "autoCreate"}, enable_staging=True)
        for table in tables
    ]
    return {
        "properties": {
            "jobMode": "Batch",
            "source": {
                "type": "DataWarehouseTable",
                "connectionSettings": _warehouse_connection(
                    source_workspace_id, source_item_id, source_endpoint
                ),
            },
            "destination": {
                "type": "DataWarehouseTable",
                "connectionSettings": _warehouse_connection(
                    target_workspace_id, target_item_id, target_endpoint
                ),
            },
            "policy": {"timeout": COPY_JOB_TIMEOUT},
        },
        "activities": activities,
    }


def create_copy_job(
    client: FabricClient,
    workspace_id: str,
    display_name: str,
    content: dict[str, Any],
) -> dict[str, Any]:
    body = {
        "displayName": display_name,
        "definition": {
            "parts": [
                part(COPY_JOB_CONTENT_PART, content),
                platform_part("CopyJob", display_name),
            ]
        },
    }
    copy_job = client.post(f"workspaces/{workspace_id}/copyJobs", json=body)
    if not isinstance(copy_job.get("id"), str) or not copy_job["id"]:
        raise CopyJobFailed("Copy Job creation did not return an item ID")
    return copy_job


def start_copy_job(client: FabricClient, workspace_id: str, copy_job_id: str) -> str:
    """Kick off a Copy Job run and return the job instance id from the ``Location`` header."""
    return _submit_copy_job(client, workspace_id, copy_job_id).instance_id


@dataclass(frozen=True, slots=True)
class _CopyJobSubmission:
    instance_id: str
    first_poll_seconds: float


def _submit_copy_job(
    client: FabricClient, workspace_id: str, copy_job_id: str,
) -> _CopyJobSubmission:
    response = client.request(
        "POST",
        f"workspaces/{workspace_id}/items/{copy_job_id}/jobs/CopyJob/instances",
        expected=(200, 202),
    )
    location = response.headers.get("Location", "")
    try:
        segments = urlsplit(location).path.rstrip("/").rsplit("/", 2)
    except ValueError as error:
        raise CopyJobFailed(
            f"Copy Job {copy_job_id} returned an invalid job instance location: {error}"
        ) from error
    if len(segments) < 2 or segments[-2] != "instances" or not segments[-1]:
        raise CopyJobFailed(f"Copy Job {copy_job_id} did not return a job instance location")
    # The job scheduler requires waiting at least this long before the first status GET.
    # https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/run-on-demand-item-job
    return _CopyJobSubmission(
        segments[-1],
        max(SETTINGS.copy_job_poll_seconds, _retry_after_seconds(response, 0.0)),
    )


def wait_for_copy_job(
    client: FabricClient,
    workspace_id: str,
    copy_job_id: str,
    instance_id: str,
    *,
    on_status: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    return _wait_for_copy_job(client, workspace_id, copy_job_id, instance_id, on_status=on_status)


def _wait_for_copy_job(
    client: FabricClient,
    workspace_id: str,
    copy_job_id: str,
    instance_id: str,
    *,
    on_status: Callable[[str], None] | None = None,
    first_poll_seconds: float = 0.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + SETTINGS.copy_job_timeout_seconds
    last_status = ""

    if first_poll_seconds:
        time.sleep(min(first_poll_seconds, max(0.0, deadline - time.monotonic())))
        if time.monotonic() >= deadline:
            raise CopyJobFailed(
                f"Copy Job {copy_job_id}, instance {instance_id} could not be polled before "
                f"the waiting budget of {SETTINGS.copy_job_timeout_seconds}s expired. "
                "Completion is unknown; check this run in Fabric before retrying or cleaning up."
            )

    while True:
        instance = client.get(
            f"workspaces/{workspace_id}/items/{copy_job_id}/jobs/instances/{instance_id}"
        )
        status = instance.get("status", "NotStarted")

        if status != last_status:
            last_status = status
            if on_status:
                on_status(status)

        if status in TERMINAL_JOB_STATES:
            if status in SUCCESS_JOB_STATES:
                return instance
            raise CopyJobFailed(
                f"Copy Job {copy_job_id} ended as {status}{_failure_detail(instance)}"
            )

        if time.monotonic() > deadline:
            raise CopyJobFailed(
                f"Copy Job {copy_job_id} was still {status} after {SETTINGS.copy_job_timeout_seconds}s"
            )
        time.sleep(SETTINGS.copy_job_poll_seconds)


def run_copy_job(
    client: FabricClient,
    workspace_id: str,
    display_name: str,
    content: dict[str, Any],
    *,
    on_status: Callable[[str], None] | None = None,
) -> str:
    """Create, run, and await a single Copy Job. Returns the created Copy Job item id."""
    copy_job = create_copy_job(client, workspace_id, display_name, content)
    copy_job_id = copy_job["id"]
    submission = _submit_copy_job(client, workspace_id, copy_job_id)
    _wait_for_copy_job(
        client, workspace_id, copy_job_id, submission.instance_id,
        on_status=on_status, first_poll_seconds=submission.first_poll_seconds,
    )
    return copy_job_id


@dataclass(frozen=True, slots=True)
class CopyJobSpec:
    """One Copy Job to run, and how to name it if it goes wrong."""

    workspace_id: str
    display_name: str
    content: dict[str, Any]
    label: str
    # The source item whose data this moves, so a caller can write down that it is done and
    # not spend hours doing it again. Not needed to run the job.
    item_id: str = ""


@dataclass(slots=True)
class _InFlight:
    spec: CopyJobSpec
    copy_job_id: str | None
    instance_id: str | None
    last_status: str = "Unknown"
    last_error: str = ""
    read_failures: int = 0
    next_poll_at: float = 0.0
    submission_state: _SubmissionState = "unknown"
    poll_not_before: float | None = None
    terminal_observed: bool = False

    def snapshot(self) -> CopyJobRun:
        return CopyJobRun(
            workspace_id=self.spec.workspace_id,
            copy_job_id=self.copy_job_id,
            instance_id=self.instance_id,
            item_id=self.spec.item_id,
            display_name=self.spec.display_name,
            label=self.spec.label,
            last_status=self.last_status,
            last_error=self.last_error,
            submission_state=self.submission_state,
            poll_not_before=self.poll_not_before,
        )


@dataclass(frozen=True, slots=True)
class CopyJobRun:
    """Recovery metadata for an unresolved run, without its definition or credentials."""

    workspace_id: str
    copy_job_id: str | None
    instance_id: str | None
    item_id: str
    display_name: str
    label: str
    last_status: str = "Unknown"
    last_error: str = ""
    submission_state: _SubmissionState = "unknown"
    # UTC Unix seconds survive process restarts; monotonic timestamps do not.
    poll_not_before: float | None = None


@dataclass(frozen=True, slots=True)
class CopyJobCounts:
    total: int
    succeeded: int
    failed: int
    failed_to_start: int
    running: int
    unknown: int
    not_started: int

    @property
    def finished(self) -> int:
        return self.succeeded + self.failed

    def summary(self) -> str:
        return (
            f"{self.finished} of {self.total} copy job(s) finished: "
            f"{self.succeeded} succeeded, {self.failed} failed, "
            f"{self.failed_to_start} failed to start, {self.running} running, "
            f"{self.unknown} unknown, {self.not_started} not started"
        )


class CopyJobBatchIncomplete(FabricError):
    """The batch cannot safely advance or clean up; remote work may still be running."""

    def __init__(
        self,
        reason: str,
        *,
        active_jobs: Sequence[CopyJobRun],
        not_started: Sequence[CopyJobSpec],
        created: Sequence[tuple[str, str]],
        warnings: Sequence[str],
        counts: CopyJobCounts,
    ) -> None:
        self.active_jobs = tuple(active_jobs)
        self.not_started = tuple(not_started)
        self.created = list(created)
        self.counts = counts
        self.warnings = list(warnings)
        for job in self.active_jobs:
            if job.submission_state == "created":
                outcome = "was created but has not been submitted"
                action = "Resume using this existing Copy Job; do not recreate it."
            elif job.last_status in TERMINAL_JOB_STATES:
                outcome = f"ended as {job.last_status}, but checkpointing is incomplete"
                action = "Resume this existing run to record its result before cleaning up."
            else:
                outcome = "completion is unknown"
                action = "Check this run in Fabric before retrying."
            self.warnings.append(
                f"{job.label}: {outcome} (workspace {job.workspace_id}, "
                f"Copy Job {job.copy_job_id or 'unknown'}, instance {job.instance_id or 'unknown'}, "
                f"last status {job.last_status})"
                + (f": {job.last_error}" if job.last_error else "")
                + f". {action} Keep its source, target and scratch workspace until it is reconciled."
            )
        for spec in self.not_started:
            if any(
                job.submission_state == "created"
                and (job.workspace_id, job.item_id, job.display_name)
                == (spec.workspace_id, spec.item_id, spec.display_name)
                for job in self.active_jobs
            ):
                continue
            self.warnings.append(
                f"{spec.label} was not started. Resume this transfer after reconciling "
                "the unresolved Copy Jobs."
            )
        super().__init__(
            f"Copy Job batch incomplete: {reason}. {counts.summary()}. "
            "Do not clean up or start replacement copies until existing runs are reconciled.\n"
            + "\n".join(self.warnings)
        )


def _failure_detail(instance: dict[str, Any]) -> str:
    reason = instance.get("failureReason")
    return f": {json.dumps(reason)}" if reason else ""


def _job_status(client: FabricClient, job: _InFlight) -> tuple[str, dict[str, Any]]:
    instance = client.get(
        f"workspaces/{job.spec.workspace_id}/items/{job.copy_job_id}"
        f"/jobs/instances/{job.instance_id}"
    )
    return str(instance.get("status") or "Unknown"), instance


def _submission_rejected(error: FabricError | CopyJobFailed) -> bool:
    # A server/transport timeout does not prove that a POST was rejected.
    return (
        isinstance(error, FabricApiError)
        and error.method.upper() == "POST"
        and 400 <= error.status_code < 500
        and error.status_code != 408
    ) or (isinstance(error, OperationFailed) and error.status == "Failed")


def run_copy_jobs(
    client: FabricClient,
    specs: Sequence[CopyJobSpec],
    *,
    concurrency: int | None = None,
    on_progress: Callable[[str], None] | None = None,
    on_done: Callable[[CopyJobSpec], None] | None = None,
    on_started: Callable[[CopyJobSpec, str, str], None] | None = None,
    on_state: Callable[[CopyJobRun], None] | None = None,
    resume_jobs: Sequence[CopyJobRun] = (),
) -> tuple[list[tuple[str, str]], list[str]]:
    """Run a batch of Copy Jobs together rather than one after another.

    Starting a job and waiting for one are separate calls, so there is no reason to finish
    the first before starting the second. Several run at once and every one of them is polled
    in the same loop, which needs no threads: all of this is waiting on HTTP.

    Concurrency is bounded, and deliberately low. A Copy Job runs on the target capacity, so
    a dozen at once on a small SKU is not a dozen times faster; Fabric queues them, and past
    a point turns the over-subscription into a failed job rather than a slow one.

    The timeout is a batch-wide waiting budget, including submission and reconciliation,
    checked between HTTP operations. A failed status read holds its concurrency slot; three
    consecutive exhausted reads or the batch deadline raise ``CopyJobBatchIncomplete`` with
    all unresolved and queued work. Unknown work must be reconciled, not blindly retried.

    ``on_state`` checkpoints intent before creation, the known created item, intent before
    submission, the accepted run with its wall-clock first-poll deadline, and terminal
    observations. A callback failure aborts with recovery metadata without invoking another
    callback on the error path. ``on_started`` remains available for older callers;
    ``on_done`` records confirmed successes only.

    ``resume_jobs`` adopts existing runs matching the current specs by workspace, source item
    and display name. Only an explicit ``created`` state may submit an existing item without
    a run ID; other missing IDs require reconciliation, never automatic recreation. The
    caller must scope those records to the current target incarnation.

    Each new run's first poll honors its submission's ``Retry-After`` header independently,
    so waiting for one run never delays another that is eligible for a status read.

    Returns the ids of the jobs that were created/adopted and warnings only once no work is
    unresolved or queued; this is the only result that permits cleanup.
    """
    limit = max(1, concurrency or SETTINGS.copy_job_concurrency or 1)
    queue = deque(specs)
    total = len(queue)
    created: list[tuple[str, str]] = []
    warnings: list[str] = []
    in_flight: list[_InFlight] = []
    pending: deque[_InFlight] = deque()
    succeeded = failed = failed_to_start = 0

    def counts() -> CopyJobCounts:
        active = [job for job in in_flight if not job.terminal_observed]
        running = sum(job.last_status in RUNNING_JOB_STATES for job in active)
        return CopyJobCounts(
            total, succeeded, failed, failed_to_start,
            running, len(active) - running, len(queue) + len(pending),
        )

    def stop(reason: str, *, cause: Exception | None = None) -> NoReturn:
        error = CopyJobBatchIncomplete(
            reason,
            active_jobs=[job.snapshot() for job in [*in_flight, *pending]],
            not_started=[job.spec for job in pending] + list(queue),
            created=created,
            warnings=warnings,
            counts=counts(),
        )
        if cause is not None:
            raise error from cause
        raise error

    def notify(
        name: str,
        callback: Callable[_CallbackArgs, None],
        *args: _CallbackArgs.args,
        **kwargs: _CallbackArgs.kwargs,
    ) -> None:
        try:
            callback(*args, **kwargs)
        except Exception as error:
            stop(
                f"{name} callback failed: {type(error).__name__}: {error}. "
                "Repair checkpoint/reporting storage before resuming these existing jobs",
                cause=error,
            )

    def checkpoint(job: _InFlight) -> None:
        if on_state:
            notify("on_state", on_state, job.snapshot())

    def report() -> None:
        if on_progress and total:
            notify("on_progress", on_progress, counts().summary())

    for saved in resume_jobs:
        matches = [
            spec for spec in queue
            if (spec.workspace_id, spec.item_id, spec.display_name)
            == (saved.workspace_id, saved.item_id, saved.display_name)
        ]
        if len(matches) != 1:
            raise FabricError(
                f"Recorded Copy Job {saved.copy_job_id or 'unknown'} does not uniquely match "
                "this batch's workspace, source item and display name. Reconcile the saved "
                "run against the current target before resuming."
            )
        spec = matches[0]
        queue.remove(spec)
        delay = max(
            SETTINGS.copy_job_poll_seconds,
            (saved.poll_not_before - time.time()) if saved.poll_not_before is not None else 0.0,
        )
        job = _InFlight(
            spec, saved.copy_job_id, saved.instance_id, saved.last_status, saved.last_error,
            next_poll_at=time.monotonic() + delay,
            submission_state=saved.submission_state,
            poll_not_before=saved.poll_not_before,
        )
        if saved.submission_state == "created" and saved.copy_job_id and not saved.instance_id:
            pending.append(job)
        else:
            in_flight.append(job)
        if saved.copy_job_id:
            created.append((spec.workspace_id, saved.copy_job_id))
    if any(not job.copy_job_id or not job.instance_id for job in in_flight):
        stop("a saved submission has no confirmed job/instance ID; reconcile it in Fabric before resuming")

    deadline = time.monotonic() + SETTINGS.copy_job_timeout_seconds

    while queue or pending or in_flight:
        if time.monotonic() >= deadline:
            stop(f"the batch waiting budget of {SETTINGS.copy_job_timeout_seconds}s expired")
        while (queue or pending) and len(in_flight) < limit:
            if time.monotonic() >= deadline:
                stop(f"the batch waiting budget of {SETTINGS.copy_job_timeout_seconds}s expired")
            if not pending:
                spec = queue.popleft()
                job = _InFlight(spec, None, None)
                in_flight.append(job)
                checkpoint(job)
                if time.monotonic() >= deadline:
                    stop(f"the batch waiting budget of {SETTINGS.copy_job_timeout_seconds}s expired")
                try:
                    copy_job = create_copy_job(client, spec.workspace_id, spec.display_name, spec.content)
                except (FabricError, CopyJobFailed) as error:
                    if _submission_rejected(error):
                        in_flight.remove(job)
                        failed_to_start += 1
                        warnings.append(
                            f"{spec.label} did not start: {error}. Correct the reported error and retry."
                        )
                        continue
                    job.last_error = str(error)
                    stop("a creation's outcome is unknown", cause=error)
                job.copy_job_id = copy_job["id"]
                created.append((spec.workspace_id, copy_job["id"]))
                job.submission_state = "created"
                job.last_status = "NotStarted"
                in_flight.remove(job)
                pending.append(job)
                checkpoint(job)

            if time.monotonic() >= deadline:
                stop(f"the batch waiting budget of {SETTINGS.copy_job_timeout_seconds}s expired")
            job = pending.popleft()
            copy_job_id = job.copy_job_id
            if not copy_job_id:
                pending.appendleft(job)
                stop("a saved creation has no item ID; reconcile it before resuming")
            job.submission_state = "submitting"
            job.last_status = "Unknown"
            in_flight.append(job)
            checkpoint(job)
            if time.monotonic() >= deadline:
                stop(f"the batch waiting budget of {SETTINGS.copy_job_timeout_seconds}s expired")
            try:
                submission = _submit_copy_job(client, job.spec.workspace_id, copy_job_id)
            except (FabricError, CopyJobFailed) as error:
                if _submission_rejected(error):
                    in_flight.remove(job)
                    failed_to_start += 1
                    warnings.append(
                        f"{job.spec.label} did not start: {error}. Correct the reported error and retry."
                    )
                    continue
                job.last_error = str(error)
                stop("a submission's outcome is unknown", cause=error)
            job.instance_id = submission.instance_id
            job.last_status = "NotStarted"
            job.last_error = ""
            job.submission_state = "submitted"
            job.next_poll_at = time.monotonic() + submission.first_poll_seconds
            job.poll_not_before = time.time() + submission.first_poll_seconds
            checkpoint(job)
            if on_started:
                notify("on_started", on_started, job.spec, copy_job_id, submission.instance_id)
        report()

        if not in_flight:
            continue

        now = time.monotonic()
        next_poll = min(job.next_poll_at for job in in_flight)
        time.sleep(min(max(0.0, next_poll - now), max(0.0, deadline - now)))

        for index, job in enumerate(list(in_flight)):
            if index and time.monotonic() >= deadline:
                stop(f"the batch waiting budget of {SETTINGS.copy_job_timeout_seconds}s expired")
            if time.monotonic() < job.next_poll_at:
                continue
            try:
                status, instance = _job_status(client, job)
            except FabricError as error:
                job.last_status = "Unknown"
                job.last_error = str(error)
                job.read_failures += 1
                job.next_poll_at = time.monotonic() + SETTINGS.copy_job_poll_seconds
                logger.warning(
                    "%s: progress is unknown for Copy Job %s, instance %s (read %s/%s): %s",
                    job.spec.label, job.copy_job_id, job.instance_id,
                    job.read_failures, MAX_STATUS_READ_FAILURES, error,
                )
                continue

            job.last_status = status
            job.last_error = ""
            job.read_failures = 0
            if status not in TERMINAL_JOB_STATES:
                job.next_poll_at = time.monotonic() + SETTINGS.copy_job_poll_seconds
                continue

            job.terminal_observed = True
            if status not in SUCCESS_JOB_STATES:
                failed += 1
                warnings.append(
                    f"{job.spec.label} did not copy: the job ended as {status}{_failure_detail(instance)}. "
                    "Correct the reported error and retry this transfer."
                )
            else:
                succeeded += 1
            if status in SUCCESS_JOB_STATES and on_done:
                notify("on_done", on_done, job.spec)
            checkpoint(job)
            in_flight.remove(job)

        report()
        if any(job.read_failures >= MAX_STATUS_READ_FAILURES for job in in_flight):
            stop(
                f"progress could not be read on {MAX_STATUS_READ_FAILURES} "
                "consecutive reconciliation attempts"
            )

    report()
    return created, warnings


__all__ = [
    "COPY_JOB_CONTENT_PART",
    "CopyJobBatchIncomplete",
    "CopyJobCounts",
    "CopyJobFailed",
    "CopyJobRun",
    "CopyJobSpec",
    "build_lakehouse_copy_job",
    "build_warehouse_copy_job",
    "create_copy_job",
    "run_copy_job",
    "run_copy_jobs",
    "start_copy_job",
    "wait_for_copy_job",
]
