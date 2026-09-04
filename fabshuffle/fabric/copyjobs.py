"""Copy Job construction, submission, and monitoring.

Copy Jobs are how Fab Shuffle moves table data between regions. The job definition is
built in Python (v1 kept it in ``CopyJobTemplates/*.json`` and patched it with string
replacement) and shipped as a base64 item definition.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from fabshuffle.config import SETTINGS
from fabshuffle.fabric.client import FabricClient, FabricError
from fabshuffle.fabric.data_stores import TableRef
from fabshuffle.fabric.definitions import part, platform_part

logger = logging.getLogger(__name__)

COPY_JOB_CONTENT_PART = "copyjob-content.json"
COPY_JOB_TIMEOUT = "0.12:00:00"

TERMINAL_JOB_STATES = frozenset({"Completed", "Failed", "Cancelled", "Deduped"})
SUCCESS_JOB_STATES = frozenset({"Completed"})


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
    return client.post(f"workspaces/{workspace_id}/copyJobs", json=body)


def start_copy_job(client: FabricClient, workspace_id: str, copy_job_id: str) -> str:
    """Kick off a Copy Job run and return the job instance id from the ``Location`` header."""
    response = client.request(
        "POST",
        f"workspaces/{workspace_id}/items/{copy_job_id}/jobs/CopyJob/instances",
        expected=(200, 202),
    )
    location = response.headers.get("Location", "")
    instance_id = location.rstrip("/").rsplit("/", 1)[-1].split("?")[0]
    if not instance_id:
        raise CopyJobFailed(f"Copy Job {copy_job_id} did not return a job instance location")
    return instance_id


def wait_for_copy_job(
    client: FabricClient,
    workspace_id: str,
    copy_job_id: str,
    instance_id: str,
    *,
    on_status: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    deadline = time.monotonic() + SETTINGS.copy_job_timeout_seconds
    last_status = ""

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
            reason = instance.get("failureReason") or {}
            raise CopyJobFailed(
                f"Copy Job {copy_job_id} ended as {status}: {reason.get('message') or reason}"
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
    instance_id = start_copy_job(client, workspace_id, copy_job_id)
    wait_for_copy_job(client, workspace_id, copy_job_id, instance_id, on_status=on_status)
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
    copy_job_id: str
    instance_id: str


def _job_status(client: FabricClient, job: _InFlight) -> str:
    instance = client.get(
        f"workspaces/{job.spec.workspace_id}/items/{job.copy_job_id}"
        f"/jobs/instances/{job.instance_id}"
    )
    return str(instance.get("status") or "NotStarted"), instance


def run_copy_jobs(
    client: FabricClient,
    specs: Sequence[CopyJobSpec],
    *,
    concurrency: int | None = None,
    on_progress: Callable[[str], None] | None = None,
    on_done: Callable[[CopyJobSpec], None] | None = None,
) -> tuple[list[tuple[str, str]], list[str]]:
    """Run a batch of Copy Jobs together rather than one after another.

    Starting a job and waiting for one are separate calls, so there is no reason to finish
    the first before starting the second. Several run at once and every one of them is polled
    in the same loop, which needs no threads: all of this is waiting on HTTP.

    Concurrency is bounded, and deliberately low. A Copy Job runs on the target capacity, so
    a dozen at once on a small SKU is not a dozen times faster; Fabric queues them, and past
    a point turns the over-subscription into a failed job rather than a slow one.

    ``on_done`` is called for each job that finished cleanly, and only those, so a caller can
    record what it will not need to repeat. A job that failed, timed out, or could not be read
    is left unrecorded on purpose: it has to be attempted again.

    Returns the ids of the jobs that were created, so the caller can clean them up, and a
    warning for each one that did not finish cleanly.
    """
    limit = max(1, concurrency or SETTINGS.copy_job_concurrency or 1)
    queue = list(specs)
    total = len(queue)
    created: list[tuple[str, str]] = []
    warnings: list[str] = []
    in_flight: list[_InFlight] = []
    finished = 0

    def report() -> None:
        if on_progress and total:
            running = len(in_flight)
            on_progress(
                f"{finished} of {total} copy job(s) finished"
                + (f", {running} running" if running else "")
            )

    deadline = time.monotonic() + SETTINGS.copy_job_timeout_seconds

    while queue or in_flight:
        while queue and len(in_flight) < limit:
            spec = queue.pop(0)
            try:
                copy_job = create_copy_job(client, spec.workspace_id, spec.display_name, spec.content)
                copy_job_id = copy_job["id"]
                created.append((spec.workspace_id, copy_job_id))
                instance_id = start_copy_job(client, spec.workspace_id, copy_job_id)
            except (FabricError, CopyJobFailed) as error:
                finished += 1
                warnings.append(f"{spec.label} did not start: {error}")
                continue
            in_flight.append(_InFlight(spec, copy_job_id, instance_id))
        report()

        if not in_flight:
            continue

        time.sleep(SETTINGS.copy_job_poll_seconds)

        still_running: list[_InFlight] = []
        for job in in_flight:
            try:
                status, instance = _job_status(client, job)
            except FabricError as error:
                finished += 1
                warnings.append(f"{job.spec.label}: could not read the copy job's progress: {error}")
                continue

            if status not in TERMINAL_JOB_STATES:
                still_running.append(job)
                continue

            finished += 1
            if status not in SUCCESS_JOB_STATES:
                reason = instance.get("failureReason") or {}
                warnings.append(
                    f"{job.spec.label} did not copy: the job ended as {status}"
                    + (f": {reason.get('message') or reason}" if reason else "")
                )
            elif on_done:
                on_done(job.spec)
        in_flight = still_running

        if in_flight and time.monotonic() > deadline:
            for job in in_flight:
                warnings.append(
                    f"{job.spec.label} was still running after "
                    f"{SETTINGS.copy_job_timeout_seconds}s, so we stopped waiting. Check it in "
                    "the new workspace before copying anything by hand."
                )
            break

    if on_progress and total:
        on_progress(f"{total} of {total} copy job(s) finished")
    return created, warnings


__all__ = [
    "COPY_JOB_CONTENT_PART",
    "CopyJobFailed",
    "CopyJobSpec",
    "build_lakehouse_copy_job",
    "build_warehouse_copy_job",
    "create_copy_job",
    "run_copy_job",
    "run_copy_jobs",
    "start_copy_job",
    "wait_for_copy_job",
]
