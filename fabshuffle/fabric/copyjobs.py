"""Copy Job construction, submission, and monitoring.

Copy Jobs are how Fab Shuffle moves table data between regions. The job definition is
built in Python (v1 kept it in ``CopyJobTemplates/*.json`` and patched it with string
replacement) and shipped as a base64 item definition.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable, Iterable
from typing import Any

from fabshuffle.config import SETTINGS
from fabshuffle.fabric.client import FabricClient
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
    """Create, run, and await a Copy Job. Returns the created Copy Job item id."""
    copy_job = create_copy_job(client, workspace_id, display_name, content)
    copy_job_id = copy_job["id"]
    instance_id = start_copy_job(client, workspace_id, copy_job_id)
    wait_for_copy_job(client, workspace_id, copy_job_id, instance_id, on_status=on_status)
    return copy_job_id


__all__ = [
    "COPY_JOB_CONTENT_PART",
    "CopyJobFailed",
    "build_lakehouse_copy_job",
    "build_warehouse_copy_job",
    "create_copy_job",
    "run_copy_job",
    "start_copy_job",
    "wait_for_copy_job",
]
