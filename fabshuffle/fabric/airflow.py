"""Apache Airflow job migration.

The item definition is configuration only: pool sizing, Airflow settings, environment
variables. The DAGs themselves are files behind a separate API, so a definition-only copy
produces a job with nothing to run. Both halves move here.

Two things about that file API are worth knowing. It is **beta**, so every request carries
``?beta=true``, and it is not the usual JSON-in-JSON-out shape: reads return the file's bytes
and writes send them, rather than a document. Listing returns paths already relative to the
job root (``dags/my_dag.py``), so there is no folder tree to walk.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any
from urllib.parse import quote

from fabshuffle.fabric.client import FabricApiError, FabricClient
from fabshuffle.fabric.definitions import decode_json_part, part

logger = logging.getLogger(__name__)

APACHE_AIRFLOW_JOB = "ApacheAirflowJob"

BETA = {"beta": "true"}
PLATFORM_PART = ".platform"

# Any file bigger than this is skipped rather than pulled through memory. DAGs are source
# files; something this large is data that does not belong in the job definition.
MAX_FILE_BYTES = 25 * 1024 * 1024


def _files_path(workspace_id: str, job_id: str) -> str:
    return f"workspaces/{workspace_id}/apacheAirflowJobs/{job_id}/files"


def list_files(client: FabricClient, workspace_id: str, job_id: str) -> list[dict[str, Any]]:
    """List every file in a job. Paths come back relative to the job root."""
    return client.list_all(_files_path(workspace_id, job_id), params=dict(BETA))


def read_file(client: FabricClient, workspace_id: str, job_id: str, file_path: str) -> bytes:
    response = client.request(
        "GET",
        f"{_files_path(workspace_id, job_id)}/{quote(file_path)}",
        params=dict(BETA),
    )
    return response.content


def write_file(
    client: FabricClient,
    workspace_id: str,
    job_id: str,
    file_path: str,
    content: bytes,
) -> None:
    client.request(
        "PUT",
        f"{_files_path(workspace_id, job_id)}/{quote(file_path)}",
        content=content,
        params=dict(BETA),
        headers={"Content-Type": "application/octet-stream"},
        expected=(200, 201, 202, 204),
    )


def copy_files(
    client: FabricClient,
    *,
    source_workspace_id: str,
    source_job_id: str,
    target_workspace_id: str,
    target_job_id: str,
    job_name: str,
    on_progress: Callable[[str], None] | None = None,
) -> tuple[int, list[str]]:
    """Copy a job's DAGs and supporting files. Returns how many moved, and what did not.

    A job whose files cannot be listed at all is reported loudly: the copy looks complete in
    the item list, and an Airflow job with no DAGs is not obviously broken until someone
    tries to run it.
    """
    try:
        files = list_files(client, source_workspace_id, source_job_id)
    except FabricApiError as error:
        return 0, [
            f"Apache Airflow job '{job_name}' was created, but its files could not be listed "
            f"(HTTP {error.status_code}), so it has no DAGs. Copy them across by hand."
        ]

    copied = 0
    warnings: list[str] = []
    for entry in files:
        file_path = entry.get("filePath")
        if not file_path:
            continue
        if (entry.get("sizeInBytes") or 0) > MAX_FILE_BYTES:
            warnings.append(
                f"Apache Airflow job '{job_name}': '{file_path}' is larger than "
                f"{MAX_FILE_BYTES // (1024 * 1024)} MB and was not copied."
            )
            continue

        if on_progress:
            on_progress(f"Copying '{file_path}' for Apache Airflow job '{job_name}'")
        try:
            content = read_file(client, source_workspace_id, source_job_id, file_path)
            write_file(client, target_workspace_id, target_job_id, file_path, content)
            copied += 1
        except FabricApiError as error:
            warnings.append(
                f"Apache Airflow job '{job_name}': '{file_path}' did not copy "
                f"(HTTP {error.status_code}). Copy it across by hand."
            )

    return copied, warnings


def _config_part(parts: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The job's configuration part.

    Named by extension rather than by filename: the REST article calls it
    ``ApacheAirflowJob.json`` and the definition article ``ApacheAirflowJobV1.json``, and a
    round trip should not care which this tenant returns.
    """
    for candidate in parts:
        path = candidate.get("path") or ""
        if path != PLATFORM_PART and path.lower().endswith(".json"):
            return candidate
    return None


def retarget_location(parts: list[dict[str, Any]], region: str) -> list[dict[str, Any]]:
    """Point the job's compute at the region it is moving to.

    ``computeProperties.location`` is a hardcoded region *display* name such as
    ``Central US``, so it needs the capacity's raw region string rather than the normalised
    one used elsewhere. Left alone, the job would keep asking for compute in the region we
    are migrating away from.
    """
    config = _config_part(parts)
    if not config or not region:
        return parts

    try:
        document = decode_json_part(config["payload"])
    except (ValueError, KeyError):
        return parts
    compute = document.get("computeProperties") if isinstance(document, dict) else None
    if not isinstance(compute, dict) or "location" not in compute:
        return parts

    compute["location"] = region
    return [part(config["path"], document) if p is config else p for p in parts]


def configuration_warnings(parts: list[dict[str, Any]], job_name: str) -> list[str]:
    """Settings that travel but will not work until someone acts on them."""
    config = _config_part(parts)
    if not config:
        return []
    try:
        document = decode_json_part(config["payload"])
    except (ValueError, KeyError):
        return []
    if not isinstance(document, dict):
        return []

    warnings: list[str] = []
    if document.get("secrets"):
        warnings.append(
            f"Apache Airflow job '{job_name}' defines secrets. Their values are not returned "
            "by the API, so re-enter them in the new workspace before running the job."
        )
    variables = document.get("environmentVariables")
    if isinstance(variables, dict) and variables:
        warnings.append(
            f"Apache Airflow job '{job_name}' sets {len(variables)} environment variable(s). "
            "They are copied as they are, so check any that name a workspace, an item, or a "
            "region: those still point at the old workspace."
        )
    return warnings


__all__ = [
    "APACHE_AIRFLOW_JOB",
    "MAX_FILE_BYTES",
    "configuration_warnings",
    "copy_files",
    "list_files",
    "read_file",
    "retarget_location",
    "write_file",
]
