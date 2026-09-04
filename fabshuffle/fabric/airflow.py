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
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import quote

from fabshuffle.fabric import analytics
from fabshuffle.fabric.client import FabricApiError, FabricClient, FabricError
from fabshuffle.fabric.definitions import (
    build_rewriter,
    decode_json_part,
    is_text_part,
    part,
)
from fabshuffle.lifecycle import EvidenceState, ItemLifecycle

logger = logging.getLogger(__name__)

APACHE_AIRFLOW_JOB = "ApacheAirflowJob"

BETA = {"beta": "true"}
PLATFORM_PART = ".platform"

# Refuse a job containing a larger file rather than pulling it through memory.
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


def preflight_references(
    parts: list[dict[str, Any]],
    *,
    source_job_id: str,
    job_name: str,
    id_map: Mapping[str, str],
    source_items: Mapping[str, Mapping[str, Any]],
) -> None:
    """Refuse operational self references unless an existing target ID can replace them."""
    own_item = {
        source_job_id: {"id": source_job_id, "type": APACHE_AIRFLOW_JOB, "displayName": job_name},
    }
    if analytics.dangling_references(parts, id_map, own_item):
        paths = ", ".join(str(candidate.get("path") or "?") for candidate in parts)
        raise FabricError(
            f"Apache Airflow job '{job_name}' contains its own source job ID in {paths}. "
            "The new job ID is not available before creation. Remove hardcoded self references "
            "from the source configuration or DAG, retry, then configure the destination job ID "
            "before running it"
        )
    needed = analytics.dangling_references(parts, id_map, source_items)
    if needed:
        raise analytics.StrandedReference(needed)


def preflight_files(
    client: FabricClient,
    *,
    source_workspace_id: str,
    source_job_id: str,
    job_name: str,
    id_map: Mapping[str, str],
    source_items: Mapping[str, Mapping[str, Any]],
    on_progress: Callable[[str], None] | None = None,
) -> list[tuple[str, bytes]]:
    """Read and rebind supported UTF-8 files before creating a job.

    Literal known IDs and endpoint/path strings only: this does not evaluate Python,
    environment lookups, imported packages, or dynamically assembled references.
    """
    prepared: list[tuple[str, bytes]] = []
    rewrite = build_rewriter(id_map)
    for entry in list_files(client, source_workspace_id, source_job_id):
        file_path = entry.get("filePath")
        if not file_path:
            raise FabricError(f"Apache Airflow job '{job_name}' returned a file without a path")
        if (entry.get("sizeInBytes") or 0) > MAX_FILE_BYTES:
            raise FabricError(
                f"Apache Airflow job '{job_name}': '{file_path}' is larger than "
                f"{MAX_FILE_BYTES // (1024 * 1024)} MB; move it out of the job, then retry"
            )
        if on_progress:
            on_progress(f"Inspecting '{file_path}' for Apache Airflow job '{job_name}'")
        content = read_file(client, source_workspace_id, source_job_id, file_path)
        if len(content) > MAX_FILE_BYTES:
            raise FabricError(
                f"Apache Airflow job '{job_name}': '{file_path}' exceeds the file size limit; "
                "move it out of the job, then retry"
            )
        if is_text_part(file_path):
            try:
                text = content.decode("utf-8")
            except UnicodeDecodeError as error:
                raise FabricError(
                    f"Apache Airflow job '{job_name}': '{file_path}' is not UTF-8; "
                    "convert this text file to UTF-8, then retry"
                ) from error
            preflight_references(
                [part(file_path, content)],
                source_job_id=source_job_id,
                job_name=job_name,
                id_map=id_map,
                source_items=source_items,
            )
            content = (rewrite(text) if rewrite else text).encode("utf-8")
        prepared.append((file_path, content))
    return prepared


def copy_files(
    client: FabricClient,
    *,
    source_workspace_id: str,
    source_job_id: str,
    target_workspace_id: str,
    target_job_id: str,
    job_name: str,
    on_progress: Callable[[str], None] | None = None,
    prepared_files: list[tuple[str, bytes]] | None = None,
    id_map: Mapping[str, str] | None = None,
    source_items: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[int, list[str]]:
    """Upload preflighted files, or preflight with explicit reference context first."""
    try:
        if prepared_files is None and (id_map is None or source_items is None):
            raise FabricError("file copying requires preflighted files or a migration reference map")
        files = prepared_files if prepared_files is not None else preflight_files(
            client,
            source_workspace_id=source_workspace_id,
            source_job_id=source_job_id,
            job_name=job_name,
            id_map=id_map or {},
            source_items=source_items or {},
            on_progress=on_progress,
        )
    except FabricError as error:
        return 0, [
            f"Apache Airflow job '{job_name}': files could not be prepared: {error}. "
            "Fix the source files or access, then retry."
        ]

    copied = 0
    warnings: list[str] = []
    for file_path, content in files:
        if on_progress:
            on_progress(f"Copying '{file_path}' for Apache Airflow job '{job_name}'")
        try:
            write_file(client, target_workspace_id, target_job_id, file_path, content)
            copied += 1
        except FabricApiError as error:
            warnings.append(
                f"Apache Airflow job '{job_name}': '{file_path}' did not copy "
                f"({error}). Fix file access and retry."
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


def configuration_warnings(
    parts: list[dict[str, Any]], job_name: str, *, lifecycle: ItemLifecycle | None = None,
) -> list[str]:
    """Settings that travel but will not work until someone acts on them."""
    if lifecycle:
        lifecycle.step(
            "configuration", EvidenceState.UNKNOWN, "Airflow configuration has not been inspected.",
            action="Inspect the target job configuration before running it.",
        )
    config = _config_part(parts)
    if not config:
        return []
    try:
        document = decode_json_part(config["payload"])
    except (ValueError, KeyError):
        return []
    if not isinstance(document, dict):
        return []

    if lifecycle:
        lifecycle.step(
            "configuration", EvidenceState.SKIPPED if document.get("secrets") else EvidenceState.SUCCEEDED,
            "Secret values were not transferred." if document.get("secrets")
            else "Configuration inspected; no omitted secret values identified.",
            action="Re-enter the target Airflow job's secrets before running it."
            if document.get("secrets") else "",
        )
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
            "Known literal references are rebound. Review dynamically constructed references "
            "and region settings before running it."
        )
    return warnings


__all__ = [
    "APACHE_AIRFLOW_JOB",
    "MAX_FILE_BYTES",
    "configuration_warnings",
    "copy_files",
    "list_files",
    "preflight_files",
    "preflight_references",
    "read_file",
    "retarget_location",
    "write_file",
]
