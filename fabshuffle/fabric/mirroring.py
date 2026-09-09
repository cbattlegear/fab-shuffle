"""Explicit destination-only activation of an already-created database mirror."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping

from fabshuffle.config import SETTINGS
from fabshuffle.fabric.client import FabricClient, FabricError, OperationFailed


def status(client: FabricClient, workspace_id: str, database_id: str) -> str:
    """Read status without hiding access errors or a mirroring error inside HTTP 200."""
    result = client.post(
        f"workspaces/{workspace_id}/mirroredDatabases/{database_id}/getMirroringStatus"
    )
    if not isinstance(result, Mapping):
        raise FabricError("Get mirroring status returned no status object.")
    if result.get("error"):
        if not isinstance(result["error"], Mapping):
            raise FabricError("Get mirroring status returned an unrecognised error object.")
        raise OperationFailed(database_id, str(result.get("status") or "unknown"), result["error"])
    value = result.get("status")
    if not isinstance(value, str) or not value:
        raise FabricError("Get mirroring status returned no status value.")
    return value


def ensure_running(
    client: FabricClient, workspace_id: str, database_id: str, *,
    check_cancel: Callable[[], None],
    on_progress: Callable[[str], None],
) -> None:
    """Submit at most one start, then require observed Running (not data catch-up).

    Start/status endpoints both support service principals. Starting mirrors are polled,
    not started again. Initializing and Stopping are documented transitions, so wait before
    starting. Other states are sent to the service rather than predicted to fail.
    https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/start-mirroring
    https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/get-mirroring-status
    """
    deadline = time.monotonic() + SETTINGS.lro_timeout_seconds
    submitted = False
    while True:
        check_cancel()
        current = status(client, workspace_id, database_id)
        on_progress(f"Destination mirroring status: {current}")
        if current == "Running":
            return
        if not submitted and current not in ("Initializing", "Stopping", "Starting"):
            check_cancel()
            client.post(f"workspaces/{workspace_id}/mirroredDatabases/{database_id}/startMirroring")
            submitted = True
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Destination mirror {database_id} did not report Running within "
                f"{SETTINGS.lro_timeout_seconds}s (last status: {current}). "
                "Inspect mirroring in the destination before retrying; a start request may have taken effect."
            )
        # Small waits keep operator cancellation responsive even with a long poll interval.
        wait_until = min(deadline, time.monotonic() + max(1, SETTINGS.lro_poll_seconds))
        while time.monotonic() < wait_until:
            check_cancel()
            time.sleep(max(0, min(0.5, wait_until - time.monotonic())))
