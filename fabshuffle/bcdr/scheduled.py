"""Noninteractive metadata synchronization: fresh standby only, never incident recovery."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from pathlib import Path

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.contracts import RecoveryMode
from fabshuffle.bcdr.deployment_lock import LEASE_ENV, validate_blob_url
from fabshuffle.bcdr.service import BcdrService, ServiceResult, SyncRequest, create_service

MAX_REQUEST_BYTES = 128 * 1024


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Scheduled request contains duplicate JSON keys; remove duplicates")
        result[key] = value
    return result


def read_request(path: Path) -> SyncRequest:
    with path.open("rb") as stream:
        raw = stream.read(MAX_REQUEST_BYTES + 1)
    if len(raw) > MAX_REQUEST_BYTES:
        raise ValueError("Scheduled request exceeds 128 KiB; reduce the approved request file")
    # Validate strict JSON before Pydantic, which otherwise accepts duplicate keys/NaN.
    try:
        json.loads(raw, object_pairs_hook=_unique_object, parse_constant=lambda _: _invalid_constant())
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Scheduled request must contain valid UTF-8 JSON") from error
    request = SyncRequest.model_validate_json(raw)
    validate_request(request)
    return request


def _invalid_constant() -> None:
    raise ValueError("Scheduled request must contain finite JSON numbers")


def validate_request(request: SyncRequest) -> None:
    if request.capture is not True or request.park is not True:
        raise ValueError("Scheduled synchronization requires capture=true and park=true")


def scheduled_sync(
    bootstrap_path: Path, request: SyncRequest, *, target_tokens: TokenProvider,
    service_factory: Callable[..., BcdrService] = create_service,
) -> ServiceResult:
    validate_request(request)
    url = os.environ.get(LEASE_ENV)
    if not url:
        raise RecoveryBlocked(
            f"Scheduled synchronization requires {LEASE_ENV}, shared exactly with the web controller; "
            "local/Azure Files locks do not coordinate Container Apps Job executions"
        )
    validate_blob_url(url)
    service = service_factory(
        bootstrap_path, target_tokens=target_tokens, source_tokens=target_tokens,
    )
    try:
        coordinator = service.coordinator
        lock = coordinator.capacities.lock
        if lock is None or lock.remote is None:
            raise RecoveryBlocked("Scheduled synchronization did not acquire its required distributed lease")
        lock.assert_held()
        state = coordinator.catalog.state()
        pending = coordinator.catalog.pending_operations()
        if state.mode != RecoveryMode.STANDBY or state.controller_id is not None or pending:
            return ServiceResult(
                mode=state.mode, outcome="blocked",
                warnings=(
                    "Scheduled synchronization skipped: return to unowned STANDBY and explicitly "
                    "reconcile pending/abandoned operations before the next run. "
                    "No capture or pause occurred.",
                ),
                details={
                    "scheduled_status": "skipped",
                    "controller_id": state.controller_id,
                    "pending_operation_ids": [row.operation_id for row in pending],
                },
            )
        # Warehouse acquire_controller rejects *any* persisted owner, even our own
        # deployment ID. The mode is rechecked under that epoch before source capture.
        result = service._call(lambda: coordinator.synchronize(request, standby_only=True))
        lock.assert_held()
        return result.model_copy(update={
            "details": {**result.details, "scheduled_status": "completed"},
        })
    finally:
        service.close()
