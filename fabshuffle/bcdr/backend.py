"""Fenced durable effects. A lost response is not permission to retry a create."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4, uuid5

import httpx
from pydantic import JsonValue

from fabshuffle.bcdr.catalog import CatalogConflict, RecoveryCatalog
from fabshuffle.bcdr.contracts import (
    ControllerLease,
    ItemIdentity,
    OperationRecord,
    OperationState,
    RecoveryMode,
    canonical_json,
    reject_embedded_secrets,
)
from fabshuffle.fabric.client import (
    FabricApiError,
    FabricClient,
    FabricError,
    OperationFailed,
)
from fabshuffle.lifecycle import safe_text


class RecoveryBlocked(RuntimeError):
    """An actionable policy refusal; never represented as successful recovery."""


def now() -> datetime:
    return datetime.now(UTC)


def safe_error(error: FabricError) -> tuple[str | None, str, str | None]:
    code = error.error_code if isinstance(error, (FabricApiError, OperationFailed)) else None
    request_id = None
    if isinstance(error, FabricApiError):
        try:
            document = json.loads(error.body)
        except (ValueError, TypeError):
            document = {}
        if isinstance(document, dict):
            request_id = document.get("requestId")
    return code, safe_text(str(error)), safe_text(str(request_id)) if request_id else None


class DurableRuntime:
    def __init__(self, catalog: RecoveryCatalog, controller_id: str | None = None) -> None:
        self.catalog = catalog
        self.controller_id = str(UUID(controller_id)) if controller_id else str(uuid4())
        self.lease: ControllerLease | None = None
        self.mode: RecoveryMode | None = None
        self.current_operation: OperationRecord | None = None
        self.deployment_guard: Callable[[], None] | None = None

    def deployment_fence(self) -> None:
        if self.deployment_guard is not None:
            self.deployment_guard()

    def require_lease(self) -> ControllerLease:
        if self.lease is None:
            raise CatalogConflict("Acquire explicit controller ownership before mutation")
        return self.lease

    def fence(self) -> None:
        self.deployment_fence()
        lease = self.require_lease()
        state = self.catalog.state()
        if (
            state.controller_id != lease.controller_id
            or state.epoch != lease.epoch
            or state.mode != self.mode
        ):
            raise CatalogConflict("Controller ownership or recovery mode changed; stop this worker")

    @contextmanager
    def controller(self, modes: set[RecoveryMode]) -> Iterator[None]:
        self.deployment_fence()
        self.lease = self.catalog.acquire_controller(self.controller_id)
        self.mode = self.catalog.state().mode
        try:
            if self.mode not in modes:
                raise RecoveryBlocked(
                    f"Operation not allowed in mode {self.mode}; use the explicit lifecycle action"
                )
            if self.catalog.pending_operations():
                raise RecoveryBlocked(
                    "Reconcile pending/ambiguous operations before starting another controller action"
                )
            yield
        finally:
            # On catalog loss or ambiguity ownership deliberately remains fenced. No timer steals it.
            self.deployment_fence()
            if self.mode != RecoveryMode.PARKING and not self.catalog.pending_operations():
                self.catalog.release_controller(self.require_lease())
                self.lease = None

    def transition(self, desired: RecoveryMode) -> None:
        self.fence()
        self.catalog.transition_mode(
            self.require_lease(),
            self.mode,
            desired,
            str(uuid4()),
        )
        self.mode = desired

    def get(self, namespace: str, key: str) -> dict[str, JsonValue] | None:
        row = self.catalog.get_record(namespace, key)
        return row.document if row else None

    def put(self, namespace: str, key: str, document: dict[str, JsonValue]) -> None:
        self.fence()
        row = self.catalog.get_record(namespace, key)
        self.catalog.put_record(
            self.require_lease(),
            namespace,
            key,
            document,
            expected_revision=row.revision if row else None,
        )

    def effect(
        self,
        kind: str,
        key: str,
        action: Callable[[], dict[str, JsonValue]],
        *,
        generation_id: str | None = None,
        source: ItemIdentity | None = None,
        target: ItemIdentity | None = None,
    ) -> dict[str, JsonValue]:
        self.fence()
        operation_id = str(uuid5(UUID(self.require_lease().recovery_set_id), f"{kind}:{key}"))
        previous = next((o for o in self.catalog.operations() if o.operation_id == operation_id), None)
        if previous:
            if previous.state == OperationState.SUCCEEDED:
                result = json.loads(previous.message or "{}")
                if not isinstance(result, dict):
                    raise RecoveryBlocked(
                        "Stored effect result is not an object; inspect the operation journal"
                    )
                return result
            if previous.state == OperationState.FAILED:
                operation_id = str(
                    uuid5(
                        UUID(self.require_lease().recovery_set_id),
                        f"{kind}:{key}:retry:{self.require_lease().epoch}",
                    )
                )
            else:
                raise RecoveryBlocked(
                    f"Operation {operation_id} is {previous.state}; "
                    "reconcile its exact target before another attempt"
                )
        intent = OperationRecord(
            operation_id=operation_id,
            kind=kind,
            state=OperationState.INTENT,
            recorded_at=now(),
            generation_id=generation_id,
            source=source,
            target=target,
            ownership_evidence=f"controller={self.controller_id};epoch={self.require_lease().epoch};key={key}",
        )
        self.catalog.begin_operation(self.require_lease(), intent)
        return self._perform(intent, action)

    def resume_effect(
        self,
        operation: OperationRecord,
        action: Callable[[], dict[str, JsonValue]],
    ) -> dict[str, JsonValue]:
        """Continue a journaled effect only after its exact service receipt was reconciled."""
        self.fence()
        pending = {row.operation_id: row for row in self.catalog.pending_operations()}
        if pending.get(operation.operation_id) != operation or operation.target is None:
            raise RecoveryBlocked(
                "Reconcile an exact pending operation and returned target before continuing"
            )
        return self._perform(operation, action)

    def _perform(
        self,
        intent: OperationRecord,
        action: Callable[[], dict[str, JsonValue]],
    ) -> dict[str, JsonValue]:
        self.current_operation = intent
        try:
            self.fence()
            result = action()
            self.fence()
        except FabricError as error:
            self.fence()
            code, message, request_id = safe_error(error)
            # Even a definite rejection may follow earlier successful calls in a composite adapter.
            observed = self.current_operation or intent
            rejected = (
                observed.state == OperationState.INTENT
                and isinstance(error, FabricApiError)
                and 400 <= error.status_code < 500
                and error.status_code != 408
            )
            self.catalog.record_operation(
                self.require_lease(),
                observed.model_copy(
                    update={
                        "state": OperationState.FAILED if rejected else OperationState.AMBIGUOUS,
                        "recorded_at": now(),
                        "error_code": code,
                        "message": message,
                        "request_id": request_id,
                    }
                ),
            )
            action = (
                "; correct the service-reported problem before retrying"
                if rejected
                else "; reconcile the recorded operation before retrying"
            )
            raise RecoveryBlocked(message + action) from error
        finally:
            observed = self.current_operation or intent
            self.current_operation = None
        encoded = canonical_json(result)
        reject_embedded_secrets(encoded)
        refused = "applied" in result and result["applied"] is None
        if refused and observed.state != OperationState.INTENT:
            self.catalog.record_operation(
                self.require_lease(),
                observed.model_copy(
                    update={
                        "state": OperationState.AMBIGUOUS,
                        "recorded_at": now(),
                        "message": "Adapter incomplete after a service effect; reconcile owned resources",
                    }
                ),
            )
            raise RecoveryBlocked("Reconcile the partial adapter operation before another attempt")
        if isinstance(result.get("applied"), dict):
            applied_target = ItemIdentity.model_validate(result["applied"]["target"])
            if observed.target is not None and observed.target != applied_target:
                raise RecoveryBlocked("Adapter returned a different target than the committed update intent")
            observed = observed.model_copy(update={"target": applied_target})
        self.catalog.record_operation(
            self.require_lease(),
            observed.model_copy(
                update={
                    "state": OperationState.FAILED if refused else OperationState.SUCCEEDED,
                    "recorded_at": now(),
                    "message": encoded.decode("utf-8"),
                }
            ),
        )
        return result


class FencedFabricClient(FabricClient):
    """Every adapter REST mutation checks the live catalog, including compound operations."""

    def __init__(self, client: FabricClient, runtime: DurableRuntime) -> None:
        self.client = client
        self.runtime = runtime
        self.base_url = client.base_url

    def tenant_id(self) -> str:
        return self.client.tenant_id()

    @property
    def application_id(self) -> str:
        return self.client.application_id

    def close(self) -> None:
        self.client.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        json: Any | None = None,
        content: bytes | None = None,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        expected: Sequence[int] | None = None,
    ) -> httpx.Response:
        # getDefinition is a read-only POST; it does not need a mutation intent.
        mutation = method.upper() not in {"GET", "HEAD", "OPTIONS"} and not path.endswith("/getDefinition")
        if mutation:
            self.runtime.fence()
            if self.runtime.current_operation is None:
                raise CatalogConflict("Commit an operation intent before a Fabric mutation")
        response = self.client.request(
            method,
            path,
            json=json,
            content=content,
            params=params,
            headers=headers,
            expected=expected,
        )
        if mutation:
            operation = self.runtime.current_operation
            observed = operation.model_copy(
                update={
                    "state": OperationState.RUNNING,
                    "recorded_at": now(),
                    "service_operation_id": response.headers.get("x-ms-operation-id"),
                    "request_id": response.headers.get("requestId")
                    or response.headers.get("x-ms-request-id"),
                }
            )
            self.runtime.catalog.record_operation(self.runtime.require_lease(), observed)
            self.runtime.current_operation = observed
        return response
