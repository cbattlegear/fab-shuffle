"""Explicit Azure F-capacity controls and fail-closed catalog parking.

ARM and Fabric have different audiences. This client deliberately does not reuse
FabricClient's permissive absolute-URL/redirect transport.
"""

from __future__ import annotations

import logging
import math
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import parse_qs, urlsplit
from uuid import UUID, uuid4

import httpx

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.bootstrap import (
    ARM_VERSION,
    BootstrapDescriptor,
    BootstrapError,
    BootstrapStore,
    CapacityAuthorization,
    CapacityOperation,
    ParkingIntent,
    canonical_arm_id,
)
from fabshuffle.bcdr.catalog import RecoveryCatalog
from fabshuffle.bcdr.contracts import CatalogState, ControllerLease, RecoveryMode

ARM_BASE = "https://management.azure.com"
ARM_SCOPE = "https://management.azure.com/.default"
logger = logging.getLogger(__name__)
_POLL_PATH = re.compile(
    r"/subscriptions/([0-9a-f-]{36})/providers/microsoft\.fabric/locations/"
    r"([a-z0-9-]+)/operation(?:statuses|results)/([0-9a-f-]{36})", re.IGNORECASE,
)


class _ArmQueryLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.args, tuple):
            record.args = tuple(
                str(value.copy_with(query=None)) + "?[redacted]"
                if isinstance(value, httpx.URL) and value.host == "management.azure.com" and value.query
                else value
                for value in record.args
            )
        return True


# httpx logs full request URLs at INFO, including ARM's opaque polling context.
logging.getLogger("httpx").addFilter(_ArmQueryLogFilter())


def _https_url(url: str, host: str) -> Any:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https" or parsed.netloc.lower() != host
        or parsed.username is not None or parsed.password is not None or parsed.fragment
        or "%" in parsed.path or "\\" in url or any(ord(char) <= 32 for char in url)
    ):
        raise BootstrapError(f"Refusing an unsafe polling/request URL; only https://{host} is allowed")
    return parsed


def validate_poll_url(url: str, arm_resource_id: str, *, previous: str | None = None) -> str:
    """Allow documented regional Fabric operation URLs in this exact subscription.

    ARM's operation endpoint is not beneath the capacity resource ID. Restrict it
    to Microsoft.Fabric's documented operation path, then pin region and operation
    identity across followups. Both response headers are checked before any token.
    https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/resume
    """
    resource = canonical_arm_id(arm_resource_id)
    parsed = _https_url(url, "management.azure.com")
    match = _POLL_PATH.fullmatch(parsed.path)
    if not match or match[1].lower() != resource.split("/")[2]:
        raise BootstrapError("ARM polling URL is outside the authorized Fabric subscription/operation scope")
    UUID(match[3])
    # ARM says GET the returned monitoring URL, not rebuild its query using the
    # initiating request's version or an example's t/c keys. The exact host/path
    # and operation identity above/below bind this read; query context is opaque.
    # https://learn.microsoft.com/azure/azure-resource-manager/management/async-operations
    if previous:
        old = _POLL_PATH.fullmatch(urlsplit(validate_poll_url(previous, resource)).path)
        if old is None or (match[2].lower(), match[3].lower()) != (old[2].lower(), old[3].lower()):
            raise BootstrapError("ARM returned a polling URL for a different operation or region")
    return url


def retry_after(response: httpx.Response, fallback: float = 2.0) -> float:
    try:
        delay = float(response.headers.get("Retry-After", fallback))
        return max(0.0, delay) if math.isfinite(delay) else fallback
    except ValueError:
        return fallback


def response_body(response: httpx.Response) -> dict:
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


class CapacityError(RuntimeError):
    """Service error details, preserved independently of our interpretation."""

    def __init__(self, response: httpx.Response, *, context: str) -> None:
        body = response_body(response)
        error = body.get("error") if isinstance(body.get("error"), dict) else body
        self.error_code = str(error.get("code") or error.get("errorCode") or "")
        self.detail = str(error.get("message") or response.text or "")
        self.request_id = str(
            body.get("requestId") or response.headers.get("x-ms-request-id")
            or response.headers.get("request-id") or ""
        )
        self.status_code = response.status_code
        super().__init__(
            f"{context}: HTTP {self.status_code}; {self.error_code}: {self.detail}"
            + (f" (request ID {self.request_id})" if self.request_id else "")
        )


class CapacityOutcomeUnknown(BootstrapError):
    """Keep the intent and reconcile through ARM; do not blindly replay mutations."""


@dataclass(frozen=True)
class CapacityState:
    arm_resource_id: str
    state: str
    provisioning_state: str


class ArmCapacityClient:
    def __init__(
        self, tokens: TokenProvider, *, transport: httpx.BaseTransport | None = None,
        timeout_seconds: float = 900, max_attempts: int = 4,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        guard: Callable[[], None] | None = None,
    ) -> None:
        if timeout_seconds <= 0 or max_attempts < 1:
            raise ValueError("ARM polling timeout and retry count must be positive")
        self.tokens = tokens
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.sleep = sleep
        self.clock = clock
        self.guard = guard
        self.http = httpx.Client(transport=transport, timeout=60, follow_redirects=False)

    def close(self) -> None:
        self.http.close()

    def __enter__(self) -> ArmCapacityClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _request(
        self, method: str, url: str, capacity: CapacityAuthorization,
        *, polling: bool = False, authorize: Callable[[], None] | None = None,
    ) -> httpx.Response:
        resource = canonical_arm_id(capacity.arm_resource_id)
        parsed = _https_url(url, "management.azure.com")
        if polling:
            validate_poll_url(url, resource)
        elif (
            parsed.path.lower() not in {resource, resource + "/resume", resource + "/suspend"}
            or parsed.query != f"api-version={ARM_VERSION}"
        ):
            raise BootstrapError("ARM request is outside the explicitly authorized capacity")
        return self._send(method, url, authorize=authorize)

    def _send(
        self, method: str, url: str, *, authorize: Callable[[], None] | None = None,
    ) -> httpx.Response:
        for attempt in range(self.max_attempts):
            if self.guard is not None:
                self.guard()
            if authorize is not None:
                authorize()
            try:
                response = self.http.request(
                    method, url,
                    headers={"Authorization": f"Bearer {self.tokens.token(ARM_SCOPE)}"},
                )
            except httpx.TransportError as error:
                if method != "GET":
                    raise CapacityOutcomeUnknown(
                        "ARM mutation response was lost; reconcile the persisted intent before retrying"
                    ) from error
                if attempt + 1 == self.max_attempts:
                    raise
                self.sleep(min(2 ** attempt, 30))
                continue
            if self.guard is not None:
                self.guard()
            if (
                response.status_code == 429
                or (method == "GET" and response.status_code in {500, 502, 503, 504})
            ) and attempt + 1 < self.max_attempts:
                self.sleep(retry_after(response))
                continue
            if not response.is_success:
                raise CapacityError(response, context=f"ARM {method}")
            return response
        raise AssertionError("ARM request attempts exhausted")

    def _collection(self, path: str, version: str) -> list[dict]:
        url = f"{ARM_BASE}{path}?api-version={version}"
        result, seen = [], set()
        while url:
            parsed = _https_url(url, "management.azure.com")
            if parsed.path.lower() != path.lower() or url in seen:
                raise BootstrapError("ARM discovery continuation changed scope or repeated a page")
            seen.add(url)
            body = response_body(self._send("GET", url))
            rows = body.get("value")
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise BootstrapError("ARM discovery returned an invalid resource list; retry discovery")
            result.extend(rows)
            url = body.get("nextLink", "")
            if not isinstance(url, str):
                raise BootstrapError("ARM discovery returned an invalid continuation; retry discovery")
        return result

    def list_capacities(self) -> list[dict]:
        """Read the recovery principal's same-tenant Azure Fabric resources only.

        https://learn.microsoft.com/rest/api/resources/subscriptions/list
        https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/list-by-subscription
        """
        tenant = str(UUID(self.tokens.tenant_id()))
        resources = []
        for subscription in self._collection("/subscriptions", "2022-12-01"):
            try:
                subscription_id = str(UUID(subscription["subscriptionId"]))
                subscription_tenant = str(UUID(subscription["tenantId"]))
            except (KeyError, ValueError, TypeError, AttributeError) as error:
                raise BootstrapError("ARM discovery could not establish the subscription identity") from error
            if subscription_tenant != tenant:
                continue
            name = subscription.get("displayName") or "Unnamed subscription"
            path = f"/subscriptions/{subscription_id}/providers/Microsoft.Fabric/capacities"
            try:
                entries = self._collection(path, ARM_VERSION)
            except CapacityError as error:
                raise BootstrapError(
                    f"Could not list Fabric capacities in subscription {name!r}: {error}. "
                    "Check the recovery principal's capacity read access, then retry discovery."
                ) from error
            for entry in entries:
                resource_id = canonical_arm_id(str(entry.get("id", "")))
                if (
                    resource_id.split("/")[2] != subscription_id
                    or entry.get("type", "").lower() != "microsoft.fabric/capacities"
                    or resource_id.rsplit("/", 1)[-1] != str(entry.get("name", "")).lower()
                    or entry.get("sku", {}).get("tier") != "Fabric"
                ):
                    raise BootstrapError("ARM discovery returned an inconsistent Fabric capacity identity")
                resources.append({
                    "id": resource_id, "name": entry["name"], "location": entry.get("location", ""),
                    "subscriptionName": name, "resourceGroup": resource_id.split("/")[4],
                })
        return resources

    def get(self, capacity: CapacityAuthorization) -> CapacityState:
        response = self._request(
            "GET", f"{ARM_BASE}{capacity.arm_resource_id}?api-version={ARM_VERSION}", capacity,
        )
        body = response_body(response)
        if canonical_arm_id(str(body.get("id", ""))) != capacity.arm_resource_id:
            raise BootstrapError("ARM returned a different capacity resource identity")
        if (
            str(body.get("type", "")).lower() != "microsoft.fabric/capacities"
            or body.get("sku", {}).get("tier") != "Fabric"
            or not re.fullmatch(r"F[1-9][0-9]*", str(body.get("sku", {}).get("name", "")))
        ):
            raise BootstrapError("Only an explicitly authorized Azure Fabric F capacity can be controlled")
        properties = body.get("properties", {})
        return CapacityState(
            capacity.arm_resource_id, str(properties.get("state", "")),
            str(properties.get("provisioningState", "")),
        )

    def resume(
        self, capacity: CapacityAuthorization, *, owner_id: str,
        on_progress: Callable[[CapacityOperation], None],
        pending: CapacityOperation | None = None,
    ) -> CapacityState:
        return self._operate(capacity, "resume", owner_id, on_progress, pending=pending)

    def suspend(
        self, capacity: CapacityAuthorization, *, owner_id: str,
        on_progress: Callable[[CapacityOperation], None], authorize: Callable[[], None],
        pending: CapacityOperation | None = None,
    ) -> CapacityState:
        """Low-level suspension requires a live, fail-closed parking authorization."""
        authorize()
        return self._operate(
            capacity, "suspend", owner_id, on_progress, pending=pending, authorize=authorize,
        )

    def _poll_headers(
        self, response: httpx.Response, capacity: CapacityAuthorization, previous: str | None = None,
    ) -> tuple[str, str] | None:
        selected = None
        for header, kind in (("Location", "location"), ("Azure-AsyncOperation", "async")):
            url = response.headers.get(header)
            if url:
                try:
                    validate_poll_url(
                        url, capacity.arm_resource_id,
                        previous=previous or (selected[0] if selected else None),
                    )
                except (BootstrapError, ValueError):
                    # Log shape, not the URL: c/t and unknown query values can be signed.
                    try:
                        parsed = urlsplit(url)
                        query = parse_qs(parsed.query, keep_blank_values=True)
                        match = _POLL_PATH.fullmatch(parsed.path)
                    except ValueError:
                        query, match = {}, None
                    versions = query.get("api-version", [])
                    logger.warning(
                        "ARM polling header rejected: resource=%s header=%s http_status=%s "
                        "operation=%s region=%s api_versions=%s query_keys=%s",
                        capacity.arm_resource_id, header, response.status_code,
                        match[3] if match else "unrecognized", match[2] if match else "unrecognized",
                        [value if re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:-preview)?", value)
                         else "[nonstandard]" for value in versions],
                        [key if re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_-]{0,63}", key)
                         else "[nonstandard]" for key in query],
                    )
                    raise
                selected = (url, kind)
        return selected

    def _operate(
        self, capacity: CapacityAuthorization, action: str, owner_id: str,
        on_progress: Callable[[CapacityOperation], None], *,
        pending: CapacityOperation | None = None,
        authorize: Callable[[], None] | None = None,
    ) -> CapacityState:
        deadline = self.clock() + self.timeout_seconds
        owner_id = str(UUID(owner_id))
        desired = {"Active"} if action == "resume" else {"Suspended", "Paused"}
        operation = pending
        if operation is not None and (
            operation.owner_id != owner_id or operation.arm_resource_id != capacity.arm_resource_id
            or operation.action != action
        ):
            raise BootstrapError("Pending ARM operation ownership or resource scope does not match")
        state = self.get(capacity)
        if operation is None:
            if state.state in desired and state.provisioning_state == "Succeeded":
                return state
            allowed = {"Suspended", "Paused"} if action == "resume" else {"Active"}
            if state.state not in allowed or state.provisioning_state != "Succeeded":
                raise BootstrapError(
                    f"Capacity state {state.state!r} is not settled; reconcile before {action}"
                )
            operation = CapacityOperation(
                intent_id=str(uuid4()), owner_id=owner_id,
                arm_resource_id=capacity.arm_resource_id, action=action,
            )
            on_progress(operation)
            response = self._request(
                "POST", f"{ARM_BASE}{capacity.arm_resource_id}/{action}?api-version={ARM_VERSION}",
                capacity, authorize=authorize,
            )
            request_id = response.headers.get("x-ms-request-id")
            if request_id and re.fullmatch(r"[a-zA-Z0-9:_.-]{1,256}", request_id):
                operation = operation.model_copy(update={"request_id": request_id})
                on_progress(operation)
            try:
                poll = self._poll_headers(response, capacity)
            except (BootstrapError, ValueError) as error:
                raise CapacityOutcomeUnknown(
                    f"ARM {action} returned HTTP {response.status_code}, but its polling receipt "
                    f"could not be used: {error}. The operation may already have taken effect. "
                    "Reconcile the recorded intent; do not submit another operation. "
                    "See the server logs for the polling header details."
                ) from error
            if response.status_code == 202:
                if poll is None:
                    raise CapacityOutcomeUnknown("ARM accepted the operation without a usable polling URL")
                operation = operation.model_copy(update={
                    "phase": "accepted", "poll_url": poll[0], "poll_kind": poll[1],
                })
                on_progress(operation)
                self._wait(retry_after(response), deadline)
            elif response.status_code != 200:
                raise CapacityOutcomeUnknown(f"Unexpected ARM mutation response HTTP {response.status_code}")
        elif operation.phase == "failed":
            raise BootstrapError("The recorded ARM operation failed; reconcile it before issuing another")
        elif operation.phase == "intent":
            # A lost POST response cannot be replayed based only on an unchanged state.
            if state.state not in desired or state.provisioning_state != "Succeeded":
                raise CapacityOutcomeUnknown(
                    "ARM intent has no receipt and its outcome remains unknown; reconcile with the operator"
                )

        while operation.phase == "accepted":
            self._wait(0, deadline)
            response = self._request("GET", operation.poll_url, capacity, polling=True)
            poll = self._poll_headers(response, capacity, previous=operation.poll_url)
            if poll:
                operation = operation.model_copy(update={"poll_url": poll[0], "poll_kind": poll[1]})
                on_progress(operation)
            body = response_body(response)
            status = str(body.get("status", "")).lower()
            if status in {"failed", "canceled", "cancelled"}:
                on_progress(operation.model_copy(update={"phase": "failed"}))
                raise CapacityError(response, context=f"ARM {action} operation {status}")
            if status == "succeeded" or (
                operation.poll_kind == "location" and response.status_code in {200, 204} and not status
            ):
                break
            self._wait(retry_after(response), deadline)
        while True:
            state = self.get(capacity)
            if state.state in desired and state.provisioning_state == "Succeeded":
                on_progress(operation.model_copy(update={"phase": "succeeded"}))
                return state
            if state.state == "Failed" or state.provisioning_state in {"Failed", "Canceled"}:
                raise BootstrapError(
                    f"ARM capacity is {state.state}/{state.provisioning_state}; "
                    "inspect the recorded operation"
                )
            self._wait(2, deadline)

    def _wait(self, delay: float, deadline: float) -> None:
        if self.clock() + delay >= deadline:
            raise CapacityOutcomeUnknown(
                "ARM operation exceeded its wait budget; retain and reconcile its receipt"
            )
        self.sleep(delay)


@dataclass(frozen=True)
class PauseProof:
    recovery_set_id: str
    owner_id: str
    epoch: int
    mode: RecoveryMode
    drained: bool
    capacity_ids: tuple[str, ...]


class PauseGuard(Protocol):
    """Catalog implementation must serialize all competing mode transitions.

    enter_parking commits PARKING and drains/records final work under the current
    controller lease. assert_parking rereads authoritative mode, owner, epoch,
    dedicated capacity membership and pending operations. It must fail if unknown.
    PARKING remains durable until the next startup reconciles bootstrap receipts.
    """

    def enter_parking(self, owner_id: str, capacity_ids: tuple[str, ...]) -> PauseProof: ...
    def assert_parking(self, proof: PauseProof) -> PauseProof: ...


class CatalogPauseGuard:
    """Use an already-held catalog lease, never reacquire or release it for pause.

    read_authorized_capacities must read current dedicated-capacity authorization
    from the authoritative catalog, including any serving/unrelated-workspace
    exclusion checks. Bootstrap deployment configuration alone is not that proof.
    """

    def __init__(
        self, catalog: RecoveryCatalog, lease: ControllerLease, *,
        read_authorized_capacities: Callable[[], tuple[CapacityAuthorization, ...]],
    ) -> None:
        self.catalog = catalog
        self.lease = lease
        self.read_authorized_capacities = read_authorized_capacities

    def _owned_state(self) -> CatalogState:
        state = self.catalog.state()
        if (
            state.recovery_set_id != self.lease.recovery_set_id
            or state.controller_id != self.lease.controller_id or state.epoch != self.lease.epoch
        ):
            raise BootstrapError("Current catalog ownership no longer matches the parking controller lease")
        return state

    def _proof(self, capacity_ids: tuple[str, ...], mode: RecoveryMode) -> PauseProof:
        before = self._owned_state()
        authorizations = tuple(
            CapacityAuthorization.model_validate(capacity.model_dump())
            for capacity in self.read_authorized_capacities()
        )
        authorized_ids = tuple(capacity.arm_resource_id for capacity in authorizations)
        if (
            len(set(authorized_ids)) != len(authorized_ids)
            or len(set(capacity_ids)) != len(capacity_ids)
            or not capacity_ids or set(capacity_ids) != set(authorized_ids)
        ):
            raise BootstrapError("Authoritative dedicated recovery-capacity scope does not match parking")
        pending = self.catalog.pending_operations()
        after = self._owned_state()
        if before != after or after.mode != mode or pending:
            raise BootstrapError("Catalog mode changed or operations remain undrained; do not suspend")
        return PauseProof(
            after.recovery_set_id, self.lease.controller_id, after.epoch, after.mode, True, capacity_ids,
        )

    def enter_parking(self, owner_id: str, capacity_ids: tuple[str, ...]) -> PauseProof:
        if str(UUID(owner_id)) != self.lease.controller_id:
            raise BootstrapError("Parking requester does not hold the catalog controller lease")
        self._proof(capacity_ids, RecoveryMode.STANDBY)
        self.catalog.transition_mode(
            self.lease, RecoveryMode.STANDBY, RecoveryMode.PARKING, str(uuid4()),
        )
        return self._proof(capacity_ids, RecoveryMode.PARKING)

    def assert_parking(self, proof: PauseProof) -> PauseProof:
        current = self._proof(proof.capacity_ids, RecoveryMode.PARKING)
        if current != proof:
            raise BootstrapError("Parking proof no longer matches authoritative catalog ownership")
        return current


class CapacityCoordinator:
    def __init__(self, store: BootstrapStore, arm: ArmCapacityClient) -> None:
        self.store = store
        self.arm = arm

    def _identity(self, descriptor: BootstrapDescriptor) -> None:
        if (
            self.arm.tokens.tenant_id() != descriptor.tenant_id
            or str(UUID(self.arm.tokens.principal.client_id)) != descriptor.application_id
        ):
            raise BootstrapError("Authenticated tenant/application does not match the bootstrap owner")

    def _record(self, operation: CapacityOperation) -> None:
        def change(descriptor: BootstrapDescriptor) -> BootstrapDescriptor:
            operations = [op for op in descriptor.capacity_operations if op.intent_id != operation.intent_id]
            operations.append(operation)
            return descriptor.model_copy(update={"capacity_operations": tuple(operations)})

        self.store.update(change)

    def resume_catalog_capacity(self) -> BootstrapDescriptor:
        """Settle the recorded capacity operation before any SQL or provisioning."""
        descriptor = self.store.load()
        self._identity(descriptor)
        capacity = descriptor.capacity(descriptor.catalog_capacity_id)
        pending = [
            op for op in descriptor.capacity_operations
            if op.arm_resource_id == capacity.arm_resource_id and op.phase in {"intent", "accepted"}
        ]
        if len(pending) > 1:
            raise BootstrapError("Conflicting pending catalog-capacity operations need reconciliation")
        if pending and pending[0].action == "suspend":
            # Observe an existing suspend, never issue another suspend during startup.
            self.arm._operate(
                capacity, "suspend", descriptor.controller_id, self._record, pending=pending[0],
            )
            pending = []
        self.arm.resume(
            capacity, owner_id=descriptor.controller_id, on_progress=self._record,
            pending=pending[0] if pending else None,
        )
        return self.store.load()

    def resume_catalog(
        self, *, wait_sql: Callable[[BootstrapDescriptor], None],
        reconcile: Callable[[BootstrapDescriptor], None],
    ) -> BootstrapDescriptor:
        """Resume before the first SQL callback; reconcile before clearing receipts.

        wait_sql must actually establish TDS readiness and fail on timeout.
        reconcile must commit all observations to the catalog and resolve PARKING
        under its authoritative controller lease. Failure retains deployment state.
        """
        self.resume_catalog_capacity()
        descriptor = self.store.load()
        wait_sql(descriptor)
        descriptor = self.store.load()
        reconcile(descriptor)

        def clear_reconciled(current: BootstrapDescriptor) -> BootstrapDescriptor:
            if any(op.phase in {"intent", "accepted"} for op in current.capacity_operations):
                raise BootstrapError(
                    "Unsettled bootstrap operations remain; reconcile their outcomes before leaving PARKING"
                )
            return current.model_copy(update={"capacity_operations": (), "parking": None})

        return self.store.update(clear_reconciled)

    def park(self, *, owner_id: str, guard: PauseGuard) -> BootstrapDescriptor:
        descriptor = self.store.load()
        self._identity(descriptor)
        if str(UUID(owner_id)) != descriptor.controller_id:
            raise BootstrapError("Only the bootstrap controller can park these capacities")
        if descriptor.parking or any(
            operation.phase in {"intent", "accepted", "failed"}
            for operation in descriptor.capacity_operations
        ):
            raise BootstrapError("Reconcile previous bootstrap capacity operations before parking")
        ordered = (
            *(cap.arm_resource_id for cap in descriptor.capacities
              if cap.arm_resource_id != descriptor.catalog_capacity_id),
            descriptor.catalog_capacity_id,
        )
        proof = guard.enter_parking(descriptor.controller_id, ordered)

        def authorize() -> None:
            current = guard.assert_parking(proof)
            if (
                current != proof or current.recovery_set_id != descriptor.recovery_set_id
                or current.owner_id != descriptor.controller_id or current.epoch < 1
                or current.mode != RecoveryMode.PARKING or not current.drained
                or current.capacity_ids != ordered
            ):
                raise BootstrapError("Current catalog ownership/mode/drain proof does not authorize parking")

        authorize()
        self.store.update(lambda current: current.model_copy(update={
            "parking": ParkingIntent(owner_id=proof.owner_id, epoch=proof.epoch, capacity_ids=ordered),
        }))
        for resource_id in ordered:
            authorize()
            self.arm.suspend(
                descriptor.capacity(resource_id), owner_id=descriptor.controller_id,
                on_progress=self._record, authorize=authorize,
            )
        # No SQL callback/write here: the final capacity owns the catalog.
        return self.store.load()
