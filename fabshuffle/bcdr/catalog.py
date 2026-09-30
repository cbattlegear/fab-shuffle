"""The durable boundary consumed by capture and source-independent coordinators.

Every REST mutation needs a committed intent first. A SQL commit does not encompass
the REST request; ambiguous service outcomes remain pending until reconciled.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from fabshuffle.bcdr.contracts import (
    AppliedItem,
    CaptureSnapshot,
    CatalogDocument,
    CatalogState,
    ControllerLease,
    GenerationInfo,
    OperationRecord,
    RecoveryMode,
)
from fabshuffle.bcdr.payloads import CapturedPayload


class CatalogError(RuntimeError):
    """Catalog work failed; do not issue an unjournaled business mutation."""


class CatalogConflict(CatalogError):
    """Ownership, mode or expected-version conflict; no implicit successful fallback."""


class AmbiguousCommit(CatalogError):
    """A commit acknowledgement was lost; reconcile before retrying."""


@dataclass(frozen=True, slots=True)
class CapturedGeneration:
    snapshot: CaptureSnapshot
    payloads: tuple[CapturedPayload, ...]

    def payload(self, payload_id: str) -> CapturedPayload:
        matches = [payload for payload in self.payloads if payload.descriptor.payload_id == payload_id]
        if len(matches) != 1:
            raise CatalogError(f"Expected one captured payload for {payload_id}, found {len(matches)}")
        return matches[0]


class RecoveryCatalog(Protocol):
    def state(self) -> CatalogState: ...
    def acquire_controller(self, controller_id: str) -> ControllerLease: ...
    def release_controller(self, lease: ControllerLease) -> None: ...
    def transition_mode(
        self, lease: ControllerLease, expected: RecoveryMode, desired: RecoveryMode, operation_id: str,
    ) -> CatalogState: ...
    def stage_generation(
        self, lease: ControllerLease, snapshot: CaptureSnapshot, payloads: Sequence[CapturedPayload],
    ) -> None: ...
    def stage_failback_generation(
        self, lease: ControllerLease, snapshot: CaptureSnapshot, payloads: Sequence[CapturedPayload],
    ) -> None: ...
    def publish_generation(
        self, lease: ControllerLease, generation_id: str, *, expected_current: str | None,
    ) -> CapturedGeneration: ...
    def publish_failback_generation(
        self, lease: ControllerLease, generation_id: str, *, failover_generation_id: str,
    ) -> CapturedGeneration: ...
    def load_generation(self, generation_id: str | None = None) -> CapturedGeneration: ...
    def generations(self) -> tuple[GenerationInfo, ...]: ...
    def begin_operation(self, lease: ControllerLease, operation: OperationRecord) -> None: ...
    def begin_parking_operation(self, lease: ControllerLease, operation: OperationRecord) -> None: ...
    def record_operation(self, lease: ControllerLease, operation: OperationRecord) -> None: ...
    def operations(self) -> tuple[OperationRecord, ...]: ...
    def pending_operations(self) -> tuple[OperationRecord, ...]: ...
    def record_applied(self, lease: ControllerLease, applied: AppliedItem) -> None: ...
    def applied_items(self) -> tuple[AppliedItem, ...]: ...
    def get_record(self, namespace: str, key: str) -> CatalogDocument | None: ...
    def list_records(self, namespace: str) -> tuple[CatalogDocument, ...]: ...
    def put_record(
        self, lease: ControllerLease, namespace: str, key: str, document: dict,
        *, expected_revision: int | None,
    ) -> CatalogDocument: ...
