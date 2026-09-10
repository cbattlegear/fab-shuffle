"""Shared limits and cancellation for client-mediated data transfers."""

from collections.abc import Callable
from dataclasses import dataclass

from fabshuffle.config import DEFAULT_MAX_DISK_STAGING_BYTES, DEFAULT_MAX_MEMORY_BYTES
from fabshuffle.run import CancelledError

DEFAULT_MAX_STAGING_BYTES = DEFAULT_MAX_MEMORY_BYTES
TransferCancelled = CancelledError


class StagingBudgetError(RuntimeError):
    """A transfer cannot fit its next indivisible value inside the configured budget."""


@dataclass(frozen=True, slots=True)
class TransferBudgets:
    max_memory_bytes: int
    max_disk_staging_bytes: int


def check_budget(value: int, name: str = "max_staging_bytes") -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be a positive integer")
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def resolve_memory_budget(
    *,
    max_memory_bytes: int | None = None,
    max_staging_bytes: int | None = None,
) -> int:
    legacy = None
    if max_staging_bytes is not None:
        check_budget(max_staging_bytes, "max_staging_bytes")
        legacy = max_staging_bytes
    if max_memory_bytes is not None:
        check_budget(max_memory_bytes, "max_memory_bytes")
        return max_memory_bytes
    return legacy if legacy is not None else DEFAULT_MAX_MEMORY_BYTES


def resolve_transfer_budgets(
    *,
    max_memory_bytes: int | None = None,
    max_disk_staging_bytes: int | None = None,
    max_staging_bytes: int | None = None,
) -> TransferBudgets:
    legacy = None
    if max_staging_bytes is not None:
        check_budget(max_staging_bytes, "max_staging_bytes")
        legacy = max_staging_bytes
    if max_memory_bytes is not None:
        check_budget(max_memory_bytes, "max_memory_bytes")
        memory = max_memory_bytes
    else:
        memory = legacy if legacy is not None else DEFAULT_MAX_MEMORY_BYTES
    if max_disk_staging_bytes is not None:
        check_budget(max_disk_staging_bytes, "max_disk_staging_bytes")
        disk = max_disk_staging_bytes
    else:
        disk = legacy if legacy is not None else DEFAULT_MAX_DISK_STAGING_BYTES
    return TransferBudgets(memory, disk)


def check_cancelled(cancel_requested: Callable[[], bool] | None) -> None:
    # A run.raise_if_cancelled callback is also valid: it raises rather than returning True.
    if cancel_requested and cancel_requested():
        raise CancelledError("Migration cancelled by the operator")
