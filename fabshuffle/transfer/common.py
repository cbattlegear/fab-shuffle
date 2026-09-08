"""Shared limits and cancellation for client-mediated data transfers."""

from collections.abc import Callable

from fabshuffle.run import CancelledError

DEFAULT_MAX_STAGING_BYTES = 1024 * 1024 * 1024
TransferCancelled = CancelledError


class StagingBudgetError(RuntimeError):
    """A transfer cannot fit its next indivisible value inside the configured budget."""


def check_budget(max_staging_bytes: int) -> None:
    if isinstance(max_staging_bytes, bool) or not isinstance(max_staging_bytes, int):
        raise ValueError("max_staging_bytes must be a positive integer")
    if max_staging_bytes <= 0:
        raise ValueError("max_staging_bytes must be a positive integer")


def check_cancelled(cancel_requested: Callable[[], bool] | None) -> None:
    # A run.raise_if_cancelled callback is also valid: it raises rather than returning True.
    if cancel_requested and cancel_requested():
        raise CancelledError("Migration cancelled by the operator")
