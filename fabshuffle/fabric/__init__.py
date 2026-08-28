"""Typed wrappers over the Fabric REST API surface used by Fab Shuffle."""

from fabshuffle.fabric.client import (
    FabricApiError,
    FabricClient,
    OperationFailed,
    OperationTimeout,
)

__all__ = [
    "FabricApiError",
    "FabricClient",
    "OperationFailed",
    "OperationTimeout",
]
