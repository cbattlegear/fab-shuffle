"""Workspace Spark configuration: custom pools and workspace Spark settings.

A custom pool belongs to the workspace it was created in, so an environment or notebook that
pins one lands in the migrated workspace referencing a pool that does not exist there. The
pools are recreated and their new ids go into the id map, which is what repoints the
``instance_pool_id`` in an environment's ``Sparkcompute.yml``.

Pools therefore have to be created before the engineering phase runs.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any

from fabshuffle.fabric.client import FabricApiError, FabricClient

logger = logging.getLogger(__name__)

# Fabric provisions this in every workspace and rejects it as a custom pool name.
STARTER_POOL = "Starter Pool"

# A capacity level pool belongs to the capacity rather than the workspace, so it cannot be
# recreated through the workspace pools API.
WORKSPACE_POOL_TYPE = "Workspace"

# The fields Create Workspace Custom Pool accepts. Everything else List returns (id, type)
# is assigned by Fabric.
_POOL_FIELDS = ("name", "nodeFamily", "nodeSize", "autoScale", "dynamicExecutorAllocation")


def list_pools(client: FabricClient, workspace_id: str) -> list[dict[str, Any]]:
    try:
        return client.list_all(f"workspaces/{workspace_id}/spark/pools")
    except FabricApiError as error:
        if error.status_code in (401, 403, 404):
            logger.info("Cannot read Spark pools for %s: HTTP %s", workspace_id, error.status_code)
            return []
        raise


def create_pool(client: FabricClient, workspace_id: str, pool: Mapping[str, Any]) -> dict[str, Any]:
    payload = {field: pool[field] for field in _POOL_FIELDS if pool.get(field) is not None}
    return client.post(f"workspaces/{workspace_id}/spark/pools", json=payload)


def get_settings(client: FabricClient, workspace_id: str) -> dict[str, Any] | None:
    try:
        return client.get(f"workspaces/{workspace_id}/spark/settings")
    except FabricApiError as error:
        if error.status_code in (401, 403, 404):
            return None
        raise


def update_settings(
    client: FabricClient,
    workspace_id: str,
    settings: Mapping[str, Any],
) -> dict[str, Any]:
    return client.patch(f"workspaces/{workspace_id}/spark/settings", json=dict(settings))


def copy_pools(
    client: FabricClient,
    source_workspace_id: str,
    target_workspace_id: str,
    *,
    pools: Iterable[Mapping[str, Any]] | None = None,
) -> tuple[dict[str, str], list[str], list[str]]:
    """Recreate the source workspace's custom pools.

    Returns the old id to new id map, the names created, and any warnings.
    """
    source = list(pools) if pools is not None else list_pools(client, source_workspace_id)

    id_map: dict[str, str] = {}
    created: list[str] = []
    warnings: list[str] = []

    for pool in source:
        name = pool.get("name") or ""
        if name == STARTER_POOL:
            continue
        if pool.get("type") and pool["type"] != WORKSPACE_POOL_TYPE:
            warnings.append(
                f"Spark pool '{name}' is a {pool['type']} level pool, which belongs to the "
                "capacity rather than the workspace, so it was not recreated. Create it on the "
                "target capacity if items depend on it."
            )
            continue

        try:
            new_pool = create_pool(client, target_workspace_id, pool)
        except FabricApiError as error:
            warnings.append(
                f"Spark pool '{name}' could not be recreated (HTTP {error.status_code}). "
                f"Its node size may not be available on the target capacity."
            )
            continue

        if pool.get("id") and new_pool.get("id"):
            id_map[pool["id"]] = new_pool["id"]
        created.append(name)

    return id_map, created, warnings


def build_settings_payload(
    settings: Mapping[str, Any],
    pool_id_map: Mapping[str, str],
) -> tuple[dict[str, Any], list[str]]:
    """Build the settings patch for the new workspace, repointing the default pool.

    Only the parts that carry a real choice are copied. A default pool that was a custom pool
    is remapped to its recreated counterpart; if that pool could not be recreated, the setting
    is dropped rather than pointing at a pool in the old region.
    """
    payload: dict[str, Any] = {}
    warnings: list[str] = []

    for key in ("automaticLog", "highConcurrency", "job"):
        if isinstance(settings.get(key), Mapping):
            payload[key] = dict(settings[key])

    pool = settings.get("pool")
    if isinstance(pool, Mapping):
        pool_payload: dict[str, Any] = {}
        if "customizeComputeEnabled" in pool:
            pool_payload["customizeComputeEnabled"] = pool["customizeComputeEnabled"]
        if isinstance(pool.get("starterPool"), Mapping):
            pool_payload["starterPool"] = dict(pool["starterPool"])

        default_pool = pool.get("defaultPool")
        if isinstance(default_pool, Mapping):
            name = default_pool.get("name")
            old_id = default_pool.get("id")
            if name == STARTER_POOL:
                pool_payload["defaultPool"] = {"name": STARTER_POOL, "type": WORKSPACE_POOL_TYPE}
            elif old_id and old_id in pool_id_map:
                pool_payload["defaultPool"] = {"id": pool_id_map[old_id]}
            elif name:
                warnings.append(
                    f"The workspace default Spark pool was '{name}', which was not recreated, "
                    "so the new workspace falls back to the starter pool."
                )

        if pool_payload:
            payload["pool"] = pool_payload

    # The default environment is referenced by name, and the migrated one keeps its name.
    environment = settings.get("environment")
    if isinstance(environment, Mapping) and environment.get("name"):
        payload["environment"] = dict(environment)

    return payload, warnings


__all__ = [
    "STARTER_POOL",
    "WORKSPACE_POOL_TYPE",
    "build_settings_payload",
    "copy_pools",
    "create_pool",
    "get_settings",
    "list_pools",
    "update_settings",
]
