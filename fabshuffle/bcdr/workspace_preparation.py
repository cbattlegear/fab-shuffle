"""Journaled workspace preparation using the migration Spark helpers and captured inputs."""

import json

from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.contracts import OperationState, canonical_id, canonical_json, digest
from fabshuffle.fabric import spark
from fabshuffle.fabric.client import FabricError


def prepare_spark(coordinator, generation, workspace, target):
    metadata = workspace.properties.get("bcdr", {})
    pools = metadata.get("spark_pools", [])
    settings = metadata.get("spark_settings", {})
    if not isinstance(pools, list) or not isinstance(settings, dict):
        raise RecoveryBlocked("Capture inspectable workspace Spark pools and settings before preparing them")
    if not pools and not settings:
        return []
    runtime, client = coordinator.runtime, coordinator.destination
    mappings, warnings = {}, []
    recorded = {row.key: row.document for row in coordinator.catalog.list_records("spark-pools")}
    existing = None
    for pool in pools:
        source_id = canonical_id(pool["id"])
        key = f"{workspace.identity.key}/{source_id}"
        source_hash = digest(canonical_json(spark.pool_configuration(pool)))
        previous = recorded.get(key)
        if previous:
            if previous["target_workspace"] != target.model_dump(mode="json"):
                raise RecoveryBlocked("The recorded Spark pool belongs to another target workspace")
            if previous["source_hash"] != source_hash:
                raise RecoveryBlocked("Captured pool settings changed; review the owned pool before resync")
            if existing is None:
                existing = {canonical_id(row["id"]): row for row in client.list_all(
                    f"workspaces/{target.workspace_id}/spark/pools"
                )}
            observed = existing.get(previous["target_id"])
            if (
                observed is None
                or digest(canonical_json(spark.pool_configuration(observed))) != previous["observed_hash"]
            ):
                raise RecoveryBlocked("A recorded destination Spark pool is missing or drifted; reconcile it")
            operation = next(
                (row for row in coordinator.catalog.operations()
                 if row.operation_id == previous["operation_id"]), None,
            )
            if (
                operation is None or operation.state != OperationState.SUCCEEDED
                or operation.kind != "spark-pool-create"
            ):
                raise RecoveryBlocked("The Spark pool mapping has no successful ownership receipt")
            receipt = json.loads(operation.message or "{}")
            if receipt.get("mapping", {}).get(source_id) != previous["target_id"]:
                raise RecoveryBlocked("The Spark pool mapping differs from its exact creation receipt")
            mappings[source_id] = previous["target_id"]
            continue

        def create(value=pool, identifier=source_id):
            pool_map, _, messages = spark.copy_pools(
                client, workspace.identity.workspace_id, target.workspace_id,
                pools=[{**value, "id": identifier}], target_client=client, strict=True,
            )
            if runtime.current_operation is None:
                raise FabricError("Spark pool preparation requires a recorded operation")
            return {
                "mapping": pool_map, "warnings": messages,
                "operation_id": runtime.current_operation.operation_id,
            }

        result = runtime.effect(
            "spark-pool-create", f"{target.key}/{source_id}/{source_hash}", create,
            generation_id=generation.snapshot.generation_id,
        )
        warnings.extend(result["warnings"])
        if source_id not in result["mapping"]:
            continue
        target_id = canonical_id(result["mapping"][source_id])
        observed = next(
            (row for row in client.list_all(f"workspaces/{target.workspace_id}/spark/pools")
             if canonical_id(row["id"]) == target_id), None,
        )
        if observed is None:
            raise RecoveryBlocked("Observe the created Spark pool before mapping dependent environments")
        operation = next((row for row in coordinator.catalog.operations()
                          if row.operation_id == result["operation_id"]), None)
        if operation is None or operation.state != OperationState.SUCCEEDED:
            raise RecoveryBlocked("The created Spark pool has no successful operation receipt")
        if spark.pool_configuration(observed) != spark.pool_configuration(pool):
            raise RecoveryBlocked("The created Spark pool does not match the captured configuration")
        runtime.put("spark-pools", key, {
            "target_workspace": target.model_dump(mode="json"), "target_id": target_id,
            "source_hash": source_hash,
            "observed_hash": digest(canonical_json(spark.pool_configuration(observed))),
            "operation_id": operation.operation_id,
        })
        mappings[source_id] = target_id
    if settings:
        current = client.get(f"workspaces/{target.workspace_id}/spark/settings")
        patches, messages = spark.build_settings_payload(settings, mappings, target=current)
        warnings.extend(messages)
        for _label, body in patches:
            runtime.effect(
                "spark-workspace-settings",
                f"{generation.snapshot.generation_id}/{target.key}/{digest(canonical_json(body))}",
                lambda patch=body: spark.update_settings(client, target.workspace_id, patch),
                generation_id=generation.snapshot.generation_id,
            )
    return warnings


def apply_default_environment(coordinator, generation, workspace, target, applied):
    settings = workspace.properties.get("bcdr", {}).get("spark_settings", {})
    patch = spark.default_environment_patch(settings)
    if patch is None:
        return
    name = patch["environment"]["name"]
    candidates = [
        row for row in generation.snapshot.items
        if row.identity.workspace_id == workspace.identity.workspace_id
        and row.item_type == "Environment" and row.display_name == name
    ]
    if len(candidates) != 1 or candidates[0].identity.key not in applied:
        raise RecoveryBlocked(
            f"Prepare default Environment '{name}' before applying workspace settings"
        )
    destination = applied[candidates[0].identity.key].target
    if destination.workspace_id != target.workspace_id:
        raise RecoveryBlocked("The default Environment mapping belongs to another target workspace")
    observed = coordinator.destination.get(
        f"workspaces/{target.workspace_id}/environments/{destination.item_id}"
    )
    if observed.get("id") != destination.item_id or observed.get("displayName") != name:
        raise RecoveryBlocked("The mapped default Environment does not match the captured workspace setting")
    current = coordinator.destination.get(f"workspaces/{target.workspace_id}/spark/settings")
    if current.get("environment") == patch["environment"]:
        return
    coordinator.runtime.effect(
        "spark-default-environment",
        f"{generation.snapshot.generation_id}/{target.key}/{digest(canonical_json(patch))}",
        lambda: spark.update_settings(coordinator.destination, target.workspace_id, patch),
        generation_id=generation.snapshot.generation_id,
    )
