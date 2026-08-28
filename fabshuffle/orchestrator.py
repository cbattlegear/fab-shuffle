"""The migration orchestrator.

This is the Python replacement for ``fab-shuffle.ps1``. It drives the whole region move.

Phase order is load bearing, and matches v1's. Each phase records the source-to-target ids
it created in ``_Context.id_map`` (v1's ``$replacements`` hash table), and later phases
rewrite their exported definitions through that map. A phase can therefore only reference
items created by an *earlier* phase:

1. ``workspaces``   create the target and scratch workspaces, and the folder tree.
2. ``eventhouses``  eventhouses before their KQL databases, since a database is created
   against ``parentEventhouseItemId``; data is copied once the schema exists.
3. ``lakehouses``   before warehouses, because warehouse views can reference lakehouse
   tables through the SQL analytics endpoint.
4. ``warehouses``   schema before data, so Copy Job activities have tables to land in.
5. ``shortcuts``    after *every* data item exists, since a shortcut can point at any of
   them. The SQL analytics endpoint is refreshed only now, so it picks up both the copied
   tables and the new shortcuts, and only then is its schema copied.
6. ``analytics``    semantic models, then reports. Models bind to lakehouse and warehouse
   SQL endpoints, so they need step 5 finished; reports bind to models, so they run after.
7. ``permissions``  role assignments last, so nothing is visible half built.
8. ``cleanup``      drop the scratch workspace and local staging.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fabshuffle.auth import ServicePrincipal, TokenProvider
from fabshuffle.config import SETTINGS
from fabshuffle.fabric import (
    analytics,
    copyjobs,
    data_stores,
    eventhouses,
    powerbi,
    shortcuts,
    workspaces,
)
from fabshuffle.fabric.client import FabricClient
from fabshuffle.fabric.items import list_items
from fabshuffle.fabric.support import (
    Strategy,
    WorkspaceAssessment,
    assess_workspace,
    supports_large_semantic_models,
)
from fabshuffle.run import CancelledError, MigrationRun, RunStatus, StepStatus
from fabshuffle.transfer import files as file_transfer
from fabshuffle.transfer import kql, sqlschema

logger = logging.getLogger(__name__)


@dataclass
class MigrationPlan:
    capacity_id: str
    capacity_name: str
    capacity_region: str
    source_workspace_id: str
    source_workspace_name: str
    target_workspace_name: str
    strategy: Strategy = Strategy.REBUILD
    include_files: bool = True
    include_data: bool = True
    copy_permissions: bool = True


@dataclass
class _Context:
    client: FabricClient
    tokens: TokenProvider
    principal: ServicePrincipal
    plan: MigrationPlan
    run: MigrationRun
    scratch_dir: Path
    target_workspace_id: str = ""
    scratch_workspace_id: str = ""
    # Maps every source identifier (workspace, item, endpoint, cluster URI) to its target
    # equivalent so shortcuts and definitions can be rewritten before import.
    id_map: dict[str, str] = field(default_factory=dict)
    copy_job_ids: list[tuple[str, str]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    assessment: WorkspaceAssessment | None = None


def default_target_name(source_name: str, region: str) -> str:
    return f"{source_name}-{region}" if region else f"{source_name}-copy"


def run_migration(
    run: MigrationRun,
    principal: ServicePrincipal,
    plan: MigrationPlan,
    *,
    cleanup: bool = True,
) -> None:
    """Execute a migration, recording every phase on ``run``."""
    tokens = TokenProvider(principal)
    scratch_dir = SETTINGS.scratch_dir_for(run.id)
    run.mark_running()

    with FabricClient(tokens) as client:
        context = _Context(
            client=client,
            tokens=tokens,
            principal=principal,
            plan=plan,
            run=run,
            scratch_dir=scratch_dir,
        )
        try:
            if plan.strategy is Strategy.REASSIGN:
                _reassign_capacity(context)
                run.summary["strategy"] = Strategy.REASSIGN.value
                run.summary["warnings"] = context.warnings
                run.mark_finished(RunStatus.SUCCEEDED)
                return

            _report_unsupported_items(context)
            _create_workspaces(context)
            _migrate_eventhouses(context)
            _migrate_lakehouses(context)
            _migrate_warehouses(context)
            _migrate_shortcuts_and_endpoints(context)
            _migrate_reports_and_models(context)
            _copy_permissions(context)
            if cleanup:
                cleanup_run(context.run, context.client, scratch_dir)
            run.summary["strategy"] = Strategy.REBUILD.value
            run.summary["warnings"] = context.warnings
            run.mark_finished(RunStatus.SUCCEEDED)
        except CancelledError as error:
            run.mark_finished(RunStatus.CANCELLED, str(error))
        except Exception as error:
            logger.exception("Migration %s failed", run.id)
            run.mark_finished(RunStatus.FAILED, str(error))


# --------------------------------------------------------------- reassign path


def _reassign_capacity(ctx: _Context) -> None:
    """Move a Power BI only workspace by pointing it at a capacity in the target region.

    Large semantic models are backed by Azure Premium Files, which pins their workspace to
    its region, so each one is converted to the small format first and restored afterwards.
    If any conversion or the assignment itself fails, the models that were already converted
    are put back rather than leaving the workspace half changed.
    """
    step = "reassign"
    ctx.run.start_step(step, "Reassigning the workspace to the target capacity")
    ctx.run.raise_if_cancelled()

    workspace_id = ctx.plan.source_workspace_id
    warnings: list[str] = []
    converted: list[powerbi.SemanticModel] = []

    with powerbi.PowerBiClient(ctx.tokens) as pbi:
        ctx.run.update_step(step, "Checking semantic model storage format")
        models = pbi.list_semantic_models(workspace_id)
        large_models = [model for model in models if model.is_large]

        if large_models and not supports_large_semantic_models(ctx.plan.capacity_region):
            raise RuntimeError(
                f"{len(large_models)} semantic model(s) use the large storage format, but region "
                f"'{ctx.plan.capacity_region}' does not support it, so they could not be "
                "restored after the move."
            )

        blocked = [model for model in large_models if not model.convertible]
        if blocked:
            names = ", ".join(f"'{model.name}'" for model in blocked)
            raise RuntimeError(
                "These semantic models cannot leave the large storage format, so the workspace "
                f"cannot be reassigned: {names}"
            )

        try:
            for model in large_models:
                ctx.run.raise_if_cancelled()
                pbi.convert(
                    workspace_id,
                    model,
                    powerbi.SMALL,
                    on_progress=lambda message: ctx.run.update_step(step, message),
                )
                converted.append(model)
        except (powerbi.PowerBiError, CancelledError) as error:
            ctx.run.update_step(step, "Conversion failed, restoring large semantic model storage")
            warnings.extend(_restore_large_models(ctx, pbi, converted))
            ctx.warnings.extend(warnings)
            ctx.run.finish_step(step, StepStatus.FAILED, "Could not convert every model", warnings)
            raise RuntimeError(
                f"Semantic models could not be converted to the small storage format: {error}"
            ) from error

        ctx.run.update_step(step, f"Assigning workspace to '{ctx.plan.capacity_name}'")
        try:
            workspaces.assign_to_capacity(ctx.client, workspace_id, ctx.plan.capacity_id)
        except Exception:
            ctx.run.update_step(step, "Assignment failed, restoring large semantic model storage")
            warnings.extend(_restore_large_models(ctx, pbi, converted))
            ctx.warnings.extend(warnings)
            ctx.run.finish_step(step, StepStatus.FAILED, "Capacity assignment failed", warnings)
            raise

        if converted:
            ctx.run.update_step(step, "Restoring large semantic model storage")
            warnings.extend(_restore_large_models(ctx, pbi, converted))

    ctx.run.target_workspace = {"id": workspace_id, "displayName": ctx.plan.source_workspace_name}
    ctx.warnings.extend(warnings)

    detail = f"Workspace now runs on '{ctx.plan.capacity_name}' in {ctx.plan.capacity_region}"
    if converted:
        detail += f", {len(converted)} semantic model(s) restored to large storage"
    ctx.run.finish_step(step, StepStatus.SUCCEEDED, detail, warnings)


def _restore_large_models(
    ctx: _Context,
    pbi: powerbi.PowerBiClient,
    models: list[powerbi.SemanticModel],
) -> list[str]:
    """Put models back on the large storage format, collecting failures instead of raising."""
    warnings: list[str] = []
    for model in models:
        try:
            pbi.convert(
                ctx.plan.source_workspace_id,
                model,
                powerbi.LARGE,
                on_progress=lambda message: ctx.run.update_step("reassign", message),
            )
        except powerbi.PowerBiError as error:
            warnings.append(
                f"Semantic model '{model.name}' is still on the small storage format; "
                f"re-enable large storage manually: {error}"
            )
    return warnings


# ----------------------------------------------------------- unsupported items


def _report_unsupported_items(ctx: _Context) -> None:
    step = "assessment"
    ctx.run.start_step(step, "Checking the workspace for unsupported items")
    ctx.run.raise_if_cancelled()

    assessment = assess_workspace(list_items(ctx.client, ctx.plan.source_workspace_id))
    ctx.assessment = assessment
    ctx.run.summary["unsupported"] = [item.as_dict() for item in assessment.unsupported]

    if not assessment.unsupported:
        ctx.run.finish_step(step, StepStatus.SUCCEEDED, "Everything in this workspace is supported")
        return

    warnings = [item.message() for item in assessment.unsupported]
    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step,
        StepStatus.SUCCEEDED,
        f"{len(assessment.unsupported)} item(s) will be left behind in the source workspace",
        warnings,
    )


# --------------------------------------------------------------------- phase 1


def _create_workspaces(ctx: _Context) -> None:
    step = "workspaces"
    ctx.run.start_step(step, "Creating target and scratch workspaces")
    ctx.run.raise_if_cancelled()

    target = workspaces.create_workspace(
        ctx.client, ctx.plan.target_workspace_name, ctx.plan.capacity_id
    )
    ctx.target_workspace_id = target["id"]
    ctx.run.target_workspace = {"id": target["id"], "displayName": ctx.plan.target_workspace_name}
    ctx.id_map[ctx.plan.source_workspace_id] = target["id"]

    # Copy Jobs must live somewhere that is not the workspace being built, otherwise they
    # show up as leftover items in the migrated workspace.
    scratch_name = workspaces.scratch_workspace_name()
    ctx.run.update_step(step, "Creating scratch workspace for Copy Jobs")
    scratch = workspaces.create_workspace(ctx.client, scratch_name, ctx.plan.capacity_id)
    ctx.scratch_workspace_id = scratch["id"]
    ctx.run.scratch_workspace = {"id": scratch["id"], "displayName": scratch_name}

    # A workspace is not fully initialised for Copy Jobs until it holds a lakehouse.
    data_stores.create_lakehouse(ctx.client, ctx.scratch_workspace_id, "hold")

    ctx.run.update_step(step, "Recreating workspace folders")
    folder_map = workspaces.clone_folder_tree(
        ctx.client, ctx.plan.source_workspace_id, ctx.target_workspace_id
    )
    ctx.id_map.update(folder_map)

    ctx.run.finish_step(step, StepStatus.SUCCEEDED, f"Created '{ctx.plan.target_workspace_name}'")


# --------------------------------------------------------------------- phase 2


def _migrate_eventhouses(ctx: _Context) -> None:
    step = "eventhouses"
    ctx.run.start_step(step, "Migrating eventhouses and KQL databases")
    ctx.run.raise_if_cancelled()

    source_eventhouses = eventhouses.list_eventhouses(ctx.client, ctx.plan.source_workspace_id)
    if not source_eventhouses:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "No eventhouses in the source workspace")
        return

    warnings: list[str] = []
    databases_moved = 0

    for eventhouse in source_eventhouses:
        ctx.run.raise_if_cancelled()
        name = eventhouse["displayName"]
        ctx.run.update_step(step, f"Creating eventhouse '{name}'")

        created = eventhouses.create_eventhouse(ctx.client, ctx.target_workspace_id, name)
        new_eventhouse = eventhouses.get_eventhouse(ctx.client, ctx.target_workspace_id, created["id"])

        source_properties = eventhouse.get("properties") or {}
        target_properties = new_eventhouse.get("properties") or {}
        ctx.id_map[eventhouse["id"]] = created["id"]
        for key in ("queryServiceUri", "ingestionServiceUri"):
            if source_properties.get(key) and target_properties.get(key):
                ctx.id_map[source_properties[key]] = target_properties[key]

        # Creating an eventhouse also creates a child KQL database named after it, so the
        # target already holds a database that the source is about to ask us to create.
        auto_created = eventhouses.eventhouse_databases(
            ctx.client, ctx.target_workspace_id, new_eventhouse
        )
        adopted_names: set[str] = set()

        for database_id in source_properties.get("databasesItemIds") or []:
            ctx.run.raise_if_cancelled()
            moved, database_warnings, adopted = _migrate_kql_database(
                ctx,
                step,
                database_id=database_id,
                target_eventhouse_id=created["id"],
                source_query_uri=source_properties.get("queryServiceUri", ""),
                target_query_uri=target_properties.get("queryServiceUri", ""),
                existing_databases=auto_created,
            )
            databases_moved += 1 if moved else 0
            warnings.extend(database_warnings)
            if adopted:
                adopted_names.add(adopted)

        # A default database whose name no source database matched is left behind empty,
        # which happens when the source default database was renamed.
        for leftover in sorted(set(auto_created) - adopted_names):
            warnings.append(
                f"Eventhouse '{name}' came with an empty default KQL database '{leftover}' that "
                "no source database matched. Delete it if you do not want it."
            )

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step,
        StepStatus.SUCCEEDED,
        f"Migrated {len(source_eventhouses)} eventhouse(s) and {databases_moved} KQL database(s)",
        warnings,
    )


def _migrate_kql_database(
    ctx: _Context,
    step: str,
    *,
    database_id: str,
    target_eventhouse_id: str,
    source_query_uri: str,
    target_query_uri: str,
    existing_databases: dict[str, Any],
) -> tuple[bool, list[str], str | None]:
    """Migrate one KQL database. Returns (moved, warnings, adopted name if any)."""
    database = eventhouses.get_kql_database(ctx.client, ctx.plan.source_workspace_id, database_id)
    name = database["displayName"]

    if eventhouses.database_type(database) != "ReadWrite":
        payload = eventhouses.shortcut_creation_payload(database, target_eventhouse_id)
        if not payload:
            return (
                False,
                [
                    f"KQL database '{name}' is a shortcut/follower database and Fabric does not "
                    "expose its source, so it was skipped"
                ],
                None,
            )
        eventhouses.create_kql_database(
            ctx.client, ctx.target_workspace_id, name, creation_payload=payload
        )
        return True, [], None

    ctx.run.update_step(step, f"Recreating KQL database '{name}'")
    parts = eventhouses.kql_database_definition_parts(
        ctx.client, ctx.plan.source_workspace_id, database_id
    )
    parts = eventhouses.retarget_database_definition(parts, target_eventhouse_id)

    target, adopted = eventhouses.create_or_adopt_kql_database(
        ctx.client,
        ctx.target_workspace_id,
        name,
        parts=parts,
        existing=existing_databases,
    )
    ctx.id_map[database_id] = target["id"]
    if adopted:
        logger.info("Applied schema to the default KQL database '%s'", name)

    adopted_name = name if adopted else None

    if not ctx.plan.include_data:
        return True, [], adopted_name
    if not source_query_uri or not target_query_uri:
        return True, [f"KQL database '{name}' has no query endpoint, data was not copied"], adopted_name

    ctx.run.update_step(step, f"Copying data for KQL database '{name}'")
    result = kql.copy_database(
        source_cluster_uri=source_query_uri,
        target_cluster_uri=target_query_uri,
        database=name,
        principal=ctx.principal,
        on_progress=lambda message: ctx.run.update_step(step, message),
    )
    logger.info("KQL database %s: copied %s table(s)", name, result["tables"])
    return True, [], adopted_name


# --------------------------------------------------------------------- phase 3


def _migrate_lakehouses(ctx: _Context) -> None:
    step = "lakehouses"
    ctx.run.start_step(step, "Migrating lakehouses")
    ctx.run.raise_if_cancelled()

    source_lakehouses = data_stores.list_lakehouses(ctx.client, ctx.plan.source_workspace_id)
    if not source_lakehouses:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "No lakehouses in the source workspace")
        return

    warnings: list[str] = []
    for lakehouse in source_lakehouses:
        ctx.run.raise_if_cancelled()
        name = lakehouse["displayName"]
        schema_enabled = data_stores.is_schema_enabled(lakehouse)

        ctx.run.update_step(step, f"Creating lakehouse '{name}'")
        created = data_stores.create_lakehouse(
            ctx.client,
            ctx.target_workspace_id,
            name,
            schema_enabled=schema_enabled,
            folder_id=ctx.id_map.get(lakehouse.get("folderId", "")),
        )
        target = data_stores.get_lakehouse(ctx.client, ctx.target_workspace_id, created["id"])
        ctx.id_map[lakehouse["id"]] = created["id"]

        source_endpoint = data_stores.lakehouse_sql_endpoint(lakehouse)
        target_endpoint = data_stores.lakehouse_sql_endpoint(target)
        if source_endpoint.get("connectionString") and target_endpoint.get("connectionString"):
            ctx.id_map[source_endpoint["connectionString"]] = target_endpoint["connectionString"]

        if ctx.plan.include_data:
            warnings.extend(_copy_lakehouse_tables(ctx, step, lakehouse, created["id"], schema_enabled))

        if ctx.plan.include_files:
            warnings.extend(_copy_lakehouse_files(ctx, step, lakehouse, target))

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step, StepStatus.SUCCEEDED, f"Migrated {len(source_lakehouses)} lakehouse(s)", warnings
    )


def _copy_lakehouse_tables(
    ctx: _Context,
    step: str,
    lakehouse: dict[str, Any],
    target_id: str,
    schema_enabled: bool,
) -> list[str]:
    name = lakehouse["displayName"]
    tables = data_stores.managed_tables(
        ctx.client,
        ctx.plan.source_workspace_id,
        lakehouse["id"],
        schema_enabled=schema_enabled,
    )
    if not tables:
        return []

    ctx.run.update_step(step, f"Copying {len(tables)} table(s) from lakehouse '{name}'")
    content = copyjobs.build_lakehouse_copy_job(
        source_workspace_id=ctx.plan.source_workspace_id,
        source_item_id=lakehouse["id"],
        target_workspace_id=ctx.target_workspace_id,
        target_item_id=target_id,
        tables=tables,
    )
    try:
        copy_job_id = copyjobs.run_copy_job(
            ctx.client,
            ctx.scratch_workspace_id,
            f"CopyJob_Lakehouse_{name}",
            content,
            on_status=lambda status: ctx.run.update_step(step, f"Lakehouse '{name}' copy job: {status}"),
        )
        ctx.copy_job_ids.append((ctx.scratch_workspace_id, copy_job_id))
        return []
    except copyjobs.CopyJobFailed as error:
        return [f"Table data for lakehouse '{name}' did not copy: {error}"]


def _copy_lakehouse_files(
    ctx: _Context,
    step: str,
    lakehouse: dict[str, Any],
    target: dict[str, Any],
) -> list[str]:
    name = lakehouse["displayName"]
    source_files = (lakehouse.get("properties") or {}).get("oneLakeFilesPath")
    target_files = (target.get("properties") or {}).get("oneLakeFilesPath")
    if not source_files or not target_files:
        return []

    ctx.run.update_step(step, f"Copying files for lakehouse '{name}'")
    try:
        file_transfer.copy_files(
            source_files_path=source_files,
            target_files_path=target_files,
            principal=ctx.principal,
            scratch_dir=ctx.scratch_dir / f"lakehouse-{lakehouse['id']}",
            on_progress=lambda message: ctx.run.update_step(step, f"{name}: {message}"),
        )
        return []
    except file_transfer.FileTransferError as error:
        return [f"Files for lakehouse '{name}' did not copy: {error}"]


# --------------------------------------------------------------------- phase 4


def _migrate_warehouses(ctx: _Context) -> None:
    step = "warehouses"
    ctx.run.start_step(step, "Migrating warehouses")
    ctx.run.raise_if_cancelled()

    source_warehouses = data_stores.list_warehouses(ctx.client, ctx.plan.source_workspace_id)
    if not source_warehouses:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "No warehouses in the source workspace")
        return

    warnings: list[str] = []
    for warehouse in source_warehouses:
        ctx.run.raise_if_cancelled()
        name = warehouse["displayName"]
        collation = (warehouse.get("properties") or {}).get("collationType")

        ctx.run.update_step(step, f"Creating warehouse '{name}'")
        created = data_stores.create_warehouse(
            ctx.client,
            ctx.target_workspace_id,
            name,
            collation_type=collation,
            folder_id=ctx.id_map.get(warehouse.get("folderId", "")),
        )
        target = data_stores.get_warehouse(ctx.client, ctx.target_workspace_id, created["id"])

        source_endpoint = data_stores.warehouse_connection_string(warehouse)
        target_endpoint = data_stores.warehouse_connection_string(target)
        ctx.id_map[warehouse["id"]] = created["id"]
        if source_endpoint and target_endpoint:
            ctx.id_map[source_endpoint] = target_endpoint

        ctx.run.update_step(step, f"Transferring schema for warehouse '{name}'")
        try:
            schema_warnings = sqlschema.transfer_schema(
                source_server=source_endpoint,
                target_server=target_endpoint,
                database=name,
                principal=ctx.principal,
                tokens=ctx.tokens,
                scratch_dir=ctx.scratch_dir / "sql",
                source_type="Warehouse",
                on_progress=lambda message: ctx.run.update_step(step, message),
            )
            warnings.extend(f"Warehouse '{name}': {w}" for w in schema_warnings)
        except sqlschema.SchemaTransferError as error:
            warnings.append(f"Schema for warehouse '{name}' did not transfer: {error}")
            continue

        if ctx.plan.include_data:
            warnings.extend(
                _copy_warehouse_tables(ctx, step, warehouse, created["id"], source_endpoint, target_endpoint)
            )

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step, StepStatus.SUCCEEDED, f"Migrated {len(source_warehouses)} warehouse(s)", warnings
    )


def _copy_warehouse_tables(
    ctx: _Context,
    step: str,
    warehouse: dict[str, Any],
    target_id: str,
    source_endpoint: str,
    target_endpoint: str,
) -> list[str]:
    name = warehouse["displayName"]
    try:
        tables = _warehouse_tables(ctx, source_endpoint, name)
    except sqlschema.SchemaTransferError as error:
        return [f"Could not enumerate tables in warehouse '{name}': {error}"]

    if not tables:
        return []

    ctx.run.update_step(step, f"Copying {len(tables)} table(s) from warehouse '{name}'")
    content = copyjobs.build_warehouse_copy_job(
        source_workspace_id=ctx.plan.source_workspace_id,
        source_item_id=warehouse["id"],
        source_endpoint=source_endpoint,
        target_workspace_id=ctx.target_workspace_id,
        target_item_id=target_id,
        target_endpoint=target_endpoint,
        tables=tables,
    )
    try:
        copy_job_id = copyjobs.run_copy_job(
            ctx.client,
            ctx.scratch_workspace_id,
            f"CopyJob_Warehouse_{name}",
            content,
            on_status=lambda status: ctx.run.update_step(step, f"Warehouse '{name}' copy job: {status}"),
        )
        ctx.copy_job_ids.append((ctx.scratch_workspace_id, copy_job_id))
        return []
    except copyjobs.CopyJobFailed as error:
        return [f"Table data for warehouse '{name}' did not copy: {error}"]


def _warehouse_tables(ctx: _Context, endpoint: str, database: str) -> list[data_stores.TableRef]:
    """Enumerate base tables over TDS; warehouses have no REST table listing API."""
    with sqlschema.connect(endpoint, database, ctx.tokens) as connection:
        rows = (
            connection.cursor()
            .execute(
                "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE'"
            )
            .fetchall()
        )
    return [data_stores.TableRef(name=row[1], schema=row[0]) for row in rows]


# --------------------------------------------------------------------- phase 5


def _migrate_shortcuts_and_endpoints(ctx: _Context) -> None:
    step = "shortcuts"
    ctx.run.start_step(step, "Recreating shortcuts and syncing SQL endpoints")
    ctx.run.raise_if_cancelled()

    source_lakehouses = data_stores.list_lakehouses(ctx.client, ctx.plan.source_workspace_id)
    if not source_lakehouses:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "No lakehouses to process")
        return

    warnings: list[str] = []
    shortcuts_created = 0

    for lakehouse in source_lakehouses:
        ctx.run.raise_if_cancelled()
        name = lakehouse["displayName"]
        target_id = ctx.id_map.get(lakehouse["id"])
        if not target_id:
            continue

        ctx.run.update_step(step, f"Recreating shortcuts for '{name}'")
        created, shortcut_warnings = shortcuts.copy_shortcuts(
            ctx.client,
            ctx.plan.source_workspace_id,
            lakehouse["id"],
            ctx.target_workspace_id,
            target_id,
            ctx.id_map,
        )
        shortcuts_created += created
        warnings.extend(f"Lakehouse '{name}': {w}" for w in shortcut_warnings)

        # The endpoint must re-read OneLake after tables and shortcuts land, otherwise the
        # schema copy below sees an empty database.
        ctx.run.update_step(step, f"Refreshing SQL endpoint for '{name}'")
        target = data_stores.get_lakehouse(ctx.client, ctx.target_workspace_id, target_id)
        endpoint = data_stores.lakehouse_sql_endpoint(target)
        if endpoint.get("id"):
            data_stores.refresh_sql_endpoint_metadata(ctx.client, ctx.target_workspace_id, endpoint["id"])

        source_endpoint = data_stores.lakehouse_sql_endpoint(lakehouse).get("connectionString")
        target_endpoint = endpoint.get("connectionString")
        if not source_endpoint or not target_endpoint:
            continue

        ctx.run.update_step(step, f"Transferring SQL endpoint schema for '{name}'")
        try:
            schema_warnings = sqlschema.transfer_schema(
                source_server=source_endpoint,
                target_server=target_endpoint,
                database=name,
                principal=ctx.principal,
                tokens=ctx.tokens,
                scratch_dir=ctx.scratch_dir / "sql",
                source_type="Lakehouse",
                on_progress=lambda message: ctx.run.update_step(step, message),
            )
            warnings.extend(f"Lakehouse '{name}' SQL endpoint: {w}" for w in schema_warnings)
        except sqlschema.SchemaTransferError as error:
            warnings.append(f"SQL endpoint schema for '{name}' did not transfer: {error}")

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step, StepStatus.SUCCEEDED, f"Created {shortcuts_created} shortcut(s)", warnings
    )


# --------------------------------------------------------------------- phase 6


def _migrate_reports_and_models(ctx: _Context) -> None:
    """Recreate semantic models and reports, rebound to the items in the new workspace.

    Runs last of the content phases because it is entirely driven by ``id_map``: a semantic
    model's exported definition embeds the SQL analytics endpoint and GUID of the lakehouse
    or warehouse it reads, and a report's ``definition.pbir`` embeds its model's GUID. Both
    are only resolvable once those items exist.
    """
    step = "analytics"
    ctx.run.start_step(step, "Migrating semantic models and reports")
    ctx.run.raise_if_cancelled()

    source_id = ctx.plan.source_workspace_id
    models = analytics.list_of_type(ctx.client, source_id, analytics.SEMANTIC_MODEL)
    reports = analytics.list_of_type(ctx.client, source_id, analytics.REPORT)

    # Lakehouses and warehouses bring their own default semantic model, so the target
    # workspace already has one under the same name.
    default_names = analytics.default_semantic_model_names(ctx.client, source_id)
    skipped = [model for model in models if model["displayName"] in default_names]
    models = [model for model in models if model["displayName"] not in default_names]

    if not models and not reports:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "No semantic models or reports to migrate")
        return

    warnings: list[str] = []

    def progress(message: str) -> None:
        ctx.run.update_step(step, message)

    migrated_models, model_warnings = analytics.migrate_items(
        ctx.client,
        source_workspace_id=source_id,
        target_workspace_id=ctx.target_workspace_id,
        items=models,
        item_type=analytics.SEMANTIC_MODEL,
        id_map=ctx.id_map,
        folder_map=ctx.id_map,
        on_progress=progress,
    )
    warnings.extend(model_warnings)

    ctx.run.raise_if_cancelled()

    # Reports run in a second pass so every model id is already in the map above.
    migrated_reports, report_warnings = analytics.migrate_items(
        ctx.client,
        source_workspace_id=source_id,
        target_workspace_id=ctx.target_workspace_id,
        items=reports,
        item_type=analytics.REPORT,
        id_map=ctx.id_map,
        folder_map=ctx.id_map,
        on_progress=progress,
    )
    warnings.extend(report_warnings)

    unbound = [item.name for item in migrated_reports if item.rebound_parts == 0]
    if unbound:
        warnings.append(
            "These reports had no reference to rewrite, so they still point at their original "
            "semantic model: " + ", ".join(unbound)
        )
    if skipped:
        logger.info("Skipped %s default semantic model(s)", len(skipped))

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step,
        StepStatus.SUCCEEDED,
        f"Migrated {len(migrated_models)} semantic model(s) and {len(migrated_reports)} report(s)",
        warnings,
    )


# --------------------------------------------------------------------- phase 7


def _copy_permissions(ctx: _Context) -> None:
    step = "permissions"
    if not ctx.plan.copy_permissions:
        ctx.run.add_step(step, "Copying workspace permissions")
        ctx.run.finish_step(step, StepStatus.SKIPPED, "Disabled for this run")
        return

    ctx.run.start_step(step, "Copying workspace permissions")
    ctx.run.raise_if_cancelled()

    assignments = workspaces.list_role_assignments(ctx.client, ctx.plan.source_workspace_id)
    warnings = workspaces.copy_role_assignments(ctx.client, assignments, ctx.target_workspace_id)
    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step,
        StepStatus.SUCCEEDED,
        f"Replayed {len(assignments) - len(warnings)} of {len(assignments)} role assignment(s)",
        warnings,
    )


# --------------------------------------------------------------------- cleanup


def cleanup_run(run: MigrationRun, client: FabricClient, scratch_dir: Path | None = None) -> list[str]:
    """Delete the scratch workspace and local staging created for a run."""
    step = "cleanup"
    run.start_step(step, "Removing temporary artifacts")
    warnings: list[str] = []

    scratch = run.scratch_workspace
    if scratch and scratch.get("id"):
        run.update_step(step, "Deleting scratch workspace")
        try:
            # Deleting the workspace removes the items under it. They must not be deleted
            # individually first: derived items such as a lakehouse SQL analytics endpoint
            # reject a direct delete with OperationNotSupportedForItem.
            workspaces.delete_workspace(client, scratch["id"])
            run.scratch_workspace = None
        except Exception as error:
            warnings.append(f"Scratch workspace {scratch['id']} could not be deleted: {error}")

    directory = scratch_dir or (SETTINGS.scratch_root / run.id)
    if directory.exists():
        run.update_step(step, "Deleting local staging directory")
        shutil.rmtree(directory, ignore_errors=True)

    run.cleanup_done = not warnings
    run.finish_step(
        step,
        StepStatus.SUCCEEDED if not warnings else StepStatus.FAILED,
        "Temporary artifacts removed" if not warnings else "Some artifacts remain",
        warnings,
    )
    return warnings


def build_plan(
    client: FabricClient,
    *,
    capacity_id: str,
    source_workspace_id: str,
    target_workspace_name: str | None = None,
    include_files: bool = True,
    include_data: bool = True,
    copy_permissions: bool = True,
    strategy: Strategy | None = None,
) -> MigrationPlan:
    capacity = workspaces.get_capacity(client, capacity_id)
    workspace = workspaces.get_workspace(client, source_workspace_id)
    region = workspaces.capacity_region(capacity)
    source_name = workspace["displayName"]

    if strategy is None:
        strategy = assess_workspace(list_items(client, source_workspace_id)).strategy

    return MigrationPlan(
        capacity_id=capacity_id,
        capacity_name=capacity.get("displayName", capacity_id),
        capacity_region=region,
        source_workspace_id=source_workspace_id,
        source_workspace_name=source_name,
        # Reassignment moves the workspace itself, so its name never changes.
        target_workspace_name=(
            source_name
            if strategy is Strategy.REASSIGN
            else (target_workspace_name or default_target_name(source_name, region))
        ),
        strategy=strategy,
        include_files=include_files,
        include_data=include_data,
        copy_permissions=copy_permissions,
    )


__all__ = [
    "MigrationPlan",
    "build_plan",
    "cleanup_run",
    "default_target_name",
    "run_migration",
]
