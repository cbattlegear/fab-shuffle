"""The migration orchestrator.

This is the Python replacement for ``fab-shuffle.ps1``. It drives the whole region move.

Phase order is load bearing, and matches v1's. Each phase records the source-to-target ids
it created in ``_Context.id_map`` (v1's ``$replacements`` hash table), and later phases
rewrite their exported definitions through that map. A phase can therefore only reference
items created by an *earlier* phase:

0. ``assessment`` and ``dependencies`` run before anything is created, so a workspace that
   cannot migrate cleanly can be abandoned before it is half built.
1. ``workspaces``   create the target and scratch workspaces, the folder tree, and the custom
   Spark pools plus workspace Spark settings. Pools go here because an environment pins one
   by id, so it must exist and be in the id map before the engineering phase.
2. ``eventhouses``  eventhouses before their KQL databases, since a database is created
   against ``parentEventhouseItemId``; data is copied once the schema exists.
3. ``lakehouses``   before warehouses, because warehouse views can reference lakehouse
   tables through the SQL analytics endpoint.
4. ``warehouses``   schema before data, so Copy Job activities have tables to land in.
5. ``sqldatabases`` Fabric SQL databases, schema through the item definition and rows through
   a Copy Job. A data store like the rest, and read by GraphQL APIs, pipelines and Copy Jobs.
6. ``mirrored``     mirrored databases, which are data stores with their own SQL analytics
   endpoint, so they belong with the others and before anything that reads them.
7. ``shortcuts``    after *every* data item exists, since a shortcut can point at any of
   them. This covers both lakehouse shortcuts and KQL database table shortcuts. The SQL
   analytics endpoint is refreshed only now, so it picks up both the copied tables and the
   new shortcuts, and only then is its schema copied.
7. ``connections``  recreate connections that point into the source workspace, aimed at the
   items just created. Their new ids go into the id map, so everything after this binds to
   them. A connection's target cannot be changed in place, so this is a replacement.
8. ``realtime``     eventstreams, KQL querysets, and KQL dashboards. They read the
   eventhouses and data stores above, and an eventstream sources from connections.
9. ``engineering``  environments, then notebooks, then dataflows, then Spark job definitions,
   GraphQL APIs, graph models and query sets, maps, variable libraries, and mounted data
   factories. A notebook attaches to an environment and reads a lakehouse; a semantic model
   can read a dataflow; a query set names its graph model. All of them come before analytics.
10. ``analytics``   semantic models, then reports. Models bind to lakehouse and warehouse
    SQL endpoints, so they need the data store phases finished; reports bind to models, so
    they run after.
11. ``orchestration`` data pipelines and Copy Jobs, which read, refresh, and invoke anything
    above.
12. ``reflexes``    Activator items, last of the content phases. One watches an eventstream or
    KQL database and acts by running pipelines and notebooks, so both sides must exist first.
13. ``permissions`` remaining role assignments. Admins were granted back in step 1.
14. ``cleanup``     drop the scratch workspace and local staging.
"""

from __future__ import annotations

import logging
import shutil
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fabshuffle import concurrency
from fabshuffle.auth import ServicePrincipal, TokenProvider
from fabshuffle.config import SETTINGS
from fabshuffle.fabric import (
    airflow,
    analytics,
    connections,
    copyjobs,
    cosmosdb,
    data_stores,
    definitions,
    eventhouses,
    powerbi,
    relations,
    shortcuts,
    spark,
    special_items,
    sqldatabases,
    workspaces,
)
from fabshuffle.fabric import items as items_module
from fabshuffle.fabric.client import FabricApiError, FabricClient, FabricError
from fabshuffle.fabric.items import is_monitoring_item, list_items
from fabshuffle.fabric.support import (
    Strategy,
    WorkspaceAssessment,
    assess_workspace,
    supports_large_semantic_models,
)
from fabshuffle.run import CancelledError, MigrationRun, RunStatus, StepStatus
from fabshuffle.transfer import bulkcopy, kql, sqlschema
from fabshuffle.transfer import cosmos as cosmos_transfer
from fabshuffle.transfer import files as file_transfer

logger = logging.getLogger(__name__)

# Item types whose definitions bind a tenant connection. Creating one of these is refused
# outright if the service principal cannot use the connection, so they are worth checking
# before anything is built.
CONNECTION_BINDING_TYPES = (
    analytics.DATA_PIPELINE,
    analytics.COPY_JOB,
    analytics.EVENTSTREAM,
    analytics.MIRRORED_DATABASE,
    analytics.REFLEX,
)


@dataclass
class DependencyReport:
    """What a dependency check found, whether it ran for a preview or for a real run."""

    graph: relations.DependencyGraph
    available: bool = True
    issues: list[relations.DependencyIssue] = field(default_factory=list)
    prerequisites: list[connections.ConnectionPrerequisite] = field(default_factory=list)
    access: list[ConnectionAccess] = field(default_factory=list)

    def messages(self) -> list[str]:
        """Warnings for the run log.

        Connection access is deliberately not repeated here. It has its own section on the
        review screen, and listing it twice made the same nine lines appear under two
        headings.
        """
        warnings = [issue.message() for issue in self.issues]
        if self.prerequisites:
            warnings.append(inward_connection_summary(self.prerequisites))
        return warnings


def inward_connection_summary(prerequisites: list[connections.ConnectionPrerequisite]) -> str:
    """One line for connections that point at items in the workspace being migrated.

    Their target cannot be changed and a replacement needs credentials Fabric never returns,
    so this is work for whoever owns them. Naming each one individually said the same
    sentence several times over, so it is a count and a list of names.
    """
    named = sorted({p.connection_name or p.connection_id for p in prerequisites})
    return (
        f"{len(prerequisites)} connection(s) point at items in this workspace and will need to "
        "be repointed by hand after the migration, because Fabric does not allow a "
        f"connection's target to be changed: {', '.join(named)}."
    )


@dataclass
class MigrationPlan:
    capacity_id: str
    capacity_name: str
    capacity_region: str
    source_workspace_id: str
    source_workspace_name: str
    target_workspace_name: str
    # The capacity's region as the service spells it, such as ``West Central US``. Almost
    # everything wants the normalised form, but an Apache Airflow job records its compute
    # location as the display string and will not accept anything else.
    capacity_display_region: str = ""
    # The target capacity's SKU. Copy Jobs run on it, so how many are worth running at once
    # is decided from its size.
    capacity_sku: str = ""
    strategy: Strategy = Strategy.REBUILD
    capacity_warning: str | None = None
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
    source_role_assignments: list[dict[str, Any]] = field(default_factory=list)
    # KQL databases that were migrated, and the table shortcuts each one had. Shortcuts can
    # target any item in the workspace, so they are recreated in the shortcut phase rather
    # than while the eventhouses are being built.
    kql_databases: list[tuple[str, str, str]] = field(default_factory=list)
    kql_table_shortcuts: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    graph: relations.DependencyGraph = field(default_factory=relations.DependencyGraph)
    spark_settings: dict[str, Any] | None = None
    # Every item in the workspace being migrated, by id. Used to say which of them a
    # definition referred to but did not get, since the workspace id in that reference is
    # always rewritten while the item id beside it is only rewritten if it migrated.
    source_items: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Source ids of items that migrated but arrive with nothing in them, because we leave
    # their replication switched off so a copy does not start doing the original's work in a
    # second region. Keyed to why, so a shortcut that fails against one can say so.
    dormant: dict[str, str] = field(default_factory=dict)


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
            _check_dependencies(context)
            _create_workspaces(context)
            _migrate_eventhouses(context)
            _migrate_lakehouses(context)
            _migrate_warehouses(context)
            _migrate_sql_databases(context)
            _migrate_mirrored_databases(context)
            _migrate_shortcuts_and_endpoints(context)
            _migrate_realtime(context)
            _migrate_engineering(context)
            _migrate_reports_and_models(context)
            _migrate_orchestration(context)
            _migrate_reflexes(context)
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

    if ctx.plan.capacity_warning:
        warnings.append(ctx.plan.capacity_warning)

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

    # list_items already filters system items out, so monitoring is detected separately.
    all_items = ctx.client.list_all(f"workspaces/{ctx.plan.source_workspace_id}/items")
    monitoring = [item for item in all_items if is_monitoring_item(item)]

    assessment = assess_workspace(list_items(ctx.client, ctx.plan.source_workspace_id))
    ctx.assessment = assessment
    # Kept for the whole run: a definition that names one of these and does not get it ends
    # up pointing at the new workspace for an item that was never in it.
    ctx.source_items = {item["id"]: item for item in all_items if item.get("id")}
    ctx.run.summary["unsupported"] = [item.as_dict() for item in assessment.unsupported]

    warnings = assessment.grouped_messages()
    if ctx.plan.capacity_warning:
        warnings.append(ctx.plan.capacity_warning)
    if monitoring:
        warnings.append(
            "Workspace monitoring is on in the source workspace. Its eventhouse and KQL "
            "database are created by enabling the feature rather than as normal items, so "
            "they are not migrated. Turn workspace monitoring on in the new workspace's "
            "settings if you want it there."
        )

    if not warnings:
        ctx.run.finish_step(step, StepStatus.SUCCEEDED, "Everything in this workspace is supported")
        return

    ctx.warnings.extend(warnings)
    if assessment.unsupported:
        detail = f"{len(assessment.unsupported)} item(s) will be left behind in the source workspace"
    elif monitoring:
        detail = "Workspace monitoring is not migrated"
    else:
        detail = "Everything in this workspace is supported, with warnings"
    ctx.run.finish_step(step, StepStatus.SUCCEEDED, detail, warnings)


@dataclass(frozen=True, slots=True)
class ConnectionAccess:
    """A connection this service principal needs access to before the migration works."""

    connection_id: str
    used_by: tuple[str, ...] = ()
    connection_name: str = ""

    @property
    def label(self) -> str:
        """A name where Fabric has one, otherwise the id.

        Many of these have no display name at all, and the ones that matter most are exactly
        the ones this service principal cannot see, so there is nothing else to show.
        """
        return self.connection_name or self.connection_id

    def as_dict(self) -> dict[str, Any]:
        return {
            "connectionId": self.connection_id,
            "connectionName": self.connection_name,
            "label": self.label,
            "usedBy": list(self.used_by),
        }

    def message(self) -> str:
        return (
            f"Connection {self.label} is used by {', '.join(self.used_by)}, but this service "
            "principal cannot see it, so those items will be refused."
        )


def portal_instructions(client_id: str) -> list[str]:
    """How to share a connection with the service principal, in the portal.

    There is an API for this, but adding a role assignment needs Owner on the connection,
    which is what is missing in the first place, so it cannot be done for the operator.
    """
    return [
        "Open the Fabric portal and go to Settings, then Manage connections and gateways.",
        "Find each connection listed above by its id on the Connections tab.",
        "Select it, then open Manage users from the toolbar or the row's ... menu.",
        f"Add the service principal (application id {client_id}) with the User role, then "
        "Share. User is enough to bind a connection; Owner is not needed.",
        "Come back here and choose Re-check, which runs the assessment again.",
    ]


def _script_comment(text: str) -> str:
    """Make an item name safe to sit in a PowerShell comment."""
    return " ".join(str(text).split())[:80]


def grant_script(
    client_id: str,
    entries: Iterable[ConnectionAccess],
    *,
    object_id: str = "",
    tenant_id: str = "",
) -> str:
    """A PowerShell script that shares each connection with the service principal.

    Finding a connection in the portal means matching a bare GUID by eye against a list that
    does not show ids, which is unreasonable for more than one or two. The same grant is a
    single API call, so the ids that make it hard by hand are exactly what make it easy in a
    script.

    Fabric identifies a principal by object id rather than application id. Fab Shuffle knows
    its own object id from the ``oid`` claim of the token it already holds, so the script is
    given the value outright. Only if that is somehow unavailable does it fall back to asking
    the directory, which needs a second sign-in scope and the Az.Resources module.
    """
    listed = "\n".join(
        f"    '{entry.connection_id}'  # {_script_comment(', '.join(entry.used_by))}"
        for entry in entries
    )

    if object_id:
        requires = "#Requires -Modules Az.Accounts"
        resolve = (
            "# Fab Shuffle read this from its own access token, so no directory lookup is\n"
            "# needed. It is the object id of the service principal, which is what Fabric\n"
            f"# wants here; the application id ({client_id}) is a different GUID.\n"
            f"$principalId = '{object_id}'"
        )
    else:
        requires = "#Requires -Modules Az.Accounts, Az.Resources"
        resolve = (
            "# The role assignment API wants the service principal's object id, not its\n"
            "# application id.\n"
            f"$principal = Get-AzADServicePrincipal -ApplicationId '{client_id}'\n"
            "if (-not $principal) {\n"
            f"    throw \"No service principal found for application id {client_id}.\"\n"
            "}\n"
            "$principalId = $principal.Id"
        )

    # Signing in without a tenant lands on the "organizations" pseudo-tenant, which a
    # conditional access policy cannot be evaluated against, so the sign-in is refused.
    tenant = f"'{tenant_id}'" if tenant_id else "(Get-AzContext).Tenant.Id"

    return f"""{requires}
# Shares the connections Fab Shuffle needs with its service principal.
# Run this as a user who owns them, then choose Re-check in Fab Shuffle.

$ErrorActionPreference = 'Stop'

$fabric = 'https://api.fabric.microsoft.com'
$tenantId = {tenant}
$connectionIds = @(
{listed}
)

function Get-FabricToken {{
    # Az 14 / Az.Accounts 5 return a SecureString and warn about it beforehand. Asking for
    # one where the parameter exists takes the new behaviour deliberately and silences the
    # warning, rather than waiting to be broken by it.
    if ((Get-Command Get-AzAccessToken).Parameters.ContainsKey('AsSecureString')) {{
        $secure = (Get-AzAccessToken -ResourceUrl $fabric -TenantId $tenantId -AsSecureString).Token
        return [System.Net.NetworkCredential]::new('', $secure).Password
    }}
    $value = (Get-AzAccessToken -ResourceUrl $fabric -TenantId $tenantId).Token
    if ($value -isnot [string]) {{
        return [System.Net.NetworkCredential]::new('', $value).Password
    }}
    return $value
}}

function Connect-ForFabric {{
    # -AuthScope asks for a token Fabric will accept; -TenantId keeps conditional access off
    # the "organizations" pseudo-tenant, where it cannot be evaluated.
    Connect-AzAccount -TenantId $tenantId -AuthScope $fabric | Out-Null
}}

$context = Get-AzContext
if (-not $context -or $context.Tenant.Id -ne $tenantId) {{ Connect-ForFabric }}

try {{
    $token = Get-FabricToken
}} catch {{
    # An existing sign-in from somewhere else looks fine to Get-AzContext and is still
    # refused for Fabric, so the only way to find out is to ask.
    Write-Host 'Signing in again, scoped to Fabric.' -ForegroundColor Yellow
    Connect-ForFabric
    $token = Get-FabricToken
}}

{resolve}

$body = @{{
    principal = @{{ id = $principalId; type = 'ServicePrincipal' }}
    role      = 'User'
}} | ConvertTo-Json

foreach ($id in $connectionIds) {{
    try {{
        Invoke-RestMethod -Method POST -ContentType 'application/json' -Body $body `
            -Headers @{{ Authorization = "Bearer $token" }} `
            -Uri "$fabric/v1/connections/$id/roleAssignments" | Out-Null
        Write-Host "Shared $id" -ForegroundColor Green
    }} catch {{
        # One connection you do not own should not stop the rest.
        Write-Warning "Could not share $id : $($_.Exception.Message)"
    }}
}}
"""


def scan_connection_access(
    client: FabricClient,
    *,
    source_workspace_id: str,
) -> list[ConnectionAccess]:
    """Connections that items bind but this service principal cannot use.

    An item that binds an unusable connection is rejected outright at creation, and the
    rejection arrives per item, so a workspace where one connection is shared by six items
    fails six times for the same reason after everything else has already been built. The
    definitions are read up front instead, and reported by connection, because one grant
    fixes every item that shares it.
    """
    known = connections.connections_by_id(client)
    # Without the tenant's connections there is nothing to compare against, and the run
    # already reports that separately.
    if not known:
        return []
    return _unusable_bound_connections(client, source_workspace_id, known)


def _unusable_bound_connections(
    client: FabricClient,
    source_workspace_id: str,
    known: Mapping[str, Any],
) -> list[ConnectionAccess]:
    """Connections that items bind but this service principal cannot see."""
    binding_items: list[tuple[str, str, str]] = []
    for item_type in CONNECTION_BINDING_TYPES:
        for item in analytics.list_of_type(client, source_workspace_id, item_type):
            binding_items.append((item_type, item.get("displayName") or item["id"], item["id"]))

    if not binding_items:
        return []

    unusable: dict[str, list[str]] = {}
    for item_type, name, item_id in binding_items:
        definition = items_module.try_get_item_definition(client, source_workspace_id, item_id)
        if not definition:
            continue
        for connection_id in connections.referenced_connection_ids(definition.get("parts") or []):
            if connection_id not in known:
                unusable.setdefault(connection_id, []).append(f"{item_type} '{name}'")

    return [
        ConnectionAccess(
            connection_id=connection_id,
            used_by=tuple(sorted(items)),
        )
        for connection_id, items in sorted(unusable.items())
    ]


def bound_connection_warnings(
    client: FabricClient,
    *,
    source_workspace_id: str,
    client_id: str,
) -> list[str]:
    """The connection access problem as warnings, for the run's warning list."""
    blocked = scan_connection_access(client, source_workspace_id=source_workspace_id)
    if not blocked:
        return []
    return [entry.message() for entry in blocked] + [
        f"Share the connection(s) above with application id {client_id} in Manage connections "
        "and gateways, then re-check."
    ]


def dependency_warnings(
    client: FabricClient,
    *,
    source_workspace_id: str,
    migrated: list[dict[str, Any]],
    client_id: str,
) -> DependencyReport:
    """Work out which references will not survive the move.

    Kept free of run state so the review screen and the migration itself reach exactly the
    same conclusions — a warning the operator was shown before starting should not reappear
    as a surprise, or worse, appear only once the workspace is half built.
    """
    graph = relations.build_graph(client, source_workspace_id, migrated)
    if not graph.available:
        return DependencyReport(graph=graph, available=False)

    issues = relations.analyse(
        graph,
        migrated_ids={item["id"] for item in migrated if item.get("id")},
        source_workspace_id=source_workspace_id,
    )
    prerequisites = connection_prerequisites(
        client,
        source_workspace_id=source_workspace_id,
        migrated=migrated,
        client_id=client_id,
    )
    return DependencyReport(
        graph=graph,
        issues=issues,
        prerequisites=prerequisites,
        access=scan_connection_access(client, source_workspace_id=source_workspace_id),
    )


def connection_prerequisites(
    client: FabricClient,
    *,
    source_workspace_id: str,
    migrated: list[dict[str, Any]],
    client_id: str,
) -> list[connections.Prerequisite]:
    """Find connections that point at items in the workspace being migrated.

    Fabric does not let a connection's target change, so these cannot be repointed at the
    migrated items. Surfacing them before anything is created means the operator learns what
    they will have to rebuild by hand while it is still cheap to stop.
    """
    endpoints: list[str] = []
    for lakehouse in data_stores.list_lakehouses(client, source_workspace_id):
        endpoint = data_stores.lakehouse_sql_endpoint(lakehouse)
        endpoints.extend(filter(None, [endpoint.get("connectionString"), endpoint.get("id")]))
    for warehouse in data_stores.list_warehouses(client, source_workspace_id):
        endpoints.append(data_stores.warehouse_connection_string(warehouse))
    for eventhouse in eventhouses.list_eventhouses(client, source_workspace_id):
        properties = eventhouse.get("properties") or {}
        endpoints.extend(
            filter(None, [properties.get("queryServiceUri"), properties.get("ingestionServiceUri")])
        )

    identifiers = connections.source_identifiers(source_workspace_id, migrated, endpoints)
    return connections.scan_prerequisites(client, identifiers=identifiers, client_id=client_id)


def _check_dependencies(ctx: _Context) -> None:
    """Report dependencies that will not survive the move, before anything is created.

    References are rewritten through an id map covering only the items this run creates, so a
    dependency on another workspace, or on an item type Fab Shuffle does not migrate, leaves
    the copy pointing somewhere it should not. That is invisible in the item definitions.
    """
    step = "dependencies"
    ctx.run.start_step(step, "Checking dependencies between items")
    ctx.run.raise_if_cancelled()

    migrated = (ctx.assessment.migrated if ctx.assessment else []) or []
    if not migrated:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "Nothing to check")
        return

    ctx.run.update_step(step, f"Reading relations for {len(migrated)} item(s)")
    report = dependency_warnings(
        ctx.client,
        source_workspace_id=ctx.plan.source_workspace_id,
        migrated=migrated,
        client_id=ctx.principal.client_id,
    )
    ctx.graph = report.graph

    if not report.available:
        ctx.run.finish_step(
            step,
            StepStatus.SKIPPED,
            "The relations API is unavailable to this service principal, so dependencies "
            "could not be checked",
        )
        return

    if report.issues:
        ctx.run.summary["dependencyIssues"] = [issue.as_dict() for issue in report.issues]
    if report.prerequisites:
        ctx.run.summary["connectionPrerequisites"] = [p.as_dict() for p in report.prerequisites]
    if report.access:
        ctx.run.summary["connectionAccess"] = [entry.as_dict() for entry in report.access]

    warnings = report.messages()
    # Access problems are listed separately, because each one stops specific items from being
    # created at all rather than leaving them subtly wrong.
    warnings.extend(entry.message() for entry in report.access)
    if report.access:
        warnings.append(
            "Share the connection(s) above with application id "
            f"{ctx.principal.client_id} in Manage connections and gateways, then re-check."
        )
    if not warnings:
        ctx.run.finish_step(step, StepStatus.SUCCEEDED, "Every dependency is inside this migration")
        return

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step,
        StepStatus.SUCCEEDED,
        f"{len(warnings)} reference(s) need attention before this workspace is usable",
        warnings,
    )



# --------------------------------------------------------------------- phase 1


def _create_workspaces(ctx: _Context) -> None:
    step = "workspaces"
    ctx.run.start_step(step, "Creating target and scratch workspaces")
    ctx.run.raise_if_cancelled()

    target = workspaces.create_workspace(
        ctx.client,
        ctx.plan.target_workspace_name,
        ctx.plan.capacity_id,
        description=(
            f"Created by Fab Shuffle from '{ctx.plan.source_workspace_name}' "
            f"in {ctx.plan.capacity_region}."
        ),
    )
    ctx.target_workspace_id = target["id"]
    ctx.run.target_workspace = {"id": target["id"], "displayName": ctx.plan.target_workspace_name}
    ctx.id_map[ctx.plan.source_workspace_id] = target["id"]

    # Grant the source workspace's admins straight away rather than waiting for the final
    # permissions phase. A run that fails before then would otherwise leave a workspace only
    # this service principal can see, which nobody else can inspect or delete.
    ctx.run.update_step(step, "Granting workspace admins access")
    ctx.source_role_assignments = workspaces.list_role_assignments(
        ctx.client, ctx.plan.source_workspace_id
    )
    admin_warnings = workspaces.copy_role_assignments(
        ctx.client, ctx.source_role_assignments, ctx.target_workspace_id, roles={"Admin"}
    )
    ctx.warnings.extend(admin_warnings)

    # Copy Jobs must live somewhere that is not the workspace being built, otherwise they
    # show up as leftover items in the migrated workspace.
    scratch_name = workspaces.scratch_workspace_name()
    ctx.run.update_step(step, "Creating scratch workspace for Copy Jobs")
    scratch = workspaces.create_workspace(
        ctx.client,
        scratch_name,
        ctx.plan.capacity_id,
        description="Temporary Fab Shuffle workspace for Copy Jobs. Safe to delete.",
    )
    ctx.scratch_workspace_id = scratch["id"]
    ctx.run.scratch_workspace = {"id": scratch["id"], "displayName": scratch_name}

    # The scratch workspace needs the same treatment so a stranded one stays deletable.
    workspaces.copy_role_assignments(
        ctx.client, ctx.source_role_assignments, ctx.scratch_workspace_id, roles={"Admin"}
    )

    # A workspace is not fully initialised for Copy Jobs until it holds a lakehouse.
    data_stores.create_lakehouse(ctx.client, ctx.scratch_workspace_id, "hold")

    ctx.run.update_step(step, "Recreating workspace folders")
    folder_map = workspaces.clone_folder_tree(
        ctx.client, ctx.plan.source_workspace_id, ctx.target_workspace_id
    )
    ctx.id_map.update(folder_map)

    warnings = _copy_spark_configuration(ctx, step)
    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step, StepStatus.SUCCEEDED, f"Created '{ctx.plan.target_workspace_name}'", warnings
    )


def _copy_spark_configuration(ctx: _Context, step: str) -> list[str]:
    """Recreate custom Spark pools and workspace Spark settings.

    Runs here rather than in the engineering phase because an environment pins a pool by id,
    so the pool has to exist, and be in the id map, before any environment is migrated.
    """
    ctx.run.update_step(step, "Recreating custom Spark pools")
    pool_map, created, warnings = spark.copy_pools(
        ctx.client, ctx.plan.source_workspace_id, ctx.target_workspace_id
    )
    ctx.id_map.update(pool_map)
    if created:
        logger.info("Recreated %s custom Spark pool(s)", len(created))

    settings = spark.get_settings(ctx.client, ctx.plan.source_workspace_id)
    if settings:
        ctx.spark_settings = settings
        ctx.run.update_step(step, "Applying workspace Spark settings")
        # The new workspace already carries Fabric's defaults for its capacity, so only the
        # settings that actually differ are worth sending.
        target = spark.get_settings(ctx.client, ctx.target_workspace_id)
        patches, settings_warnings = spark.build_settings_payload(settings, pool_map, target=target)
        warnings.extend(settings_warnings)
        warnings.extend(_apply_spark_patches(ctx, patches))

    return warnings


def _apply_spark_patches(
    ctx: _Context,
    patches: list[tuple[str, dict[str, Any]]],
) -> list[str]:
    """Apply Spark settings one section at a time.

    Fabric answers a rejected settings body with a bare 400, so sending them separately keeps
    one unsupported value from discarding the rest, and names the section that failed.
    """
    warnings: list[str] = []
    for label, payload in patches:
        try:
            spark.update_settings(ctx.client, ctx.target_workspace_id, payload)
        except FabricApiError as error:
            if "SparkSettingsInvalidNodeCount" in error.body:
                warnings.append(
                    f"The source workspace's starter pool is larger than capacity "
                    f"'{ctx.plan.capacity_name}' allows, so the new workspace keeps its own "
                    "starter pool sizing."
                )
            else:
                warnings.append(
                    f"Could not apply {label} (HTTP {error.status_code}: {error.body[:200]}). "
                    "Check them in the new workspace."
                )
    return warnings


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
    # (database, target eventhouse id, source query URI) for shortcut databases, which are
    # created once every leader they might follow exists.
    deferred_followers: list[tuple[dict[str, Any], str, str]] = []

    for eventhouse in source_eventhouses:
        ctx.run.raise_if_cancelled()
        name = eventhouse["displayName"]
        ctx.run.update_step(step, f"Creating eventhouse '{name}'")

        created = eventhouses.create_eventhouse(
            ctx.client,
            ctx.target_workspace_id,
            name,
            folder_id=ctx.id_map.get(eventhouse.get("folderId", "")),
        )
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
            database = eventhouses.get_kql_database(
                ctx.client, ctx.plan.source_workspace_id, database_id
            )
            if eventhouses.database_type(database) != "ReadWrite":
                # A follower may point at a leader elsewhere in this same workspace, which
                # might not exist yet, so every follower waits until the leaders are done.
                deferred_followers.append(
                    (database, created["id"], source_properties.get("queryServiceUri", ""))
                )
                continue

            moved, database_warnings, adopted = _migrate_kql_database(
                ctx,
                step,
                database=database,
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

    for database, target_eventhouse_id, source_query_uri in deferred_followers:
        ctx.run.raise_if_cancelled()
        ctx.run.update_step(step, f"Recreating shortcut database '{database['displayName']}'")
        moved, database_warnings = _migrate_follower_database(
            ctx,
            database=database,
            target_eventhouse_id=target_eventhouse_id,
            source_query_uri=source_query_uri,
        )
        databases_moved += 1 if moved else 0
        warnings.extend(database_warnings)

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step,
        StepStatus.SUCCEEDED,
        f"Migrated {len(source_eventhouses)} eventhouse(s) and {databases_moved} KQL database(s)",
        warnings,
    )


def _migrate_follower_database(
    ctx: _Context,
    *,
    database: dict[str, Any],
    target_eventhouse_id: str,
    source_query_uri: str,
) -> tuple[bool, list[str]]:
    """Recreate a shortcut (follower) KQL database against the same leader.

    A follower holds no data of its own, so it is recreated rather than copied. The source is
    not in the item's properties; it comes from asking the follower's own cluster with
    ``.show follower database``, whose ``OriginalDatabaseName`` is the leader's KQL Database
    item id when the leader is a Fabric eventhouse.
    """
    name = database["displayName"]
    kusto_name = database.get("id") or name

    source: kql.FollowerSource | None = None
    if source_query_uri:
        try:
            source = kql.follower_source(source_query_uri, kusto_name, ctx.principal)
        except Exception as error:  # any Kusto failure just means the source is unknown
            logger.info("Could not read the follower source for '%s': %s", name, error)

    source_database_name = ""
    if source:
        source_database_name = source.database_name
        if source.is_fabric_source:
            # Fabric resolves a leader by item id within the tenant, so no cluster URI is
            # needed. The leader may be in this very workspace, in which case the copy has to
            # follow the copy rather than reaching back across the region boundary.
            source_database_name = ctx.id_map.get(source.database_name, source.database_name)
        elif not (database.get("properties") or {}).get("sourceClusterUri"):
            # An Azure Data Explorer leader is identified by name, which means nothing without
            # its cluster URI, and that is not recoverable from what the follower reports.
            return (
                False,
                [
                    f"KQL database '{name}' follows the Azure Data Explorer database "
                    f"'{source.database_name}', but its cluster URI is not exposed by the "
                    "API, so it was skipped. Recreate the shortcut by hand."
                ],
            )

    payload = eventhouses.shortcut_creation_payload(
        database,
        target_eventhouse_id,
        source_database_name=source_database_name,
    )
    if not payload:
        return (
            False,
            [
                f"KQL database '{name}' is a shortcut/follower database and its source could "
                "not be determined, so it was skipped. Recreate the shortcut by hand."
            ],
        )

    target = eventhouses.create_kql_database(
        ctx.client,
        ctx.target_workspace_id,
        name,
        creation_payload=payload,
        folder_id=ctx.id_map.get(database.get("folderId", "")),
    )
    ctx.id_map[database["id"]] = target["id"]
    return True, []


def _migrate_kql_database(
    ctx: _Context,
    step: str,
    *,
    database: dict[str, Any],
    target_eventhouse_id: str,
    source_query_uri: str,
    target_query_uri: str,
    existing_databases: dict[str, Any],
) -> tuple[bool, list[str], str | None]:
    """Migrate one ReadWrite KQL database. Returns (moved, warnings, adopted name if any)."""
    database_id = database["id"]
    name = database["displayName"]

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
        folder_id=ctx.id_map.get(database.get("folderId", "")),
    )
    ctx.id_map[database_id] = target["id"]
    ctx.kql_databases.append((database_id, target["id"], name))
    if adopted:
        logger.info("Applied schema to the default KQL database '%s'", name)

    adopted_name = name if adopted else None

    # Table shortcuts point at other items, which may not exist yet, so they are created in
    # the shortcut phase. Their names are still needed now to keep them out of the copy.
    table_shortcuts = shortcuts.list_table_shortcuts(
        ctx.client, ctx.plan.source_workspace_id, database_id
    )
    ctx.kql_table_shortcuts[database_id] = table_shortcuts
    shortcut_names = {s["name"] for s in table_shortcuts if s.get("name")}

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
        exclude=shortcut_names,
        on_progress=lambda message: ctx.run.update_step(step, message),
    )
    logger.info("KQL database %s: copied %s table(s)", name, result["tables"])
    return True, [], adopted_name


# --------------------------------------------------------------------- phase 3


def _migrate_lakehouses(ctx: _Context) -> None:
    """Recreate lakehouses, then copy their data.

    Creation and data movement are separated so the copy jobs can run together. Nothing in
    this phase reads another lakehouse, so there is no reason to finish one before starting
    the next, and the table copies are the longest thing in the whole migration.
    """
    step = "lakehouses"
    ctx.run.start_step(step, "Migrating lakehouses")
    ctx.run.raise_if_cancelled()

    source_lakehouses = data_stores.list_lakehouses(ctx.client, ctx.plan.source_workspace_id)
    if not source_lakehouses:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "No lakehouses in the source workspace")
        return

    warnings: list[str] = []
    created_pairs: list[tuple[dict[str, Any], dict[str, Any], bool]] = []

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
        # Both may be absent. The endpoint is provisioned asynchronously, so a lakehouse read
        # straight after creation usually has no endpoint id or connection string yet. The
        # shortcut phase reads it again, by which time it exists, and records what is missing.
        _map_sql_endpoint(ctx, source_endpoint, target_endpoint)

        created_pairs.append((lakehouse, target, schema_enabled))

    if ctx.plan.include_data:
        warnings.extend(_copy_lakehouse_tables(ctx, step, created_pairs))

    if ctx.plan.include_files:
        warnings.extend(_copy_all_lakehouse_files(ctx, step, created_pairs))

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step, StepStatus.SUCCEEDED, f"Migrated {len(source_lakehouses)} lakehouse(s)", warnings
    )


def _map_sql_endpoint(
    ctx: _Context,
    source_endpoint: Mapping[str, Any],
    target_endpoint: Mapping[str, Any],
) -> None:
    """Record a SQL analytics endpoint's identifiers, if both ends have them yet.

    An endpoint is created by Fabric alongside its lakehouse, not by us, and not
    synchronously: the item read back straight after creation frequently reports no endpoint
    at all. So this is called twice, once at creation and again once the endpoint has
    appeared, and only fills in what it can each time.

    Both identifiers matter. A semantic model refers to its endpoint by connection string in
    one place and by item id in another, and missing either leaves the model reading from the
    workspace being migrated away from.
    """
    for key in ("connectionString", "id"):
        source_value = source_endpoint.get(key)
        target_value = target_endpoint.get(key)
        if source_value and target_value:
            ctx.id_map[source_value] = target_value


def _copy_lakehouse_tables(
    ctx: _Context,
    step: str,
    pairs: list[tuple[dict[str, Any], dict[str, Any], bool]],
) -> list[str]:
    """Copy every lakehouse's tables, several jobs at a time."""
    warnings: list[str] = []
    specs: list[copyjobs.CopyJobSpec] = []

    for lakehouse, target, schema_enabled in pairs:
        ctx.run.raise_if_cancelled()
        name = lakehouse["displayName"]
        try:
            tables = _lakehouse_tables(ctx, lakehouse, schema_enabled=schema_enabled)
        except (sqlschema.SchemaTransferError, FabricApiError) as error:
            warnings.append(
                f"Could not list tables in lakehouse '{name}', so its data was not copied: {error}"
            )
            continue
        if not tables:
            continue

        specs.append(
            copyjobs.CopyJobSpec(
                workspace_id=ctx.scratch_workspace_id,
                display_name=f"CopyJob_Lakehouse_{name}",
                content=copyjobs.build_lakehouse_copy_job(
                    source_workspace_id=ctx.plan.source_workspace_id,
                    source_item_id=lakehouse["id"],
                    target_workspace_id=ctx.target_workspace_id,
                    target_item_id=target["id"],
                    tables=tables,
                ),
                label=f"Table data for lakehouse '{name}'",
            )
        )

    warnings.extend(_run_copy_jobs(ctx, step, specs, "lakehouse"))
    return warnings


def _run_copy_jobs(
    ctx: _Context,
    step: str,
    specs: list[copyjobs.CopyJobSpec],
    what: str,
) -> list[str]:
    if not specs:
        return []
    # Sized from the capacity the jobs will run on, not from a fixed number: an F64 can keep
    # four of these busy where an F16 can barely keep one.
    concurrent = workspaces.copy_job_concurrency(ctx.plan.capacity_sku)
    ctx.run.update_step(
        step,
        f"Copying {what} data in {len(specs)} job(s), {concurrent} at a time",
    )
    created, warnings = copyjobs.run_copy_jobs(
        ctx.client,
        specs,
        concurrency=concurrent,
        on_progress=lambda message: ctx.run.update_step(step, message),
    )
    ctx.copy_job_ids.extend(created)
    return warnings


def _lakehouse_tables(
    ctx: _Context,
    lakehouse: dict[str, Any],
    *,
    schema_enabled: bool,
) -> list[data_stores.TableRef]:
    """List the copyable tables in a lakehouse.

    Two things have to be worked around. The lakehouse tables API rejects schema-enabled
    lakehouses with ``UnsupportedOperationForSchemasEnabledLakehouse``, so those are
    enumerated over TDS through the SQL analytics endpoint instead.

    Either way shortcuts have to come out, and neither source can tell us which tables they
    are: ``TableType`` is only ``Managed`` or ``External``, and on the SQL endpoint a shortcut
    is just a table. So both lists are filtered against the shortcuts API. Their data belongs
    to whatever they point at, and they are recreated properly in the shortcut phase.
    """
    workspace_id = ctx.plan.source_workspace_id
    if schema_enabled:
        endpoint = data_stores.lakehouse_sql_endpoint(lakehouse).get("connectionString")
        if not endpoint:
            raise sqlschema.SchemaTransferError(
                "the lakehouse has no SQL analytics endpoint, which is the only way to list the "
                "tables of a schema-enabled lakehouse"
            )
        found = [
            data_stores.TableRef(name=table, schema=schema)
            for schema, table in sqlschema.list_base_tables(
                endpoint, lakehouse["displayName"], ctx.tokens
            )
        ]
    else:
        found = data_stores.managed_tables(
            ctx.client, workspace_id, lakehouse["id"], schema_enabled=False
        )

    shortcut_keys = shortcuts.table_shortcut_keys(
        shortcuts.list_shortcuts(ctx.client, workspace_id, lakehouse["id"])
    )
    return [
        ref
        for ref in found
        if (ref.schema.casefold() if ref.schema else None, ref.name.casefold())
        not in shortcut_keys
    ]


def _copy_all_lakehouse_files(
    ctx: _Context,
    step: str,
    pairs: list[tuple[dict[str, Any], dict[str, Any], bool]],
) -> list[str]:
    """Copy every lakehouse's ``Files/``, a couple at a time.

    Bounded tightly. Each copy stages the whole directory through local disk on the way past,
    and azcopy already tunes its own parallelism, so more of them at once mostly means more
    of them competing for the same disk.
    """
    jobs = [
        concurrency.Job(
            key=lakehouse["displayName"],
            run=_lakehouse_file_job(ctx, lakehouse, target),
        )
        for lakehouse, target, _schema in pairs
    ]
    return concurrency.run_bounded(
        jobs,
        limit=SETTINGS.file_transfer_concurrency,
        on_progress=lambda message: ctx.run.update_step(step, f"Copying files: {message}"),
        noun="file copy",
    )


def _lakehouse_file_job(
    ctx: _Context,
    lakehouse: dict[str, Any],
    target: dict[str, Any],
) -> Callable[[], list[str]]:
    name = lakehouse["displayName"]
    source_files = (lakehouse.get("properties") or {}).get("oneLakeFilesPath")
    target_files = (target.get("properties") or {}).get("oneLakeFilesPath")

    def run() -> list[str]:
        if not source_files or not target_files:
            return []
        ctx.run.raise_if_cancelled()
        try:
            file_transfer.copy_files(
                source_files_path=source_files,
                target_files_path=target_files,
                principal=ctx.principal,
                scratch_dir=ctx.scratch_dir / f"lakehouse-{lakehouse['id']}",
            )
            return []
        except file_transfer.FileTransferError as error:
            return [f"Files for lakehouse '{name}' did not copy: {error}"]

    return run


# --------------------------------------------------------------------- phase 4


def _migrate_warehouses(ctx: _Context) -> None:
    """Recreate warehouses: create them all, transfer schema, then copy all the data.

    Each stage waits for the last because each depends on it, but within a stage nothing
    here reads another warehouse, so the schema transfers run together and so do the copies.
    """
    step = "warehouses"
    ctx.run.start_step(step, "Migrating warehouses")
    ctx.run.raise_if_cancelled()

    source_warehouses = data_stores.list_warehouses(ctx.client, ctx.plan.source_workspace_id)
    if not source_warehouses:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "No warehouses in the source workspace")
        return

    warnings: list[str] = []
    created_pairs: list[tuple[dict[str, Any], str, str, str]] = []

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

        created_pairs.append((warehouse, created["id"], source_endpoint, target_endpoint))

    ready, schema_warnings = _transfer_warehouse_schemas(ctx, step, created_pairs)
    warnings.extend(schema_warnings)

    if ctx.plan.include_data:
        warnings.extend(_copy_warehouse_tables(ctx, step, ready))

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step, StepStatus.SUCCEEDED, f"Migrated {len(source_warehouses)} warehouse(s)", warnings
    )


def _transfer_warehouse_schemas(
    ctx: _Context,
    step: str,
    pairs: list[tuple[dict[str, Any], str, str, str]],
) -> tuple[list[tuple[dict[str, Any], str, str, str]], list[str]]:
    """Copy every warehouse's schema, a couple at a time.

    Most of the elapsed time here is spent waiting: a freshly created endpoint takes minutes
    to answer at all, and it does that whether or not we are watching another one.
    """
    ready: list[tuple[dict[str, Any], str, str, str]] = []
    jobs: list[concurrency.Job] = []

    def transfer(entry: tuple[dict[str, Any], str, str, str]) -> Callable[[], list[str]]:
        warehouse, _target_id, source_endpoint, target_endpoint = entry
        name = warehouse["displayName"]

        def run() -> list[str]:
            ctx.run.raise_if_cancelled()
            try:
                warnings = sqlschema.transfer_schema(
                    source_server=source_endpoint,
                    target_server=target_endpoint,
                    database=name,
                    principal=ctx.principal,
                    tokens=ctx.tokens,
                    scratch_dir=ctx.scratch_dir / "sql",
                    source_type="Warehouse",
                )
            except sqlschema.SchemaTransferError as error:
                # Without a schema there is nowhere for the rows to land, so this warehouse
                # is left out of the copy rather than failing a table at a time.
                return [f"Schema for warehouse '{name}' did not transfer: {error}"]
            ready.append(entry)
            return [f"Warehouse '{name}': {w}" for w in warnings]

        return run

    for entry in pairs:
        jobs.append(concurrency.Job(key=entry[0]["displayName"], run=transfer(entry)))

    warnings = concurrency.run_bounded(
        jobs,
        limit=SETTINGS.schema_transfer_concurrency,
        on_progress=lambda message: ctx.run.update_step(step, f"Transferring schema: {message}"),
        noun="schema transfer",
    )
    # Restored to the order the warehouses were listed in, so the copy jobs that follow are
    # started in a fixed order too.
    order = {id(entry): index for index, entry in enumerate(pairs)}
    ready.sort(key=lambda entry: order[id(entry)])
    return ready, warnings


def _copy_warehouse_tables(
    ctx: _Context,
    step: str,
    ready: list[tuple[dict[str, Any], str, str, str]],
) -> list[str]:
    warnings: list[str] = []
    specs: list[copyjobs.CopyJobSpec] = []

    for warehouse, target_id, source_endpoint, target_endpoint in ready:
        ctx.run.raise_if_cancelled()
        name = warehouse["displayName"]
        try:
            tables = _warehouse_tables(ctx, source_endpoint, name)
        except sqlschema.SchemaTransferError as error:
            warnings.append(f"Could not enumerate tables in warehouse '{name}': {error}")
            continue
        if not tables:
            continue

        specs.append(
            copyjobs.CopyJobSpec(
                workspace_id=ctx.scratch_workspace_id,
                display_name=f"CopyJob_Warehouse_{name}",
                content=copyjobs.build_warehouse_copy_job(
                    source_workspace_id=ctx.plan.source_workspace_id,
                    source_item_id=warehouse["id"],
                    source_endpoint=source_endpoint,
                    target_workspace_id=ctx.target_workspace_id,
                    target_item_id=target_id,
                    target_endpoint=target_endpoint,
                    tables=tables,
                ),
                label=f"Table data for warehouse '{name}'",
            )
        )

    warnings.extend(_run_copy_jobs(ctx, step, specs, "warehouse"))
    return warnings


def _warehouse_tables(ctx: _Context, endpoint: str, database: str) -> list[data_stores.TableRef]:
    """Enumerate base tables over TDS; warehouses have no REST table listing API."""
    return [
        data_stores.TableRef(name=table, schema=schema)
        for schema, table in sqlschema.list_base_tables(endpoint, database, ctx.tokens)
    ]


# --------------------------------------------------------------------- phase 5


def _migrate_sql_databases(ctx: _Context) -> None:
    """Recreate Fabric SQL and Cosmos DB databases.

    Both run here rather than with the definition-backed items, because each is a data store
    that other things read: a GraphQL API, a Copy Job, and a pipeline can all bind a SQL
    database, and the relations APIs call it ``SqlDbNative`` when they report that.
    """
    step = "sqldatabases"
    ctx.run.start_step(step, "Migrating SQL and Cosmos databases")
    ctx.run.raise_if_cancelled()

    source_databases = sqldatabases.list_sql_databases(ctx.client, ctx.plan.source_workspace_id)
    cosmos_migrated, cosmos_warnings = _migrate_cosmos_databases(ctx)
    if not source_databases and not cosmos_migrated and not cosmos_warnings:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "No SQL or Cosmos databases to migrate")
        return

    warnings: list[str] = list(cosmos_warnings)
    migrated = 0
    ready: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for database in source_databases:
        ctx.run.raise_if_cancelled()
        name = database["displayName"]
        properties = database.get("properties") or {}

        ctx.run.update_step(step, f"Creating SQL database '{name}'")
        try:
            created = sqldatabases.create_sql_database(
                ctx.client,
                ctx.target_workspace_id,
                name,
                collation=properties.get("collation"),
                backup_retention_days=properties.get("backupRetentionDays"),
                description=database.get("description") or None,
                folder_id=ctx.id_map.get(database.get("folderId", "")),
            )
        except FabricError as error:
            warnings.append(analytics.describe_failure(sqldatabases.SQL_DATABASE, name, error))
            continue

        target = sqldatabases.get_sql_database(ctx.client, ctx.target_workspace_id, created["id"])
        ctx.id_map[database["id"]] = created["id"]
        source_server = sqldatabases.server_fqdn(database)
        target_server = sqldatabases.server_fqdn(target)
        if source_server and target_server:
            ctx.id_map[source_server] = target_server
        migrated += 1

        ctx.run.update_step(step, f"Applying schema to SQL database '{name}'")
        try:
            sqldatabases.copy_schema(
                ctx.client,
                source_workspace_id=ctx.plan.source_workspace_id,
                source_id=database["id"],
                target_workspace_id=ctx.target_workspace_id,
                target_id=created["id"],
            )
        except FabricError as error:
            # Without the schema there is nowhere to land rows, so the data copy is skipped
            # too rather than left to fail one table at a time.
            warnings.append(
                f"Schema for SQL database '{name}' did not transfer, so its data was not "
                f"copied either: {error}"
            )
            continue

        ready.append((database, target))

    if ctx.plan.include_data and ready:
        warnings.extend(_copy_sql_database_tables(ctx, step, ready))

    ctx.warnings.extend(warnings)
    summary = ", ".join(
        part
        for part in (
            f"{migrated} SQL database(s)" if migrated or source_databases else "",
            f"{cosmos_migrated} Cosmos DB database(s)" if cosmos_migrated else "",
        )
        if part
    )
    ctx.run.finish_step(step, StepStatus.SUCCEEDED, f"Migrated {summary or 'nothing'}", warnings)


def _copy_sql_database_tables(
    ctx: _Context,
    step: str,
    ready: list[tuple[dict[str, Any], dict[str, Any]]],
) -> list[str]:
    """Copy rows between SQL databases with bcp.

    A Copy Job cannot do this. The SQL database in Fabric connector accepts only an
    organizational account, so there is no connection a service principal can create that the
    job would be able to use; the one we tried failed with an invalid token, having asked for
    one against the wrong thing.

    bcp supports SQL database in Fabric directly, and authenticates with the access token we
    already hold and already use to read these tables. So the rows go out to a native-format
    file and straight back in, with no connection involved at either end.
    """
    warnings: list[str] = []

    for source, target in ready:
        ctx.run.raise_if_cancelled()
        name = source["displayName"]
        source_server = sqldatabases.server_fqdn(source)
        source_catalog = sqldatabases.database_name(source)
        target_server = sqldatabases.server_fqdn(target)
        target_catalog = sqldatabases.database_name(target)
        if not source_server or not source_catalog or not target_server or not target_catalog:
            warnings.append(
                f"SQL database '{name}' did not report a server and database name, so its "
                "data was not copied."
            )
            continue

        try:
            tables = [
                data_stores.TableRef(name=table, schema=schema)
                for schema, table in sqlschema.list_base_tables(
                    source_server, source_catalog, ctx.tokens
                )
            ]
        except sqlschema.SchemaTransferError as error:
            warnings.append(f"Could not enumerate tables in SQL database '{name}': {error}")
            continue
        if not tables:
            continue

        ctx.run.update_step(step, f"Copying {len(tables)} table(s) from SQL database '{name}'")
        try:
            warnings.extend(
                f"SQL database '{name}': {w}"
                for w in bulkcopy.copy_tables(
                    source_server=source_server,
                    source_database=source_catalog,
                    target_server=target_server,
                    target_database=target_catalog,
                    tables=tables,
                    tokens=ctx.tokens,
                    scratch_dir=ctx.scratch_dir / f"bcp-{source['id']}",
                    on_progress=_bulk_copy_progress(ctx, step, f"SQL database '{name}'"),
                )
            )
        except bulkcopy.BulkCopyError as error:
            warnings.append(f"Table data for SQL database '{name}' did not copy: {error}")

    return warnings


def _bulk_copy_progress(ctx: _Context, step: str, label: str) -> Callable[[str], None]:
    """Bind the label now, rather than reading it from the loop when the callback runs."""
    return lambda message: ctx.run.update_step(step, f"{label}: {message}")


def _document_progress(ctx: _Context, step: str, name: str) -> Any:
    """Bind the database name now, rather than reading it from the loop when called."""
    return lambda message: ctx.run.update_step(step, f"{name}: {message}")


def _migrate_cosmos_databases(ctx: _Context) -> tuple[int, list[str]]:
    """Recreate Cosmos DB databases: containers from the definition, documents over the SDK.

    Runs in the SQL database phase because it is the same kind of work at the same point in
    the order, and a workspace almost never has both.
    """
    source_databases = cosmosdb.list_cosmos_databases(ctx.client, ctx.plan.source_workspace_id)
    if not source_databases:
        return 0, []

    step = "sqldatabases"
    warnings: list[str] = []
    migrated = 0
    for database in source_databases:
        ctx.run.raise_if_cancelled()
        name = database["displayName"]

        # The definition holds only container metadata and no Fabric ids, so there is
        # nothing to rewrite and the generic path handles it.
        ctx.run.update_step(step, f"Creating Cosmos DB database '{name}'")
        try:
            result = analytics.migrate_definition_item(
                ctx.client,
                source_workspace_id=ctx.plan.source_workspace_id,
                target_workspace_id=ctx.target_workspace_id,
                item=database,
                item_type=cosmosdb.COSMOS_DB_DATABASE,
                id_map=ctx.id_map,
                folder_id=ctx.id_map.get(database.get("folderId", "")),
            )
        except FabricError as error:
            warnings.append(
                analytics.describe_failure(cosmosdb.COSMOS_DB_DATABASE, name, error)
            )
            continue

        ctx.id_map[result.source_id] = result.target_id
        migrated += 1

        if not ctx.plan.include_data:
            continue

        target = cosmosdb.get_cosmos_database(ctx.client, ctx.target_workspace_id, result.target_id)
        source_endpoint = cosmosdb.endpoint_url(database)
        target_endpoint = cosmosdb.endpoint_url(target)
        if not source_endpoint or not target_endpoint:
            warnings.append(
                f"Cosmos DB database '{name}' did not report an endpoint, so its documents "
                "were not copied. Its containers are there and ready for them."
            )
            continue

        ctx.run.update_step(step, f"Copying documents for Cosmos DB database '{name}'")
        progress = _document_progress(ctx, step, name)
        try:
            warnings.extend(
                f"Cosmos DB database '{name}': {w}"
                for w in cosmos_transfer.copy_documents(
                    source_endpoint=source_endpoint,
                    source_database=cosmosdb.database_name(database),
                    target_endpoint=target_endpoint,
                    target_database=cosmosdb.database_name(target),
                    tokens=ctx.tokens,
                    on_progress=progress,
                )
            )
        except cosmos_transfer.CosmosTransferError as error:
            warnings.append(
                f"Documents for Cosmos DB database '{name}' did not copy: {error}. Its "
                "containers are there and ready for them."
            )

    return migrated, warnings


# --------------------------------------------------------------------- phase 6


def _migrate_mirrored_databases(ctx: _Context) -> None:
    """Recreate mirrored databases.

    A mirrored database is a data store with its own SQL analytics endpoint, so it goes with
    the other data stores: a semantic model can read it, and its endpoint has to be in the id
    map before the analytics phase.

    Mirroring is deliberately not started. Creating the item does not start replication, and
    starting it would add a second live mirror against the same source database while the
    original is presumably still running. That is the operator's call.
    """
    step = "mirrored"
    ctx.run.start_step(step, "Migrating mirrored databases")
    ctx.run.raise_if_cancelled()

    source_id = ctx.plan.source_workspace_id
    databases = data_stores.list_mirrored_databases(ctx.client, source_id)
    catalogs = analytics.list_of_type(ctx.client, source_id, analytics.MIRRORED_ADB_CATALOG)
    snowflake = analytics.list_of_type(ctx.client, source_id, analytics.SNOWFLAKE_DATABASE)
    if not databases and not catalogs and not snowflake:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "No mirrored data stores in the source workspace")
        return

    warnings: list[str] = []

    def progress(message: str) -> None:
        ctx.run.update_step(step, message)

    if not databases:
        migrated: list[analytics.MigratedItem] = []
        running: dict[str, str | None] = {}
    else:
        # Record what was running before the move, so the warning can say whether replication
        # actually needs restarting in the new region.
        running = {
            db["id"]: data_stores.mirroring_status(ctx.client, source_id, db["id"])
            for db in databases
        }

        migrated, item_warnings = analytics.migrate_items(
            ctx.client,
            source_workspace_id=source_id,
            target_workspace_id=ctx.target_workspace_id,
            items=databases,
            item_type=analytics.MIRRORED_DATABASE,
            id_map=ctx.id_map,
            folder_map=ctx.id_map,
            source_items=ctx.source_items,
            on_progress=progress,
        )
        warnings.extend(item_warnings)

    for result in migrated:
        target = ctx.client.get(
            f"workspaces/{ctx.target_workspace_id}/mirroredDatabases/{result.target_id}"
        )
        source = next((db for db in databases if db["id"] == result.source_id), {})
        source_endpoint = data_stores.mirrored_database_sql_endpoint(source)
        target_endpoint = data_stores.mirrored_database_sql_endpoint(target)
        for key in ("connectionString", "id"):
            if source_endpoint.get(key) and target_endpoint.get(key):
                ctx.id_map[source_endpoint[key]] = target_endpoint[key]

        was = running.get(result.source_id)
        state = f"was {was} in the source workspace" if was else "could not be read"
        warnings.append(
            f"Mirrored database '{result.name}' was created but its mirroring is not started "
            f"(replication {state}). Start it from the new workspace once you are ready for a "
            "second mirror to read the source database."
        )
        # Nothing has replicated into it yet, so anything reading its tables has nothing to
        # read. A shortcut into it fails, and the reason is three steps back from the failure.
        ctx.dormant[result.source_id] = (
            "arrives with its mirroring not started, so no data has replicated into it yet. "
            "Start mirroring in the new workspace."
        )

    if catalogs:
        results, item_warnings = analytics.migrate_items(
            ctx.client,
            source_workspace_id=source_id,
            target_workspace_id=ctx.target_workspace_id,
            items=catalogs,
            item_type=analytics.MIRRORED_ADB_CATALOG,
            id_map=ctx.id_map,
            folder_map=ctx.id_map,
            source_items=ctx.source_items,
            on_progress=progress,
        )
        migrated.extend(results)
        warnings.extend(item_warnings)
        for result in results:
            ctx.dormant[result.source_id] = (
                "arrives with its automatic sync disabled, so no data has synced into it yet. "
                "Enable sync in the new workspace."
            )

    if snowflake:
        warnings.extend(_migrate_snowflake_databases(ctx, step, snowflake, progress))

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step, StepStatus.SUCCEEDED, f"Migrated {len(migrated) + len(snowflake)} item(s)", warnings
    )


def _migrate_snowflake_databases(
    ctx: _Context,
    step: str,
    items: list[dict[str, Any]],
    progress: Any,
) -> list[str]:
    """Recreate Snowflake database items against the same Snowflake database.

    These are not migrated through their definition. The definition article marks its two
    fields as having to be empty on create, which only makes sense when creating *with* a
    definition; the creation payload takes both directly. The data stays in Snowflake and the
    connection is tenant scoped, so nothing else has to move.
    """
    warnings: list[str] = []

    for item in items:
        name = item["displayName"]
        progress(f"Migrating SnowflakeDatabase '{name}'")

        parts: list[dict[str, Any]] = []
        try:
            definition = items_module.get_item_definition(
                ctx.client, ctx.plan.source_workspace_id, item["id"]
            )
            parts = list(definition.get("parts") or [])
        except FabricError:
            # The item properties alone are usually enough; the definition is a fallback.
            logger.info("Could not export the definition of Snowflake database '%s'", name)

        payload = special_items.snowflake_creation_payload(item, parts)
        if not payload:
            warnings.append(
                f"SnowflakeDatabase '{name}' was not migrated because the database name and "
                "connection it uses could not be read. Recreate it by hand."
            )
            continue

        try:
            created = items_module.create_item(
                ctx.client,
                ctx.target_workspace_id,
                name,
                analytics.SNOWFLAKE_DATABASE,
                description=item.get("description") or None,
                creation_payload=payload,
                folder_id=ctx.id_map.get(item.get("folderId", "")),
            )
        except FabricError as error:
            warnings.append(analytics.describe_failure(analytics.SNOWFLAKE_DATABASE, name, error))
            continue

        ctx.id_map[item["id"]] = created["id"]

    return warnings


# --------------------------------------------------------------------- phase 6


def _migrate_shortcuts_and_endpoints(ctx: _Context) -> None:
    step = "shortcuts"
    ctx.run.start_step(step, "Recreating shortcuts and syncing SQL endpoints")
    ctx.run.raise_if_cancelled()

    source_lakehouses = data_stores.list_lakehouses(ctx.client, ctx.plan.source_workspace_id)
    if not source_lakehouses and not ctx.kql_databases:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "Nothing references other items")
        return

    warnings: list[str] = []
    shortcuts_created = 0
    endpoints: list[tuple[str, str, str]] = []

    # Names for the items in the source workspace, so a shortcut pointing at something that
    # did not migrate can say which item that was rather than quoting a GUID.
    source_items = {
        item["id"]: item
        for item in list_items(ctx.client, ctx.plan.source_workspace_id)
        if item.get("id")
    }

    # KQL table shortcuts can target lakehouses, warehouses, or other KQL databases, so they
    # are only safe to create now that every one of those exists.
    for source_db_id, target_db_id, database_name in ctx.kql_databases:
        ctx.run.raise_if_cancelled()
        table_shortcuts = ctx.kql_table_shortcuts.get(source_db_id) or []
        if not table_shortcuts:
            continue

        ctx.run.update_step(step, f"Recreating table shortcuts for KQL database '{database_name}'")
        created, shortcut_warnings = shortcuts.copy_table_shortcuts(
            ctx.client,
            ctx.plan.source_workspace_id,
            source_db_id,
            ctx.target_workspace_id,
            target_db_id,
            ctx.id_map,
            shortcuts=table_shortcuts,
            source_items=source_items,
            dormant=ctx.dormant,
        )
        shortcuts_created += created
        warnings.extend(f"KQL database '{database_name}': {w}" for w in shortcut_warnings)

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
            source_items=source_items,
            dormant=ctx.dormant,
        )
        shortcuts_created += created
        warnings.extend(f"Lakehouse '{name}': {w}" for w in shortcut_warnings)

        # The endpoint must re-read OneLake after tables and shortcuts land, otherwise the
        # schema copy below sees an empty database.
        ctx.run.update_step(step, f"Refreshing SQL endpoint for '{name}'")
        target = data_stores.get_lakehouse(ctx.client, ctx.target_workspace_id, target_id)
        endpoint = data_stores.lakehouse_sql_endpoint(target)
        # By now the endpoint exists, which it very often did not when the lakehouse was
        # created. This is the point at which its id and connection string can be recorded,
        # and everything that binds to one is migrated after this phase.
        _map_sql_endpoint(ctx, data_stores.lakehouse_sql_endpoint(lakehouse), endpoint)
        if endpoint.get("id"):
            refreshed = data_stores.refresh_sql_endpoint_metadata(
                ctx.client,
                ctx.target_workspace_id,
                endpoint["id"],
                on_progress=_bulk_copy_progress(ctx, step, f"Lakehouse '{name}'"),
            )
            # The refresh reports success while individual tables failed to sync, and a table
            # that did not sync is invisible to the schema deploy that follows.
            warnings.extend(
                f"Lakehouse '{name}' SQL endpoint: {failure}"
                for failure in data_stores.sync_failures(refreshed)
            )

        source_endpoint = data_stores.lakehouse_sql_endpoint(lakehouse).get("connectionString")
        target_endpoint = endpoint.get("connectionString")
        if source_endpoint and target_endpoint:
            endpoints.append((name, source_endpoint, target_endpoint))

    # Every endpoint has been refreshed by now, so the schema copies can run together. They
    # are the slow part of this phase: a refreshed endpoint takes minutes to answer, and it
    # takes them whether or not we are also waiting on another one.
    warnings.extend(_transfer_endpoint_schemas(ctx, step, endpoints))

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step, StepStatus.SUCCEEDED, f"Created {shortcuts_created} shortcut(s)", warnings
    )


def _transfer_endpoint_schemas(
    ctx: _Context,
    step: str,
    endpoints: list[tuple[str, str, str]],
) -> list[str]:
    def transfer(name: str, source_endpoint: str, target_endpoint: str) -> Callable[[], list[str]]:
        def run() -> list[str]:
            ctx.run.raise_if_cancelled()
            try:
                warnings = sqlschema.transfer_schema(
                    source_server=source_endpoint,
                    target_server=target_endpoint,
                    database=name,
                    principal=ctx.principal,
                    tokens=ctx.tokens,
                    scratch_dir=ctx.scratch_dir / "sql",
                    source_type="Lakehouse",
                )
            except sqlschema.SchemaTransferError as error:
                return [f"SQL endpoint schema for '{name}' did not transfer: {error}"]
            return [f"Lakehouse '{name}' SQL endpoint: {w}" for w in warnings]

        return run

    return concurrency.run_bounded(
        [concurrency.Job(key=name, run=transfer(name, source, target)) for name, source, target in endpoints],
        limit=SETTINGS.schema_transfer_concurrency,
        on_progress=lambda message: ctx.run.update_step(step, f"Transferring endpoint schema: {message}"),
        noun="schema transfer",
    )


# --------------------------------------------------------------------- phase 7


def _migrate_realtime(ctx: _Context) -> None:
    """Recreate eventstreams, KQL querysets, and KQL dashboards.

    All three read from the real-time items built earlier: a queryset and a dashboard target
    an eventhouse cluster URI, and an eventstream routes into lakehouses, eventhouses, and
    other items while sourcing from connections. So this runs after the data stores, the
    eventhouses, and the connection replacements.
    """
    step = "realtime"
    ctx.run.start_step(step, "Migrating eventstreams, querysets, and dashboards")
    ctx.run.raise_if_cancelled()

    source_id = ctx.plan.source_workspace_id
    groups = [
        (analytics.EVENTSTREAM, analytics.list_of_type(ctx.client, source_id, analytics.EVENTSTREAM)),
        (analytics.KQL_QUERYSET, analytics.list_of_type(ctx.client, source_id, analytics.KQL_QUERYSET)),
        (analytics.KQL_DASHBOARD, analytics.list_of_type(ctx.client, source_id, analytics.KQL_DASHBOARD)),
    ]
    if not any(items for _, items in groups):
        ctx.run.finish_step(step, StepStatus.SKIPPED, "Nothing to migrate in this phase")
        return

    warnings: list[str] = []
    counts: dict[str, int] = {}
    migrated: list[analytics.MigratedItem] = []

    def progress(message: str) -> None:
        ctx.run.update_step(step, message)

    for item_type, items in groups:
        if not items:
            continue
        results, item_warnings = analytics.migrate_items(
            ctx.client,
            source_workspace_id=source_id,
            target_workspace_id=ctx.target_workspace_id,
            items=items,
            item_type=item_type,
            id_map=ctx.id_map,
            folder_map=ctx.id_map,
            source_items=ctx.source_items,
            on_progress=progress,
        )
        counts[item_type] = len(results)
        migrated.extend(results)
        warnings.extend(item_warnings)

    # An eventstream sources from connections, which are checked the same way pipelines are.
    warnings.extend(_check_connections(ctx, step, migrated))

    ctx.warnings.extend(warnings)
    summary = ", ".join(f"{count} {name}" for name, count in counts.items()) or "nothing"
    ctx.run.finish_step(step, StepStatus.SUCCEEDED, f"Migrated {summary}", warnings)


# --------------------------------------------------------------------- phase 9


def _migrate_engineering(ctx: _Context) -> None:
    """Recreate environments, notebooks, and dataflows.

    Ordered environments first, because a notebook attaches to one; then notebooks and
    dataflows, which read the lakehouses and warehouses built earlier. All of them run before
    semantic models, since a model can source from a dataflow.
    """
    step = "engineering"
    ctx.run.start_step(step, "Migrating environments, notebooks, and dataflows")
    ctx.run.raise_if_cancelled()

    source_id = ctx.plan.source_workspace_id
    environments = analytics.list_of_type(ctx.client, source_id, analytics.ENVIRONMENT)
    notebooks = analytics.list_of_type(ctx.client, source_id, analytics.NOTEBOOK)
    dataflows = analytics.list_of_type(ctx.client, source_id, analytics.DATAFLOW)
    # Ordered by what reads what. A Spark job definition pins an environment and a lakehouse,
    # a GraphQuerySet names its GraphModel, and a Map reads lakehouses and KQL databases.
    later_types = [
        analytics.SPARK_JOB_DEFINITION,
        analytics.GRAPHQL_API,
        analytics.GRAPH_MODEL,
        analytics.GRAPH_QUERY_SET,
        analytics.MAP,
        analytics.VARIABLE_LIBRARY,
        analytics.MOUNTED_DATA_FACTORY,
    ]
    later = [
        (item_type, analytics.list_of_type(ctx.client, source_id, item_type))
        for item_type in later_types
    ]

    if not environments and not notebooks and not dataflows and not any(i for _, i in later):
        ctx.run.finish_step(step, StepStatus.SKIPPED, "Nothing to migrate in this phase")
        return

    warnings: list[str] = []
    counts: dict[str, int] = {}

    def progress(message: str) -> None:
        ctx.run.update_step(step, message)

    for item_type, items in (
        (analytics.ENVIRONMENT, environments),
        (analytics.NOTEBOOK, notebooks),
    ):
        if not items:
            continue
        results, item_warnings = analytics.migrate_items(
            ctx.client,
            source_workspace_id=source_id,
            target_workspace_id=ctx.target_workspace_id,
            items=items,
            item_type=item_type,
            id_map=ctx.id_map,
            folder_map=ctx.id_map,
            source_items=ctx.source_items,
            on_progress=progress,
        )
        counts[item_type] = len(results)
        warnings.extend(item_warnings)

        if item_type == analytics.ENVIRONMENT:
            # The rewriter has already repointed pool ids that were recreated, so only a pool
            # that did not transfer is worth warning about.
            recreated_pools = set(ctx.id_map.values())
            for result in results:
                warnings.extend(
                    analytics.environment_warnings(
                        result.name, result.parts, known_pool_ids=recreated_pools
                    )
                )
            if results:
                warnings.append(
                    f"{len(results)} environment(s) were created but not published. Publish "
                    "them in the new workspace before running anything that depends on them."
                )

    if dataflows:
        counts[analytics.DATAFLOW] = _migrate_dataflows(ctx, step, dataflows, warnings, progress)

    # Everything above is either depended on by these or independent of them, so they go last.
    for item_type, items in later:
        if not items:
            continue
        results, item_warnings = analytics.migrate_items(
            ctx.client,
            source_workspace_id=source_id,
            target_workspace_id=ctx.target_workspace_id,
            items=items,
            item_type=item_type,
            id_map=ctx.id_map,
            folder_map=ctx.id_map,
            source_items=ctx.source_items,
            on_progress=progress,
        )
        counts[item_type] = len(results)
        warnings.extend(item_warnings)

        if item_type == analytics.GRAPH_MODEL and results:
            warnings.append(
                f"{len(results)} graph model(s) were created with their mappings intact, but "
                "the graph index itself is built from the data rather than copied. Refresh "
                "them in the new workspace before running queries."
            )

    # The workspace default environment is referenced by name, so it can only be set once the
    # environment it names exists here.
    if ctx.spark_settings:
        patch = spark.default_environment_patch(ctx.spark_settings)
        if patch:
            progress("Setting the workspace default environment")
            warnings.extend(_apply_spark_patches(ctx, [("the default Spark environment", patch)]))

    ctx.warnings.extend(warnings)
    summary = ", ".join(f"{count} {name}" for name, count in counts.items()) or "nothing"
    ctx.run.finish_step(step, StepStatus.SUCCEEDED, f"Migrated {summary}", warnings)


def _migrate_dataflows(
    ctx: _Context,
    step: str,
    dataflows: list[dict[str, Any]],
    warnings: list[str],
    progress: Any,
) -> int:
    """Migrate the dataflows that can move, and explain the ones that cannot.

    Only Dataflow Gen2 (CI/CD) items work with the definition APIs, so each one is classified
    by probing its definition rather than trusting the item listing, which Fabric documents
    as unreliable for this type.
    """
    movable: list[dict[str, Any]] = []
    parts_by_id: dict[str, list[dict[str, Any]]] = {}

    for dataflow in dataflows:
        ctx.run.raise_if_cancelled()
        progress(f"Checking dataflow '{dataflow.get('displayName')}'")
        parts, reason = analytics.classify_dataflow(ctx.client, ctx.plan.source_workspace_id, dataflow)
        if reason:
            warnings.append(reason)
            continue
        movable.append(dataflow)
        parts_by_id[dataflow["id"]] = parts or []

    if not movable:
        return 0

    results, item_warnings = analytics.migrate_items(
        ctx.client,
        source_workspace_id=ctx.plan.source_workspace_id,
        target_workspace_id=ctx.target_workspace_id,
        items=movable,
        item_type=analytics.DATAFLOW,
        id_map=ctx.id_map,
        folder_map=ctx.id_map,
        source_items=ctx.source_items,
        parts_by_id=parts_by_id,
        on_progress=progress,
    )
    warnings.extend(item_warnings)
    return len(results)


# --------------------------------------------------------------------- phase 8


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

    if not models and not reports:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "No semantic models or reports to migrate")
        return

    warnings: list[str] = []

    def progress(message: str) -> None:
        ctx.run.update_step(step, message)

    # A composite semantic model can read another semantic model, so they cannot be created
    # in arbitrary order. The relations graph gives the real order; without it, source order.
    ordered_ids = relations.topological_order([model["id"] for model in models], ctx.graph)
    by_id = {model["id"]: model for model in models}
    models = [by_id[model_id] for model_id in ordered_ids if model_id in by_id]

    migrated_models, model_warnings = analytics.migrate_items(
        ctx.client,
        source_workspace_id=source_id,
        target_workspace_id=ctx.target_workspace_id,
        items=models,
        item_type=analytics.SEMANTIC_MODEL,
        id_map=ctx.id_map,
        folder_map=ctx.id_map,
        source_items=ctx.source_items,
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
        source_items=ctx.source_items,
        on_progress=progress,
    )
    warnings.extend(report_warnings)

    unbound = [
        message
        for item in migrated_reports
        if item.rebound_parts == 0
        and (message := analytics.report_binding_warning(item.name, item.parts, ctx.id_map))
    ]
    warnings.extend(unbound)

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step,
        StepStatus.SUCCEEDED,
        f"Migrated {len(migrated_models)} semantic model(s) and {len(migrated_reports)} report(s)",
        warnings,
    )


# --------------------------------------------------------------------- phase 7


def _migrate_orchestration(ctx: _Context) -> None:

    """Recreate data pipelines, Copy Jobs, and Apache Airflow jobs.

    These run last of the content phases because they orchestrate everything else: a pipeline
    can read a lakehouse, refresh a semantic model, or invoke another pipeline, so every one
    of those has to exist and be in the id map first.

    Connections are deliberately not recreated. They are tenant scoped, so the same
    connection id resolves from the new workspace, and the API never returns credentials so a
    faithful copy is impossible anyway. Instead the ones each item binds are checked.
    """
    step = "orchestration"
    ctx.run.start_step(step, "Migrating data pipelines, Copy Jobs, and Airflow jobs")
    ctx.run.raise_if_cancelled()

    source_id = ctx.plan.source_workspace_id
    pipelines = analytics.list_of_type(ctx.client, source_id, analytics.DATA_PIPELINE)
    jobs = analytics.list_of_type(ctx.client, source_id, analytics.COPY_JOB)
    airflow_jobs = analytics.list_of_type(ctx.client, source_id, airflow.APACHE_AIRFLOW_JOB)

    if not pipelines and not jobs and not airflow_jobs:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "Nothing to orchestrate in this workspace")
        return

    warnings: list[str] = []

    def progress(message: str) -> None:
        ctx.run.update_step(step, message)

    migrated: list[analytics.MigratedItem] = []
    for item_type, items in ((analytics.DATA_PIPELINE, pipelines), (analytics.COPY_JOB, jobs)):
        if not items:
            continue
        # A pipeline can invoke another pipeline, so order them by their real dependencies.
        by_id = {item["id"]: item for item in items}
        ordered_ids = relations.topological_order(list(by_id), ctx.graph)
        ordered = [by_id[item_id] for item_id in ordered_ids if item_id in by_id]

        results, item_warnings = analytics.migrate_items(
            ctx.client,
            source_workspace_id=source_id,
            target_workspace_id=ctx.target_workspace_id,
            items=ordered,
            item_type=item_type,
            id_map=ctx.id_map,
            folder_map=ctx.id_map,
            source_items=ctx.source_items,
            on_progress=progress,
        )
        migrated.extend(results)
        warnings.extend(item_warnings)

    airflow_migrated = 0
    if airflow_jobs:
        airflow_migrated, airflow_warnings = _migrate_airflow_jobs(ctx, step, airflow_jobs, progress)
        warnings.extend(airflow_warnings)

    warnings.extend(_check_connections(ctx, step, migrated))

    counts = ", ".join(
        part
        for part in (
            f"{len(pipelines)} data pipeline(s)" if pipelines else "",
            f"{len(jobs)} Copy Job(s)" if jobs else "",
            f"{airflow_migrated} Apache Airflow job(s)" if airflow_jobs else "",
        )
        if part
    )
    ctx.warnings.extend(warnings)
    ctx.run.finish_step(step, StepStatus.SUCCEEDED, f"Migrated {counts}", warnings)


def _migrate_airflow_jobs(
    ctx: _Context,
    step: str,
    jobs: list[dict[str, Any]],
    progress: Any,
) -> tuple[int, list[str]]:
    """Recreate Apache Airflow jobs, configuration and DAG files both.

    The definition holds configuration only, so a job created from it alone would look
    migrated and have nothing to run. The files follow immediately, over a separate beta API.
    """
    migrated = 0
    warnings: list[str] = []

    for job in jobs:
        ctx.run.raise_if_cancelled()
        name = job["displayName"]
        progress(f"Migrating Apache Airflow job '{name}'")

        try:
            definition = items_module.get_item_definition(
                ctx.client, ctx.plan.source_workspace_id, job["id"]
            )
            parts = definitions.strip_part(definition.get("parts") or [], airflow.PLATFORM_PART)
            rewritten, _ = definitions.rewrite_parts(parts, ctx.id_map)
            rewritten = airflow.retarget_location(rewritten, ctx.plan.capacity_display_region)

            created = items_module.create_item(
                ctx.client,
                ctx.target_workspace_id,
                name,
                airflow.APACHE_AIRFLOW_JOB,
                description=job.get("description") or None,
                parts=rewritten,
                folder_id=ctx.id_map.get(job.get("folderId", "")),
            )
        except FabricError as error:
            warnings.append(analytics.describe_failure(airflow.APACHE_AIRFLOW_JOB, name, error))
            continue

        ctx.id_map[job["id"]] = created["id"]
        migrated += 1
        warnings.extend(airflow.configuration_warnings(rewritten, name))

        copied, file_warnings = airflow.copy_files(
            ctx.client,
            source_workspace_id=ctx.plan.source_workspace_id,
            source_job_id=job["id"],
            target_workspace_id=ctx.target_workspace_id,
            target_job_id=created["id"],
            job_name=name,
            on_progress=progress,
        )
        warnings.extend(file_warnings)
        if not copied and not file_warnings:
            warnings.append(
                f"Apache Airflow job '{name}' had no files to copy, so the new job has no DAGs."
            )

    return migrated, warnings


# -------------------------------------------------------------------- phase 7b


def _migrate_reflexes(ctx: _Context) -> None:
    """Recreate Reflex (Activator) items, with every rule switched off.

    This is the last content phase. A Reflex reacts to something and then acts on something
    else: it watches an eventstream or a KQL database, and its actions run pipelines and
    notebooks. Everything on both sides therefore has to exist and be in the id map already,
    which puts it after orchestration rather than with the other real-time items.
    """
    step = "reflexes"
    ctx.run.start_step(step, "Migrating Activator items")
    ctx.run.raise_if_cancelled()

    source_id = ctx.plan.source_workspace_id
    reflexes = analytics.list_of_type(ctx.client, source_id, analytics.REFLEX)
    if not reflexes:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "No Activator items to migrate")
        return

    migrated, warnings = analytics.migrate_items(
        ctx.client,
        source_workspace_id=source_id,
        target_workspace_id=ctx.target_workspace_id,
        items=reflexes,
        item_type=analytics.REFLEX,
        id_map=ctx.id_map,
        folder_map=ctx.id_map,
        source_items=ctx.source_items,
        on_progress=lambda message: ctx.run.update_step(step, message),
    )
    warnings.extend(_check_connections(ctx, step, migrated))

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step, StepStatus.SUCCEEDED, f"Migrated {len(migrated)} Activator item(s)", warnings
    )


def _check_connections(
    ctx: _Context,
    step: str,
    migrated: list[analytics.MigratedItem],
) -> list[str]:
    """Report bound connections that will not work from the new workspace."""
    bound = [(item.name, connections.referenced_connection_ids(item.parts)) for item in migrated]
    if not any(ids for _, ids in bound):
        return []

    ctx.run.update_step(step, "Checking bound connections")
    known = connections.connections_by_id(ctx.client)
    if not known:
        return [
            "Could not read the tenant's connections, so the ones bound by pipelines and Copy "
            "Jobs were not checked. Grant the service principal Connection.Read.All."
        ]

    issues: list[connections.ConnectionIssue] = []
    for name, connection_ids in bound:
        issues.extend(
            connections.check(
                f"'{name}'",
                connection_ids,
                known,
                target_region=ctx.plan.capacity_region,
            )
        )

    if issues:
        ctx.run.summary["connectionIssues"] = [issue.as_dict() for issue in issues]
    return [issue.message() for issue in issues]


# --------------------------------------------------------------------- phase 8


def _copy_permissions(ctx: _Context) -> None:
    step = "permissions"
    if not ctx.plan.copy_permissions:
        ctx.run.add_step(step, "Copying workspace permissions")
        ctx.run.finish_step(step, StepStatus.SKIPPED, "Disabled for this run")
        return

    ctx.run.start_step(step, "Copying workspace permissions")
    ctx.run.raise_if_cancelled()

    # Admins were granted when the workspace was created; this pass adds everyone else.
    assignments = ctx.source_role_assignments or workspaces.list_role_assignments(
        ctx.client, ctx.plan.source_workspace_id
    )
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

    # Capacity size caps Spark pools, starter pool sizing, and semantic model memory, so a
    # mismatch is worth knowing about before anything is created.
    capacity_warning = workspaces.compare_capacities(
        workspaces.workspace_capacity_sku(client, workspace), capacity.get("sku") or ""
    )

    if strategy is None:
        strategy = assess_workspace(list_items(client, source_workspace_id)).strategy

    return MigrationPlan(
        capacity_id=capacity_id,
        capacity_name=capacity.get("displayName", capacity_id),
        capacity_region=region,
        capacity_display_region=capacity.get("region") or capacity.get("location") or "",
        capacity_sku=capacity.get("sku") or "",
        source_workspace_id=source_workspace_id,
        source_workspace_name=source_name,
        # Reassignment moves the workspace itself, so its name never changes.
        target_workspace_name=(
            source_name
            if strategy is Strategy.REASSIGN
            else (target_workspace_name or default_target_name(source_name, region))
        ),
        strategy=strategy,
        capacity_warning=capacity_warning,
        include_files=include_files,
        include_data=include_data,
        copy_permissions=copy_permissions,
    )


__all__ = [
    "DependencyReport",
    "MigrationPlan",
    "build_plan",
    "cleanup_run",
    "connection_prerequisites",
    "default_target_name",
    "dependency_warnings",
    "run_migration",
]
