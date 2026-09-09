"""The migration orchestrator.

This is the Python replacement for ``fab-shuffle.ps1``. It drives the whole region move.

Phase order is load bearing, and matches v1's. Each phase records the source-to-target ids
it created in ``_Context.id_map`` (v1's ``$replacements`` hash table), and later phases
rewrite their exported definitions through that map. A phase can therefore only reference
items created by an *earlier* phase:

0. ``assessment`` and ``dependencies`` run before anything is created, so a workspace that
   cannot migrate cleanly can be abandoned before it is half built. ``dependencies`` also
   detects which tenant connections are actually referenced by the items and shortcuts being
   migrated - an explicit ``connection_mappings`` entry, an item definition that binds one
   directly, or a shortcut - so later phases can recognise them (see
   ``_load_source_references``). It is detection only: nothing is created, adopted by name,
   or deleted, and a supplied mapping is not yet checked, because the data stores its path
   might reference do not exist yet.
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
6. ``connections``  only runs when the operator supplied ``connection_mappings``; with none
   supplied there is no phase and no rebuild noise about connections it never touched. Each
   supplied mapping is validated and rewritten through the id map now that every phase that
   can add to it has run (see ``_validate_connection_mappings``), so a connection whose source
   path names a lakehouse or warehouse endpoint is not misdiagnosed as broken before that
   endpoint existed. Nothing is created, adopted by name, or deleted here either: a connection
   id is tenant scoped, so it resolves unchanged from the migrated workspace, and the API
   never returns an existing connection's credentials, so a faithful copy is never possible
   anyway.
7. ``mirrored``     mirrored databases, which are data stores with their own SQL analytics
   endpoint and can themselves bind a connection directly, so they belong with the others and
   after connection mappings are validated, and before anything that reads them. Created
   stopped; an explicit operator opt-in starts them here and observes Running before shortcuts.
8. ``shortcuts``    after *every* data item exists, since a shortcut can point at any of
   them. This covers both lakehouse shortcuts and KQL database table shortcuts. The SQL
   analytics endpoint is refreshed only now, so it picks up both the copied tables and the
   new shortcuts, and only then is its schema copied.
9. ``realtime``     eventstreams, KQL querysets, and KQL dashboards. They read the
   eventhouses and data stores above, and an eventstream sources from connections.
10. ``engineering``  environments, then notebooks, then dataflows, then Spark job definitions,
    GraphQL APIs, graph models and query sets, maps, variable libraries, and mounted data
    factories. A notebook attaches to an environment and reads a lakehouse; a semantic model
    can read a dataflow; a query set names its graph model. All of them come before analytics.
11. ``analytics``   semantic models, then reports. Models bind to lakehouse and warehouse
    SQL endpoints, so they need the data store phases finished; reports bind to models, so
    they run after.
12. ``orchestration`` data pipelines and Copy Jobs, which read, refresh, and invoke anything
    above.
13. ``reflexes``    Activator items, last of the content phases. One watches an eventstream or
    KQL database and acts by running pipelines and notebooks, so both sides must exist first.
14. ``connectionadvisory`` a tenant-wide, read-only scan of every connection this credential
    can see, looking for a path that still names the source workspace, one of its items, or
    one of its data-store endpoints (see ``fabshuffle.fabric.connection_advisory``). Placed
    here because it wants the id map as complete as it will get, to preview what a matched
    connection's path would become if repointed by hand, but still before the workspace-level
    ``permissions``/``cleanup`` steps that do not touch it either way. It is advisory only: it
    never creates, adopts, deletes, or grants anything, and never gates item migration or
    changes ``id_map``. A failure before this phase leaves the scan unknown (or a previous
    attempt's snapshot stale); cancellation never starts another network scan.
15. ``permissions`` remaining role assignments. Admins were granted back in step 1.
16. ``cleanup``     drop the scratch workspace and local staging.

A dependent item that still binds an unresolved source-bound connection - none supplied, or a
supplied mapping that failed validation - is refused, with a warning naming the connection and
the item, rather than silently created against a connection that will not work once the source
workspace is gone.

A ``REASSIGN`` strategy moves the existing workspace onto the new capacity in place, so none of
the phases above run, and neither does the connection advisory scan: the workspace's id and
every connection's path are unchanged by reassignment, so there is nothing to repoint and
nothing worth scanning for.
"""

from __future__ import annotations

import logging
import re
import shutil
from collections.abc import Callable, Iterable, Mapping
from contextlib import ExitStack
from dataclasses import asdict, dataclass, field, fields
from functools import partial
from pathlib import Path
from typing import Any
from uuid import UUID

from fabshuffle import concurrency
from fabshuffle import journal as journal_module
from fabshuffle.auth import AuthError, ServicePrincipal, TokenProvider
from fabshuffle.config import SETTINGS
from fabshuffle.fabric import (
    airflow,
    analytics,
    connection_advisory,
    connections,
    copyjobs,
    cosmosdb,
    data_stores,
    definitions,
    eventhouses,
    migration_refs,
    mirroring,
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
    STOPPED_EVENTSTREAM_REASON,
    Strategy,
    WorkspaceAssessment,
    assess_workspace,
    supports_large_semantic_models,
)
from fabshuffle.lifecycle import (
    CopyOutcome,
    Disposition,
    EvidenceState,
    ItemLifecycle,
    Lifecycle,
    contract_for,
)
from fabshuffle.run import CancelledError, MigrationRun, RunStatus, StepStatus
from fabshuffle.transfer import bulkcopy, kql, sqlschema
from fabshuffle.transfer import cosmos as cosmos_transfer
from fabshuffle.transfer import files as file_transfer
from fabshuffle.transfer.common import TransferCancelled

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
    access: list[ConnectionAccess] = field(default_factory=list)

    def messages(self) -> list[str]:
        """Warnings for the run log.

        Connection access is deliberately not repeated here. It has its own section on the
        review screen, and listing it twice made the same nine lines appear under two
        headings.
        """
        return [issue.message() for issue in self.issues]


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
    # The source workspace reports a finished capacity assignment but names no capacity - an
    # observed shape, not a documented guarantee, that a deleted or expired-trial capacity can
    # produce (see workspaces.unresolved_capacity_assignment). Recorded at plan time so the
    # reassign path can offer it as a possible explanation if the assignment then fails.
    source_capacity_id_missing: bool = False
    include_files: bool = True
    include_data: bool = True
    copy_permissions: bool = True
    source_tenant_id: str = ""
    target_tenant_id: str = ""
    source_client_id: str = ""
    target_client_id: str = ""
    write_freeze_confirmed: bool = False
    start_database_mirrors: bool = False
    connection_mappings: dict[str, str] = field(default_factory=dict)
    reference_mappings: list[dict[str, str]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if type(self.start_database_mirrors) is not bool:
            raise ValueError("Starting database mirrors requires an explicit boolean option.")
        if bool(self.source_tenant_id) != bool(self.target_tenant_id):
            raise ValueError("A tenant-qualified plan requires both source and target tenant IDs.")
        if self.source_tenant_id:
            self.source_tenant_id = str(UUID(self.source_tenant_id))
            self.target_tenant_id = str(UUID(self.target_tenant_id))
        if self.paired:
            self.copy_permissions = False
        normalized_connections: dict[str, str] = {}
        for source, target in self.connection_mappings.items():
            if (
                not isinstance(source, str) or not isinstance(target, str)
                or not source.strip() or not target.strip()
            ):
                raise ValueError(
                    "Connection mappings require nonempty source and destination connection IDs."
                )
            source, target = source.strip().casefold(), target.strip().casefold()
            if source in normalized_connections and normalized_connections[source] != target:
                raise ValueError(f"Connection '{source}' has conflicting destination mappings.")
            normalized_connections[source] = target
        self.connection_mappings = normalized_connections

    @property
    def paired(self) -> bool:
        return bool(self.source_tenant_id)

    @property
    def cross_tenant(self) -> bool:
        return bool(self.source_tenant_id) and self.source_tenant_id != self.target_tenant_id

    @property
    def execution_blocker(self) -> str | None:
        if self.start_database_mirrors and self.strategy is not Strategy.REBUILD:
            return "Starting destination database mirrors is only available for rebuild migrations."
        if self.paired and (not self.source_client_id or not self.target_client_id):
            return "Sign in to both tenants again; this plan is missing its application identities."
        if self.paired and self.strategy is not Strategy.REBUILD:
            return (
                "Separate source and destination credentials recreate the workspace; "
                "they never reassign it."
            )
        if (
            self.paired and (self.include_data or self.include_files)
            and not self.write_freeze_confirmed
        ):
            return (
                "Stop writes to the source and confirm the write freeze before copying data or "
                "files with separate credentials. Keep the freeze in place through final reconciliation."
            )
        return None


@dataclass
class _Context:
    client: FabricClient
    tokens: TokenProvider
    principal: ServicePrincipal
    plan: MigrationPlan
    run: MigrationRun
    scratch_dir: Path
    # Where this run is writing down what it did, so it can be picked up again. Defaults to
    # recording nothing, which is what a preview or a test wants.
    journal: journal_module.Journal = field(default=journal_module.DISCARD)
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
    # Do not duplicate an inherited connection merely because access to it was lost.
    unverified_connections: set[str] = field(default_factory=set)
    # What an earlier attempt at this run already did, when this is a resume. ``None`` on a
    # first attempt, which is what makes every "have we done this already" check answer no.
    prior: journal_module.Replay | None = None
    adopted_targets: dict[str, str] = field(default_factory=dict)
    refresh_needed: set[str] = field(default_factory=set)
    active_copy_jobs: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    target_client: FabricClient | None = None
    target_tokens: TokenProvider | None = None
    target_principal: ServicePrincipal | None = None
    primary_item_ids: set[str] = field(default_factory=set)

    @property
    def destination_client(self) -> FabricClient:
        return self.target_client if self.target_client is not None else self.client

    @property
    def destination_tokens(self) -> TokenProvider:
        return self.target_tokens if self.target_tokens is not None else self.tokens

    @property
    def destination_principal(self) -> ServicePrincipal:
        return self.target_principal if self.target_principal is not None else self.principal

    @property
    def target_kwargs(self) -> dict[str, Any]:
        return {"target_client": self.target_client} if self.target_client is not None else {}

    def lifecycle(self, item: Mapping[str, Any], item_type: str = "") -> ItemLifecycle:
        item_type = item_type or str(item.get("type") or "")
        self.source_items.setdefault(str(item["id"]), {**item, "type": item_type})
        result = self.run.lifecycle.item(
            str(item["id"]), str(item.get("displayName") or item["id"]), item_type,
        )
        known = self.source_items.get(str(item["id"]), {})
        if self.plan.paired and (
            item.get("sensitivityLabel") or known.get("sensitivityLabel")
            or item.get("sensitivityLabelId") or known.get("sensitivityLabelId")
        ):
            result.step(
                "protection", EvidenceState.UNKNOWN, "Source sensitivity labels are not migrated.",
                action="Apply a destination sensitivity label before granting access or cutting over.",
            )
        return result

    def evidence(self, source_id: str) -> ItemLifecycle:
        return self.lifecycle(self.source_items.get(source_id) or {
            "id": source_id, "displayName": source_id, "type": "",
        })

    def resolve_item(
        self, item: Mapping[str, Any], target_id: str, item_type: str, *,
        disposition: Disposition | None = None,
    ) -> ItemLifecycle:
        outcome = self.lifecycle(item, item_type)
        if disposition is None:
            disposition = (
                Disposition.ADOPTED if self.already_created(str(item["id"])) else Disposition.CREATED
            )
        self.id_map[item["id"]] = target_id
        self.journal.item(item["id"], target_id, item_type, str(item["displayName"]))
        self.map_item_paths({**item, "type": item_type}, target_id)
        outcome.resolve(target_id, self.target_workspace_id, disposition)
        contract = contract_for(item_type)
        for step, kind, enabled in (
            ("data", contract.data_kind, self.plan.include_data),
            ("files", contract.files_kind, self.plan.include_files),
        ):
            if kind and not enabled and (step == "data" or item_type == "Lakehouse"):
                outcome.step(
                    step, EvidenceState.SKIPPED, f"Skipped by include_{step}=false.",
                    action=f"Copy the {step} separately or start a migration with {step} enabled.",
                )
        return outcome

    def map_item_paths(self, item: Mapping[str, Any], target_id: str) -> None:
        if self.plan.paired:
            for old, new in migration_refs.onelake_aliases(
                self.plan.source_workspace_id, self.plan.source_workspace_name, item,
                self.target_workspace_id, target_id,
            ).items():
                self.map_alias(old, new, str(item["id"]))

    def resolve_or_create(
        self, item: Mapping[str, Any], item_type: str,
        create: Callable[[], Mapping[str, Any]],
    ) -> str:
        evidence = self.lifecycle(item, item_type)
        adopted = self.already_created(str(item["id"]))
        with evidence.operation("target", "Target resolved."):
            target_id = self.id_map[item["id"]] if adopted else str(create()["id"])
            self.resolve_item(
                item, target_id, item_type,
                disposition=Disposition.ADOPTED if adopted else Disposition.CREATED,
            )
        return target_id

    def already_copied(self, item_id: str, kind: str, key: str = "") -> bool:
        """Whether an earlier attempt finished moving this data.

        Copy Jobs overwrite, KQL replaces, Cosmos upserts, azcopy overwrites and bcp now
        clears first, so doing any of it twice is *correct*. It is only slow. This is what
        keeps a resume from spending another six hours being correct.
        """
        if self.prior is None or not self.prior.data_is_done(item_id, kind, key):
            return False
        target = self.id_map.get(item_id)
        recorded_target = self.prior.data_targets.get(
            (item_id, kind, key), self.prior.id_map.get(item_id)
        )
        return bool(target and target == recorded_target)

    def data_copied(
        self, item_id: str, kind: str, key: str = "", *, outcome: CopyOutcome | None = None,
    ) -> None:
        self.journal.data(item_id, kind, key, target_id=self.id_map.get(item_id, ""))
        if not key:
            self.evidence(item_id).copied(outcome or CopyOutcome(kind))

    def map_alias(self, source: str, target: str, owner: str) -> None:
        self.id_map[source] = target
        self.journal.mapping(source, target, owner=owner)

    def invalidate_mapping(self, source: str) -> None:
        refresh = set(self.adopted_targets).intersection(self.source_items)
        self.journal.invalidate([source], refresh=refresh)
        self.run.lifecycle.invalidate([source], target_lost=True)
        self.run.lifecycle.invalidate(refresh - {source})
        self.id_map.pop(source, None)
        self.refresh_needed.update(refresh)

    def _record_mapping(self, source: str, target: str) -> None:
        if self.prior and source in self.prior.id_map and self.prior.id_map[source] != target:
            refresh = set(self.adopted_targets).intersection(self.source_items)
            self.journal.refresh(refresh)
            self.run.lifecycle.invalidate(refresh)
            self.refresh_needed.update(refresh)
        item = self.source_items.get(source)
        if source in self.primary_item_ids and item:
            self.journal.item(
                source, target, str(item.get("type") or ""), str(item.get("displayName") or source),
            )
        else:
            self.journal.mapping(source, target)

    def already_created(self, source_id: str) -> bool:
        """Whether an earlier attempt already built the target item for this source item.

        Read from ``id_map`` after it has been checked against the workspace, so an item that
        was recorded and has since been deleted is not counted. Only ever true on a resume:
        within one attempt each type is walked once, and answering yes here on a first run
        would silently skip something.
        """
        return self.prior is not None and bool(source_id) and source_id in self.id_map

    def to_migrate(self, items: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
        """Items still missing, plus retained consumers whose references need refreshing."""
        return [
            item for item in items
            if not self.already_created(str(item.get("id") or ""))
            or item.get("id") in self.refresh_needed
        ]

    def __post_init__(self) -> None:
        """Point the three collections a resume depends on at the journal.

        Recorded here rather than beside each of the dozens of places that add to them: a
        forgotten call site would not fail, it would produce a resume that quietly rebinds an
        item to something that is no longer there. Anything already present is kept as the
        starting point, which is how a resumed run begins with what the journal replayed.
        """
        if self.prior:
            self.refresh_needed.update(self.prior.refresh_needed)
            self.active_copy_jobs.update(self.prior.copy_jobs)
        self.run.lifecycle = Lifecycle(
            attempt_id=self.run.id, source_workspace=self.plan.source_workspace_id,
            record=self.journal.outcome, changed=self.run.readiness_changed,
            initial=self.prior.outcomes if self.prior else None,
            secrets=(getattr(self.principal, "client_secret", ""),),
        )
        self.run.inventory_complete = bool(self.prior and self.prior.inventory_complete)
        if self.prior:
            self.run.readiness_attempts = [
                {key: attempt.get(key, "") for key in ("run_id", "status", "last_phase")}
                for attempt in [*self.prior.attempts, {
                    "run_id": self.prior.run_id, "status": self.prior.status,
                    "last_phase": self.prior.phases_started[-1] if self.prior.phases_started else "",
                }]
            ]
            # Carried forward so a resume that fails again before the phase reruns still
            # shows the previous attempt's scan, rather than nothing at all. It is still
            # attributed to the earlier attempt id, so a report reading it back can tell it
            # has not been refreshed yet.
            self.run.connection_advisory = self.prior.connection_advisory
        self.id_map = journal_module.RecordingMap(self._record_mapping, self.id_map)
        self.warnings = journal_module.RecordingList(self.journal.warning, self.warnings)
        self.dormant = journal_module.RecordingMap(self.journal.dormant, self.dormant)


def default_target_name(source_name: str, region: str) -> str:
    return f"{source_name}-{region}" if region else f"{source_name}-copy"


def run_migration(
    run: MigrationRun,
    principal: ServicePrincipal,
    plan: MigrationPlan,
    *,
    cleanup: bool = True,
    prior: journal_module.Replay | None = None,
    target_principal: ServicePrincipal | None = None,
) -> None:
    """Execute a migration, recording every phase on ``run``.

    ``prior`` is an earlier attempt at this same move, read back from its journal. Given one,
    the phases all run again but skip what was already done: creation is adopted from the id
    map, and data that finished moving is left alone. Every phase runs because two of them
    only read, and what they read is what the later ones need in order to rebind anything.
    """
    if plan.execution_blocker:
        raise ValueError(plan.execution_blocker)
    if plan.paired != (target_principal is not None):
        raise ValueError(
            "The migration plan and its source/destination credentials must identify the same pair."
        )
    tokens = TokenProvider(principal)
    target_tokens = TokenProvider(target_principal) if target_principal is not None else None
    if plan.paired:
        for label, provider, tenant_id, client_id in (
            ("Source", tokens, plan.source_tenant_id, plan.source_client_id),
            ("Destination", target_tokens, plan.target_tenant_id, plan.target_client_id),
        ):
            if provider is None or (
                provider.tenant_id() != tenant_id
                or provider.principal.client_id.casefold() != client_id.casefold()
            ):
                raise ValueError(f"{label} credentials do not match the tenant and application in this plan.")
    run.plan = _plan_record(plan)
    if prior is not None:
        journal_module.validate_resume_plan(run.plan, prior)
    scratch_dir = SETTINGS.scratch_dir_for(run.id)
    journal_directory = SETTINGS.journal_dir_for_plan(run.plan)
    book = journal_module.Journal(SETTINGS.journal_for_plan(run.id, run.plan))
    journal_module.prune(journal_directory)
    if not run.journal_started:
        book.run_created(_plan_record(plan), cleanup=cleanup, prior=prior)
        run.journal_started = True
        run.lineage_id = (prior.lineage_id or prior.run_id) if prior else run.id
        run.resumed_from = prior.run_id if prior else ""
    run.mark_running()

    with ExitStack() as stack:
        client = stack.enter_context(FabricClient(tokens))
        target_client = (
            stack.enter_context(FabricClient(target_tokens)) if target_tokens is not None else None
        )
        context = _Context(
            client=client,
            tokens=tokens,
            principal=principal,
            plan=plan,
            run=run,
            scratch_dir=scratch_dir,
            journal=book,
            prior=prior,
            warnings=list(prior.warnings) if prior else [],
            target_client=target_client,
            target_tokens=target_tokens,
            target_principal=target_principal,
        )
        try:
            if prior is not None:
                # Inside the try: a resume that has to be refused is a failed run with a
                # reason on it, not an exception escaping into the thread that started it.
                verifying = run.lifecycle.snapshot()
                for source in verifying:
                    context.evidence(source).step(
                        "verification", EvidenceState.UNKNOWN,
                        "Recovery has not verified the recorded target.",
                        action="Restore target workspace access and resume to verify the recorded items.",
                    )
                try:
                    starting_map, notes = verify_prior(client, prior, **context.target_kwargs)
                except Exception as error:
                    for source in verifying:
                        context.evidence(source).step(
                            "verification", EvidenceState.UNKNOWN, "Recorded targets could not be verified.",
                            error=error.__cause__ or error,
                            action="Restore the target workspace or access and retry recovery.",
                        )
                    raise
                for source, outcome in verifying.items():
                    if starting_map.get(source) == outcome.targetId:
                        context.evidence(source).step(
                            "verification", EvidenceState.SUCCEEDED,
                            "Recorded target identity is present in the recovery inventory.",
                        )
                for key, record in list(prior.copy_jobs.items()):
                    job = record["job"]
                    checkpoint = (job["item_id"], "tables", "")
                    if (
                        checkpoint in prior.data_done
                        and prior.data_targets.get(checkpoint) == record["target"]
                        and record["target"] == starting_map.get(job["item_id"])
                    ):
                        book.copy_job(job, target_id=record["target"], active=False)
                        del prior.copy_jobs[key]
                        context.active_copy_jobs.pop(key, None)
                        continue
                    created_only = job.get("submission_state") == "created" and job.get("copy_job_id")
                    if not created_only and (not job.get("copy_job_id") or not job.get("instance_id")):
                        raise ResumeRefused(
                            f"Copy Job '{job.get('label')}' has an unconfirmed submission. "
                            "Check and stop it in the scratch workspace before resolving its "
                            f"journal record; do not start another copy. {job.get('last_error', '')}"
                        )
                    if record["target"] != starting_map.get(job["item_id"]):
                        raise ResumeRefused(
                            f"Copy Job '{job['label']}' may still be running against a replaced "
                            f"target {record['target']}. Stop and reconcile that job before resuming."
                        )
                removed = set(prior.id_map) - set(starting_map)
                retained_items = set(prior.id_map).intersection(starting_map)
                if removed:
                    book.invalidate(removed, refresh=retained_items)
                    run.lifecycle.invalidate(removed, target_lost=True)
                    run.lifecycle.invalidate(retained_items - removed)
                    context.refresh_needed.update(retained_items)
                context.adopted_targets.update(starting_map)
                context.id_map.update(starting_map)
                context.dormant.update(prior.dormant)
                context.warnings.extend(notes)
                context.target_workspace_id = prior.target_workspace_id
                context.scratch_workspace_id = surviving_scratch(
                    context.destination_client, prior.scratch_workspace_id
                )
                if prior.copy_jobs and not context.scratch_workspace_id:
                    raise ResumeRefused(
                        "The scratch workspace for unfinished Copy Jobs cannot be read. Restore "
                        "access and reconcile those jobs before resuming; no replacement was started."
                    )
                if prior.target_workspace_id:
                    book.workspace("target", prior.target_workspace_id)
                if context.scratch_workspace_id:
                    book.workspace("scratch", context.scratch_workspace_id)

            if plan.strategy is Strategy.REASSIGN:
                _reassign_capacity(context)
                run.summary["strategy"] = Strategy.REASSIGN.value
                run.summary["warnings"] = context.warnings
                book.finished(RunStatus.SUCCEEDED.value)
                run.mark_finished(RunStatus.SUCCEEDED)
                return

            for phase, migrate in _REBUILD_PHASES:
                book.phase_started(phase)
                migrate(context)
                book.phase_finished(phase)
            if context.active_copy_jobs:
                raise ResumeRefused(
                    "Unfinished Copy Jobs were not reconciled by the data phases. Restore their "
                    "source items and retry before cleaning up the scratch workspace."
                )
            if cleanup:
                cleanup_run(context.run, context.destination_client, scratch_dir)
            run.summary["strategy"] = Strategy.REBUILD.value
            run.summary["warnings"] = context.warnings
            book.finished(RunStatus.SUCCEEDED.value)
            run.mark_finished(RunStatus.SUCCEEDED)
        except (CancelledError, TransferCancelled) as error:
            book.finished(RunStatus.CANCELLED.value, str(error))
            run.mark_finished(RunStatus.CANCELLED, str(error))
        except Exception as error:
            logger.exception("Migration %s failed", run.id)
            book.finished(RunStatus.FAILED.value, str(error))
            run.mark_finished(RunStatus.FAILED, str(error))


def _plan_record(plan: MigrationPlan) -> dict[str, Any]:
    """The plan as the journal keeps it, so a resume can rebuild it without asking again."""
    record = asdict(plan)
    record["strategy"] = plan.strategy.value
    return record


def plan_from_journal(replay: journal_module.Replay) -> MigrationPlan:
    """Rebuild the plan a run was given. Raises if the journal never recorded one."""
    record = dict(replay.plan)
    if not record.get("source_workspace_id"):
        raise ValueError("This run's journal does not say what it was migrating.")
    record["strategy"] = Strategy(record.get("strategy") or Strategy.REBUILD.value)
    known = {f.name for f in fields(MigrationPlan)}
    # Anything the journal carries that this build does not know about is dropped rather than
    # passed on, so an older build can still resume a newer build's run.
    return MigrationPlan(**{k: v for k, v in record.items() if k in known})


class ResumeRefused(RuntimeError):
    """The run cannot be picked up, and rebuilding into nothing would be worse than saying so."""


def verify_prior(
    client: FabricClient, replay: journal_module.Replay, *, target_client: FabricClient | None = None,
) -> tuple[dict[str, str], list[str]]:
    """Check what the journal claims against the workspaces themselves.

    The journal records what a previous attempt *did*, which is not the same as what is there
    now: somebody may have deleted an item, or the whole workspace, in between. Anything that
    has gone is dropped from the map so that this attempt builds it again.

    Item mappings are checked against the listing. Missing targets invalidate their data
    checkpoints and owned aliases. Unattributed legacy aliases are rebuilt conservatively.
    Pools and connections are reconciled through their own APIs in the resource phases.
    """
    if not replay.target_workspace_id:
        raise ResumeRefused(
            "This run stopped before it created the new workspace, so there is nothing to "
            "pick up. Start it again."
        )

    try:
        target_items = {
            item["id"]
            for item in list_items(
                target_client if target_client is not None else client, replay.target_workspace_id,
            ) if item.get("id")
        }
    except FabricApiError as error:
        if error.status_code in (403, 404):
            raise ResumeRefused(
                f"The workspace this run was building, {replay.target_workspace_id}, cannot be "
                f"read any more: the service said {error}. It was deleted, or this service "
                "principal lost access. Start the migration again rather than resuming into "
                "a workspace that is not there."
            ) from error
        raise

    source_workspace = str(replay.plan.get("source_workspace_id") or "")
    source_items = {
        item["id"] for item in list_items(client, source_workspace) if item.get("id")
    }

    kept: dict[str, str] = {}
    lost: list[str] = []
    for source, target in replay.id_map.items():
        if source in source_items and target not in target_items:
            lost.append(source)
            continue
        kept[source] = target

    if lost:
        # Old journals did not attribute endpoint aliases to their owning item. On a missing
        # target, discard those unverified aliases too; resource phases reload them before use.
        for source in list(kept):
            owner = replay.mapping_owners.get(source)
            if owner in lost or (
                not owner and source not in source_items and source != source_workspace
            ):
                del kept[source]
        replay.data_done = {key for key in replay.data_done if key[0] not in lost}
        replay.data_targets = {
            key: target for key, target in replay.data_targets.items() if key[0] not in lost
        }

    notes = []
    if lost:
        notes.append(
            f"{len(lost)} item(s) recorded by the earlier attempt are no longer in the new "
            "workspace, so they will be created again."
        )
    if replay.damaged_lines:
        notes.append(
            "The end of this run's journal was cut off, which is what a run that was "
            "interrupted looks like. Anything it did not manage to write down will be done "
            "again."
        )
    return kept, notes


def _migrate_definition_items(ctx: _Context, **kwargs: Any) -> tuple[list[analytics.MigratedItem], list[str]]:
    selected = list(kwargs["items"])
    kwargs["items"] = selected
    for item in selected:
        ctx.primary_item_ids.add(item["id"])
        ctx.source_items[item["id"]] = {
            **ctx.source_items.get(item["id"], {}), **item, "type": kwargs["item_type"],
        }
    existing = {
        str(item["id"]): ctx.id_map[str(item["id"])]
        for item in selected if ctx.already_created(str(item["id"]))
    }
    if existing:
        kwargs["existing_targets"] = existing
    kwargs["on_lifecycle"] = ctx.lifecycle
    kwargs.update(ctx.target_kwargs)
    if ctx.plan.paired:
        kwargs["exclude_identity"] = True
    if ctx.plan.cross_tenant:
        kwargs["cross_tenant"] = True
    results, warnings = analytics.migrate_items(ctx.client, **kwargs)
    if ctx.plan.paired:
        selected_by_id = {item["id"]: item for item in selected}
        for result in results:
            ctx.map_item_paths(
                {**selected_by_id[result.source_id], "type": kwargs["item_type"]}, result.target_id,
            )
    completed = {item.source_id for item in results}
    ctx.journal.refresh(completed, required=False)
    ctx.refresh_needed.difference_update(completed)
    failed = set(existing) - completed
    if failed:
        raise ResumeRefused(
            "Existing target items could not be rebound after a dependency changed. "
            "Resume again after resolving these failures: " + "; ".join(warnings)
        )
    return results, warnings


def surviving_scratch(client: FabricClient, scratch_workspace_id: str) -> str:
    """The earlier attempt's scratch workspace, if it is still there.

    A run that finished tidily deleted its own scratch workspace, so retrying the items it
    left behind finds the id recorded but nothing at the end of it. Returning empty makes the
    workspace phase build a new one rather than aim Copy Jobs at a workspace that has gone.
    """
    if not scratch_workspace_id:
        return ""
    try:
        workspaces.get_workspace(client, scratch_workspace_id)
    except FabricApiError as error:
        if error.status_code in (403, 404):
            return ""
        raise
    return scratch_workspace_id


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
        except Exception as error:
            ctx.run.update_step(step, "Assignment failed, restoring large semantic model storage")
            warnings.extend(_restore_large_models(ctx, pbi, converted))
            ctx.warnings.extend(warnings)
            ctx.run.finish_step(step, StepStatus.FAILED, "Capacity assignment failed", warnings)
            if (
                ctx.plan.source_capacity_id_missing
                and isinstance(error, FabricApiError)
                and error.error_code == "AssignWorkspaceToCapacityFailed"
            ):
                # Fabric's own error names the *target* capacity and says only that the
                # assignment failed, which can send the operator looking at the wrong end of
                # the move. The service's own words are kept first, unchanged; what follows is
                # this tool's interpretation of an observed shape, not something the service
                # confirmed, so it is offered as a possibility to check rather than a cause or
                # a fix that is known to work.
                raise RuntimeError(
                    f"{error}\n\n"
                    f"'{ctx.plan.source_workspace_name}' reports a completed capacity "
                    "assignment but names no capacity, which a deleted capacity or an expired "
                    "trial can produce - worth checking on the source workspace's assignment. "
                    "If a capacity is restored in the region the workspace was created in and "
                    "the workspace is assigned to it, retrying this migration may succeed, but "
                    "that recovery has not been confirmed here. If the region's capacity "
                    "cannot be restored, the content may need recreating in a new workspace "
                    "instead."
                ) from error
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

    assessment = assess_workspace(
        list_items(ctx.client, ctx.plan.source_workspace_id),
        force_rebuild=ctx.plan.strategy is Strategy.REBUILD,
        require_stopped=ctx.plan.paired,
    )
    ctx.assessment = assessment
    # Kept for the whole run: a definition that names one of these and does not get it ends
    # up pointing at the new workspace for an item that was never in it.
    ctx.source_items = {item["id"]: item for item in all_items if item.get("id")}
    ctx.primary_item_ids = {item["id"] for item in assessment.migrated}
    for item in ctx.source_items.values():
        evidence = ctx.lifecycle(item)
        if ctx.already_created(item["id"]):
            evidence.resolve(
                ctx.id_map[item["id"]], ctx.target_workspace_id, Disposition.ADOPTED,
            )
        unsupported = next((
            entry for entry in assessment.unsupported
            if entry.name == item.get("displayName") and entry.type == item.get("type")
        ), None)
        if unsupported:
            evidence.step(
                "target", EvidenceState.SKIPPED, "The assessment left this item out of the rebuild.",
                action=unsupported.reason,
            )
        if is_monitoring_item(item):
            evidence.step(
                "target", EvidenceState.SKIPPED, "Workspace monitoring was not migrated.",
                action="Enable workspace monitoring in the target workspace settings if needed.",
            )
    ctx.run.inventory_complete = True
    ctx.journal.inventory()
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


_TENANT_LITERAL = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$")


def _validated_connection_ids(connection_ids: Iterable[str]) -> list[str]:
    """Keep only well-formed connection GUIDs, normalised to their canonical form.

    Refuse malformed recorded identifiers rather than turning a broken ID list into the
    list-everything fallback.
    """
    seen: dict[str, str] = {}
    for raw in connection_ids:
        try:
            normalised = str(UUID(str(raw).strip()))
        except (ValueError, AttributeError, TypeError) as error:
            raise ValueError("A recorded connection ID is not a GUID; inspect the saved journal.") from error
        seen[normalised.casefold()] = normalised
    return sorted(seen.values())


def _validated_tenant_literal(tenant_id: str) -> str:
    """A tenant identifier safe to embed as a script literal: a GUID, or a verified-domain
    style name (letters, digits, dots and hyphens only). Anything else is refused outright
    rather than embedded, since a legacy plan can carry a principal's tenant as either shape.
    """
    candidate = str(tenant_id or "").strip()
    try:
        return str(UUID(candidate))
    except (ValueError, AttributeError, TypeError):
        pass
    if candidate and _TENANT_LITERAL.match(candidate):
        return candidate
    raise ValueError(f"'{tenant_id}' is not a safe tenant identifier for a generated script.")


def connections_lookup_script(connection_ids: Iterable[str], *, tenant_id: str) -> str:
    """A PowerShell script an operator runs, signed in as themself, to look up what each
    connection id the advisory scan found actually is.

    Fab Shuffle's own credential is deliberately not used here. It is frequently the very
    credential the scan just reported cannot see one of these connections, and even where it
    can, the API may not return a display name. The user's account may have different access,
    but names and permissions are not guaranteed. This script only ever reads: no role is
    granted, nothing is created or changed, no token or secret is ever printed, and looking a
    connection up here does not itself grant this account, or anyone, access to it.

    ``connection_ids`` can be empty - an older report, from before this scan existed, has
    nothing recorded to embed. Rather than generate a script that loops over nothing, the
    script itself lists every connection the signed-in account can see when it is handed none,
    so the operator still gets something usable. The same ``-ConnectionId`` parameter also
    lets an operator add ids by hand at run time, against any report.
    """
    ids = _validated_connection_ids(connection_ids)
    tenant = _validated_tenant_literal(tenant_id)
    listed = "\n".join(f"    '{connection_id}'" for connection_id in ids)

    lines = [
        "#Requires -Version 7.0",
        "#Requires -Modules Az.Accounts",
        "# Looks up the display name, connectivity type and definition type for connection",
        "# ids a Fab Shuffle cutover readiness scan found. Run this signed in as yourself,",
        "# not the service principal; it only reads. It does not create, change, or grant",
        "# anything, and looking a connection up here does not give this account - or anyone",
        "# else - any access to it.",
        "",
        "param(",
        "    # Extra connection ids to look up alongside whatever this report",
        "    # recorded. Also what makes this script useful on its own against a report from",
        "    # before this scan existed, which has nothing recorded to embed.",
        "    [ValidatePattern('^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
        "-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')]",
        "    [string[]] $ConnectionId = @()",
        ")",
        "",
        "$ErrorActionPreference = 'Stop'",
        "",
        "$fabric = 'https://api.fabric.microsoft.com'",
        f"$tenantId = '{tenant}'",
        "$recordedConnectionIds = @(",
        listed,
        ")",
        "$suppliedConnectionIds = @($ConnectionId)",
        "$connectionIds = @($recordedConnectionIds + $suppliedConnectionIds | Select-Object -Unique)",
        "",
        "function Get-FabricSecureToken {",
        "    # Az 14 / Az.Accounts 5 return a SecureString already, which -Authentication",
        "    # Bearer wants directly; older versions return a plain string, wrapped here",
        "    # rather than ever being printed, logged, or passed around unprotected.",
        "    if ((Get-Command Get-AzAccessToken).Parameters.ContainsKey('AsSecureString')) {",
        "        return (Get-AzAccessToken -ResourceUrl $fabric -TenantId $tenantId -AsSecureString).Token",
        "    }",
        "    $value = (Get-AzAccessToken -ResourceUrl $fabric -TenantId $tenantId).Token",
        "    if ($value -is [string]) {",
        "        return ConvertTo-SecureString -String $value -AsPlainText -Force",
        "    }",
        "    return $value",
        "}",
        "",
        "function Connect-ForFabric {",
        "    # -TenantId keeps conditional access off the \"organizations\" pseudo-tenant,",
        "    # where it cannot be evaluated. No -ServicePrincipal: this signs in as whoever",
        "    # runs the script. -SkipContextPopulation skips listing Azure subscriptions,",
        "    # which a Fabric-only lookup does not need and which an account with none - or",
        "    # many - would otherwise pay for on every sign-in.",
        "    Connect-AzAccount -TenantId $tenantId -SkipContextPopulation | Out-Null",
        "}",
        "",
        "$context = Get-AzContext",
        "if (-not $context -or $context.Account.Type -ne 'User' -or "
        "$context.Tenant.Id -ne $tenantId) { Connect-ForFabric }",
        "if ((Get-AzContext).Account.Type -ne 'User') {",
        "    throw 'Sign in with a user account, not a service principal or managed identity.'",
        "}",
        "",
        "try {",
        "    $secureToken = Get-FabricSecureToken",
        "} catch {",
        "    # An existing sign-in from somewhere else looks fine to Get-AzContext and is",
        "    # still refused for Fabric, so the only way to find out is to ask.",
        "    Write-Host 'Signing in again, scoped to Fabric.' -ForegroundColor Yellow",
        "    Connect-ForFabric",
        "    $secureToken = Get-FabricSecureToken",
        "}",
        "",
        "function Get-FabricServiceError($ErrorRecord) {",
        "    # The service's own code and message say far more than a bare HTTP status; a",
        "    # failed lookup is reported with these, never left as a blank name.",
        "    $parsed = $null",
        "    if ($ErrorRecord.ErrorDetails.Message) {",
        "        try { $parsed = $ErrorRecord.ErrorDetails.Message | ConvertFrom-Json } catch {}",
        "    }",
        "    $code = if ($parsed.errorCode) { $parsed.errorCode } else { '' }",
        "    $message = if ($parsed.message) { $parsed.message } else { $ErrorRecord.Exception.Message }",
        "    return \"$code $message\".Trim()",
        "}",
        "",
        "if ($connectionIds.Count -gt 0) {",
        "    $results = foreach ($id in $connectionIds) {",
        "        try {",
        "            $connection = Invoke-RestMethod -Method Get -Authentication Bearer "
        "-Token $secureToken -Uri \"$fabric/v1/connections/$id\"",
        "            [PSCustomObject]@{",
        "                Id               = $id",
        "                Name             = if ($connection.displayName) { $connection.displayName } "
        "else { '(not returned)' }",
        "                ConnectivityType = if ($connection.connectivityType) "
        "{ $connection.connectivityType } else { '(not returned)' }",
        "                Type             = if ($connection.connectionDetails.type) "
        "{ $connection.connectionDetails.type } else { '(not returned)' }",
        "            }",
        "        } catch {",
        "            # One connection this account cannot read should not stop the rest.",
        "            Write-Warning \"Connection $id : $(Get-FabricServiceError $_)\"",
        "            [PSCustomObject]@{ Id = $id; Name = '(unavailable)'; "
        "ConnectivityType = '(unavailable)'; Type = '(unavailable)' }",
        "        }",
        "    }",
        "} else {",
        "    # Nothing was recorded or supplied - most likely a report from before this scan",
        "    # existed. Listing every connection this account can see is still more useful",
        "    # than an empty result.",
        "    Write-Host 'No connection ids were recorded or supplied; listing every ' `",
        "        'connection this account can see instead.' -ForegroundColor Yellow",
        "    $results = @()",
        "    $continuationToken = $null",
        "    do {",
        "        # Always the fixed Fabric host, with only the (URL-encoded) continuation",
        "        # token appended - never a server-returned continuationUri. Following an",
        "        # absolute URI the response supplies would hand this account's bearer",
        "        # token to wherever that URI points, which does not have to stay on",
        "        # $fabric.",
        "        $uri = if ($continuationToken) {",
        "            \"$fabric/v1/connections?continuationToken=\" `",
        "                + [System.Uri]::EscapeDataString($continuationToken)",
        "        } else {",
        "            \"$fabric/v1/connections\"",
        "        }",
        "        try {",
        "            $page = Invoke-RestMethod -Method Get -Authentication Bearer "
        "-Token $secureToken -Uri $uri",
        "        } catch {",
        "            Write-Warning \"Listing connections : $(Get-FabricServiceError $_)\"",
        "            break",
        "        }",
        "        foreach ($connection in $page.value) {",
        "            $results += [PSCustomObject]@{",
        "                Id               = $connection.id",
        "                Name             = if ($connection.displayName) { $connection.displayName } "
        "else { '(not returned)' }",
        "                ConnectivityType = if ($connection.connectivityType) "
        "{ $connection.connectivityType } else { '(not returned)' }",
        "                Type             = if ($connection.connectionDetails.type) "
        "{ $connection.connectionDetails.type } else { '(not returned)' }",
        "            }",
        "        }",
        "        $continuationToken = $page.continuationToken",
        "    } while ($continuationToken)",
        "}",
        "",
        "$results | Format-Table -AutoSize",
        "",
    ]
    return "\n".join(lines)


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
    return DependencyReport(
        graph=graph,
        issues=issues,
        access=scan_connection_access(client, source_workspace_id=source_workspace_id),
    )


def _check_dependencies(ctx: _Context) -> None:
    """Report dependencies that will not survive the move, before anything is created.

    References are rewritten through an id map covering only the items this run creates, so a
    dependency on another workspace, or on an item type Fab Shuffle does not migrate, leaves
    the copy pointing somewhere it should not. That is invisible in the item definitions.
    """
    step = "dependencies"
    ctx.run.start_step(step, "Checking dependencies between items")
    ctx.run.raise_if_cancelled()

    # Source-bound connections and endpoint aliases must be known even when relations are
    # unavailable. Early data-store consumers use the same missing-reference guard.
    _load_source_references(ctx)

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
    if ctx.plan.cross_tenant:
        for item_id, item in ctx.graph.items.items():
            ctx.source_items.setdefault(item_id, {**item, "id": item_id})

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


def _load_source_references(ctx: _Context) -> None:
    source_id = ctx.plan.source_workspace_id
    primary_ids = set(ctx.source_items)
    if ctx.prior is not None:
        for identifier, label in ctx.prior.blocked_references.items():
            ctx.source_items.setdefault(identifier, {
                "id": identifier, "displayName": label, "type": "ExternalReference",
            })
        previous_connections = {
            key.casefold() for key in (ctx.prior.plan.get("connection_mappings") or {})
        }
        for removed in previous_connections - set(ctx.plan.connection_mappings):
            key = next((key for key in ctx.id_map if key.casefold() == removed), removed)
            ctx.journal.block_references({key: key}, refresh=ctx.adopted_targets)
            ctx.source_items.setdefault(key, {"id": key, "displayName": key, "type": "Connection"})
            ctx.invalidate_mapping(key)
        fields = ("source_workspace_id", "source_item_id", "target_workspace_id", "target_item_id")
        current_references = {
            tuple(str(entry.get(key) or "").casefold() for key in fields)
            for entry in ctx.plan.reference_mappings
        }
        previous_references = {
            tuple(str(entry.get(key) or "").casefold() for key in fields)
            for entry in (ctx.prior.plan.get("reference_mappings") or [])
        }
        if previous_references - current_references:
            # Older paired records did not attribute every external endpoint alias to its
            # item. Rebuild non-item aliases rather than retaining a removed external route.
            for alias in list(ctx.id_map):
                if alias in primary_ids or alias == source_id:
                    continue
                ctx.source_items.setdefault(alias, {
                    "id": alias, "displayName": alias, "type": "ExternalReference",
                })
                ctx.journal.block_references({alias: alias}, refresh=ctx.adopted_targets)
                ctx.invalidate_mapping(alias)
    if ctx.plan.reference_mappings:
        external_map, external_items = migration_refs.resolve(
            ctx.client, ctx.destination_client, ctx.plan.reference_mappings,
            migrating_workspace_id=source_id,
        )
        ctx.source_items.update(external_items)
        for old, new in external_map.items():
            if old in ctx.id_map and ctx.id_map[old] != new:
                ctx.invalidate_mapping(old)
            ctx.map_alias(old, new, old)
    types = {item.get("type") for item in ctx.source_items.values()}
    for item_type, read in (
        ("Lakehouse", data_stores.list_lakehouses),
        ("Warehouse", data_stores.list_warehouses),
        ("Eventhouse", eventhouses.list_eventhouses),
        ("MirroredDatabase", data_stores.list_mirrored_databases),
        ("SQLDatabase", sqldatabases.list_sql_databases),
    ):
        if item_type not in types:
            continue
        for item in read(ctx.client, source_id):
            if item.get("id"):
                ctx.source_items[item["id"]] = {
                    **ctx.source_items.get(item["id"], {}), **item, "type": item_type,
                }
    if ctx.plan.paired:
        for item_id in primary_ids:
            item = ctx.source_items[item_id]
            for root in migration_refs.onelake_aliases(source_id, ctx.plan.source_workspace_name, item):
                ctx.source_items[root] = item
    identifiers = {source_id, *analytics.reference_identifiers(ctx.source_items)}
    source_connections = {
        str(connection["id"]).casefold(): connection
        for connection in connections.list_connections(ctx.client)
        if connection.get("id")
    }
    # A connection only matters here if something actually being migrated points at it: an
    # explicit operator mapping, an item whose definition binds one directly, or a shortcut
    # (connections do not always appear in an item's own definition parts). Fab Shuffle never
    # scans every tenant connection looking for a reason to act on one - only the connections
    # its selected items and shortcuts actually reference.
    referenced_connections: set[str] = {key.casefold() for key in ctx.plan.connection_mappings}
    for item in ctx.assessment.migrated if ctx.assessment else ():
        if item.get("type") not in CONNECTION_BINDING_TYPES or not item.get("id"):
            continue
        try:
            definition = items_module.try_get_item_definition(ctx.client, source_id, item["id"])
        except FabricError as error:
            ctx.warnings.append(
                f"Connection bindings for '{item.get('displayName') or item['id']}' could "
                f"not be read: {error}. Restore source definition access before retrying."
            )
            continue
        if definition is None:
            continue
        referenced_connections.update(
            value.casefold()
            for value in connections.referenced_connection_ids(definition.get("parts") or [])
        )
    for item in ctx.assessment.migrated if ctx.assessment else ():
        item_id = item.get("id")
        item_type = item.get("type")
        if not item_id or item_type not in ("Lakehouse", "KQLDatabase"):
            continue
        try:
            found = (
                shortcuts.list_shortcuts(ctx.client, source_id, item_id) if item_type == "Lakehouse"
                else shortcuts.list_table_shortcuts(ctx.client, source_id, item_id)
            )
        except FabricApiError as error:
            ctx.warnings.append(
                f"Shortcuts for '{item.get('displayName') or item_id}' could not be listed: "
                f"{error}. Restore source shortcut access before retrying."
            )
            continue
        referenced_connections.update(
            value.casefold() for value in shortcuts.referenced_connection_ids(found)
        )
    for connection_id in ctx.plan.connection_mappings:
        if connection_id not in source_connections:
            source_connections[connection_id] = ctx.client.get(f"connections/{connection_id}")
    for connection in source_connections.values():
        connection_id = str(connection.get("id") or "")
        if not connection_id or connection_id.casefold() not in referenced_connections:
            continue
        path = (connection.get("connectionDetails") or {}).get("path") or ""
        if (
            not ctx.plan.cross_tenant and connection_id.casefold() not in ctx.plan.connection_mappings
            and not any(key.casefold() in path.casefold() for key in identifiers if key)
        ):
            # Referenced, but not pointing back into this workspace: a standard external
            # connection that is reused unchanged, with nothing here for the operator to do.
            continue
        source_connection_id = connection_id.casefold()
        ctx.source_items[source_connection_id] = {
            **connection, "id": source_connection_id, "type": "Connection",
        }
        # Only detection happens here: this runs in the "dependencies" phase, before any
        # data store exists, so a supplied ``connection_mappings`` entry cannot yet be
        # rewritten against the id map (its lakehouse or warehouse endpoint has not been
        # created). Applying and validating the mapping is ``_validate_connection_mappings``,
        # which runs later, once every phase that can add to the id map has finished.


def _create_workspaces(ctx: _Context) -> None:
    step = "workspaces"
    ctx.run.start_step(step, "Creating target and scratch workspaces")
    ctx.run.raise_if_cancelled()

    # A resumed run reuses the workspaces the earlier attempt made. Creating a second pair
    # would abandon everything already built in the first and collide on the name besides.
    resumed = bool(ctx.target_workspace_id)
    if resumed:
        ctx.run.update_step(step, "Reusing the workspace the earlier attempt created")
        ctx.run.target_workspace = {
            "id": ctx.target_workspace_id,
            "displayName": ctx.plan.target_workspace_name,
        }
        ctx.id_map[ctx.plan.source_workspace_id] = ctx.target_workspace_id
    else:
        target = workspaces.create_workspace(
            ctx.destination_client,
            ctx.plan.target_workspace_name,
            ctx.plan.capacity_id,
            description=(
                f"Created by Fab Shuffle from '{ctx.plan.source_workspace_name}' "
                f"in {ctx.plan.capacity_region}."
            ),
        )
        ctx.target_workspace_id = target["id"]
        ctx.run.target_workspace = {
            "id": target["id"],
            "displayName": ctx.plan.target_workspace_name,
        }
        ctx.journal.workspace("target", target["id"], ctx.plan.target_workspace_name)
        ctx.id_map[ctx.plan.source_workspace_id] = target["id"]
    if ctx.plan.paired:
        for item in ctx.assessment.migrated if ctx.assessment else ():
            if item["id"] in ctx.id_map:
                ctx.map_item_paths(item, ctx.id_map[item["id"]])

    # Grant the source workspace's admins straight away rather than waiting for the final
    # permissions phase. A run that fails before then would otherwise leave a workspace only
    # this service principal can see, which nobody else can inspect or delete.
    if not ctx.plan.paired:
        ctx.run.update_step(step, "Granting workspace admins access")
        ctx.source_role_assignments = workspaces.list_role_assignments(
            ctx.client, ctx.plan.source_workspace_id
        )
        admin_warnings = workspaces.copy_role_assignments(
            ctx.destination_client, ctx.source_role_assignments, ctx.target_workspace_id, roles={"Admin"}
        )
        ctx.warnings.extend(admin_warnings)
    else:
        ctx.warnings.append(
            f"Destination workspace {ctx.target_workspace_id} was created without copying access "
            "assignments. A destination tenant administrator must arrange operator/user access."
        )

    # Copy Jobs must live somewhere that is not the workspace being built, otherwise they
    # show up as leftover items in the migrated workspace.
    if not ctx.scratch_workspace_id and not ctx.plan.paired:
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
        ctx.journal.workspace("scratch", scratch["id"], scratch_name)

        # The scratch workspace needs the same treatment so a stranded one stays deletable.
        workspaces.copy_role_assignments(
            ctx.client, ctx.source_role_assignments, ctx.scratch_workspace_id, roles={"Admin"}
        )

        # A workspace is not fully initialised for Copy Jobs until it holds a lakehouse.
        data_stores.create_lakehouse(ctx.client, ctx.scratch_workspace_id, "hold")
    elif ctx.scratch_workspace_id:
        ctx.run.scratch_workspace = {"id": ctx.scratch_workspace_id, "displayName": ""}

    ctx.run.update_step(step, "Recreating workspace folders")
    folder_map = workspaces.clone_folder_tree(
        ctx.client, ctx.plan.source_workspace_id, ctx.target_workspace_id, **ctx.target_kwargs,
    )
    ctx.id_map.update(folder_map)

    warnings = _copy_spark_configuration(ctx, step)
    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step,
        StepStatus.SUCCEEDED,
        (
            f"Reusing '{ctx.plan.target_workspace_name}'"
            if resumed
            else f"Created '{ctx.plan.target_workspace_name}'"
        ),
        warnings,
    )


def _copy_spark_configuration(ctx: _Context, step: str) -> list[str]:
    """Recreate custom Spark pools and workspace Spark settings.

    Runs here rather than in the engineering phase because an environment pins a pool by id,
    so the pool has to exist, and be in the id map, before any environment is migrated.
    """
    ctx.run.update_step(step, "Recreating custom Spark pools")
    source_pools = spark.list_pools(ctx.client, ctx.plan.source_workspace_id)
    pools_by_id = {pool["id"]: pool for pool in source_pools if pool.get("id")}
    for source_id, pool in pools_by_id.items():
        if pool.get("name") != spark.STARTER_POOL:
            ctx.source_items[source_id] = {
                "id": source_id, "displayName": pool.get("name") or source_id, "type": "SparkPool",
            }

    def mapped(source: str, target: str) -> None:
        ctx.id_map[source] = target
        ctx.journal.item(source, target, "SparkPool", str(pools_by_id[source].get("name") or source))

    pool_map, created, warnings = spark.copy_pools(
        ctx.client, ctx.plan.source_workspace_id, ctx.target_workspace_id,
        pools=source_pools,
        prior_map=ctx.prior.id_map if ctx.prior else None,
        on_mapped=mapped,
        on_missing=ctx.invalidate_mapping,
        **ctx.target_kwargs,
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
        target = spark.get_settings(ctx.destination_client, ctx.target_workspace_id)
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
            spark.update_settings(ctx.destination_client, ctx.target_workspace_id, payload)
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

        adopted_eventhouse = ctx.already_created(eventhouse["id"])
        target_id = ctx.resolve_or_create(
            eventhouse, "Eventhouse", partial(
                eventhouses.create_eventhouse,
                ctx.destination_client,
                ctx.target_workspace_id,
                name,
                folder_id=ctx.id_map.get(eventhouse.get("folderId", "")),
            ),
        )
        new_eventhouse = eventhouses.get_eventhouse(
            ctx.destination_client, ctx.target_workspace_id, target_id,
        )

        source_properties = eventhouse.get("properties") or {}
        target_properties = new_eventhouse.get("properties") or {}
        for key in ("queryServiceUri", "ingestionServiceUri"):
            if source_properties.get(key) and target_properties.get(key):
                ctx.map_alias(source_properties[key], target_properties[key], eventhouse["id"])

        # Creating an eventhouse also creates a child KQL database named after it, so the
        # target already holds a database that the source is about to ask us to create.
        auto_created = eventhouses.eventhouse_databases(
            ctx.destination_client, ctx.target_workspace_id, new_eventhouse
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
                    (database, target_id, source_properties.get("queryServiceUri", ""))
                )
                continue

            moved, database_warnings, adopted = _migrate_kql_database(
                ctx,
                step,
                database=database,
                target_eventhouse_id=target_id,
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
        for leftover in sorted(set(auto_created) - adopted_names) if not adopted_eventhouse else []:
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
    if ctx.already_created(database["id"]) and database["id"] not in ctx.refresh_needed:
        return True, []

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
            if ctx.plan.cross_tenant and source.database_name not in ctx.id_map:
                return False, [
                    f"KQL database '{name}' needs a destination mapping for leader "
                    f"'{source.database_name}'. Add that mapping and retry; "
                    "no source-bound follower was created."
                ]
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

    if ctx.already_created(database["id"]):
        # Shortcut definitions only support ReadWrite databases. Reusing an unchanged leader
        # is safe, but a changed leader must not be reported as a successful adoption.
        target_id = ctx.id_map[database["id"]]
        binding = ctx.prior.follower_bindings.get(database["id"], {})
        if binding.get("target") != target_id:
            existing = eventhouses.get_kql_database(
                ctx.destination_client, ctx.target_workspace_id, target_id,
            )
            properties = existing.get("properties") or {}
            query_uri = properties.get("queryServiceUri") or ctx.id_map.get(source_query_uri)
            actual_source = kql.follower_source(
                query_uri, target_id, ctx.destination_principal
            ) if query_uri else None
            binding = {
                "target": target_id, "parent": properties.get("parentEventhouseItemId") or "",
                "leader": actual_source.database_name if actual_source else "",
            }
        previous_leader = binding.get("leader")
        previous_parent = binding.get("parent")
        if previous_leader == source_database_name and previous_parent == target_eventhouse_id:
            ctx.journal.follower(database["id"], target_id, source_database_name, target_eventhouse_id)
            ctx.journal.refresh([database["id"]], required=False)
            ctx.refresh_needed.discard(database["id"])
            return True, []
        raise ResumeRefused(
            f"Shortcut KQL database '{name}' needs leader '{source_database_name or 'unknown'}' "
            f"in eventhouse '{target_eventhouse_id}' instead of its previous leader "
            f"'{previous_leader or 'unknown'}' or eventhouse '{previous_parent or 'unknown'}'. Delete its "
            f"old target item {ctx.id_map[database['id']]} and resume to recreate it with "
            "the new binding; Fabric's definition API only accepts ReadWrite databases."
        )
    target = eventhouses.create_kql_database(
        ctx.destination_client,
        ctx.target_workspace_id,
        name,
        creation_payload=payload,
        folder_id=ctx.id_map.get(database.get("folderId", "")),
    )
    ctx.id_map[database["id"]] = target["id"]
    ctx.journal.item(database["id"], target["id"], "KQLDatabase", name)
    ctx.journal.follower(database["id"], target["id"], source_database_name, target_eventhouse_id)
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
    evidence = ctx.lifecycle(database, "KQLDatabase")
    evidence.step("definition", EvidenceState.UNKNOWN, "KQL schema refresh has not completed.")

    ctx.run.update_step(step, f"Recreating KQL database '{name}'")
    parts = eventhouses.kql_database_definition_parts(
        ctx.client, ctx.plan.source_workspace_id, database_id
    )

    existing = dict(existing_databases)
    if ctx.already_created(database_id):
        existing[name] = eventhouses.get_kql_database(
            ctx.destination_client, ctx.target_workspace_id, ctx.id_map[database_id]
        )
    precreated = False
    if ctx.plan.paired:
        if name not in existing:
            existing[name] = eventhouses.create_kql_database(
                ctx.destination_client, ctx.target_workspace_id, name,
                creation_payload={
                    "databaseType": "ReadWrite", "parentEventhouseItemId": target_eventhouse_id,
                },
                folder_id=ctx.id_map.get(database.get("folderId", "")),
            )
            precreated = True
        ctx.resolve_item(
            database, existing[name]["id"], "KQLDatabase",
            disposition=Disposition.CREATED if precreated else Disposition.ADOPTED,
        )
        if ctx.plan.cross_tenant:
            ctx.warnings.extend(analytics.validate_cross_tenant_references(
                parts, source_workspace_id=ctx.plan.source_workspace_id,
                target_workspace_id=ctx.target_workspace_id, id_map=ctx.id_map,
                target_client=ctx.destination_client, source_items=ctx.source_items,
                item_type="KQLDatabase", lifecycle=evidence,
            ))
        parts, _ = definitions.rewrite_parts(parts, ctx.id_map)
    parts = eventhouses.retarget_database_definition(parts, target_eventhouse_id)
    target, adopted = eventhouses.create_or_adopt_kql_database(
        ctx.destination_client,
        ctx.target_workspace_id,
        name,
        parts=parts,
        existing=existing,
        folder_id=ctx.id_map.get(database.get("folderId", "")),
    )
    evidence = ctx.resolve_item(
        database, target["id"], "KQLDatabase",
        disposition=Disposition.CREATED if precreated or not adopted else Disposition.REFRESHED,
    )
    evidence.step("definition", EvidenceState.SUCCEEDED, "KQL schema definition applied.")
    evidence.step("rebind", EvidenceState.SUCCEEDED, "Parent eventhouse reference rewritten.")
    if ctx.plan.paired:
        if target_query_uri:
            policy_warnings = kql.stop_database_update_policies(
                target_cluster_uri=target_query_uri, target_database=target["id"],
                target_principal=ctx.destination_principal,
                on_progress=_bulk_copy_progress(ctx, step, name),
                cancel_requested=lambda: ctx.run.cancelled,
            )
            if policy_warnings:
                evidence.step(
                    "activation", EvidenceState.SKIPPED, "Target update policies were left disabled.",
                    action=" ".join(policy_warnings),
                )
                ctx.warnings.extend(policy_warnings)
        else:
            evidence.step(
                "activation", EvidenceState.UNKNOWN, "Target update policies could not be inspected.",
                action="Restore the target KQL endpoint and stop update policies before copying data.",
            )
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
    if not table_shortcuts:
        evidence.step("shortcuts", EvidenceState.SUCCEEDED, "No KQL table shortcuts found.")
    shortcut_names = {s["name"] for s in table_shortcuts if s.get("name")}

    if not ctx.plan.include_data:
        return True, [], adopted_name
    if not source_query_uri or not target_query_uri:
        evidence.step(
            "data", EvidenceState.UNKNOWN, "Query endpoint missing; data was not copied.",
            action="Restore the KQL query endpoints and retry copying the data.",
        )
        return True, [f"KQL database '{name}' has no query endpoint, data was not copied"], adopted_name
    if ctx.already_copied(database_id, "kql"):
        return True, [], adopted_name

    ctx.run.update_step(step, f"Copying data for KQL database '{name}'")
    with evidence.operation("data", "KQL data copy completed."):
        copy = kql.copy_database_streaming if ctx.plan.paired else kql.copy_database
        options: dict[str, Any] = {}
        if ctx.plan.paired:
            options = {
                "target_database": target["id"],
                "target_principal": ctx.destination_principal,
                "max_staging_bytes": SETTINGS.max_staging_bytes,
                "cancel_requested": lambda: ctx.run.cancelled,
            }
        result = copy(
            source_cluster_uri=source_query_uri,
            target_cluster_uri=target_query_uri,
            database=database_id if ctx.plan.paired else name,
            principal=ctx.principal,
            exclude=shortcut_names,
            on_progress=lambda message: ctx.run.update_step(step, message),
            **options,
        )
    ctx.data_copied(database_id, "kql", outcome=CopyOutcome("kql", empty=result["tables"] == 0))
    ctx.warnings.extend(result.get("warnings") or [])
    evidence.complete()
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
        target_id = ctx.resolve_or_create(
            lakehouse, "Lakehouse", partial(
                data_stores.create_lakehouse,
                ctx.destination_client,
                ctx.target_workspace_id,
                name,
                schema_enabled=schema_enabled,
                folder_id=ctx.id_map.get(lakehouse.get("folderId", "")),
            ),
        )
        target = data_stores.get_lakehouse(ctx.destination_client, ctx.target_workspace_id, target_id)
        for folder in ("Files", "Tables") if ctx.plan.paired else ():
            old_path = _lakehouse_storage_path(lakehouse, folder)
            new_path = _lakehouse_storage_path(target, folder)
            if old_path and new_path:
                ctx.map_alias(old_path, new_path, lakehouse["id"])

        source_endpoint = data_stores.lakehouse_sql_endpoint(lakehouse)
        target_endpoint = data_stores.lakehouse_sql_endpoint(target)
        # Both may be absent. The endpoint is provisioned asynchronously, so a lakehouse read
        # straight after creation usually has no endpoint id or connection string yet. The
        # shortcut phase reads it again, by which time it exists, and records what is missing.
        _map_sql_endpoint(ctx, source_endpoint, target_endpoint, owner=lakehouse["id"])

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
    *,
    owner: str = "",
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
        if source_value:
            known = analytics.reference_identifiers(ctx.source_items).get(source_value)
            ctx.source_items.setdefault(source_value, dict(known) if known else {
                "id": source_endpoint.get("id") or source_value,
                "type": "SQLEndpoint",
                "displayName": source_endpoint.get("displayName") or source_endpoint.get("id"),
            })
        if source_value and target_value:
            ctx.map_alias(source_value, target_value, owner)


def _lakehouse_storage_path(lakehouse: Mapping[str, Any], folder: str) -> str:
    properties = lakehouse.get("properties") or {}
    path = properties.get(f"oneLake{folder}Path")
    if path:
        return str(path)
    files_path = str(properties.get("oneLakeFilesPath") or "").rstrip("/")
    if folder == "Tables" and files_path.rsplit("/", 1)[-1] == "Files":
        return files_path.rsplit("/", 1)[0] + "/Tables"
    return ""


def _shortcut_exclusions(ctx: _Context, item_id: str, folder: str) -> tuple[str, ...]:
    excluded = []
    for shortcut in shortcuts.list_shortcuts(ctx.client, ctx.plan.source_workspace_id, item_id):
        path = f"{shortcut.get('path', '').strip('/')}/{shortcut.get('name', '')}".strip("/")
        if path.casefold().startswith(folder.casefold() + "/"):
            excluded.append(path[len(folder) + 1:])
    return tuple(excluded)


def _copy_lakehouse_tables(
    ctx: _Context,
    step: str,
    pairs: list[tuple[dict[str, Any], dict[str, Any], bool]],
) -> list[str]:
    """Copy every lakehouse's tables, several jobs at a time."""
    warnings: list[str] = []
    specs: list[copyjobs.CopyJobSpec] = []
    if ctx.plan.paired:
        for lakehouse, target, _schema_enabled in pairs:
            ctx.run.raise_if_cancelled()
            if ctx.already_copied(lakehouse["id"], "lakehouse"):
                continue
            evidence = ctx.evidence(lakehouse["id"])
            source_path = _lakehouse_storage_path(lakehouse, "Tables")
            target_path = _lakehouse_storage_path(target, "Tables")
            if not source_path or not target_path:
                evidence.step(
                    "data", EvidenceState.UNKNOWN, "OneLake table paths were not returned.",
                    action="Restore source and destination OneLake table paths, then retry.",
                )
                warnings.append(f"Lakehouse '{lakehouse['displayName']}' has no OneLake table copy path.")
                continue
            try:
                with evidence.operation("data", "Quiesced OneLake table files transferred."):
                    outcome = file_transfer.copy_tree_streaming(
                        source_path=source_path, target_path=target_path,
                        tokens=ctx.tokens, target_tokens=ctx.destination_tokens,
                        max_staging_bytes=SETTINGS.max_staging_bytes,
                        exclude_paths=_shortcut_exclusions(ctx, lakehouse["id"], "Tables"),
                        kind="lakehouse", scratch_dir=ctx.scratch_dir / f"delta-{lakehouse['id']}",
                        cancel_requested=lambda: ctx.run.cancelled,
                        on_progress=_bulk_copy_progress(ctx, step, lakehouse["displayName"]),
                    )
                ctx.data_copied(lakehouse["id"], "lakehouse", outcome=outcome)
            except file_transfer.FileTransferError as error:
                evidence.step(
                    "data", EvidenceState.FAILED, "OneLake table copy failed.", error=error,
                    action="Resolve the reported table-file failure and retry with source writes stopped.",
                )
                warnings.append(f"Tables for lakehouse '{lakehouse['displayName']}' did not copy: {error}")
        return warnings

    for lakehouse, target, schema_enabled in pairs:
        ctx.run.raise_if_cancelled()
        name = lakehouse["displayName"]
        try:
            tables = _lakehouse_tables(ctx, lakehouse, schema_enabled=schema_enabled)
        except (sqlschema.SchemaTransferError, FabricApiError) as error:
            ctx.evidence(lakehouse["id"]).step(
                "data", EvidenceState.FAILED, "Table enumeration failed.", error=error,
                action="Restore access to the source tables and retry the data copy.",
            )
            warnings.append(
                f"Could not list tables in lakehouse '{name}', so its data was not copied: {error}"
            )
            continue
        if not tables:
            ctx.evidence(lakehouse["id"]).step(
                "data", EvidenceState.SUCCEEDED, "No copyable lakehouse tables found.",
            )
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
                item_id=lakehouse["id"],
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
    if ctx.plan.paired:
        raise ValueError("Paired migrations use independent data clients, not native cross-tenant Copy Jobs.")
    # A source rename must not hide a job still writing to the same target. Keep the saved
    # submission name for reconciliation; item identity, not a label, selects it.
    resolved_specs = []
    for spec in specs:
        saved = [
            record for (item_id, _), record in ctx.active_copy_jobs.items() if item_id == spec.item_id
        ]
        if len(saved) > 1:
            raise ResumeRefused(
                f"Several unfinished Copy Jobs exist for '{spec.label}'; reconcile them first."
            )
        if saved:
            record = saved[0]
            if (
                record["target"] != ctx.id_map.get(spec.item_id)
                or record["job"]["workspace_id"] != spec.workspace_id
            ):
                raise ResumeRefused(
                    f"Copy Job '{spec.label}' belongs to an older target or scratch workspace; "
                    "reconcile it first."
                )
            spec = copyjobs.CopyJobSpec(
                workspace_id=spec.workspace_id, display_name=record["job"]["display_name"],
                content=spec.content, label=spec.label, item_id=spec.item_id,
            )
        resolved_specs.append(spec)
    specs = resolved_specs
    # A resume does not re-copy what an earlier attempt finished. These are the longest thing
    # in the whole migration, so this is most of what makes picking one up worth doing.
    specs = [spec for spec in specs if not ctx.already_copied(spec.item_id, "tables")]
    if not specs:
        return []
    for spec in specs:
        ctx.evidence(spec.item_id).step(
            "data", EvidenceState.UNKNOWN, "Copy Job has not completed.",
            action="Wait for completion or reconcile the recorded Copy Job before retrying.",
        )
    # Sized from the capacity the jobs will run on, not from a fixed number: an F64 can keep
    # four of these busy where an F16 can barely keep one.
    concurrent = workspaces.copy_job_concurrency(ctx.plan.capacity_sku)
    ctx.run.update_step(
        step,
        f"Copying {what} data in {len(specs)} job(s), {concurrent} at a time",
    )
    def remember(job: copyjobs.CopyJobRun) -> None:
        record = {"job": asdict(job), "target": ctx.id_map.get(job.item_id, ""), "active": True}
        ctx.active_copy_jobs[(job.item_id, job.display_name)] = record
        ctx.journal.copy_job(record["job"], target_id=record["target"])

    def state(job: copyjobs.CopyJobRun) -> None:
        if job.last_status and job.last_status != "Completed":
            ctx.evidence(job.item_id).step(
                "data",
                EvidenceState.FAILED if job.last_status in copyjobs.TERMINAL_JOB_STATES
                else EvidenceState.UNKNOWN,
                f"Copy Job status: {job.last_status}.",
                action="Review the Copy Job in the scratch workspace and retry after resolving it.",
                error=RuntimeError(job.last_error) if job.last_error else None,
            )
        if job.last_status in copyjobs.TERMINAL_JOB_STATES and job.instance_id:
            ctx.journal.copy_job(asdict(job), target_id=ctx.id_map.get(job.item_id, ""), active=False)
            ctx.active_copy_jobs.pop((job.item_id, job.display_name), None)
        else:
            remember(job)

    def done(spec: copyjobs.CopyJobSpec) -> None:
        ctx.data_copied(spec.item_id, "tables")

    def clear(spec: copyjobs.CopyJobSpec) -> None:
        key = (spec.item_id, spec.display_name)
        record = ctx.active_copy_jobs.get(key)
        if record:
            ctx.journal.copy_job(record["job"], target_id=record["target"], active=False)
            del ctx.active_copy_jobs[key]

    selected = {(spec.item_id, spec.display_name) for spec in specs}
    resume_jobs = []
    for key, record in ctx.active_copy_jobs.items():
        if key in selected:
            if record["target"] != ctx.id_map.get(key[0]):
                raise ResumeRefused(f"Copy Job '{key[1]}' belongs to an older target; reconcile it first.")
            resume_jobs.append(copyjobs.CopyJobRun(**record["job"]))
    try:
        created, warnings = copyjobs.run_copy_jobs(
            ctx.client,
            specs,
            concurrency=concurrent,
            on_progress=lambda message: ctx.run.update_step(step, message),
            on_done=done,
            on_state=state,
            resume_jobs=resume_jobs,
        )
    except copyjobs.CopyJobBatchIncomplete as error:
        ctx.run.summary["unresolvedCopyJobs"] = [asdict(job) for job in error.active_jobs]
        ctx.copy_job_ids.extend(error.created)
        ctx.warnings.extend(error.warnings)
        try:
            for job in error.active_jobs:
                remember(job)
            unresolved = {(job.item_id, job.display_name) for job in error.active_jobs}
            for spec in specs:
                if (spec.item_id, spec.display_name) not in unresolved:
                    clear(spec)
        except OSError as persistence_error:
            error.add_note(f"Recovery metadata could not be persisted: {persistence_error}")
            raise error from persistence_error
        raise
    for spec in specs:
        clear(spec)
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
        limit=1 if ctx.plan.paired else SETTINGS.file_transfer_concurrency,
        on_progress=lambda message: ctx.run.update_step(step, f"Copying files: {message}"),
        noun="file copy",
    )


def _lakehouse_file_job(
    ctx: _Context,
    lakehouse: dict[str, Any],
    target: dict[str, Any],
) -> Callable[[], list[str]]:
    name = lakehouse["displayName"]
    source_files = _lakehouse_storage_path(lakehouse, "Files")
    target_files = _lakehouse_storage_path(target, "Files")

    def run() -> list[str]:
        if not source_files or not target_files:
            ctx.evidence(lakehouse["id"]).step(
                "files", EvidenceState.UNKNOWN, "File paths missing; copy not attempted.",
                action="Restore the source and target OneLake file paths and retry.",
            )
            return []
        if ctx.already_copied(lakehouse["id"], "files"):
            return []
        ctx.run.raise_if_cancelled()
        ctx.evidence(lakehouse["id"]).step("files", EvidenceState.UNKNOWN, "File copy has not completed.")
        try:
            if ctx.plan.paired:
                result = file_transfer.copy_tree_streaming(
                    source_path=source_files, target_path=target_files,
                    tokens=ctx.tokens, target_tokens=ctx.destination_tokens,
                    max_staging_bytes=SETTINGS.max_staging_bytes,
                    exclude_paths=_shortcut_exclusions(ctx, lakehouse["id"], "Files"),
                    scratch_dir=ctx.scratch_dir / f"delta-files-{lakehouse['id']}",
                    cancel_requested=lambda: ctx.run.cancelled,
                    on_progress=_bulk_copy_progress(ctx, "lakehouses", name),
                )
            else:
                result = file_transfer.copy_files(
                    source_files_path=source_files,
                    target_files_path=target_files,
                    principal=ctx.principal,
                    scratch_dir=ctx.scratch_dir / f"lakehouse-{lakehouse['id']}",
                )
            ctx.data_copied(lakehouse["id"], "files", outcome=result)
            return []
        except file_transfer.FileTransferError as error:
            ctx.evidence(lakehouse["id"]).step(
                "files", EvidenceState.FAILED, "File copy failed.", error=error,
                action="Fix the file transfer failure and retry this lakehouse.",
            )
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
        target_id = ctx.resolve_or_create(
            warehouse, "Warehouse", partial(
                data_stores.create_warehouse,
                ctx.destination_client,
                ctx.target_workspace_id,
                name,
                collation_type=collation,
                folder_id=ctx.id_map.get(warehouse.get("folderId", "")),
            ),
        )
        target = data_stores.get_warehouse(ctx.destination_client, ctx.target_workspace_id, target_id)

        source_endpoint = data_stores.warehouse_connection_string(warehouse)
        target_endpoint = data_stores.warehouse_connection_string(target)
        if source_endpoint and target_endpoint:
            ctx.map_alias(source_endpoint, target_endpoint, warehouse["id"])

        created_pairs.append((warehouse, target_id, source_endpoint, target_endpoint))

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
            ctx.evidence(warehouse["id"]).step(
                "schema", EvidenceState.UNKNOWN, "Warehouse schema transfer has not completed.",
            )
            try:
                warnings = sqlschema.transfer_schema(
                    source_server=source_endpoint,
                    target_server=target_endpoint,
                    database=name,
                    principal=ctx.principal,
                    tokens=ctx.tokens,
                    scratch_dir=ctx.scratch_dir / "sql",
                    source_type="Warehouse",
                    **({
                        "target_tokens": ctx.destination_tokens,
                        "exclude_security": True,
                        "max_staging_bytes": SETTINGS.max_staging_bytes,
                        "cancel_requested": lambda: ctx.run.cancelled,
                        "id_map": ctx.id_map,
                        "source_identifiers": tuple(analytics.reference_identifiers(ctx.source_items)),
                    } if ctx.plan.paired else {}),
                )
            except sqlschema.SchemaTransferError as error:
                ctx.evidence(warehouse["id"]).step(
                    "schema", EvidenceState.FAILED, "Warehouse schema transfer failed.", error=error,
                    action="Resolve the schema failure, then retry schema and data transfer.",
                )
                # Without a schema there is nowhere for the rows to land, so this warehouse
                # is left out of the copy rather than failing a table at a time.
                return [f"Schema for warehouse '{name}' did not transfer: {error}"]
            ready.append(entry)
            ctx.evidence(warehouse["id"]).step(
                "schema", EvidenceState.UNKNOWN if warnings else EvidenceState.SUCCEEDED,
                "Schema transfer returned diagnostics; completeness unmeasured." if warnings
                else "Warehouse schema transfer completed.",
                action="Review the schema transfer diagnostics before cutover." if warnings else "",
            )
            return [f"Warehouse '{name}': {w}" for w in warnings]

        return run

    for entry in pairs:
        jobs.append(concurrency.Job(key=entry[0]["displayName"], run=transfer(entry)))

    warnings = concurrency.run_bounded(
        jobs,
        limit=1 if ctx.plan.paired else SETTINGS.schema_transfer_concurrency,
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
            ctx.evidence(warehouse["id"]).step(
                "data", EvidenceState.FAILED, "Table enumeration failed.", error=error,
                action="Restore access to the source tables and retry the data copy.",
            )
            warnings.append(f"Could not enumerate tables in warehouse '{name}': {error}")
            continue
        if not tables:
            ctx.evidence(warehouse["id"]).step(
                "data", EvidenceState.SUCCEEDED, "No copyable warehouse tables found.",
            )
            continue

        if ctx.plan.paired:
            warnings.extend(_stream_relational_tables(
                ctx, step, warehouse["id"], name, tables,
                source_endpoint, name, target_endpoint, name, target_type="Warehouse",
            ))
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
                item_id=warehouse["id"],
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


def _stream_relational_tables(
    ctx: _Context, step: str, source_id: str, name: str, tables: list[data_stores.TableRef],
    source_server: str, source_database: str, target_server: str, target_database: str,
    *, target_type: str = "SQLDatabase",
) -> list[str]:
    expected = {bulkcopy.qualified_name(table) for table in tables}
    completed = {key for key in expected if ctx.already_copied(source_id, "table", key)}
    remaining = [table for table in tables if bulkcopy.qualified_name(table) not in completed]
    evidence = ctx.evidence(source_id)
    evidence.step("data", EvidenceState.UNKNOWN, "Independent SQL endpoint transfer has not completed.")
    try:
        warnings = bulkcopy.copy_tables_streaming(
            source_server=source_server, source_database=source_database,
            target_server=target_server, target_database=target_database,
            tables=remaining, tokens=ctx.tokens, target_tokens=ctx.destination_tokens,
            max_staging_bytes=SETTINGS.max_staging_bytes,
            target_type=target_type,
            cancel_requested=lambda: ctx.run.cancelled,
            on_progress=_bulk_copy_progress(ctx, step, name),
            on_copied=_table_recorder(ctx, source_id, completed),
        )
    except bulkcopy.BulkCopyError as error:
        evidence.step(
            "data", EvidenceState.FAILED, "Independent SQL endpoint transfer failed.", error=error,
            action="Resolve the reported copy failure and retry with source writes stopped.",
        )
        return [f"Table data for '{name}' did not copy: {error}"]
    missing = expected - completed
    evidence.step(
        "data", EvidenceState.FAILED if missing else EvidenceState.SUCCEEDED,
        f"Tables without copy checkpoints: {', '.join(sorted(missing))}."
        if missing else "Every enumerated table has a target-bound transfer checkpoint.",
        action="Resolve failed table copies and retry." if missing else "",
    )
    evidence.complete()
    return [f"'{name}': {warning}" for warning in warnings]


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
            target_id = ctx.resolve_or_create(
                database, sqldatabases.SQL_DATABASE, partial(
                    sqldatabases.create_sql_database,
                    ctx.destination_client,
                    ctx.target_workspace_id,
                    name,
                    collation=properties.get("collation"),
                    backup_retention_days=properties.get("backupRetentionDays"),
                    description=database.get("description") or None,
                    folder_id=ctx.id_map.get(database.get("folderId", "")),
                ),
            )
        except FabricError as error:
            warnings.append(analytics.describe_failure(sqldatabases.SQL_DATABASE, name, error))
            continue

        target = sqldatabases.get_sql_database(ctx.destination_client, ctx.target_workspace_id, target_id)
        source_server = sqldatabases.server_fqdn(database)
        target_server = sqldatabases.server_fqdn(target)
        if source_server and target_server:
            ctx.map_alias(source_server, target_server, database["id"])
        source_catalog = sqldatabases.database_name(database)
        target_catalog = sqldatabases.database_name(target)
        if source_catalog and target_catalog:
            ctx.map_alias(source_catalog, target_catalog, database["id"])
        migrated += 1

        ctx.run.update_step(step, f"Applying schema to SQL database '{name}'")
        try:
            with ctx.evidence(database["id"]).operation("schema", "SQL database schema applied."):
                if ctx.plan.paired:
                    schema_warnings = sqlschema.transfer_schema(
                        source_server=source_server, target_server=target_server,
                        database=source_catalog, target_database=target_catalog,
                        principal=ctx.principal, tokens=ctx.tokens,
                        target_tokens=ctx.destination_tokens, exclude_security=True,
                        max_staging_bytes=SETTINGS.max_staging_bytes,
                        cancel_requested=lambda: ctx.run.cancelled,
                        scratch_dir=ctx.scratch_dir / f"schema-{database['id']}",
                        source_type="SQLDatabase", id_map=ctx.id_map,
                        source_identifiers=tuple(analytics.reference_identifiers(ctx.source_items)),
                    )
                    if schema_warnings:
                        raise sqlschema.SchemaTransferError("; ".join(schema_warnings))
                else:
                    sqldatabases.copy_schema(
                        ctx.client,
                        source_workspace_id=ctx.plan.source_workspace_id,
                        source_id=database["id"],
                        target_workspace_id=ctx.target_workspace_id,
                        target_id=target_id,
                    )
        except (FabricError, sqlschema.SchemaTransferError) as error:
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
    file and straight back in, without either end being parsed or retyped. The only connection
    involved empties each target table first, because ``bcp in`` appends.
    """
    warnings: list[str] = []

    for source, target in ready:
        ctx.run.raise_if_cancelled()
        name = source["displayName"]
        evidence = ctx.lifecycle(source, sqldatabases.SQL_DATABASE)
        source_server = sqldatabases.server_fqdn(source)
        source_catalog = sqldatabases.database_name(source)
        target_server = sqldatabases.server_fqdn(target)
        target_catalog = sqldatabases.database_name(target)
        if not source_server or not source_catalog or not target_server or not target_catalog:
            evidence.step(
                "data", EvidenceState.UNKNOWN, "SQL connection coordinates missing.",
                action="Restore the source and target SQL endpoints, then retry copying data.",
            )
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
            evidence.step(
                "data", EvidenceState.FAILED, "SQL table enumeration failed.", error=error,
                action="Restore source table access and retry.",
            )
            warnings.append(f"Could not enumerate tables in SQL database '{name}': {error}")
            continue
        if ctx.plan.paired:
            warnings.extend(_stream_relational_tables(
                ctx, step, source["id"], name, tables,
                source_server, source_catalog, target_server, target_catalog,
            ))
            continue
        expected = {bulkcopy.qualified_name(table) for table in tables}
        copied = {
            key for key in expected if ctx.already_copied(source["id"], "table", key)
        }
        # A resume does not repeat a table an earlier attempt finished. Doing so would be
        # correct now that the target is cleared first, but for a large database it is hours.
        tables = [
            table
            for table in tables
            if not ctx.already_copied(source["id"], "table", bulkcopy.qualified_name(table))
        ]
        if not tables:
            if not expected:
                evidence.step("data", EvidenceState.SUCCEEDED, "No SQL base tables found.")
            elif source["id"] not in ctx.refresh_needed and ctx.prior and all(
                ctx.prior.data_targets.get((source["id"], "table", key)) == target["id"]
                for key in expected
            ):
                evidence.step(
                    "data", EvidenceState.SUCCEEDED,
                    "Every enumerated table has a checkpoint for this target incarnation.",
                )
            continue

        ctx.run.update_step(step, f"Copying {len(tables)} table(s) from SQL database '{name}'")
        evidence.step("data", EvidenceState.UNKNOWN, "SQL table copy has not completed.")
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
                    on_copied=_table_recorder(ctx, source["id"], copied),
                )
            )
            missing = expected - copied
            evidence.step(
                "data", EvidenceState.FAILED if missing else EvidenceState.SUCCEEDED,
                f"Tables without successful copy checkpoints: {', '.join(sorted(missing))}."
                if missing else "Every enumerated table has a successful target-bound copy checkpoint.",
                action="Resolve the table copy failures and retry." if missing else "",
            )
        except bulkcopy.BulkCopyError as error:
            evidence.step(
                "data", EvidenceState.FAILED, "SQL table copy failed.", error=error,
                action="Resolve the reported table copy failure and retry.",
            )
            warnings.append(f"Table data for SQL database '{name}' did not copy: {error}")
        evidence.complete()

    return warnings


def _bulk_copy_progress(ctx: _Context, step: str, label: str) -> Callable[[str], None]:
    """Bind the label now, rather than reading it from the loop when the callback runs."""
    return lambda message: ctx.run.update_step(step, f"{label}: {message}")


def _table_recorder(
    ctx: _Context, item_id: str, completed: set[str] | None = None,
) -> Callable[[str], None]:
    """Bind the item now, so each table is written down against the right database."""
    def record(table: str) -> None:
        ctx.data_copied(item_id, "table", table)
        if completed is not None:
            completed.add(table)
    return record


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
        evidence = ctx.lifecycle(database, cosmosdb.COSMOS_DB_DATABASE)

        # The definition holds only container metadata and no Fabric ids, so there is
        # nothing to rewrite and the generic path handles it.
        ctx.run.update_step(step, f"Creating Cosmos DB database '{name}'")
        if ctx.already_created(database["id"]):
            target_id = ctx.id_map[database["id"]]
            ctx.resolve_item(database, target_id, cosmosdb.COSMOS_DB_DATABASE)
        else:
            try:
                result = analytics.migrate_definition_item(
                    ctx.client,
                    source_workspace_id=ctx.plan.source_workspace_id,
                    target_workspace_id=ctx.target_workspace_id,
                    item=database,
                    item_type=cosmosdb.COSMOS_DB_DATABASE,
                    id_map=ctx.id_map,
                    folder_id=ctx.id_map.get(database.get("folderId", "")),
                    lifecycle=evidence,
                    **ctx.target_kwargs,
                    **({"cross_tenant": True} if ctx.plan.cross_tenant else {}),
                )
            except FabricError as error:
                warnings.append(
                    analytics.describe_failure(cosmosdb.COSMOS_DB_DATABASE, name, error)
                )
                continue
            target_id = result.target_id
            ctx.resolve_item(
                database, target_id, cosmosdb.COSMOS_DB_DATABASE, disposition=Disposition.CREATED,
            )
        migrated += 1

        target = cosmosdb.get_cosmos_database(ctx.destination_client, ctx.target_workspace_id, target_id)
        ctx.source_items[database["id"]] = {**database, "type": cosmosdb.COSMOS_DB_DATABASE}
        source_endpoint = cosmosdb.endpoint_url(database)
        target_endpoint = cosmosdb.endpoint_url(target)
        if source_endpoint and target_endpoint:
            ctx.map_alias(source_endpoint, target_endpoint, database["id"])
        for read_property in (cosmosdb.server_fqdn, cosmosdb.database_name):
            source_value, target_value = read_property(database), read_property(target)
            if source_value and target_value:
                ctx.map_alias(source_value, target_value, database["id"])
        if not ctx.plan.include_data:
            continue
        if ctx.already_copied(database["id"], "documents"):
            continue

        if not source_endpoint or not target_endpoint:
            evidence.step(
                "data", EvidenceState.UNKNOWN, "Cosmos endpoint missing; documents were not copied.",
                action="Restore source and target Cosmos endpoints and retry document transfer.",
            )
            warnings.append(
                f"Cosmos DB database '{name}' did not report an endpoint, so its documents "
                "were not copied. Its containers are there and ready for them."
            )
            continue

        ctx.run.update_step(step, f"Copying documents for Cosmos DB database '{name}'")
        progress = _document_progress(ctx, step, name)
        evidence.step("data", EvidenceState.UNKNOWN, "Document copy has not completed.")
        observed: list[CopyOutcome] = []
        try:
            document_warnings = [
                f"Cosmos DB database '{name}': {w}"
                for w in cosmos_transfer.copy_documents(
                    source_endpoint=source_endpoint,
                    source_database=cosmosdb.database_name(database),
                    target_endpoint=target_endpoint,
                    target_database=cosmosdb.database_name(target),
                    tokens=ctx.tokens,
                    on_progress=progress,
                    on_complete=observed.append,
                    **({
                        "target_tokens": ctx.destination_tokens,
                        "max_staging_bytes": SETTINGS.max_staging_bytes,
                        "cancel_requested": lambda: ctx.run.cancelled,
                    } if ctx.plan.paired else {}),
                )
            ]
            warnings.extend(document_warnings)
            # Only written down when every container came across. A partly copied database
            # has to be attempted again, and the writes are upserts, so repeating it converges.
            if not document_warnings:
                ctx.data_copied(
                    database["id"], "documents", outcome=observed[-1] if observed else None,
                )
            else:
                evidence.step(
                    "data", EvidenceState.FAILED, "Not every container completed document transfer.",
                    action="Review the container failures in the migration log and retry the copy.",
                )
        except cosmos_transfer.CosmosTransferError as error:
            evidence.step(
                "data", EvidenceState.FAILED, "Document copy failed.", error=error,
                action="Resolve the document transfer failure and retry.",
            )
            warnings.append(
                f"Documents for Cosmos DB database '{name}' did not copy: {error}. Its "
                "containers are there and ready for them."
            )
        evidence.complete()

    return migrated, warnings


# -------------------------------------------------------------------- phase 5b


def _validate_connection_mappings(ctx: _Context) -> None:
    """Validate operator-supplied connection replacements now that data stores exist.

    Only runs when the operator supplied ``connection_mappings``: with none supplied there is
    nothing to validate, and no phase is shown at all - a rebuild with no explicit mappings
    gets no connection step and no rebuild noise about connections it never touched.

    ``_load_source_references`` (the ``dependencies`` phase) only detects which connections
    are actually referenced; it runs before any data store exists, so a mapping whose source
    path names a lakehouse or warehouse SQL endpoint cannot yet be rewritten through the id
    map. Checking it there would misdiagnose a mapping that is fine but simply hasn't had its
    target created yet. This phase runs the same rewrite-and-verify check once every phase
    that can add to the id map (workspaces through sqldatabases) has finished, so the id map is
    as complete as it will be before anything that could bind the connection - starting with
    mirrored databases, which can bind one directly - is created.

    Nothing is created, adopted by name, or deleted here, in any mode. Ordinary same-tenant
    connections are reused; cross-tenant identities require explicit mappings. No credentials
    or connection definitions are inferred from a tenant-wide source-target scan.
    A mapping that does not hold up is refused and its id map entry withdrawn, so a dependent
    item is refused too rather than built against a stale binding.
    """
    step = "connections"
    ctx.run.start_step(step, "Validating supplied connection mappings")
    ctx.run.raise_if_cancelled()
    if not ctx.plan.connection_mappings:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "No connection mappings supplied")
        return

    warnings: list[str] = []
    for raw_source_id, supplied_target in ctx.plan.connection_mappings.items():
        source_connection_id = raw_source_id.casefold()
        connection = ctx.source_items.get(source_connection_id)
        if not connection or connection.get("type") != "Connection":
            # Not a connection any migrated item or shortcut actually references: nothing to
            # validate or bind.
            continue
        path = (connection.get("connectionDetails") or {}).get("path") or ""
        if source_connection_id in ctx.id_map and ctx.id_map[source_connection_id] != supplied_target:
            ctx.invalidate_mapping(source_connection_id)
        ctx.map_alias(source_connection_id, supplied_target, source_connection_id)
        # A journaled ID is not proof that a tenant-scoped connection still exists, is
        # readable, or targets the expected path. Validate before any consumer uses it.
        mapped_key = next(
            (key for key in ctx.id_map if key.casefold() == source_connection_id), None
        )
        if mapped_key is None:
            continue
        target_id = ctx.id_map[mapped_key]
        rewrite = definitions.build_rewriter(ctx.id_map)
        new_path = rewrite(path) if rewrite else path
        needed = analytics.dangling_references(
            [definitions.part("connection.txt", path)], ctx.id_map, ctx.source_items,
            ignore=(source_connection_id,),
        )
        try:
            target = ctx.destination_client.get(f"connections/{target_id}")
        except FabricError as error:
            ctx.invalidate_mapping(mapped_key)
            ctx.unverified_connections.add(source_connection_id)
            warnings.append(
                f"Connection '{connections.display_name(connection)}' replacement {target_id} "
                f"could not be verified: {error}. Grant access or restore it, then retry."
            )
            continue
        if (
            needed or not connections.matches_replacement(target, connection, new_path)
        ):
            ctx.invalidate_mapping(mapped_key)
            warnings.append(
                f"Connection '{connections.display_name(connection)}' replacement {target_id} no longer "
                "matches its migrated target. Restore the replacement against the migrated "
                "store, then retry; dependent items will not be created with the old binding."
            )
            continue
        if mapped_key != source_connection_id:
            del ctx.id_map[mapped_key]
            ctx.id_map[source_connection_id] = target_id

    ctx.warnings.extend(warnings)
    if warnings:
        ctx.run.finish_step(step, StepStatus.SUCCEEDED, "Some connection mappings need attention", warnings)
    else:
        ctx.run.finish_step(
            step, StepStatus.SUCCEEDED,
            f"Validated {len(ctx.plan.connection_mappings)} connection mapping(s)",
        )


# --------------------------------------------------------------------- phase 6


def _migrate_mirrored_databases(ctx: _Context) -> None:
    """Recreate mirrored databases.

    A mirrored database is a data store with its own SQL analytics endpoint, so it goes with
    the other data stores: a semantic model can read it, and its endpoint has to be in the id
    map before the analytics phase.

    Mirrors are created stopped. Only the explicit start_database_mirrors option authorizes
    the subsequent start action, before shortcuts are reconciled.
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
    else:
        migrated, item_warnings = _migrate_definition_items(
            ctx,
            source_workspace_id=source_id,
            target_workspace_id=ctx.target_workspace_id,
            items=ctx.to_migrate(databases),
            item_type=analytics.MIRRORED_DATABASE,
            id_map=ctx.id_map,
            folder_map=ctx.id_map,
            source_items=ctx.source_items,
            on_progress=progress,
        )
        warnings.extend(item_warnings)

    mapped_mirrors = {result.source_id for result in migrated}
    for database in databases:
        if ctx.already_created(database["id"]) and database["id"] not in mapped_mirrors:
            migrated.append(analytics.MigratedItem(
                source_id=database["id"], target_id=ctx.id_map[database["id"]],
                name=database["displayName"], rebound_parts=0,
            ))
    for result in migrated:
        target = ctx.destination_client.get(
            f"workspaces/{ctx.target_workspace_id}/mirroredDatabases/{result.target_id}"
        )
        source = next((db for db in databases if db["id"] == result.source_id), {})
        source_endpoint = data_stores.mirrored_database_sql_endpoint(source)
        target_endpoint = data_stores.mirrored_database_sql_endpoint(target)
        _map_sql_endpoint(ctx, source_endpoint, target_endpoint, owner=result.source_id)

        _configure_database_mirror(ctx, source, result, warnings)

    if catalogs:
        results, item_warnings = _migrate_definition_items(
            ctx,
            source_workspace_id=source_id,
            target_workspace_id=ctx.target_workspace_id,
            items=ctx.to_migrate(catalogs),
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


def _configure_database_mirror(
    ctx: _Context, source: Mapping[str, Any], result: analytics.MigratedItem,
    warnings: list[str],
) -> None:
    evidence = ctx.resolve_item(source, result.target_id, analytics.MIRRORED_DATABASE)
    ctx.run.raise_if_cancelled()
    # An empty recorded value also clears dormant explanations inherited from older attempts.
    ctx.dormant[result.source_id] = ""
    evidence.step(
        "activation", EvidenceState.UNKNOWN, "Checking destination mirroring state.",
        action="Inspect destination mirroring status before retrying a start.",
    )
    try:
        if (
            not ctx.target_workspace_id
            or ctx.target_workspace_id.casefold() == ctx.plan.source_workspace_id.casefold()
        ):
            raise FabricError("Refusing mirror activation against a missing or source workspace.")
        if ctx.plan.start_database_mirrors:
            mirroring.ensure_running(
                ctx.destination_client, ctx.target_workspace_id, result.target_id,
                check_cancel=ctx.run.raise_if_cancelled,
                on_progress=lambda message: ctx.run.update_step("mirrored", f"{result.name}: {message}"),
            )
            state = "Running"
        else:
            state = mirroring.status(ctx.destination_client, ctx.target_workspace_id, result.target_id)
    except (FabricError, AuthError, TimeoutError) as error:
        message = f"Mirrored database '{result.name}' activation could not be confirmed: {error}"
        action = (
            "Check destination mirroring and resolve the service error before retrying. "
            "A submitted start may still be running; the source mirror was not changed."
        )
        evidence.step("activation", EvidenceState.FAILED, message, action=action, error=error)
        warnings.append(f"{message}. {action}")
        return
    if state == "Running":
        evidence.step(
            "activation", EvidenceState.SUCCEEDED,
            "Destination reports Running; no start was repeated for an already-running mirror.",
        )
        evidence.step(
            "replication", EvidenceState.UNKNOWN,
            "Running does not prove initial replication or table readiness.",
            action="Check replicated tables and initial synchronization in the destination before cutover.",
        )
        warnings.append(
            f"Mirrored database '{result.name}' reports Running. Check initial synchronization "
            "and table availability before cutover; shortcuts can still need a retry while tables arrive."
        )
    else:
        evidence.step(
            "activation", EvidenceState.SKIPPED,
            f"Automatic start was not selected. Destination reports {state}.",
            action="Start mirroring in the destination when ready, or select "
            "Start destination database mirrors on retry. This can create a second active replica.",
        )
        ctx.dormant[result.source_id] = (
            f"reports mirroring status {state} in the destination; replicated table availability "
            "has not been verified. Check mirroring and the target table in the new workspace."
        )
        warnings.append(
            f"Mirrored database '{result.name}' reports {state}; no start was requested. "
            "Start it in the destination when ready, or select the mirror-start option on retry."
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
        adopted = ctx.already_created(item["id"])

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
            if adopted:
                raise ResumeRefused(
                    f"SnowflakeDatabase '{name}' cannot be verified because its database name "
                    "and connection could not be read. Restore access and resume before using it."
                )
            warnings.append(
                f"SnowflakeDatabase '{name}' was not migrated because the database name and "
                "connection it uses could not be read. Recreate it by hand."
            )
            continue

        try:
            payload_parts = [definitions.part("snowflake.json", payload)]
            if ctx.plan.cross_tenant:
                warnings.extend(analytics.validate_cross_tenant_references(
                    payload_parts, source_workspace_id=ctx.plan.source_workspace_id,
                    target_workspace_id=ctx.target_workspace_id, id_map=ctx.id_map,
                    target_client=ctx.destination_client, source_items=ctx.source_items,
                    item_type=analytics.SNOWFLAKE_DATABASE,
                    lifecycle=ctx.lifecycle(item, analytics.SNOWFLAKE_DATABASE),
                ))
            needed = analytics.dangling_references(
                payload_parts, ctx.id_map, ctx.source_items, ignore=(item["id"],)
            )
            if needed:
                raise analytics.StrandedReference(needed)
            rewritten, _ = definitions.rewrite_parts(payload_parts, ctx.id_map)
            payload = definitions.decode_json_part(rewritten[0]["payload"])
            if adopted:
                if item["id"] in ctx.refresh_needed:
                    items_module.update_item_definition(
                        ctx.destination_client, ctx.target_workspace_id, ctx.id_map[item["id"]],
                        [definitions.part(special_items.SNOWFLAKE_PROPERTIES_PART, payload)],
                    )
                    ctx.journal.refresh([item["id"]], required=False)
                    ctx.refresh_needed.discard(item["id"])
                continue
            created = items_module.create_item(
                ctx.destination_client,
                ctx.target_workspace_id,
                name,
                analytics.SNOWFLAKE_DATABASE,
                description=item.get("description") or None,
                creation_payload=payload,
                folder_id=ctx.id_map.get(item.get("folderId", "")),
            )
        except FabricError as error:
            if adopted:
                raise ResumeRefused(
                    f"SnowflakeDatabase '{name}' could not be rebound. Resolve the error and "
                    f"resume before using it: {error}"
                ) from error
            warnings.append(analytics.describe_failure(analytics.SNOWFLAKE_DATABASE, name, error))
            continue

        ctx.id_map[item["id"]] = created["id"]
        ctx.journal.item(item["id"], created["id"], analytics.SNOWFLAKE_DATABASE, name)

    return warnings


# --------------------------------------------------------------------- phase 6


def _migrate_shortcuts_and_endpoints(ctx: _Context) -> None:
    step = "shortcuts"
    ctx.run.start_step(step, "Reconciling shortcuts and syncing SQL endpoints")
    ctx.run.raise_if_cancelled()

    source_lakehouses = data_stores.list_lakehouses(ctx.client, ctx.plan.source_workspace_id)
    if not source_lakehouses and not ctx.kql_databases:
        ctx.run.finish_step(step, StepStatus.SKIPPED, "Nothing references other items")
        return

    warnings: list[str] = []
    shortcuts_completed = 0
    endpoints: list[tuple[str, str, str]] = []

    # Names for the items in the source workspace, so a shortcut pointing at something that
    # did not migrate can say which item that was rather than quoting a GUID.
    source_items = {**ctx.source_items, **{
        item["id"]: item
        for item in list_items(ctx.client, ctx.plan.source_workspace_id)
        if item.get("id")
    }}

    # KQL table shortcuts can target lakehouses, warehouses, or other KQL databases, so they
    # are only safe to create now that every one of those exists.
    for source_db_id, target_db_id, database_name in ctx.kql_databases:
        ctx.run.raise_if_cancelled()
        table_shortcuts = ctx.kql_table_shortcuts.get(source_db_id) or []
        if not table_shortcuts:
            continue

        ctx.run.update_step(step, f"Reconciling table shortcuts for KQL database '{database_name}'")
        completed, shortcut_warnings = shortcuts.copy_table_shortcuts(
            ctx.client,
            ctx.plan.source_workspace_id,
            source_db_id,
            ctx.target_workspace_id,
            target_db_id,
            ctx.id_map,
            shortcuts=table_shortcuts,
            source_items=source_items,
            dormant=ctx.dormant,
            lifecycle=ctx.evidence(source_db_id),
            **ctx.target_kwargs,
            **({"cross_tenant": True} if ctx.plan.cross_tenant else {}),
        )
        shortcuts_completed += completed
        warnings.extend(f"KQL database '{database_name}': {w}" for w in shortcut_warnings)

    for lakehouse in source_lakehouses:
        ctx.run.raise_if_cancelled()
        name = lakehouse["displayName"]
        target_id = ctx.id_map.get(lakehouse["id"])
        if not target_id:
            continue

        ctx.run.update_step(step, f"Reconciling shortcuts for '{name}'")
        completed, shortcut_warnings = shortcuts.copy_shortcuts(
            ctx.client,
            ctx.plan.source_workspace_id,
            lakehouse["id"],
            ctx.target_workspace_id,
            target_id,
            ctx.id_map,
            source_items=source_items,
            dormant=ctx.dormant,
            lifecycle=ctx.evidence(lakehouse["id"]),
            **ctx.target_kwargs,
            **({"cross_tenant": True} if ctx.plan.cross_tenant else {}),
        )
        shortcuts_completed += completed
        warnings.extend(f"Lakehouse '{name}': {w}" for w in shortcut_warnings)

        # The endpoint must re-read OneLake after tables and shortcuts land, otherwise the
        # schema copy below sees an empty database.
        ctx.run.update_step(step, f"Refreshing SQL endpoint for '{name}'")
        target = data_stores.get_lakehouse(ctx.destination_client, ctx.target_workspace_id, target_id)
        endpoint = data_stores.lakehouse_sql_endpoint(target)
        # By now the endpoint exists, which it very often did not when the lakehouse was
        # created. This is the point at which its id and connection string can be recorded,
        # and everything that binds to one is migrated after this phase.
        _map_sql_endpoint(ctx, data_stores.lakehouse_sql_endpoint(lakehouse), endpoint, owner=lakehouse["id"])
        if endpoint.get("id"):
            with ctx.evidence(lakehouse["id"]).operation("endpoint", None):
                refreshed = data_stores.refresh_sql_endpoint_metadata(
                    ctx.destination_client,
                    ctx.target_workspace_id,
                    endpoint["id"],
                    on_progress=_bulk_copy_progress(ctx, step, f"Lakehouse '{name}'"),
                )
                failed_sync = data_stores.sync_failures(refreshed)
                ctx.evidence(lakehouse["id"]).step(
                    "endpoint", EvidenceState.FAILED if failed_sync else EvidenceState.SUCCEEDED,
                    "SQL endpoint refresh reported table failures." if failed_sync
                    else "SQL endpoint metadata refresh completed.",
                    action="Review and retry the failed endpoint table syncs." if failed_sync else "",
                )
            # The refresh reports success while individual tables failed to sync, and a table
            # that did not sync is invisible to the schema deploy that follows.
            warnings.extend(
                f"Lakehouse '{name}' SQL endpoint: {failure}"
                for failure in data_stores.sync_failures(refreshed)
            )
        else:
            ctx.evidence(lakehouse["id"]).step(
                "endpoint", EvidenceState.UNKNOWN, "SQL endpoint identity is missing.",
                action="Wait for the target SQL endpoint and retry its refresh.",
            )

        source_endpoint = data_stores.lakehouse_sql_endpoint(lakehouse).get("connectionString")
        target_endpoint = endpoint.get("connectionString")
        if source_endpoint and target_endpoint:
            endpoints.append((name, source_endpoint, target_endpoint))
            if ctx.plan.paired and ctx.plan.include_data:
                try:
                    expected = {
                        (table.schema or "dbo", table.name)
                        for table in _lakehouse_tables(
                            ctx, lakehouse, schema_enabled=data_stores.is_schema_enabled(lakehouse),
                        )
                    }
                    actual = set(sqlschema.list_base_tables(
                        target_endpoint, name, ctx.destination_tokens,
                    ))
                    missing = expected - actual
                    ctx.evidence(lakehouse["id"]).step(
                        "catalog", EvidenceState.FAILED if missing else EvidenceState.SUCCEEDED,
                        "Tables missing from the destination SQL catalog: "
                        + ", ".join(f"{schema}.{table}" for schema, table in sorted(missing))
                        if missing else "Copied tables are visible in the destination SQL catalog.",
                        action="Repair the destination table catalog, then retry." if missing else "",
                    )
                    if missing:
                        warnings.append(
                            f"Lakehouse '{name}' has copied files but {len(missing)} table(s) are "
                            "not visible in the destination SQL catalog. Refresh the catalog and retry."
                        )
                except (sqlschema.SchemaTransferError, FabricError) as error:
                    ctx.evidence(lakehouse["id"]).step(
                        "catalog", EvidenceState.UNKNOWN, "Destination catalog could not be reconciled.",
                        error=error, action="Restore SQL endpoint access and retry catalog reconciliation.",
                    )
                    warnings.append(f"Lakehouse '{name}' catalog reconciliation failed: {error}")

    # Every endpoint has been refreshed by now, so the schema copies can run together. They
    # are the slow part of this phase: a refreshed endpoint takes minutes to answer, and it
    # takes them whether or not we are also waiting on another one.
    warnings.extend(_transfer_endpoint_schemas(ctx, step, endpoints))

    ctx.warnings.extend(warnings)
    ctx.run.finish_step(
        step, StepStatus.SUCCEEDED, f"Created or reused {shortcuts_completed} shortcut(s)", warnings
    )


def _transfer_endpoint_schemas(
    ctx: _Context,
    step: str,
    endpoints: list[tuple[str, str, str]],
) -> list[str]:
    def transfer(name: str, source_endpoint: str, target_endpoint: str) -> Callable[[], list[str]]:
        def run() -> list[str]:
            ctx.run.raise_if_cancelled()
            evidence = next((
                ctx.evidence(item_id) for item_id, item in ctx.source_items.items()
                if item.get("type") == "Lakehouse" and item.get("displayName") == name
            ), None)
            if evidence:
                evidence.step("schema", EvidenceState.UNKNOWN, "Endpoint schema transfer has not completed.")
            try:
                warnings = sqlschema.transfer_schema(
                    source_server=source_endpoint,
                    target_server=target_endpoint,
                    database=name,
                    principal=ctx.principal,
                    tokens=ctx.tokens,
                    scratch_dir=ctx.scratch_dir / "sql",
                    source_type="Lakehouse",
                    **({
                        "target_tokens": ctx.destination_tokens,
                        "exclude_security": True,
                        "max_staging_bytes": SETTINGS.max_staging_bytes,
                        "cancel_requested": lambda: ctx.run.cancelled,
                        "id_map": ctx.id_map,
                        "source_identifiers": tuple(analytics.reference_identifiers(ctx.source_items)),
                    } if ctx.plan.paired else {}),
                )
            except sqlschema.SchemaTransferError as error:
                if evidence:
                    evidence.step(
                        "schema", EvidenceState.FAILED, "SQL endpoint schema transfer failed.",
                        error=error, action="Resolve the SQL endpoint schema failure and retry.",
                    )
                return [f"SQL endpoint schema for '{name}' did not transfer: {error}"]
            if evidence:
                evidence.step(
                    "schema", EvidenceState.UNKNOWN if warnings else EvidenceState.SUCCEEDED,
                    "Schema diagnostics need review." if warnings else "Endpoint schema transferred.",
                    action="Review the schema diagnostics before cutover." if warnings else "",
                )
                evidence.complete()
            return [f"Lakehouse '{name}' SQL endpoint: {w}" for w in warnings]

        return run

    return concurrency.run_bounded(
        [concurrency.Job(key=name, run=transfer(name, source, target)) for name, source, target in endpoints],
        limit=1 if ctx.plan.paired else SETTINGS.schema_transfer_concurrency,
        on_progress=lambda message: ctx.run.update_step(step, f"Transferring endpoint schema: {message}"),
        noun="schema transfer",
    )


def _migrate_realtime(ctx: _Context) -> None:
    """Recreate eventstreams, KQL querysets, and KQL dashboards.

    All three read from the real-time items built earlier: a queryset and a dashboard target
    an eventhouse cluster URI, and an eventstream routes into lakehouses, eventhouses, and
    other items while sourcing from connections. So this runs after the data stores, the
    eventhouses, and any validated operator-supplied connection mappings.
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
        if ctx.plan.paired and item_type == analytics.EVENTSTREAM:
            for item in items:
                action = f"Eventstream '{item['displayName']}' was not created: {STOPPED_EVENTSTREAM_REASON}."
                ctx.lifecycle(item, item_type).step(
                    "activation", EvidenceState.SKIPPED, "Inactive creation was not established.",
                    action=action,
                )
                warnings.append(action)
            continue
        if not items:
            continue
        results, item_warnings = _migrate_definition_items(
            ctx,
            source_workspace_id=source_id,
            target_workspace_id=ctx.target_workspace_id,
            items=ctx.to_migrate(items),
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
        results, item_warnings = _migrate_definition_items(
            ctx,
            source_workspace_id=source_id,
            target_workspace_id=ctx.target_workspace_id,
            items=ctx.to_migrate(items),
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
        results, item_warnings = _migrate_definition_items(
            ctx,
            source_workspace_id=source_id,
            target_workspace_id=ctx.target_workspace_id,
            items=ctx.to_migrate(items),
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
            ctx.lifecycle(dataflow, analytics.DATAFLOW).step(
                "definition", EvidenceState.SKIPPED, "Dataflow definition was not eligible to migrate.",
                action=reason,
            )
            warnings.append(reason)
            continue
        movable.append(dataflow)
        parts_by_id[dataflow["id"]] = parts or []

    if not movable:
        return 0

    results, item_warnings = _migrate_definition_items(
        ctx,
        source_workspace_id=ctx.plan.source_workspace_id,
        target_workspace_id=ctx.target_workspace_id,
        items=ctx.to_migrate(movable),
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


def _restore_large_semantic_models(ctx: _Context, step: str) -> list[str]:
    """Set rebuilt models back to the large storage format their source used.

    Abf is the usual default, but workspaces can default to PremiumFiles. Read the actual
    target setting rather than assuming every rebuilt model was downgraded. Source reads
    and destination changes use their respective identities; failures remain actionable
    evidence, and a cached region list never substitutes for the service's response.
    https://learn.microsoft.com/power-bi/enterprise/service-premium-large-models
    """
    ctx.run.raise_if_cancelled()
    with powerbi.PowerBiClient(ctx.tokens) as source_pbi:
        try:
            source_models = source_pbi.list_semantic_models(ctx.plan.source_workspace_id)
        except (powerbi.PowerBiError, AuthError) as error:
            message = (
                "Could not read the source semantic models' storage formats, so any that used "
                "the large (PremiumFiles) format could not be checked in the new workspace. "
                f"Check each model's storage format by hand: {error}"
            )
            for source_id, item in list(ctx.source_items.items()):
                if (
                    source_id == item.get("id") and item.get("type") == analytics.SEMANTIC_MODEL
                    and source_id in ctx.id_map
                ):
                    ctx.lifecycle(item, analytics.SEMANTIC_MODEL).step(
                        "storage", EvidenceState.UNKNOWN, "Source storage format could not be read.",
                        action=message, error=error,
                    )
            return [message]

    warnings: list[str] = []
    large_targets: list[tuple[powerbi.SemanticModel, str]] = []
    for model in source_models:
        target_id = ctx.id_map.get(model.id)
        if not target_id:
            continue
        if not model.storage_mode_known:
            message = (
                f"Semantic model '{model.name}' did not report its source storage format. "
                "Check the source and destination format settings; no format was inferred."
            )
            ctx.lifecycle({
                "id": model.id, "displayName": model.name, "type": analytics.SEMANTIC_MODEL,
            }).step(
                "storage", EvidenceState.UNKNOWN, "Source storage format was not reported.", action=message,
            )
            warnings.append(message)
        elif model.is_large:
            large_targets.append((model, target_id))

    if not large_targets:
        return warnings

    with powerbi.PowerBiClient(ctx.destination_tokens) as target_pbi:
        for model, target_id in large_targets:
            ctx.run.raise_if_cancelled()
            evidence = ctx.lifecycle({
                "id": model.id, "displayName": model.name, "type": analytics.SEMANTIC_MODEL,
            })
            evidence.step("storage", EvidenceState.UNKNOWN, "Destination large storage setting not checked.")
            try:
                current = target_pbi.get_semantic_model(ctx.target_workspace_id, target_id)
                if current is not None and current.is_large:
                    evidence.step(
                        "storage", EvidenceState.SUCCEEDED, "Destination already reports PremiumFiles.",
                    )
                    continue
                ctx.run.update_step(step, f"Restoring large storage format for '{model.name}'")
                target_model = powerbi.SemanticModel(
                    id=target_id,
                    name=model.name,
                    storage_mode=current.storage_mode if current else "",
                    content_provider=model.content_provider,
                )
                target_pbi.convert(
                    ctx.target_workspace_id,
                    target_model,
                    powerbi.LARGE,
                    on_progress=lambda message: ctx.run.update_step(step, message),
                    check_cancelled=ctx.run.raise_if_cancelled,
                )
                evidence.step(
                    "storage", EvidenceState.SUCCEEDED, "Destination reports targetStorageMode=PremiumFiles.",
                )
            except (powerbi.PowerBiError, AuthError) as error:
                message = (
                    f"Semantic model '{model.name}' requires large (PremiumFiles) storage, but "
                    f"the destination setting could not be confirmed or restored: {error}. "
                    "Re-enable large storage manually after checking destination ownership "
                    "and capacity support."
                )
                evidence.step(
                    "storage", EvidenceState.FAILED, "Large storage setting could not be restored.",
                    action=message, error=error,
                )
                warnings.append(message)
    return warnings


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

    migrated_models, model_warnings = _migrate_definition_items(
        ctx,
        source_workspace_id=source_id,
        target_workspace_id=ctx.target_workspace_id,
        items=ctx.to_migrate(models),
        item_type=analytics.SEMANTIC_MODEL,
        id_map=ctx.id_map,
        folder_map=ctx.id_map,
        source_items=ctx.source_items,
        on_progress=progress,
    )
    warnings.extend(model_warnings)

    # Definition import does not preserve the source's storage setting. Confirm or restore
    # PremiumFiles where needed without assuming the target workspace's default format.
    if models:
        warnings.extend(_restore_large_semantic_models(ctx, step))

    ctx.run.raise_if_cancelled()

    # Reports run in a second pass so every model id is already in the map above.
    migrated_reports, report_warnings = _migrate_definition_items(
        ctx,
        source_workspace_id=source_id,
        target_workspace_id=ctx.target_workspace_id,
        items=ctx.to_migrate(reports),
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

    Source-bound connections must already have replacements. Genuinely external connections
    remain tenant scoped and the ones each item binds are checked.
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

        results, item_warnings = _migrate_definition_items(
            ctx,
            source_workspace_id=source_id,
            target_workspace_id=ctx.target_workspace_id,
            items=ctx.to_migrate(ordered),
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
        refresh = job["id"] in ctx.refresh_needed
        evidence = ctx.lifecycle(job, airflow.APACHE_AIRFLOW_JOB)
        adopted = ctx.already_created(job["id"])
        evidence.step("rebind", EvidenceState.UNKNOWN, "Airflow reference preflight not completed.")

        try:
            definition = items_module.get_item_definition(
                ctx.client, ctx.plan.source_workspace_id, job["id"]
            )
            parts = definitions.strip_part(definition.get("parts") or [], airflow.PLATFORM_PART)
            configuration_messages = airflow.configuration_warnings(parts, name, lifecycle=evidence)
            airflow.preflight_references(
                parts,
                source_job_id=job["id"],
                job_name=name,
                id_map=ctx.id_map,
                source_items=ctx.source_items,
                **({"cross_tenant": True} if ctx.plan.cross_tenant else {}),
            )
            prepared_files = airflow.preflight_files(
                ctx.client,
                source_workspace_id=ctx.plan.source_workspace_id,
                source_job_id=job["id"],
                job_name=name,
                id_map=ctx.id_map,
                source_items=ctx.source_items,
                on_progress=progress,
                **ctx.target_kwargs,
                **({"cross_tenant": True} if ctx.plan.cross_tenant else {}),
            )
            if ctx.plan.cross_tenant:
                warnings.extend(analytics.validate_cross_tenant_references(
                    parts, source_workspace_id=ctx.plan.source_workspace_id,
                    target_workspace_id=ctx.target_workspace_id, id_map=ctx.id_map,
                    target_client=ctx.destination_client, source_items=ctx.source_items,
                    item_type=airflow.APACHE_AIRFLOW_JOB, lifecycle=evidence,
                ))
            rewritten, _ = definitions.rewrite_parts(parts, ctx.id_map)
            rewritten = airflow.retarget_location(rewritten, ctx.plan.capacity_display_region)
            evidence.references(analytics.referenced_item_ids(
                [*parts, *(definitions.part(path, content) for path, content in prepared_files)],
                ctx.source_items, id_map=ctx.id_map,
            ))
            evidence.step(
                "rebind", EvidenceState.SUCCEEDED, "Airflow configuration and file references checked.",
            )

            if ctx.already_created(job["id"]):
                target_id = ctx.id_map[job["id"]]
                if refresh:
                    items_module.update_item_definition(
                        ctx.destination_client, ctx.target_workspace_id, target_id, rewritten
                    )
            else:
                created = items_module.create_item(
                    ctx.destination_client,
                    ctx.target_workspace_id,
                    name,
                    airflow.APACHE_AIRFLOW_JOB,
                    description=job.get("description") or None,
                    parts=rewritten,
                    folder_id=ctx.id_map.get(job.get("folderId", "")),
                )
                target_id = created["id"]
            ctx.resolve_item(
                job, target_id, airflow.APACHE_AIRFLOW_JOB,
                disposition=Disposition.REFRESHED if refresh else (
                    Disposition.ADOPTED if adopted else Disposition.CREATED
                ),
            )
            if not adopted or refresh:
                evidence.step("definition", EvidenceState.SUCCEEDED, "Airflow configuration applied.")
        except FabricError as error:
            evidence.step(
                "definition", EvidenceState.FAILED, "Airflow migration failed.", error=error,
                action="Resolve the reported configuration or reference problem and retry.",
            )
            if isinstance(error, analytics.StrandedReference):
                evidence.add_references((), error.needed)
            if refresh and ctx.already_created(job["id"]):
                raise ResumeRefused(
                    f"Apache Airflow job '{name}' could not be rebound. Resolve the error and "
                    f"resume before using it: {error}"
                ) from error
            warnings.append(analytics.describe_failure(airflow.APACHE_AIRFLOW_JOB, name, error))
            continue

        migrated += 1
        warnings.extend(configuration_messages)
        if not refresh and ctx.already_copied(job["id"], "airflow-files"):
            continue

        copied, file_warnings = airflow.copy_files(
            ctx.client,
            source_workspace_id=ctx.plan.source_workspace_id,
            source_job_id=job["id"],
            target_workspace_id=ctx.target_workspace_id,
            target_job_id=target_id,
            job_name=name,
            on_progress=progress,
            prepared_files=prepared_files,
            **ctx.target_kwargs,
            **({
                "cross_tenant": True, "id_map": ctx.id_map, "source_items": ctx.source_items,
            } if ctx.plan.cross_tenant else {}),
        )
        warnings.extend(file_warnings)
        evidence.step(
            "files", EvidenceState.FAILED if file_warnings else EvidenceState.SUCCEEDED,
            f"Uploaded {copied} Airflow file(s)." if not file_warnings
            else "Not every Airflow file was uploaded.",
            action="Review file upload failures and retry the Airflow job." if file_warnings else "",
        )
        if not file_warnings:
            ctx.data_copied(job["id"], "airflow-files")
            ctx.journal.refresh([job["id"]], required=False)
            ctx.refresh_needed.discard(job["id"])
        elif refresh:
            raise ResumeRefused(
                f"Apache Airflow job '{name}' files could not be rebound. Resolve the errors "
                "and resume before using it: " + "; ".join(file_warnings)
            )
        if not copied and not file_warnings:
            evidence.step(
                "files", EvidenceState.SUCCEEDED, "File inventory was empty; no DAG files to upload.",
            )
            warnings.append(
                f"Apache Airflow job '{name}' had no files to copy, so the new job has no DAGs."
            )
        evidence.complete()

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

    migrated, warnings = _migrate_definition_items(
        ctx,
        source_workspace_id=source_id,
        target_workspace_id=ctx.target_workspace_id,
        items=ctx.to_migrate(reflexes),
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
    known = connections.connections_by_id(ctx.destination_client)
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


# -------------------------------------------------------------------- phase 14


def _scan_connection_advisories(ctx: _Context) -> None:
    """Advisory scan: which tenant-visible connections still point at the source workspace.

    Runs once ``id_map`` is as complete as it will get - after every phase that can add to it,
    and before the workspace-level ``permissions``/``cleanup`` steps that do not touch it
    either way - so a matched connection's preview of its repointed path is as accurate as
    this run can make it. See ``fabshuffle.fabric.connection_advisory`` for what "matched"
    means and why a bare catalog name is never enough on its own.

    Reporting only. Nothing here is created, adopted by name, deleted, or granted, and the
    result never blocks or gates anything already migrated - it cannot, because it runs after
    every phase that migrates something. A read failure narrows what the scan could check; it
    never fails the step, because an incomplete answer is still worth having.

    Honour cancellation between reads. An interrupted scan leaves missing or stale evidence,
    not a fresh network scan started from the cancellation handler.
    """
    step = "connectionadvisory"
    ctx.run.start_step(step, "Scanning tenant connections for source workspace references")
    scan = connection_advisory.scan_source_connections(
        ctx.client,
        source_workspace_id=ctx.plan.source_workspace_id,
        source_workspace_name=ctx.plan.source_workspace_name,
        target_workspace_id=ctx.target_workspace_id,
        attempt_id=ctx.run.id,
        id_map=dict(ctx.id_map),
        check_cancel=ctx.run.raise_if_cancelled,
    )
    payload = scan.as_dict()
    ctx.journal.connection_advisory(payload)
    ctx.run.set_connection_advisory(payload)
    ctx.run.finish_step(
        step, StepStatus.SUCCEEDED, scan.message,
        [scan.message, scan.action] if scan.action else [],
    )


# --------------------------------------------------------------------- phase 8


def _copy_permissions(ctx: _Context) -> None:
    step = "permissions"
    if not ctx.plan.copy_permissions or ctx.plan.cross_tenant:
        ctx.run.add_step(step, "Copying workspace permissions")
        ctx.run.finish_step(step, StepStatus.SKIPPED, "Disabled for this run")
        return

    ctx.run.start_step(step, "Copying workspace permissions")
    ctx.run.raise_if_cancelled()

    # Admins were granted when the workspace was created; this pass adds everyone else.
    assignments = ctx.source_role_assignments or workspaces.list_role_assignments(
        ctx.client, ctx.plan.source_workspace_id
    )
    warnings = workspaces.copy_role_assignments(ctx.destination_client, assignments, ctx.target_workspace_id)
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
    warnings: list[str] = []

    scratch = run.scratch_workspace
    if scratch and scratch.get("id") and run.plan.get("source_tenant_id"):
        binding = journal_module.tenant_binding(run.plan)
        if (
            client.tenant_id() != binding["target_tenant_id"]
            or client.application_id.casefold() != binding["target_client_id"]
        ):
            raise journal_module.TenantBindingError(
                "Scratch cleanup requires the recorded destination tenant and application."
            )
        replay = journal_module.read(SETTINGS.journal_for_plan(run.id, run.plan))
        journal_module.validate_replay_binding(replay, **binding)
        owned_id = replay.owned_workspace_id(
            "scratch", tenant_id=binding["target_tenant_id"], client_id=binding["target_client_id"],
        )
        if owned_id != scratch["id"]:
            raise journal_module.TenantBindingError(
                "The scratch workspace is not owned by this run's destination journal."
            )
    run.start_step(step, "Removing temporary artifacts")
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
    target_client: FabricClient | None = None,
    source_tenant_id: str = "",
    target_tenant_id: str = "",
    source_client_id: str = "",
    target_client_id: str = "",
    write_freeze_confirmed: bool = False,
    start_database_mirrors: bool = False,
    connection_mappings: Mapping[str, str] | None = None,
    reference_mappings: list[dict[str, str]] | None = None,
) -> MigrationPlan:
    paired = target_client is not None
    if paired != bool(source_tenant_id) or paired != bool(target_tenant_id):
        raise ValueError("Separate planning clients require both source and target tenant IDs.")
    if paired:
        source_tenant_id = str(UUID(source_tenant_id))
        target_tenant_id = str(UUID(target_tenant_id))
    cross_tenant = paired and source_tenant_id != target_tenant_id
    if paired and strategy is Strategy.REASSIGN:
        raise ValueError(
            "Paired plans recreate the workspace; reassignment requires a single-principal sign-in."
        )
    capacity = workspaces.get_capacity(target_client if target_client is not None else client, capacity_id)
    workspace = workspaces.get_workspace(client, source_workspace_id)
    region = workspaces.capacity_region(capacity)
    source_name = workspace["displayName"]

    # Capacity size caps Spark pools, starter pool sizing, and semantic model memory, so a
    # mismatch is worth knowing about before anything is created.
    capacity_warning = workspaces.compare_capacities(
        workspaces.workspace_capacity_sku(client, workspace), capacity.get("sku") or ""
    )
    if reference_mappings:
        migration_refs.resolve(
            client, target_client if target_client is not None else client, reference_mappings,
            migrating_workspace_id=source_workspace_id,
        )

    if strategy is None:
        strategy = assess_workspace(
            list_items(client, source_workspace_id), force_rebuild=paired,
        ).strategy

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
        source_capacity_id_missing=workspaces.unresolved_capacity_assignment(workspace),
        include_files=include_files,
        include_data=include_data,
        copy_permissions=copy_permissions and not cross_tenant,
        source_tenant_id=source_tenant_id,
        target_tenant_id=target_tenant_id,
        source_client_id=source_client_id,
        target_client_id=target_client_id,
        write_freeze_confirmed=write_freeze_confirmed,
        start_database_mirrors=start_database_mirrors,
        connection_mappings=dict(connection_mappings or {}),
        reference_mappings=list(reference_mappings or []),
    )


#: The rebuild, in the order the module docstring explains. Named rather than called inline so
#: that each phase can be recorded as it starts and finishes, which is what lets a resume know
#: where the last attempt stopped. The ids match the step ids shown on screen.
_REBUILD_PHASES: tuple[tuple[str, Callable[[_Context], None]], ...] = (
    ("assessment", _report_unsupported_items),
    ("dependencies", _check_dependencies),
    ("workspaces", _create_workspaces),
    ("eventhouses", _migrate_eventhouses),
    ("lakehouses", _migrate_lakehouses),
    ("warehouses", _migrate_warehouses),
    ("sqldatabases", _migrate_sql_databases),
    ("connections", _validate_connection_mappings),
    ("mirrored", _migrate_mirrored_databases),
    ("shortcuts", _migrate_shortcuts_and_endpoints),
    ("realtime", _migrate_realtime),
    ("engineering", _migrate_engineering),
    ("analytics", _migrate_reports_and_models),
    ("orchestration", _migrate_orchestration),
    ("reflexes", _migrate_reflexes),
    ("connectionadvisory", _scan_connection_advisories),
    ("permissions", _copy_permissions),
)


__all__ = [
    "DependencyReport",
    "MigrationPlan",
    "build_plan",
    "cleanup_run",
    "default_target_name",
    "dependency_warnings",
    "run_migration",
]
