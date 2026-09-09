"""FastAPI application backing the Fab Shuffle wizard."""

from __future__ import annotations

import asyncio
import json
import logging
import queue
import re
import secrets
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

from fabshuffle import __version__, journal, recovery
from fabshuffle.auth import AuthError, ServicePrincipal, TokenProvider
from fabshuffle.config import SETTINGS
from fabshuffle.fabric import (
    analytics,
    connection_advisory,
    connections,
    definitions,
    migration_refs,
    relations,
    workspaces,
)
from fabshuffle.fabric.client import FabricApiError, FabricClient, FabricError
from fabshuffle.fabric.items import get_item_definition, list_items
from fabshuffle.fabric.powerbi import PowerBiClient, PowerBiError
from fabshuffle.fabric.support import (
    Strategy,
    assess_workspace,
    supports_large_semantic_models,
)
from fabshuffle.lifecycle import ItemOutcome, contract_for, readiness_report
from fabshuffle.orchestrator import (
    MigrationPlan,
    _plan_record,
    build_plan,
    cleanup_run,
    connections_lookup_script,
    default_target_name,
    dependency_warnings,
    grant_script,
    plan_from_journal,
    portal_instructions,
    run_migration,
)
from fabshuffle.run import REGISTRY, MigrationRun, RunConflict, RunStatus

logger = logging.getLogger(__name__)

WEB_DIR = Path(__file__).parent
STATIC_DIR = WEB_DIR / "static"
TEMPLATES_DIR = WEB_DIR / "templates"

SESSION_HEADER = "X-Fab-Shuffle-Session"


# --------------------------------------------------------------------- sessions


@dataclass(frozen=True)
class Session:
    """Immutable source and optional destination sign-ins, pinned for each attempt."""

    id: str
    principal: ServicePrincipal
    tokens: TokenProvider
    target_tokens: TokenProvider | None = None
    source_tenant_id: str = ""
    target_tenant_id: str = ""

    @property
    def destination_tokens(self) -> TokenProvider:
        return self.target_tokens if self.target_tokens is not None else self.tokens

    @property
    def paired(self) -> bool:
        return self.target_tokens is not None

    @property
    def cross_tenant(self) -> bool:
        return self.paired and self.source_tenant_id != self.target_tenant_id


class SessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, Session] = {}
        self._lock = threading.Lock()

    def create(
        self, principal: ServicePrincipal, tokens: TokenProvider, *,
        target_tokens: TokenProvider | None = None,
    ) -> Session:
        source_tenant_id = target_tenant_id = ""
        if target_tokens is not None:
            source_tenant_id = _endpoint_tenant_id(tokens, "Source")
            target_tenant_id = _endpoint_tenant_id(target_tokens, "Destination")
        session = Session(
            id=secrets.token_urlsafe(32), principal=principal, tokens=tokens,
            target_tokens=target_tokens, source_tenant_id=source_tenant_id,
            target_tenant_id=target_tenant_id,
        )
        with self._lock:
            self._sessions[session.id] = session
        return session

    def get(self, session_id: str | None) -> Session | None:
        if not session_id:
            return None
        with self._lock:
            return self._sessions.get(session_id)

    def drop(self, session_id: str) -> None:
        with self._lock:
            self._sessions.pop(session_id, None)


SESSIONS = SessionStore()


def _endpoint_tenant_id(tokens: TokenProvider, side: str) -> str:
    try:
        return tokens.tenant_id()
    except AuthError as error:
        raise AuthError(f"{side} tenant identification failed: {error}") from error


def require_session(
    session_id: str | None = Header(default=None, alias=SESSION_HEADER),
) -> Session:
    session = SESSIONS.get(session_id)
    if not session:
        raise HTTPException(status_code=401, detail="Sign in with a service principal first")
    return session


def require_execution_session(session: Session = Depends(require_session)) -> Session:
    return session


def require_legacy_session(session: Session = Depends(require_session)) -> Session:
    if session.paired:
        raise HTTPException(
            status_code=409,
            detail="Global scratch cleanup and copying source admins are disabled for paired sign-ins. "
            "Use the migration's own cleanup action, and grant destination access in its tenant.",
        )
    return session


@contextmanager
def _planning_clients(session: Session) -> Iterator[tuple[FabricClient, FabricClient]]:
    with FabricClient(session.tokens) as source:
        if session.paired:
            with FabricClient(session.destination_tokens) as target:
                yield source, target
        else:
            yield source, source


def _tenant_plan_inputs(session: Session, target: FabricClient) -> dict[str, Any]:
    if not session.paired:
        return {}
    return {
        "target_client": target,
        "source_tenant_id": session.source_tenant_id,
        "target_tenant_id": session.target_tenant_id,
        "source_client_id": session.principal.client_id,
        "target_client_id": session.destination_tokens.principal.client_id,
    }


def _run_conflict(error: RunConflict) -> HTTPException:
    return HTTPException(
        status_code=409, detail={"message": str(error), "runId": error.run_id}
    )


def _start_attempt(
    session: Session, plan: MigrationPlan, *, cleanup: bool, prior: journal.Replay | None = None
) -> MigrationRun:
    if session.paired:
        plan.copy_permissions = False
    record = _plan_record(plan)
    _require_identity(session, record)
    if session.paired and plan.strategy is not Strategy.REBUILD:
        raise HTTPException(status_code=409, detail="Paired migration must rebuild the workspace.")
    if session.paired and (plan.include_data or plan.include_files) and not plan.write_freeze_confirmed:
        raise HTTPException(
            status_code=409,
            detail="Pause source writes and confirm the write freeze before copying data or files "
            "with separate destination credentials, or turn off both copy options.",
        )
    if plan.execution_blocker:
        raise HTTPException(status_code=409, detail=plan.execution_blocker)
    run = MigrationRun(
        source_workspace_name=plan.source_workspace_name, capacity_name=plan.capacity_name
    )
    registry = REGISTRY
    try:
        prior = registry.admit(
            run, directory=_session_directory(session), plan=record, cleanup=cleanup, prior=prior
        )
    except RunConflict as error:
        raise _run_conflict(error) from error
    except journal.TenantBindingError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error

    def failed(error: Exception) -> None:
        logger.exception("Migration attempt %s could not run", run.id)
        try:
            journal.Journal(_session_journal(session, run.id)).finished("failed", str(error))
        except Exception:
            logger.exception("Could not record failure for migration attempt %s", run.id)
        run.mark_finished(RunStatus.FAILED, str(error))

    def work() -> None:
        try:
            kwargs: dict[str, Any] = {"cleanup": cleanup}
            if prior is not None:
                kwargs["prior"] = prior
            if session.paired:
                kwargs["target_principal"] = session.destination_tokens.principal
            run_migration(run, session.principal, plan, **kwargs)
        except Exception as error:
            failed(error)
        finally:
            registry.release(run.id)

    try:
        threading.Thread(target=work, name=f"fab-shuffle-{run.id}", daemon=True).start()
    except Exception as error:
        failed(error)
        registry.release(run.id)
        raise HTTPException(status_code=500, detail=f"Could not start migration: {error}") from error
    return run


# ---------------------------------------------------------------------- schemas


class PrincipalCredentials(BaseModel):
    tenant_id: str = Field(min_length=1)
    client_id: str = Field(min_length=1)
    client_secret: str = Field(min_length=1, repr=False)

    def principal(self) -> ServicePrincipal:
        return ServicePrincipal(
            tenant_id=self.tenant_id.strip(), client_id=self.client_id.strip(),
            client_secret=self.client_secret,
        )


class LoginRequest(PrincipalCredentials):
    destination: PrincipalCredentials | None = None


class RestoreAccessRequest(BaseModel):
    """Copy the admins of one workspace onto another."""

    source_workspace_id: str = Field(min_length=1)
    target_workspace_id: str = Field(min_length=1)


class ConfirmSavedAction(BaseModel):
    model_config = ConfigDict(extra="forbid")
    confirmed: StrictBool


class ConfirmFullRestart(ConfirmSavedAction):
    target_workspace_id: str


class StartRunRequest(BaseModel):
    capacity_id: str = Field(min_length=1)
    source_workspace_id: str = Field(min_length=1)
    target_workspace_name: str | None = None
    # Omit to let Fab Shuffle choose; send "rebuild" to force a full rebuild of a
    # Power BI only workspace instead of reassigning it.
    strategy: Strategy | None = None
    include_data: bool = True
    include_files: bool = True
    copy_permissions: bool = True
    cleanup_when_done: bool = True
    write_freeze_confirmed: bool = False
    start_database_mirrors: StrictBool = False
    connection_mappings: dict[str, str] = Field(default_factory=dict, max_length=1000)
    reference_mappings: list[dict[str, str]] = Field(default_factory=list, max_length=1000)

    @field_validator("connection_mappings")
    @classmethod
    def connection_ids(cls, value: dict[str, str]) -> dict[str, str]:
        result: dict[str, str] = {}
        for source, target in value.items():
            try:
                source, target = str(UUID(source.strip())), str(UUID(target.strip()))
            except ValueError as error:
                raise ValueError(
                    "Connection mappings require source and destination connection GUIDs."
                ) from error
            if source in result and result[source] != target:
                raise ValueError("Each source connection must have only one destination mapping.")
            result[source] = target
        return result

    @field_validator("reference_mappings")
    @classmethod
    def item_ids(cls, value: list[dict[str, str]]) -> list[dict[str, str]]:
        fields = {"source_workspace_id", "source_item_id", "target_workspace_id", "target_item_id"}
        result = []
        for entry in value:
            if set(entry) != fields:
                raise ValueError(
                    "External mappings require exactly source and destination workspace and item IDs."
                )
            try:
                result.append({key: str(UUID(entry[key].strip())) for key in fields})
            except ValueError as error:
                raise ValueError("External mappings require workspace and item GUIDs.") from error
        return result

    def plan_options(self) -> dict[str, Any]:
        return self.model_dump(exclude={"cleanup_when_done"})


class ResumeRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    start_database_mirrors: StrictBool = False
    connection_mappings: dict[str, str] | None = Field(default=None, max_length=1000)
    reference_mappings: list[dict[str, str]] | None = Field(default=None, max_length=1000)

    @field_validator("connection_mappings")
    @classmethod
    def connection_ids(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        return StartRunRequest.connection_ids(value) if value is not None else None

    @field_validator("reference_mappings")
    @classmethod
    def item_ids(cls, value: list[dict[str, str]] | None) -> list[dict[str, str]] | None:
        return StartRunRequest.item_ids(value) if value is not None else None

    def apply(self, plan: MigrationPlan) -> MigrationPlan:
        return replace(plan, **self.model_dump(exclude_none=True))


# ------------------------------------------------------------------------- app


def create_app() -> FastAPI:
    app = FastAPI(title="Fab Shuffle", version=__version__, docs_url="/api/docs")
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(TEMPLATES_DIR / "index.html")

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    # ------------------------------------------------------------------ login

    @app.post("/api/login")
    async def login(body: LoginRequest) -> dict[str, Any]:
        principal = body.principal()
        tokens = TokenProvider(principal)
        try:
            await asyncio.to_thread(tokens.verify)
        except AuthError as error:
            detail = f"Source sign-in failed: {error}" if body.destination else str(error)
            raise HTTPException(status_code=401, detail=detail) from error

        target_tokens = None
        if body.destination is not None:
            target_tokens = TokenProvider(body.destination.principal())
            try:
                await asyncio.to_thread(target_tokens.verify)
            except AuthError as error:
                raise HTTPException(status_code=401, detail=f"Destination sign-in failed: {error}") from error
        try:
            session = await asyncio.to_thread(
                SESSIONS.create, principal, tokens, target_tokens=target_tokens,
            )
        except AuthError as error:
            raise HTTPException(status_code=401, detail=str(error)) from error
        result: dict[str, Any] = {"sessionId": session.id, "principal": principal.redacted()}
        if target_tokens is not None:
            result.update(
                destinationPrincipal=target_tokens.principal.redacted(),
                sourceTenantId=session.source_tenant_id, targetTenantId=session.target_tenant_id,
                paired=True, crossTenant=session.cross_tenant,
            )
        return result

    @app.post("/api/logout")
    async def logout(session: Session = Depends(require_session)) -> dict[str, bool]:
        SESSIONS.drop(session.id)
        return {"ok": True}

    # -------------------------------------------------------------- discovery

    @app.get("/api/capacities")
    async def list_capacities(session: Session = Depends(require_session)) -> dict[str, Any]:
        def work() -> list[dict[str, Any]]:
            with FabricClient(session.destination_tokens) as client:
                return [
                    {
                        "id": capacity["id"],
                        "displayName": capacity.get("displayName"),
                        "region": capacity.get("region"),
                        "sku": capacity.get("sku"),
                        "state": capacity.get("state"),
                    }
                    for capacity in workspaces.list_capacities(client)
                ]

        return {"capacities": await _run_fabric(work)}

    @app.get("/api/workspaces")
    async def list_workspaces(session: Session = Depends(require_session)) -> dict[str, Any]:
        def work() -> list[dict[str, Any]]:
            with FabricClient(session.tokens) as client:
                return [
                    {
                        "id": workspace["id"],
                        "displayName": workspace.get("displayName"),
                        "capacityId": workspace.get("capacityId"),
                        "capacityRegion": workspace.get("capacityRegion"),
                    }
                    for workspace in workspaces.list_workspaces(client)
                    if workspace.get("type") != "AdminWorkspace"
                ]

        return {"workspaces": await _run_fabric(work)}

    @app.get("/api/connections")
    async def connections(
        side: Literal["source", "target"] = "target",
        session: Session = Depends(require_session),
    ) -> dict[str, Any]:
        def work() -> list[dict[str, Any]]:
            tokens = session.tokens if side == "source" else session.destination_tokens
            with FabricClient(tokens) as client:
                # Return only selector metadata, never connection paths or credentials.
                # https://learn.microsoft.com/rest/api/fabric/core/connections/list-connections
                return [
                    {
                        "id": str(entry.get("id", ""))[:128],
                        "displayName": str(entry.get("displayName", ""))[:256],
                        "connectivityType": str(entry.get("connectivityType", ""))[:64],
                    }
                    for entry in client.list_all("connections")[:1000] if entry.get("id")
                ]
        return {"side": side, "connections": await _run_fabric(work)}

    @app.get("/api/preview")
    async def preview(
        capacity_id: str,
        source_workspace_id: str,
        strategy: Literal["rebuild"] | None = None,
        session: Session = Depends(require_session),
    ) -> dict[str, Any]:
        """Summarise what the migration would create before the operator commits."""

        def work() -> dict[str, Any]:
            with _planning_clients(session) as (client, target):
                plan = build_plan(
                    client,
                    capacity_id=capacity_id,
                    source_workspace_id=source_workspace_id,
                    copy_permissions=not session.paired,
                    **({"strategy": Strategy.REBUILD} if strategy else {}),
                    **_tenant_plan_inputs(session, target),
                )
                assessment = assess_workspace(
                    list_items(client, source_workspace_id), force_rebuild=plan.strategy is Strategy.REBUILD,
                    require_stopped=session.paired,
                )

                result: dict[str, Any] = {
                    "targetWorkspaceName": plan.target_workspace_name,
                    "capacityRegion": plan.capacity_region,
                    "capacityName": plan.capacity_name,
                    "sourceWorkspaceName": plan.source_workspace_name,
                    "strategy": plan.strategy.value,
                    "unsupported": [item.as_dict() for item in assessment.unsupported],
                    "unsupportedItemTypes": assessment.unsupported_types,
                    "unsupportedSummary": assessment.grouped_messages(),
                    "capacityWarning": plan.capacity_warning,
                    "largeSemanticModels": [],
                    "blockers": [],
                }
                if session.paired:
                    result.update(
                        paired=True, crossTenant=session.cross_tenant, sourceTenantId=plan.source_tenant_id,
                        targetTenantId=plan.target_tenant_id, copyPermissions=plan.copy_permissions,
                        sourceClientId=plan.source_client_id, targetClientId=plan.target_client_id,
                        assessmentNotice=(
                            "Counts describe rebuild candidates, not qualified cross-tenant support. "
                            "Re-check destination mappings below. "
                            "Validate copied data and items before cutover."
                        ),
                    )

                if plan.strategy is Strategy.REASSIGN:
                    result["counts"] = []
                    result["migratedTotal"] = 0
                    result.update(_semantic_model_preview(session, source_workspace_id, plan))
                    return result

                # Counted from the assessment rather than by listing each type again: it costs
                # nothing, covers every supported type automatically, and cannot disagree with
                # what the migration then does.
                result["counts"] = assessment.migrated_counts()
                result["migratedTotal"] = assessment.migrated_total
                return result

        return await _run_fabric(work)

    @app.get("/api/preview/dependencies")
    async def preview_dependencies(
        source_workspace_id: str,
        strategy: Literal["rebuild"] | None = None,
        session: Session = Depends(require_session),
    ) -> dict[str, Any]:
        """The dependency check, split out because it is much slower than the rest of the preview.

        It walks the relations API once per item and then reads every connection in the tenant,
        so folding it into the preview left the review screen blank for long enough to look
        stuck. The front end shows the rest of the review first and fills this in when it lands.
        """

        def work() -> dict[str, Any]:
            with FabricClient(session.tokens) as client:
                assessment = assess_workspace(
                    list_items(client, source_workspace_id), force_rebuild=session.paired or bool(strategy),
                    require_stopped=session.paired,
                )
                if assessment.strategy is Strategy.REASSIGN:
                    # Nothing is rebuilt, so no reference has to be rewritten.
                    return {"dependencies": [], "connectionAccess": None}

                report = _dependency_report(
                    client,
                    source_workspace_id=source_workspace_id,
                    migrated=assessment.migrated,
                    client_id=session.principal.client_id,
                    object_id=_object_id(session),
                    tenant_id=session.principal.tenant_id,
                )
                return {
                    "dependencies": report["dependencies"],
                    "connectionAccess": report["connectionAccess"],
                }

        result = await _run_fabric(work)
        if session.paired:
            result.update(
                paired=True, sourceTenantId=session.source_tenant_id,
                targetTenantId=session.target_tenant_id, connectionAccessScope="source",
                blockers=[],
                assessmentNotice=(
                    "This is source-side dependency inventory only. Source connection access "
                    "does not establish destination connection access or migration readiness."
                ),
            )
        return result

    @app.post("/api/preview/dependencies")
    async def preview_destination_dependencies(
        body: StartRunRequest, session: Session = Depends(require_session),
    ) -> dict[str, Any]:
        def work() -> dict[str, Any]:
            with _planning_clients(session) as (client, target):
                plan = build_plan(client, **body.plan_options(), **_tenant_plan_inputs(session, target))
                return _paired_dependency_report(session, client, target, plan)
        return await _run_fabric(work)

    # ------------------------------------------------------------------- runs

    @app.post("/api/runs")
    async def start_run(
        body: StartRunRequest,
        session: Session = Depends(require_execution_session),
    ) -> dict[str, Any]:
        def prepare() -> MigrationPlan:
            with _planning_clients(session) as (client, target):
                return build_plan(
                    client, **body.plan_options(), **_tenant_plan_inputs(session, target),
                )

        plan = await _run_fabric(prepare)
        run = _start_attempt(
            session, plan, cleanup=body.cleanup_when_done,
        )

        return {"runId": run.id, "plan": _plan_dict(plan)}

    @app.get("/api/scratch-workspaces")
    async def list_scratch(session: Session = Depends(require_legacy_session)) -> dict[str, Any]:
        """Scratch workspaces left behind by runs this process no longer knows about."""

        def work() -> list[dict[str, Any]]:
            with FabricClient(session.tokens) as client:
                return [
                    {"id": workspace["id"], "displayName": workspace.get("displayName")}
                    for workspace in workspaces.list_scratch_workspaces(client)
                ]

        return {"workspaces": await _run_fabric(work)}

    @app.post("/api/scratch-workspaces/cleanup")
    async def cleanup_scratch(session: Session = Depends(require_legacy_session)) -> dict[str, Any]:
        def work() -> tuple[int, list[str]]:
            with REGISTRY.cleanup_claim(
                directory=SETTINGS.journal_dir
            ), FabricClient(session.tokens) as client:
                return workspaces.delete_scratch_workspaces(client)

        try:
            deleted, warnings = await _run_fabric(work)
        except RunConflict as error:
            raise _run_conflict(error) from error
        return {"deleted": deleted, "warnings": warnings}

    @app.post("/api/workspaces/restore-access")
    async def restore_access(
        body: RestoreAccessRequest,
        session: Session = Depends(require_legacy_session),
    ) -> dict[str, Any]:
        """Grant a workspace's admins access to another workspace.

        A workspace this service principal created is only visible to the service principal
        until its permissions are copied, so an interrupted run can leave one that nobody
        else can open or delete. This grants access without ever revoking any.
        """

        def work() -> dict[str, Any]:
            with FabricClient(session.tokens) as client:
                assignments = workspaces.list_role_assignments(client, body.source_workspace_id)
                admins = [a for a in assignments if a.get("role") == "Admin"]
                warnings = workspaces.copy_role_assignments(
                    client, admins, body.target_workspace_id, roles={"Admin"}
                )
                return {"granted": len(admins) - len(warnings), "warnings": warnings}

        return await _run_fabric(work)

    @app.get("/api/runs/{run_id}")
    async def get_run(run_id: str, session: Session = Depends(require_execution_session)) -> dict[str, Any]:
        return _require_run(run_id, session).snapshot()

    @app.get("/api/runs/{run_id}/readiness")
    async def get_readiness(
        run_id: str, download: bool = False, session: Session = Depends(require_execution_session),
    ) -> JSONResponse:
        report = await asyncio.to_thread(_readiness, run_id, session)
        headers = {"Cache-Control": "no-store"}
        if download:
            headers["Content-Disposition"] = f'attachment; filename="cutover-{run_id}.json"'
        return JSONResponse(report, headers=headers)

    @app.get("/api/runs/{run_id}/connections/script")
    async def get_connections_lookup_script(
        run_id: str, session: Session = Depends(require_execution_session),
    ) -> PlainTextResponse:
        """A PowerShell script, for download, that looks up the connections the advisory
        scan found - by id, signed in as the operator rather than the service principal.

        Same authorization as the readiness report it is generated from: a run this session
        cannot see refuses here too, before any script is produced.
        """
        script = await asyncio.to_thread(_connections_lookup_script, run_id, session)
        headers = {
            "Cache-Control": "no-store",
            "Content-Disposition": f'attachment; filename="connection-lookup-{run_id}.ps1"',
        }
        return PlainTextResponse(script, headers=headers)

    @app.get("/api/resumable")
    async def resumable(session: Session = Depends(require_execution_session)) -> dict[str, Any]:
        """Runs that stopped without finishing, from their journals on disk.

        Read from disk rather than from the run registry on purpose: the runs worth offering
        back are exactly the ones this process has no memory of, because it was restarted.
        """

        def work() -> list[dict[str, Any]]:
            return [
                _resumable_dict(replay) for replay in REGISTRY.resumable(
                    _session_directory(session), **_session_identity(session),
                )
                if _replay_matches(session, replay)
            ]

        return {"runs": await asyncio.to_thread(work)}

    @app.get("/api/runs/{run_id}/resume-plan")
    async def resume_plan(
        run_id: str,
        session: Session = Depends(require_execution_session),
    ) -> dict[str, Any]:
        replay, plan = await asyncio.to_thread(_resume_plan, session, run_id)
        return {
            "runId": run_id, "plan": _plan_dict(plan), "cleanupWhenDone": replay.cleanup,
            "targetWorkspaceId": replay.target_workspace_id,
            "items": [
                {"sourceId": source_id, "targetId": item.get("target", ""),
                 "name": item.get("name", ""), "type": item.get("type", "")}
                for source_id, item in replay.items.items()
            ],
        }

    @app.post("/api/runs/{run_id}/ignore")
    async def ignore_saved_run(
        run_id: str, body: ConfirmSavedAction,
        session: Session = Depends(require_execution_session),
    ) -> dict[str, Any]:
        if not body.confirmed:
            raise HTTPException(status_code=400, detail="Confirm Ignore before hiding this saved migration.")

        def work() -> dict[str, Any]:
            path = _session_journal(session, run_id)
            if not path.is_file():
                raise HTTPException(status_code=404, detail="No journal for that run")
            with REGISTRY.saved_action_claim(
                run_id, directory=path.parent, **_session_identity(session),
            ) as replay:
                if not replay.ignored:
                    journal.Journal(path).recovery_action("ignore")
                return {"ignored": True, "runId": run_id}

        return await _run_saved_action(work)

    @app.post("/api/runs/{run_id}/restart")
    async def restart_saved_run(
        run_id: str, body: ConfirmFullRestart,
        session: Session = Depends(require_execution_session),
    ) -> dict[str, Any]:
        if not body.confirmed:
            raise HTTPException(status_code=400, detail="Confirm destination deletion before a full restart.")

        def work() -> dict[str, Any]:
            path = _session_journal(session, run_id)
            if not path.is_file():
                raise HTTPException(status_code=404, detail="No journal for that run")
            with REGISTRY.saved_action_claim(
                run_id, directory=path.parent, destructive=True, **_session_identity(session),
            ) as replay:
                # Confirmation, mode and source protection precede all remote work.
                if body.target_workspace_id != replay.target_workspace_id:
                    raise ValueError(
                        "The destination changed. Reload the list before confirming full restart.",
                    )
                if reason := recovery.restart_blocker(replay):
                    raise ValueError(reason)
                with _planning_clients(session) as (source, target):
                    return recovery.full_restart(
                        replay, journal.Journal(path), source_client=source, target_client=target,
                        confirmed_target_id=body.target_workspace_id, identity=_session_identity(session),
                    )

        return await _run_saved_action(work)

    @app.post("/api/runs/{run_id}/resume-preview")
    async def resume_preview(
        run_id: str, body: ResumeRunRequest,
        session: Session = Depends(require_execution_session),
    ) -> dict[str, Any]:
        def work() -> dict[str, Any]:
            replay, plan = _resume_plan(session, run_id)
            with _planning_clients(session) as (source, target):
                return _paired_dependency_report(session, source, target, body.apply(plan), prior=replay)
        return await _run_fabric(work)

    @app.post("/api/runs/{run_id}/resume")
    async def resume(
        run_id: str, body: ResumeRunRequest | None = None,
        session: Session = Depends(require_execution_session),
    ) -> dict[str, Any]:
        """Adopt the previous target with explicit mappings and this attempt's activation opt-in."""
        replay, plan = await asyncio.to_thread(_resume_plan, session, run_id)
        if body is not None:
            plan = body.apply(plan)
        run = _start_attempt(session, plan, cleanup=replay.cleanup, prior=replay)
        return {"runId": run.id, "plan": _plan_dict(plan), "resumedFrom": run_id}

    @app.post("/api/runs/{run_id}/cancel")
    async def cancel_run(
        run_id: str, session: Session = Depends(require_execution_session),
    ) -> dict[str, Any]:
        run = _require_run(run_id, session)
        run.cancel()
        return {"ok": True, "status": run.status.value}

    @app.post("/api/runs/{run_id}/cleanup")
    async def cleanup(
        run_id: str,
        session: Session = Depends(require_execution_session),
    ) -> dict[str, Any]:
        run = _require_run(run_id, session)
        def work() -> list[str]:
            with REGISTRY.cleanup_claim(
                run, directory=_session_directory(session), **_session_identity(session),
            ), FabricClient(session.destination_tokens) as client:
                return cleanup_run(run, client)

        try:
            warnings = await _run_fabric(work)
        except RunConflict as error:
            raise _run_conflict(error) from error
        return {"ok": not warnings, "warnings": warnings, "run": run.snapshot()}

    @app.get("/api/runs/{run_id}/events")
    async def run_events(run_id: str, request: Request, session_id: str | None = None):
        """Server-sent events feed. EventSource cannot set headers, so the id comes as a query."""
        session = SESSIONS.get(session_id)
        if not session:
            raise HTTPException(status_code=401, detail="Sign in with a service principal first")
        run = _require_run(run_id, session)

        async def stream():
            subscriber = run.subscribe()
            try:
                while True:
                    if SESSIONS.get(session.id) is not session or await request.is_disconnected():
                        return
                    try:
                        event = await asyncio.to_thread(subscriber.get, True, 15)
                    except queue.Empty:
                        # A run that finished before this client connected never publishes
                        # again, so close the stream instead of idling on keep-alives.
                        if run.status not in (RunStatus.PENDING, RunStatus.RUNNING):
                            yield f"data: {json.dumps(run.snapshot())}\n\n"
                            return
                        yield ": keep-alive\n\n"
                        continue
                    if event is None:
                        yield f"data: {json.dumps(run.snapshot())}\n\n"
                        return
                    yield f"data: {json.dumps(event)}\n\n"
                    if event.get("status") not in (RunStatus.PENDING, RunStatus.RUNNING):
                        return
            finally:
                run.unsubscribe(subscriber)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return app


# ------------------------------------------------------------------- utilities


def _session_identity(session: Session) -> dict[str, str]:
    if not session.paired:
        return {}
    return {
        "source_tenant_id": session.source_tenant_id,
        "target_tenant_id": session.target_tenant_id,
        "source_client_id": session.principal.client_id,
        "target_client_id": session.destination_tokens.principal.client_id,
    }


def _session_directory(session: Session) -> Path:
    return SETTINGS.journal_dir_for(
        source_tenant_id=session.source_tenant_id, target_tenant_id=session.target_tenant_id,
    )


def _session_journal(session: Session, run_id: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run_id):
        raise HTTPException(status_code=400, detail="Invalid run ID")
    return _session_directory(session) / f"{run_id}.jsonl"


def _require_identity(session: Session, plan: dict[str, Any]) -> None:
    try:
        journal.validate_tenant_binding(plan, **_session_identity(session))
    except journal.TenantBindingError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


def _require_replay(session: Session, replay: journal.Replay) -> None:
    try:
        journal.validate_replay_binding(replay, **_session_identity(session))
    except journal.TenantBindingError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


def _replay_matches(session: Session, replay: journal.Replay) -> bool:
    try:
        _require_replay(session, replay)
    except HTTPException:
        return False
    return True


def _resume_plan(session: Session, run_id: str) -> tuple[journal.Replay, MigrationPlan]:
    path = _session_journal(session, run_id)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="No journal for that run")
    replay = journal.read(path)
    _require_replay(session, replay)
    if replay.ignored or replay.restart_state:
        raise HTTPException(
            status_code=409,
            detail="This migration was ignored or marked for full restart. It cannot be resumed.",
        )
    try:
        return replay, replace(plan_from_journal(replay), start_database_mirrors=False)
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


def _readiness(run_id: str, session: Session) -> dict[str, Any]:
    # Validate even for in-memory runs: the id is also used in the download filename.
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run_id):
        raise HTTPException(status_code=400, detail="Invalid run ID")
    path = _session_journal(session, run_id)
    run = REGISTRY.get(run_id)
    if path.is_file():
        saved = journal.read(path)
        if saved.restart_state:
            _require_replay(session, saved)
            raise HTTPException(
                status_code=409,
                detail="This migration's destination is being removed or was removed by Full restart. "
                "Its old cutover report no longer describes a usable destination.",
            )
    if run is not None:
        _require_identity(session, run.plan)
        report = readiness_report(
            run.lifecycle.snapshot(), run_id=run_id, lineage_id=run.lineage_id,
            run_status=run.status.value, inventory_complete=run.inventory_complete,
            attempts=run.readiness_attempts,
        )
        report["connectionAdvisories"] = connection_advisory.section_for_report(
            run.connection_advisory, run_id=run_id, strategy=str(run.plan.get("strategy") or ""),
        )
        return report
    if not path.is_file():
        raise HTTPException(status_code=404, detail="No run or saved journal for that run")
    replay = journal.read(path)
    _require_replay(session, replay)
    outcomes = dict(replay.outcomes)
    for source, item in replay.items.items():
        outcomes.setdefault(source, ItemOutcome(
            source, item["name"], item["type"],
            str(replay.plan.get("source_workspace_id") or ""), replay.target_workspace_id,
            item["target"], required=list(contract_for(item["type"]).required),
        ))
    report = readiness_report(
        outcomes, run_id=run_id, lineage_id=replay.lineage_id,
        run_status=replay.status or "interrupted",
        inventory_complete=replay.inventory_complete and not replay.damaged_lines,
        attempts=_readiness_attempts(replay),
    )
    report["connectionAdvisories"] = connection_advisory.section_for_report(
        replay.connection_advisory, run_id=run_id, strategy=str(replay.plan.get("strategy") or ""),
    )
    return report


def _readiness_attempts(replay: journal.Replay | None) -> list[dict[str, Any]]:
    # Run errors can echo arbitrary inputs. Per-item evidence contains only sanitized errors.
    return [
        {key: attempt.get(key, "") for key in ("run_id", "status", "last_phase")}
        for attempt in replay.attempts
    ] if replay else []


def _plan_record_for_run(run_id: str, session: Session) -> dict[str, Any]:
    """The stored plan for a run, live or saved, with the same authorization as its report."""
    run = REGISTRY.get(run_id)
    if run is not None:
        _require_identity(session, run.plan)
        return run.plan
    path = _session_journal(session, run_id)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="No run or saved journal for that run")
    replay = journal.read(path)
    _require_replay(session, replay)
    return replay.plan


def _connections_lookup_script(run_id: str, session: Session) -> str:
    """The PowerShell lookup script for a run's recorded connection advisory scan.

    The tenant it signs into is always the *source* workspace's tenant - where these
    connections live - not necessarily this session's own: a paired, cross-tenant plan
    records its source tenant explicitly, and only a same-tenant plan falls back to the
    signed-in principal's own tenant.

    Not refused merely because no connection was recorded: a report from before this scan
    existed, or one whose scan has not run yet, has nothing to embed either way, and the
    script itself falls back to listing every connection the operator can see when it is
    handed none. Only ``not_applicable`` (a reassign, which never scans because nothing about
    a connection's path changes) has genuinely nothing to look up.
    """
    report = _readiness(run_id, session)
    advisories = report.get("connectionAdvisories") or {}
    if advisories.get("scanState") == "not_applicable":
        raise HTTPException(
            status_code=404,
            detail="This run's strategy does not repoint connections, so there is nothing to look up.",
        )
    connection_ids = [
        str(entry["connectionId"]) for entry in advisories.get("connections") or []
        if entry.get("connectionId")
    ]
    plan_record = _plan_record_for_run(run_id, session)
    tenant_id = str(plan_record.get("source_tenant_id") or session.principal.tenant_id or "")
    try:
        return connections_lookup_script(connection_ids, tenant_id=tenant_id)
    except ValueError as error:
        raise HTTPException(status_code=500, detail=str(error)) from error


def _plan_dict(plan: MigrationPlan) -> dict[str, Any]:
    return {
        "capacityId": plan.capacity_id,
        "capacityName": plan.capacity_name,
        "capacityRegion": plan.capacity_region,
        "sourceWorkspaceName": plan.source_workspace_name,
        "sourceWorkspaceId": plan.source_workspace_id,
        "targetWorkspaceName": plan.target_workspace_name,
        "strategy": plan.strategy.value,
        "capacityWarning": plan.capacity_warning,
        "includeData": plan.include_data,
        "includeFiles": plan.include_files,
        "copyPermissions": plan.copy_permissions,
        "sourceTenantId": plan.source_tenant_id,
        "targetTenantId": plan.target_tenant_id,
        "sourceClientId": plan.source_client_id,
        "targetClientId": plan.target_client_id,
        "writeFreezeConfirmed": plan.write_freeze_confirmed,
        "startDatabaseMirrors": plan.start_database_mirrors,
        "connectionMappings": plan.connection_mappings,
        "referenceMappings": plan.reference_mappings,
    }


def _resumable_dict(replay: journal.Replay) -> dict[str, Any]:
    """One interrupted run, as much as can be told without asking Fabric anything.

    Everything here comes from the journal on disk, so the list can be shown before the
    operator has signed in to anything the run touched.
    """
    plan = replay.plan
    return {
        "runId": replay.run_id,
        "lineageId": replay.lineage_id,
        "resumedFrom": replay.resumed_from,
        "status": replay.status or "interrupted",
        "error": replay.error,
        "startedAt": replay.created_at,
        "sourceWorkspaceName": plan.get("source_workspace_name") or "",
        "targetWorkspaceName": plan.get("target_workspace_name") or "",
        "capacityName": plan.get("capacity_name") or "",
        "capacityRegion": plan.get("capacity_region") or "",
        "targetWorkspaceId": replay.target_workspace_id,
        "recoveryAction": "restart_pending" if replay.restart_state == "pending" else "",
        "canRestart": not recovery.restart_blocker(replay),
        "restartBlockedReason": recovery.restart_blocker(replay),
        # What it managed before it stopped, which is what the operator is deciding about.
        "itemsCreated": len(replay.items) or len(replay.id_map),
        "lastPhase": replay.phases_started[-1] if replay.phases_started else "",
        "warnings": len(replay.warnings),
        "sourceTenantId": plan.get("source_tenant_id", ""),
        "targetTenantId": plan.get("target_tenant_id", ""),
        "sourceClientId": plan.get("source_client_id", ""),
        "targetClientId": plan.get("target_client_id", ""),
    }


def _object_id(session: Session) -> str:
    """The service principal's object id, for the grant script.

    Read from the ``oid`` claim of a token this process already holds, so the script does not
    have to ask the directory for it and the operator does not need a second sign-in scope.
    """
    try:
        return session.tokens.object_id()
    except AuthError as error:
        logger.info("Could not read the service principal object id: %s", error)
        return ""


def _paired_dependency_report(
    session: Session, source: FabricClient, target: FabricClient, plan: MigrationPlan,
    *, prior: journal.Replay | None = None,
) -> dict[str, Any]:
    assessment = assess_workspace(
        list_items(source, plan.source_workspace_id), force_rebuild=plan.strategy is Strategy.REBUILD,
        require_stopped=session.paired,
    )
    if not session.paired:
        return _dependency_report(
            source, source_workspace_id=plan.source_workspace_id, migrated=assessment.migrated,
            client_id=session.principal.client_id, object_id=_object_id(session),
            tenant_id=session.principal.tenant_id,
        )
    if plan.strategy is Strategy.REASSIGN:
        return {"dependencies": [], "connectionAccess": None, "blockers": [], "paired": True}
    messages: list[str] = []
    aliases, _ = migration_refs.resolve(
        source, target, plan.reference_mappings, migrating_workspace_id=plan.source_workspace_id,
    )
    if prior is not None:
        aliases = {**prior.id_map, **aliases}
        if prior.target_workspace_id:
            aliases[plan.source_workspace_id] = prior.target_workspace_id
    rewrite = definitions.build_rewriter(aliases)
    bound: dict[str, list[str]] = {}
    for item in assessment.migrated if plan.strategy is Strategy.REBUILD else []:
        name = item.get("displayName") or item["id"]
        try:
            if session.paired and item.get("type") == "SemanticModel":
                # Default TMDL cannot prove the absence of source principal assignments.
                definition = get_item_definition(source, plan.source_workspace_id, item["id"], fmt="TMSL")
            else:
                definition = get_item_definition(source, plan.source_workspace_id, item["id"])
        except FabricError as error:
            messages.append(
                f"Source connection references for '{name}' could not be checked: {error}. "
                "Restore source definition access and re-check."
            )
            continue
        parts = definition.get("parts") or []
        if session.paired:
            try:
                if item.get("type") == "SemanticModel":
                    parts, omissions = analytics.without_model_memberships(parts)
                    messages.extend(f"'{name}' definition preview: {warning}" for warning in omissions)
                messages.extend(
                    f"'{name}': {warning}"
                    for warning in analytics.validate_cross_tenant_identities(
                        parts, item_type=item.get("type") or "",
                    )
                )
            except analytics.IdentityBindingError as error:
                messages.append(f"'{name}' will not be created with its source identity bindings: {error}")
                continue
        for connection_id in connections.referenced_connection_ids(parts):
            bound.setdefault(connection_id.casefold(), []).append(name)

    for source_id in sorted(set(bound) | set(plan.connection_mappings)):
        target_id = plan.connection_mappings.get(source_id)
        if not target_id and prior is not None:
            target_id = prior.id_map.get(source_id)
        consumers = ", ".join(bound.get(source_id) or [source_id])
        if not target_id:
            if session.cross_tenant:
                messages.append(
                    f"{consumers}: map source connection {source_id} to a destination connection. "
                    "Items with unresolved source connections will not be created."
                )
                continue
            target_id = source_id
        try:
            original = source.get(f"connections/{source_id}")
            replacement = target.get(f"connections/{target_id}")
        except FabricError as error:
            messages.append(
                f"{consumers}: connection mapping {source_id} → {target_id} could not be checked: {error}. "
                "Grant the appropriate app access to its connection and re-check; "
                "affected items may be skipped."
            )
            continue
        old_path = (original.get("connectionDetails") or {}).get("path") or ""
        new_path = rewrite(old_path) if rewrite else old_path
        reused = not session.cross_tenant and target_id.casefold() == source_id.casefold()
        if plan.source_workspace_id.casefold() in new_path.casefold() or (
            not reused and not connections.matches_replacement(replacement, original, new_path)
        ):
            messages.append(
                f"{consumers}: destination connection {target_id} does not match the mapped source. "
                "Create a replacement against the destination store and update this mapping; "
                "dependent items will be skipped until the replacement can be verified."
            )

    graph = relations.build_graph(source, plan.source_workspace_id, assessment.migrated)
    mapped_external = {
        (entry["source_workspace_id"].casefold(), entry["source_item_id"].casefold())
        for entry in plan.reference_mappings
    }
    if not graph.available:
        messages.append(
            "Source item relationships could not be checked. Verify external item mappings "
            "and inspect skipped-item details after migration before cutover."
        )
    for item_id, dependencies in graph.dependencies.items():
        for dependency in dependencies:
            workspace_id = graph.workspace_of(dependency)
            if (
                session.cross_tenant and workspace_id
                and workspace_id.casefold() != plan.source_workspace_id.casefold()
                and (workspace_id.casefold(), dependency.casefold()) not in mapped_external
            ):
                messages.append(
                    f"'{graph.name_of(item_id)}' needs external item '{graph.name_of(dependency)}'. "
                    f"Map source workspace {workspace_id}, item {dependency} to a destination item. "
                    "Consumers with unresolved source references will not be created."
                )
    return {
        "dependencies": messages, "connectionAccess": None, "blockers": [],
        "connectionAccessScope": "destination", "paired": True,
        "sourceTenantId": session.source_tenant_id, "targetTenantId": session.target_tenant_id,
        "assessmentNotice": (
            "Destination mapping metadata was checked, not data-copy or cutover readiness. "
            "Resolve the prerequisites listed above; items with unresolved source references are skipped. "
            "Validate the destination and the exported readiness report before cutover."
        ),
    }


def _dependency_report(
    client: FabricClient,
    *,
    source_workspace_id: str,
    migrated: list[dict[str, Any]],
    client_id: str,
    object_id: str = "",
    tenant_id: str = "",
) -> dict[str, Any]:
    """Run the same dependency check the migration runs, before anything is created.

    The check is read only, so there is no reason to make the operator start a run to find
    out that a semantic model points somewhere the migration cannot follow, or that a
    connection has to be shared first.
    """
    if not migrated:
        return {"dependencies": [], "connectionAccess": None}

    try:
        report = dependency_warnings(
            client,
            source_workspace_id=source_workspace_id,
            migrated=migrated,
            client_id=client_id,
        )
    except FabricApiError as error:
        return {
            "dependencies": [f"Dependencies could not be checked: {error}"],
            "connectionAccess": None,
        }

    if not report.available:
        return {
            "dependencies": [
                "The relations API is unavailable to this service principal, so dependencies "
                "between items could not be checked."
            ],
            "connectionAccess": None,
        }

    access = (
        {
            "connections": [entry.as_dict() for entry in report.access],
            "instructions": portal_instructions(client_id),
            "script": grant_script(
                client_id,
                report.access,
                object_id=object_id,
                tenant_id=tenant_id,
            ),
        }
        if report.access
        else None
    )
    return {"dependencies": report.messages(), "connectionAccess": access}


def _semantic_model_preview(
    session: Session,
    workspace_id: str,
    plan: MigrationPlan,
) -> dict[str, Any]:
    """Report which semantic models would have to be converted, and anything that blocks it.

    Surfaced before the run so the operator learns about a blocker at review time rather
    than after the models have already been touched.
    """
    try:
        with PowerBiClient(session.tokens) as pbi:
            models = pbi.list_semantic_models(workspace_id)
    except PowerBiError as error:
        return {
            "blockers": [
                "Could not read semantic model storage settings. The service principal needs "
                f"access to the Power BI APIs: {error}"
            ]
        }

    large = [model for model in models if model.is_large]
    blockers: list[str] = []

    if large and not supports_large_semantic_models(plan.capacity_region):
        blockers.append(
            f"Region '{plan.capacity_region}' does not support large semantic model storage, "
            f"so {len(large)} model(s) could not be restored after the move."
        )

    blocked = [model.name for model in large if not model.convertible]
    if blocked:
        blockers.append(
            "These semantic models cannot leave the large storage format: " + ", ".join(blocked)
        )

    return {
        "largeSemanticModels": [{"id": m.id, "name": m.name} for m in large],
        "blockers": blockers,
    }


def _require_run(run_id: str, session: Session) -> MigrationRun:
    _session_journal(session, run_id)
    run = REGISTRY.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Unknown migration run")
    _require_identity(session, run.plan)
    return run


async def _run_saved_action(work):
    try:
        return await _run_fabric(work)
    except OSError as error:
        logger.exception("Could not persist the saved-migration action")
        raise HTTPException(
            status_code=500,
            detail=f"Could not save the migration action: {error}. "
            "Restore journal storage, reload the saved migrations and retry the action.",
        ) from error


async def _run_fabric(work):
    """Run a blocking Fabric call off the event loop, mapping API errors to HTTP errors."""
    try:
        return await asyncio.to_thread(work)
    except FabricApiError as error:
        status = error.status_code if error.status_code in (401, 403, 404, 409, 429) else 502
        raise HTTPException(status_code=status, detail=error.body[:500] or str(error)) from error
    except FabricError as error:
        raise HTTPException(status_code=502, detail=str(error)) from error
    except AuthError as error:
        raise HTTPException(status_code=401, detail=str(error)) from error
    except RunConflict as error:
        raise _run_conflict(error) from error
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


app = create_app()

__all__ = ["app", "create_app", "default_target_name"]
