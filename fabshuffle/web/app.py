"""FastAPI application backing the Fab Shuffle wizard."""

from __future__ import annotations

import asyncio
import json
import logging
import queue
import secrets
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from fabshuffle import __version__
from fabshuffle.auth import AuthError, ServicePrincipal, TokenProvider
from fabshuffle.fabric import workspaces
from fabshuffle.fabric.client import FabricApiError, FabricClient
from fabshuffle.orchestrator import (
    MigrationPlan,
    build_plan,
    cleanup_run,
    default_target_name,
    run_migration,
)
from fabshuffle.run import REGISTRY, MigrationRun, RunStatus

logger = logging.getLogger(__name__)

WEB_DIR = Path(__file__).parent
STATIC_DIR = WEB_DIR / "static"
TEMPLATES_DIR = WEB_DIR / "templates"

SESSION_HEADER = "X-Fab-Shuffle-Session"


# --------------------------------------------------------------------- sessions


@dataclass
class Session:
    """One signed-in service principal. Credentials never leave this process."""

    id: str
    principal: ServicePrincipal
    tokens: TokenProvider


class SessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, Session] = {}
        self._lock = threading.Lock()

    def create(self, principal: ServicePrincipal, tokens: TokenProvider) -> Session:
        session = Session(id=secrets.token_urlsafe(32), principal=principal, tokens=tokens)
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


def require_session(
    session_id: str | None = Header(default=None, alias=SESSION_HEADER),
) -> Session:
    session = SESSIONS.get(session_id)
    if not session:
        raise HTTPException(status_code=401, detail="Sign in with a service principal first")
    return session


# ---------------------------------------------------------------------- schemas


class LoginRequest(BaseModel):
    tenant_id: str = Field(min_length=1)
    client_id: str = Field(min_length=1)
    client_secret: str = Field(min_length=1)


class StartRunRequest(BaseModel):
    capacity_id: str = Field(min_length=1)
    source_workspace_id: str = Field(min_length=1)
    target_workspace_name: str | None = None
    include_data: bool = True
    include_files: bool = True
    copy_permissions: bool = True
    cleanup_when_done: bool = True


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
        principal = ServicePrincipal(
            tenant_id=body.tenant_id.strip(),
            client_id=body.client_id.strip(),
            client_secret=body.client_secret,
        )
        tokens = TokenProvider(principal)
        try:
            await asyncio.to_thread(tokens.verify)
        except AuthError as error:
            raise HTTPException(status_code=401, detail=str(error)) from error

        session = SESSIONS.create(principal, tokens)
        return {"sessionId": session.id, "principal": principal.redacted()}

    @app.post("/api/logout")
    async def logout(session: Session = Depends(require_session)) -> dict[str, bool]:
        SESSIONS.drop(session.id)
        return {"ok": True}

    # -------------------------------------------------------------- discovery

    @app.get("/api/capacities")
    async def list_capacities(session: Session = Depends(require_session)) -> dict[str, Any]:
        def work() -> list[dict[str, Any]]:
            with FabricClient(session.tokens) as client:
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

    @app.get("/api/preview")
    async def preview(
        capacity_id: str,
        source_workspace_id: str,
        session: Session = Depends(require_session),
    ) -> dict[str, Any]:
        """Summarise what the migration would create before the operator commits."""

        def work() -> dict[str, Any]:
            from fabshuffle.fabric import data_stores, eventhouses
            from fabshuffle.fabric.items import list_items

            with FabricClient(session.tokens) as client:
                plan = build_plan(
                    client,
                    capacity_id=capacity_id,
                    source_workspace_id=source_workspace_id,
                )
                lakehouses = data_stores.list_lakehouses(client, source_workspace_id)
                warehouses = data_stores.list_warehouses(client, source_workspace_id)
                houses = eventhouses.list_eventhouses(client, source_workspace_id)
                all_items = list_items(client, source_workspace_id)

                supported = {"Lakehouse", "Warehouse", "Eventhouse", "KQLDatabase", "SQLEndpoint"}
                unsupported = sorted(
                    {item.get("type", "Unknown") for item in all_items if item.get("type") not in supported}
                )

                return {
                    "targetWorkspaceName": plan.target_workspace_name,
                    "capacityRegion": plan.capacity_region,
                    "capacityName": plan.capacity_name,
                    "sourceWorkspaceName": plan.source_workspace_name,
                    "counts": {
                        "lakehouses": len(lakehouses),
                        "warehouses": len(warehouses),
                        "eventhouses": len(houses),
                    },
                    "unsupportedItemTypes": unsupported,
                }

        return await _run_fabric(work)

    # ------------------------------------------------------------------- runs

    @app.post("/api/runs")
    async def start_run(
        body: StartRunRequest,
        session: Session = Depends(require_session),
    ) -> dict[str, Any]:
        def prepare() -> MigrationPlan:
            with FabricClient(session.tokens) as client:
                return build_plan(
                    client,
                    capacity_id=body.capacity_id,
                    source_workspace_id=body.source_workspace_id,
                    target_workspace_name=body.target_workspace_name,
                    include_files=body.include_files,
                    include_data=body.include_data,
                    copy_permissions=body.copy_permissions,
                )

        plan = await _run_fabric(prepare)
        run = REGISTRY.add(
            MigrationRun(
                source_workspace_name=plan.source_workspace_name,
                capacity_name=plan.capacity_name,
            )
        )

        thread = threading.Thread(
            target=run_migration,
            args=(run, session.principal, plan),
            kwargs={"cleanup": body.cleanup_when_done},
            name=f"fab-shuffle-{run.id}",
            daemon=True,
        )
        thread.start()

        return {"runId": run.id, "plan": _plan_dict(plan)}

    @app.get("/api/runs/{run_id}")
    async def get_run(run_id: str, _: Session = Depends(require_session)) -> dict[str, Any]:
        return _require_run(run_id).snapshot()

    @app.post("/api/runs/{run_id}/cancel")
    async def cancel_run(run_id: str, _: Session = Depends(require_session)) -> dict[str, Any]:
        run = _require_run(run_id)
        run.cancel()
        return {"ok": True, "status": run.status.value}

    @app.post("/api/runs/{run_id}/cleanup")
    async def cleanup(
        run_id: str,
        session: Session = Depends(require_session),
    ) -> dict[str, Any]:
        run = _require_run(run_id)
        if run.status == RunStatus.RUNNING:
            raise HTTPException(status_code=409, detail="Wait for the migration to finish first")

        def work() -> list[str]:
            with FabricClient(session.tokens) as client:
                return cleanup_run(run, client)

        warnings = await _run_fabric(work)
        return {"ok": not warnings, "warnings": warnings, "run": run.snapshot()}

    @app.get("/api/runs/{run_id}/events")
    async def run_events(run_id: str, request: Request, session_id: str | None = None):
        """Server-sent events feed. EventSource cannot set headers, so the id comes as a query."""
        if not SESSIONS.get(session_id):
            raise HTTPException(status_code=401, detail="Sign in with a service principal first")
        run = _require_run(run_id)

        async def stream():
            subscriber = run.subscribe()
            try:
                while True:
                    if await request.is_disconnected():
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


def _plan_dict(plan: MigrationPlan) -> dict[str, Any]:
    return {
        "capacityName": plan.capacity_name,
        "capacityRegion": plan.capacity_region,
        "sourceWorkspaceName": plan.source_workspace_name,
        "targetWorkspaceName": plan.target_workspace_name,
        "includeData": plan.include_data,
        "includeFiles": plan.include_files,
        "copyPermissions": plan.copy_permissions,
    }


def _require_run(run_id: str) -> MigrationRun:
    run = REGISTRY.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Unknown migration run")
    return run


async def _run_fabric(work):
    """Run a blocking Fabric call off the event loop, mapping API errors to HTTP errors."""
    try:
        return await asyncio.to_thread(work)
    except FabricApiError as error:
        status = error.status_code if error.status_code in (401, 403, 404, 409, 429) else 502
        raise HTTPException(status_code=status, detail=error.body[:500] or str(error)) from error
    except AuthError as error:
        raise HTTPException(status_code=401, detail=str(error)) from error


app = create_app()

__all__ = ["app", "create_app", "default_target_name"]
