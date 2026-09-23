"""Authenticated transport for the Warehouse-backed BCDR service.

The service owns mode, identity, readiness, ACL and capacity policy. HTTP only
validates the request/explicit action confirmation and keeps a worker alive until
the synchronous service has settled its operation and closed its clients.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter, Depends, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict

from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.bootstrap import BootstrapError, BootstrapStore, canonical_arm_id
from fabshuffle.bcdr.capacity import ArmCapacityClient, CapacityError
from fabshuffle.bcdr.catalog import CatalogConflict, CatalogError
from fabshuffle.bcdr.discovery import match_recovery_capacities
from fabshuffle.bcdr.protection_binding import ConfigureProtectionRequest
from fabshuffle.bcdr.service import (
    BcdrService,
    ConfigureReplicaRequest,
    ContinueDrTestRequest,
    CutbackRequest,
    CutoverRequest,
    EnableRecoveryRequest,
    EndDrTestRequest,
    FailbackExecuteRequest,
    FailbackRequest,
    PlanRequest,
    RearmRequest,
    ReconcileOperationRequest,
    ScheduleGuideRequest,
    ServiceResult,
    SetupRequest,
    StartDrTestRequest,
    SyncRequest,
    create_service,
)
from fabshuffle.bcdr.service import (
    setup as setup_recovery,
)
from fabshuffle.config import SETTINGS
from fabshuffle.fabric import data_stores, workspaces
from fabshuffle.fabric.client import FabricApiError, FabricClient, FabricError
from fabshuffle.lifecycle import safe_text

if TYPE_CHECKING:
    from fabshuffle.web.app import Session

T = TypeVar("T", bound=BaseModel)
logger = logging.getLogger(__name__)


class ConfirmedRequest(BaseModel, Generic[T]):
    model_config = ConfigDict(extra="forbid", frozen=True)

    confirmation: str
    request: T


class BcdrRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def handle(request):
            try:
                return await original(request)
            except RequestValidationError as error:
                # Invalid input may contain a mistakenly pasted secret. Never echo
                # arbitrary request values through FastAPI's default 422 response.
                return JSONResponse(status_code=422, content={"detail": [
                    {"loc": entry["loc"], "msg": entry["msg"], "type": entry["type"]}
                    for entry in error.errors()
                ]})

        return handle


def bootstrap_path() -> Path:
    """Only deployment configuration may select a server-side bootstrap file."""
    return Path(os.environ.get(
        "FAB_SHUFFLE_BCDR_BOOTSTRAP", str(SETTINGS.scratch_root / "bcdr" / "bootstrap.json")
    )).resolve()


COMMANDS = (
    ("start-dr-test", "Start DR Test", StartDrTestRequest,
     "Exercise selected existing standby groups with recovery owners only. Production stays primary. "
     "Automatic synchronization is held until the test ends; no source metadata is captured.",
     "Resume recovery capacity and prepare stopped standby targets for an owners-only test. "
     "Do not pause production, replay business ACLs, change writer authority or route production consumers."),
    ("continue-dr-test", "Validate DR Test", ContinueDrTestRequest,
     "Continue the recorded test and supply current owner data, reference and access evidence. "
     "Untested and blocked workloads are not a passed recovery exercise.",
     "Prepare or recheck the same pinned test targets. "
     "This is not production admission or proof of regional failover."),
    ("end-dr-test", "End test and return to standby", EndDrTestRequest,
     "Verify the shared standby is unchanged except for recorded restoration, retain its resources, "
     "and release automatic sync only after ending safely.",
     "End this DR Test without production cutover or failback. Do not delete the standby estate. "
     "Park only when the existing capacity safety checks allow it."),
    ("schedule-guide", "Prepare scheduled-sync instructions", ScheduleGuideRequest,
     "Review the completed metadata scope and export a guarded recurring request. "
     "Data-recovery gaps stay visible. No Azure job is created or enabled.",
     "Approve this scope for fresh metadata capture and safe parking in recurring runs. "
     "This only prepares a configuration download and deployment instructions."),
    ("setup", "Set up the control Warehouse", SetupRequest,
     "Create a control workspace or choose an existing one where the recovery principal has Admin access. "
     "Additional members and owner-role differences produce warnings; existing grants stay unchanged. "
     "New control workspace owners use the Admin role. "
     "Choose an existing metadata Warehouse, continue saved setup, or create a new Warehouse. "
     "A selected Warehouse must be empty or contain this recovery set's compatible catalog. Persist its "
     "non-secret bootstrap in durable controller storage. No metadata lakehouse, Spark or Git is used.",
     "Continue or prepare the selected metadata Warehouse and, if selected, create its control workspace. "
     "An existing Warehouse is explicitly designated, not adopted by name. "
     "Incompatible contents are not overwritten. "
     "Confirm the "
     "source-capacity scope, dedicated recovery capacities, suspend authorization and owner allowlist. "
     "This does not enable recovery or grant general user access."),
    ("status", "Read recovery status", None,
     "Read the saved recovery state without source metadata reads. This resumes the metadata "
     "Warehouse capacity if paused; storage is still billed while paused.", ""),
    ("reconcile-operation", "Reconcile interrupted operation", ReconcileOperationRequest,
     "Read status, select the exact recorded operation, stop the previous controller and provide "
     "controller-fencing and destination-quiescence evidence. The backend verifies the service receipt; "
     "an uncertain create is never repeated or adopted by name.",
     "Take over the recorded controller epoch after fencing its previous worker and reconcile only "
     "the exact owned receipt. This may finish interrupted metadata application, but does not "
     "enable recovery, admit a dependency group or approve cutover. Receipt-free ambiguity remains blocked."),
    ("plan", "Preview standby selection", PlanRequest,
     "Preview exact workspace selection, dependency additions and capacity routes against a captured "
     "generation. Empty includes mean the configured source-capacity scope, not the whole tenant.", ""),
    ("synchronize", "Sync standby", SyncRequest,
     "Capture source metadata and update actual inactive standby items. Optional protection gaps "
     "affect the named items and dependents, not unrelated groups.",
     "Resume dedicated recovery capacities, capture metadata and update inactive standby items. "
     "Keep access restricted to the recovery principal and designated owners. This is NOT Enable "
     "recovery. The service pauses dedicated capacities only when safe and requested."),
    ("configure-protection", "Configure optional data protection", ConfigureProtectionRequest,
     "Configure an approved SQL, Cosmos, KQL or independent Lakehouse protection input for the next capture. "
     "Metadata synchronization itself does not copy business data. Keep credentials in the runtime provider.",
     "Record this optional protection configuration for the next capture. Review the off-region "
     "storage approval, integrity and consistency evidence. "
     "This is not a native backup or readiness approval."),
    ("configure-replica", "Configure qualified temporary attachments", ConfigureReplicaRequest,
     "Record independently qualified, exact retained-source OneLake paths and access evidence. "
     "Enable recovery performs the attachment; configuration alone does not attach data or prove readiness. "
     "Do not delete the retained source or treat a healthy-primary drill as outage qualification.",
     "Record these exact temporary attachment identities and their independently verified access evidence. "
     "Retain the original data for the full lifetime of the attachments. This is not independent recovery, "
     "does not enable recovery, and does not establish read-only enforcement or approve cutover."),
    ("enable-recovery", "Enable recovery", EnableRecoveryRequest,
     "Use a captured generation without source metadata reads. Approve only the displayed deferred "
     "ACL IDs. Enabling recovery stops scheduled source-to-standby sync and automatic pause.",
     "Enable recovery for the selected groups and replay only approved, backend-eligible ACLs. "
     "Recovery capacities remain running. This does not fence external writers or approve cutover. "
     "Additional control workspace access is reported without changing existing grants."),
    ("cutover", "Approve cutover", CutoverRequest,
     "Supply identity-bound, time-limited readiness observations and separate operator evidence "
     "that primary writers are fenced. A controller lock is not a writer fence.",
     "Approve the selected ready groups for production cutover. Confirm external primary writers "
     "are fenced and review the exact readiness observations. Definition creation is not readiness."),
    ("plan-failback", "Plan failback", FailbackRequest,
     "Confirm primary availability with evidence, then plan return targets and reconciliation of "
     "changes made in recovery. This is not a reversed migration.", ""),
    ("execute-failback", "Reconcile failback", FailbackExecuteRequest,
     "Fence recovery writers and supply reconciliation evidence, including deletes, expiration "
     "and ingestion offsets where applicable. Keep the recovery estate for rollback.",
     "Reconcile into the approved return targets after fencing recovery writers. Do not assume "
     "conflicting primary and recovery writes can be merged automatically."),
    ("cutback", "Approve cutback", CutbackRequest,
     "Approve consumer return only after validating the return targets, current writer epoch "
     "and time-limited data, binding and security evidence.",
     "Approve consumer cutback to validated return targets. Retain recovery resources until "
     "business sign-off; rollback after new writes is not a simple traffic switch."),
    ("rearm", "Rearm standby", RearmRequest,
     "Explicitly restore restricted standby access and source authority only after recovery "
     "no longer serves production. Pause dedicated capacities only when safe.",
     "Rearm source-to-standby synchronization, restore restricted standby access and, if requested, "
     "pause dedicated recovery capacity after it no longer serves production."),
)


def _same_tenant(session: Session) -> str:
    tenant = session.destination_tokens.tenant_id()
    if session.tokens.tenant_id() != tenant:
        raise ValueError("BCDR requires source and recovery credentials in the same tenant. "
                         "Use the separate migration workflow for another tenant.")
    return tenant


def _saved_setup(session: Session) -> dict | None:
    path = bootstrap_path()
    if not path.exists():
        return None
    try:
        descriptor = BootstrapStore(path).load()
        if (
            descriptor.tenant_id != session.destination_tokens.tenant_id()
            or descriptor.application_id != str(UUID(session.destination_tokens.principal.client_id))
        ):
            raise RecoveryBlocked("Saved setup belongs to a different recovery identity. Use its sign-in.")
    except BootstrapError as error:
        raise RecoveryBlocked(safe_text(str(error))) from error
    intent = descriptor.warehouse_intent
    return {
        "revision": descriptor.revision, "workspaceId": descriptor.control_workspace_id,
        "workspaceName": descriptor.workspace_intent.display_name if descriptor.workspace_intent else None,
        "warehouseId": descriptor.control_warehouse_id,
        "warehouseName": intent.display_name if intent else None,
        "warehousePhase": intent.phase if intent else None,
        "hasOperation": bool(intent and intent.operation_id),
    }


def _service_call(
    session: Session, invoke: Callable[[BcdrService], ServiceResult], *, source_access: bool = False,
) -> ServiceResult:
    _same_tenant(session)
    service = create_service(
        bootstrap_path(),
        target_tokens=session.destination_tokens,
        source_tokens=session.tokens if source_access else None,
    )
    try:
        result = invoke(service)
        for capacity in result.details.get("capacities", []):
            logger.info("BCDR observed capacity: fabric_capacity=%s arm_resource=%s state=%s",
                        capacity.get("capacity_id"), capacity.get("arm_resource_id"), capacity.get("state"))
        return result
    finally:
        service.close()


def _capacity_choices(tokens, capacities: list[dict] | None = None) -> list[dict]:
    if capacities is None:
        with FabricClient(tokens) as client:
            capacities = workspaces.list_capacities(client)
    with ArmCapacityClient(tokens) as arm:
        resources = arm.list_capacities()
    return match_recovery_capacities(capacities, resources)


def _validate_setup_choices(session: Session, request: SetupRequest) -> None:
    try:
        choices = {entry["id"]: entry for entry in _capacity_choices(session.destination_tokens)}
        catalog = canonical_arm_id(request.catalog_capacity_id)
        selected = {}
        for capacity in request.recovery_capacities:
            choice = choices.get(capacity.fabric_capacity_id)
            resource = canonical_arm_id(capacity.arm_resource_id)
            if not choice or choice["matchStatus"] != "matched" or choice["arm_resource_id"] != resource:
                raise RecoveryBlocked(
                    "The recovery capacity name/region match changed or is unavailable. "
                    "Refresh Discover setup choices and review the named capacity before submitting."
                )
            if resource in selected or capacity.fabric_capacity_id in selected.values():
                raise RecoveryBlocked("Choose each dedicated recovery capacity only once.")
            selected[resource] = capacity.fabric_capacity_id
        if catalog not in selected:
            raise RecoveryBlocked(
                "Choose the control Warehouse capacity from the selected recovery capacities."
            )
        if request.control_workspace_id:
            with FabricClient(session.destination_tokens) as client:
                workspace = workspaces.get_workspace(client, request.control_workspace_id)
            if workspace.get("capacityId") != selected[catalog]:
                raise RecoveryBlocked(
                    "The control workspace is not assigned to the selected control Warehouse capacity. "
                    "Choose its assigned recovery capacity or a different workspace."
                )
    except (BootstrapError, CapacityError, httpx.HTTPError, ValueError) as error:
        raise RecoveryBlocked(safe_text(str(error))) from error


def _setup_call(session: Session, request: SetupRequest) -> ServiceResult:
    _same_tenant(session)
    _validate_setup_choices(session, request)
    logger.info(
        "BCDR setup selection: control_workspace=%s source_capacities=%s",
        request.control_workspace_id or "[new workspace]", request.source_capacity_ids,
    )
    for capacity in request.recovery_capacities:
        try:
            resource = canonical_arm_id(capacity.arm_resource_id)
        except ValueError:
            resource = "[invalid ARM resource ID]"
        logger.info(
            "BCDR setup recovery capacity: fabric_capacity=%s arm_resource=%s catalog=%s",
            capacity.fabric_capacity_id, resource,
            capacity.arm_resource_id.lower() == request.catalog_capacity_id.lower(),
        )
    result = setup_recovery(request, bootstrap_path(), target_tokens=session.destination_tokens)
    logger.info("BCDR setup result: workspace=%s warehouse=%s",
                result.details.get("control_workspace_id"), result.details.get("control_warehouse_id"))
    return result


def create_router(
    require_session: Callable[..., Session],
    run_fabric: Callable[[Callable], Awaitable[Any]],
) -> APIRouter:
    router = APIRouter(prefix="/api/bcdr", tags=["BCDR"], route_class=BcdrRoute)

    async def execute(session, work):
        # A disconnected request must not abandon a mutating worker or close its
        # clients underneath it. Service operations settle before cancellation exits.
        task = asyncio.create_task(run_fabric(work))
        try:
            return await asyncio.shield(task)
        except (RecoveryBlocked, CatalogConflict) as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except CatalogError as error:
            raise HTTPException(
                status_code=503,
                detail=f"{error} Reconcile the recorded catalog operation before retrying.",
            ) from error
        except asyncio.CancelledError:
            try:
                await task
            finally:
                raise

    def confirmed(body, action):
        if body.confirmation != action:
            raise HTTPException(
                status_code=409, detail=f"Review the consequences and explicitly confirm '{action}'."
            )
        return body.request

    @router.get("/forms")
    async def forms(session=Depends(require_session)):
        if session.cross_tenant:
            raise HTTPException(status_code=409, detail="BCDR is same-tenant only.")
        def identity():
            tenant = _same_tenant(session)
            return {
                "tenant_id": tenant, "client_id": session.destination_tokens.principal.client_id,
                "object_id": session.destination_tokens.object_id(),
            }

        principal = await execute(session, identity)
        saved = await execute(session, lambda: _saved_setup(session))
        return {"identity": principal, "savedSetup": saved, "commands": [
            {
                "name": name, "label": label, "path": f"/api/bcdr/{name}",
                "method": "GET" if model is None else "POST",
                "schema": model.model_json_schema() if model else {"type": "object", "properties": {}},
                "description": description, "confirmation": confirmation,
                "primary": name == "synchronize",
            }
            for name, label, model, description, confirmation in COMMANDS
        ]}

    @router.get("/workspaces/{workspace_id}/warehouses")
    async def warehouse_choices(workspace_id: UUID, session=Depends(require_session)):
        def work():
            _same_tenant(session)
            saved = _saved_setup(session)
            identifier = str(workspace_id)
            if saved and saved["workspaceId"] and saved["workspaceId"] != identifier:
                raise RecoveryBlocked(
                    "This deployment has setup saved in another metadata workspace. "
                    "Use that workspace to continue; no saved state was replaced."
                )
            with FabricClient(session.destination_tokens) as client:
                workspace = workspaces.get_workspace(client, identifier)
                if workspace.get("id") != identifier:
                    raise RecoveryBlocked("The service returned a different metadata workspace.")
                entries = data_stores.list_warehouses(client, identifier)
            choices = []
            for entry in entries:
                if entry.get("workspaceId") != identifier or entry.get("type") != "Warehouse":
                    raise RecoveryBlocked("Warehouse discovery returned an item from another workspace.")
                item_id = str(UUID(entry["id"]))
                choices.append({
                    "id": item_id, "displayName": entry.get("displayName") or "Unnamed Warehouse",
                    "recorded": bool(saved and saved["warehouseId"] == item_id),
                    "endpointReported": bool(entry.get("properties", {}).get("connectionString")),
                })
            logger.info("BCDR Warehouse choices: workspace=%s count=%s", identifier, len(choices))
            for entry in choices:
                logger.info("BCDR Warehouse choice: workspace=%s warehouse=%s name=%r",
                            identifier, entry["id"], safe_text(entry["displayName"]))
            return {
                "workspaceId": identifier, "workspaceName": workspace.get("displayName"),
                "warehouses": choices, "savedSetup": saved,
            }
        return await execute(session, work)

    @router.post("/setup", response_model=ServiceResult)
    async def setup(body: ConfirmedRequest[SetupRequest], session=Depends(require_session)):
        request = confirmed(body, "setup")
        return await execute(session, lambda: _setup_call(session, request))

    @router.get("/discovery")
    async def discovery(session=Depends(require_session)):
        def work():
            _same_tenant(session)
            discovery_id = str(uuid4())

            def inventory(tokens, side):
                # These principal-scoped, paginated reads are explicit preparation,
                # never an implicit dependency of recovery or catalog status.
                # https://learn.microsoft.com/rest/api/fabric/core/workspaces/list-workspaces
                # https://learn.microsoft.com/rest/api/fabric/core/capacities/list-capacities
                with FabricClient(tokens) as client:
                    result: dict[str, Any] = {"errors": {}}
                    for kind, read, keys in (
                        ("capacities", workspaces.list_capacities,
                         ("id", "displayName", "region", "state", "sku")),
                        ("workspaces", workspaces.list_workspaces,
                         ("id", "displayName", "capacityId", "capacityRegion", "description")),
                    ):
                        try:
                            entries = read(client)
                        except FabricError as error:
                            result["errors"][kind] = safe_text(
                                f"HTTP {error.status_code}; {error.error_code}: {error.detail or error.body}"
                                if isinstance(error, FabricApiError) else str(error)
                            )
                            logger.warning(
                                "BCDR discovery=%s side=%s kind=%s failed: %s",
                                discovery_id, side, kind, result["errors"][kind],
                            )
                            continue
                        result[kind] = [
                            {key: entry.get(key) for key in keys}
                            for entry in entries if kind != "workspaces" or entry.get("type") == "Workspace"
                        ]
                        logger.info(
                            "BCDR discovery=%s side=%s kind=%s count=%s",
                            discovery_id, side, kind, len(result[kind]),
                        )
                        for entry in result[kind]:
                            logger.info(
                                "BCDR discovery=%s side=%s kind=%s name=%r resource_id=%r capacity_id=%r",
                                discovery_id, side, kind, safe_text(str(entry.get("displayName") or "")),
                                safe_text(str(entry.get("id") or "")), entry.get("capacityId"),
                            )
                    return result

            source = inventory(session.tokens, "source")
            target = inventory(session.destination_tokens, "recovery") if session.paired else {
                **source, "errors": dict(source["errors"]),
            }
            if "capacities" in target:
                try:
                    target["capacityChoices"] = _capacity_choices(
                        session.destination_tokens, target["capacities"],
                    )
                    for choice in target["capacityChoices"]:
                        logger.info(
                            "BCDR discovery=%s capacity=%s name=%r region=%r arm_resource=%s match=%s",
                            discovery_id, choice["id"], safe_text(str(choice.get("displayName") or "")),
                            choice.get("region"), choice["arm_resource_id"], choice["matchStatus"],
                        )
                except (BootstrapError, CapacityError, httpx.HTTPError, ValueError) as error:
                    target["errors"]["capacityMapping"] = safe_text(str(error))
                    logger.warning("BCDR discovery=%s capacity matching failed: %s",
                                   discovery_id, target["errors"]["capacityMapping"])
            return {"source": source, "recovery": target, "discovery_id": discovery_id,
                    "savedSetup": _saved_setup(session)}

        return await execute(session, work)

    @router.get("/status", response_model=ServiceResult)
    async def status(session=Depends(require_session)):
        return await execute(session, lambda: _service_call(session, lambda service: service.status()))

    @router.post("/reconcile-operation", response_model=ServiceResult)
    async def reconcile_operation(
        body: ConfirmedRequest[ReconcileOperationRequest], session=Depends(require_session),
    ):
        request = confirmed(body, "reconcile-operation")
        return await execute(
            session, lambda: _service_call(session, lambda service: service.reconcile_operation(request))
        )

    @router.post("/plan", response_model=ServiceResult)
    async def plan(body: PlanRequest, session=Depends(require_session)):
        return await execute(session, lambda: _service_call(session, lambda service: service.plan(body)))

    @router.post("/synchronize", response_model=ServiceResult)
    async def synchronize(body: ConfirmedRequest[SyncRequest], session=Depends(require_session)):
        request = confirmed(body, "synchronize")
        return await execute(
            session, lambda: _service_call(
                session, lambda service: service.synchronize(request), source_access=request.capture,
            )
        )

    @router.post("/start-dr-test", response_model=ServiceResult)
    async def start_dr_test(body: ConfirmedRequest[StartDrTestRequest], session=Depends(require_session)):
        request = confirmed(body, "start-dr-test")
        return await execute(
            session, lambda: _service_call(session, lambda service: service.start_dr_test(request)),
        )

    @router.post("/continue-dr-test", response_model=ServiceResult)
    async def continue_dr_test(
        body: ConfirmedRequest[ContinueDrTestRequest], session=Depends(require_session),
    ):
        request = confirmed(body, "continue-dr-test")
        return await execute(
            session, lambda: _service_call(session, lambda service: service.continue_dr_test(request)),
        )

    @router.post("/end-dr-test", response_model=ServiceResult)
    async def end_dr_test(body: ConfirmedRequest[EndDrTestRequest], session=Depends(require_session)):
        request = confirmed(body, "end-dr-test")
        return await execute(
            session, lambda: _service_call(session, lambda service: service.end_dr_test(request)),
        )

    @router.post("/schedule-guide", response_model=ServiceResult)
    async def schedule_guide(body: ConfirmedRequest[ScheduleGuideRequest], session=Depends(require_session)):
        request = confirmed(body, "schedule-guide")
        return await execute(
            session, lambda: _service_call(session, lambda service: service.schedule_guide(request)),
        )

    @router.post("/enable-recovery", response_model=ServiceResult)
    async def enable_recovery(
        body: ConfirmedRequest[EnableRecoveryRequest], session=Depends(require_session),
    ):
        request = confirmed(body, "enable-recovery")
        return await execute(
            session, lambda: _service_call(session, lambda service: service.enable_recovery(request))
        )

    @router.post("/configure-protection", response_model=ServiceResult)
    async def configure_protection(
        body: ConfirmedRequest[ConfigureProtectionRequest], session=Depends(require_session),
    ):
        request = confirmed(body, "configure-protection")
        return await execute(
            session, lambda: _service_call(session, lambda service: service.configure_protection(request))
        )

    @router.post("/configure-replica", response_model=ServiceResult)
    async def configure_replica(
        body: ConfirmedRequest[ConfigureReplicaRequest], session=Depends(require_session),
    ):
        request = confirmed(body, "configure-replica")
        return await execute(
            session, lambda: _service_call(session, lambda service: service.configure_replica(request))
        )

    @router.post("/cutover", response_model=ServiceResult)
    async def cutover(body: ConfirmedRequest[CutoverRequest], session=Depends(require_session)):
        request = confirmed(body, "cutover")
        return await execute(
            session, lambda: _service_call(session, lambda service: service.cutover(request))
        )

    @router.post("/plan-failback", response_model=ServiceResult)
    async def plan_failback(body: FailbackRequest, session=Depends(require_session)):
        return await execute(
            session, lambda: _service_call(
                session, lambda service: service.plan_failback(body), source_access=True,
            )
        )

    @router.post("/execute-failback", response_model=ServiceResult)
    async def execute_failback(
        body: ConfirmedRequest[FailbackExecuteRequest], session=Depends(require_session),
    ):
        request = confirmed(body, "execute-failback")
        return await execute(
            session, lambda: _service_call(session, lambda service: service.execute_failback(request))
        )

    @router.post("/cutback", response_model=ServiceResult)
    async def cutback(body: ConfirmedRequest[CutbackRequest], session=Depends(require_session)):
        request = confirmed(body, "cutback")
        return await execute(
            session, lambda: _service_call(session, lambda service: service.cutback(request))
        )

    @router.post("/rearm", response_model=ServiceResult)
    async def rearm(body: ConfirmedRequest[RearmRequest], session=Depends(require_session)):
        request = confirmed(body, "rearm")
        return await execute(
            session, lambda: _service_call(session, lambda service: service.rearm(request))
        )

    return router
