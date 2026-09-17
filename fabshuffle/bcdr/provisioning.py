"""SPN control workspace/Warehouse creation with exact-ID ownership receipts.

No name adoption, definition upload, SQL execution or source ACL replay occurs.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from typing import Self
from urllib.parse import parse_qs
from uuid import UUID, uuid4

import httpx

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.bootstrap import (
    BootstrapDescriptor,
    BootstrapError,
    BootstrapStore,
    WarehouseIntent,
    WorkspaceIntent,
    WorkspacePrincipal,
    WorkspaceRoleIntent,
)
from fabshuffle.bcdr.capacity import CapacityError, _https_url, response_body, retry_after

FABRIC_BASE = "https://api.fabric.microsoft.com/v1"


class WarehouseCreationUnknown(BootstrapError):
    """A create cannot be replayed or adopted by name without ownership evidence."""


class _ControlProvisioner:
    def __init__(
        self, store: BootstrapStore, tokens: TokenProvider, *,
        transport: httpx.BaseTransport | None = None,
        timeout_seconds: float = 900, max_attempts: int = 4,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if timeout_seconds <= 0 or max_attempts < 1:
            raise ValueError("Provisioning timeout and retry count must be positive")
        self.store = store
        self.tokens = tokens
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.sleep = sleep
        self.clock = clock
        self.http = httpx.Client(transport=transport, timeout=60, follow_redirects=False)

    def close(self) -> None:
        self.http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _request(
        self, method: str, path: str, *, json: dict | None = None, params: dict | None = None,
    ) -> httpx.Response:
        descriptor = self.store.load()
        workspace_path = f"/v1/workspaces/{descriptor.control_workspace_id}"
        role_path = workspace_path + "/roleAssignments"
        allowed = {workspace_path, workspace_path + "/warehouses", role_path}
        if method == "POST":
            allowed.add("/v1/workspaces")
        if descriptor.control_warehouse_id:
            allowed.add(workspace_path + "/warehouses/" + descriptor.control_warehouse_id)
        if descriptor.warehouse_intent and descriptor.warehouse_intent.operation_id:
            operation = f"/v1/operations/{descriptor.warehouse_intent.operation_id}"
            allowed.update({operation, operation + "/result"})
        url = FABRIC_BASE + path
        parsed = _https_url(url, "api.fabric.microsoft.com")
        if parsed.path not in allowed or parsed.query:
            raise BootstrapError("Fabric request is outside the recorded control-resource/operation scope")
        if params and (
            method != "GET" or parsed.path != role_path or set(params) != {"continuationToken"}
        ):
            raise BootstrapError("Unexpected Fabric request query")
        for attempt in range(self.max_attempts):
            try:
                response = self.http.request(
                    method, url, json=json, params=params,
                    headers={"Authorization": f"Bearer {self.tokens.fabric_token()}"},
                )
            except httpx.TransportError as error:
                if method != "GET":
                    raise WarehouseCreationUnknown(
                        "Control-resource mutation response was lost. Reconcile the recorded intent; "
                        "do not adopt by name."
                    ) from error
                if attempt + 1 == self.max_attempts:
                    raise
                self.sleep(min(2 ** attempt, 30))
                continue
            if (
                response.status_code == 429
                or (method == "GET" and response.status_code in {500, 502, 503, 504})
            ) and attempt + 1 < self.max_attempts:
                self.sleep(retry_after(response))
                continue
            if not response.is_success:
                raise CapacityError(response, context=f"Fabric control provisioning {method}")
            return response
        raise AssertionError("Fabric request attempts exhausted")


class ControlWarehouseProvisioner(_ControlProvisioner):
    @staticmethod
    def _validate_location(response: httpx.Response, operation_id: str) -> None:
        location = response.headers.get("Location")
        if location:
            parsed = _https_url(location, "api.fabric.microsoft.com")
            operation_path = f"/v1/operations/{operation_id}"
            if parsed.path not in {operation_path, operation_path + "/result"} or parsed.query:
                raise BootstrapError("Fabric returned a Location outside the recorded Warehouse operation")
        returned_id = response.headers.get("x-ms-operation-id")
        if returned_id and str(UUID(returned_id)) != operation_id:
            raise BootstrapError("Fabric returned a different operation ID")

    def _persist(self, intent: WarehouseIntent, **updates: object) -> BootstrapDescriptor:
        def change(current: BootstrapDescriptor) -> BootstrapDescriptor:
            if (
                current.warehouse_intent is None
                or current.warehouse_intent.intent_id != intent.intent_id
                or current.controller_id != intent.owner_id
            ):
                raise BootstrapError("Warehouse provisioning ownership changed; stop and reconcile")
            return current.model_copy(update={"warehouse_intent": intent, **updates})

        return self.store.update(change)

    def ensure(self, *, display_name: str, owner_id: str) -> BootstrapDescriptor:
        """Create once, or resume the exact recorded operation/resource.

        An operator-explicit existing workspace ID needs no workspace creation
        receipt. The setup caller must establish and verify its SPN/owner-only
        access before this method; explicit ID selection is not name adoption.

        Create/Get Warehouse support SPNs; the endpoint's Location and operation-ID
        contract is documented here, not inferred from generic item APIs:
        https://learn.microsoft.com/rest/api/fabric/warehouse/items/create-warehouse
        https://learn.microsoft.com/rest/api/fabric/articles/long-running-operation
        """
        descriptor = self.store.load()
        if (
            descriptor.control_workspace_id is None
            or (descriptor.workspace_intent is not None and descriptor.workspace_intent.phase != "ready")
        ):
            raise BootstrapError("Prepare the restricted control workspace before creating its Warehouse")
        if (
            str(UUID(owner_id)) != descriptor.controller_id
            or self.tokens.tenant_id() != descriptor.tenant_id
            or str(UUID(self.tokens.principal.client_id)) != descriptor.application_id
        ):
            raise BootstrapError("Authenticated tenant/application/controller does not own this bootstrap")
        deadline = self.clock() + self.timeout_seconds
        intent = descriptor.warehouse_intent
        if intent and intent.phase in {"intent", "failed"}:
            raise WarehouseCreationUnknown(
                "Existing Warehouse intent has no successful ownership receipt. Reconcile with the service "
                "or operator; never replay this create or adopt an item by display name."
            )
        if intent is None and descriptor.control_warehouse_id is not None:
            raise BootstrapError("Control Warehouse ID has no recorded provisioning ownership intent")
        if intent is not None and intent.display_name != display_name:
            raise BootstrapError("Requested Warehouse name does not match the recorded provisioning intent")
        workspace = response_body(self._request("GET", f"/workspaces/{descriptor.control_workspace_id}"))
        catalog_capacity = descriptor.capacity(descriptor.catalog_capacity_id)
        if (
            str(UUID(str(workspace.get("id", "")))) != descriptor.control_workspace_id
            or str(UUID(str(workspace.get("capacityId", "")))) != catalog_capacity.fabric_capacity_id
            or workspace.get("capacityAssignmentProgress") != "Completed"
        ):
            raise BootstrapError("Control workspace is not settled on the recorded Fabric catalog capacity")
        if intent is None:
            intent = WarehouseIntent(
                intent_id=str(uuid4()), owner_id=descriptor.controller_id, display_name=display_name,
            )
            descriptor = self.store.save(
                descriptor.model_copy(update={"warehouse_intent": intent}),
                expected_revision=descriptor.revision,
            )
            response = self._request(
                "POST", f"/workspaces/{descriptor.control_workspace_id}/warehouses",
                json={
                    "displayName": display_name,
                    "description": f"BCDR control {descriptor.recovery_set_id}",
                },
            )
            if response.status_code == 202:
                operation_id = response.headers.get("x-ms-operation-id")
                if not operation_id:
                    location = response.headers.get("Location", "")
                    parsed = _https_url(location, "api.fabric.microsoft.com")
                    operation_id = parsed.path.rsplit("/", 1)[-1]
                operation_id = str(UUID(operation_id))
                intent = intent.model_copy(update={
                    "phase": "accepted", "operation_id": operation_id,
                    "request_id": response.headers.get("request-id"),
                })
                self._persist(intent)
                self._validate_location(response, operation_id)
                self._wait(retry_after(response), deadline)
            elif response.status_code == 201:
                intent = self._created(intent, response_body(response), descriptor)
            else:
                raise WarehouseCreationUnknown("Warehouse create returned an undocumented success response")
        if intent.phase == "accepted":
            while True:
                self._wait(0, deadline)
                response = self._request("GET", f"/operations/{intent.operation_id}")
                self._validate_location(response, intent.operation_id)
                status = str(response_body(response).get("status", ""))
                if status in {"Failed", "Canceled", "Cancelled", "Undefined"}:
                    self._persist(intent.model_copy(update={"phase": "failed"}))
                    raise CapacityError(response, context=f"Fabric Warehouse operation {status}")
                if status == "Succeeded":
                    result = self._request("GET", f"/operations/{intent.operation_id}/result")
                    self._validate_location(result, intent.operation_id)
                    intent = self._created(intent, response_body(result), descriptor)
                    break
                self._wait(retry_after(response), deadline)
        while True:
            self._wait(0, deadline)
            response = self._request(
                "GET", f"/workspaces/{descriptor.control_workspace_id}/warehouses/{intent.warehouse_id}",
            )
            body = response_body(response)
            self._check_resource(body, descriptor, expected_id=intent.warehouse_id)
            host = body.get("properties", {}).get("connectionString")
            if host:
                # Learn confirms ID is supported as InitialCatalog; it avoids rename/name-match adoption.
                # https://learn.microsoft.com/fabric/data-warehouse/connectivity
                return self._persist(
                    intent.model_copy(update={"phase": "ready"}),
                    tds_host=host, tds_catalog=intent.warehouse_id,
                )
            self._wait(retry_after(response), deadline)

    def _created(
        self, intent: WarehouseIntent, body: dict, descriptor: BootstrapDescriptor,
    ) -> WarehouseIntent:
        warehouse_id = self._check_resource(body, descriptor)
        intent = intent.model_copy(update={"phase": "created", "warehouse_id": warehouse_id})
        self._persist(intent, control_warehouse_id=warehouse_id)
        return intent

    @staticmethod
    def _check_resource(
        body: dict, descriptor: BootstrapDescriptor, *, expected_id: str | None = None,
    ) -> str:
        warehouse_id = str(UUID(str(body.get("id", ""))))
        if (
            body.get("type") != "Warehouse"
            or str(UUID(str(body.get("workspaceId", "")))) != descriptor.control_workspace_id
            or (expected_id is not None and expected_id != warehouse_id)
        ):
            raise BootstrapError("Fabric returned a different resource than the owned control Warehouse")
        return warehouse_id

    def _wait(self, delay: float, deadline: float) -> None:
        if self.clock() + delay >= deadline:
            raise WarehouseCreationUnknown(
                "Control Warehouse readiness timed out; resume its recorded operation/resource, not its name"
            )
        self.sleep(delay)


class ControlWorkspaceProvisioner(_ControlProvisioner):
    """Create on the designated capacity, then verify exact SPN/owner-only roles.

    Existing exact IDs use the separate read-only verify_existing path and are not
    claimed as created resources. Owner grant intent for new workspaces is
    persisted before POST and reconciled by exact principal/assignment ID.
    https://learn.microsoft.com/rest/api/fabric/core/workspaces/create-workspace
    https://learn.microsoft.com/rest/api/fabric/core/workspaces/add-workspace-role-assignment
    """

    def _owners(
        self, *, owner_id: str,
        allowed_principals: tuple[WorkspacePrincipal, ...],
    ) -> tuple[BootstrapDescriptor, dict[str, WorkspacePrincipal], str]:
        descriptor = self.store.load()
        if (
            str(UUID(owner_id)) != descriptor.controller_id
            or self.tokens.tenant_id() != descriptor.tenant_id
            or str(UUID(self.tokens.principal.client_id)) != descriptor.application_id
        ):
            raise BootstrapError("Authenticated tenant/application/controller does not own this bootstrap")
        spn = WorkspacePrincipal(object_id=self.tokens.object_id(), principal_type="ServicePrincipal")
        principals = {}
        for principal in (spn, *allowed_principals):
            principal = WorkspacePrincipal.model_validate(principal.model_dump())
            existing = principals.get(principal.object_id)
            if existing is not None and existing != principal:
                raise BootstrapError("A designated owner ID has conflicting principal types")
            principals[principal.object_id] = principal
        scope_digest = hashlib.sha256(json.dumps(
            [principals[key].model_dump(mode="json") for key in sorted(principals)], sort_keys=True,
        ).encode("utf-8")).hexdigest()
        return descriptor, principals, scope_digest

    def ensure(
        self, *, display_name: str, owner_id: str,
        allowed_principals: tuple[WorkspacePrincipal, ...],
    ) -> BootstrapDescriptor:
        descriptor, principals, scope_digest = self._owners(
            owner_id=owner_id, allowed_principals=allowed_principals,
        )
        intent = descriptor.workspace_intent
        if intent is not None:
            if intent.origin == "designated":
                return self.verify_existing(owner_id=owner_id, allowed_principals=allowed_principals)
            if intent.display_name != display_name or intent.owner_scope_sha256 != scope_digest:
                raise BootstrapError(
                    "Workspace name/owner allowlist changed; reconcile the original setup intent"
                )
            if intent.phase == "intent":
                raise WarehouseCreationUnknown(
                    "Workspace create has no returned ownership receipt; reconcile it, never adopt by name"
                )
        elif descriptor.control_workspace_id is not None:
            raise BootstrapError(
                "Existing control workspace has no recorded creation ownership; do not adopt"
            )
        else:
            intent = WorkspaceIntent(
                owner_id=descriptor.controller_id, intent_id=str(uuid4()),
                display_name=display_name, owner_scope_sha256=scope_digest,
            )
            descriptor = self.store.save(
                descriptor.model_copy(update={"workspace_intent": intent}),
                expected_revision=descriptor.revision,
            )
            response = self._request("POST", "/workspaces", json={
                "displayName": display_name,
                "capacityId": descriptor.capacity(descriptor.catalog_capacity_id).fabric_capacity_id,
                "description": f"BCDR restricted control {descriptor.recovery_set_id}",
            })
            if response.status_code != 201:
                raise WarehouseCreationUnknown("Workspace create returned an undocumented success response")
            body = response_body(response)
            workspace_id = str(UUID(str(body.get("id", ""))))
            if body.get("type") != "Workspace":
                raise BootstrapError("Fabric returned an unexpected control workspace resource type")
            intent = intent.model_copy(update={"phase": "created", "workspace_id": workspace_id})
            descriptor = self._persist_workspace(intent, control_workspace_id=workspace_id)
            self._resource_location(response, f"/v1/workspaces/{workspace_id}")
        deadline = self.clock() + self.timeout_seconds
        while True:
            response = self._request("GET", f"/workspaces/{intent.workspace_id}")
            body = response_body(response)
            if str(UUID(str(body.get("id", "")))) != intent.workspace_id:
                raise BootstrapError("Fabric returned another workspace identity")
            progress = body.get("capacityAssignmentProgress")
            if progress == "Completed":
                if (
                    str(UUID(str(body.get("capacityId", ""))))
                    != descriptor.capacity(descriptor.catalog_capacity_id).fabric_capacity_id
                ):
                    raise BootstrapError("Control workspace is assigned to another Fabric capacity")
                break
            if progress == "Failed":
                raise BootstrapError(
                    "Control workspace capacity assignment failed; reconcile before continuing"
                )
            delay = retry_after(response)
            if self.clock() + delay >= deadline:
                raise WarehouseCreationUnknown("Control workspace capacity assignment is not yet ready")
            self.sleep(delay)
        intent = self._restrict_roles(intent, principals)
        return self._persist_workspace(intent.model_copy(update={"phase": "ready"}))

    def verify_existing(
        self, *, owner_id: str, allowed_principals: tuple[WorkspacePrincipal, ...],
    ) -> BootstrapDescriptor:
        """Audit an explicitly supplied workspace GUID without granting or removing access."""
        descriptor, principals, scope_digest = self._owners(
            owner_id=owner_id, allowed_principals=allowed_principals,
        )
        workspace_id = descriptor.control_workspace_id
        if workspace_id is None:
            raise BootstrapError(
                "Supply an exact designated workspace ID; no name lookup or adoption is allowed"
            )
        intent = descriptor.workspace_intent
        if intent is not None and (
            intent.phase != "ready" or intent.owner_scope_sha256 != scope_digest
        ):
            raise BootstrapError(
                "Reconcile the original workspace/owner intent before changing its designation"
            )
        response = self._request("GET", f"/workspaces/{workspace_id}")
        body = response_body(response)
        if (
            str(UUID(str(body.get("id", "")))) != workspace_id
            or str(UUID(str(body.get("capacityId", ""))))
            != descriptor.capacity(descriptor.catalog_capacity_id).fabric_capacity_id
            or body.get("capacityAssignmentProgress") != "Completed"
        ):
            raise BootstrapError("Designated workspace is not settled on the recorded catalog capacity")
        roles = self._roles(workspace_id)
        self._check_roles(roles, principals)
        if set(roles) != set(principals):
            raise BootstrapError(
                "Assign only the recovery SPN and every designated owner the Admin role, "
                "then rerun verification"
            )
        if intent is None:
            intent = WorkspaceIntent(
                owner_id=descriptor.controller_id, intent_id=str(uuid4()), origin="designated",
                display_name=str(body.get("displayName") or workspace_id),
                owner_scope_sha256=scope_digest, phase="ready", workspace_id=workspace_id,
            )
        return self.store.save(
            descriptor.model_copy(update={"workspace_intent": intent}),
            expected_revision=descriptor.revision,
        )

    def _persist_workspace(self, intent: WorkspaceIntent, **updates: object) -> BootstrapDescriptor:
        def change(current: BootstrapDescriptor) -> BootstrapDescriptor:
            if (
                current.workspace_intent is None or current.workspace_intent.intent_id != intent.intent_id
                or current.controller_id != intent.owner_id
            ):
                raise BootstrapError("Control workspace provisioning ownership changed")
            return current.model_copy(update={"workspace_intent": intent, **updates})

        return self.store.update(change)

    @staticmethod
    def _resource_location(response: httpx.Response, expected_path: str) -> None:
        location = response.headers.get("Location")
        if location:
            parsed = _https_url(location, "api.fabric.microsoft.com")
            if parsed.path != expected_path or parsed.query:
                raise BootstrapError("Fabric Location does not identify the recorded control resource")

    def _roles(self, workspace_id: str) -> dict[str, dict]:
        roles = {}
        token = None
        seen = set()
        while True:
            response = self._request(
                "GET", f"/workspaces/{workspace_id}/roleAssignments",
                params={"continuationToken": token} if token else None,
            )
            body = response_body(response)
            values = body.get("value")
            if not isinstance(values, list):
                raise BootstrapError("Fabric did not return a complete control-workspace role inventory")
            for role in values:
                if not isinstance(role, dict) or not isinstance(role.get("principal"), dict):
                    raise BootstrapError("Fabric returned a malformed workspace role assignment")
                principal_id = str(UUID(str(role["principal"].get("id", ""))))
                if principal_id in roles:
                    raise BootstrapError(
                        "Duplicate control workspace principal assignments; reconcile access"
                    )
                role["id"] = str(UUID(str(role.get("id", ""))))
                roles[principal_id] = role
            if len(roles) > 1000:
                raise BootstrapError("Control workspace role inventory exceeds the supported service limit")
            uri = body.get("continuationUri")
            if uri:
                parsed = _https_url(uri, "api.fabric.microsoft.com")
                if (
                    parsed.path != f"/v1/workspaces/{workspace_id}/roleAssignments"
                    or set(parse_qs(parsed.query)) != {"continuationToken"}
                ):
                    raise BootstrapError(
                        "Role inventory continuation is outside the control workspace scope"
                    )
            token = body.get("continuationToken")
            if not token:
                if uri:
                    raise BootstrapError(
                        "Role inventory continuation has no token; cannot prove restricted access"
                    )
                return roles
            if not isinstance(token, str) or token in seen:
                raise BootstrapError("Role inventory continuation is invalid or repeated")
            seen.add(token)

    @staticmethod
    def _check_roles(roles: dict[str, dict], principals: dict[str, WorkspacePrincipal]) -> None:
        for principal_id, assignment in roles.items():
            expected = principals.get(principal_id)
            if expected is None:
                raise BootstrapError(
                    f"Remove unexpected principal {principal_id} from the control workspace, "
                    "then resume setup"
                )
            if (
                assignment["principal"].get("type") != expected.principal_type
                or assignment.get("role") != expected.role
            ):
                raise BootstrapError(
                    f"Reconcile the control workspace role for designated owner {principal_id}; "
                    "expected Admin"
                )

    def _restrict_roles(
        self, intent: WorkspaceIntent, principals: dict[str, WorkspacePrincipal],
    ) -> WorkspaceIntent:
        roles = self._roles(intent.workspace_id)
        self._check_roles(roles, principals)
        if intent.pending_role:
            pending = intent.pending_role
            observed = roles.get(pending.principal.object_id)
            if observed is None or (
                pending.assignment_id is not None and pending.assignment_id != observed["id"]
            ):
                raise WarehouseCreationUnknown(
                    "Owner role mutation has no matching assignment receipt; "
                    "reconcile without repeating the grant"
                )
            intent = intent.model_copy(update={"pending_role": None})
            self._persist_workspace(intent)
        for principal in principals.values():
            if principal.object_id in roles:
                continue
            pending = WorkspaceRoleIntent(intent_id=str(uuid4()), principal=principal)
            intent = intent.model_copy(update={"phase": "created", "pending_role": pending})
            self._persist_workspace(intent)
            response = self._request("POST", f"/workspaces/{intent.workspace_id}/roleAssignments", json={
                "principal": {"id": principal.object_id, "type": principal.principal_type},
                "role": principal.role,
            })
            if response.status_code != 201:
                raise WarehouseCreationUnknown("Owner role grant returned an undocumented success response")
            result = response_body(response)
            assignment_id = str(UUID(str(result.get("id", ""))))
            observed_principal = result.get("principal", {})
            if (
                str(UUID(str(observed_principal.get("id", "")))) != principal.object_id
                or observed_principal.get("type") != principal.principal_type
                or result.get("role") != principal.role
            ):
                raise BootstrapError("Fabric returned a different role assignment than the designated owner")
            intent = intent.model_copy(update={
                "pending_role": pending.model_copy(update={"assignment_id": assignment_id}),
            })
            self._persist_workspace(intent)
            self._resource_location(
                response, f"/v1/workspaces/{intent.workspace_id}/roleAssignments/{assignment_id}",
            )
            roles = self._roles(intent.workspace_id)
            self._check_roles(roles, principals)
            if roles.get(principal.object_id, {}).get("id") != assignment_id:
                raise WarehouseCreationUnknown(
                    "Owner role grant is not yet observable; resume setup to reconcile"
                )
            intent = intent.model_copy(update={"pending_role": None})
            self._persist_workspace(intent)
        roles = self._roles(intent.workspace_id)
        self._check_roles(roles, principals)
        if set(roles) != set(principals):
            raise BootstrapError(
                "Control workspace owner/SPN role inventory is incomplete; reconcile access"
            )
        return intent
