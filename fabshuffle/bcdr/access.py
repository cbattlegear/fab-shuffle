"""Two-stage access: owner workspace roles first; approved business grants only on enable.

REST contracts: Learn core/workspaces and core/connections list/add/delete role-assignment
endpoint pages. Admin/items/list-item-access-details is observation, not an ACL replay API.
SQL, OneLake and model membership are distinct surfaces, never emulated with workspace Admin.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from fabshuffle.bcdr.backend import DurableRuntime, RecoveryBlocked
from fabshuffle.bcdr.contracts import (
    AclScope,
    ConnectionIdentity,
    DesiredAcl,
    ItemIdentity,
    Principal,
    RecoveryMode,
    RecoverySet,
    WorkspaceIdentity,
)
from fabshuffle.fabric.client import FabricClient
from fabshuffle.fabric.workspaces import add_role_assignment, list_role_assignments


def _principal(raw: Mapping[str, Any], tenant_id: str) -> Principal:
    return Principal(tenant_id=tenant_id, object_id=raw["id"], kind=raw["type"])


def target_key(acl: DesiredAcl) -> str:
    target = acl.workspace or acl.item or acl.connection
    if target is None:
        raise ValueError("ACL has no target")
    return target.key


def remap_acl(
    acl: DesiredAcl,
    workspaces: Mapping[str, WorkspaceIdentity],
    items: Mapping[str, ItemIdentity],
    connections: Mapping[str, ConnectionIdentity],
) -> DesiredAcl:
    if acl.workspace:
        target = workspaces.get(acl.workspace.key)
        field = "workspace"
    elif acl.item:
        target = items.get(acl.item.key)
        field = "item"
    else:
        target = connections.get(acl.connection.key)
        field = "connection"
    if target is None:
        raise RecoveryBlocked(f"Map the recovery target for deferred ACL {acl.acl_id} before enabling it")
    return DesiredAcl.model_validate({**acl.model_dump(), field: target.model_dump()})


class FabricAccess:
    def __init__(self, client: FabricClient) -> None:
        self.client = client

    @staticmethod
    def supported(acl: DesiredAcl) -> bool:
        return acl.scope in {AclScope.WORKSPACE, AclScope.CONNECTION}

    @staticmethod
    def path(acl: DesiredAcl) -> str:
        if acl.workspace:
            return f"workspaces/{acl.workspace.workspace_id}/roleAssignments"
        if acl.connection:
            return f"connections/{acl.connection.connection_id}/roleAssignments"
        raise RecoveryBlocked(
            f"Apply and verify {acl.scope} permission '{acl.permission}' for {acl.principal.object_id} "
            f"on {target_key(acl)} using that workload's supported security tooling; "
            "automatic replay of this surface is not qualified"
        )

    def assignments(self, acl: DesiredAcl) -> list[dict[str, Any]]:
        if acl.workspace:
            return list_role_assignments(self.client, acl.workspace.workspace_id)
        return self.client.list_all(self.path(acl))

    def grant(self, acl: DesiredAcl) -> dict[str, Any]:
        if acl.workspace:
            return add_role_assignment(
                self.client,
                acl.workspace.workspace_id,
                acl.principal.object_id,
                acl.principal.kind,
                acl.permission,
            )
        return self.client.post(
            self.path(acl),
            json={
                "principal": {"id": acl.principal.object_id, "type": acl.principal.kind},
                "role": acl.permission,
            },
        )

    def revoke(self, acl: DesiredAcl, assignment_id: str) -> dict[str, Any]:
        self.client.delete(f"{self.path(acl)}/{assignment_id}")
        return {"revoked": assignment_id}

    def inspect_items(self, items: Sequence[tuple[ItemIdentity, str]], allowed: set[str]) -> None:
        for identity, item_type in items:
            result = self.client.get(
                f"admin/workspaces/{identity.workspace_id}/items/{identity.item_id}/users",
                params={"type": item_type},
            )
            if not isinstance(result.get("accessDetails"), list):
                raise RecoveryBlocked(
                    f"Read a complete item access inventory for {identity.key} before syncing"
                )
            for entry in result["accessDetails"]:
                principal = _principal(entry["principal"], identity.tenant_id)
                if principal.key not in allowed:
                    raise RecoveryBlocked(
                        f"Unexpected item access for {principal.object_id} on {identity.key}; "
                        "review and remove the unowned grant explicitly before standby synchronization"
                    )


class AccessController:
    def __init__(self, runtime: DurableRuntime, fabric: FabricAccess, recovery_set: RecoverySet) -> None:
        self.runtime = runtime
        self.fabric = fabric
        self.recovery_set = recovery_set

    def owner_acls(self, workspace: WorkspaceIdentity) -> tuple[DesiredAcl, ...]:
        return tuple(
            DesiredAcl(
                acl_id=str(uuid5(NAMESPACE_URL, f"{workspace.key}/{grant.principal.key}/{grant.role}")),
                scope=AclScope.WORKSPACE,
                workspace=workspace,
                principal=grant.principal,
                permission=grant.role,
                provenance="Explicit recovery owner allowlist",
            )
            for grant in self.recovery_set.access_policy.workspace_grants()
        )

    def restrict_workspace(self, workspace: WorkspaceIdentity) -> None:
        allowed = self.owner_acls(workspace)
        assignments = self.fabric.assignments(allowed[0])
        for row in assignments:
            principal = _principal(row["principal"], workspace.tenant_id)
            if not any(principal == acl.principal and row["role"] == acl.permission for acl in allowed):
                raise RecoveryBlocked(
                    f"Workspace {workspace.key} has unowned {row['role']} access for {principal.object_id}; "
                    "review this drift explicitly, then remove or correct the grant before synchronizing"
                )
        for acl in allowed:
            if not any(
                _principal(row["principal"], workspace.tenant_id) == acl.principal
                and row["role"] == acl.permission
                for row in assignments
            ):
                self.apply(acl, approved=True)

    def apply(self, acl: DesiredAcl, *, approved: bool) -> None:
        self.runtime.fence()
        if not self.recovery_set.access_policy.permits(
            acl,
            mode=self.runtime.mode,
            control_workspace=self.recovery_set.control_workspace,
        ):
            raise RecoveryBlocked(f"ACL {acl.acl_id} stays deferred until explicit Enable recovery")
        if not approved:
            raise RecoveryBlocked(f"Approve deferred ACL {acl.acl_id} explicitly before replay")
        self.fabric.path(acl)
        receipt_key = f"{acl.acl_id}/{target_key(acl)}"
        receipt = self.runtime.get("owned-acls", receipt_key)
        current = self.fabric.assignments(acl)
        matching = [
            row for row in current if _principal(row["principal"], acl.principal.tenant_id) == acl.principal
        ]
        owner = self.recovery_set.access_policy.permits(
            acl,
            mode=RecoveryMode.STANDBY,
            control_workspace=self.recovery_set.control_workspace,
        )
        if matching:
            if len(matching) != 1 or matching[0]["role"] != acl.permission:
                raise RecoveryBlocked(
                    f"Resolve concurrent permission drift for ACL {acl.acl_id}; no Admin fallback"
                )
            if owner or (receipt and receipt["assignment_id"] == matching[0]["id"]):
                return
            raise RecoveryBlocked(
                f"ACL {acl.acl_id} exists without owned receipt; reconcile rather than adopt it"
            )
        result = self.runtime.effect(
            "grant",
            f"{receipt_key}/{self.runtime.require_lease().epoch}",
            lambda: self.fabric.grant(acl),
            target=acl.item,
        )
        if not result.get("id"):
            raise RecoveryBlocked(f"Grant {acl.acl_id} returned no assignment ID; reconcile its operation")
        self.runtime.put(
            "owned-acls",
            receipt_key,
            {
                "acl": acl.model_dump(mode="json"),
                "assignment_id": result["id"],
                "revoked": False,
                "grant_epoch": self.runtime.require_lease().epoch,
            },
        )

    def rollback_grants(self, grants: Sequence[DesiredAcl]) -> None:
        """Undo only this admission attempt's positively owned grants after a definite denial."""
        for acl in reversed(grants):
            key = f"{acl.acl_id}/{target_key(acl)}"
            receipt = self.runtime.get("owned-acls", key)
            if (
                receipt is None
                or receipt["revoked"]
                or receipt.get("grant_epoch") != self.runtime.require_lease().epoch
                or self.recovery_set.access_policy.permits(
                    acl,
                    mode=RecoveryMode.STANDBY,
                    control_workspace=self.recovery_set.control_workspace,
                )
            ):
                continue
            matches = [row for row in self.fabric.assignments(acl) if row["id"] == receipt["assignment_id"]]
            if len(matches) != 1 or (
                matches[0]["role"] != acl.permission
                or _principal(matches[0]["principal"], acl.principal.tenant_id) != acl.principal
            ):
                raise RecoveryBlocked(f"Grant {acl.acl_id} changed during admission; review before rollback")
            self.runtime.effect(
                "rollback-grant",
                f"{key}/{receipt['assignment_id']}",
                lambda a=acl, identifier=receipt["assignment_id"]: self.fabric.revoke(a, identifier),
            )
            self.runtime.put("owned-acls", key, {**receipt, "revoked": True})

    def rearm(self) -> None:
        if self.runtime.mode != RecoveryMode.REARMING:
            raise RecoveryBlocked("Use the explicit rearm transition before removing recovery-applied access")
        for row in self.runtime.catalog.list_records("owned-acls"):
            receipt = row.document
            acl = DesiredAcl.model_validate(receipt["acl"])
            if receipt["revoked"] or self.recovery_set.access_policy.permits(
                acl,
                mode=RecoveryMode.STANDBY,
                control_workspace=self.recovery_set.control_workspace,
            ):
                continue
            current = self.fabric.assignments(acl)
            matching = [entry for entry in current if entry["id"] == receipt["assignment_id"]]
            if matching and (
                len(matching) != 1
                or matching[0]["role"] != acl.permission
                or _principal(matching[0]["principal"], acl.principal.tenant_id) != acl.principal
            ):
                raise RecoveryBlocked(f"Owned ACL {acl.acl_id} changed; review drift instead of deleting it")
            if matching:
                self.runtime.effect(
                    "revoke",
                    f"{row.key}/{receipt['assignment_id']}",
                    lambda a=acl, identifier=receipt["assignment_id"]: self.fabric.revoke(a, identifier),
                )
            self.runtime.put("owned-acls", row.key, {**receipt, "revoked": True})
