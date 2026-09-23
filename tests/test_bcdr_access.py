from unittest.mock import Mock

import pytest

from fabshuffle.bcdr.access import AccessController, FabricAccess, remap_acl
from fabshuffle.bcdr.backend import DurableRuntime, RecoveryBlocked, safe_error
from fabshuffle.bcdr.bootstrap import BootstrapError
from fabshuffle.bcdr.contracts import (
    AclScope,
    ConnectionIdentity,
    DesiredAcl,
    ItemIdentity,
    Principal,
    RecoveryMode,
    WorkspaceIdentity,
)
from fabshuffle.bcdr.control_access import control_workspace_warnings
from fabshuffle.fabric.client import FabricApiError
from tests.test_bcdr_contracts import guid, recovery_set
from tests.test_bcdr_coordinator import system as system


def acl(config, scope, *, target=None):
    workspace = target or WorkspaceIdentity(tenant_id=config.tenant_id, workspace_id=guid())
    fields = {}
    if scope == AclScope.WORKSPACE:
        fields["workspace"] = workspace
    elif scope == AclScope.CONNECTION:
        fields["connection"] = ConnectionIdentity(tenant_id=config.tenant_id, connection_id=guid())
    else:
        fields["item"] = ItemIdentity(
            tenant_id=config.tenant_id,
            workspace_id=workspace.workspace_id,
            item_id=guid(),
        )
    return DesiredAcl(
        acl_id=guid(),
        scope=scope,
        principal=Principal(
            tenant_id=config.tenant_id,
            object_id=guid(),
            kind="User",
        ),
        permission="Viewer" if scope == AclScope.WORKSPACE else "Read",
        provenance="captured source grant",
        **fields,
    )


@pytest.mark.parametrize("scope", list(AclScope))
def test_all_nonowner_surfaces_are_deferred_in_standby(scope):
    config = recovery_set()
    runtime = Mock(spec=DurableRuntime)
    runtime.mode = RecoveryMode.SYNCING
    transport = Mock(spec=FabricAccess)
    access = AccessController(runtime, transport, config)
    with pytest.raises(RecoveryBlocked, match="deferred"):
        access.apply(acl(config, scope), approved=True)
    transport.grant.assert_not_called()


@pytest.mark.parametrize("mode", list(RecoveryMode))
def test_control_workspace_never_allows_business_acl(mode):
    config = recovery_set()
    runtime = Mock(spec=DurableRuntime)
    runtime.mode = mode
    access = AccessController(runtime, Mock(spec=FabricAccess), config)
    with pytest.raises(RecoveryBlocked):
        access.apply(acl(config, AclScope.WORKSPACE, target=config.control_workspace), approved=True)


@pytest.mark.parametrize("scope", [AclScope.ITEM, AclScope.SQL, AclScope.ONELAKE, AclScope.MODEL])
def test_unqualified_surface_is_actionable_not_admin_fallback(scope):
    config = recovery_set()
    transport = FabricAccess(Mock())
    with pytest.raises(RecoveryBlocked, match=str(scope)):
        transport.path(acl(config, scope))
    transport.client.post.assert_not_called()


def test_additional_control_workspace_admin_warns_without_changing_access(system):
    extra = {
        "id": guid(),
        "principal": {"id": guid(), "type": "User", "displayName": "Additional owner"},
        "role": "Admin",
    }
    roles = [
        {"id": guid(), "principal": {"id": grant.principal.object_id, "type": grant.principal.kind},
         "role": grant.role}
        for grant in system.config.access_policy.workspace_grants()
    ] + [extra]
    system.estate.roles[system.config.control_workspace.workspace_id] = roles.copy()
    result = system.service.synchronize(system.request)
    assert any("Additional owner" in warning and "left unchanged" in warning for warning in result.warnings)
    assert system.estate.roles[system.config.control_workspace.workspace_id] == roles
    assert not any(method == "DELETE" for method, _ in system.estate.calls)


@pytest.mark.parametrize("mode", list(RecoveryMode))
def test_control_workspace_membership_is_advisory_in_every_mode(mode):
    config = recovery_set()
    policy = config.access_policy
    rows = [
        {"principal": {"id": policy.recovery_spn.object_id, "type": "ServicePrincipal"}, "role": "Admin"},
        {"principal": {"id": policy.owners[0].principal.object_id, "type": "User",
                       "displayName": "Configured owner"}, "role": "Viewer"},
        {"principal": {"id": guid(), "type": "Group", "displayName": "New team"}, "role": "Member"},
    ]
    runtime = Mock(spec=DurableRuntime)
    runtime.mode = mode
    fabric = Mock(spec=FabricAccess)
    fabric.assignments.return_value = rows
    access = AccessController(runtime, fabric, config)
    warnings = access.restrict_workspace(config.control_workspace)
    assert len(warnings) == 2
    assert "Configured owner has Viewer" in warnings[0] and "New team has Member" in warnings[1]
    fabric.grant.assert_not_called()
    fabric.revoke.assert_not_called()
    runtime.effect.assert_not_called()


@pytest.mark.parametrize("role", [None, "Member", "Viewer"])
def test_controller_admin_access_is_still_required(role):
    config = recovery_set()
    rows = [] if role is None else [{
        "principal": {"id": config.access_policy.recovery_spn.object_id, "type": "ServicePrincipal"},
        "role": role,
    }]
    with pytest.raises(BootstrapError, match="required Admin role"):
        control_workspace_warnings(rows, config.access_policy.recovery_spn, config.access_policy.owners)


def test_missing_control_owner_is_a_warning_not_an_automatic_grant():
    config = recovery_set()
    rows = [{"principal": {"id": config.access_policy.recovery_spn.object_id, "type": "ServicePrincipal"},
             "role": "Admin"}]
    warnings = control_workspace_warnings(
        rows, config.access_policy.recovery_spn, config.access_policy.owners,
    )
    assert len(warnings) == 1 and "no access was added" in warnings[0]


def test_business_workspace_unowned_access_remains_blocking():
    config = recovery_set()
    fabric = Mock(spec=FabricAccess)
    fabric.assignments.return_value = [{
        "principal": {"id": guid(), "type": "User"}, "role": "Admin",
    }]
    access = AccessController(Mock(spec=DurableRuntime), fabric, config)
    with pytest.raises(RecoveryBlocked, match="unowned"):
        access.restrict_workspace(WorkspaceIdentity(tenant_id=config.tenant_id, workspace_id=guid()))
    fabric.grant.assert_not_called()
    fabric.revoke.assert_not_called()


def test_supported_grant_and_owned_revoke_are_journaled(system):
    system.service.synchronize(system.request)
    target = next(iter(system.c.workspace_mappings().values()))
    desired = acl(system.config, AclScope.WORKSPACE, target=target)
    runtime = system.c.runtime
    with runtime.controller({RecoveryMode.STANDBY}):
        runtime.transition(RecoveryMode.ENABLING_RECOVERY)
        system.c.access.apply(desired, approved=True)
        runtime.transition(RecoveryMode.ACTIVE_RECOVERY)
        runtime.transition(RecoveryMode.FAILING_BACK)
        runtime.transition(RecoveryMode.REARMING)
        system.c.access.rearm()
    assert system.catalog.list_records("owned-acls")
    assert any(method == "DELETE" for method, _ in system.estate.calls)
    assert not any(
        row["principal"]["id"] == desired.principal.object_id
        for row in system.estate.roles[target.workspace_id]
    )


def test_rearm_refuses_concurrently_changed_grant(system):
    system.service.synchronize(system.request)
    target = next(iter(system.c.workspace_mappings().values()))
    desired = acl(system.config, AclScope.WORKSPACE, target=target)
    runtime = system.c.runtime
    with runtime.controller({RecoveryMode.STANDBY}):
        runtime.transition(RecoveryMode.ENABLING_RECOVERY)
        system.c.access.apply(desired, approved=True)
        runtime.transition(RecoveryMode.ACTIVE_RECOVERY)
        runtime.transition(RecoveryMode.FAILING_BACK)
        runtime.transition(RecoveryMode.REARMING)
        next(
            row
            for row in system.estate.roles[target.workspace_id]
            if row["principal"]["id"] == desired.principal.object_id
        )["role"] = "Admin"
        with pytest.raises(RecoveryBlocked, match="changed"):
            system.c.access.rearm()
    assert not any(method == "DELETE" for method, _ in system.estate.calls)


def test_remap_never_uses_source_target_when_missing():
    config = recovery_set()
    with pytest.raises(RecoveryBlocked, match="Map the recovery target"):
        remap_acl(acl(config, AclScope.WORKSPACE), {}, {}, {})


def test_service_words_survive_credential_redaction():
    error = FabricApiError(
        "POST",
        "https://example.test",
        403,
        (
            '{"errorCode":"PermissionDenied","message":"Grant User on connection; password=do-not-leak",'
            '"requestId":"evidence-request"}'
        ),
    )
    code, message, request_id = safe_error(error)
    assert code == "PermissionDenied"
    assert "Grant User on connection" in message
    assert request_id == "evidence-request"
    assert "do-not-leak" not in message


def test_reenable_after_rearm_creates_new_owned_grant(system):
    system.service.synchronize(system.request)
    target = next(iter(system.c.workspace_mappings().values()))
    desired = acl(system.config, AclScope.WORKSPACE, target=target)
    runtime = system.c.runtime
    with runtime.controller({RecoveryMode.STANDBY}):
        runtime.transition(RecoveryMode.ENABLING_RECOVERY)
        system.c.access.apply(desired, approved=True)
        first_id = next(
            row["id"]
            for row in system.estate.roles[target.workspace_id]
            if row["principal"]["id"] == desired.principal.object_id
        )
        runtime.transition(RecoveryMode.ACTIVE_RECOVERY)
        runtime.transition(RecoveryMode.FAILING_BACK)
        runtime.transition(RecoveryMode.REARMING)
        system.c.access.rearm()
        runtime.transition(RecoveryMode.STANDBY)
    with runtime.controller({RecoveryMode.STANDBY}):
        runtime.transition(RecoveryMode.ENABLING_RECOVERY)
        system.c.access.apply(desired, approved=True)
    new_id = next(
        row["id"]
        for row in system.estate.roles[target.workspace_id]
        if row["principal"]["id"] == desired.principal.object_id
    )
    assert new_id != first_id


def test_permission_denial_rolls_back_only_current_group_grants_and_can_retry(system):
    from fabshuffle.bcdr.catalog import CapturedGeneration
    from fabshuffle.bcdr.service import EnableRecoveryRequest
    from tests.test_bcdr_coordinator import proofs

    source_workspace = system.captured.workspaces[0].identity
    grants = tuple(acl(system.config, AclScope.WORKSPACE, target=source_workspace) for _ in range(2))
    captured = system.captured.model_copy(update={"desired_acls": grants})
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(captured, ())
    synced = system.service.synchronize(system.request)
    original = system.c.access.fabric.grant

    def denied(assignment):
        if assignment.principal == grants[1].principal:
            raise FabricApiError(
                "POST",
                "https://api.fabric.microsoft.com/v1/roleAssignments",
                403,
                (
                    '{"errorCode":"PermissionDenied","message":"Grant assignment denied",'
                    '"requestId":"request-42"}'
                ),
            )
        return original(assignment)

    system.c.access.fabric.grant = denied
    request = EnableRecoveryRequest(
        generation_id=synced.generation_id,
        group_ids=(synced.groups[0].group_id,),
        approved_acl_ids=tuple(row.acl_id for row in grants),
        readiness=proofs(system, synced.generation_id),
    )
    first = system.service.enable_recovery(request)
    assert not first.groups[0].access_enabled
    assert "PermissionDenied" in first.groups[0].blockers[0]
    assert not system.catalog.pending_operations()
    target = next(iter(system.c.workspace_mappings().values()))
    assert not any(
        row["principal"]["id"] in {acl.principal.object_id for acl in grants}
        for row in system.estate.roles[target.workspace_id]
    )
    system.c.access.fabric.grant = original
    retried = system.service.enable_recovery(request)
    assert retried.groups[0].access_enabled
