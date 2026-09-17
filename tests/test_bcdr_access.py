from unittest.mock import Mock

import pytest

from fabshuffle.bcdr.access import AccessController, FabricAccess, remap_acl
from fabshuffle.bcdr.backend import DurableRuntime, RecoveryBlocked, safe_error
from fabshuffle.bcdr.contracts import (
    AclScope,
    ConnectionIdentity,
    DesiredAcl,
    ItemIdentity,
    Principal,
    RecoveryMode,
    WorkspaceIdentity,
)
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


def test_unowned_workspace_admin_drift_is_never_removed(system):
    extra = {
        "id": guid(),
        "principal": {"id": guid(), "type": "User"},
        "role": "Admin",
    }
    system.estate.roles[system.config.control_workspace.workspace_id] = [extra]
    with pytest.raises(RecoveryBlocked, match="unowned"):
        system.service.synchronize(system.request)
    assert system.estate.roles[system.config.control_workspace.workspace_id] == [extra]
    assert not any(method == "DELETE" for method, _ in system.estate.calls)


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
