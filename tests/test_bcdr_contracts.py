from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from fabshuffle.bcdr.contracts import (
    AclScope,
    CaptureSnapshot,
    ConnectionIdentity,
    DependencyEdge,
    DesiredAcl,
    EndpointIdentity,
    ItemIdentity,
    ItemRecord,
    Principal,
    ProtectionKind,
    ProtectionRecord,
    Qualification,
    RecoveryDataBinding,
    RecoveryMode,
    RecoveryOutcome,
    RecoverySet,
    RecoveryStrategy,
    StandbyAccessPolicy,
    WorkspaceGrant,
    WorkspaceIdentity,
    WorkspaceRecord,
    logical_path,
    reject_embedded_secrets,
)
from fabshuffle.bcdr.registry import TYPE_REGISTRY, validate_registry
from fabshuffle.fabric.support import POWER_BI_TYPES, REBUILT_TYPES


def guid():
    return str(uuid4())


def recovery_set():
    tenant, workspace = guid(), guid()
    return RecoverySet(
        recovery_set_id=guid(), tenant_id=tenant,
        control_workspace=WorkspaceIdentity(tenant_id=tenant, workspace_id=workspace),
        control_warehouse=ItemIdentity(tenant_id=tenant, workspace_id=workspace, item_id=guid()),
        access_policy=StandbyAccessPolicy(
            recovery_spn=Principal(tenant_id=tenant, object_id=guid(), kind="ServicePrincipal"),
            owners=(WorkspaceGrant(principal=Principal(
                tenant_id=tenant, object_id=guid(), kind="User",
            ), role="Admin"),),
        ),
        source_capacity_ids=(guid(),), target_capacity_ids=(guid(),),
    )


def snapshot(config, *, generation_id=None, parent=None):
    now = datetime.now(UTC)
    workspace = WorkspaceIdentity(tenant_id=config.tenant_id, workspace_id=guid())
    return CaptureSnapshot(
        recovery_set_id=config.recovery_set_id, generation_id=generation_id or guid(),
        parent_generation_id=parent, captured_at=now, inventory_complete=True,
        workspaces=(WorkspaceRecord(
            identity=workspace, capacity_id=config.source_capacity_ids[0], display_name="Source",
            captured_at=now, inventory_complete=True,
        ),),
        items=(ItemRecord(
            identity=ItemIdentity(**workspace.model_dump(exclude={"schema_version"}), item_id=guid()),
            item_type="Notebook", display_name="A notebook", api_version="v1",
            captured_at=now, capture_complete=True,
        ),),
    )


def test_registry_covers_every_current_type_without_claiming_outage_rebuild():
    validate_registry()
    assert len(REBUILT_TYPES) == 28
    assert set(TYPE_REGISTRY) == REBUILT_TYPES | POWER_BI_TYPES
    assert not TYPE_REGISTRY["Dashboard"].migration_rebuild
    assert TYPE_REGISTRY["PaginatedReport"].restore_qualification == Qualification.MANUAL
    assert TYPE_REGISTRY["SemanticModel"].migration_rebuild
    assert all(
        contract.failback_qualification == Qualification.UNVERIFIED for contract in TYPE_REGISTRY.values()
    )
    assert "geo-replicated" in TYPE_REGISTRY["SQLDatabase"].restrictions[0]
    assert {"definition", "data_access_roles"} <= set(TYPE_REGISTRY["Lakehouse"].capture_requirements)
    assert "definition" in TYPE_REGISTRY["Eventhouse"].capture_requirements


def test_identity_is_qualified_normalized_and_frozen():
    tenant, workspace, item = guid(), guid(), guid()
    identity = ItemIdentity(tenant_id=tenant.upper(), workspace_id=workspace.upper(), item_id=item.upper())
    assert identity.key == f"{tenant}/{workspace}/{item}"
    assert identity != ItemIdentity(tenant_id=tenant, workspace_id=guid(), item_id=item)
    with pytest.raises(ValidationError):
        identity.item_id = guid()
    with pytest.raises(ValidationError):
        ItemIdentity(tenant_id="unqualified-name", workspace_id=workspace, item_id=item)
    with pytest.raises(ValidationError):
        ItemIdentity(tenant_id=tenant, workspace_id=workspace, item_id=item, secret="not-allowed")


@pytest.mark.parametrize("path", ["../Main.py", "/etc/passwd", r"C:\Main.py", r"Main\..\x", "a//b",
                                 "a/./b", "a/%2e%2e/b", "a\x00b", "https://example/a"])
def test_unsafe_logical_paths_rejected(path):
    with pytest.raises(ValueError):
        logical_path(path)


def test_snapshot_rejects_duplicate_and_unowned_records():
    config = recovery_set()
    value = snapshot(config).model_dump(mode="json")
    value["items"].append(value["items"][0])
    with pytest.raises(ValidationError, match="Duplicate"):
        CaptureSnapshot.model_validate(value)
    value["items"] = value["items"][:1]
    value["items"][0]["identity"]["workspace_id"] = guid()
    with pytest.raises(ValidationError, match="captured workspace"):
        CaptureSnapshot.model_validate(value)


def test_publication_distinguishes_partial_capture_and_control_workspace():
    config = recovery_set()
    capture = snapshot(config)
    capture.require_publishable(config)
    with pytest.raises(ValueError, match="inventory"):
        capture.model_copy(update={"inventory_complete": False}).require_publishable(config)
    workspace = capture.workspaces[0].model_copy(update={"identity": config.control_workspace})
    with pytest.raises(ValueError, match="control workspace"):
        capture.model_copy(update={"workspaces": (workspace,)}).require_publishable(config)


def test_inventory_only_cannot_hide_missing_metadata_for_a_supported_type():
    config = recovery_set()
    capture = snapshot(config)
    item = capture.items[0].model_copy(update={
        "capture_complete": False,
        "properties": {"bcdr": {"inventory_only": True, "unsupported_reason": "Pretend it is unsupported"}},
    })
    with pytest.raises(ValueError, match="Invalid unsupported-inventory"):
        capture.model_copy(update={"items": (item,)}).require_publishable(config)

def test_standby_is_only_spn_and_explicit_owner_workspace_roles_forever_on_control():
    config = recovery_set()
    policy = config.access_policy
    user = Principal(tenant_id=config.tenant_id, object_id=guid(), kind="User")
    for mode in RecoveryMode:
        for grant in policy.workspace_grants():
            acl = DesiredAcl(
                acl_id=guid(), scope=AclScope.WORKSPACE, workspace=config.control_workspace,
                principal=grant.principal, permission=grant.role, provenance="explicit deployment owner",
            )
            assert policy.permits(acl, mode=mode, control_workspace=config.control_workspace)
        forbidden = acl.model_copy(update={"principal": user})
        assert not policy.permits(forbidden, mode=mode, control_workspace=config.control_workspace)
        item_grant = DesiredAcl(
            acl_id=guid(), scope=AclScope.ITEM, item=config.control_warehouse,
            principal=policy.recovery_spn, permission="Read", provenance="captured item share",
        )
        assert not policy.permits(item_grant, mode=mode, control_workspace=config.control_workspace)
    business = forbidden.model_copy(update={"workspace": WorkspaceIdentity(
        tenant_id=config.tenant_id, workspace_id=guid(),
    )})
    assert not policy.permits(business, mode=RecoveryMode.SYNCING, control_workspace=config.control_workspace)
    assert policy.permits(business, mode=RecoveryMode.ENABLING_RECOVERY,
                          control_workspace=config.control_workspace)


def test_unprotected_never_means_protected_and_warning_is_actionable():
    item = snapshot(recovery_set()).items[0]
    with pytest.raises(ValidationError, match="Missing protection"):
        ProtectionRecord(
            protection_id=guid(), item=item.identity, kind=ProtectionKind.UNPROTECTED,
            outcome=RecoveryOutcome.PROTECTED, provenance="capture", action="Supply an off-region export",
        )
    record = ProtectionRecord(
        protection_id=guid(), item=item.identity, kind=ProtectionKind.UNPROTECTED,
        outcome=RecoveryOutcome.PROTECTION_MISSING, provenance="capture",
        action="Supply an off-region export or wait for the original database to recover",
    )
    assert record.qualification == Qualification.UNVERIFIED


def test_optional_missing_data_does_not_make_metadata_unpublishable():
    config = recovery_set()
    capture = snapshot(config)
    missing = ProtectionRecord(
        protection_id=guid(), item=capture.items[0].identity, kind=ProtectionKind.UNPROTECTED,
        outcome=RecoveryOutcome.PROTECTION_MISSING, provenance="capture",
        action="Supply a protected off-region export before restoring this dependency group",
    )
    capture.model_copy(update={"protections": (missing,)}).require_publishable(config)


@pytest.mark.parametrize("target_kind", ["item", "connection", "endpoint", "acl"])
def test_foreign_tenant_qualified_references_cannot_be_published(target_kind):
    config = recovery_set()
    capture = snapshot(config)
    foreign = ItemIdentity(tenant_id=guid(), workspace_id=guid(), item_id=guid())
    if target_kind == "acl":
        acl = DesiredAcl(
            acl_id=guid(), scope=AclScope.ITEM, item=foreign,
            principal=Principal(tenant_id=foreign.tenant_id, object_id=guid(), kind="User"),
            permission="Read", provenance="captured share",
        )
        capture = capture.model_copy(update={"desired_acls": (acl,)})
    else:
        prerequisite = foreign
        if target_kind == "connection":
            prerequisite = ConnectionIdentity(tenant_id=foreign.tenant_id, connection_id=guid())
        elif target_kind == "endpoint":
            prerequisite = EndpointIdentity(item=foreign, endpoint_kind="sql", endpoint_id="alias")
        edge = DependencyEdge(
            edge_id=guid(), consumer=capture.items[0].identity, prerequisite=prerequisite,
            phase="bind", provenance="captured reference", detail="Required target",
        )
        capture = capture.model_copy(update={"dependencies": (edge,)})
    with pytest.raises(ValueError, match="recovery tenant"):
        capture.require_publishable(config)


def test_retained_source_reference_requires_scoped_verified_read_only_contract():
    config = recovery_set()
    source = snapshot(config).items[0].identity
    fields = dict(
        generation_id=guid(), source=source, source_path="Tables/dbo/orders",
        consumer=ItemIdentity(tenant_id=config.tenant_id, workspace_id=guid(), item_id=guid()),
        strategy=RecoveryStrategy.TEMPORARY_CONTINUITY, qualification=Qualification.UNVERIFIED,
        read_only_verified=True, retention_acknowledged=True, evidence="drill-123",
    )
    with pytest.raises(ValidationError, match="Qualify"):
        RecoveryDataBinding(**fields)
    fields["qualification"] = Qualification.VERIFIED
    assert RecoveryDataBinding(**fields).source == source
    fields["consumer"] = source.model_copy(update={"item_id": guid()})
    with pytest.raises(ValidationError, match="distinct same-tenant"):
        RecoveryDataBinding(**fields)


@pytest.mark.parametrize("value", [
    b'{"client_secret":"unsafe"}', b"Password=unsafe;", b"https://example/path?sig=sas-value",
    b"-----BEGIN PRIVATE KEY-----", b"Bearer abcdefghijklmnop",
])
def test_obvious_credentials_are_rejected_not_redacted(value):
    with pytest.raises(ValueError, match="credential"):
        reject_embedded_secrets(value)
