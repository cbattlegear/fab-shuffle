from __future__ import annotations

import json
import shutil
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from fabshuffle.auth import ServicePrincipal
from fabshuffle.bcdr.bootstrap import (
    BootstrapDescriptor,
    BootstrapError,
    BootstrapStore,
    CapacityAuthorization,
    CapacityOperation,
    WarehouseIntent,
    WorkspaceIntent,
    WorkspacePrincipal,
)
from fabshuffle.bcdr.provisioning import (
    ControlWarehouseProvisioner,
    ControlWorkspaceProvisioner,
    WarehouseCreationUnknown,
)

TENANT = "10000000-0000-0000-0000-000000000001"
APP = "10000000-0000-0000-0000-000000000002"
OWNER = "10000000-0000-0000-0000-000000000003"
SET = "10000000-0000-0000-0000-000000000004"
WORKSPACE = "10000000-0000-0000-0000-000000000005"
WAREHOUSE = "10000000-0000-0000-0000-000000000006"
FABRIC_CAP = "10000000-0000-0000-0000-000000000007"
OP = "10000000-0000-0000-0000-000000000008"
SPN_OBJECT = "10000000-0000-0000-0000-000000000009"
ARM_ID = f"/subscriptions/{TENANT}/resourcegroups/recovery/providers/microsoft.fabric/capacities/catalog"
HOST = "example.datawarehouse.fabric.microsoft.com"


def descriptor(**updates):
    return BootstrapDescriptor(
        recovery_set_id=SET, tenant_id=TENANT, application_id=APP, controller_id=OWNER,
        capacities=(CapacityAuthorization(
            arm_resource_id=ARM_ID, fabric_capacity_id=FABRIC_CAP,
            dedicated_recovery=True, authorized_for_suspend=True,
        ),),
        catalog_capacity_id=ARM_ID, control_workspace_id=WORKSPACE, **updates,
    )


class Tokens:
    principal = ServicePrincipal(TENANT, APP, "not-a-real-secret")

    def tenant_id(self):
        return TENANT

    def fabric_token(self):
        return "fabric-test-token"

    def object_id(self):
        return SPN_OBJECT


@pytest.fixture
def store():
    directory = Path.cwd() / f".bcdr-bootstrap-test-{uuid4().hex}"
    directory.mkdir()
    result = BootstrapStore(directory / "bootstrap.json")
    result.save(descriptor(workspace_intent=WorkspaceIntent(
        owner_id=OWNER, intent_id=OP, display_name="Control workspace",
        owner_scope_sha256="0" * 64, phase="ready", workspace_id=WORKSPACE,
    )), expected_revision=None)
    try:
        yield result
    finally:
        shutil.rmtree(directory)


def test_atomic_revision_validation_and_no_credentials(store):
    original = store.load()
    assert original.revision == 0
    assert "not-a-real-secret" not in store.path.read_text()
    assert store.path.stat().st_mode & 0o777 == 0o600
    saved = store.save(original, expected_revision=0)
    assert saved.revision == 1
    with pytest.raises(BootstrapError, match="revision"):
        store.save(original, expected_revision=0)
    invalid = saved.model_copy(update={"tds_host": "host;password=secret"})
    with pytest.raises(ValidationError):
        store.save(invalid, expected_revision=1)
    assert store.load() == saved


def test_failed_atomic_replace_preserves_previous_file(store, monkeypatch):
    original = store.path.read_bytes()

    def fail_replace(*_):
        raise OSError("simulated disk failure")

    monkeypatch.setattr("fabshuffle.bcdr.bootstrap.os.replace", fail_replace)
    with pytest.raises(OSError, match="disk"):
        store.save(store.load(), expected_revision=0)
    assert store.path.read_bytes() == original
    assert not list(store.path.parent.glob("*.writing"))


@pytest.mark.parametrize("extra", [{"secret": "bad"}, {"definitions": []}, {"desired_acls": []}])
def test_unknown_fields_are_not_a_metadata_backup(extra):
    with pytest.raises(ValidationError, match="Extra inputs"):
        BootstrapDescriptor.model_validate({**descriptor().model_dump(), **extra})


@pytest.mark.parametrize("change", [
    {"tenant_id": "not-a-guid"},
    {"schema_version": 2},
    {"catalog_capacity_id": ARM_ID.replace("catalog", "unrelated")},
    {"tds_host": HOST},
    {"tds_host": HOST, "tds_catalog": WAREHOUSE},
    {"capacities": ()},
])
def test_descriptor_rejects_invalid_or_inconsistent_identity(change):
    with pytest.raises(ValidationError):
        BootstrapDescriptor.model_validate({**descriptor().model_dump(), **change})


@pytest.mark.parametrize("resource_id", [
    "https://management.azure.com" + ARM_ID,
    ARM_ID + "/suspend",
    ARM_ID.replace("microsoft.fabric", "microsoft.powerbidedicated"),
    ARM_ID.replace("catalog", "catalog%2fother"),
    ARM_ID.replace("catalog", "../catalog"),
])
def test_capacity_requires_exact_fabric_arm_id(resource_id):
    with pytest.raises(ValidationError):
        CapacityAuthorization(
            arm_resource_id=resource_id, fabric_capacity_id=FABRIC_CAP,
            dedicated_recovery=True, authorized_for_suspend=True,
        )


def test_descriptor_rejects_duplicate_capacities_and_unowned_operations():
    value = descriptor()
    with pytest.raises(ValidationError, match="unique"):
        BootstrapDescriptor.model_validate({
            **value.model_dump(), "capacities": value.capacities + value.capacities,
        })
    operation = CapacityOperation(
        intent_id=OP, owner_id=APP, arm_resource_id=ARM_ID, action="resume",
    )
    with pytest.raises(ValidationError, match="scope"):
        BootstrapDescriptor.model_validate({**value.model_dump(), "capacity_operations": (operation,)})


@pytest.mark.parametrize("payload", [b"{", b"{}", b" " * (128 * 1024 + 1)])
def test_corrupt_or_oversized_file_fails_closed(store, payload):
    store.path.write_bytes(payload)
    with pytest.raises(BootstrapError):
        store.load()


def workspace_response():
    return httpx.Response(200, json={
        "id": WORKSPACE, "capacityId": FABRIC_CAP, "capacityAssignmentProgress": "Completed",
    })


def warehouse_body(*, endpoint=True):
    body = {"id": WAREHOUSE, "workspaceId": WORKSPACE, "type": "Warehouse", "displayName": "Control"}
    if endpoint:
        body["properties"] = {"connectionString": HOST}
    return body


def test_real_warehouse_post_persists_intent_id_before_followup(store):
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path))
        assert request.headers["authorization"] == "Bearer fabric-test-token"
        if request.url.path == f"/v1/workspaces/{WORKSPACE}":
            return workspace_response()
        if request.method == "POST":
            intent = store.load().warehouse_intent
            assert intent.phase == "intent" and intent.owner_id == OWNER
            assert json.loads(request.content)["displayName"] == "Control"
            return httpx.Response(201, json=warehouse_body(endpoint=False))
        assert store.load().control_warehouse_id == WAREHOUSE
        assert store.load().warehouse_intent.phase == "created"
        return httpx.Response(200, json=warehouse_body())

    with ControlWarehouseProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        result = provisioner.ensure(display_name="Control", owner_id=OWNER)
    assert result.control_warehouse_id == result.tds_catalog == WAREHOUSE
    assert result.tds_host == HOST and result.warehouse_intent.phase == "ready"
    assert sum(method == "POST" for method, _ in calls) == 1
    assert all("/items" not in path for _, path in calls)


def test_warehouse_accepts_operator_explicit_existing_workspace_without_creation_intent(store):
    store.update(lambda current: current.model_copy(update={"workspace_intent": None}))
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path))
        if request.url.path == f"/v1/workspaces/{WORKSPACE}":
            return workspace_response()
        if request.method == "POST":
            assert store.load().warehouse_intent.phase == "intent"
            return httpx.Response(201, json=warehouse_body(endpoint=False))
        assert store.load().control_warehouse_id == WAREHOUSE
        return httpx.Response(200, json=warehouse_body())

    with ControlWarehouseProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        result = provisioner.ensure(display_name="Control", owner_id=OWNER)
    assert result.workspace_intent is None
    assert result.tds_catalog == result.control_warehouse_id == WAREHOUSE
    assert [path for method, path in calls if method == "POST"] == [
        f"/v1/workspaces/{WORKSPACE}/warehouses",
    ]


def test_warehouse_lro_restart_polls_recorded_id_without_recreating(store):
    sleeps = []
    first_calls = []

    def first_handler(request):
        first_calls.append(request.method)
        if request.method == "GET":
            return workspace_response()
        return httpx.Response(202, headers={
            "x-ms-operation-id": OP, "Retry-After": "3",
            "Location": f"https://api.fabric.microsoft.com/v1/operations/{OP}",
        })

    def interrupted_sleep(delay):
        sleeps.append(delay)
        raise RuntimeError("controller interrupted")

    with ControlWarehouseProvisioner(
        store, Tokens(), transport=httpx.MockTransport(first_handler), sleep=interrupted_sleep,
    ) as provisioner:
        with pytest.raises(RuntimeError, match="interrupted"):
            provisioner.ensure(display_name="Control", owner_id=OWNER)
    assert store.load().warehouse_intent.operation_id == OP
    assert store.load().warehouse_intent.phase == "accepted"
    assert sleeps == [3]
    second_calls = []

    def second_handler(request):
        second_calls.append(request.method)
        assert request.method == "GET"
        if request.url.path == f"/v1/workspaces/{WORKSPACE}":
            return workspace_response()
        assert store.load().warehouse_intent.operation_id == OP
        if request.url.path == f"/v1/operations/{OP}":
            return httpx.Response(200, json={"status": "Succeeded"}, headers={
                "Location": f"https://api.fabric.microsoft.com/v1/operations/{OP}/result",
            })
        if request.url.path.endswith("/result"):
            return httpx.Response(200, json=warehouse_body(endpoint=False))
        assert store.load().control_warehouse_id == WAREHOUSE
        return httpx.Response(200, json=warehouse_body())

    with ControlWarehouseProvisioner(
        store, Tokens(), transport=httpx.MockTransport(second_handler), sleep=lambda _: None,
    ) as provisioner:
        assert provisioner.ensure(display_name="Control", owner_id=OWNER).tds_host == HOST
    assert first_calls.count("POST") == 1 and second_calls.count("POST") == 0


def test_ambiguous_warehouse_create_never_replays_or_adopts_name(store):
    calls = []

    def handler(request):
        calls.append(request.method)
        if request.method == "GET":
            return workspace_response()
        raise httpx.ReadTimeout("response lost", request=request)

    with ControlWarehouseProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        with pytest.raises(WarehouseCreationUnknown, match="lost"):
            provisioner.ensure(display_name="Control", owner_id=OWNER)
        with pytest.raises(WarehouseCreationUnknown, match="never replay"):
            provisioner.ensure(display_name="Control", owner_id=OWNER)
    assert calls == ["GET", "POST"]
    assert store.load().warehouse_intent.phase == "intent"


@pytest.mark.parametrize("location", [
    f"http://api.fabric.microsoft.com/v1/operations/{OP}",
    f"https://evil.example/v1/operations/{OP}",
    f"https://api.fabric.microsoft.com@evil.example/v1/operations/{OP}",
    f"https://api.fabric.microsoft.com/v1/operations/{WAREHOUSE}",
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE}",
])
def test_warehouse_operation_location_cannot_exfiltrate_fabric_token(store, location):
    hosts = []

    def handler(request):
        hosts.append(request.url.host)
        if request.method == "GET":
            return workspace_response()
        return httpx.Response(202, headers={"x-ms-operation-id": OP, "Location": location})

    with ControlWarehouseProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        with pytest.raises(BootstrapError):
            provisioner.ensure(display_name="Control", owner_id=OWNER)
    assert set(hosts) == {"api.fabric.microsoft.com"}
    assert store.load().warehouse_intent.operation_id == OP


def test_warehouse_operation_accepts_relative_location(store):
    """Fabric's LRO Location header is a URI-reference (RFC 9110) and may be
    returned relative to the request URI rather than as an absolute URL. A
    same-origin relative Location must resolve and validate, not be rejected
    outright as an unsafe absolute URL.
    """
    hosts = []

    def handler(request):
        hosts.append(request.url.host)
        if request.method == "GET":
            if request.url.path == f"/v1/workspaces/{WORKSPACE}":
                return workspace_response()
            if request.url.path == f"/v1/operations/{OP}":
                return httpx.Response(200, json={"status": "Succeeded"}, headers={
                    "Location": f"/v1/operations/{OP}/result",
                })
            if request.url.path.endswith("/result"):
                return httpx.Response(200, json=warehouse_body(endpoint=False))
            assert store.load().control_warehouse_id == WAREHOUSE
            return httpx.Response(200, json=warehouse_body())
        return httpx.Response(202, headers={
            "x-ms-operation-id": OP, "Location": f"/v1/operations/{OP}",
        })

    with ControlWarehouseProvisioner(
        store, Tokens(), transport=httpx.MockTransport(handler), sleep=lambda _: None,
    ) as provisioner:
        assert provisioner.ensure(display_name="Control", owner_id=OWNER).tds_host == HOST
    assert set(hosts) == {"api.fabric.microsoft.com"}


def test_warehouse_http_redirect_is_not_followed(store):
    hosts = []

    def handler(request):
        hosts.append(request.url.host)
        return httpx.Response(307, headers={"Location": "https://evil.example/steal"})

    with ControlWarehouseProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        with pytest.raises(RuntimeError, match="HTTP 307"):
            provisioner.ensure(display_name="Control", owner_id=OWNER)
    assert hosts == ["api.fabric.microsoft.com"]
    assert store.load().warehouse_intent is None


def test_warehouse_lro_preserves_service_failure(store):
    intent = WarehouseIntent(
        intent_id=OP, owner_id=OWNER, display_name="Control", phase="accepted", operation_id=OP,
    )
    store.update(lambda current: current.model_copy(update={"warehouse_intent": intent}))

    def handler(request):
        if "/workspaces/" in request.url.path:
            return workspace_response()
        return httpx.Response(200, json={
            "status": "Failed", "error": {"errorCode": "DeniedByPolicy", "message": "Ask your admin"},
            "requestId": WAREHOUSE,
        })

    with ControlWarehouseProvisioner(
        store, Tokens(), transport=httpx.MockTransport(handler), sleep=lambda _: None,
    ) as provisioner:
        with pytest.raises(RuntimeError, match=f"DeniedByPolicy: Ask your admin.*{WAREHOUSE}"):
            provisioner.ensure(display_name="Control", owner_id=OWNER)
    assert store.load().warehouse_intent.phase == "failed"


def test_warehouse_rejects_wrong_capacity_before_create(store):
    def handler(_):
        return httpx.Response(200, json={
            "id": WORKSPACE, "capacityId": APP, "capacityAssignmentProgress": "Completed",
        })

    with ControlWarehouseProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        with pytest.raises(BootstrapError, match="catalog capacity"):
            provisioner.ensure(display_name="Control", owner_id=OWNER)
    assert store.load().warehouse_intent is None


def unprovisioned(store):
    return store.update(lambda current: current.model_copy(update={
        "control_workspace_id": None, "workspace_intent": None,
    }))


def role_assignment(principal_id, principal_type="User", role="Admin"):
    return {
        "id": principal_id, "principal": {"id": principal_id, "type": principal_type}, "role": role,
    }


def test_workspace_create_persists_intent_then_id_then_restricted_grant_receipts(store):
    unprovisioned(store)
    owners = (WorkspacePrincipal(object_id=OWNER, principal_type="User"),)
    roles = [role_assignment(SPN_OBJECT, "ServicePrincipal")]
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path))
        assert request.headers["authorization"] == "Bearer fabric-test-token"
        if request.url.path == "/v1/workspaces":
            current = store.load()
            assert current.control_workspace_id is None and current.workspace_intent.phase == "intent"
            assert json.loads(request.content)["capacityId"] == FABRIC_CAP
            return httpx.Response(201, json={"id": WORKSPACE, "type": "Workspace"}, headers={
                "Location": f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE}",
            })
        assert store.load().control_workspace_id == WORKSPACE
        if request.url.path == f"/v1/workspaces/{WORKSPACE}":
            return workspace_response()
        assert request.url.path.endswith("/roleAssignments")
        if request.method == "GET":
            return httpx.Response(200, json={"value": list(roles)})
        assert store.load().workspace_intent.pending_role.principal.object_id == OWNER
        payload = json.loads(request.content)
        assert payload == {"principal": {"id": OWNER, "type": "User"}, "role": "Admin"}
        added = role_assignment(OWNER)
        roles.append(added)
        return httpx.Response(201, json=added, headers={
            "Location": (
                f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE}/roleAssignments/{OWNER}"
            ),
        })

    with ControlWorkspaceProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        result = provisioner.ensure(display_name="Control", owner_id=OWNER, allowed_principals=owners)
    assert result.control_workspace_id == WORKSPACE and result.workspace_intent.phase == "ready"
    assert result.workspace_intent.pending_role is None
    assert result.warehouse_intent is None
    assert [path for method, path in calls if method == "POST"] == [
        "/v1/workspaces", f"/v1/workspaces/{WORKSPACE}/roleAssignments",
    ]
    assert "allowed_principals" not in store.path.read_text()


def test_ambiguous_workspace_create_is_not_repeated_or_adopted_by_name(store):
    unprovisioned(store)
    calls = []

    def handler(request):
        calls.append(request.method)
        assert store.load().workspace_intent.phase == "intent"
        raise httpx.ReadTimeout("lost", request=request)

    with ControlWorkspaceProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        with pytest.raises(WarehouseCreationUnknown, match="lost"):
            provisioner.ensure(display_name="Control", owner_id=OWNER, allowed_principals=())
        with pytest.raises(WarehouseCreationUnknown, match="never adopt by name"):
            provisioner.ensure(display_name="Control", owner_id=OWNER, allowed_principals=())
    assert calls == ["POST"] and store.load().control_workspace_id is None


def test_workspace_unexpected_roles_warn_without_removing_grants(store):
    unprovisioned(store)
    methods = []

    def handler(request):
        methods.append(request.method)
        if request.url.path == "/v1/workspaces":
            return httpx.Response(201, json={"id": WORKSPACE, "type": "Workspace"})
        if request.url.path.endswith("/roleAssignments"):
            return httpx.Response(200, json={"value": [
                role_assignment(SPN_OBJECT, "ServicePrincipal"), role_assignment(APP),
            ]})
        return workspace_response()

    with ControlWorkspaceProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        provisioner.ensure(display_name="Control", owner_id=OWNER, allowed_principals=())
        assert any(APP in warning and "left unchanged" in warning for warning in provisioner.warnings)
        provisioner.ensure(display_name="Control", owner_id=OWNER, allowed_principals=())
    assert methods.count("POST") == 1 and "DELETE" not in methods
    assert store.load().workspace_intent.phase == "ready"


def test_workspace_role_lost_response_reconciles_exact_principal_without_regrant(store):
    unprovisioned(store)
    owners = (WorkspacePrincipal(object_id=OWNER, principal_type="User"),)
    roles = [role_assignment(SPN_OBJECT, "ServicePrincipal")]
    grants = []

    def handler(request):
        if request.url.path == "/v1/workspaces":
            return httpx.Response(201, json={"id": WORKSPACE, "type": "Workspace"})
        if request.url.path.endswith("/roleAssignments"):
            if request.method == "GET":
                return httpx.Response(200, json={"value": list(roles)})
            grants.append(request)
            roles.append(role_assignment(OWNER))
            raise httpx.ReadTimeout("lost", request=request)
        return workspace_response()

    with ControlWorkspaceProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        with pytest.raises(WarehouseCreationUnknown, match="lost"):
            provisioner.ensure(display_name="Control", owner_id=OWNER, allowed_principals=owners)
        assert store.load().workspace_intent.pending_role.principal.object_id == OWNER
        result = provisioner.ensure(display_name="Control", owner_id=OWNER, allowed_principals=owners)
    assert len(grants) == 1 and result.workspace_intent.pending_role is None
    assert result.workspace_intent.phase == "ready"


def test_workspace_changed_owner_allowlist_cannot_resume_original_setup(store):
    unprovisioned(store)

    def handler(request):
        if request.url.path == "/v1/workspaces":
            return httpx.Response(201, json={"id": WORKSPACE, "type": "Workspace"})
        if request.url.path.endswith("/roleAssignments"):
            return httpx.Response(200, json={"value": [role_assignment(SPN_OBJECT, "ServicePrincipal")]})
        return workspace_response()

    with ControlWorkspaceProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        provisioner.ensure(display_name="Control", owner_id=OWNER, allowed_principals=())
        with pytest.raises(BootstrapError, match="allowlist changed"):
            provisioner.ensure(
                display_name="Control", owner_id=OWNER,
                allowed_principals=(WorkspacePrincipal(object_id=OWNER, principal_type="User"),),
            )


def test_role_pagination_cannot_follow_foreign_continuation(store):
    unprovisioned(store)
    hosts = []

    def handler(request):
        hosts.append(request.url.host)
        if request.url.path == "/v1/workspaces":
            return httpx.Response(201, json={"id": WORKSPACE, "type": "Workspace"})
        if request.url.path.endswith("/roleAssignments"):
            return httpx.Response(200, json={
                "value": [role_assignment(SPN_OBJECT, "ServicePrincipal")], "continuationToken": "next",
                "continuationUri": "https://evil.example/?continuationToken=next",
            })
        return workspace_response()

    with ControlWorkspaceProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        with pytest.raises(BootstrapError, match="unsafe"):
            provisioner.ensure(display_name="Control", owner_id=OWNER, allowed_principals=())
    assert set(hosts) == {"api.fabric.microsoft.com"}
    assert store.load().workspace_intent.phase != "ready"


@pytest.mark.parametrize("drift", ["none", "extra", "missing-owner", "changed-role"])
def test_explicit_existing_workspace_is_audited_without_claiming_creation_or_mutating_roles(store, drift):
    store.update(lambda current: current.model_copy(update={"workspace_intent": None}))
    owners = (WorkspacePrincipal(object_id=OWNER, principal_type="User"),)
    calls = []

    def handler(request):
        calls.append(request.method)
        assert request.method == "GET"
        if request.url.path.endswith("/roleAssignments"):
            roles = [role_assignment(SPN_OBJECT, "ServicePrincipal")]
            if drift != "missing-owner":
                roles.append(role_assignment(OWNER, role="Viewer" if drift == "changed-role" else "Admin"))
            if drift == "extra":
                roles.append(role_assignment(APP))
            return httpx.Response(200, json={"value": roles})
        return workspace_response()

    with ControlWorkspaceProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        result = provisioner.verify_existing(owner_id=OWNER, allowed_principals=owners)
        assert bool(provisioner.warnings) == (drift != "none")
    assert result.workspace_intent.origin == "designated"
    assert result.workspace_intent.phase == "ready"
    assert result.control_workspace_id == WORKSPACE and calls == ["GET", "GET"]


@pytest.mark.parametrize("roles", [
    [role_assignment(SPN_OBJECT, "ServicePrincipal", role="Member")],
    [],
])
def test_existing_workspace_access_failure_does_not_create_warehouse_or_fix_unowned_acl(store, roles):
    store.update(lambda current: current.model_copy(update={"workspace_intent": None}))

    def handler(request):
        assert request.method == "GET"
        if request.url.path.endswith("/roleAssignments"):
            return httpx.Response(200, json={"value": roles})
        return workspace_response()

    with ControlWorkspaceProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        with pytest.raises(BootstrapError):
            provisioner.verify_existing(owner_id=OWNER, allowed_principals=())
    assert store.load().workspace_intent is None


def test_existing_workspace_verification_cannot_resolve_an_ambiguous_name_only_create(store):
    unprovisioned(store)

    def handler(request):
        raise httpx.ReadTimeout("lost", request=request)

    with ControlWorkspaceProvisioner(store, Tokens(), transport=httpx.MockTransport(handler)) as provisioner:
        with pytest.raises(WarehouseCreationUnknown):
            provisioner.ensure(display_name="Control", owner_id=OWNER, allowed_principals=())
        with pytest.raises(BootstrapError, match="exact designated workspace ID"):
            provisioner.verify_existing(owner_id=OWNER, allowed_principals=())
