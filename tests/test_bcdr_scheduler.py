from __future__ import annotations

import json

import httpx
import pytest

from fabshuffle.auth import ManagedIdentity, TokenProvider
from fabshuffle.bcdr.deployment_lock import LEASE_ENV
from fabshuffle.bcdr.scheduler import (
    BOOTSTRAP_PATH,
    CONTROLLER_RESOURCE_ENV,
    ENVIRONMENT_RESOURCE_ENV,
    IDENTITY_RESOURCE_ENV,
    JOB_RESOURCE_ENV,
    REQUEST_PATH,
    SchedulerClient,
    SchedulerError,
    validate_cron,
)
from fabshuffle.bcdr.service import CapacityRoute, SyncRequest

TENANT = "00000000-0000-0000-0000-000000000001"
CLIENT = "00000000-0000-0000-0000-000000000002"
SUBSCRIPTION = "00000000-0000-0000-0000-000000000003"
CONTROLLER = (
    f"/subscriptions/{SUBSCRIPTION}/resourceGroups/recovery/providers/"
    "Microsoft.App/containerApps/fab-shuffle"
)
ENVIRONMENT = (
    f"/subscriptions/{SUBSCRIPTION}/resourceGroups/recovery/providers/"
    "Microsoft.App/managedEnvironments/fab-shuffle-env"
)
IDENTITY = (
    f"/subscriptions/{SUBSCRIPTION}/resourceGroups/recovery/providers/"
    "Microsoft.ManagedIdentity/userAssignedIdentities/fab-shuffle"
)
JOB = (
    f"/subscriptions/{SUBSCRIPTION}/resourceGroups/recovery/providers/"
    "Microsoft.App/jobs/fab-shuffle-sync-test"
)
LEASE = "https://fabshuffle.blob.core.windows.net/fab-shuffle-locks/deployment.lock"


class Tokens(TokenProvider):
    def __init__(self):
        super().__init__(ManagedIdentity(TENANT, CLIENT))

    def token(self, scope: str) -> str:
        assert scope == "https://management.azure.com/.default"
        return "arm-token"


def environment() -> dict[str, str]:
    return {
        CONTROLLER_RESOURCE_ENV: CONTROLLER,
        ENVIRONMENT_RESOURCE_ENV: ENVIRONMENT,
        IDENTITY_RESOURCE_ENV: IDENTITY,
        JOB_RESOURCE_ENV: JOB,
        "FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID": CLIENT,
        "FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID": TENANT,
        "FAB_SHUFFLE_BCDR_BOOTSTRAP": str(BOOTSTRAP_PATH),
        LEASE_ENV: LEASE,
    }


def controller() -> dict:
    return {
        "id": CONTROLLER,
        "location": "canadacentral",
        "identity": {"type": "UserAssigned", "userAssignedIdentities": {IDENTITY: {}}},
        "properties": {
            "managedEnvironmentId": ENVIRONMENT,
            "template": {
                "containers": [{
                    "name": "fab-shuffle",
                    "image": "ghcr.io/cbattlegear/fab-shuffle@sha256:" + "a" * 64,
                    "resources": {"cpu": 2, "memory": "4Gi"},
                    "env": [
                        {"name": LEASE_ENV, "value": LEASE},
                        {"name": "FAB_SHUFFLE_BCDR_BOOTSTRAP", "value": str(BOOTSTRAP_PATH)},
                        {"name": "FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID", "value": CLIENT},
                        {"name": "FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID", "value": TENANT},
                        {"name": "FAB_SHUFFLE_MAX_DISK_STAGING_BYTES", "value": "10737418240"},
                        {"name": "FAB_SHUFFLE_MAX_MEMORY_BYTES", "value": "1073741824"},
                    ],
                    "volumeMounts": [{
                        "volumeName": "fab-shuffle-scratch", "mountPath": "/app/local",
                    }],
                }],
                "volumes": [{
                    "name": "fab-shuffle-scratch", "storageType": "AzureFile",
                    "storageName": "fab-shuffle-storage",
                }],
            },
        },
    }


class Arm:
    def __init__(self, request_path):
        self.job = None
        self.triggers = []
        self.fail_schedule = False
        self.request_path = request_path

    def handle(self, request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer arm-token"
        path = request.url.path
        if path.casefold() == CONTROLLER.casefold():
            return httpx.Response(200, json=controller())
        if path.casefold() == f"{JOB}/executions".casefold():
            return httpx.Response(200, json={"value": [{
                "name": "fab-shuffle-sync-test-abc",
                "properties": {
                    "status": "Succeeded", "startTime": "2026-10-09T02:00:00Z",
                    "endTime": "2026-10-09T02:03:00Z",
                },
            }]})
        if path.casefold() != JOB.casefold():
            return httpx.Response(404, json={"error": {"code": "NotFound", "message": path}})
        if request.method == "GET":
            return (
                httpx.Response(404, json={"error": {"code": "NotFound", "message": "missing"}})
                if self.job is None else httpx.Response(200, json=self.job)
            )
        body = json.loads(request.content)
        trigger = body["properties"]["configuration"]["triggerType"]
        self.triggers.append(trigger)
        if trigger == "Schedule":
            assert self.request_path.exists()
            if self.fail_schedule:
                return httpx.Response(
                    403,
                    headers={"x-ms-request-id": "request-1"},
                    json={"error": {"code": "AuthorizationFailed", "message": "jobs/write denied"}},
                )
        self.job = {
            **body,
            "id": JOB,
            "name": "fab-shuffle-sync-test",
            "properties": {**body["properties"], "provisioningState": "Succeeded"},
        }
        return httpx.Response(200, json=self.job)


def request() -> SyncRequest:
    return SyncRequest(capacity_routes=(CapacityRoute(
        source_capacity_id="00000000-0000-0000-0000-000000000004",
        target_capacity_id="00000000-0000-0000-0000-000000000005",
    ),))


def test_configure_stages_manual_writes_exact_request_then_enables_schedule(tmp_path, monkeypatch):
    path = tmp_path / "scheduled-sync-request.json"
    monkeypatch.setattr("fabshuffle.bcdr.scheduler.REQUEST_PATH", path)
    arm = Arm(path)
    with SchedulerClient(
        Tokens(), environ=environment(), transport=httpx.MockTransport(arm.handle),
    ) as client:
        result = client.configure(request(), "0 2 * * *")
    assert arm.triggers == ["Manual", "Schedule"]
    assert result["status"] == "deployed"
    assert result["schedule_utc"] == "0 2 * * *"
    assert result["runtime_matches_controller"]
    assert result["latest_execution"]["status"] == "Succeeded"
    approved = SyncRequest.model_validate_json(path.read_bytes())
    assert approved.capture is True and approved.park is True
    assert "secret" not in path.read_text().casefold()


def test_failed_final_update_leaves_job_manual_and_preserves_service_error(tmp_path, monkeypatch):
    path = tmp_path / "scheduled-sync-request.json"
    monkeypatch.setattr("fabshuffle.bcdr.scheduler.REQUEST_PATH", path)
    arm = Arm(path)
    arm.fail_schedule = True
    with SchedulerClient(
        Tokens(), environ=environment(), transport=httpx.MockTransport(arm.handle),
    ) as client:
        with pytest.raises(SchedulerError) as caught:
            client.configure(request(), "0 3 * * *")
    assert arm.triggers == ["Manual", "Schedule", "Manual"]
    assert arm.job["properties"]["configuration"]["triggerType"] == "Manual"
    assert path.exists()
    assert "AuthorizationFailed: jobs/write denied" in str(caught.value)
    assert "request ID request-1" in str(caught.value)
    assert "Microsoft.App/jobs/write" in str(caught.value)


def test_matching_observed_schedule_is_idempotent(tmp_path, monkeypatch):
    path = tmp_path / "scheduled-sync-request.json"
    monkeypatch.setattr("fabshuffle.bcdr.scheduler.REQUEST_PATH", path)
    arm = Arm(path)
    with SchedulerClient(
        Tokens(), environ=environment(), transport=httpx.MockTransport(arm.handle),
    ) as client:
        assert client.configure(request(), "0 4 * * *")["changed"]
        arm.triggers.clear()
        result = client.configure(request(), "0 4 * * *")
    assert not result["changed"]
    assert arm.triggers == []


@pytest.mark.parametrize("expression", [
    "99 99 99 99 99", "0 2 * *", "0 2 * * * *", "0 2 * * MON",
    "0 2 * 12-1 *", "*/0 2 * * *", "0  2 * * *",
])
def test_invalid_container_apps_cron_is_rejected(expression):
    with pytest.raises(ValueError):
        validate_cron(expression)


def test_scheduler_requires_deployment_metadata_before_any_arm_call():
    with pytest.raises(SchedulerError, match=CONTROLLER_RESOURCE_ENV):
        SchedulerClient(Tokens(), environ={})


def test_production_paths_are_the_shared_controller_paths():
    assert str(BOOTSTRAP_PATH) == "/app/local/bcdr/bootstrap.json"
    assert str(REQUEST_PATH) == "/app/local/bcdr/scheduled-sync-request.json"
