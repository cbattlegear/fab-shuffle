"""Controller-managed deployment of the guarded Container Apps scheduled-sync job."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlsplit

import httpx

from fabshuffle.auth import ManagedIdentity, TokenProvider
from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.contracts import canonical_json, digest, reject_embedded_secrets
from fabshuffle.bcdr.deployment_lock import LEASE_ENV, validate_blob_url
from fabshuffle.bcdr.scheduled import MAX_REQUEST_BYTES, read_request, validate_request
from fabshuffle.bcdr.service import SyncRequest

ARM_API_VERSION = "2024-03-01"
ARM_SCOPE = "https://management.azure.com/.default"
REQUEST_PATH = PurePosixPath("/app/local/bcdr/scheduled-sync-request.json")
BOOTSTRAP_PATH = PurePosixPath("/app/local/bcdr/bootstrap.json")
STORAGE_NAME = "fab-shuffle-storage"
VOLUME_NAME = "fab-shuffle-scratch"
MOUNT_PATH = "/app/local"
CONTAINER_NAME = "fab-shuffle-sync"

CONTROLLER_RESOURCE_ENV = "FAB_SHUFFLE_CONTROLLER_RESOURCE_ID"
ENVIRONMENT_RESOURCE_ENV = "FAB_SHUFFLE_CONTAINER_APPS_ENVIRONMENT_ID"
IDENTITY_RESOURCE_ENV = "FAB_SHUFFLE_CONTROLLER_UAMI_RESOURCE_ID"
JOB_RESOURCE_ENV = "FAB_SHUFFLE_BCDR_JOB_RESOURCE_ID"

_RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 6))


class SchedulerError(RecoveryBlocked):
    """Scheduling prerequisite or Azure management failure."""


def validate_cron(expression: str) -> str:
    """Validate the numeric five-field UTC cron accepted by this product."""
    fields = expression.split(" ")
    if len(fields) != 5 or any(not field for field in fields):
        raise ValueError("Use exactly five UTC cron fields separated by single spaces.")
    for field, (minimum, maximum) in zip(fields, _RANGES, strict=True):
        for entry in field.split(","):
            base, separator, step_text = entry.partition("/")
            if separator and (not step_text.isdigit() or int(step_text) < 1):
                raise ValueError("Cron steps must be positive integers.")
            if base == "*":
                continue
            start_text, range_separator, end_text = base.partition("-")
            values = (start_text, end_text) if range_separator else (start_text,)
            if any(not value.isdigit() for value in values):
                raise ValueError("Cron fields must use numeric values, ranges, lists, steps or '*'.")
            numbers = tuple(int(value) for value in values)
            if any(value < minimum or value > maximum for value in numbers):
                raise ValueError(
                    f"Cron value is outside the supported {minimum}-{maximum} range."
                )
            if range_separator and numbers[0] > numbers[1]:
                raise ValueError("Cron ranges must increase from start to end.")
    return expression


def _arm_resource(value: str, resource_type: str) -> str:
    path = value.strip().rstrip("/")
    parsed = urlsplit(path)
    if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment or "\\" in path:
        raise SchedulerError(f"{resource_type} must be an Azure resource ID, not a URL.")
    parts = path.split("/")
    if (
        len(parts) < 9 or parts[0] != "" or parts[1].casefold() != "subscriptions"
        or parts[3].casefold() != "resourcegroups"
        or parts[5].casefold() != "providers"
        or "/".join(parts[6:8]).casefold() != resource_type.casefold()
        or not all(parts[index] for index in (2, 4, 8))
    ):
        raise SchedulerError(f"{resource_type} is not a complete Azure resource ID.")
    return path


def _required_environment(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name, "").strip()
    if not value:
        raise SchedulerError(
            f"Scheduling automation requires {name}. Redeploy the controller from this release, "
            "then retry; no request or job was changed."
        )
    return value


def _env_map(container: Mapping[str, Any]) -> dict[str, str]:
    result = {}
    for entry in container.get("env") or []:
        if isinstance(entry, dict) and isinstance(entry.get("name"), str):
            if "secretRef" in entry:
                continue
            if isinstance(entry.get("value"), str):
                result[entry["name"]] = entry["value"]
    return result


def _single_container(resource: Mapping[str, Any]) -> Mapping[str, Any]:
    containers = resource.get("properties", {}).get("template", {}).get("containers") or []
    if len(containers) != 1 or not isinstance(containers[0], dict):
        raise SchedulerError("The controller must have exactly one observable application container.")
    return containers[0]


def _request_bytes(request: SyncRequest) -> bytes:
    validate_request(request)
    raw = canonical_json(request.model_dump(mode="json"))
    if len(raw) > MAX_REQUEST_BYTES:
        raise SchedulerError("The approved scheduled request exceeds 128 KiB; reduce its scope.")
    try:
        reject_embedded_secrets(raw)
    except ValueError as error:
        raise SchedulerError(str(error)) from error
    return raw


class SchedulerClient:
    def __init__(
        self,
        tokens: TokenProvider,
        *,
        environ: Mapping[str, str] | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.tokens = tokens
        self.environ = os.environ if environ is None else environ
        principal = tokens.principal
        if not isinstance(principal, ManagedIdentity):
            raise SchedulerError(
                "Automated scheduling requires the controller's user-assigned managed identity. "
                "Redeploy with managedIdentityResourceId; service-principal secrets are not copied."
            )
        self.controller_id = _arm_resource(
            _required_environment(self.environ, CONTROLLER_RESOURCE_ENV),
            "Microsoft.App/containerApps",
        )
        self.environment_id = _arm_resource(
            _required_environment(self.environ, ENVIRONMENT_RESOURCE_ENV),
            "Microsoft.App/managedEnvironments",
        )
        self.identity_id = _arm_resource(
            _required_environment(self.environ, IDENTITY_RESOURCE_ENV),
            "Microsoft.ManagedIdentity/userAssignedIdentities",
        )
        self.job_id = _arm_resource(
            _required_environment(self.environ, JOB_RESOURCE_ENV), "Microsoft.App/jobs",
        )
        if self.controller_id.split("/")[:5] != self.job_id.split("/")[:5]:
            raise SchedulerError("The configured scheduler job is not in the controller resource group.")
        if self.environment_id.split("/")[:5] != self.controller_id.split("/")[:5]:
            raise SchedulerError(
                "The configured Container Apps environment is not in the controller resource group."
            )
        if principal.client_id != self.environ.get(
            "FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID", ""
        ).strip():
            raise SchedulerError(
                "The authenticated managed identity differs from the controller configuration."
            )
        validate_blob_url(_required_environment(self.environ, LEASE_ENV))
        configured_bootstrap = PurePosixPath(_required_environment(
            self.environ, "FAB_SHUFFLE_BCDR_BOOTSTRAP",
        ))
        if configured_bootstrap != BOOTSTRAP_PATH:
            raise SchedulerError(
                f"Scheduling automation requires the shared bootstrap at {BOOTSTRAP_PATH}; "
                f"the controller reports {configured_bootstrap}."
            )
        self.http = httpx.Client(transport=transport, timeout=60, follow_redirects=False)

    def close(self) -> None:
        self.http.close()

    def __enter__(self) -> SchedulerClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _request(self, method: str, resource_id: str, *, json_body: dict | None = None) -> dict:
        if resource_id not in {
            self.controller_id, self.job_id, f"{self.job_id}/executions",
        }:
            raise SchedulerError("Refusing an ARM request outside the configured controller and job.")
        url = f"https://management.azure.com{resource_id}?api-version={ARM_API_VERSION}"
        try:
            response = self.http.request(
                method, url,
                headers={"Authorization": f"Bearer {self.tokens.token(ARM_SCOPE)}"},
                json=json_body,
            )
        except httpx.TransportError as error:
            raise SchedulerError(
                f"Azure did not return the result of ARM {method} {resource_id}. "
                "Read scheduler status before retrying."
            ) from error
        if response.status_code == 404 and method == "GET":
            return {}
        if not response.is_success:
            try:
                body = response.json()
            except ValueError:
                body = {}
            service_error = body.get("error") if isinstance(body.get("error"), dict) else body
            code = str(service_error.get("code") or service_error.get("errorCode") or "")
            message = str(service_error.get("message") or response.text or "")
            request_id = response.headers.get("x-ms-request-id", "")
            remediation = (
                " Grant the controller identity a custom role at this resource group with "
                "Microsoft.App/containerApps/read, Microsoft.App/jobs/read, "
                "Microsoft.App/jobs/write, Microsoft.App/jobs/executions/read, "
                "Microsoft.App/managedEnvironments/read and "
                "Microsoft.App/managedEnvironments/join/action, then retry."
                if response.status_code in {401, 403} else ""
            )
            raise SchedulerError(
                f"Azure ARM {method} failed: HTTP {response.status_code}; {code}: {message}"
                + (f" (request ID {request_id})" if request_id else "")
                + remediation
            )
        if not response.content:
            return {}
        try:
            body = response.json()
        except ValueError as error:
            raise SchedulerError("Azure ARM returned a non-JSON scheduler response.") from error
        if not isinstance(body, dict):
            raise SchedulerError("Azure ARM returned an invalid scheduler response.")
        return body

    def _controller(self) -> tuple[dict, dict[str, str]]:
        controller = self._request("GET", self.controller_id)
        if not controller:
            raise SchedulerError(
                "Azure could not find the configured controller app. Redeploy and retry."
            )
        properties = controller.get("properties") or {}
        if str(properties.get("managedEnvironmentId") or "").casefold() != self.environment_id.casefold():
            raise SchedulerError("The live controller uses a different Container Apps environment.")
        identities = (controller.get("identity") or {}).get("userAssignedIdentities") or {}
        if {str(key).casefold() for key in identities} != {self.identity_id.casefold()}:
            raise SchedulerError(
                "The live controller does not use exactly the configured scheduling identity."
            )
        container = _single_container(controller)
        environment = _env_map(container)
        expected = {
            LEASE_ENV: _required_environment(self.environ, LEASE_ENV),
            "FAB_SHUFFLE_BCDR_BOOTSTRAP": str(BOOTSTRAP_PATH),
            "FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID": self.tokens.principal.client_id,
            "FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID": self.tokens.principal.tenant_id,
        }
        for name, value in expected.items():
            if environment.get(name) != value:
                raise SchedulerError(f"The live controller's {name} differs from this process.")
        mounts = container.get("volumeMounts") or []
        volumes = properties.get("template", {}).get("volumes") or []
        if not any(
            row.get("volumeName") == VOLUME_NAME and row.get("mountPath") == MOUNT_PATH
            for row in mounts if isinstance(row, dict)
        ) or not any(
            row.get("name") == VOLUME_NAME and row.get("storageType") == "AzureFile"
            and row.get("storageName") == STORAGE_NAME
            for row in volumes if isinstance(row, dict)
        ):
            raise SchedulerError("The live controller does not expose the required shared Azure Files mount.")
        if not container.get("image"):
            raise SchedulerError("Azure did not report the live controller image.")
        if "@sha256:" not in str(container["image"]):
            raise SchedulerError(
                "Automated scheduling requires the controller image to be pinned by digest. "
                "Redeploy the reviewed production image as <repository>@sha256:<digest>; "
                "a mutable tag cannot bind the job to this controller revision."
            )
        if not isinstance(controller.get("location"), str) or not controller["location"]:
            raise SchedulerError("Azure did not report the live controller location.")
        return controller, environment

    def _job_body(
        self, controller: Mapping[str, Any], controller_env: Mapping[str, str],
        schedule_utc: str | None,
    ) -> dict:
        source = _single_container(controller)
        environment = [
            {"name": "FAB_SHUFFLE_BCDR_AUTH_MODE", "value": "managed_identity"},
            {"name": "FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID",
             "value": self.tokens.principal.client_id},
            {"name": "FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID",
             "value": self.tokens.principal.tenant_id},
            {"name": "FAB_SHUFFLE_SCRATCH", "value": MOUNT_PATH},
            {"name": "FAB_SHUFFLE_BCDR_BOOTSTRAP", "value": str(BOOTSTRAP_PATH)},
            {"name": LEASE_ENV, "value": controller_env[LEASE_ENV]},
        ]
        for name in ("FAB_SHUFFLE_MAX_DISK_STAGING_BYTES", "FAB_SHUFFLE_MAX_MEMORY_BYTES"):
            if name in controller_env:
                environment.append({"name": name, "value": controller_env[name]})
        configuration = {
            "triggerType": "Manual" if schedule_utc is None else "Schedule",
            "replicaTimeout": 14400,
            "replicaRetryLimit": 0,
        }
        trigger = {"replicaCompletionCount": 1, "parallelism": 1}
        configuration[
            "manualTriggerConfig" if schedule_utc is None else "scheduleTriggerConfig"
        ] = trigger if schedule_utc is None else {**trigger, "cronExpression": schedule_utc}
        return {
            "location": controller.get("location"),
            "identity": {
                "type": "UserAssigned",
                "userAssignedIdentities": {self.identity_id: {}},
            },
            "properties": {
                "environmentId": self.environment_id,
                "configuration": configuration,
                "template": {
                    "containers": [{
                        "name": CONTAINER_NAME,
                        "image": source["image"],
                        "command": ["python"],
                        "args": [
                            "-m", "fabshuffle.bcdr", "scheduled-sync",
                            "--bootstrap", str(BOOTSTRAP_PATH),
                            "--request", str(REQUEST_PATH),
                            "--confirm", "scheduled-sync",
                        ],
                        "resources": source.get("resources") or {"cpu": 2, "memory": "4Gi"},
                        "env": environment,
                        "volumeMounts": [{"volumeName": VOLUME_NAME, "mountPath": MOUNT_PATH}],
                    }],
                    "volumes": [{
                        "name": VOLUME_NAME, "storageType": "AzureFile",
                        "storageName": STORAGE_NAME,
                    }],
                },
            },
        }

    def _persist(self, content: bytes) -> None:
        request_path = Path(REQUEST_PATH)
        request_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = request_path.with_name(f".{request_path.name}.{os.getpid()}.tmp")
        try:
            try:
                with temporary.open("xb") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.chmod(temporary, 0o600)
                temporary.replace(request_path)
            except OSError as error:
                raise SchedulerError(
                    f"Could not persist the reviewed request at {REQUEST_PATH}. "
                    "The scheduled trigger remains disabled; repair the shared Azure Files mount."
                ) from error
        finally:
            temporary.unlink(missing_ok=True)

    def observe(self, *, controller: dict | None = None) -> dict:
        controller, controller_env = self._controller() if controller is None else (
            controller, _env_map(_single_container(controller)),
        )
        job = self._request("GET", self.job_id)
        if not job:
            return {
                "status": "not_deployed", "job_resource_id": self.job_id,
                "request_path": str(REQUEST_PATH),
                "message": "No scheduled job is deployed. Review and create the live schedule.",
            }
        executions = self._request("GET", f"{self.job_id}/executions")
        values = executions.get("value") if isinstance(executions.get("value"), list) else []
        latest = max(
            (row for row in values if isinstance(row, dict)),
            key=lambda row: str((row.get("properties") or {}).get("startTime") or ""),
            default=None,
        )
        properties = job.get("properties") or {}
        configuration = properties.get("configuration") or {}
        schedule = configuration.get("scheduleTriggerConfig") or {}
        container = _single_container(job)
        env = _env_map(container)
        expected = self._job_body(
            controller, controller_env,
            schedule.get("cronExpression") if configuration.get("triggerType") == "Schedule" else None,
        )
        expected_properties = expected["properties"]
        expected_container = expected_properties["template"]["containers"][0]
        identities = (job.get("identity") or {}).get("userAssignedIdentities") or {}
        mounts = container.get("volumeMounts") or []
        volumes = properties.get("template", {}).get("volumes") or []
        matches_runtime = (
            str(properties.get("environmentId") or "").casefold() == self.environment_id.casefold()
            and {str(key).casefold() for key in identities} == {self.identity_id.casefold()}
            and container.get("image") == _single_container(controller).get("image")
            and container.get("command") == ["python"]
            and container.get("args") == expected_container["args"]
            and container.get("resources") == expected_container["resources"]
            and env.get(LEASE_ENV) == controller_env.get(LEASE_ENV)
            and env.get("FAB_SHUFFLE_BCDR_BOOTSTRAP") == str(BOOTSTRAP_PATH)
            and env.get("FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID") == self.tokens.principal.client_id
            and env.get("FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID") == self.tokens.principal.tenant_id
            and env.get("FAB_SHUFFLE_BCDR_AUTH_MODE") == "managed_identity"
            and all(env.get(name) == controller_env.get(name) for name in (
                "FAB_SHUFFLE_MAX_DISK_STAGING_BYTES", "FAB_SHUFFLE_MAX_MEMORY_BYTES",
            ) if name in controller_env)
            and mounts == [{"volumeName": VOLUME_NAME, "mountPath": MOUNT_PATH}]
            and volumes == [{
                "name": VOLUME_NAME, "storageType": "AzureFile", "storageName": STORAGE_NAME,
            }]
        )
        request_sha256 = None
        request_path = Path(REQUEST_PATH)
        if request_path.exists():
            try:
                content = request_path.read_bytes()
                reject_embedded_secrets(content)
                read_request(request_path)
            except (OSError, ValueError) as error:
                raise SchedulerError(
                    f"Could not validate the approved request at {REQUEST_PATH}: {error}"
                ) from error
            request_sha256 = digest(content)
        return {
            "status": "deployed" if configuration.get("triggerType") == "Schedule" else "disabled",
            "job_resource_id": self.job_id,
            "provisioning_state": properties.get("provisioningState"),
            "trigger_type": configuration.get("triggerType"),
            "schedule_utc": schedule.get("cronExpression"),
            "replica_timeout_seconds": configuration.get("replicaTimeout"),
            "runtime_matches_controller": matches_runtime,
            "request_path": str(REQUEST_PATH),
            "request_sha256": request_sha256,
            "latest_execution": None if latest is None else {
                "name": latest.get("name"),
                "status": (latest.get("properties") or {}).get("status"),
                "start_time": (latest.get("properties") or {}).get("startTime"),
                "end_time": (latest.get("properties") or {}).get("endTime"),
            },
        }

    def configure(self, request: SyncRequest, schedule_utc: str) -> dict:
        schedule_utc = validate_cron(schedule_utc)
        content = _request_bytes(request)
        controller, controller_env = self._controller()
        current = self.observe(controller=controller)
        if (
            current["status"] == "deployed"
            and current["schedule_utc"] == schedule_utc
            and current["runtime_matches_controller"]
            and current["request_sha256"] == digest(content)
        ):
            return {**current, "changed": False}

        disabled = self._job_body(controller, controller_env, None)
        enabled = self._job_body(controller, controller_env, schedule_utc)
        self._request("PUT", self.job_id, json_body=disabled)
        try:
            self._persist(content)
            self._request("PUT", self.job_id, json_body=enabled)
        except SchedulerError as error:
            try:
                self._request("PUT", self.job_id, json_body=disabled)
            except SchedulerError as disable_error:
                raise SchedulerError(
                    f"{error} Azure also could not verify that the scheduled trigger was disabled: "
                    f"{disable_error}"
                ) from error
            raise
        try:
            observed = self.observe(controller=controller)
            if (
                observed["status"] != "deployed"
                or observed["schedule_utc"] != schedule_utc
                or not observed["runtime_matches_controller"]
                or observed["request_sha256"] != digest(content)
            ):
                raise SchedulerError(
                    "Azure returned success but the observed scheduled job does not match the "
                    "reviewed request and controller."
                )
        except SchedulerError as error:
            try:
                self._request("PUT", self.job_id, json_body=disabled)
            except SchedulerError as disable_error:
                raise SchedulerError(
                    f"{error} Azure also could not verify that the scheduled trigger was disabled: "
                    f"{disable_error}"
                ) from error
            raise SchedulerError(
                f"{error} The controller disabled scheduled triggers; inspect scheduler status "
                "and retry after repairing the reported issue."
            ) from error
        return {**observed, "changed": True}
