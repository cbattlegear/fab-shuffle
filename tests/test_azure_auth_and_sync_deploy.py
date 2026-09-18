"""Static deployment contracts; no Azure evaluation, credentials or live resources."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

DEPLOY = Path(__file__).resolve().parent.parent / "deploy"
IDENTITY_REFERENCE = "reference(parameters('managedIdentityResourceId'), '2023-01-31')"
LOCK_SCOPE = (
    "[resourceId('Microsoft.Storage/storageAccounts/blobServices/containers', "
    "variables('storageAccountName'), 'default', variables('bcdrLeaseContainerName'))]"
)


@pytest.fixture
def web() -> dict[str, Any]:
    return json.loads((DEPLOY / "azuredeploy.json").read_text(encoding="utf-8"))


@pytest.fixture
def job() -> dict[str, Any]:
    return json.loads((DEPLOY / "azuredeploy-sync-job.json").read_text(encoding="utf-8"))


def _resource(template: dict[str, Any], resource_type: str) -> dict[str, Any]:
    matches = [resource for resource in template["resources"] if resource["type"] == resource_type]
    assert len(matches) == 1
    return matches[0]


def _container(template: dict[str, Any], resource_type: str) -> dict[str, Any]:
    [container] = _resource(template, resource_type)["properties"]["template"]["containers"]
    return container


def test_easyauth_registration_and_allowlist_have_no_empty_or_tenant_wide_defaults(web) -> None:
    params = web["parameters"]
    for name in ("easyAuthTenantId", "easyAuthClientId"):
        assert params[name]["type"] == "string"
        assert params[name]["minLength"] == params[name]["maxLength"] == 36
        assert "defaultValue" not in params[name]
    assert params["easyAuthClientSecret"]["type"] == "securestring"
    assert params["easyAuthClientSecret"]["minLength"] == 1
    assert "defaultValue" not in params["easyAuthClientSecret"]
    assert params["allowedOperatorObjectIds"]["type"] == "array"
    assert params["allowedOperatorObjectIds"]["minLength"] == 1
    assert params["allowedOperatorObjectIds"]["maxLength"] == 100
    assert "defaultValue" not in params["allowedOperatorObjectIds"]


def test_auth_config_always_protects_all_but_exact_health_path(web) -> None:
    auth = _resource(web, "Microsoft.App/containerApps/authConfigs")
    assert "condition" not in auth
    assert auth["apiVersion"] == "2024-03-01"
    assert auth["name"] == "[concat(variables('appName'), '/current')]"
    assert auth["dependsOn"] == [
        "[resourceId('Microsoft.App/containerApps', variables('appName'))]"
    ]
    assert auth["properties"]["platform"] == {"enabled": True}
    assert auth["properties"]["httpSettings"] == {"requireHttps": True}
    assert auth["properties"]["login"] == {"tokenStore": {"enabled": False}}
    assert auth["properties"]["globalValidation"] == {
        "unauthenticatedClientAction": "RedirectToLoginPage",
        "redirectToProvider": "azureActiveDirectory",
        "excludedPaths": ["/api/health"],
    }
    # requireAuthentication is not a property of this Container Apps API version.
    assert "requireAuthentication" not in json.dumps(auth)
    assert set(auth["properties"]["identityProviders"]) == {"azureActiveDirectory"}
    provider = auth["properties"]["identityProviders"]["azureActiveDirectory"]
    assert provider["enabled"] is True
    assert provider["isAutoProvisioned"] is False
    assert provider["registration"]["openIdIssuer"] == (
        "[concat('https://login.microsoftonline.com/', parameters('easyAuthTenantId'), '/v2.0')]"
    )
    assert provider["validation"] == {
        "allowedAudiences": [
            "[parameters('easyAuthClientId')]",
            "[concat('api://', parameters('easyAuthClientId'))]",
        ],
        "defaultAuthorizationPolicy": {
            "allowedPrincipals": {"identities": "[parameters('allowedOperatorObjectIds')]"},
        },
    }


def test_hybrid_secret_is_only_a_container_secret_referenced_by_registration(web) -> None:
    app = _resource(web, "Microsoft.App/containerApps")
    auth = _resource(web, "Microsoft.App/containerApps/authConfigs")
    registration = auth["properties"]["identityProviders"]["azureActiveDirectory"]["registration"]
    assert registration["clientId"] == "[parameters('easyAuthClientId')]"
    assert registration["clientSecretSettingName"] == "[variables('easyAuthSecretName')]"
    assert web["variables"]["easyAuthSecretName"] == "fab-shuffle-easyauth"
    assert app["properties"]["configuration"]["secrets"] == [{
        "name": "[variables('easyAuthSecretName')]",
        "value": "[parameters('easyAuthClientSecret')]",
    }]
    assert json.dumps(web).count("parameters('easyAuthClientSecret')") == 1
    assert "easyAuthClientSecret" not in json.dumps(web["outputs"])
    assert "easyAuthClientSecret" not in json.dumps(web["variables"])
    assert "easyAuthClientSecret" not in json.dumps(app["properties"]["template"])
    assert web["outputs"]["easyAuthCallbackUrl"]["value"] == (
        "[concat('https://', variables('appName'), '.', "
        "reference(resourceId('Microsoft.App/managedEnvironments', "
        "variables('environmentName')), '2024-03-01').defaultDomain, "
        "'/.auth/login/aad/callback')]"
    )


def test_application_guard_is_unconditional_before_auth_child_is_created(web) -> None:
    env = {entry["name"]: entry["value"] for entry in web["variables"]["containerEnvironment"]}
    assert env["FAB_SHUFFLE_EASYAUTH_ENABLED"] == "true"
    assert env["FAB_SHUFFLE_EASYAUTH_READY"] == "false"
    assert env["FAB_SHUFFLE_EASYAUTH_TENANT_ID"] == "[parameters('easyAuthTenantId')]"
    assert env["FAB_SHUFFLE_EASYAUTH_ALLOWED_OBJECT_IDS"] == (
        "[join(parameters('allowedOperatorObjectIds'), ',')]"
    )
    assert not any(name.startswith("FAB_SHUFFLE_MANAGED_IDENTITY") for name in env)
    assert all("SECRET" not in name for name in env)


def test_public_ingress_and_readiness_cannot_be_enabled_by_template_parameters(web) -> None:
    app = _resource(web, "Microsoft.App/containerApps")
    assert app["properties"]["configuration"]["ingress"]["external"] is False
    assert "FAB_SHUFFLE_EASYAUTH_READY=true" not in json.dumps(web["resources"])
    assert "FAB_SHUFFLE_EASYAUTH_READY=true" not in json.dumps(web["variables"])
    commands = {
        "verifyAuthenticationCommand": "auth show",
        "activateApplicationCommand": "update",
        "enablePublicIngressCommand": "ingress enable",
        "readPublicFqdnCommand": "show",
    }
    for output, command in commands.items():
        assert web["outputs"][output]["value"].startswith(f"[format('az containerapp {command} ")
        assert "subscription().subscriptionId, resourceGroup().name, variables('appName')" in (
            web["outputs"][output]["value"]
        )
    activate = web["outputs"]["activateApplicationCommand"]["value"]
    assert "--set-env-vars FAB_SHUFFLE_EASYAUTH_READY=true" in activate
    for unsafe_replacement in ("--replace-env-vars", "--yaml", "--image", "--remove"):
        assert unsafe_replacement not in activate
    ingress = web["outputs"]["enablePublicIngressCommand"]["value"]
    assert "--type external --target-port 8080 --transport auto" in ingress
    assert "--allow-insecure" not in ingress
    assert not any("ready" in name.lower() or "ingress" in name.lower() for name in web["parameters"])
    assert "LAST" in web["outputs"]["activateApplicationCommand"]["metadata"]["description"]
    assert "WHILE application-ready=false" in (
        web["outputs"]["enablePublicIngressCommand"]["metadata"]["description"]
    )


def test_optional_existing_identity_has_lazy_runtime_references_and_no_secret_fallback(web) -> None:
    assert web["parameters"]["managedIdentityResourceId"]["defaultValue"] == ""
    assert web["variables"]["useManagedIdentity"] == (
        "[not(empty(parameters('managedIdentityResourceId')))]"
    )
    app = _resource(web, "Microsoft.App/containerApps")
    assert app["identity"] == (
        "[if(variables('useManagedIdentity'), createObject('type', 'UserAssigned', "
        "'userAssignedIdentities', createObject(parameters('managedIdentityResourceId'), "
        "createObject())), createObject('type', 'None'))]"
    )
    env = _container(web, "Microsoft.App/containerApps")["env"]
    assert env == (
        "[concat(variables('containerEnvironment'), if(variables('useManagedIdentity'), "
        "createArray(createObject('name', 'FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID', 'value', "
        f"{IDENTITY_REFERENCE}.clientId), createObject('name', "
        "'FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID', 'value', "
        f"{IDENTITY_REFERENCE}.tenantId)), createArray()))]"
    )
    assert "reference(" not in json.dumps(web["variables"])
    for output, property_name in (
        ("managedIdentityClientId", "clientId"),
        ("managedIdentityPrincipalId", "principalId"),
        ("managedIdentityTenantId", "tenantId"),
    ):
        assert web["outputs"][output]["value"] == (
            f"[if(variables('useManagedIdentity'), {IDENTITY_REFERENCE}.{property_name}, '')]"
        )
    assert not any(r["type"].startswith("Microsoft.ManagedIdentity/") for r in web["resources"])


def test_private_blob_lock_is_not_a_metadata_archive_or_a_broad_role_assignment(web) -> None:
    container = _resource(web, "Microsoft.Storage/storageAccounts/blobServices/containers")
    assert container["apiVersion"] == "2023-01-01"
    assert container["name"] == (
        "[format('{0}/default/{1}', variables('storageAccountName'), "
        "variables('bcdrLeaseContainerName'))]"
    )
    assert container["properties"] == {"publicAccess": "None"}
    assert container["dependsOn"] == [
        "[resourceId('Microsoft.Storage/storageAccounts', variables('storageAccountName'))]"
    ]
    role = _resource(web, "Microsoft.Authorization/roleAssignments")
    assert role["condition"] == "[variables('useManagedIdentity')]"
    assert role["apiVersion"] == "2022-04-01"
    assert role["scope"] == LOCK_SCOPE
    assert role["dependsOn"] == [LOCK_SCOPE]
    assert role["properties"] == {
        "roleDefinitionId": (
            "[subscriptionResourceId('Microsoft.Authorization/roleDefinitions', "
            "'ba92f5b4-2d11-453d-a403-e96b0029c9fe')]"
        ),
        "principalId": f"[if(variables('useManagedIdentity'), {IDENTITY_REFERENCE}.principalId, '')]",
        "principalType": "ServicePrincipal",
    }
    assert web["variables"]["bcdrLeaseContainerName"] == "fab-shuffle-locks"
    assert web["variables"]["bcdrLeaseBlobUrl"] == (
        "[concat('https://', variables('storageAccountName'), '.blob.core.windows.net/', "
        "variables('bcdrLeaseContainerName'), '/deployment.lock')]"
    )
    env = {entry["name"]: entry["value"] for entry in web["variables"]["containerEnvironment"]}
    assert env["FAB_SHUFFLE_BCDR_LEASE_BLOB_URL"] == "[variables('bcdrLeaseBlobUrl')]"
    assert web["outputs"]["bcdrLeaseBlobUrl"]["value"] == "[variables('bcdrLeaseBlobUrl')]"
    assert web["outputs"]["bcdrLeaseContainerResourceId"]["value"] == LOCK_SCOPE
    assert not any(r["type"] == "Microsoft.Resources/deploymentScripts" for r in web["resources"])


def test_job_only_reuses_an_existing_environment_and_has_no_ingress_or_new_catalog(job) -> None:
    assert len(job["resources"]) == 1
    resource = _resource(job, "Microsoft.App/jobs")
    assert resource["apiVersion"] == "2024-03-01"
    assert resource["properties"]["environmentId"] == (
        "[resourceId('Microsoft.App/managedEnvironments', "
        "parameters('containerAppsEnvironmentName'))]"
    )
    assert "ingress" not in json.dumps(resource)
    assert "EASYAUTH" not in json.dumps(job)
    assert "secrets" not in resource["properties"]["configuration"]
    assert "initContainers" not in resource["properties"]["template"]
    assert "listKeys" not in json.dumps(job)
    assert "dependsOn" not in resource  # All prerequisites already exist, outside this deployment.
    for name in ("containerAppsEnvironmentName", "managedIdentityResourceId", "bcdrLeaseBlobUrl"):
        assert job["parameters"][name]["minLength"] == 1
        assert "defaultValue" not in job["parameters"][name]


def test_job_cannot_interpolate_a_shell_command_or_select_an_activation_action(job) -> None:
    container = _container(job, "Microsoft.App/jobs")
    assert container["command"] == ["python"]
    assert container["args"] == [
        "-m", "fabshuffle.bcdr", "scheduled-sync",
        "--bootstrap", "[variables('bootstrapPath')]",
        "--request", "[variables('requestPath')]",
        "--confirm", "scheduled-sync",
    ]
    assert job["variables"]["bootstrapPath"] == "/app/local/bcdr/bootstrap.json"
    assert job["variables"]["requestPath"] == "/app/local/bcdr/scheduled-sync-request.json"
    assert set(job["parameters"]) == {
        "jobName", "containerAppsEnvironmentName", "location", "containerImage",
        "managedIdentityResourceId", "bcdrLeaseBlobUrl", "scheduleUtc",
        "replicaTimeoutSeconds", "cpuCores", "memoryGb", "maxDiskStagingGiB", "maxMemoryMiB",
    }


def test_job_requires_one_explicit_identity_and_the_shared_remote_guard(job) -> None:
    resource = _resource(job, "Microsoft.App/jobs")
    assert resource["identity"] == {
        "type": "UserAssigned",
        "userAssignedIdentities": {"[parameters('managedIdentityResourceId')]": {}},
    }
    env = {entry["name"]: entry["value"] for entry in _container(job, "Microsoft.App/jobs")["env"]}
    assert env["FAB_SHUFFLE_BCDR_AUTH_MODE"] == "managed_identity"
    assert env["FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID"] == f"[{IDENTITY_REFERENCE}.clientId]"
    assert env["FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID"] == f"[{IDENTITY_REFERENCE}.tenantId]"
    assert env["FAB_SHUFFLE_BCDR_LEASE_BLOB_URL"] == "[parameters('bcdrLeaseBlobUrl')]"
    assert env["FAB_SHUFFLE_BCDR_BOOTSTRAP"] == "[variables('bootstrapPath')]"
    assert all("SECRET" not in name and "TOKEN" not in name for name in env)
    for output, property_name in (
        ("managedIdentityClientId", "clientId"),
        ("managedIdentityPrincipalId", "principalId"),
        ("managedIdentityTenantId", "tenantId"),
    ):
        assert job["outputs"][output]["value"] == f"[{IDENTITY_REFERENCE}.{property_name}]"


def test_job_schedule_has_bounded_duration_no_retries_and_one_replica_per_execution(job) -> None:
    config = _resource(job, "Microsoft.App/jobs")["properties"]["configuration"]
    assert config == {
        "triggerType": "Schedule",
        "replicaTimeout": "[parameters('replicaTimeoutSeconds')]",
        "replicaRetryLimit": 0,
        "scheduleTriggerConfig": {
            "cronExpression": "[parameters('scheduleUtc')]",
            "replicaCompletionCount": 1,
            "parallelism": 1,
        },
    }
    timeout = job["parameters"]["replicaTimeoutSeconds"]
    assert timeout["minValue"] == 60
    assert timeout["defaultValue"] == 14400
    assert timeout["maxValue"] == 86400
    cron = job["parameters"]["scheduleUtc"]
    assert cron["defaultValue"] == "0 2 * * *"
    assert "UTC" in cron["metadata"]["description"]
    assert "per execution, not a global overlap lock" in cron["metadata"]["description"]


def test_job_uses_the_controller_mount_and_budgets_without_mutating_persisted_files(web, job) -> None:
    for name in ("storageMountName", "scratchMountPath", "scratchVolumeName"):
        assert job["variables"][name] == web["variables"][name]
    job_template = _resource(job, "Microsoft.App/jobs")["properties"]["template"]
    web_template = _resource(web, "Microsoft.App/containerApps")["properties"]["template"]
    assert job_template["volumes"] == web_template["volumes"]
    assert job_template["containers"][0]["volumeMounts"] == web_template["containers"][0]["volumeMounts"]
    assert job["variables"]["bootstrapPath"] == web["outputs"]["controllerBootstrapPath"]["value"]
    assert "nobrl" not in json.dumps(job_template["volumes"])
    for parameter in ("containerImage", "cpuCores", "memoryGb", "maxDiskStagingGiB", "maxMemoryMiB"):
        assert job["parameters"][parameter]["defaultValue"] == web["parameters"][parameter]["defaultValue"]
    env = {entry["name"]: entry["value"] for entry in job_template["containers"][0]["env"]}
    assert env["FAB_SHUFFLE_SCRATCH"] == "[variables('scratchMountPath')]"
    assert env["FAB_SHUFFLE_MAX_DISK_STAGING_BYTES"] == (
        "[string(mul(parameters('maxDiskStagingGiB'), 1073741824))]"
    )
    assert env["FAB_SHUFFLE_MAX_MEMORY_BYTES"] == "[string(mul(parameters('maxMemoryMiB'), 1048576))]"
    assert "shareQuotaGiB" not in job["parameters"]


@pytest.mark.parametrize("filename", ["azuredeploy.json", "azuredeploy-sync-job.json"])
def test_templates_have_no_dangling_parameter_or_variable_references(filename) -> None:
    template = json.loads((DEPLOY / filename).read_text(encoding="utf-8"))
    for name in re.findall(r"parameters\('([^']+)'\)", json.dumps(template)):
        assert name in template["parameters"], name
    for name in re.findall(r"variables\('([^']+)'\)", json.dumps(template)):
        assert name in template["variables"], name
    for parameter in template["parameters"].values():
        default = parameter.get("defaultValue")
        assert not (isinstance(default, str) and default.startswith("["))
    assert "reference(" not in json.dumps(template["variables"])
    assert "listKeys(" not in json.dumps(template["outputs"])
