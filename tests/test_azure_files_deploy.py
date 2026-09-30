"""The Azure deploy template must ship a persistent Azure Files share by default.

Fab Shuffle stages lakehouse files and warehouse schema on local disk on the way past, and a
Container App's writable layer is thrown away with the container. Before this template shipped
an Azure Files mount, using Fab Shuffle in Azure durably meant a manual, three-step exercise
(create a storage account, create a share, wire the environment storage config by hand) that
the default template did not do for you. This asserts the template now does it: a classic
``Microsoft.Storage/storageAccounts/fileServices/shares`` (the only kind Container Apps SMB
mounts support; the newer ``Microsoft.FileShares`` resource type is NFS-only and can't be
mounted here) is created, quota-configurable, and mounted read/write at ``/app/local`` without
any extra deployment step.

Every resource this test inspects is a plain ARM JSON dict read from disk: no template engine,
CLI, or bicep tooling is invoked, and nothing here talks to Azure. That mirrors the rest of the
suite, which never contacts a real Fabric tenant either.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "deploy" / "azuredeploy.json"


def _load_template() -> dict[str, Any]:
    return json.loads(TEMPLATE_PATH.read_text(encoding="utf-8"))


def _resources_by_type(template: dict[str, Any], resource_type: str) -> list[dict[str, Any]]:
    return [r for r in template["resources"] if r["type"] == resource_type]


def _one(template: dict[str, Any], resource_type: str) -> dict[str, Any]:
    matches = _resources_by_type(template, resource_type)
    assert len(matches) == 1, f"expected exactly one {resource_type}, found {len(matches)}"
    return matches[0]


def _base_environment(template: dict[str, Any]) -> list[dict[str, str]]:
    container = _one(template, "Microsoft.App/containerApps")["properties"]["template"]["containers"][0]
    assert container["env"].startswith("[concat(variables('containerEnvironment'), if(")
    return template["variables"]["containerEnvironment"]


def test_template_schema_and_content_version_untouched() -> None:
    template = _load_template()
    expected_schema = "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#"
    assert template["$schema"] == expected_schema
    assert template["contentVersion"] == "1.0.0.0"


def test_only_controller_storage_authentication_and_lock_resources_are_provisioned() -> None:
    """No second metadata archive or broad-scope authorization is provisioned."""
    template = _load_template()
    types = sorted(r["type"] for r in template["resources"])
    assert types == sorted(
        [
            "Microsoft.OperationalInsights/workspaces",
            "Microsoft.App/managedEnvironments",
            "Microsoft.App/containerApps",
            "Microsoft.Storage/storageAccounts",
            "Microsoft.Storage/storageAccounts/fileServices/shares",
            "Microsoft.App/managedEnvironments/storages",
            "Microsoft.App/containerApps/authConfigs",
            "Microsoft.Storage/storageAccounts/blobServices/containers",
            "Microsoft.Authorization/roleAssignments",
        ]
    )


def test_uses_classic_file_share_not_the_new_fileshares_resource_provider() -> None:
    """Container Apps SMB mounts only support classic shares under a storage account.

    Microsoft.FileShares is a newer, storage-account-less resource type, but per Microsoft
    Learn's Container Apps storage-mounts documentation it supports only NFS, and Azure Files
    SMB mounts (which this template uses, since NFS mounts require a custom VNet this simple
    template does not set up) need the classic Microsoft.Storage/storageAccounts/fileServices/
    shares resource instead.
    """
    template = _load_template()
    types = [r["type"] for r in template["resources"]]
    assert "Microsoft.Storage/storageAccounts/fileServices/shares" in types
    assert "Microsoft.FileShares" not in types
    assert not any(t.startswith("Microsoft.FileShares/") for t in types)


def test_share_quota_parameter_is_int_with_the_documented_range_and_default() -> None:
    template = _load_template()
    param = template["parameters"]["shareQuotaGiB"]
    assert param["type"] == "int"
    assert param["defaultValue"] == 100
    assert param["minValue"] == 1
    # 102400 GiB (100 TiB) is the documented ceiling for a classic pay-as-you-go file share.
    assert param["maxValue"] == 102400


def test_storage_account_name_is_deterministic_lowercase_and_within_length_limit() -> None:
    """Storage account names must be 3-24 lowercase alphanumeric characters, globally.

    The name must not be built from the raw, hyphenated appName parameter (storage account
    names can't contain hyphens); it must instead come from uniqueString, which is documented
    to always return a 13-character lowercase alphanumeric string. appName is only allowed to
    feed that hash, as an extra uniqueString seed, so overriding appName can't itself produce
    an invalid account name.
    """
    template = _load_template()
    expression = template["variables"]["storageAccountName"]
    assert expression == (
        "[toLower(concat('fabshuffle', uniqueString(resourceGroup().id, variables('appName'))))]"
    )
    # Extract the literal prefix concatenated ahead of the uniqueString(...) hash.
    match = re.search(r"concat\('([a-z0-9]+)',\s*uniqueString\(", expression)
    assert match, f"expected a lowercase literal prefix concatenated with uniqueString: {expression}"
    prefix = match.group(1)
    assert prefix.isalnum() and prefix == prefix.lower()
    # uniqueString(...) always returns exactly 13 characters (Microsoft Learn: ARM template
    # string functions, uniqueString remarks); the deterministic name must still fit in 24.
    assert len(prefix) + 13 <= 24


def test_file_share_depends_on_storage_account() -> None:
    template = _load_template()
    share = _one(template, "Microsoft.Storage/storageAccounts/fileServices/shares")
    assert "Microsoft.Storage/storageAccounts', variables('storageAccountName')" in share["dependsOn"][0]
    expected_share_name = (
        "[format('{0}/default/{1}', variables('storageAccountName'), variables('fileShareName'))]"
    )
    assert share["name"] == expected_share_name
    assert share["properties"]["shareQuota"] == "[parameters('shareQuotaGiB')]"
    assert share["properties"]["enabledProtocols"] == "SMB"
    assert share["properties"]["accessTier"] == "TransactionOptimized"


def test_environment_storage_depends_on_environment_and_file_share() -> None:
    template = _load_template()
    storage = _one(template, "Microsoft.App/managedEnvironments/storages")
    depends_on = " ".join(storage["dependsOn"])
    assert "Microsoft.App/managedEnvironments', variables('environmentName')" in depends_on
    assert "Microsoft.Storage/storageAccounts/fileServices/shares" in depends_on


def test_environment_storage_uses_read_write_azure_file_with_list_keys() -> None:
    """Matches the ARM properties documented for managedEnvironments/storages: accessMode,
    accountKey, accountName, shareName under an azureFile block, with the key obtained via
    listKeys() rather than a plaintext parameter.
    """
    template = _load_template()
    storage = _one(template, "Microsoft.App/managedEnvironments/storages")
    azure_file = storage["properties"]["azureFile"]
    assert azure_file["accessMode"] == "ReadWrite"
    assert azure_file["accountName"] == "[variables('storageAccountName')]"
    assert azure_file["shareName"] == "[variables('fileShareName')]"
    assert azure_file["accountKey"].startswith("[listKeys(")
    assert "storageAccountName" in azure_file["accountKey"]


def test_storage_account_key_never_becomes_a_parameter_output_or_container_env_value() -> None:
    """The account key must stay inside the listKeys() expression: not an output, not logged,
    not handed to the container as a plain environment variable.
    """
    template = _load_template()
    serialized = json.dumps(template["outputs"])
    assert "listKeys" not in serialized
    assert "accountKey" not in serialized
    for env_entry in _base_environment(template):
        assert "listKeys" not in json.dumps(env_entry)
        assert env_entry["name"] != "accountKey"
    container = _one(template, "Microsoft.App/containerApps")["properties"]["template"]["containers"][0]
    assert "listKeys" not in container["env"]
    assert set(template["outputs"]) == {
        "url", "easyAuthCallbackUrl", "containerAppsEnvironmentName", "storageMountName",
        "controllerBootstrapPath", "bcdrLeaseBlobUrl", "bcdrLeaseContainerResourceId",
        "managedIdentityResourceId", "managedIdentityClientId", "managedIdentityPrincipalId",
        "managedIdentityTenantId", "verifyAuthenticationCommand", "activateApplicationCommand",
        "enablePublicIngressCommand", "readPublicFqdnCommand",
    }


def test_storage_account_security_settings() -> None:
    """SMB mounting needs the account key (Shared Key), so allowSharedKeyAccess must stay
    true; everything else favors the safer default regardless. largeFileSharesState is
    explicitly Enabled so a shareQuotaGiB above 5120 (5 TiB) isn't relying on new-account
    default behaviour the shares API reference still documents as conditional.
    """
    template = _load_template()
    account = _one(template, "Microsoft.Storage/storageAccounts")
    assert account["kind"] == "StorageV2"
    assert account["sku"]["name"] == "Standard_LRS"
    props = account["properties"]
    assert props["allowSharedKeyAccess"] is True
    assert props["allowBlobPublicAccess"] is False
    assert props["supportsHttpsTrafficOnly"] is True
    assert props["minimumTlsVersion"] == "TLS1_2"
    assert props["largeFileSharesState"] == "Enabled"


def test_share_mount_does_not_depend_on_optional_runtime_managed_identity() -> None:
    """SMB still uses the deployment-resolved account key, not runtime identity credentials."""
    template = _load_template()
    storage = _one(template, "Microsoft.App/managedEnvironments/storages")
    assert "condition" not in storage
    assert "identity" not in json.dumps(storage).lower()
    assert "listKeys" in storage["properties"]["azureFile"]["accountKey"]


def test_scratch_mount_path_and_env_var_agree_with_the_container_image_default() -> None:
    """FAB_SHUFFLE_SCRATCH must point at the same path as the volume mount, and that path
    must be the /app/local the Dockerfile already treats as the scratch root, so a rebuild
    of the image doesn't silently stop lining up with this template.
    """
    template = _load_template()
    container_app = _one(template, "Microsoft.App/containerApps")
    container = container_app["properties"]["template"]["containers"][0]

    # The template resolves this path through a single shared variable, so the environment
    # variable and the mount below cannot drift apart from each other.
    assert template["variables"]["scratchMountPath"] == "/app/local"

    scratch_env = next(e for e in _base_environment(template) if e["name"] == "FAB_SHUFFLE_SCRATCH")
    assert scratch_env["value"] == "[variables('scratchMountPath')]"

    [mount] = container["volumeMounts"]
    assert mount["mountPath"] == "[variables('scratchMountPath')]"

    dockerfile = (TEMPLATE_PATH.parent.parent / "Dockerfile").read_text(encoding="utf-8")
    assert "FAB_SHUFFLE_SCRATCH=/app/local" in dockerfile


def test_recovery_journals_land_on_the_mounted_share_not_just_staged_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The README promises the share backs both staging *and* recovery journals. That is a
    property of ``fabshuffle.config.Settings``, not of this template, but it is exactly the
    behaviour this template's env var switches on: confirm it here, against the same
    FAB_SHUFFLE_SCRATCH value the template sets, so the two files can't quietly disagree.
    """
    from fabshuffle.config import Settings

    template = _load_template()
    scratch_mount_path = template["variables"]["scratchMountPath"]
    monkeypatch.setenv("FAB_SHUFFLE_SCRATCH", scratch_mount_path)

    settings = Settings()
    assert settings.scratch_root == Path(scratch_mount_path).resolve()
    # journal_dir is a sibling directory under scratch_root, so it lives on the same mounted
    # share as per-run staging directories (scratch_dir_for), not somewhere ephemeral.
    assert settings.journal_dir == settings.scratch_root / "journal"
    assert settings.journal_dir.is_relative_to(settings.scratch_root)


def test_volume_mount_wires_to_the_azure_file_volume_in_the_environment() -> None:
    template = _load_template()
    container_app = _one(template, "Microsoft.App/containerApps")
    tmpl = container_app["properties"]["template"]
    container = tmpl["containers"][0]

    [mount] = container["volumeMounts"]
    [volume] = tmpl["volumes"]
    assert mount["volumeName"] == volume["name"]
    assert volume["storageType"] == "AzureFile"
    assert volume["storageName"] == "[variables('storageMountName')]"


def test_container_app_waits_for_the_environment_storage_before_it_can_start() -> None:
    """Without this dependsOn edge, ARM is free to create the container app (and have it try
    to mount a volume that doesn't exist yet) before the environment's storage config exists.
    """
    template = _load_template()
    container_app = _one(template, "Microsoft.App/containerApps")
    depends_on = " ".join(container_app["dependsOn"])
    assert "Microsoft.App/managedEnvironments'," in depends_on
    assert "Microsoft.App/managedEnvironments/storages'," in depends_on


def test_replica_limit_and_activation_mode_unchanged() -> None:
    """Fab Shuffle keeps session and run state in memory; a second replica would not see a
    sign-in or an in-flight migration. This must survive the Azure Files change untouched.
    """
    template = _load_template()
    container_app = _one(template, "Microsoft.App/containerApps")
    scale = container_app["properties"]["template"]["scale"]
    assert scale["minReplicas"] == 1
    assert scale["maxReplicas"] == 1
    assert container_app["properties"]["configuration"]["activeRevisionsMode"] == "Single"


def test_https_sticky_ingress_stays_internal_until_authentication_is_verified() -> None:
    template = _load_template()
    container_app = _one(template, "Microsoft.App/containerApps")
    ingress = container_app["properties"]["configuration"]["ingress"]
    assert ingress["external"] is False
    assert ingress["targetPort"] == 8080
    assert ingress["allowInsecure"] is False
    assert ingress["stickySessions"] == {"affinity": "sticky"}
    assert "ipSecurityRestrictions" not in ingress
    assert "allowedClientIpAddress" not in json.dumps(template)
    assert "restrictIngress" not in json.dumps(template)
    assert "ipRestrictions" not in json.dumps(template)


def test_existing_sizing_and_image_defaults_unchanged() -> None:
    template = _load_template()
    params = template["parameters"]
    assert params["containerImage"]["defaultValue"] == "ghcr.io/cbattlegear/fab-shuffle:latest"
    assert params["cpuCores"]["allowedValues"] == ["1", "2", "4"]
    assert params["memoryGb"]["allowedValues"] == ["2", "4", "8"]
    assert params["appName"]["maxLength"] == 32
    # appName still isn't reused verbatim as the storage account name (see the dedicated test
    # above); this just confirms the parameter's own shape survived.
    assert params["appName"]["defaultValue"] == ""


def test_app_and_environment_api_versions_unchanged() -> None:
    template = _load_template()
    container_app = _one(template, "Microsoft.App/containerApps")
    environment = _one(template, "Microsoft.App/managedEnvironments")
    assert container_app["apiVersion"] == "2024-03-01"
    assert environment["apiVersion"] == "2024-03-01"


def test_portal_parameters_do_not_display_unevaluated_arm_expressions() -> None:
    """The generated portal form displayed concat/resourceGroup expressions in text inputs."""
    template = _load_template()
    for name, parameter in template["parameters"].items():
        default = parameter.get("defaultValue")
        assert not (isinstance(default, str) and default.startswith("[")), name
    for name in ("appName", "location"):
        assert template["parameters"][name]["defaultValue"] == ""
        assert "leave blank" in template["parameters"][name]["metadata"]["description"].lower()


def test_blank_inputs_keep_the_previous_defaults_and_explicit_overrides() -> None:
    variables = _load_template()["variables"]
    assert variables["appName"] == (
        "[if(empty(parameters('appName')), "
        "concat('fab-shuffle-', uniqueString(resourceGroup().id)), parameters('appName'))]"
    )
    assert variables["location"] == (
        "[if(empty(parameters('location')), resourceGroup().location, parameters('location'))]"
    )


def test_every_deployed_name_uses_the_resolved_app_name_including_storage() -> None:
    template = _load_template()
    variables = template["variables"]
    assert variables["environmentName"] == "[concat(variables('appName'), '-env')]"
    assert variables["logAnalyticsName"] == "[concat(variables('appName'), '-logs')]"
    assert _one(template, "Microsoft.App/containerApps")["name"] == "[variables('appName')]"
    assert "resourceGroup().id, variables('appName')" in variables["storageAccountName"]
    assert "variables('appName'), '.'" in template["outputs"]["url"]["value"]
    assert "resourceId('Microsoft.App/managedEnvironments', variables('environmentName'))" in (
        template["outputs"]["url"]["value"]
    )
    # Leaving a raw parameter reference behind would create empty resource names or a
    # differently named share, losing the operator's existing persistent staging/journals.
    downstream = json.dumps({
        "variables": {key: value for key, value in variables.items() if key not in ("appName", "location")},
        "resources": template["resources"],
        "outputs": template["outputs"],
    })
    assert "parameters('appName')" not in downstream
    assert "parameters('location')" not in downstream


def test_every_regional_resource_uses_the_resolved_location() -> None:
    resources = [item for item in _load_template()["resources"] if "location" in item]
    assert len(resources) == 4
    assert all(item["location"] == "[variables('location')]" for item in resources)


def test_disk_memory_and_share_capacity_are_independent_parameters() -> None:
    template = _load_template()
    params = template["parameters"]
    assert params["shareQuotaGiB"]["defaultValue"] == 100
    assert params["maxDiskStagingGiB"]["defaultValue"] == 10
    assert params["maxMemoryMiB"]["defaultValue"] == 1024
    for key in ("maxDiskStagingGiB", "maxMemoryMiB"):
        assert params[key]["type"] == "int"
        assert params[key]["minValue"] > 0
    env = {entry["name"]: entry["value"] for entry in _base_environment(template)}
    assert env["FAB_SHUFFLE_MAX_DISK_STAGING_BYTES"] == (
        "[string(mul(parameters('maxDiskStagingGiB'), 1073741824))]"
    )
    assert env["FAB_SHUFFLE_MAX_MEMORY_BYTES"] == (
        "[string(mul(parameters('maxMemoryMiB'), 1048576))]"
    )
    assert "FAB_SHUFFLE_MAX_STAGING_BYTES" not in env
    assert "shareQuotaGiB" not in json.dumps(env)


def test_template_budget_defaults_match_the_runtime_without_legacy_overrides(monkeypatch) -> None:
    from fabshuffle.config import Settings

    for name in ("FAB_SHUFFLE_MAX_STAGING_BYTES", "FAB_SHUFFLE_MAX_MEMORY_BYTES",
                 "FAB_SHUFFLE_MAX_DISK_STAGING_BYTES"):
        monkeypatch.delenv(name, raising=False)
    settings = Settings()
    params = _load_template()["parameters"]
    assert params["maxDiskStagingGiB"]["defaultValue"] * 1024 ** 3 == settings.max_disk_staging_bytes
    assert params["maxMemoryMiB"]["defaultValue"] * 1024 ** 2 == settings.max_memory_bytes
