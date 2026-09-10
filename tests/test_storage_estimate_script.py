"""Execute the user sizing script with mocked Az cmdlets, exclusively in Docker."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "Get-FabShuffleStorageEstimate.ps1"
PWSH = shutil.which("pwsh") if Path("/.dockerenv").is_file() else None
pytestmark = pytest.mark.skipif(PWSH is None, reason="Use the Dockerfile test target with PowerShell")

MOCKS = r"""
$global:UserSignedIn = $false
function Get-AzContext {
    [pscustomobject]@{
        Tenant = [pscustomobject]@{ Id = '11111111-1111-1111-1111-111111111111' }
        Account = [pscustomobject]@{ Type = if ($global:UserSignedIn) { 'User' } else { 'ServicePrincipal' } }
    }
}
function Connect-AzAccount {
    param($TenantId, [switch]$SkipContextPopulation)
    $global:UserSignedIn = $true
}
function Get-AzAccessToken {
    param($ResourceUrl, $TenantId, [switch]$AsSecureString)
    if (-not $global:UserSignedIn) { throw 'User sign-in was skipped' }
    if ($ResourceUrl -ne 'https://api.fabric.microsoft.com') { throw 'Wrong token audience' }
    [pscustomobject]@{ Token = (ConvertTo-SecureString 'TOKEN-MUST-NOT-PRINT' -AsPlainText -Force) }
}
function New-AzStorageContext {
    param($StorageAccountName, [switch]$UseConnectedAccount, $Endpoint)
    if ($StorageAccountName -ne 'onelake' -or $Endpoint -ne 'fabric.microsoft.com' `
        -or -not $UseConnectedAccount) {
        throw 'Incorrect OneLake context'
    }
    [pscustomobject]@{ Secret = 'CONTEXT-MUST-NOT-PRINT' }
}
function Invoke-RestMethod {
    [CmdletBinding()]
    param($Method, $Uri, $Authentication, $Token, $MaximumRedirection)
    if ($Method -ne 'Get' -or $MaximumRedirection -ne 0) { throw 'Only fixed-host GET is allowed' }
    if ($Token -isnot [System.Security.SecureString]) { throw 'Token is not secure' }
    $base = 'https://api.fabric.microsoft.com/v1/workspaces/22222222-2222-2222-2222-222222222222/items'
    if ($Scenario -eq 'inventory-fails') {
        $record = [System.Management.Automation.ErrorRecord]::new(
            [Exception]::new('Forbidden'), 'Forbidden', 'NotSpecified', $null)
        $record.ErrorDetails = [System.Management.Automation.ErrorDetails]::new(
            '{"errorCode":"WorkspaceDenied","message":"Inventory denied","payload":"DO-NOT-PRINT"}')
        $PSCmdlet.ThrowTerminatingError($record)
    }
    if ($Uri -eq $base) {
        return [pscustomobject]@{
            value = @([pscustomobject]@{
                id = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'; type = 'Lakehouse'; displayName = 'Sixty GiB'
            })
            continuationToken = 'page+2/'
            continuationUri = 'https://not-fabric.invalid/steal'
        }
    }
    if ($Uri -ne "$base`?continuationToken=page%2B2%2F") { throw "Unexpected URI: $Uri" }
    $result = @(
        [pscustomobject]@{
            id = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'; type = 'Lakehouse'; displayName = 'Eighty GiB'
        },
        [pscustomobject]@{
            id = 'AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA'; type = 'Lakehouse'; displayName = 'duplicate'
        }
    )
    if ($Scenario -eq 'sql') {
        $result += [pscustomobject]@{
            id = 'cccccccc-cccc-cccc-cccc-cccccccccccc'; type = 'SQLDatabase'; displayName = 'Source SQL'
        }
    }
    [pscustomobject]@{ value = $result }
}
function Get-AzDataLakeGen2ChildItem {
    [CmdletBinding()]
    param($Context, $FileSystem, $Path, [switch]$Recurse, [switch]$FetchProperty,
          $MaxCount, $ContinuationToken)
    if (-not $Recurse -or -not $FetchProperty -or $MaxCount -ne 1000) { throw 'Unbounded/missing properties' }
    if ($FileSystem -ne '22222222-2222-2222-2222-222222222222') { throw 'Wrong workspace' }
    if ($Scenario -eq 'no-file-calls') { throw 'Paired/SkipFiles should not enumerate Files' }
    if ($Scenario -eq 'files-fail') {
        $record = [System.Management.Automation.ErrorRecord]::new(
            [Exception]::new('Forbidden'), 'Forbidden', 'NotSpecified', $null)
        $record.ErrorDetails = [System.Management.Automation.ErrorDetails]::new(
            '{"error":{"code":"AuthorizationFailure","message":"Cannot read files"}}')
        $PSCmdlet.ThrowTerminatingError($record)
    }
    if ($Scenario -eq 'missing-length') {
        return [pscustomobject]@{ IsDirectory = $false; ContinuationToken = $null }
    }
    if ($Scenario -eq 'empty') {
        return [pscustomobject]@{ IsDirectory = $true; Length = $null; ContinuationToken = $null }
    }
    if ($Scenario -eq 'huge-file') {
        return [pscustomobject]@{ IsDirectory = $false; Length = 5TB; ContinuationToken = $null }
    }
    if ($Scenario -eq 'huge-share') {
        return @(1..40 | ForEach-Object {
            [pscustomobject]@{ IsDirectory = $false; Length = 3TB; ContinuationToken = $null }
        })
    }
    if ($Path -eq 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb/Files') {
        return [pscustomobject]@{ IsDirectory = $false; Length = 80GB; ContinuationToken = $null }
    }
    if ($Path -ne 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/Files') { throw 'Unexpected path' }
    if (-not $ContinuationToken) {
        return @(
            [pscustomobject]@{ IsDirectory = $true; Length = $null },
            [pscustomobject]@{ IsDirectory = $false; Length = 20GB; ContinuationToken = 'next' }
        )
    }
    if ($ContinuationToken -ne 'next') { throw 'Unexpected storage continuation token' }
    [pscustomobject]@{
        IsDirectory = $false; Length = 40GB
        ContinuationToken = if ($Scenario -eq 'repeated-token') { 'next' } else { $null }
    }
}
"""


def run_script(tmp_path, scenario="normal", arguments=""):
    for module in ("Az.Accounts", "Az.Storage"):
        directory = tmp_path / "Modules" / module / "1.0.0"
        directory.mkdir(parents=True)
        (directory / f"{module}.psd1").write_text("@{ ModuleVersion = '1.0.0' }", encoding="utf-8")
    fixture = tmp_path / "fixture.ps1"
    fixture.write_text(
        "$ErrorActionPreference = 'Stop'\n"
        f'$env:PSModulePath = "{tmp_path.as_posix()}/Modules:$env:PSModulePath"\n'
        f"$Scenario = '{scenario}'\n{MOCKS}\n"
        f"& '{SCRIPT.as_posix()}' -TenantId '11111111-1111-1111-1111-111111111111' "
        "-WorkspaceId '22222222-2222-2222-2222-222222222222' -AsJson "
        f"{arguments}\n",
        encoding="utf-8",
    )
    return subprocess.run(
        [PWSH, "-NoLogo", "-NoProfile", "-NonInteractive", "-File", str(fixture)],
        capture_output=True, text=True, timeout=30,
    )


def report(tmp_path, scenario="normal", arguments=""):
    result = run_script(tmp_path, scenario, arguments)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "TOKEN-MUST-NOT-PRINT" not in result.stdout + result.stderr
    assert "CONTEXT-MUST-NOT-PRINT" not in result.stdout + result.stderr
    return json.loads(result.stdout)


def test_sums_concurrent_files_but_does_not_double_count_sequential_schema_phase(tmp_path):
    result = report(tmp_path)
    assert result["PeakFilesGiB"] == 140
    assert result["SchemaAllowanceGiB"] == 20
    assert result["SuggestedShareQuotaGiB"] == 175
    assert len(result["Lakehouses"]) == 2
    assert result["Problems"] == []


def test_concurrency_and_existing_files_are_part_of_the_estimate(tmp_path):
    result = report(tmp_path, arguments="-FileConcurrency 1 -ExistingStagingGiB 20 -HeadroomPercent 50")
    assert result["PeakFilesGiB"] == 80
    assert result["SuggestedShareQuotaGiB"] == 150


@pytest.mark.parametrize("arguments", ["-Mode Paired", "-SkipFiles"])
def test_streamed_or_skipped_files_do_not_need_full_datastore_staging(tmp_path, arguments):
    result = report(tmp_path, "no-file-calls", arguments)
    assert result["PeakFilesGiB"] == 0
    assert result["SuggestedShareQuotaGiB"] == 25
    assert result["Lakehouses"] == []


def test_unknown_sql_export_refuses_to_present_an_incomplete_recommendation(tmp_path):
    result = report(tmp_path, "sql")
    assert result["SuggestedShareQuotaGiB"] is None
    assert "LargestBcpTableGiB" in result["Problems"][0]
    assert "Source SQL" in result["Problems"][0]


def test_explicit_bcp_allowance_uses_largest_table_not_whole_database(tmp_path):
    result = report(tmp_path, "sql", "-LargestBcpTableGiB 250")
    assert result["LargestBcpExportGiB"] == 250
    assert result["SuggestedShareQuotaGiB"] == 313


def test_skipped_sql_data_does_not_require_a_bcp_estimate(tmp_path):
    result = report(tmp_path, "sql", "-SkipData")
    assert result["Problems"] == []
    assert result["SuggestedShareQuotaGiB"] == 175


@pytest.mark.parametrize("scenario,detail", [
    ("files-fail", "AuthorizationFailure Cannot read files"),
    ("missing-length", "file length was not returned"),
    ("repeated-token", "repeated a continuation token"),
])
def test_unknown_files_are_not_treated_as_zero_bytes(tmp_path, scenario, detail):
    result = report(tmp_path, scenario)
    assert result["SuggestedShareQuotaGiB"] is None
    assert any(detail in issue for issue in result["Problems"])


def test_inspection_limit_is_not_a_successful_partial_scan(tmp_path):
    result = report(tmp_path, arguments="-MaxPathsPerLakehouse 1")
    assert result["SuggestedShareQuotaGiB"] is None
    assert any("Path inspection limit" in issue for issue in result["Problems"])


@pytest.mark.parametrize("scenario,detail", [
    ("huge-file", "4 TiB per-file limit"),
    ("huge-share", "share maximum of 102400"),
])
def test_impossible_share_sizes_are_not_silently_clamped(tmp_path, scenario, detail):
    result = report(tmp_path, scenario)
    assert result["SuggestedShareQuotaGiB"] is None
    assert any(detail in issue for issue in result["Problems"])


def test_fabric_inventory_error_preserves_service_error_without_echoing_payload(tmp_path):
    result = run_script(tmp_path, "inventory-fails")
    assert result.returncode != 0
    assert "WorkspaceDenied Inventory denied" in result.stderr
    assert "DO-NOT-PRINT" not in result.stderr


def test_empty_directories_are_measured_as_zero_not_missing_length(tmp_path):
    result = report(tmp_path, "empty")
    assert result["SuggestedShareQuotaGiB"] == 25
    assert result["PeakFilesGiB"] == 0
    assert all(item["Measured"] and item["Files"] == 0 for item in result["Lakehouses"])


@pytest.mark.parametrize("arguments", [
    "-FileConcurrency 0", "-DiskStagingGiB 0", "-ExistingStagingGiB -1", "-HeadroomPercent -1",
])
def test_invalid_estimator_options_fail_instead_of_making_a_small_quota(tmp_path, arguments):
    result = run_script(tmp_path, arguments=arguments)
    assert result.returncode != 0
    assert "SuggestedShareQuotaGiB" not in result.stdout
