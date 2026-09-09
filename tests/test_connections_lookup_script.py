"""The PowerShell script that looks up connection names for the operator, by id.

Fab Shuffle's own credential frequently cannot read these connections at all - that is often
exactly why the advisory scan only has an id to show. This script is meant to be run signed
in as a person instead: it only ever reads, and it never prints, logs, or grants anything.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from fabshuffle.orchestrator import connections_lookup_script

TENANT_ID = "72f988bf-86f1-41af-91ab-2d7cd011db47"
DOMAIN_TENANT = "contoso.onmicrosoft.com"
CONNECTION_A = "081da81e-f477-4715-8b66-c2a1debf8909"
CONNECTION_B = "aa89a365-f638-49a8-81d7-ded77940ce84"


def test_every_valid_connection_id_is_included():
    script = connections_lookup_script([CONNECTION_A, CONNECTION_B], tenant_id=TENANT_ID)
    assert f"'{CONNECTION_A}'" in script
    assert f"'{CONNECTION_B}'" in script


def test_ids_are_normalised_and_deduplicated():
    script = connections_lookup_script(
        [CONNECTION_A.upper(), CONNECTION_A, f" {CONNECTION_A} "], tenant_id=TENANT_ID,
    )
    assert script.count(f"'{CONNECTION_A}'") == 1


def test_a_value_that_is_not_a_guid_is_reported_not_embedded():
    hostile = "'; Remove-Item C:\\ -Recurse; #"
    with pytest.raises(ValueError, match="not a GUID"):
        connections_lookup_script([CONNECTION_A, hostile], tenant_id=TENANT_ID)


def test_an_empty_list_still_produces_a_runnable_script():
    script = connections_lookup_script([], tenant_id=TENANT_ID)
    assert "$connectionIds = @(" in script
    assert "foreach ($id in $connectionIds)" in script


def test_a_guid_tenant_is_embedded_as_is():
    script = connections_lookup_script([CONNECTION_A], tenant_id=TENANT_ID.upper())
    assert f"$tenantId = '{TENANT_ID}'" in script


def test_a_domain_style_tenant_is_accepted():
    script = connections_lookup_script([CONNECTION_A], tenant_id=DOMAIN_TENANT)
    assert f"$tenantId = '{DOMAIN_TENANT}'" in script


def test_an_unsafe_tenant_value_is_refused_outright():
    for hostile in ["'; whoami; #", "tenant'; Remove-Item C:\\", "", "   "]:
        with pytest.raises(ValueError):
            connections_lookup_script([CONNECTION_A], tenant_id=hostile)


def test_the_sign_in_is_scoped_to_the_tenant_and_never_a_service_principal():
    script = connections_lookup_script([CONNECTION_A], tenant_id=TENANT_ID)
    assert "Connect-AzAccount -TenantId $tenantId -SkipContextPopulation | Out-Null" in script
    assert "Connect-AzAccount -ServicePrincipal" not in script
    assert "$context.Account.Type -ne 'User'" in script
    assert "(Get-AzContext).Account.Type -ne 'User'" in script


def test_sign_in_skips_subscription_context_population():
    """A Fabric-only lookup needs no Azure Resource Manager subscription, and an account with
    zero (or many) would otherwise pay for listing them on every sign-in for nothing."""
    script = connections_lookup_script([CONNECTION_A], tenant_id=TENANT_ID)
    assert "-SkipContextPopulation" in script


def test_looking_up_a_name_does_not_promise_granting_access():
    script = connections_lookup_script([CONNECTION_A], tenant_id=TENANT_ID)
    assert "access to it" in script
    assert "does not create, change, or grant" in script
    assert "-AuthScope" not in script  # -Authentication Bearer replaces the header dance instead


def test_requires_powershell_seven_and_az_accounts_only():
    script = connections_lookup_script([CONNECTION_A], tenant_id=TENANT_ID)
    assert "#Requires -Version 7.0" in script
    assert "#Requires -Modules Az.Accounts" in script
    assert "Az.Resources" not in script


def test_it_reads_with_authentication_bearer_and_a_secure_token():
    script = connections_lookup_script([CONNECTION_A], tenant_id=TENANT_ID)
    assert "-Authentication Bearer -Token $secureToken" in script
    assert "-Method Get" in script
    assert "/v1/connections/$id" in script


def test_it_never_creates_updates_or_grants_anything():
    script = connections_lookup_script([CONNECTION_A], tenant_id=TENANT_ID)
    assert "-Method Post" not in script
    assert "-Method POST" not in script
    assert "roleAssignments" not in script
    assert "-Method Put" not in script
    assert "-Method Delete" not in script


def test_no_token_or_secret_is_ever_printed():
    """The token stays a SecureString end to end; nothing unwraps or echoes it."""
    script = connections_lookup_script([CONNECTION_A], tenant_id=TENANT_ID)
    assert "NetworkCredential" not in script
    assert "$secureToken" in script
    for line in script.splitlines():
        if "Write-Host" in line or "Write-Warning" in line or "Write-Output" in line:
            assert "$secureToken" not in line
            assert "$token" not in line.replace("$tenantId", "")


def test_a_secure_string_token_is_produced_on_both_az_versions():
    script = connections_lookup_script([CONNECTION_A], tenant_id=TENANT_ID)
    assert "Parameters.ContainsKey('AsSecureString')" in script
    assert "-AsSecureString" in script
    assert "ConvertTo-SecureString -String $value -AsPlainText -Force" in script


def test_one_failed_lookup_does_not_stop_the_rest():
    script = connections_lookup_script([CONNECTION_A, CONNECTION_B], tenant_id=TENANT_ID)
    assert "try {" in script
    assert "catch {" in script
    assert "Write-Warning" in script


def test_per_id_errors_preserve_the_services_own_code_and_message():
    script = connections_lookup_script([CONNECTION_A], tenant_id=TENANT_ID)
    assert "$parsed.errorCode" in script
    assert "$parsed.message" in script
    assert "ErrorDetails.Message" in script


def test_names_are_not_claimed_to_always_be_returned():
    script = connections_lookup_script([CONNECTION_A], tenant_id=TENANT_ID)
    assert "(not returned)" in script
    assert "(unavailable)" in script


# --------------------------------------------------- operator-supplied ids and list fallback


def test_operator_can_supply_extra_ids_at_run_time():
    script = connections_lookup_script([CONNECTION_A], tenant_id=TENANT_ID)
    assert "param(" in script
    assert "[string[]] $ConnectionId = @()" in script
    assert "$recordedConnectionIds + $suppliedConnectionIds" in script


def test_only_well_formed_guids_are_trusted_from_the_operator_parameter():
    """The Python-side validation only ever sees the ids this run recorded; whatever an
    operator types at the prompt is untrusted input the script itself must filter."""
    script = connections_lookup_script([CONNECTION_A], tenant_id=TENANT_ID)
    assert "[ValidatePattern(" in script
    assert "$suppliedConnectionIds = @($ConnectionId)" in script


def test_no_known_ids_falls_back_to_listing_every_visible_connection():
    """An older report, from before this scan existed, has nothing recorded to embed - the
    script must not just loop over an empty array and print nothing."""
    script = connections_lookup_script([], tenant_id=TENANT_ID)
    assert "if ($connectionIds.Count -gt 0) {" in script
    assert "} else {" in script
    assert '"$fabric/v1/connections"' in script


def test_the_fallback_listing_is_paged():
    script = connections_lookup_script([], tenant_id=TENANT_ID)
    assert "continuationToken" in script
    assert "[System.Uri]::EscapeDataString($continuationToken)" in script


def test_the_fallback_listing_never_follows_a_server_returned_continuation_uri():
    """Fabric's own pagination convention offers a ready-to-use ``continuationUri`` as well
    as a ``continuationToken``, but following an absolute URI the response supplies would
    hand this account's bearer token to wherever that URI points. Every subsequent page is
    always requested against the fixed Fabric host with only the encoded token appended."""
    script = connections_lookup_script([], tenant_id=TENANT_ID)
    # The word may appear in an explanatory comment, but the script must never read the
    # property itself, nor ever assign $uri from anything but the fixed host.
    assert "$page.continuationUri" not in script
    assert ".continuationUri" not in script
    assert '"$fabric/v1/connections?continuationToken="' in script


def test_the_fallback_listing_also_shows_a_visible_error_not_a_silent_empty_table():
    script = connections_lookup_script([], tenant_id=TENANT_ID)
    assert "Listing connections" in script


# --------------------------------------------------------------- real PowerShell, in Docker
#
# A generated string that merely *looks* like PowerShell is not proof it runs correctly, and
# the container this suite otherwise runs in has no PowerShell at all. Both tests below shell
# out to Docker instead of asking for a host-installed ``pwsh``/``powershell``, so the check
# works the same way in the sandboxed validation container as anywhere else Docker is
# available, and is skipped (not faked, and never run against a host ``pwsh``) where it is
# not. Pinned by digest to the exact image already verified locally, not a floating tag.
POWERSHELL_IMAGE = (
    "mcr.microsoft.com/powershell:7.4-ubuntu-22.04"
    "@sha256:62300a213a9293916333df2b014cd3a8f22fb0b0b65f2bb446aaf436bcf8c868"
)
DOCKER = shutil.which("docker")
PWSH = shutil.which("pwsh") if Path("/.dockerenv").is_file() else None

# A connection id the mocked ``Invoke-RestMethod`` below recognises as "found"; anything else
# (including CONNECTION_A/CONNECTION_B, used elsewhere in this file) is treated as a lookup
# failure, so the exec test exercises both branches of the script's own per-id try/catch with
# one shared mock. Deliberately distinct from CONNECTION_A/CONNECTION_B: the Python-side id
# list is deduplicated, so reusing one of those here would silently collapse the two-id exec
# test down to testing only the successful branch.
GOOD_ID = "33333333-4444-5555-6666-777788889999"

# Stands in for Az.Accounts and the Fabric REST API so the exec test never makes a network
# call. Deliberately returns *more* than the script should ever surface - a full path with a
# SAS-looking query string and a credential block - so the assertions can prove those never
# reach the printed output, not just that the script's own source never mentions them. Also
# answers the "list every connection" fallback with two pages, joined by continuationToken.
MOCK_CMDLETS = rf"""
$global:UserSignedIn = $false
function Connect-AzAccount {{
    param($TenantId, [switch]$SkipContextPopulation)
    $global:UserSignedIn = $true
}}
function Get-AzContext {{
    [PSCustomObject]@{{
        Tenant = [PSCustomObject]@{{ Id = '{TENANT_ID}' }}
        Account = [PSCustomObject]@{{
            Type = if ($global:UserSignedIn) {{ 'User' }} else {{ 'ServicePrincipal' }}
        }}
    }}
}}
function Get-AzAccessToken {{
    param($ResourceUrl, $TenantId, [switch]$AsSecureString)
    if ($AsSecureString) {{
        [PSCustomObject]@{{ Token = (ConvertTo-SecureString -String 'fake-token' -AsPlainText -Force) }}
    }} else {{
        [PSCustomObject]@{{ Token = 'fake-token' }}
    }}
}}
function Invoke-RestMethod {{
    [CmdletBinding()]
    param($Method, $Authentication, $Token, $Uri)
    if (-not $global:UserSignedIn) {{ throw 'A cached service principal must not be used' }}

    # Every legitimate request in this script always targets the fixed Fabric host. A
    # request anywhere else can only mean the script followed a server-returned
    # continuationUri instead of rebuilding the next page's URI itself - exactly the
    # bearer-token-leak this mock exists to catch.
    if ($Uri -notmatch '^https://api\.fabric\.microsoft\.com/') {{
        throw "SECURITY VIOLATION: request left the Fabric host: $Uri"
    }}

    if ($Uri -match 'connections/(?<id>[0-9a-fA-F-]{{36}})$') {{
        if ($Matches['id'] -eq '{GOOD_ID}') {{
            return [PSCustomObject]@{{
                id = $Matches['id']
                displayName = 'Bronze SQL'
                connectivityType = 'ShareableCloud'
                connectionDetails = [PSCustomObject]@{{
                    type = 'SQL'
                    path = 'secret-server.example.com;catalog?sig=SHOULD-NOT-LEAK'
                }}
                credentialDetails = [PSCustomObject]@{{
                    credentialType = 'Basic'; username = 'shouldnotleak'
                }}
            }}
        }}
        $body = '{{"errorCode":"EntityNotFound","message":"Connection could not be found."}}'
        $errorRecord = [System.Management.Automation.ErrorRecord]::new(
            [System.Exception]::new('Not Found'), 'NotFound', 'NotSpecified', $null
        )
        $errorRecord.ErrorDetails = [System.Management.Automation.ErrorDetails]::new($body)
        $PSCmdlet.ThrowTerminatingError($errorRecord)
    }}

    if ($Uri -eq 'https://api.fabric.microsoft.com/v1/connections') {{
        return [PSCustomObject]@{{
            value = @([PSCustomObject]@{{
                id = 'cccccccc-1111-2222-3333-444444444444'
                displayName = 'Page One Connection'
                connectivityType = 'ShareableCloud'
                connectionDetails = [PSCustomObject]@{{
                    type = 'Web'; path = 'https://page-one.example.com/leak'
                }}
            }})
            # Deliberately paired with a malicious, off-host continuationUri: a real
            # (or compromised) response can return both fields, and the script must
            # never even glance at this one.
            continuationUri = 'https://evil.example.com/steal-token?sig=malicious'
            continuationToken = 'PAGE2'
        }}
    }}
    if ($Uri -match 'continuationToken=PAGE2$') {{
        return [PSCustomObject]@{{
            value = @([PSCustomObject]@{{
                id = 'dddddddd-1111-2222-3333-444444444444'
                displayName = 'Page Two Connection'
                connectivityType = 'PersonalCloud'
                connectionDetails = [PSCustomObject]@{{
                    type = 'SQL'; path = 'https://page-two.example.com/leak'
                }}
            }})
        }}
    }}
    throw "Unexpected mocked Uri: $Uri"
}}
"""


# ``#Requires -Modules Az.Accounts`` is a real module check, satisfied here by an empty stub
# manifest rather than by installing the actual module - the exec test only needs the check to
# pass, not the real Az.Accounts implementation, since every cmdlet it would provide is mocked.
STUB_AZ_ACCOUNTS_MANIFEST = """@{
    ModuleVersion = '1.0.0'
    GUID = '11111111-1111-1111-1111-111111111111'
    Author = 'test-stub'
}
"""


def _run_pwsh_in_docker(directory: Path, *args: str):
    if PWSH:
        return subprocess.run(
            [PWSH, "-NoProfile", "-NonInteractive", *[
                arg.replace("/scripts", directory.as_posix()) for arg in args
            ]],
            capture_output=True, text=True, timeout=120,
        )
    return subprocess.run(
        [
            DOCKER, "run", "--rm", "-v", f"{directory}:/scripts:ro",
            POWERSHELL_IMAGE, "pwsh", "-NoProfile", "-NonInteractive", *args,
        ],
        capture_output=True, text=True, timeout=120,
    )


def _prepare_exec_fixture(directory: Path, script: str, invocation: str) -> None:
    """Write the mocked cmdlets, the generated script, a stub Az.Accounts manifest so its
    ``#Requires`` check passes, and a ``run.ps1`` that loads the mocks before ``invocation``
    runs the generated script - dot-sourced, or called with extra parameters."""
    (directory / "mocks.ps1").write_text(MOCK_CMDLETS, encoding="utf-8")
    (directory / "lookup.ps1").write_text(script, encoding="utf-8")
    module_dir = directory / "Modules" / "Az.Accounts" / "1.0.0"
    module_dir.mkdir(parents=True)
    (module_dir / "Az.Accounts.psd1").write_text(STUB_AZ_ACCOUNTS_MANIFEST, encoding="utf-8")
    runner = (
        "$WarningPreference = 'Continue'\n"
        '$env:PSModulePath = "/scripts/Modules:$env:PSModulePath"\n'
        f". /scripts/mocks.ps1\n{invocation}\n"
    )
    if PWSH:
        runner = runner.replace("/scripts", directory.as_posix())
    (directory / "run.ps1").write_text(runner, encoding="utf-8")


@pytest.mark.skipif(DOCKER is None and PWSH is None, reason="PowerShell execution requires Docker")
def test_the_generated_script_is_valid_powershell():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "lookup.ps1"
        path.write_text(
            connections_lookup_script([CONNECTION_A, CONNECTION_B], tenant_id=TENANT_ID), encoding="utf-8",
        )
        result = _run_pwsh_in_docker(
            Path(directory), "-Command",
            "$e=$null; $t=$null; "
            "[System.Management.Automation.Language.Parser]::ParseFile('/scripts/lookup.ps1', "
            "[ref]$t, [ref]$e) | Out-Null; "
            "if ($e) { $e | ForEach-Object { $_.Message }; exit 1 } else { exit 0 }",
        )

    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.skipif(DOCKER is None and PWSH is None, reason="PowerShell execution requires Docker")
def test_running_the_script_returns_only_whitelisted_fields_and_a_visible_get_error():
    """Executes the actual generated script under a real PowerShell, with ``Connect-AzAccount``,
    ``Get-AzAccessToken`` and ``Invoke-RestMethod`` replaced by mocks - never a live call. Proves,
    by running it rather than by reading its source, that a successful lookup's output holds only
    id/name/connectivity/type, and that a failed one shows the service's own error rather than a
    blank name.
    """
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)
        _prepare_exec_fixture(
            path, connections_lookup_script([GOOD_ID, CONNECTION_A], tenant_id=TENANT_ID),
            ". /scripts/lookup.ps1",
        )
        result = _run_pwsh_in_docker(path, "-File", "/scripts/run.ps1")

    output = result.stdout + result.stderr
    assert result.returncode == 0, output

    # The successful lookup's whitelisted fields reach the output...
    assert "Bronze SQL" in output
    assert "ShareableCloud" in output
    assert GOOD_ID in output
    # ...but connectionDetails.path and credentialDetails never do, even though the mocked
    # response included them.
    assert "secret-server.example.com" not in output
    assert "SHOULD-NOT-LEAK" not in output
    assert "shouldnotleak" not in output
    assert "Basic" not in output
    # The failed lookup is visible with the service's own code and message, not a blank name.
    assert "EntityNotFound" in output
    assert "Connection could not be found" in output
    assert "(unavailable)" in output


@pytest.mark.skipif(DOCKER is None and PWSH is None, reason="PowerShell execution requires Docker")
def test_running_the_script_with_no_known_ids_lists_every_visible_connection_paged():
    """A report from before this scan existed has nothing recorded; run rather than read the
    script to prove it falls back to a paged list of every connection, still only ever prints
    the whitelisted fields, and never follows the malicious off-host ``continuationUri`` the
    mocked first page pairs with a legitimate ``continuationToken`` - if it ever did, the mock
    itself would fail the request before the script could even try to print a result."""
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)
        _prepare_exec_fixture(
            path, connections_lookup_script([], tenant_id=TENANT_ID), ". /scripts/lookup.ps1",
        )
        result = _run_pwsh_in_docker(path, "-File", "/scripts/run.ps1")

    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert "SECURITY VIOLATION" not in output
    assert "evil.example.com" not in output
    assert "listing every" in output.lower()
    # Both pages, joined by the mocked continuationToken, reached the output...
    assert "Page One Connection" in output
    assert "Page Two Connection" in output
    assert "cccccccc-1111-2222-3333-444444444444" in output
    assert "dddddddd-1111-2222-3333-444444444444" in output
    # ...but neither page's full path did.
    assert "page-one.example.com" not in output
    assert "page-two.example.com" not in output


@pytest.mark.skipif(DOCKER is None and PWSH is None, reason="PowerShell execution requires Docker")
def test_running_the_script_with_a_supplied_connection_id_looks_it_up():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)
        _prepare_exec_fixture(
            path, connections_lookup_script([], tenant_id=TENANT_ID),
            f"& /scripts/lookup.ps1 -ConnectionId '{GOOD_ID}'",
        )
        result = _run_pwsh_in_docker(path, "-File", "/scripts/run.ps1")

    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert "Bronze SQL" in output
    assert GOOD_ID in output
    # The fallback listing did not also run: a usable id was supplied.
    assert "Page One Connection" not in output
    assert "not-a-guid" not in output


@pytest.mark.skipif(DOCKER is None and PWSH is None, reason="PowerShell execution requires Docker")
def test_invalid_operator_ids_fail_before_sign_in_instead_of_listing_every_connection():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)
        _prepare_exec_fixture(
            path, connections_lookup_script([], tenant_id=TENANT_ID),
            "$ErrorActionPreference = 'Stop'\n& /scripts/lookup.ps1 -ConnectionId 'not-a-guid'",
        )
        result = _run_pwsh_in_docker(path, "-File", "/scripts/run.ps1")
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "not-a-guid" in output
    assert "Page One Connection" not in output
