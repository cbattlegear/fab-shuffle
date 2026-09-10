#Requires -Version 7.0
#Requires -Modules Az.Accounts, Az.Storage
<#
.SYNOPSIS
Estimate the Azure Files share quota for one Fab Shuffle migration.
.DESCRIPTION
Read-only: lists visible workspace items and measures lakehouse Files/ metadata in
SinglePrincipal mode. No file contents, SQL rows, credentials or resources are copied.
Paired mode streams data; its estimate reserves bounded schema/checkpoint staging.
Use an account that can read every selected source item and file. Estimates are not
filesystem guarantees. Source growth, failed-run leftovers and unmeasured SQL exports
can require more space. A partial scan never produces a suggested quota.
.PARAMETER LargestBcpTableGiB
For SinglePrincipal SQLDatabase data copies, supply a measured or conservatively
estimated largest native bcp table-export size. Fabric item/OneLake metadata does not
give this size. Database storage size can understate an uncompressed export.
.PARAMETER DiskStagingGiB
Match FAB_SHUFFLE_MAX_DISK_STAGING_BYTES, in GiB (default 10). This reserves active
schema/checkpoint staging, not a cap on legacy AzCopy/bcp exports.
.PARAMETER ExistingStagingGiB
Space already used by journals or retained files on the share. Default assumes a new share.
.EXAMPLE
.\Get-FabShuffleStorageEstimate.ps1 -TenantId <tenant-guid> -WorkspaceId <workspace-guid>
.EXAMPLE
.\Get-FabShuffleStorageEstimate.ps1 -TenantId <tenant-guid> -WorkspaceId <workspace-guid> -Mode Paired
.LINK
https://learn.microsoft.com/fabric/onelake/how-to-get-item-size
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [guid] $TenantId,
    [Parameter(Mandatory)] [guid] $WorkspaceId,
    [ValidateSet('SinglePrincipal', 'Paired')] [string] $Mode = 'SinglePrincipal',
    [ValidateRange(1, 1000)] [int] $FileConcurrency = 2,
    [ValidateRange(1, 1000)] [int] $SchemaConcurrency = 2,
    [ValidateRange(1, 102400)] [decimal] $DiskStagingGiB = 10,
    [ValidateRange(0, 1000000000)] [Nullable[decimal]] $LargestBcpTableGiB = $null,
    [ValidateRange(0, 1000000000)] [decimal] $ExistingStagingGiB = 0,
    [ValidateRange(0, 200)] [int] $HeadroomPercent = 25,
    [ValidateRange(1, 100000000)] [int] $MaxPathsPerLakehouse = 1000000,
    [switch] $SkipFiles,
    [switch] $SkipData,
    [switch] $AsJson
)

$ErrorActionPreference = 'Stop'
if ($AsJson) { $ProgressPreference = 'SilentlyContinue' }
if ($TenantId -eq [guid]::Empty -or $WorkspaceId -eq [guid]::Empty) {
    throw 'TenantId and WorkspaceId must be nonempty GUIDs.'
}

function Format-ServiceError($Failure) {
    if ($Failure.ErrorDetails.Message) {
        try {
            $body = $Failure.ErrorDetails.Message | ConvertFrom-Json -AsHashtable
            if ($body.error) { $body = $body.error }
            $code = if ($body.errorCode) { $body.errorCode } else { $body.code }
            if ($code -or $body.message) { return "$code $($body.message)".Trim() }
        } catch {
            # An unparseable error body is not a successful response; retain the exception text.
        }
    }
    return $Failure.Exception.Message
}

$context = Get-AzContext
if (-not $context -or $context.Account.Type -ne 'User' -or $context.Tenant.Id -ne $TenantId.ToString()) {
    Connect-AzAccount -TenantId $TenantId.ToString() -SkipContextPopulation | Out-Null
}
if ((Get-AzContext).Account.Type -ne 'User') {
    throw 'Sign in with your user account, not a service principal or managed identity.'
}
$fabricToken = (Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com' `
    -TenantId $TenantId.ToString() -AsSecureString).Token

$items = [System.Collections.Generic.List[object]]::new()
$seenItems = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
$seenTokens = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
$continuation = ''
do {
    $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items"
    if ($continuation) { $uri += '?continuationToken=' + [uri]::EscapeDataString($continuation) }
    try {
        $page = Invoke-RestMethod -Method Get -Uri $uri -Authentication Bearer `
            -Token $fabricToken -MaximumRedirection 0
    } catch {
        throw "Workspace inventory could not be read: $(Format-ServiceError $_)"
    }
    if ($null -eq $page.value) { throw 'Workspace inventory returned no item list; no estimate is available.' }
    foreach ($item in $page.value) {
        $id = [guid]::Empty
        if (-not [guid]::TryParse([string]$item.id, [ref]$id) -or $id -eq [guid]::Empty -or -not $item.type) {
            throw 'Workspace inventory contains an invalid item identity or type; no estimate is available.'
        }
        if ($seenItems.Add($id.ToString())) { $items.Add($item) }
    }
    $continuation = [string]$page.continuationToken
    if ($continuation -and -not $seenTokens.Add($continuation)) {
        throw 'Fabric repeated a continuation token; workspace inventory is incomplete.'
    }
} while ($continuation)

$lakehouses = @($items | Where-Object type -EQ 'Lakehouse')
$schemaStores = @($items | Where-Object { $_.type -in @('Lakehouse', 'Warehouse', 'SQLDatabase') })
$sqlDatabases = @($items | Where-Object type -EQ 'SQLDatabase')
$measurements = [System.Collections.Generic.List[object]]::new()
$problems = [System.Collections.Generic.List[string]]::new()
[decimal] $largestFile = 0

if ($Mode -eq 'SinglePrincipal' -and -not $SkipFiles -and $lakehouses.Count) {
    $storage = New-AzStorageContext -StorageAccountName 'onelake' -UseConnectedAccount -Endpoint 'fabric.microsoft.com'
    foreach ($lakehouse in $lakehouses) {
        $name = if ($lakehouse.displayName) { $lakehouse.displayName } else { $lakehouse.id }
        Write-Progress -Activity 'Measure OneLake Files metadata' -Status $name
        [decimal] $bytes = 0
        $count = 0
        $paths = 0
        $token = $null
        $tokens = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        $problem = $null
        try {
            do {
                $entries = @(Get-AzDataLakeGen2ChildItem -Context $storage -FileSystem $WorkspaceId.ToString() `
                    -Path "$($lakehouse.id)/Files" -Recurse -FetchProperty -MaxCount 1000 -ContinuationToken $token)
                if (-not $entries.Count) { break }
                foreach ($entry in $entries) {
                    $paths++
                    if ($paths -gt $MaxPathsPerLakehouse) {
                        throw "Path inspection limit reached. Increase -MaxPathsPerLakehouse and rerun."
                    }
                    if ($null -eq $entry.PSObject.Properties['IsDirectory']) {
                        throw 'Directory metadata was not returned; the total cannot be trusted.'
                    }
                    if ($entry.IsDirectory -eq $true -or $entry.IsDirectory -eq 'true') { continue }
                    [decimal] $length = 0
                    $property = $entry.PSObject.Properties['Length']
                    if ($null -eq $property -or -not [decimal]::TryParse(
                        [string]$property.Value, [Globalization.NumberStyles]::Integer,
                        [Globalization.CultureInfo]::InvariantCulture, [ref]$length
                    ) -or $length -lt 0) {
                        throw 'A file length was not returned; the total cannot be trusted.'
                    }
                    $bytes += $length
                    $largestFile = [Math]::Max($largestFile, $length)
                    $count++
                }
                $token = [string]$entries[-1].ContinuationToken
                if ($token -and -not $tokens.Add($token)) { throw 'OneLake repeated a continuation token.' }
            } while ($token)
        } catch {
            $problem = "Lakehouse '$name': $(Format-ServiceError $_)"
            $problems.Add($problem)
        }
        $measurements.Add([pscustomobject]@{
            Item = $name
            ItemId = $lakehouse.id
            Files = $count
            Bytes = $bytes
            GiB = [Math]::Round($bytes / 1GB, 3)
            Measured = ($null -eq $problem)
        })
    }
    Write-Progress -Activity 'Measure OneLake Files metadata' -Completed
}

[decimal] $filePeak = 0
[decimal] $bcpPeak = 0
[decimal] $checkpointReserve = 0
$schemaReserve = [Math]::Min($schemaStores.Count, $SchemaConcurrency) * $DiskStagingGiB
if ($Mode -eq 'SinglePrincipal') {
    foreach ($entry in @($measurements | Sort-Object Bytes -Descending | Select-Object -First $FileConcurrency)) {
        $filePeak += $entry.Bytes / 1GB
    }
    if (-not $SkipData -and $sqlDatabases.Count) {
        if ($null -eq $LargestBcpTableGiB) {
            $names = ($sqlDatabases | ForEach-Object { $_.displayName ?? $_.id }) -join ', '
            $problems.Add("SQLDatabase export size is unknown for: $names. Supply -LargestBcpTableGiB " +
                '(largest native table export, not compressed database size), or use -SkipData if data is not copied.')
        } else {
            $bcpPeak = $LargestBcpTableGiB
        }
    }
} else {
    if (-not $SkipFiles -or -not $SkipData) {
        # Capacity-derived table-copy concurrency is not known here; reserve a checkpoint
        # for every source lakehouse rather than underestimate its possible parallelism.
        $checkpointReserve = $lakehouses.Count * $DiskStagingGiB
    }
}

$subtotal = if ($Mode -eq 'SinglePrincipal') {
    # Schema jobs and bulk/file copy phases finish before the next phase; private schema
    # staging is removed on exit. Leftovers from prior attempts are added separately.
    [Math]::Max([Math]::Max($filePeak, $bcpPeak), $schemaReserve) + $ExistingStagingGiB
} else {
    [Math]::Max($schemaReserve, $checkpointReserve) + $ExistingStagingGiB
}
$estimate = [Math]::Max(1, [Math]::Ceiling($subtotal * (1 + $HeadroomPercent / 100.0)))
if ($largestFile -gt 4TB -or $bcpPeak -gt 4096) {
    $problems.Add('A staged file/export exceeds the Azure Files 4 TiB per-file limit. Use streaming or split it.')
}
if ($estimate -gt 102400) {
    $problems.Add('The estimate exceeds the template share maximum of 102400 GiB. Reduce concurrent/retained staging.')
}
$report = [pscustomobject]@{
    Status = if ($problems.Count) { 'Incomplete - no quota recommendation' } else { 'Estimate - review assumptions' }
    SuggestedShareQuotaGiB = if ($problems.Count) { $null } else { [long]$estimate }
    WorkspaceId = $WorkspaceId.ToString()
    Mode = $Mode
    PeakFilesGiB = [Math]::Round($filePeak, 3)
    LargestBcpExportGiB = $bcpPeak
    SchemaAllowanceGiB = $schemaReserve
    CheckpointAllowanceGiB = $checkpointReserve
    ExistingStagingGiB = $ExistingStagingGiB
    HeadroomPercent = $HeadroomPercent
    Problems = $problems.ToArray()
    Lakehouses = $measurements.ToArray()
    Assumptions = @(
        'Only items/files visible to your user are measured. Verify access to every source item.',
        'Metadata enumeration can take time and incur read-operation costs. It does not download file contents.',
        'SinglePrincipal: largest concurrent Files/ areas, one bcp table export or active schema allowance, whichever is largest.',
        'Paired: data is streamed; reserve bounded schema/checkpoint disk space, not complete data-store copies.',
        'Use the same copy options/concurrency/budget as the migration. Schema allowance is not measured artifact size.',
        'Existing staging defaults to zero (a fresh share). Add leftovers and account for source growth before migration.',
        'OneLake shortcut enumeration can be restricted, including ADLS container-root shortcuts. Read failures are not zero bytes.'
    )
}
if ($AsJson) { $report | ConvertTo-Json -Depth 6 } else { $report }
