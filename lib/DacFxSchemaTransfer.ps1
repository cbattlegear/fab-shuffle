Import-Module Az.Accounts
Import-Module SqlServer

function ConvertFrom-SecureStringToPlainText {
    param (
        [Parameter(Mandatory = $true)]
        [System.Security.SecureString]$SecureString
    )

    $ptr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureString)
    try {
        return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
    }
    finally {
        [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
    }
}

function Test-SQLConnection
{
    [OutputType([bool])]
    Param
    (
        [string]$ServerName,
        [string]$DatabaseName,
        [string]$spnClientId,
        [string]$spnClientSecret,
        [string]$spnTenantId
    )
    try
    {
        Connect-AzAccount -ServicePrincipal -TenantId $spnTenantId -Credential (New-Object System.Management.Automation.PSCredential($spnClientId, (ConvertTo-SecureString $spnClientSecret -AsPlainText -Force)))
        $token = ConvertFrom-SecureStringToPlainText (Get-AzAccessToken -ResourceUrl "https://database.windows.net" -AsSecureString).Token
        Invoke-Sqlcmd -ServerInstance $ServerName -Database "master" -AccessToken $token -Query "select COUNT(*) from sys.dm_exec_requests" -ErrorAction Stop -Verbose 4>&1
        Invoke-Sqlcmd -ServerInstance $ServerName -Database $DatabaseName -AccessToken $token -Query "select COUNT(*) from sys.dm_exec_requests" -ErrorAction Stop -Verbose 4>&1

        return $true;
    }
    catch
    {
        Write-Host "SQL Connection failed: $($_.Exception.Message)"
        return $false;
    }
}

function DacFxSchemaTransfer {
    param (
        [string]$spnClientId,
        [string]$spnClientSecret,
        [string]$spnTenantId,
        [string]$SourceSqlEndpoint,
        [string]$TargetSqlEndpoint,
        [string]$WarehouseName,
        [string]$ScratchDirectory,
        [string]$SourceType
    )
    # Verify target SQL Endpoint is deployed and available 
    $sqlConnected = Test-SQLConnection -ServerName $TargetSqlEndpoint -DatabaseName $WarehouseName -spnClientId $spnClientId -spnClientSecret $spnClientSecret -spnTenantId $spnTenantId
    while ($sqlConnected -eq $false) {
        Write-Host "Waiting for SQL Endpoint to be available..."
        Start-Sleep -Seconds 15
        $sqlConnected = Test-SQLConnection -ServerName $TargetSqlEndpoint -DatabaseName $WarehouseName -spnClientId $spnClientId -spnClientSecret $spnClientSecret -spnTenantId $spnTenantId
    }
    $TransferGuid = [guid]::NewGuid().ToString()
    $dotnetToolsDir = "$env:HOME/.dotnet/tools/"
    $sqlpackage = "$dotnetToolsDir/sqlpackage"
    & $sqlpackage /Action:Extract /TargetFile:"$ScratchDirectory/$WarehouseName-$TransferGuid.dacpac" /SourceConnectionString:"Server=$SourceSqlEndpoint;Initial Catalog=$WarehouseName;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;Authentication=Active Directory Service Principal;User Id=$spnClientId; Password=$spnClientSecret"

    $unpackdacpac = "$dotnetToolsDir/unpackdacpac"
    if ($SourceType -eq 'Lakehouse') {
        & $unpackdacpac unpack "$ScratchDirectory/$WarehouseName-$TransferGuid.dacpac" "$ScratchDirectory/$WarehouseName-$TransferGuid/" --deploy-script-exclude-object-type Tables --deploy-script-exclude-object-type Schemas
    } else {
        & $unpackdacpac unpack "$ScratchDirectory/$WarehouseName-$TransferGuid.dacpac" "$ScratchDirectory/$WarehouseName-$TransferGuid/"
    }

    (Get-Content "$ScratchDirectory/$WarehouseName-$TransferGuid/Deploy.sql" | Select-Object -Skip 44) | Set-Content "$ScratchDirectory/$WarehouseName-$TransferGuid/Deploy.sql"

    Connect-AzAccount -ServicePrincipal -TenantId $spnTenantId -Credential (New-Object System.Management.Automation.PSCredential($spnClientId, (ConvertTo-SecureString $spnClientSecret -AsPlainText -Force)))
    $token = ConvertFrom-SecureStringToPlainText (Get-AzAccessToken -ResourceUrl "https://database.windows.net").Token
    Invoke-Sqlcmd -DisableCommands -DisableVariables -ServerInstance $TargetSqlEndpoint -Database $WarehouseName -AccessToken $token -InputFile "$ScratchDirectory/$WarehouseName-$TransferGuid/Deploy.sql"

    if ($LASTEXITCODE -ne 0) {
        Write-Error "DacFx Schema Publish failed with exit code $LASTEXITCODE"
    }
}