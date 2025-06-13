param (
    [Parameter(Mandatory=$true)]
    $spnClientId,
    [Parameter(Mandatory=$true)]
    $spnClientSecret,
    [Parameter(Mandatory=$true)]
    $spnTenantId,
    [Parameter(Mandatory=$true)]
    $capacityName,
    [Parameter(Mandatory=$true)]
    $workspaceName
)


#TODO Workspace folder support needs to be added
#TODO Research KQL Database cross db copy/powershell connectivity

function ReplaceAllOccurances {
    [OutputType([string])]
    Param
    (
        [string]$originalString,
        [HashTable]$replacements
    )
    
    foreach ($key in $replacements.Keys) {
        $originalString = $originalString -replace $key, $replacements[$key]
    }
    return $originalString
}


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
        $token = ConvertFrom-SecureStringToPlainText (Get-AzAccessToken -ResourceUrl "https://database.windows.net").Token
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

function AzCopyOneLakeFiles {
    param (
        [string]$source,
        [string]$destination,
        [string]$ScratchDirectory
    )
    # AzCopy fails on files when doing direct copy from one lake to another
    # Current workaround is to stage locally first then copy to new onelake
    New-Item -ItemType Directory -Path $ScratchDirectory -Force | Out-Null
    azcopy copy --trusted-microsoft-suffixes=onelake.dfs.fabric.microsoft.com $source $ScratchDirectory --recursive
    azcopy copy --trusted-microsoft-suffixes=onelake.dfs.fabric.microsoft.com "$ScratchDirectory/*" $destination --recursive
    rm -rf $ScratchDirectory
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
    #sqlpackage /Action:Extract /TargetFile:"$ScratchDirectory/Test-$WarehouseName-$TransferGuid.dacpac" /SourceConnectionString:"Server=$TargetSqlEndpoint;Initial Catalog=$WarehouseName;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;Authentication=Active Directory Service Principal;User Id=$spnClientId; Password=$spnClientSecret"
    #if ($LASTEXITCODE -ne 0) {
    #    Write-Error "DacFx Schema Extract failed with exit code $LASTEXITCODE"
    #}

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
    #sqlpackage /Action:Publish /SourceFile:"$ScratchDirectory/$WarehouseName-$TransferGuid.dacpac" /TargetConnectionString:"Server=$TargetSqlEndpoint;Initial Catalog=$WarehouseName;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=240;Authentication=Active Directory Service Principal;User Id=$spnClientId; Password=$spnClientSecret" /Diagnostics:True /p:IgnoreFileAndLogFilePath=True /p:VerifyDeployment=False /p:ScriptDatabaseOptions=False
    #Invoke-Sqlcmd -ServerInstance localhost -Database master -Query "CREATE DATABASE [$WarehouseName]" -ErrorAction Stop -UserName sa -Password 'MyPass@word'
    #sqlpackage /Action:Script /SourceFile:"$ScratchDirectory/$WarehouseName-$TransferGuid.dacpac" /TargetConnectionString:"Server=localhost;Initial Catalog=$WarehouseName;MultipleActiveResultSets=False;User Id=sa; Password=MyPass@word" /op:"$ScratchDirectory/sqlscripts/$WarehouseName-$TransferGuid.sql" /Diagnostics:True /p:IgnoreFileAndLogFilePath=True 
    
    if ($LASTEXITCODE -ne 0) {
        Write-Error "DacFx Schema Publish failed with exit code $LASTEXITCODE"
    }
}

function KqlCrossClusterDataMovement {
    param (
        [string]$sourceClusterQueryUri,
        [string]$targetClusterIngestUri,
        [string]$databaseName,
        [string]$spnClientId,
        [string]$spnClientSecret,
        [string]$spnTenantId
    )
    Write-Host "Moving data from $sourceClusterQueryUri $databaseName to $targetClusterIngestUri $databaseName"
    
    # Create a connection string builder for AAD application authentication
    $kqlQuery = New-Object Kusto.Data.KustoConnectionStringBuilder($sourceClusterQueryUri, $databaseName)
    $kqlQuery = $kqlQuery.WithAadApplicationKeyAuthentication($spnClientId, $spnClientSecret, $spnTenantId)

    $kqlIngest = New-Object Kusto.Data.KustoConnectionStringBuilder($targetClusterIngestUri, $databaseName)
    $kqlIngest = $kqlIngest.WithAadApplicationKeyAuthentication($spnClientId, $spnClientSecret, $spnTenantId)

    # Create a Kusto query provider
    $queryClient = [Kusto.Data.Net.Client.KustoClientFactory]::CreateCslQueryProvider($kqlQuery)
    $ingestClient = [Kusto.Data.Net.Client.KustoClientFactory]::CreateCslAdminProvider($kqlIngest)

    # Run a query
    $query = ".show tables | project TableName"
    $reader = $queryClient.ExecuteQuery($query)

    # Read results
    while ($reader.Read()) {
        $table = $reader[0]  # Output first column of each row
        $ingestQuery = ".set-or-replace $table with(distributed=true) <| cluster('$sourceClusterQueryUri').database('$databaseName').$table"
        
        $tryCount = 0
        while ($tryCount -lt 5) {
            try {
                $ingestReader = $ingestClient.ExecuteControlCommand($ingestQuery)

                Write-Host "Ingested data for table: $table"
                while ($ingestReader.Read()) {
                    Write-Host "Rows moved: $($ingestReader[5])"  # Output first column of each row
                }
                $tryCount = 6 # Exit loop on success
            } catch {
                Write-Host "Failed to set or replace table $table $($_.Exception.Message)"
                $tryCount++
                if ($tryCount -ge 5) {
                    Write-Error "Failed to set or replace table $table after 5 attempts."
                    return
                }
                Write-Host "Retrying in $(5*$tryCount) seconds... Attempt $tryCount"
                Sleep -Seconds 5*$tryCount
            }
        }
    }    
}

function DataTransferCopyJob {
    param (
        [string]$itemName,
        [string]$itemType,
        [PSCustomObject]$sourceWorkspace,
        [PSCustomObject]$targetWorkspace,
        [PSCustomObject]$scratchWorkspace
    )
    # Item Types: Warehouse, Lakehouse, SchemaEnabledLakehouse
    $sourceWorkspaceId = $sourceWorkspace.id
    $targetWorkspaceId = $targetWorkspace.id
    $scratchWorkspaceId = $scratchWorkspace.id

    $sourceWorkspaceName = $sourceWorkspace.displayName
    $targetWorkspaceName = $targetWorkspace.displayName
    $scratchWorkspaceName = $scratchWorkspace.displayName

    # Check if the item is a Lakehouse or SchemaEnabledLakehouse

    $copyJobName = "CopyJob_$($itemType)_$($itemName)"
    $activities = [System.Collections.ArrayList]::new()
    $CopyJobObject = [PSCustomObject]@{}
    if ($itemType -eq 'Lakehouse' -or $itemType -eq 'SchemaEnabledLakehouse') {
        $sourceLakehouseId = fab get "$sourceWorkspaceName.Workspace/$itemName.Lakehouse" -q id
        $targetLakehouseId = fab get "$targetWorkspaceName.Workspace/$itemName.Lakehouse" -q id
        $templateContent = Get-Content CopyJobTemplates/lakehouseJobTemplate.json
        $templateContent = $templateContent -replace 'SOURCE_WORKSPACE_ID', $sourceWorkspaceId
        $templateContent = $templateContent -replace 'TARGET_WORKSPACE_ID', $targetWorkspaceId
        $templateContent = $templateContent -replace 'SOURCE_LAKEHOUSE_ID', $sourceLakehouseId
        $templateContent = $templateContent -replace 'TARGET_LAKEHOUSE_ID', $targetLakehouseId
        $CopyJobObject = $templateContent | ConvertFrom-Json
        $activityTemplate = Get-Content CopyJobTemplates/lakehouseActivityTemplate.json | ConvertFrom-Json
        if ($itemType -eq 'SchemaEnabledLakehouse') {
            $activityTemplate.properties.source | Add-Member -MemberType NoteProperty -Name "datasetSettings" -Value $null
            $activityTemplate.properties.destination | Add-Member -MemberType NoteProperty -Name "datasetSettings" -Value $null
            $activityTemplate.properties.source.datasetSettings = [PSCustomObject]@{}
            $activityTemplate.properties.destination.datasetSettings = [PSCustomObject]@{}
            $activityTemplate.properties.source.datasetSettings | Add-Member -MemberType NoteProperty -Name "table" -Value ''
            $activityTemplate.properties.destination.datasetSettings | Add-Member -MemberType NoteProperty -Name "table" -Value ''
            $activityTemplate.properties.source.datasetSettings | Add-Member -MemberType NoteProperty -Name "schema" -Value ''
            $activityTemplate.properties.destination.datasetSettings | Add-Member -MemberType NoteProperty -Name "schema" -Value ''
            # Example Activity datasetSettings
            
            #"datasetSettings": {
            #    "schema": "year_2017",
            #    "table": "green_tripdata_2017"
            #}
            # Where-Object { $_ -notlike '*.Shortcut' } filtering all shortcuts from copy jobs
            $schemas = @(fab ls "$sourceWorkspaceName.Workspace/$itemName.Lakehouse/Tables") | Where-Object { $_ -notlike '*.Shortcut' }
            foreach ($schema in $schemas) {
                $tables = @(fab ls "$sourceWorkspaceName.Workspace/$itemName.Lakehouse/Tables/$schema") | Where-Object { $_ -notlike '*.Shortcut' }
                foreach ($table in $tables) {
                    $activityItem = $activityTemplate | ConvertTo-Json -Depth 10 | ConvertFrom-Json
                    $activityItem.properties.source.datasetSettings.schema = $schema.ToString()
                    $activityItem.properties.source.datasetSettings.table = $table.ToString()
                    $activityItem.properties.destination.datasetSettings.schema = $schema.ToString()
                    $activityItem.properties.destination.datasetSettings.table = $table.ToString()
                    $activityItem.id = [guid]::NewGuid().ToString()
                    if($table.ToString() -ne '[]') {
                        $activities.Add($activityItem) | Out-Null
                    }
                    
                } 
            }
        } else {
            $activityTemplate.properties.source | Add-Member -MemberType NoteProperty -Name "datasetSettings" -Value $null
            $activityTemplate.properties.destination | Add-Member -MemberType NoteProperty -Name "datasetSettings" -Value $null
            $activityTemplate.properties.source.datasetSettings = [PSCustomObject]@{}.PSObject.Copy()
            $activityTemplate.properties.destination.datasetSettings = [PSCustomObject]@{}.PSObject.Copy()
            $activityTemplate.properties.source.datasetSettings | Add-Member -MemberType NoteProperty -Name "table" -Value ''
            $activityTemplate.properties.destination.datasetSettings | Add-Member -MemberType NoteProperty -Name "table" -Value ''

            # Where-Object { $_ -notlike '*.Shortcut' } filtering all shortcuts from copy jobs
            $tables = @(fab ls "$sourceWorkspaceName.Workspace/$itemName.Lakehouse/Tables") | Where-Object { $_ -notlike '*.Shortcut' }

            foreach ($table in $tables) {

                $activityItem = $activityTemplate | ConvertTo-Json -Depth 10 | ConvertFrom-Json
                $activityItem.properties.source.datasetSettings.table = $table.ToString()
                $activityItem.properties.destination.datasetSettings.table = $table.ToString()
                $activityItem.id = [guid]::NewGuid().ToString()
                $activities.Add($activityItem) | Out-Null
            } 
        }
    } elseif ($itemType -eq 'Warehouse') {
        $sourceWarehouseId = fab get "$sourceWorkspaceName.Workspace/$itemName.Warehouse" -q id
        $targetWarehouseId = fab get "$targetWorkspaceName.Workspace/$itemName.Warehouse" -q id
        $sourceWarehouseEndpoint = fab get "$sourceWorkspaceName.Workspace/$itemName.Warehouse" -q properties.connectionInfo
        $targetWarehouseEndpoint = fab get "$targetWorkspaceName.Workspace/$itemName.Warehouse" -q properties.connectionInfo
        $templateContent = Get-Content CopyJobTemplates/warehouseJobTemplate.json
        $templateContent = $templateContent -replace 'SOURCE_WORKSPACE_ID', $sourceWorkspaceId
        $templateContent = $templateContent -replace 'TARGET_WORKSPACE_ID', $targetWorkspaceId
        $templateContent = $templateContent -replace 'SOURCE_WAREHOUSE_ID', $sourceWarehouseId
        $templateContent = $templateContent -replace 'TARGET_WAREHOUSE_ID', $targetWarehouseId
        $templateContent = $templateContent -replace 'SOURCE_WAREHOUSE_ENDPOINT', $sourceWarehouseEndpoint
        $templateContent = $templateContent -replace 'TARGET_WAREHOUSE_ENDPOINT', $targetWarehouseEndpoint
        $CopyJobObject = $templateContent | ConvertFrom-Json
        
        $activityTemplate = Get-Content CopyJobTemplates/warehouseActivityTemplate.json | ConvertFrom-Json

        $activityTemplate.properties.source | Add-Member -MemberType NoteProperty -Name "datasetSettings" -Value $null
        $activityTemplate.properties.destination | Add-Member -MemberType NoteProperty -Name "datasetSettings" -Value $null
        $activityTemplate.properties.source.datasetSettings = [PSCustomObject]@{}
        $activityTemplate.properties.destination.datasetSettings = [PSCustomObject]@{}
        $activityTemplate.properties.source.datasetSettings | Add-Member -MemberType NoteProperty -Name "table" -Value ''
        $activityTemplate.properties.destination.datasetSettings | Add-Member -MemberType NoteProperty -Name "table" -Value ''
        $activityTemplate.properties.source.datasetSettings | Add-Member -MemberType NoteProperty -Name "schema" -Value ''
        $activityTemplate.properties.destination.datasetSettings | Add-Member -MemberType NoteProperty -Name "schema" -Value ''

        $schemas = @(fab ls "$sourceWorkspaceName.Workspace/$itemName.Warehouse/Tables") | Where-Object { $_ -notlike '*.Shortcut' }
        foreach ($schema in $schemas) {
            $tables = @(fab ls "$sourceWorkspaceName.Workspace/$itemName.Warehouse/Tables/$schema") | Where-Object { $_ -notlike '*.Shortcut' }
            foreach ($table in $tables) {
                $activityItem = $activityTemplate | ConvertTo-Json -Depth 10 | ConvertFrom-Json
                $activityItem.properties.source.datasetSettings.schema = $schema.ToString()
                $activityItem.properties.source.datasetSettings.table = $table.ToString()
                $activityItem.properties.destination.datasetSettings.schema = $schema.ToString()
                $activityItem.properties.destination.datasetSettings.table = $table.ToString()
                $activityItem.id = [guid]::NewGuid().ToString()
                if($table.ToString() -ne '[]') {
                    $activities.Add($activityItem) | Out-Null
                }
            } 
        }

    }

    if ($activities.Count -gt 0) {
        
        $CopyJobObject | Add-Member -MemberType NoteProperty -Name "activities" -Value $activities
        New-Item -ItemType Directory -Path "./local/$scratchWorkspaceName/$copyJobName.CopyJob/" -Force | Out-Null
        $CopyJobObject | ConvertTo-Json -Depth 10 | Set-Content -Path "./local/$scratchWorkspaceName/$copyJobName.CopyJob/copyjob-content.json"
        
        # .platform hidden file required for copy job schema
        $platformFile = Get-Content CopyJobTemplates/template.copyjob.platform
        $platformFile = $platformFile -replace 'REPLACE-COPY-JOB-NAME', $copyJobName
        Set-Content -Path "./local/$scratchWorkspaceName/$copyJobName.CopyJob/.platform" -Value $platformFile
        # Create the copy job
        fab import "$scratchWorkspaceName.Workspace/$copyJobName.CopyJob" -i "./local/$scratchWorkspaceName/$copyJobName.CopyJob" -f
        $copyJobId = fab get "$scratchWorkspaceName.Workspace/$copyJobName.CopyJob" -q id
        fab api -X post "workspaces/$scratchWorkspaceId/items/$copyJobId/jobs/instances?jobType=CopyJob"
        $copyJobStatus = fab api -X get "workspaces/$scratchWorkspaceId/items/$copyJobId/jobs/instances" | ConvertFrom-Json
        $copyJobStatusValue = $copyJobStatus.text.value[0].status
        while ($copyJobStatusValue -ne 'Completed' -and $copyJobStatusValue -ne 'Failed') {
            Write-Host "Copy job $copyJobName is in progress. Status: $copyJobStatusValue"
            Start-Sleep -Seconds 5
            $copyJobStatus = fab api -X get "workspaces/$scratchWorkspaceId/items/$copyJobId/jobs/instances" | ConvertFrom-Json
            $copyJobStatusValue = $copyJobStatus.text.value[0].status
        }
        if ($copyJobStatusValue -eq 'Completed') {
            Write-Host "Copy job $copyJobName completed successfully."
        } else {
            Write-Host "Copy job $copyJobName failed."
        }
    }
}

Import-Module Az.Accounts
Import-Module SqlServer

# Manual dependency loading for KQL querying and ingestion
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Azure.Kusto.Ingest.13.0.2/lib/net8.0/Kusto.Ingest.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Azure.Kusto.Data.13.0.2/lib/net8.0/Kusto.Data.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Azure.Kusto.Cloud.Platform.13.0.2/lib/net8.0/Kusto.Cloud.Platform.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Azure.Kusto.Cloud.Platform.Msal.13.0.2/lib/net8.0/Kusto.Cloud.Platform.Msal.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Azure.Core.1.46.1/lib/net8.0/Azure.Core.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Identity.Client.4.72.1/lib/net8.0/Microsoft.Identity.Client.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.IdentityModel.Abstractions.8.9.0/lib/net8.0/Microsoft.IdentityModel.Abstractions.dll"

$trueString = '* true'

$env:AZCOPY_AUTO_LOGIN_TYPE="SPN"
$env:AZCOPY_SPA_APPLICATION_ID=$spnClientId
$env:AZCOPY_SPA_CLIENT_SECRET=$spnClientSecret
$env:AZCOPY_TENANT_ID=$spnTenantId

$env:FAB_SPN_CLIENT_ID=$spnClientId
$env:FAB_SPN_CLIENT_SECRET=$spnClientSecret
$env:FAB_TENANT_ID=$spnTenantId

# Check if capacity exists
$capExists = fab exists ".capacities/$capacityName.Capacity"

if ($capExists -eq $trueString) {
    Write-Host "Capacity $capacityName exists."
    $capacityRegion = fab get ".capacities/$capacityName.Capacity" -q location
    $workspaceExists = fab exists "$workspaceName.Workspace"
    if ($workspaceExists -eq $trueString) {
        Write-Host "Workspace $workspaceName exists."
        fab create "$workspaceName-$capacityRegion.Workspace" -P "capacityname=$capacityName"
        
        $scratchWorkspaceName = "scratch"
        Write-Host "Creating scratch workspace $scratchWorkspaceName and Lakehouse for CopyJobs."
        New-Item -ItemType Directory -Path "./local/$scratchWorkspaceName/" -Force | Out-Null
        #Create scratch workspace for CopyJobs
        fab create "$scratchWorkspaceName.Workspace" -P "capacityname=$capacityName"
        fab create "$scratchWorkspaceName.Workspace/hold.Lakehouse"

        $workspaces = fab api workspaces | ConvertFrom-Json
        $currentWorkspace = $workspaces.text.value | Where-Object { $_.displayName -eq "$workspaceName" }
        $newWorkspace = $workspaces.text.value | Where-Object { $_.displayName -eq "$workspaceName-$capacityRegion" }
        $scratchWorkspace = $workspaces.text.value | Where-Object { $_.displayName -eq "$scratchWorkspaceName" }

        $replacements = @{
            $currentWorkspace.id = $newWorkspace.id
        }
        
        $eventhouseResponse = fab api -X get "workspaces/$($currentWorkspace.id)/eventhouses" | ConvertFrom-Json
        $eventhouses = $eventhouseResponse.text.value
        <# foreach ($eventhouse in $eventhouses) {
            #fab auth login -u $spnClientId -p $spnClientSecret -t $spnTenantId
            if($eventhouse.displayName -eq 'Monitoring Eventhouse') {
                Write-Warning "Skipping Monitoring Eventhouse as it can only be created via Monitoring Settings."
                continue
            }
            fab export "$workspaceName.Workspace/$($eventhouse.displayName).Eventhouse" -o "./local/$scratchWorkspaceName" -f
            fab import "$workspaceName-$capacityRegion.Workspace/$($eventhouse.displayName).Eventhouse" -i "./local/$scratchWorkspaceName/$($eventhouse.displayName).Eventhouse/" -f

            $newEventhouseId = fab get "$workspaceName-$capacityRegion.Workspace/$($eventhouse.displayName).Eventhouse" -q id
            $newEventhouseResponse = fab api -X get "workspaces/$($newWorkspace.id)/eventhouses/$newEventhouseId" | ConvertFrom-Json
            $newEventhouse = $newEventhouseResponse.text

            $replacements[$eventhouse.id] = $newEventhouseId
            $replacements[$eventhouse.properties.queryServiceUri] = $newEventhouse.properties.queryServiceUri
            $replacements[$eventhouse.properties.ingestionServiceUri] = $newEventhouse.properties.ingestionServiceUri

            foreach($kqldbId in $eventhouse.properties.databasesItemIds) {
                $kqldbResponse = fab api -X get "workspaces/$($currentWorkspace.id)/kqldatabases/$kqldbId" | ConvertFrom-Json
                $kqldb = $kqldbResponse.text
                if($kqldb.properties.databaseType -eq 'ReadWrite') {
                    fab export "$workspaceName.Workspace/$($kqldb.displayName).KQLDatabase" -o "./local/$scratchWorkspaceName" -f
                    $kqlProperties = Get-Content "./local/$scratchWorkspaceName/$($kqldb.displayName).KQLDatabase/DatabaseProperties.json" | ConvertFrom-Json
                    $kqlProperties.parentEventhouseItemId = $newEventhouseId
                    $kqlProperties | ConvertTo-Json -Depth 10 | Set-Content -Path "./local/$scratchWorkspaceName/$($kqldb.displayName).KQLDatabase/DatabaseProperties.json"
                    fab import "$workspaceName-$capacityRegion.Workspace/$($kqldb.displayName).KQLDatabase" -i "./local/$scratchWorkspaceName/$($kqldb.displayName).KQLDatabase" -f
                    $newKqlDbId = fab get "$workspaceName-$capacityRegion.Workspace/$($kqldb.displayName).KQLDatabase" -q id
                    $replacements[$kqldb.id] = $newKqlDbId

                    KqlCrossClusterDataMovement -sourceClusterQueryUri $eventhouse.properties.queryServiceUri -targetClusterIngestUri $newEventhouse.properties.queryServiceUri -databaseName $kqldb.displayName -spnClientId $spnClientId -spnClientSecret $spnClientSecret -spnTenantId $spnTenantId
                } else {
                    Write-Warning "Skipping shortcut KQL database $($kqldb.displayName) as it is not a read-write database and is currently not supported for automated creation."
                    #fab create "$($kqldb.displayName).KQLDatabase" -P "dbtype=shortcut,eventhouseid=$newEventhouseId"
                }
            }
        }

        # Currently cannot get KQL shortcut data so we are unable to create them
        <# foreach ($eventhouse in $eventhouses) {
            fab auth login -u $spnClientId -p $spnClientSecret -t $spnTenantId

            $newEventhouseId = $replacements[$eventhouse.id]
            $newEventhouseResponse = fab api -X get "workspaces/$($newWorkspace.id)/eventhouses/$newEventhouseId" | ConvertFrom-Json
            $newEventhouse = $newEventhouseResponse.text

            foreach($kqldbId in $eventhouse.properties.databasesItemIds) {
                $kqldbResponse = fab api -X get "workspaces/$($currentWorkspace.id)/kqldatabases/$kqldbId" | ConvertFrom-Json
                $kqldb = $kqldbResponse.text
                if($kqldb.properties.databaseType -eq 'Shortcut') {
                    Write-Warning "Skipping shortcut database $($kqldb.displayName) as it is not a read-write database."
                    #fab create "$($kqldb.displayName).KQLDatabase" -P "dbtype=shortcut,eventhouseid=$newEventhouseId"
                }
            }
        } #>

        $lakehouseResponse = fab api -X get "workspaces/$($currentWorkspace.id)/lakehouses" | ConvertFrom-Json
        $lakehouses = $lakehouseResponse.text.value
        foreach ($lakehouse in $lakehouses) {
            if ($lakehouse.displayName -eq 'DataflowsStagingLakehouse') {
                Write-Warning "Skipping DataflowsStagingLakehouse as it is a system lakehouse."
                continue
            }
            #fab auth login -u $spnClientId -p $spnClientSecret -t $spnTenantId
            $itemType = 'Lakehouse'
            $schemaEnabled = [bool](Get-Member -inputobject $lakehouse.properties -name "defaultSchema" -Membertype Properties)
            if($schemaEnabled) {
                $itemType = 'SchemaEnabledLakehouse'
                fab create "$workspaceName-$capacityRegion.Workspace/$($lakehouse.displayName).Lakehouse" -P enableschemas=true
            } else {
                fab create "$workspaceName-$capacityRegion.Workspace/$($lakehouse.displayName).Lakehouse"
            }

            $newLakehouseId = fab get "$workspaceName-$capacityRegion.Workspace/$($lakehouse.displayName).Lakehouse" -q id
            $newLakehouseResponse = fab api -X get "workspaces/$($newWorkspace.id)/lakehouses/$newLakehouseId" | ConvertFrom-Json
            $newLakehouse = $newLakehouseResponse.text

            $replacements[$lakehouse.id] = $newLakehouseId

            $sourceLakehouseFilesPath = $lakehouse.properties.oneLakeFilesPath + "/*"
            $targetLakehouseFilesPath = $newLakehouse.properties.oneLakeFilesPath
            $targetLakehouseSqlEndpoint = $newLakehouse.properties.sqlEndpointProperties.connectionString
            $replacements[$lakehouse.properties.sqlEndpointProperties.connectionString] = $targetLakehouseSqlEndpoint
            #Write-Host "Source Lakehouse Files Path: $sourceLakehouseFilesPath"
            #Write-Host "Target Lakehouse Files Path: $targetLakehouseFilesPath"
            # Transfer Lakehouse Table Data   
            DataTransferCopyJob -itemName $lakehouse.displayName -itemType $itemType -sourceWorkspace $currentWorkspace -targetWorkspace $newWorkspace -scratchWorkspace $scratchWorkspace

            # Transfer Lakehouse File Data via AzCopy
            AzCopyOneLakeFiles -source $sourceLakehouseFilesPath -destination $targetLakehouseFilesPath -ScratchDirectory "./local/$scratchWorkspaceName/$($lakehouse.displayName)/"
        }

        $warehouseResponse = fab api -X get "workspaces/$($currentWorkspace.id)/warehouses" | ConvertFrom-Json

        $warehouses = $warehouseResponse.text.value
        foreach ($warehouse in $warehouses) {
            if ($lakehouse.displayName -eq 'DataflowsStagingWarehouse') {
                Write-Warning "Skipping DataflowsStagingWarehouse as it is a system warehouse."
                continue
            }
    
            #Handle if warehouse is using Case Insensitive collation
            if($warehouse.properties.collationType -ne 'Latin1_General_100_CI_AS_KS_WS_SC_UTF8') {
                fab create "$workspaceName-$capacityRegion.Workspace/$($warehouse.displayName).Warehouse"
            } else {
                fab create "$workspaceName-$capacityRegion.Workspace/$($warehouse.displayName).Warehouse" -P enableCaseInsensitive=true
            }
            $newWarehouseId = fab get "$workspaceName-$capacityRegion.Workspace/$($warehouse.displayName).Warehouse" -q id
            $targetWarehouseEndpoint = fab get "$workspaceName-$capacityRegion.Workspace/$($warehouse.displayName).Warehouse" -q properties.connectionInfo

            $replacements[$warehouse.id] = $newWarehouseId
            $replacements[$warehouse.properties.connectionInfo] = $targetWarehouseEndpoint

            DacFxSchemaTransfer -spnClientId $spnClientId -spnClientSecret $spnClientSecret -spnTenantId $spnTenantId -SourceSqlEndpoint $warehouse.properties.connectionInfo -TargetSqlEndpoint $targetWarehouseEndpoint -WarehouseName $warehouse.displayName -ScratchDirectory "./local/$scratchWorkspaceName/" -SourceType 'Warehouse'

            $itemType = 'Warehouse'
            DataTransferCopyJob -itemName $warehouse.displayName -itemType $itemType -sourceWorkspace $currentWorkspace -targetWorkspace $newWorkspace -scratchWorkspace $scratchWorkspace
        }

        # Shortcuts and SQL Endpoint Schemas
        # Shortcuts are only in Lakehouses currently so we can just loop through the lakehouses again
        foreach ($lakehouse in $lakehouses) {
            if ($lakehouse.displayName -eq 'DataflowsStagingLakehouse') {
                Write-Warning "Skipping DataflowsStagingLakehouse as it is a system lakehouse."
                continue
            }
            
            # Get all shortcuts in lakehouse via api
            $lakehouseShortcuts = fab api -X get "workspaces/$($currentWorkspace.id)/items/$($lakehouse.id)/shortcuts"
            $newLakehouseShortcuts = ReplaceAllOccurances -originalString $lakehouseShortcuts -replacements $replacements
            $newLakehouseShortcuts = $newLakehouseShortcuts | ConvertFrom-Json
            $shortcuts = $newLakehouseShortcuts.text.value
            foreach ($shortcut in $shortcuts) {
                $shortcutJson = $shortcut | ConvertTo-Json -Depth 10
                fab api -X post "workspaces/$($newWorkspace.id)/items/$($replacements[$lakehouse.id])/shortcuts" -i $shortcutJson
            }

            # Here we refresh the metadata for the new lakehouse SQL endpoint after the shortcuts have been created
            # Then we are able to do a final SQL Endpoint schema transfer

            $newLakehouseResponse = fab api -X get "workspaces/$($newWorkspace.id)/lakehouses/$($replacements[$lakehouse.id])" | ConvertFrom-Json
            $newLakehouse = $newLakehouseResponse.text

            $sqlEndpointId = $newLakehouse.properties.sqlEndpointProperties.id
            fab api -X post "workspaces/$($newWorkspace.id)/sqlEndpoints/$sqlEndpointId/refreshMetadata?preview=true" -i '{"timeout": {"Minutes": 20}}'

            DacFxSchemaTransfer -spnClientId $spnClientId -spnClientSecret $spnClientSecret -spnTenantId $spnTenantId -SourceSqlEndpoint $lakehouse.properties.sqlEndpointProperties.connectionString -TargetSqlEndpoint $targetLakehouseSqlEndpoint -WarehouseName $lakehouse.displayName -ScratchDirectory "./local/$scratchWorkspaceName/" -SourceType 'Lakehouse'
        }
    }
}

