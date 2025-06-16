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

. "$PSScriptRoot/DataTransferCopyJob.ps1"
. "$PSScriptRoot/KqlCrossClusterDataMovement.ps1"
. "$PSScriptRoot/DacFxSchemaTransfer.ps1"
. "$PSScriptRoot/AzCopyOneLakeFiles.ps1"

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
        foreach ($eventhouse in $eventhouses) {
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

