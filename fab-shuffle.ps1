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
#TODO ACL info for connections, I see this getting more important with pipelines, dataflows, and seeing it now with shortcuts
#TODO Allow for parallel Copy Jobs to be run and monitored
#TODO Add support for Dataflows, Pipelines, and other items that are not currently supported
#TODO Allow Prefix/Postfix to Workspace
#TODO Better error handling and logging
#TODO Full permissions precheck or precheck without run 

# Basic replacement function to replace all occurrences of keys in a string with their corresponding values from a hashtable
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

# Importing these modules for 
Import-Module Az.Accounts
Import-Module SqlServer

# Import extra functions, moved to lib folder for better organization
. "$PSScriptRoot/lib/DataTransferCopyJob.ps1"
. "$PSScriptRoot/lib/KqlCrossClusterDataMovement.ps1"
. "$PSScriptRoot/lib/DacFxSchemaTransfer.ps1"
. "$PSScriptRoot/lib/AzCopyOneLakeFiles.ps1"

# Magic string to handle the output from the `fab exists` command
$trueString = '* true'

# Adding environment variables for AzCopy and Fabric authentication
$env:AZCOPY_AUTO_LOGIN_TYPE="SPN"
$env:AZCOPY_SPA_APPLICATION_ID=$spnClientId
$env:AZCOPY_SPA_CLIENT_SECRET=$spnClientSecret
$env:AZCOPY_TENANT_ID=$spnTenantId

$env:FAB_SPN_CLIENT_ID=$spnClientId
$env:FAB_SPN_CLIENT_SECRET=$spnClientSecret
$env:FAB_TENANT_ID=$spnTenantId

# Check if capacity exists
$capExists = fab exists ".capacities/$capacityName.Capacity"

# I should break this logic out in a "prereq" function that can be called independently via CLI
if ($capExists -eq $trueString) {
    Write-Host "Capacity $capacityName exists."
    $capacityRegion = fab get ".capacities/$capacityName.Capacity" -q location
    $workspaceExists = fab exists "$workspaceName.Workspace"
    if ($workspaceExists -eq $trueString) {
        Write-Host "Workspace $workspaceName exists."
        fab create "$workspaceName-$capacityRegion.Workspace" -P "capacityname=$capacityName"

        # Using GUID for scratch workspace name to avoid conflicts
        $scratchWorkspaceName = [guid]::NewGuid().ToString()
        Write-Host "Creating scratch workspace $scratchWorkspaceName and Lakehouse for CopyJobs."

        # Create local directory for temporary storage
        New-Item -ItemType Directory -Path "./local/$scratchWorkspaceName/" -Force | Out-Null
        
        #Create scratch workspace for CopyJobs
        fab create "$scratchWorkspaceName.Workspace" -P "capacityname=$capacityName"

        # IMPORTANT: We need to create a Lakehouse in the scratch workspace or we cannot create CopyJobs
        # I haven't determined the exact mechanism yet, but it seems like the workspace isn't fully initialized until a Lakehouse is created
        fab create "$scratchWorkspaceName.Workspace/hold.Lakehouse"

        # Get the current workspace and new workspace details
        # Learned it was easier to get the workspace details via API than using the CLI
        $workspaces = fab api workspaces | ConvertFrom-Json
        $currentWorkspace = $workspaces.text.value | Where-Object { $_.displayName -eq "$workspaceName" }
        $newWorkspace = $workspaces.text.value | Where-Object { $_.displayName -eq "$workspaceName-$capacityRegion" }
        $scratchWorkspace = $workspaces.text.value | Where-Object { $_.displayName -eq "$scratchWorkspaceName" }

        $CurrentWorkspaceAclRespose = fab api -X get "/admin/workspaces/$($currentWorkspace.id)/users" | ConvertFrom-Json
        $CurrentWorkspaceAcl = $CurrentWorkspaceAclRespose.text.accessDetails

        #Add all workspace Admins to the scratch workspace
        foreach ($acl in $CurrentWorkspaceAcl) {
            if (($acl.principal.type -eq 'User' -or $acl.principal.type -eq 'Group') -and $acl.workspaceAccessDetails.workspaceRole -eq 'Admin') {
                fab acl set "$scratchWorkspaceName.Workspace" -I $acl.principal.id -R $acl.workspaceAccessDetails.workspaceRole.ToLower() -f
            }
        }

        # Here we track all string replacements in the hash table, eventually I'll need to figure out how to handle this better
        # or at least how to determine order of creation for items that are dependent on each other
        $replacements = @{
            $currentWorkspace.id = $newWorkspace.id
        }
        
        # Here we start with the creation of the Eventhouses for KQL Databases
        $eventhouseResponse = fab api -X get "workspaces/$($currentWorkspace.id)/eventhouses" | ConvertFrom-Json
        $eventhouses = $eventhouseResponse.text.value
        foreach ($eventhouse in $eventhouses) {
            # Skip the Monitoring Eventhouse as it is a system eventhouse and cannot be created via CLI
            if($eventhouse.displayName -eq 'Monitoring Eventhouse') {
                Write-Warning "Skipping Monitoring Eventhouse as it can only be created via Monitoring Settings."
                continue
            }
            # Eventhouses can be exported/imported so we use this to get all the details instead of manually creating them
            fab export "$workspaceName.Workspace/$($eventhouse.displayName).Eventhouse" -o "./local/$scratchWorkspaceName" -f
            fab import "$workspaceName-$capacityRegion.Workspace/$($eventhouse.displayName).Eventhouse" -i "./local/$scratchWorkspaceName/$($eventhouse.displayName).Eventhouse/" -f

            # Standard process to get the new Item ID and properties
            $newEventhouseId = fab get "$workspaceName-$capacityRegion.Workspace/$($eventhouse.displayName).Eventhouse" -q id
            $newEventhouseResponse = fab api -X get "workspaces/$($newWorkspace.id)/eventhouses/$newEventhouseId" | ConvertFrom-Json
            $newEventhouse = $newEventhouseResponse.text

            # Add the new Eventhouse ID and properties to the replacements hash table
            $replacements[$eventhouse.id] = $newEventhouseId
            $replacements[$eventhouse.properties.queryServiceUri] = $newEventhouse.properties.queryServiceUri
            $replacements[$eventhouse.properties.ingestionServiceUri] = $newEventhouse.properties.ingestionServiceUri

            # Loop through each KQL Database in the Eventhouse
            foreach($kqldbId in $eventhouse.properties.databasesItemIds) {
                $kqldbResponse = fab api -X get "workspaces/$($currentWorkspace.id)/kqldatabases/$kqldbId" | ConvertFrom-Json
                $kqldb = $kqldbResponse.text
                # If the KQL Database is a ReadWrite database, we can export and import it, shortcut/follower databases
                # don't have a way to get their connection properties via API yet so we skip them for now
                if($kqldb.properties.databaseType -eq 'ReadWrite') {
                    # Via the export process we can get the full KQL DB schema and properties for creation
                    fab export "$workspaceName.Workspace/$($kqldb.displayName).KQLDatabase" -o "./local/$scratchWorkspaceName" -f
                    $kqlProperties = Get-Content "./local/$scratchWorkspaceName/$($kqldb.displayName).KQLDatabase/DatabaseProperties.json" | ConvertFrom-Json
                    $kqlProperties.parentEventhouseItemId = $newEventhouseId
                    $kqlProperties | ConvertTo-Json -Depth 10 | Set-Content -Path "./local/$scratchWorkspaceName/$($kqldb.displayName).KQLDatabase/DatabaseProperties.json"
                    fab import "$workspaceName-$capacityRegion.Workspace/$($kqldb.displayName).KQLDatabase" -i "./local/$scratchWorkspaceName/$($kqldb.displayName).KQLDatabase" -f
                    $newKqlDbId = fab get "$workspaceName-$capacityRegion.Workspace/$($kqldb.displayName).KQLDatabase" -q id
                    $replacements[$kqldb.id] = $newKqlDbId
                    
                    # Once the schema is transferred we can do full data transfer
                    KqlCrossClusterDataMovement -sourceClusterQueryUri $eventhouse.properties.queryServiceUri -targetClusterIngestUri $newEventhouse.properties.queryServiceUri -databaseName $kqldb.displayName -spnClientId $spnClientId -spnClientSecret $spnClientSecret -spnTenantId $spnTenantId
                } else {
                    Write-Warning "Skipping shortcut KQL database $($kqldb.displayName) as it is not a read-write database and is currently not supported for automated creation."
                }
            }
        }

        # Currently cannot get KQL shortcut data so we are unable to create them
        # holding this code for potential future use....because that's a smart way to do things...
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

        # Lakehouses are complex as we need to handle Tables, Files, and SQL Endpoints all differently. 
        $lakehouseResponse = fab api -X get "workspaces/$($currentWorkspace.id)/lakehouses" | ConvertFrom-Json
        $lakehouses = $lakehouseResponse.text.value
        foreach ($lakehouse in $lakehouses) {
            # Skip the DataflowsStagingLakehouse as it is a system lakehouse and breaks things if we try to create it
            if ($lakehouse.displayName -eq 'DataflowsStagingLakehouse') {
                Write-Warning "Skipping DataflowsStagingLakehouse as it is a system lakehouse."
                continue
            }
            
            $itemType = 'Lakehouse'
            # Check if the lakehouse is schema enabled via the defaultSchema property
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
            
            # Transfer Lakehouse Table Data   
            # This strictly moves table data, I learned that AzCopy tends to fail on tables for various reasons while 
            # copyjobs honor table structure and efficiently write delta tables
            DataTransferCopyJob -itemName $lakehouse.displayName -itemType $itemType -sourceWorkspace $currentWorkspace -targetWorkspace $newWorkspace -scratchWorkspace $scratchWorkspace

            # Transfer Lakehouse File Data via AzCopy
            # This uses AzCopy to do the file transfer, currently using this to prevent issues with Shortcuts
            # as copyjobs do not distinguish between real files and shortcuts
            # This stages the files locally first so you need enough drive space on your machine to handle the transfer
            # I will look further into handling this via copyjobs in the future to prevent the data pulldown
            AzCopyOneLakeFiles -source $sourceLakehouseFilesPath -destination $targetLakehouseFilesPath -ScratchDirectory "./local/$scratchWorkspaceName/$($lakehouse.displayName)/"
        } #>

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
    
        # Set the ACLs for the new workspace
        # This will loop all permissions and add them to the new workspace
        foreach ($acl in $CurrentWorkspaceAcl) {
            fab acl set "$scratchWorkspaceName.Workspace" -I $acl.principal.id -R $acl.workspaceAccessDetails.workspaceRole.ToLower() -f
        }

    }
}

