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