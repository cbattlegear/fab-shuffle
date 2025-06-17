# Manual dependency loading for KQL querying and ingestion
# These are the minimum required libraries for Kusto querying and ingestion
# They are individually installed as part of the Dockerfile build

Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Azure.Kusto.Ingest.13.0.2/lib/net8.0/Kusto.Ingest.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Azure.Kusto.Data.13.0.2/lib/net8.0/Kusto.Data.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Azure.Kusto.Cloud.Platform.13.0.2/lib/net8.0/Kusto.Cloud.Platform.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Azure.Kusto.Cloud.Platform.Msal.13.0.2/lib/net8.0/Kusto.Cloud.Platform.Msal.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Azure.Core.1.46.1/lib/net8.0/Azure.Core.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Identity.Client.4.72.1/lib/net8.0/Microsoft.Identity.Client.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.IdentityModel.Abstractions.8.9.0/lib/net8.0/Microsoft.IdentityModel.Abstractions.dll"

# IngestUri is incorrectly named, it is actually the query URI for the target cluster
# For cross cluster .set-or-replace you use the query URI of both clusters instead of the ingest URI
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

    # Get list of tables in the source cluster
    $query = ".show tables | project TableName"
    $reader = $queryClient.ExecuteQuery($query)

    # Loop through each table in the source cluster to transfer into the target cluster
    Write-Host "Starting data movement for tables in $sourceClusterQueryUri $databaseName"
    while ($reader.Read()) {
        $table = $reader[0] 
        # Create our ingestion query, distributed=true may not be the best option for efficiency for small tables
        # Potentially should look at number of rows in the table and decide whether to use distributed or not
        Write-Host "Ingesting data for table: $table"
        $ingestQuery = ".set-or-replace $table with(distributed=true) <| cluster('$sourceClusterQueryUri').database('$databaseName').$table"
        
        $tryCount = 0
        while ($tryCount -lt 5) {
            try {
                # We could make this an async operation on the KQL side, but for simplicity we will just execute it synchronously
                $ingestReader = $ingestClient.ExecuteControlCommand($ingestQuery)

                while ($ingestReader.Read()) {
                    Write-Host "Finished $table data movement, ingested $($ingestReader[5]) rows."
                }
                # this is probably a dumb way to handle the retry logic, but it works for now
                $tryCount = 6 # Exit loop on success
            } catch {
                Write-Host "Failed to set or replace table $table $($_.Exception.Message)"
                $tryCount++
                if ($tryCount -ge 5) {
                    Write-Error "Failed to set or replace table $table after 5 attempts."
                    return
                }
                Write-Host "Retrying in $(5*$tryCount) seconds... Attempt $tryCount"
                Start-Sleep -Seconds 5*$tryCount
            }
        }
    }    
}