# Manual dependency loading for KQL querying and ingestion
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Azure.Kusto.Ingest.13.0.2/lib/net8.0/Kusto.Ingest.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Azure.Kusto.Data.13.0.2/lib/net8.0/Kusto.Data.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Azure.Kusto.Cloud.Platform.13.0.2/lib/net8.0/Kusto.Cloud.Platform.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Azure.Kusto.Cloud.Platform.Msal.13.0.2/lib/net8.0/Kusto.Cloud.Platform.Msal.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Azure.Core.1.46.1/lib/net8.0/Azure.Core.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.Identity.Client.4.72.1/lib/net8.0/Microsoft.Identity.Client.dll"
Add-Type -Path "/usr/local/share/PackageManagement/NuGet/Packages/Microsoft.IdentityModel.Abstractions.8.9.0/lib/net8.0/Microsoft.IdentityModel.Abstractions.dll"


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
                Start-Sleep -Seconds 5*$tryCount
            }
        }
    }    
}