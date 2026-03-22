<#
.SYNOPSIS
    Downloads IBKR Flex Query trade data and merges daily P&L into a CSV file.
.DESCRIPTION
    Uses the IBKR Flex Web Service (v3) to fetch trade data from a configured
    FlexQuery, groups trades by TradeDate, sums NetCash per day, and merges
    the results into an existing CSV file.
.PARAMETER MergeData
    Path to the CSV file to merge data into. Created if it doesn't exist.
.PARAMETER IbkrToken
    IBKR Flex Web Service token. Falls back to IBKR_TOKEN env var.
.PARAMETER IbkrQueryId
    IBKR Flex Query ID. Falls back to IBKR_QUERY_ID env var.
.PARAMETER Days
    Number of days of history to fetch (default: 30)
.EXAMPLE
    .\GetIBKROrders.ps1 -MergeData "..\data.csv"
.EXAMPLE
    .\GetIBKROrders.ps1 -MergeData "..\data.csv" -IbkrToken "xxxx" -IbkrQueryId "12345"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$MergeData,

    [Parameter(Mandatory=$false)]
    [string]$IbkrToken,

    [Parameter(Mandatory=$false)]
    [string]$IbkrQueryId,

    [Parameter(Mandatory=$false)]
    [int]$Days = 30
)

# Resolve auth: params win, env vars as fallback
if ([string]::IsNullOrWhiteSpace($IbkrToken)) { $IbkrToken = $env:IBKR_TOKEN }
if ([string]::IsNullOrWhiteSpace($IbkrQueryId)) { $IbkrQueryId = $env:IBKR_QUERY_ID }

if ([string]::IsNullOrWhiteSpace($IbkrToken)) {
    Write-Error "IbkrToken is required. Provide via -IbkrToken parameter or IBKR_TOKEN env var."
    exit 1
}
if ([string]::IsNullOrWhiteSpace($IbkrQueryId)) {
    Write-Error "IbkrQueryId is required. Provide via -IbkrQueryId parameter or IBKR_QUERY_ID env var."
    exit 1
}

# Read existing data if file exists
$existingData = @()
$existingDateColumn = "date"
$existingPLColumn = "pl"
if (Test-Path $MergeData) {
    Write-Host "Reading existing data from: $MergeData" -ForegroundColor Cyan
    $existingData = @(Import-Csv -Path $MergeData)

    if ($existingData.Count -gt 0) {
        # Detect column names (support both date/pl and Date/ProfitLoss)
        $firstRow = $existingData[0]
        if ($firstRow.PSObject.Properties.Name -contains "Date") {
            $existingDateColumn = "Date"
        }
        if ($firstRow.PSObject.Properties.Name -contains "ProfitLoss") {
            $existingPLColumn = "ProfitLoss"
        }

        $latestDate = $existingData | ForEach-Object { [DateTime]::Parse($_.$existingDateColumn) } | Sort-Object -Descending | Select-Object -First 1
        Write-Host "  Found $($existingData.Count) existing records" -ForegroundColor Gray
        Write-Host "  Latest date: $($latestDate.ToString('yyyy-MM-dd'))" -ForegroundColor Gray
    }
    else {
        Write-Host "  File is empty, will fetch data" -ForegroundColor Yellow
    }
}
else {
    Write-Host "MergeData file does not exist: $MergeData" -ForegroundColor Yellow
    Write-Host "  Will create new file with columns: $existingDateColumn, $existingPLColumn" -ForegroundColor Gray
}

# Build date range for Flex query override
$toDate = (Get-Date).ToString('yyyyMMdd')
$fromDate = (Get-Date).AddDays(-$Days).ToString('yyyyMMdd')
Write-Host "`nFetching IBKR data from $fromDate to $toDate" -ForegroundColor Cyan

# Step 1: Send request to get reference code
$sendUrl = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?t=$IbkrToken&q=$IbkrQueryId&fd=$fromDate&td=$toDate&v=3"
Write-Host "  Requesting Flex report..." -ForegroundColor Gray

try {
    $sendResponse = Invoke-RestMethod -Uri $sendUrl -Method Get
}
catch {
    Write-Error "Failed to send Flex request: $_"
    exit 1
}

# Parse the XML response for reference code
if ($sendResponse.FlexStatementResponse.Status -ne "Success") {
    $errorMsg = $sendResponse.FlexStatementResponse.ErrorMessage
    Write-Error "Flex SendRequest failed: $errorMsg"
    exit 1
}

$referenceCode = $sendResponse.FlexStatementResponse.ReferenceCode
Write-Host "  Reference code: $referenceCode" -ForegroundColor Gray

# Step 2: Poll GetStatement until ready
$getUrl = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement?t=$IbkrToken&q=$referenceCode&v=3"
$maxRetries = 10
$retryDelay = 5

for ($i = 1; $i -le $maxRetries; $i++) {
    Write-Host "  Fetching statement (attempt $i/$maxRetries)..." -ForegroundColor Gray

    try {
        $response = Invoke-WebRequest -Uri $getUrl -Method Get
        $content = $response.Content
    }
    catch {
        Write-Error "Failed to fetch Flex statement: $_"
        exit 1
    }

    # If response is XML, it might be a "not ready" or error status
    if ($content.TrimStart().StartsWith("<")) {
        try {
            [xml]$xmlResponse = $content
            if ($xmlResponse.FlexStatementResponse.Status -eq "Warn" -and $xmlResponse.FlexStatementResponse.ErrorCode -eq "1019") {
                Write-Host "    Statement not ready yet, waiting ${retryDelay}s..." -ForegroundColor Yellow
                Start-Sleep -Seconds $retryDelay
                continue
            }
            else {
                $errorMsg = $xmlResponse.FlexStatementResponse.ErrorMessage
                Write-Error "Flex GetStatement failed: $errorMsg"
                exit 1
            }
        }
        catch {
            # Not valid XML but starts with < — unexpected
            Write-Error "Unexpected XML response: $content"
            exit 1
        }
    }

    # Got CSV data
    Write-Host "  Statement received" -ForegroundColor Green
    break
}

if ($content.TrimStart().StartsWith("<")) {
    Write-Error "Flex statement not ready after $maxRetries attempts"
    exit 1
}

# Step 3: Parse CSV and aggregate by TradeDate
$trades = $content | ConvertFrom-Csv

if ($null -eq $trades -or @($trades).Count -eq 0) {
    Write-Host "  No trades returned for date range" -ForegroundColor Yellow
    exit 0
}

$trades = @($trades)
Write-Host "  Parsed $($trades.Count) trade(s)" -ForegroundColor Gray

# Group by TradeDate, sum NetCash
$dailyData = $trades | Group-Object -Property TradeDate | ForEach-Object {
    $dateRaw = $_.Name
    # Normalize date to yyyy-MM-dd
    $dateKey = ([DateTime]::Parse($dateRaw)).ToString('yyyy-MM-dd')
    $totalPL = ($_.Group | Measure-Object -Property NetCash -Sum).Sum

    [PSCustomObject]@{
        Date      = $dateKey
        ProfitLoss = [math]::Round($totalPL, 2)
    }
}

$dailyData = @($dailyData)
Write-Host "  Aggregated to $($dailyData.Count) trading day(s)" -ForegroundColor Gray

foreach ($day in $dailyData | Sort-Object Date) {
    Write-Host "    $($day.Date): $($day.ProfitLoss)" -ForegroundColor White
}

# Step 4: Merge into target CSV
Write-Host "`nMerging data..." -ForegroundColor Cyan

$dateCol = $existingDateColumn
$plCol = $existingPLColumn

$mergedHash = @{}

# Add existing data first
foreach ($row in $existingData) {
    $dateValue = $row.$dateCol
    $dateKey = ([DateTime]::Parse($dateValue)).ToString('yyyy-MM-dd')
    $mergedHash[$dateKey] = [ordered]@{
        $dateCol = $dateKey
        $plCol = $row.$plCol
    }
}

# Add/overwrite with new data
$updatedCount = 0
$addedCount = 0
foreach ($record in $dailyData) {
    $dateKey = $record.Date
    if ($mergedHash.ContainsKey($dateKey)) {
        $existingPL = [double]$mergedHash[$dateKey].$plCol
        if ($existingPL -ne $record.ProfitLoss) {
            $updatedCount++
        }
    } else {
        $addedCount++
    }
    $mergedHash[$dateKey] = [ordered]@{
        $dateCol = $dateKey
        $plCol = $record.ProfitLoss
    }
}

# Only write if there are actual changes
if ($addedCount -gt 0 -or $updatedCount -gt 0) {
    $mergedData = $mergedHash.Values | ForEach-Object {
        [PSCustomObject]$_
    } | Sort-Object { [DateTime]::Parse($_.$dateCol) }

    $mergedData | Export-Csv -Path $MergeData -NoTypeInformation -UseQuotes Never -Force

    Write-Host "  Records added: $addedCount" -ForegroundColor Green
    Write-Host "  Records updated: $updatedCount" -ForegroundColor Yellow
    Write-Host "  Total records: $($mergedData.Count)" -ForegroundColor White
    Write-Host "  Saved to: $MergeData" -ForegroundColor Green
}
else {
    Write-Host "  No changes detected - file unchanged" -ForegroundColor Gray
}
