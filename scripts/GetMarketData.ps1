# GetMarketData.ps1
# Fetches daily close prices from Alpha Vantage and writes to CSV

param(
    [string]$Ticker = "SPY",
    [string]$OutputPath = "spy.csv",
    [string]$StartDate = "2025-08-29",
    [string]$EndDate = "",
    [string]$ApiKey = ""
)

# If output file exists, find the latest date and use it as start
$existingRows = @()
if (Test-Path $OutputPath) {
    $existingContent = Get-Content $OutputPath | Where-Object { $_.Trim() }
    $maxDate = $null
    foreach ($row in ($existingContent | Select-Object -Skip 1)) {
        $cols = $row -split ","
        if ($cols.Length -ge 2) {
            try {
                $d = Get-Date $cols[0] -ErrorAction Stop
                # Normalize existing rows to M/d/yyyy
                $normalized = "$($d.Month)/$($d.Day)/$($d.Year),$($cols[1])"
                $existingRows += $normalized
                if ($null -eq $maxDate -or $d -gt $maxDate) { $maxDate = $d }
            } catch { continue }
        }
    }
    if ($maxDate) {
        $StartDate = $maxDate.ToString("yyyy-MM-dd")
        Write-Host "Found existing data through $StartDate, fetching new rows..."
    }
}

$url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=$Ticker&outputsize=compact&apikey=$ApiKey&datatype=csv"

$cutoff = Get-Date $StartDate
$cutoffEnd = if ($EndDate) { Get-Date $EndDate } else { Get-Date }

try {
    $response = Invoke-WebRequest -Uri $url -UseBasicParsing
    
    if ($response.Content -is [byte[]]) {
        $text = [System.Text.Encoding]::UTF8.GetString($response.Content)
    } else {
        $text = $response.Content
    }

    if ($text -match '"Information"' -or $text -match '"Error"') {
        throw $text
    }

    $rows = $text -split "`n" | Where-Object { $_.Trim() }

    # Alpha Vantage CSV columns: timestamp,open,high,low,close,volume
    $newRows = @()
    foreach ($row in ($rows | Select-Object -Skip 1)) {
        $cols = $row -split ","
        if ($cols.Length -ge 5) {
            try {
                $parsed = Get-Date $cols[0] -ErrorAction Stop
                if ($parsed -gt $cutoff -and $parsed -le $cutoffEnd) {
                    $dateFormatted = "$($parsed.Month)/$($parsed.Day)/$($parsed.Year)"
                    $newRows += "$dateFormatted,$($cols[4])"
                }
            } catch { continue }
        }
    }

    # Merge existing + new, sort chronologically
    $allRows = $existingRows + $newRows
    $sorted = $allRows | Sort-Object { (Get-Date ($_ -split ",")[0]) }
    $output = @("Date,Close") + $sorted

    $output | Out-File -FilePath $OutputPath -Encoding UTF8
    Write-Host "Wrote $($sorted.Count) total $Ticker rows to $OutputPath ($($newRows.Count) new)"
}
catch {
    Write-Error "Failed to fetch $Ticker data: $_"
    exit 1
}
