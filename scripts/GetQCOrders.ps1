<#
.SYNOPSIS
    Downloads QuantConnect Live Trading Orders and aggregates by day
.DESCRIPTION
    Connects to QuantConnect API to retrieve live trading orders for a project
    and calculates the sum of orders per day. Output is structured for SQL insertion.
.PARAMETER UserId
    QuantConnect User ID for API authentication
.PARAMETER ApiToken
    QuantConnect API Token for authentication
.PARAMETER ProjectId
    The Project ID to retrieve live trading orders from
.PARAMETER StartDate
    Optional: Start date for order retrieval (default: 30 days ago)
.PARAMETER EndDate
    Optional: End date for order retrieval (default: today)
.PARAMETER DelayBetweenRequestsMs
    Optional: Minimum delay between API requests in milliseconds (default: 2000)
.PARAMETER MaxRequestsPerMinute
    Optional: Maximum API requests per minute (default: 20)
.PARAMETER UpdateDB
    Optional: If specified, upserts data into SQL Server database
.PARAMETER SqlServer
    SQL Server instance name (required if UpdateDB is specified)
.PARAMETER Database
    Database name (required if UpdateDB is specified)
.PARAMETER SqlUsername
    SQL Server username (optional - uses Windows authentication if not provided)
.PARAMETER SqlPassword
    SQL Server password (optional - uses Windows authentication if not provided)
.EXAMPLE
    .\Get-QCLiveOrders.ps1 -UserId "101966" -ApiToken "your-token" -ProjectId "25328004"
.EXAMPLE
    .\Get-QCLiveOrders.ps1 -UserId "101966" -ApiToken "your-token" -ProjectId "25328004" -UpdateDB -SqlServer "localhost" -Database "Trading"
#>

[CmdletBinding()]
param(
    # Optional for local execution - can use env vars: USER_ID, API_TOKEN, PROJECT_ID
    [Parameter(Mandatory=$false)]
    [string]$UserId,
    
    [Parameter(Mandatory=$false)]
    [string]$ApiToken,
    
    [Parameter(Mandatory=$false)]
    [string]$ProjectId,
    
    [Parameter(Mandatory=$false)]
    [DateTime]$StartDate = (Get-Date).AddDays(-30),
    
    [Parameter(Mandatory=$false)]
    [DateTime]$EndDate = (Get-Date),
    
    [Parameter(Mandatory=$false)]
    [string]$TimeZoneId = (Get-TimeZone).Id,  # Defaults to system timezone
    
    [Parameter(Mandatory=$false)]
    [int]$DelayBetweenRequestsMs = 1000,
    
    [Parameter(Mandatory=$false)]
    [int]$MaxRequestsPerMinute = 45,
    
    [Parameter(Mandatory=$false)]
    [switch]$UpdateDB,
    
    [Parameter(Mandatory=$false)]
    [string]$SqlServer,
    
    [Parameter(Mandatory=$false)]
    [string]$Database,
    
    [Parameter(Mandatory=$false)]
    [string]$SqlUsername,
    
    [Parameter(Mandatory=$false)]
    [string]$SqlPassword,
    
    [Parameter(Mandatory=$false)]
    [string]$ExportCsv,
    
    [Parameter(Mandatory=$false)]
    [switch]$AllDeployments,
    
    [Parameter(Mandatory=$false)]
    [string]$MergeData
)

# Resolve auth params: params win, env vars as fallback
if ([string]::IsNullOrWhiteSpace($UserId)) { $UserId = $env:USER_ID }
if ([string]::IsNullOrWhiteSpace($ApiToken)) { $ApiToken = $env:API_TOKEN }
if ([string]::IsNullOrWhiteSpace($ProjectId)) { $ProjectId = $env:PROJECT_ID }

# Validate required auth params
if ([string]::IsNullOrWhiteSpace($UserId)) {
    Write-Error "UserId is required. Provide via -UserId parameter or USER_ID env var."
    exit 1
}
if ([string]::IsNullOrWhiteSpace($ApiToken)) {
    Write-Error "ApiToken is required. Provide via -ApiToken parameter or API_TOKEN env var."
    exit 1
}
if ([string]::IsNullOrWhiteSpace($ProjectId)) {
    Write-Error "ProjectId is required. Provide via -ProjectId parameter or PROJECT_ID env var."
    exit 1
}

# Validate SQL parameters if UpdateDB is specified
if ($UpdateDB) {
    if ([string]::IsNullOrWhiteSpace($SqlServer)) {
        Write-Error "SqlServer parameter is required when UpdateDB is specified"
        exit 1
    }
    if ([string]::IsNullOrWhiteSpace($Database)) {
        Write-Error "Database parameter is required when UpdateDB is specified"
        exit 1
    }
}

# Handle MergeData - read existing file and determine StartDate
$existingData = @()
$existingDateColumn = "Date"
$existingPLColumn = "ProfitLoss"
if ($MergeData) {
    if (Test-Path $MergeData) {
        Write-Host "Reading existing data from: $MergeData" -ForegroundColor Cyan
        $existingData = @(Import-Csv -Path $MergeData)
        
        if ($existingData.Count -gt 0) {
            # Detect column names (support both date/pl and Date/ProfitLoss)
            $firstRow = $existingData[0]
            if ($firstRow.PSObject.Properties.Name -contains "date") {
                $existingDateColumn = "date"
            }
            if ($firstRow.PSObject.Properties.Name -contains "pl") {
                $existingPLColumn = "pl"
            }
            
            # Find the latest date in the file
            $latestDate = $existingData | ForEach-Object { [DateTime]::Parse($_.$existingDateColumn) } | Sort-Object -Descending | Select-Object -First 1
            $StartDate = $latestDate
            Write-Host "  Found $($existingData.Count) existing records" -ForegroundColor Gray
            Write-Host "  Latest date: $($latestDate.ToString('yyyy-MM-dd'))" -ForegroundColor Gray
            Write-Host "  Will fetch orders from: $($StartDate.ToString('yyyy-MM-dd'))" -ForegroundColor Green
        }
        else {
            Write-Host "  File is empty, will fetch all orders" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host "MergeData file does not exist: $MergeData" -ForegroundColor Yellow
        Write-Host "  Will create new file" -ForegroundColor Gray
    }
    
    # Automatically enable AllDeployments for merge operations
    $AllDeployments = $true
}

# QuantConnect API Base URL
$baseUrl = "https://www.quantconnect.com/api/v2"

# Track API calls for rate limiting
$script:apiCallTimestamps = [System.Collections.ArrayList]@()

# Function to enforce rate limiting
function Wait-ForRateLimit {
    param(
        [int]$MaxRequestsPerMinute = 20,
        [int]$MinDelayMs = 2000
    )
    
    $now = Get-Date
    
    # Remove timestamps older than 1 minute
    $recentCalls = [System.Collections.ArrayList]@()
    foreach ($ts in $script:apiCallTimestamps) {
        if (($now - $ts).TotalSeconds -lt 60) {
            [void]$recentCalls.Add($ts)
        }
    }
    $script:apiCallTimestamps = $recentCalls
    
    # Check if we're at the limit
    if ($script:apiCallTimestamps.Count -ge $MaxRequestsPerMinute) {
        $oldestCall = $script:apiCallTimestamps | Sort-Object | Select-Object -First 1
        $timeSinceOldest = $now - $oldestCall
        $waitSeconds = 60 - $timeSinceOldest.TotalSeconds + 1
        
        if ($waitSeconds -gt 0) {
            Write-Host "  Rate limit reached. Waiting $([Math]::Ceiling($waitSeconds)) seconds..." -ForegroundColor Yellow
            Start-Sleep -Seconds ([Math]::Ceiling($waitSeconds))
        }
    }
    
    # Always enforce minimum delay between requests
    if ($script:apiCallTimestamps.Count -gt 0) {
        $lastCall = $script:apiCallTimestamps | Sort-Object -Descending | Select-Object -First 1
        $timeSinceLast = $now - $lastCall
        $timeSinceLastCall = $timeSinceLast.TotalMilliseconds
        
        if ($timeSinceLastCall -lt $MinDelayMs) {
            $waitMs = $MinDelayMs - $timeSinceLastCall
            Write-Verbose "Enforcing minimum delay: waiting $([Math]::Ceiling($waitMs))ms"
            Start-Sleep -Milliseconds ([Math]::Ceiling($waitMs))
        }
    }
    
    # Record this API call
    [void]$script:apiCallTimestamps.Add((Get-Date))
}

# Function to convert DateTime to Unix timestamp (treating input as specified timezone)
function ConvertTo-UnixTimestamp {
    param(
        [DateTime]$DateTime,
        [string]$TimeZoneId = "Eastern Standard Time"
    )
    
    $epoch = [DateTime]::new(1970, 1, 1, 0, 0, 0, [DateTimeKind]::Utc)
    
    # If DateTime is Unspecified, treat it as being in the specified timezone
    if ($DateTime.Kind -eq [DateTimeKind]::Unspecified) {
        $tz = [TimeZoneInfo]::FindSystemTimeZoneById($TimeZoneId)
        $utcTime = [TimeZoneInfo]::ConvertTimeToUtc($DateTime, $tz)
        return [long]($utcTime - $epoch).TotalSeconds
    }
    else {
        # If already UTC or Local, convert appropriately
        return [long]($DateTime.ToUniversalTime() - $epoch).TotalSeconds
    }
}

# Function to create authentication headers with timestamp hashing
function Get-AuthHeaders {
    $timestamp = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds().ToString()
    $timeStampedToken = "${ApiToken}:${timestamp}"
    
    # Create SHA256 hash of the timestamped token
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    $hashBytes = $sha256.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($timeStampedToken))
    $hashedToken = [System.BitConverter]::ToString($hashBytes).Replace("-", "").ToLower()
    
    # Create Basic auth string
    $authString = "${UserId}:${hashedToken}"
    $authBytes = [System.Text.Encoding]::ASCII.GetBytes($authString)
    $authBase64 = [Convert]::ToBase64String($authBytes)
    
    return @{
        "Authorization" = "Basic $authBase64"
        "Timestamp" = $timestamp
    }
}

# Function to make authenticated API calls with rate limiting
function Invoke-QCApiCall {
    param(
        [string]$Endpoint,
        [string]$Method = "GET"
    )
    
    # Enforce rate limiting BEFORE making the call
    Wait-ForRateLimit -MaxRequestsPerMinute $MaxRequestsPerMinute -MinDelayMs $DelayBetweenRequestsMs
    
    try {
        $headers = Get-AuthHeaders
        $uri = "${baseUrl}${Endpoint}"
        
        Write-Verbose "Calling API: $uri"
        Write-Verbose "API calls in last minute: $($script:apiCallTimestamps.Count)"
        
        $params = @{
            Uri = $uri
            Method = $Method
            Headers = $headers
            ErrorAction = "Stop"
        }
        
        $response = Invoke-RestMethod @params
        
        # Convert response to hashtable for consistent property access
        if ($response -is [string]) {
            Write-Verbose "Response is string, parsing JSON as hashtable..."
            $response = $response | ConvertFrom-Json -AsHashtable
        }
        elseif ($response -is [System.Management.Automation.PSCustomObject]) {
            Write-Verbose "Response is PSCustomObject, converting to hashtable..."
            $json = $response | ConvertTo-Json -Depth 100
            $response = $json | ConvertFrom-Json -AsHashtable
        }
        
        return $response
    }
    catch {
        Write-Host "API Call Exception:" -ForegroundColor Red
        Write-Host "  URI: $uri" -ForegroundColor Yellow
        Write-Host "  Message: $($_.Exception.Message)" -ForegroundColor Yellow
        
        # Check for rate limiting errors
        if ($_.Exception.Message -match "429" -or $_.Exception.Message -match "rate limit") {
            Write-Host "  RATE LIMIT HIT! Waiting 60 seconds before retrying..." -ForegroundColor Red
            Start-Sleep -Seconds 60
        }
        
        Write-Error "API call failed: $($_.Exception.Message)"
        throw
    }
}

# Function to get deployment ID from project (tries to get most recent deployment with orders)
# Function to get all deployment IDs from project that overlap with date range
function Get-AllDeploymentIds {
    param(
        [string]$ProjectId,
        [DateTime]$StartDate,
        [DateTime]$EndDate
    )
    
    Write-Host "Getting deployments for project..." -ForegroundColor Cyan
    
    try {
        $listEndpoint = "/live/list?projectId=$ProjectId"
        $listData = Invoke-QCApiCall -Endpoint $listEndpoint
        
        if ($listData['success'] -and $listData['live'] -and $listData['live'].Count -gt 0) {
            $deployments = $listData['live']
            Write-Host "  Found $($deployments.Count) total deployment(s)" -ForegroundColor Gray
            
            # Filter to deployments that overlap with our date range
            # API returns timestamps in UTC - convert to local time for comparison
            $filtered = $deployments | Where-Object {
                $launched = if ($_['launched']) { 
                    [DateTime]::SpecifyKind([DateTime]::Parse($_['launched']), [DateTimeKind]::Utc).ToLocalTime() 
                } else { 
                    [DateTime]::MinValue 
                }
                $stopped = if ($_['stopped'] -and $_['stopped'] -ne '') { 
                    [DateTime]::SpecifyKind([DateTime]::Parse($_['stopped']), [DateTimeKind]::Utc).ToLocalTime() 
                } else { 
                    [DateTime]::MaxValue 
                }
                
                # Deployment overlaps if: launched before EndDate AND stopped after StartDate
                $launched -le $EndDate -and $stopped -ge $StartDate
            }
            
            Write-Host "  $($filtered.Count) deployment(s) overlap with date range" -ForegroundColor Cyan
            
            # Sort by launched time descending
            $sorted = $filtered | Sort-Object { 
                if ($_['launched']) { [DateTime]::Parse($_['launched']) } else { [DateTime]::MinValue }
            } -Descending
            
            return $sorted | ForEach-Object { $_['deployId'] }
        }
        
        return @()
    }
    catch {
        Write-Warning "Failed to get deployments: $($_.Exception.Message)"
        return @()
    }
}

# Function to get deployment ID from project (tries to get most recent deployment with orders)
function Get-DeploymentId {
    param([string]$ProjectId)
    
    Write-Host "Getting deployment ID for project..." -ForegroundColor Cyan
    
    try {
        # First, try to list all live algorithms for this project
        $listEndpoint = "/live/list?projectId=$ProjectId"
        $listData = Invoke-QCApiCall -Endpoint $listEndpoint
        
        if ($listData['success'] -and $listData['live'] -and $listData['live'].Count -gt 0) {
            # Sort by launched time (most recent first) and return the first one
            $deployments = $listData['live']
            Write-Host "  Found $($deployments.Count) deployment(s)" -ForegroundColor Gray
            
            # Sort by launched time descending
            $sorted = $deployments | Sort-Object { 
                if ($_['launched']) { [DateTime]::Parse($_['launched']) } else { [DateTime]::MinValue }
            } -Descending
            
            foreach ($deployment in $sorted) {
                $deployId = $deployment['deployId']
                $launched = $deployment['launched']
                $stopped = $deployment['stopped']
                
                Write-Host "    - $deployId (launched: $launched, stopped: $stopped)" -ForegroundColor Gray
            }
            
            # Return the most recent deployment
            $mostRecent = $sorted | Select-Object -First 1
            $deployId = $mostRecent['deployId']
            Write-Host "  Using most recent: $deployId" -ForegroundColor Green
            return $deployId
        }
        
        # Fallback: try the /live/read endpoint for current deployment
        Write-Verbose "No deployments from /live/list, trying /live/read..."
        $liveEndpoint = "/live/read?projectId=$ProjectId"
        $liveData = Invoke-QCApiCall -Endpoint $liveEndpoint
        
        Write-Verbose "Response received. Checking success status..."
        
        # Try multiple possible locations for deployment ID
        $deployId = $null
        
        foreach ($key in @('deployId', 'DeployId', 'deploymentId', 'DeploymentId')) {
            if ($liveData.ContainsKey($key) -and $liveData[$key]) {
                $deployId = $liveData[$key]
                break
            }
        }
        
        # Check in 'live' sub-object if it exists
        if (-not $deployId -and $liveData.ContainsKey('live')) {
            $live = $liveData['live']
            foreach ($key in @('deployId', 'DeployId', 'deploymentId', 'DeploymentId')) {
                if ($live.ContainsKey($key) -and $live[$key]) {
                    $deployId = $live[$key]
                    break
                }
            }
        }
        
        if (-not $deployId) {
            Write-Host "Could not find deployment ID. Available keys:" -ForegroundColor Red
            $liveData.Keys | ForEach-Object {
                Write-Host "  - $_" -ForegroundColor Gray
            }
            throw "Could not find deployment ID in response."
        }
        
        Write-Host "  Deployment ID: $deployId" -ForegroundColor Green
        return $deployId
    }
    catch {
        Write-Host "Exception details:" -ForegroundColor Red
        Write-Host "  Message: $($_.Exception.Message)" -ForegroundColor Yellow
        Write-Error "Failed to get deployment ID: $_"
        throw
    }
}

# Function to get live trading orders
function Get-LiveTradingOrders {
    param(
        [string]$ProjectId,
        [string]$DeployId,
        [DateTime]$StartDate,
        [DateTime]$EndDate
    )
    
    Write-Host "Retrieving live trading orders..." -ForegroundColor Cyan
    Write-Host "  Date Range: $($StartDate.ToString('yyyy-MM-dd HH:mm')) to $($EndDate.ToString('yyyy-MM-dd HH:mm')) ($TimeZoneId)" -ForegroundColor Gray
    
    try {
        $allOrders = [System.Collections.ArrayList]@()
        
        # Convert dates to Unix timestamps for filtering
        Write-Verbose "Converting StartDate: $StartDate (Type: $($StartDate.GetType().FullName), Kind: $($StartDate.Kind))"
        Write-Verbose "Converting EndDate: $EndDate (Type: $($EndDate.GetType().FullName), Kind: $($EndDate.Kind))"
        Write-Verbose "Using TimeZone: $TimeZoneId"
        
        # Ensure StartDate begins at start of day (00:00:00)
        $adjustedStartDate = $StartDate.Date
        
        # Ensure EndDate includes the full day - if no time specified, use end of day (23:59:59)
        $adjustedEndDate = if ($EndDate.TimeOfDay.TotalSeconds -eq 0) {
            $EndDate.Date.AddDays(1).AddSeconds(-1)  # End of the specified day
        } else {
            $EndDate  # Keep the specified time
        }
        
        Write-Verbose "Adjusted StartDate: $($adjustedStartDate.ToString('yyyy-MM-dd HH:mm:ss'))"
        Write-Verbose "Adjusted EndDate: $($adjustedEndDate.ToString('yyyy-MM-dd HH:mm:ss'))"
        
        $startTimestamp = ConvertTo-UnixTimestamp -DateTime $adjustedStartDate -TimeZoneId $TimeZoneId
        $endTimestamp = ConvertTo-UnixTimestamp -DateTime $adjustedEndDate -TimeZoneId $TimeZoneId
        
        Write-Verbose "Filter Timestamps: start=$startTimestamp, end=$endTimestamp"
        
        # Note: The API uses 'start' and 'end' as ORDER INDICES (0, 100, 200, etc.), NOT timestamps!
        # We need to fetch orders in batches of 100 and filter by timestamp locally
        
        $batchSize = 100
        $currentIndex = 0
        $hasMoreOrders = $true
        $consecutiveEmptyBatches = 0
        $maxConsecutiveEmptyBatches = 3  # Stop after 3 consecutive empty batches
        
        Write-Host "  Fetching orders in batches of $batchSize..." -ForegroundColor Gray
        
        while ($hasMoreOrders) {
            $batchStart = $currentIndex
            $batchEnd = $currentIndex + $batchSize
            
            $ordersEndpoint = "/live/orders/read?projectId=$ProjectId&deployId=$DeployId&start=$batchStart&end=$batchEnd"
            
            Write-Verbose "  Fetching batch: indices $batchStart to $batchEnd"
            $response = Invoke-QCApiCall -Endpoint $ordersEndpoint
            
            if ($response['success'] -eq $false) {
                $errorMsg = $response['errors'] -join ', '
                Write-Warning "API returned error at index ${currentIndex}: $errorMsg"
                $hasMoreOrders = $false
                break
            }
            
            if ($response['orders']) {
                $batchOrders = $response['orders']
                $addedCount = 0
                
                Write-Verbose "  Batch has orders. Type: $($batchOrders.GetType().FullName)"
                
                # Handle different response formats
                if ($batchOrders -is [System.Collections.Hashtable] -or $batchOrders -is [System.Collections.IDictionary]) {
                    Write-Verbose "    Orders is a Hashtable/Dictionary with $($batchOrders.Keys.Count) keys"
                    foreach ($key in $batchOrders.Keys) {
                        $order = $batchOrders[$key]
                        
                        # Filter by timestamp
                        $orderTime = $null
                        $foundTimeField = $null
                        foreach ($timeField in @('Time', 'time', 'CreatedTime', 'createdTime', 'lastFillTime', 'LastFillTime')) {
                            if ($order[$timeField]) {
                                $timeValue = $order[$timeField]
                                $foundTimeField = $timeField
                                if ($timeValue -is [long] -or $timeValue -is [int]) {
                                    $orderTime = $timeValue
                                }
                                elseif ($timeValue -is [string]) {
                                    try {
                                        $orderTime = [DateTimeOffset]::Parse($timeValue).ToUnixTimeSeconds()
                                    }
                                    catch {
                                        Write-Verbose "Failed to parse time string: $timeValue"
                                    }
                                }
                                break
                            }
                        }
                        
                        # Debug: Show first order's structure
                        if ($currentIndex -eq 0 -and $addedCount -eq 0 -and $key -eq ($batchOrders.Keys | Select-Object -First 1)) {
                            Write-Host "`n  DEBUG: First order structure:" -ForegroundColor Yellow
                            Write-Host "    Found time field: $foundTimeField = $($order[$foundTimeField])" -ForegroundColor Gray
                            Write-Host "    Converted timestamp: $orderTime" -ForegroundColor Gray
                            Write-Host "    Filter range: $startTimestamp to $endTimestamp" -ForegroundColor Gray
                            Write-Host "    Available keys: $($order.Keys -join ', ')" -ForegroundColor Gray
                        }
                        
                        if ($orderTime -and $orderTime -ge $startTimestamp -and $orderTime -le $endTimestamp) {
                            [void]$allOrders.Add($order)
                            $addedCount++
                        }
                    }
                }
                elseif ($batchOrders -is [Array]) {
                    $orderIndex = 0
                    foreach ($order in $batchOrders) {
                        # Filter by timestamp
                        $orderTime = $null
                        $foundTimeField = $null
                        foreach ($timeField in @('time', 'Time', 'createdTime', 'CreatedTime', 'lastFillTime', 'LastFillTime')) {
                            if ($order[$timeField]) {
                                $timeValue = $order[$timeField]
                                $foundTimeField = $timeField
                                if ($timeValue -is [long] -or $timeValue -is [int]) {
                                    $orderTime = $timeValue
                                }
                                elseif ($timeValue -is [DateTime]) {
                                    # It's already a DateTime object
                                    try {
                                        $epoch = [DateTime]::new(1970, 1, 1, 0, 0, 0, [DateTimeKind]::Utc)
                                        $utcDate = [DateTime]::SpecifyKind($timeValue, [DateTimeKind]::Utc)
                                        $orderTime = [long]($utcDate - $epoch).TotalSeconds
                                    }
                                    catch {
                                        Write-Verbose "Failed to convert DateTime: $timeValue - $_"
                                    }
                                }
                                elseif ($timeValue -is [string]) {
                                    try {
                                        # Parse the date string and convert to Unix timestamp
                                        $parsedDate = [DateTime]::Parse($timeValue)
                                        # Treat parsed date as UTC and convert to Unix timestamp
                                        $epoch = [DateTime]::new(1970, 1, 1, 0, 0, 0, [DateTimeKind]::Utc)
                                        $utcDate = [DateTime]::SpecifyKind($parsedDate, [DateTimeKind]::Utc)
                                        $orderTime = [long]($utcDate - $epoch).TotalSeconds
                                    }
                                    catch {
                                        Write-Verbose "Failed to parse time string: $timeValue - $_"
                                    }
                                }
                                break
                            }
                        }
                        

                        
                        if ($orderTime -and $orderTime -ge $startTimestamp -and $orderTime -le $endTimestamp) {
                            [void]$allOrders.Add($order)
                            $addedCount++
                        }
                        $orderIndex++
                    }
                }
                
                Write-Verbose "  Batch ${currentIndex}-${batchEnd}: Retrieved $($batchOrders.Count) orders, $addedCount matched date filter"
                
                # If we got fewer than batchSize orders, we've reached the end
                $orderCount = if ($batchOrders -is [Array]) { $batchOrders.Count } else { $batchOrders.Keys.Count }
                
                if ($orderCount -eq 0) {
                    # Empty batch - increment consecutive empty counter
                    $consecutiveEmptyBatches++
                    Write-Verbose "  Empty batch - consecutive empty: $consecutiveEmptyBatches"
                    
                    if ($consecutiveEmptyBatches -ge $maxConsecutiveEmptyBatches) {
                        Write-Verbose "  Reached $maxConsecutiveEmptyBatches consecutive empty batches, stopping"
                        $hasMoreOrders = $false
                    }
                    else {
                        $currentIndex += $batchSize
                    }
                }
                elseif ($orderCount -lt $batchSize) {
                    Write-Verbose "  Reached end of orders (got $orderCount orders, expected $batchSize)"
                    $hasMoreOrders = $false
                }
                else {
                    # Reset consecutive empty counter when we find orders
                    $consecutiveEmptyBatches = 0
                    $currentIndex += $batchSize
                }
            }
            else {
                Write-Verbose "  No orders key in batch $batchStart-$batchEnd"
                $consecutiveEmptyBatches++
                
                if ($consecutiveEmptyBatches -ge $maxConsecutiveEmptyBatches) {
                    Write-Verbose "  Reached $maxConsecutiveEmptyBatches consecutive empty batches, stopping"
                    $hasMoreOrders = $false
                }
                else {
                    $currentIndex += $batchSize
                }
            }
        }
        
        Write-Host "  Retrieved $($allOrders.Count) orders matching date filter" -ForegroundColor Green
        return ,$allOrders.ToArray()
    }
    catch {
        Write-Host "Error in Get-LiveTradingOrders:" -ForegroundColor Red
        Write-Host "  Error: $($_.Exception.Message)" -ForegroundColor Yellow
        Write-Host "  At line: $($_.InvocationInfo.ScriptLineNumber)" -ForegroundColor Yellow
        Write-Host "  Command: $($_.InvocationInfo.Line.Trim())" -ForegroundColor Yellow
        Write-Error "Failed to retrieve orders: $_"
        throw
    }
}

# Function to aggregate orders by day
function Get-OrderAggregationByDay {
    param([array]$Orders)
    
    Write-Host "`nAggregating orders by day..." -ForegroundColor Cyan
    
    if ($Orders.Count -eq 0) {
        return @()
    }
    
    # Parse and organize orders
    $validOrders = [System.Collections.ArrayList]@()
    foreach ($order in $Orders) {
        try {
            $orderTime = $null
            
            # Try different time field names (lowercase first since that's what API returns)
            foreach ($timeField in @('time', 'Time', 'createdTime', 'CreatedTime', 'lastFillTime', 'LastFillTime')) {
                if ($order[$timeField]) {
                    $timeValue = $order[$timeField]
                    if ($timeValue -is [long] -or $timeValue -is [int]) {
                        $orderTime = [DateTimeOffset]::FromUnixTimeSeconds($timeValue).DateTime
                    }
                    elseif ($timeValue -is [DateTime]) {
                        $orderTime = $timeValue
                    }
                    else {
                        $orderTime = [DateTime]::Parse($timeValue)
                    }
                    break
                }
            }
            
            if ($orderTime) {
                $order['ParsedTime'] = $orderTime
                [void]$validOrders.Add($order)
            }
        }
        catch {
            Write-Verbose "Failed to parse order time: $_"
        }
    }
    
    Write-Host "  Successfully parsed $($validOrders.Count) orders" -ForegroundColor Cyan
    
    if ($validOrders.Count -eq 0) {
        return @()
    }
    
    # Group by day and aggregate
    $aggregated = $validOrders | Group-Object {
        $_.ParsedTime.Date.ToString('yyyy-MM-dd')
    } | ForEach-Object {
        $dayOrders = $_.Group
        
        $totalQty = 0
        $sumOrderValue = 0  # This represents ProfitLoss for the day
        $symbols = [System.Collections.ArrayList]@()
        
        foreach ($order in $dayOrders) {
            # Sum quantities (use absolute values)
            if ($order['quantity']) { 
                $totalQty += [Math]::Abs([double]$order['quantity']) 
            }
            if ($order['Quantity']) { 
                $totalQty += [Math]::Abs([double]$order['Quantity']) 
            }
            if ($order['AbsoluteQuantity']) { 
                $totalQty += [double]$order['AbsoluteQuantity'] 
            }
            if ($order['QuantityFilled']) { 
                $totalQty += [Math]::Abs([double]$order['QuantityFilled']) 
            }
            
            # Sum order values for options contracts
            # For options: P&L = value * -100 (negative value = profit, multiply by 100 for contract multiplier)
            if ($order['value']) { 
                $sumOrderValue += ([double]$order['value'] * -100)
            }
            if ($order['Value']) { 
                $sumOrderValue += ([double]$order['Value'] * -100)
            }
            
            # Collect symbols
            if ($order['Symbol']) { 
                $symbolStr = if ($order['Symbol'] -is [string]) { 
                    $order['Symbol'] 
                }
                else { 
                    $order['Symbol'].Value 
                }
                if ($symbolStr -and $symbolStr -ne "") {
                    [void]$symbols.Add($symbolStr)
                }
            }
        }
        
        [PSCustomObject]@{
            Date = $_.Name
            OrderCount = $_.Count
            SumOrderValue = [math]::Round($sumOrderValue, 2)
            TotalQuantity = [math]::Round($totalQty, 2)
            Symbols = (($symbols | Select-Object -Unique) -join ',')
        }
    } | Sort-Object Date
    
    return $aggregated
}

# Function to upsert data into SQL Server
function Update-SQLDatabase {
    param(
        [Parameter(Mandatory=$true)]
        [array]$Data,
        
        [Parameter(Mandatory=$true)]
        [string]$ServerInstance,
        
        [Parameter(Mandatory=$true)]
        [string]$DatabaseName,
        
        [Parameter(Mandatory=$false)]
        [string]$Username,
        
        [Parameter(Mandatory=$false)]
        [string]$Password
    )
    
    Write-Host "`nUpdating SQL Server database..." -ForegroundColor Cyan
    Write-Host "  Server: $ServerInstance" -ForegroundColor Gray
    Write-Host "  Database: $DatabaseName" -ForegroundColor Gray
    
    try {
        # Build connection string
        if ([string]::IsNullOrWhiteSpace($Username)) {
            # Use Windows Authentication
            $connectionString = "Server=$ServerInstance;Database=$DatabaseName;Integrated Security=True;"
            Write-Host "  Authentication: Windows (current user)" -ForegroundColor Gray
        }
        else {
            # Use SQL Authentication
            $connectionString = "Server=$ServerInstance;Database=$DatabaseName;User Id=$Username;Password=$Password;"
            Write-Host "  Authentication: SQL Server ($Username)" -ForegroundColor Gray
        }
        
        # Create SQL connection
        $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
        $connection.Open()
        
        Write-Host "  Connection established successfully" -ForegroundColor Green
        
        # UPSERT query using MERGE
        $mergeQuery = @"
MERGE INTO [dbo].[DDDResults] AS target
USING (SELECT @Date AS [Date], @OrderCount AS [OrderCount], @ProfitLoss AS [ProfitLoss]) AS source
ON (target.[Date] = source.[Date])
WHEN MATCHED THEN
    UPDATE SET 
        [OrderCount] = source.[OrderCount],
        [ProfitLoss] = source.[ProfitLoss]
WHEN NOT MATCHED THEN
    INSERT ([Date], [OrderCount], [ProfitLoss])
    VALUES (source.[Date], source.[OrderCount], source.[ProfitLoss]);
"@
        
        $updateCount = 0
        $insertCount = 0
        $skippedCount = 0
        
        foreach ($record in $Data) {
            try {
                # Skip records where SumOrderValue is 0
                if ($record.SumOrderValue -eq 0) {
                    $skippedCount++
                    Write-Verbose "  Skipped: $($record.Date) - SumOrderValue is 0"
                    continue
                }
                
                # Check if record exists
                $checkCmd = $connection.CreateCommand()
                $checkCmd.CommandText = "SELECT COUNT(*) FROM [dbo].[DDDResults] WHERE [Date] = @Date"
                $checkCmd.Parameters.AddWithValue("@Date", [DateTime]::Parse($record.Date)) | Out-Null
                $exists = [int]$checkCmd.ExecuteScalar() -gt 0
                
                # Execute MERGE
                $cmd = $connection.CreateCommand()
                $cmd.CommandText = $mergeQuery
                $cmd.Parameters.AddWithValue("@Date", [DateTime]::Parse($record.Date)) | Out-Null
                $cmd.Parameters.AddWithValue("@OrderCount", $record.OrderCount) | Out-Null
                $cmd.Parameters.AddWithValue("@ProfitLoss", $record.SumOrderValue) | Out-Null
                
                $cmd.ExecuteNonQuery() | Out-Null
                
                if ($exists) {
                    $updateCount++
                    Write-Verbose "  Updated: $($record.Date) - OrderCount: $($record.OrderCount), ProfitLoss: $($record.SumOrderValue)"
                }
                else {
                    $insertCount++
                    Write-Verbose "  Inserted: $($record.Date) - OrderCount: $($record.OrderCount), ProfitLoss: $($record.SumOrderValue)"
                }
            }
            catch {
                Write-Warning "Failed to upsert record for date $($record.Date): $($_.Exception.Message)"
            }
        }
        
        $connection.Close()
        
        Write-Host "`n  Database update complete:" -ForegroundColor Green
        Write-Host "    Records inserted: $insertCount" -ForegroundColor White
        Write-Host "    Records updated: $updateCount" -ForegroundColor White
        Write-Host "    Records skipped (zero ProfitLoss): $skippedCount" -ForegroundColor Yellow
        Write-Host "    Total processed: $($Data.Count)" -ForegroundColor White
        
        return $true
    }
    catch {
        Write-Error "Failed to update database: $($_.Exception.Message)"
        if ($connection -and $connection.State -eq 'Open') {
            $connection.Close()
        }
        return $false
    }
}

# Main execution
try {
    Write-Host "`n=== QuantConnect Live Orders Aggregation ===" -ForegroundColor Yellow
    Write-Host "Project ID: $ProjectId" -ForegroundColor White
    Write-Host "Date Range: $($StartDate.ToString('yyyy-MM-dd HH:mm')) to $($EndDate.ToString('yyyy-MM-dd HH:mm')) ($TimeZoneId)" -ForegroundColor White
    Write-Host "Rate Limiting: Max $MaxRequestsPerMinute requests/minute, $DelayBetweenRequestsMs ms between requests" -ForegroundColor White
    if ($AllDeployments) {
        Write-Host "Mode: ALL deployments" -ForegroundColor Cyan
    }
    Write-Host ""
    
    $orders = @()
    $deployId = $null
    
    if ($AllDeployments) {
        # Get orders from ALL deployments that overlap with date range
        $allDeployIds = Get-AllDeploymentIds -ProjectId $ProjectId -StartDate $StartDate -EndDate $EndDate
        $deployId = "ALL"
        
        foreach ($did in $allDeployIds) {
            Write-Host "  Checking deployment: $did" -ForegroundColor Gray
            $deployOrders = Get-LiveTradingOrders -ProjectId $ProjectId -DeployId $did -StartDate $StartDate -EndDate $EndDate
            if ($deployOrders.Count -gt 0) {
                Write-Host "    Found $($deployOrders.Count) orders" -ForegroundColor Green
                $orders += $deployOrders
            }
        }
        Write-Host "`nTotal orders from all deployments: $($orders.Count)" -ForegroundColor Cyan
    }
    else {
        # Step 1: Get deployment ID from project (most recent)
        $deployId = Get-DeploymentId -ProjectId $ProjectId
        
        # Step 2: Retrieve orders using deployment ID
        $orders = Get-LiveTradingOrders -ProjectId $ProjectId -DeployId $deployId -StartDate $StartDate -EndDate $EndDate
    }
    
    if ($orders.Count -eq 0) {
        Write-Host "`nNo new orders found in the specified date range." -ForegroundColor Yellow
        if ($MergeData -and $existingData.Count -gt 0) {
            Write-Host "  Existing data file is up to date: $MergeData" -ForegroundColor Green
        }
        exit 0
    }
    
    # Step 3: Aggregate by day
    $dailyAggregation = Get-OrderAggregationByDay -Orders $orders
    
    if ($dailyAggregation.Count -eq 0) {
        Write-Host "No aggregated data to display." -ForegroundColor Yellow
        if ($MergeData -and $existingData.Count -gt 0) {
            Write-Host "  Existing data file is up to date: $MergeData" -ForegroundColor Green
        }
        exit 0
    }
    
    # Display results
    Write-Host "`n=== Daily Order Summary ===" -ForegroundColor Yellow
    
    # Format the output with currency
    $dailyAggregation | Select-Object Date, OrderCount, @{
        Name = 'SumOrderValue'
        Expression = { $_.SumOrderValue.ToString('C2') }
    } | Format-Table -AutoSize
    
    # Export structured data for SQL insertion
    $sqlReadyData = $dailyAggregation | ForEach-Object {
        [PSCustomObject]@{
            ProjectId = $ProjectId
            DeploymentId = $deployId
            Date = $_.Date
            OrderCount = $_.OrderCount
            SumOrderValue = $_.SumOrderValue
            TotalQuantity = $_.TotalQuantity
            Symbols = $_.Symbols
            ImportedAt = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
        }
    }
    
    # Summary
    Write-Host "`nSummary:" -ForegroundColor Green
    Write-Host "  Total days with orders: $($sqlReadyData.Count)" -ForegroundColor White
    Write-Host "  Total orders processed: $(($sqlReadyData | Measure-Object -Property OrderCount -Sum).Sum)" -ForegroundColor White
    Write-Host "  Total ProfitLoss: $(($sqlReadyData | Measure-Object -Property SumOrderValue -Sum).Sum)" -ForegroundColor White
    Write-Host "  Date range: $($sqlReadyData[0].Date) to $($sqlReadyData[-1].Date)" -ForegroundColor White
    
    # Update database if requested
    if ($UpdateDB) {
        $dbUpdateSuccess = Update-SQLDatabase `
            -Data $sqlReadyData `
            -ServerInstance $SqlServer `
            -DatabaseName $Database `
            -Username $SqlUsername `
            -Password $SqlPassword
        
        if (-not $dbUpdateSuccess) {
            Write-Warning "Database update encountered errors. Please check the logs above."
        }
    }
    
    # Export to CSV if requested
    if ($ExportCsv) {
        $csvData = $sqlReadyData | Select-Object Date, @{Name='ProfitLoss'; Expression={$_.SumOrderValue}}
        $csvData | Export-Csv -Path $ExportCsv -NoTypeInformation -UseQuotes Never -Force
        Write-Host "`nExported to CSV: $ExportCsv" -ForegroundColor Green
    }
    
    # Merge data if requested
    if ($MergeData) {
        Write-Host "`nMerging data..." -ForegroundColor Cyan
        
        # Use detected column names or defaults for new file
        $dateCol = $existingDateColumn
        $plCol = $existingPLColumn
        
        # Create a hashtable for deduplication (normalized date as key, new data wins)
        $mergedHash = @{}
        
        # Add existing data first (using detected column names)
        foreach ($row in $existingData) {
            $dateValue = $row.$dateCol
            # Normalize date to yyyy-MM-dd for consistent key comparison
            $dateKey = ([DateTime]::Parse($dateValue)).ToString('yyyy-MM-dd')
            $mergedHash[$dateKey] = [ordered]@{
                $dateCol = $dateKey  # Use normalized format
                $plCol = $row.$plCol
            }
        }
        
        # Add/overwrite with new data (only count as updated if value actually changed)
        $updatedCount = 0
        $addedCount = 0
        foreach ($record in $sqlReadyData) {
            $dateKey = $record.Date  # Already in yyyy-MM-dd format
            if ($mergedHash.ContainsKey($dateKey)) {
                # Only count as update if the P/L value actually changed
                $existingPL = [double]$mergedHash[$dateKey].$plCol
                if ($existingPL -ne $record.SumOrderValue) {
                    $updatedCount++
                }
            } else {
                $addedCount++
            }
            $mergedHash[$dateKey] = [ordered]@{
                $dateCol = $dateKey
                $plCol = $record.SumOrderValue
            }
        }
        
        # Only write file if there are actual changes
        if ($addedCount -gt 0 -or $updatedCount -gt 0) {
            # Sort by date and export
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
    }
    
    # Return data object (can be piped to SQL insertion script)
    return $sqlReadyData
}
catch {
    Write-Error "Script execution failed: $_"
    exit 1
}
