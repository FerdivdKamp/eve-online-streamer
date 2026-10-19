# Publish ten 100 ISK observations followed by a threshold-boundary drop.
# Run from the repository root after starting Redpanda and the V2 Flink job.
$itemId = Get-Random -Minimum 1000000000 -Maximum 2000000000
$runId = [guid]::NewGuid().ToString('N')
$baseTime = [datetimeoffset]::UtcNow.AddSeconds(-12)
$dropPercent = if ($env:PROCESSOR_DROP_THRESHOLD_PERCENT) { [double]$env:PROCESSOR_DROP_THRESHOLD_PERCENT } else { 30.0 }
$dropPrice = 100.0 - $dropPercent
$lines = for ($index = 0; $index -le 10; $index++) {
    $timestamp = $baseTime.AddSeconds($index).ToString('o')
    @{
        item_id = $itemId
        item_name = 'Alert quick test'
        current_price = if ($index -eq 10) { $dropPrice } else { 100.0 }
        avg_price = 100.0
        price_change = if ($index -eq 10) { -$dropPercent } else { 0.0 }
        volume = 100
        source_timestamp = $timestamp
        fetched_at = $timestamp
        event_timestamp = $timestamp
        source_event_id = "$runId-$index"
        location = @{
            region_id = 10000002
            region_name = 'The Forge'
            station_id = 60003760
            station_name = 'Jita'
        }
    } | ConvertTo-Json -Compress -Depth 5
}
$lines | docker exec -i eve-redpanda rpk topic produce market-observations
if ($LASTEXITCODE -ne 0) { throw "Publishing example observations failed with exit code $LASTEXITCODE" }
Write-Host "Published 11 observations for region 10000002, item $itemId. Look for a -$dropPercent% alert on market.alerts."
