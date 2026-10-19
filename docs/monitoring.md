# Monitoring

`docker compose -f infra/compose.yml up -d --build` starts the mock API, Prometheus, and Grafana with the streaming stack. The producer stays separate so the demo can begin publishing observations on cue. Start it with the command in the [Quickstart](quickstart.md).

Prometheus is at <http://localhost:9090/targets>. It scrapes Redpanda `/public_metrics` on port 9644 and the Flink JobManager and TaskManager Prometheus reporters independently on port 9249, every five seconds. Grafana is at <http://localhost:3000/> with local development credentials `admin` / `admin`. The provisioned **EVE Streaming Pipeline Overview** dashboard shows broker and Flink availability, topic throughput, processor lag, record rates, busy time, checkpoints, disk free space, the alert topic's high watermark, and live observation evaluation latency. Prometheus and Grafana data persist in Docker volumes; Prometheus keeps seven days.

The alert high watermark counts writes to `market.alerts`, including any at-least-once duplicates. It is a useful trend indicator, not a list of alert payloads. Use [Redpanda Console](http://localhost:8080/) to inspect actual alerts. Grafana does not display individual alert records.

## Observation evaluation latency

The three latency panels use a rolling five-minute window. **Live observations within 5s** divides the number completed within five seconds by all valid live observations evaluated in that window. The target is at least 95%. When no live observations arrive, the percentage has no denominator and Grafana shows **No Data**; the adjacent count panel makes the idle state visible. The p95 panel estimates the 95th percentile from explicit cumulative latency counters. The same query can use `histogram_quantile(0.50, ...)` or `histogram_quantile(0.99, ...)` for p50 and p99. Flink's reporter adds `le` to the bucket metric name and sanitizes `+Inf` to `_Inf` and decimal points to underscores; the panel query restores those label values before calculating the percentile. Buckets are aggregated across all Flink subtasks before calculating a percentile. Percentile estimates are approximate between bucket boundaries; each event's five-second pass/fail decision is exact.

Prometheus `increase()` extrapolates counter changes to the edges of the five-minute window. A short burst can therefore make the displayed processed count slightly higher than the integer number of records actually handled. A burst before the first scrape of a new job can also be undercounted. The SLA percentage uses the same five-minute estimator for both counters. The panel queries select only the currently active job ID, so a stopped job's samples cannot leak into the new window after a restart. For an exact cumulative count since the current job started, inspect `sum(flink_taskmanager_job_task_operator_observation_live_processed_total)` in Prometheus.

An already running Flink job must be restarted on the updated processor image before these metrics appear. Follow the [V2 savepoint upgrade steps](../processor/README.md#deploying-a-later-v2-update) to retain the rolling baseline.

The processor sets PyFlink's bundle wait to 100 ms by default (`PROCESSOR_PYTHON_BUNDLE_TIME_MS`), down from Flink's 1000 ms default. This reduces waiting between Python stages at low event rates, with some throughput overhead. Adjust the setting only after measuring both latency and throughput under the expected load.

Latency is measured as `UTC time immediately after Flink evaluation and state update - event_timestamp`. The producer sets `event_timestamp` before queuing its Redpanda write, so this includes producer queueing, broker waiting, Flink source and evaluation time. It does **not** measure the Kafka alert sink, broker acknowledgement, or downstream alert delivery. The existing `processing_timestamp` in an alert is taken at evaluation start and is not used for this metric. Both clocks should be synchronized for meaningful results.

Every valid live observation counts in the SLA denominator, including observations without alerts, duplicates, and stale observations that Flink skips from its rolling baseline. Stale observations (`> PROCESSOR_MAX_EVENT_AGE_SECONDS` old at evaluation start) count as slow failures and have their own counter. Explicit `ingestion.mode=backfill` and `ingestion.mode=replay` records are counted separately and excluded. Events more than `PROCESSOR_MAX_FUTURE_SKEW_SECONDS` ahead of Flink's clock are counted separately and excluded because their latency is not trustworthy. Small negative differences within the allowed skew are treated as zero seconds. Invalid `event_timestamp`, `fetched_at`, or `source_timestamp` values, including timezone-less operational timestamps, are rejected before evaluation and counted as invalid timestamps; other validation failures have a separate counter. Metrics are process counters, so a recovery replay without an explicit marker can count an observation again.

To start only monitoring after the streaming services are running:

```powershell
docker compose -f infra/compose.yml up -d prometheus grafana
```

To stop only monitoring while leaving the pipeline running:

```powershell
docker compose -f infra/compose.yml stop prometheus grafana
```

Check all three scrape targets on Prometheus's Targets page. If Flink targets are down, check `docker compose -f infra/compose.yml logs jobmanager taskmanager`; port 8081 is the Flink UI, not the metrics endpoint. If Grafana panels are empty, check target health, select a time range with recent events, and generate a sample alert using `.\processor\tools\send_alert_example.ps1`. Some rates need at least two scrapes, and checkpoint metrics appear only after the Flink job runs. The lag panel uses the processor's committed group offsets, so it can lag behind live reads until a checkpoint commits offsets.

The latency panels measure producer-to-Flink evaluation, not observation-to-alert delivery. A delivery SLA would need a timestamp at an alert consumer or an acknowledgement from the alert sink. The mock API is not instrumented; its response time is outside the measurement boundary.
