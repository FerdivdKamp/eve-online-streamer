Implement end-to-end observation processing latency monitoring for the EVE market alert pipeline.

**Goal:** Demonstrate that at least 95% of live observations are evaluated by Flink within 5 seconds of their producer `event_timestamp`.

Requirements:

1. Inspect the existing Flink version, Python DataStream API, metrics reporter, Prometheus configuration and Grafana dashboard.
2. Measure latency immediately after each observation's evaluation, including observations that do not trigger alerts.
3. Include producer-to-Redpanda waiting time by measuring `evaluation_completed_utc - event_timestamp`.
4. Implement processed-event counters and an SLA-success counter (`latency <= 5 seconds`).
5. Implement latency distribution metrics for p50/p95/p99. Be aware that Flink's Prometheus reporter exports Flink histograms as summaries rather than classic Prometheus bucket histograms. Do not assume `_bucket` metrics exist.
6. Prefer a low-complexity solution consistent with the current stack. Ensure metrics can be aggregated across Flink subtasks, particularly the 5-second SLA percentage.
7. Exclude explicitly marked backfills/replays from the live SLA and count them separately.
8. Track invalid timestamps, future timestamps beyond clock-skew tolerance, and stale observations separately. Document the classification rules.
9. Add Grafana panels for:
   - Percentage processed within 5 seconds, over a rolling 5-minute window.
   - p95 processing latency in seconds.
   - Total live observations processed over the same window.
   - Optional Kafka consumer lag for diagnosing bottlenecks.
10. Avoid reporting healthy SLA status when no observations have been processed. Prefer No Data or an explicit idle state.
11. Add tests for latency calculation, the exact 5-second boundary, invalid timestamps, backfills, and normal observations without alerts.
12. Document the latency measurement boundaries and explicitly distinguish Flink evaluation latency from alert delivery latency.

Keep this as an extension of the existing Flink → Prometheus → Grafana monitoring infrastructure. Do not introduce a database or additional Kafka topic solely for metrics.