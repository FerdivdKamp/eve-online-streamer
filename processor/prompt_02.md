# Implement EVE market rolling average in PyFlink

## Context

We have a working Docker Compose development environment with:

- A Python mock market API and producer.
- Redpanda as the Kafka-compatible streaming broker.
- Apache Flink 2.2.1, using the PyFlink DataStream API.
- A Flink job that already consumes messages from Redpanda.

The rolling average and alert detection are not implemented yet.

Inspect the existing repository, data contracts, Docker configuration, Flink entry points and tests before making changes. Reuse existing structures and dependencies where practical.

## Objective

Implement a stateful, seven-day rolling average for EVE Online market prices, using Flink's managed keyed state, and publish qualifying price-drop alerts to a dedicated Redpanda topic.

### 1. Stream processing

- Preserve the existing Redpanda source and consumer configuration.
- Deserialize and validate incoming market events.
- Partition the stream using the relevant market identity: region, item type and any necessary market-side or order-type dimensions.
- Use a configurable seven-day rolling window based on event timestamps.
- Maintain rolling state using Flink-managed keyed state, not global Python dictionaries.
- Avoid storing unlimited historical observations.
- Calculate the average from valid observations within the window.
- Compare each new price against the historical average **before** including that event.
- Update the rolling state after evaluating the event.
- Provide a configurable minimum observation count before alerts become eligible.

Rolling average could be kept as hourly buckets of sums

```JSON
{
    "region_id": 10000002,
    "item_id": 34,
    "buckets": {
        "2026-10-16T10": {"sum": 2500, "count": 50},
        "2026-10-16T11": {"sum": 1800, "count": 40},
        "total": {"sum": 4300, "count": 90}
    }
}
```

and calculate the weighted rolling average from that 

### 2. Alert calculation

Implement a configurable percentage-drop threshold, initially 20%.

For example:

- Historical rolling average: 100 ISK.
- Incoming price: 75 ISK.
- Price deviation: -25%.
- Result: price-drop alert.

Do not alert when there is insufficient history.

Publish qualifying alerts to a dedicated Redpanda topic such as `market.alerts`.

Include in the alert payload:

- Stable alert ID.
- Item type ID and region ID.
- Current price and previous rolling average.
- Percentage deviation.
- Event timestamp and processing timestamp.
- Window definition and observation count.
- Source event identifier where available.
- Schema version.

Preserve sufficient information to trace the alert back to the source event.

### 3. Time handling and replay

- Document the distinction between event time and processing time.
- Inspect the existing event timestamp contract and ordering guarantees.
- Support out-of-order events with an explicit, configurable lateness policy.
- Do not silently corrupt rolling statistics when older events arrive.
- Identify duplicate events and design appropriate idempotency where possible.
- Distinguish live market observations from historical backfill/replay records using existing metadata or an explicit contract.
- Historical backfills must not unexpectedly generate live alerts or corrupt live rolling state.
- Normal recovery from Flink checkpoints should preserve consistent processing.
- Document the selected late-event and backfill policies before implementing them.

### 4. Fault tolerance

- Enable and configure Flink checkpointing.
- Store checkpoints in a persistent Docker volume or other suitable durable location.
- Preserve existing Kafka offset management through Flink.
- Document what happens when the TaskManager or the entire Docker environment restarts.
- Avoid claiming end-to-end exactly-once delivery unless the Kafka sink configuration and checkpointing guarantees actually provide it.
- Prefer deterministic alert IDs to support downstream deduplication.

### 5. Code structure and testing

Separate pure Python business logic from Flink-specific orchestration.

Use independently testable functions/classes for:

- Window calculation and expiry.
- Price average calculation.
- Percentage deviation.
- Alert eligibility and payload creation.

Add unit tests covering:

- First event / no history.
- Insufficient observations.
- Exactly-on-threshold behavior.
- A clear price drop.
- Gradually declining prices.
- Expiry of old observations.
- Multiple independent item/region keys.
- Duplicate and out-of-order events.
- Malformed messages.
- Recovery and replay behavior where integration tests are practical.

Keep the implementation readable. Do not introduce unnecessary frameworks or infrastructure.

### 6. Deliverables

1. Working PyFlink rolling-average implementation.
2. Redpanda alert topic and sink configuration.
3. Configuration through environment variables or the existing configuration approach.
4. Unit tests and practical integration tests.
5. Updated documentation describing state, windows, replay and recovery.
6. A short description of architectural decisions, tradeoffs and assumptions.
7. Instructions to run the pipeline and inspect emitted alerts.

Implement incrementally, preserving the existing working producer-to-Flink pipeline. Run the relevant tests and report any failures or limitations.