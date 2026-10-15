# Implement Apache Flink Stream Processor — V1

## Context

We are building an EVE Online market anomaly-detection platform as a hands-on streaming data engineering project.

The existing architecture is:

```text
Mock Market REST API
        |
        v
Python Producer
        |
        v
Redpanda: market-observations
        |
        v
Apache Flink (TO IMPLEMENT)
        |
        v
Redpanda: market-alerts (future milestone)
```

The producer, Redpanda broker, and standalone debugging consumer are already implemented and operational.

The repository contains:

- `producer/` — Python market API poller and Kafka producer
- `consumer/` — Python Kafka debugging consumer
- `analysis/` — API exploration
- `infra/compose.yml` — Docker Compose with Redpanda and Redpanda Console
- `docs/` — MkDocs documentation

Inspect the existing repository before making changes. Follow its conventions and reuse the established event schema wherever appropriate.

## Goal

Implement **Apache Flink using the PyFlink DataStream API**.

The first milestone is a functional, continuously running Flink job that consumes market observations from Redpanda, deserializes and validates them, and logs the observations.

Do not implement rolling-average calculations or alert publishing yet. Establish a reliable, testable foundation first.

## 1. Project structure

Create a new `processor/` directory as an independently managed Python project, consistent with `producer/` and `consumer/`.

Expected structure (adapt where appropriate):

```text
processor/
  pyproject.toml
  README.md
  src/
    eve_processor/
      __init__.py
      config.py
      job.py
      models.py
      processing.py
  tests/
```

Use Poetry for dependency management.

Use a supported and mutually compatible Python, Java, Flink, and PyFlink version combination. Verify compatibility before choosing versions; do not assume the existing Python 3.13 environment is supported.

Use Google-style docstrings consistent with the existing MkDocs setup.

## 2. Docker Compose integration

Extend `infra/compose.yml` with:

- Flink JobManager
- Flink TaskManager
- Flink Web UI exposed on `localhost:8081`
- Appropriate network connectivity to Redpanda
- Required configuration for executing PyFlink jobs

Keep the existing Redpanda and Console services working.

Pin compatible container versions rather than using `latest`.

Use named volumes or bind mounts where appropriate, but do not introduce unnecessary persistent volumes.

Add educational comments explaining the role of the JobManager, TaskManager, task slots, parallelism, and relevant configuration.

The Flink containers should access Redpanda using `redpanda:9092`, not `localhost:19092`.

Provide a repeatable procedure to start the infrastructure and submit the processor job.

## 3. Kafka / Redpanda source

Consume from:

```text
Topic: market-observations
Bootstrap servers: redpanda:9092
```

Use the Flink KafkaSource API, not a separate `confluent-kafka` polling loop.

Pay attention to the required Kafka connector JAR and compatibility with the chosen Flink version. Include the connector in the reproducible runtime setup.

For the initial job, explicitly configure an appropriate starting-offset policy and document it.

Avoid hardcoding the source offset to the beginning on every restart.

Ensure records from all three Kafka partitions can be consumed.

## 4. Observation schema

Reuse or reproduce the existing producer's validated event contract:

- `item_id`
- `item_name`
- `current_price`
- `avg_price`
- `price_change`
- `volume`
- `source_timestamp`
- `event_timestamp`
- `fetched_at`
- `location.region_id`
- `location.region_name`
- `location.station_id`
- `location.station_name`

Do not use the API-provided `avg_price` or `price_change` for our future anomaly calculations. We will calculate our own baselines.

Preserve the market observation's identity, original timestamps, and Kafka metadata where useful.

Validate incoming JSON with clear error reporting.

We know the topic contains at least one deliberately incomplete manual test message: Tritanium at partition 1, offset 0, without `volume` and other required fields.

The Flink job must not crash indefinitely on this known test record. Implement a basic policy for malformed messages that logs or routes them separately without pretending they are valid observations.

Do not silently substitute default values for required fields.

## 5. Processing pipeline

Implement a minimal DataStream pipeline:

```text
KafkaSource
    |
    v
Deserialize JSON
    |
    v
Validate observation
    |
    +---- Invalid observation → diagnostic output
    |
    v
Key by (region_id, item_id)
    |
    v
Log valid observation
```

The key must identify a market item within a region, matching the existing producer's key semantics.

Implement meaningful logging without dumping excessively large payloads by default.

Make the source and processing logic sufficiently modular to introduce keyed stateful processing later.

## 6. Checkpointing and recovery

Enable Flink checkpointing with a reasonable development configuration.

Use durable checkpoint storage, rather than relying exclusively on transient TaskManager memory or local temporary files.

Document the difference between:

- Flink checkpoints
- Flink savepoints
- Kafka consumer offsets
- Replaying Kafka topic history
- Rebuilding state from scratch

Ensure the deployment documentation distinguishes a restart from existing checkpointed state from a deliberate fresh replay.

Do not assume `auto.offset.reset=earliest` overrides restored Flink checkpoint state.

## 7. Event-time design

Our producer adds:

- `source_timestamp` — supplied by the mock API; potentially randomized or out of order
- `fetched_at` — when the producer retrieved the observation
- `event_timestamp` — when the producer published the observation

For V1, use `event_timestamp` as the intended event-time reference for future calculations.

Do not use `source_timestamp` as an authoritative historical market timestamp.

Document how event-time extraction, watermarks, allowed out-of-order arrivals, and idle Kafka partitions will eventually be handled.

If implementing timestamps or watermarks now, make the behavior explicit and testable. Do not introduce arbitrary watermark delays without explanation.

## 8. Historical replay and backfill

Document the following architectural decisions.

### Kafka replay

Previously captured observations can be reprocessed while they remain in Redpanda retention.

After fixing a processing bug, we should be able to launch a fresh processor using new state and read historical messages from an explicitly selected starting point.

Avoid accidentally reusing old checkpoints when performing a deliberate full replay.

Future replay-generated alerts should go to a separate or versioned topic to avoid duplicate production notifications.

### Historical backfill

In production systems, historical batch jobs may publish the same event contract as real-time producers, allowing the same stream-processing pipeline to be reused.

However, our current mock market API returns randomized observations and does not offer reliable historical snapshots.

Therefore, **we cannot reconstruct missing market history by querying the current API**.

Do not implement a fictional historical API backfill.

A future batch producer could instead republish events from an archived raw-event dataset, preserving their original observation timestamps.

### Rolling-average implications

Historical observations arriving after newer observations must not blindly modify the current rolling average.

Our future processor must distinguish event time from ingestion time and define appropriate rules for late events.

We may later introduce ingestion metadata such as:

```json
{
  "ingestion": {
    "mode": "backfill",
    "batch_id": "example-batch-id"
  }
}
```

Do not make this a required field in V1 or break existing producer messages.

Keep state reconstruction and alert emission as separate concerns. Reprocessing history may rebuild rolling state without generating live notifications.

## 9. Testing

Implement automated tests for:

- Valid market observation deserialization
- Missing required fields
- Invalid field types
- Timestamp parsing
- Market-item/region key extraction
- Handling malformed JSON
- Invalid-event handling
- Configuration validation

Use unit tests for pure processing logic wherever possible.

Include a small integration or smoke-test procedure demonstrating:

1. Start the infrastructure.
2. Submit the Flink job.
3. Verify the job appears in the Flink Web UI.
4. Publish observations using the existing producer.
5. Verify Flink processes them.
6. Confirm an invalid message does not crash the job.
7. Inspect checkpoint status.
8. Stop the job cleanly.

Do not require the mock API to be available for unit tests.

## 10. Documentation

Update the MkDocs navigation and add documentation explaining:

- Flink architecture: JobManager, TaskManager, slots and parallelism
- DataStream API and our choice over Table API / SQL
- KafkaSource and partition consumption
- Keyed streams
- Event time versus processing time
- Checkpoints, savepoints and replay
- Historical backfill limitations
- How to run and debug the processor locally

Document Windows/PowerShell commands where relevant.

### Technology decision: PyFlink

We deliberately choose PyFlink for development speed, consistency with the existing Python components, and readability.

Python processing overhead is accepted for V1.

Record that a potential future optimization is replacing Python operators with Flink SQL or JVM-native operators if profiling identifies Python execution as a bottleneck.

The eventual end-to-end processing and alert latency target is under five seconds.

## 11. Acceptance criteria

The implementation is complete when:

- Redpanda and Console still start successfully.
- Flink JobManager and TaskManager start successfully.
- Flink Web UI is accessible at `localhost:8081`.
- The PyFlink job can be submitted reproducibly.
- The job consumes real messages from `market-observations`.
- Valid market observations are processed and logged.
- Invalid records are identifiable and handled without causing a permanent restart loop.
- All three Kafka partitions are accessible.
- Checkpointing is functional and its storage is configured.
- Unit tests pass.
- Documentation explains setup, execution, and recovery.

## Scope boundaries

Do not implement yet:

- Rolling averages or standard deviation
- Price-drop anomaly detection
- Alert publishing
- Dashboard development
- Kubernetes deployment
- Historical API backfill
- Complex dead-letter infrastructure

Those belong to subsequent milestones.

Prioritize a working, understandable implementation over an elaborate architecture.

When finished, provide a summary of the files changed, the design decisions made, the commands to run, and any remaining limitations.