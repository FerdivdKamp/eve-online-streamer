# Stream processor

The repository's `processor/README.md` has the full PowerShell runbook. From the repository root, start and submit with:

```powershell
docker compose -f infra/compose.yml up -d --build
docker exec eve-redpanda rpk topic create market-observations -p 3 -r 1
docker compose -f infra/compose.yml exec jobmanager flink run -d -py /opt/processor/run_job.py
docker compose -f infra/compose.yml logs -f taskmanager
```

Create the topic only if it does not exist. Open <http://localhost:8081> to inspect the running job and completed checkpoints. Publish `producer/examples/sample_event.json` with the producer's `--kafka-only` mode to test without the mock API. Use `flink stop --savepointPath file:///opt/flink/state/savepoints <JOB_ID>` to stop the job with restorable state.

## Architecture

The **JobManager** schedules the job, coordinates checkpoints, and serves the Web UI. The **TaskManager** executes source and Python operators. A task slot is a unit of TaskManager scheduling capacity. This local setup provides three slots and sets parallelism to three, allowing one Kafka source reader per partition of `market-observations`. Redpanda is reached at `redpanda:9092` within the Compose network.

### Reading the job graph

A **record** is one market observation. A **task** is a running instance of part of the pipeline; it keeps processing records until the job stops. **Parallelism 3** means Flink runs three instances of each parallel stage. Flink chains adjacent operators into a single task when it can, so the Web UI shows two stages with three tasks each:

| Stage in the Web UI | Tasks | Purpose |
| --- | ---: | --- |
| Source, validation, and invalid-record output | 3 | Read the three Kafka partitions and classify each record. |
| Keyed valid-record output | 3 | Log valid records after `key_by` redistributes them. |

That is why the job shows **6 running tasks**. The `key_by` boundary sends observations for the same `(region_id, item_id)` to the same downstream task. A task slot is scheduling capacity, not a record or a Kafka partition. Flink can share a slot between tasks from the same job, so six running tasks fit in the three configured slots.

The PyFlink **DataStream API** exposes source, validation, `key_by`, and later stateful operators directly. Table API and SQL would be good for declarative transformations, but DataStream makes the intended per-item state and invalid-record branch clear. PyFlink keeps the code consistent with the producer and consumer. Python operator overhead is accepted for V1; profile it before considering SQL or JVM operators. The eventual end-to-end alert target is under five seconds.

The KafkaSource reads all partitions, validates JSON, and routes malformed records to diagnostic output. The existing incomplete manual Tritanium message is reported as invalid. Valid records are keyed by `(region_id, item_id)`, matching the producer's Kafka key, then logged. API `avg_price` and `price_change` are retained in the contract but will not form the future anomaly baseline.

## Time, state, and replay

Event time is the time encoded in an observation; processing time is when Flink executes an operator. `event_timestamp` is the intended event-time reference. API `source_timestamp` is unreliable for history. V1 has no windows and emits no watermarks. Future event-time windows need a measured out-of-order tolerance, a policy for late events, and idle-partition handling.

Checkpoints periodically capture Flink state and Kafka source positions for automatic recovery. Savepoints are explicit retained snapshots for planned stop and restore. Kafka group offsets are only a starting option for a new job with no restored Flink state. Replay starts a fresh job at chosen Kafka offsets and rebuilds state from retained messages. Restored checkpoint or savepoint offsets take precedence over the configured starting policy. History outside Kafka retention needs an archived raw-event dataset; the mock API cannot backfill missing history. Future replay alerts should go to an isolated topic, and state reconstruction can run without live notifications.
