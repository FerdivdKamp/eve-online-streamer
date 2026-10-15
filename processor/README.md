# EVE market processor

This PyFlink DataStream job reads `market-observations` from Redpanda, validates the producer's JSON contract, keys valid observations by `(region_id, item_id)`, and prints concise valid and invalid diagnostics to TaskManager logs. It does not calculate anomalies or publish alerts.

## Versions and setup

The runtime image pins Flink/PyFlink 2.2.1, Java 17, and Python 3.12. Its Ubuntu 24.04 base supplies Python 3.12. The image includes the matching shaded Kafka connector `flink-sql-connector-kafka-5.0.0-2.2.jar` and installs the processor in a virtual environment on both Flink nodes. For local Poetry work, select Python 3.12. PyFlink 2.2.1 does not publish Python 3.13 wheels; the repository's Python 3.13 environment cannot install this processor's runtime dependencies.

From the repository root in PowerShell:

```powershell
docker compose -f infra/compose.yml config
docker compose -f infra/compose.yml up -d --build
docker exec eve-redpanda rpk topic create market-observations -p 3 -r 1
docker compose -f infra/compose.yml exec jobmanager flink run -d -py /opt/processor/run_job.py
docker compose -f infra/compose.yml exec jobmanager flink list
docker compose -f infra/compose.yml logs -f taskmanager
```

If the topic already exists, skip topic creation. Open <http://localhost:8081> for the Flink job and Checkpoints tab; Redpanda Console is at <http://localhost:8080>. The job reads all three partitions with parallelism 3 and three available TaskManager slots. Submit once; submitting the same command again creates another running job.
If the `print()` sink lines do not appear in `docker compose logs`, inspect the TaskManager stdout file with `docker compose -f infra/compose.yml exec taskmanager sh -c 'tail -n 100 /opt/flink/log/*taskexecutor*.out'`.

Publish a known valid event without the mock API:

```powershell
cd producer
poetry run eve-producer --kafka-only --event-file examples/sample_event.json
cd ..
```

TaskManager logs should contain `VALID region=10000002 item=34`. The existing incomplete Tritanium event at partition 1 offset 0 appears as `INVALID observation` if the selected starting offset reaches it. A new malformed test value can be published from PowerShell:

```powershell
'{"item_id":34}' | docker exec -i eve-redpanda rpk topic produce market-observations
docker compose -f infra/compose.yml logs --tail 100 taskmanager
```

The job stays `RUNNING` after an invalid record. Inspect the Web UI Checkpoints tab for completed checkpoints. To stop cleanly with a savepoint, obtain the job ID from `flink list`, then run:

```powershell
docker compose -f infra/compose.yml exec jobmanager flink stop --savepointPath file:///opt/flink/state/savepoints <JOB_ID>
```

To resume that state, submit with `flink run -d -s <SAVEPOINT_PATH> -py /opt/processor/run_job.py`. Both nodes mount the `flink-state` named volume, so the savepoint path remains accessible after container recreation. Do not delete the volume while keeping state.

## Offsets and recovery

By default `PROCESSOR_START_FROM=committed`: a new job starts from committed group offsets, or the earliest retained offset when none exists. `PROCESSOR_GROUP_ID` defaults to `eve-market-processor-v1`. This starting rule applies only when no Flink state is restored. During failure recovery, Flink restores Kafka source offsets from its checkpoint. Kafka group commits are a convenience for new jobs, not the job's recovery state. A canceled job without a savepoint cannot be resumed from its internal checkpoints by this procedure; use a savepoint for planned stops.

For a deliberate fresh replay, stop the current job, set a new group ID and `PROCESSOR_START_FROM=earliest` when submitting, and omit `-s`. For example:

```powershell
docker compose -f infra/compose.yml exec -e PROCESSOR_GROUP_ID=eve-market-replay-001 -e PROCESSOR_START_FROM=earliest jobmanager flink run -d -py /opt/processor/run_job.py
```

This reads only data still within Redpanda retention. `latest` starts at the current end of each partition and waits for new records. Do not point replay-generated alerts at a live notification topic in a future milestone; use a separate or versioned topic. Rebuilding rolling state and emitting notifications are separate operations.

Flink checkpoints are periodic recovery snapshots and include source positions. Savepoints are explicit, retained snapshots for planned stop and restore. Kafka offsets mark positions within each topic partition. A fresh replay discards prior Flink state and chooses a new Kafka starting point. Rebuilding state from scratch requires processing retained events again; an offset choice alone does not restore old state.

## Event time and limitations

The producer's `event_timestamp` is the intended event-time basis for future calculations. `fetched_at` measures retrieval time. The mock API's `source_timestamp` may be randomized or out of order and is kept for provenance, not treated as reliable historical market time. V1 uses `no_watermarks` because it has no time windows. A later windowed job needs an explicit event-time timestamp assigner, tested watermark delay based on observed lateness, a late-event policy, and idleness detection so quiet partitions do not hold back watermarks. Historical events arriving after newer ones must follow that late-event policy before modifying rolling averages.

The mock API cannot reconstruct missing history: it returns randomized current observations and has no reliable historical snapshots. A future batch producer can republish archived raw events using the same contract and original timestamps. Optional ingestion metadata such as `{"ingestion":{"mode":"backfill","batch_id":"..."}}` can be added without changing V1's required fields.

V1 uses value-only Kafka deserialization, so partition, offset, and Kafka key are not carried into diagnostic lines. Invalid lines include a short payload preview, a fingerprint, and validation errors for identification. The producer's region/item identity and all event timestamps remain in the validated observation. The printed output is for development and can be repeated around failure recovery; it is not an exactly-once external sink.

## Unit tests

```powershell
cd processor
poetry env use 3.12
poetry install
poetry run python -m unittest discover -s tests -v
```
