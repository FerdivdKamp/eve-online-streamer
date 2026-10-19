# Stream processor

The PyFlink job reads validated observations from Redpanda, computes a rolling market price baseline in Flink managed keyed state, and writes qualifying drops to `market.alerts`. See the repository's `processor/README.md` for the full runbook, configuration table, and steps to replace a running V1 job.

## Run and inspect

From the repository root in PowerShell, build and start the services. Compose creates missing topics and submits the V2 job after Flink is ready. If V1 is currently running, stop it with a savepoint first as described in `processor/README.md`; its savepoint cannot initialize the V2 rolling state.

```powershell
docker compose -f infra/compose.yml up -d --build
```

Check that the job is running:

```powershell
docker compose -f infra/compose.yml exec jobmanager flink list
```

Open the [Flink Web UI](http://localhost:8081/) for the job graph and checkpoints. TaskManager logs contain `VALID`, `INVALID`, and skipped-record diagnostics. [Redpanda Console](http://localhost:8080/) shows the input and alert topics. Use `docker exec eve-redpanda rpk topic consume market.alerts -o end -n 1` in a separate terminal to wait for the next alert. The [Quickstart](quickstart.md) covers the full pipeline.

## Rolling baseline and alert rule

The default market key is region ID and item ID. `PROCESSOR_KEY_DIMENSIONS` can opt in to station ID, market side, or order type as additional key fields. Flink redistributes the stream by this key. Each active key has an hourly sum and count for the current UTC hour and the preceding 168 hours. The rolling average weights each observation equally. The incoming price is checked against the prior average **before** entering its hour bucket. The default alert threshold is a drop of at least 30%, with at least ten prior observations. Set `PROCESSOR_DROP_THRESHOLD_PERCENT` in the Compose environment to change it. An alert contains a stable ID, source and market identifiers, both prices, percentage deviation, event and processing times, the window definition, and the prior observation count.

Hourly buckets bound the price history to 169 entries per active key. They make expiry accurate to the hour: an observation at the oldest boundary can stay in the baseline for up to 59 minutes beyond an exact seven-day cutoff. Alert payloads distinguish the exact window start from the earliest retained bucket. The duplicate-ID cache is separately capped per key. Flink state TTL removes inactive keys after the window length plus one day of processing-time inactivity.

## Event time, late records, and replay

The producer sets `event_timestamp` when it publishes an observation. This is the rolling-window clock. API `source_timestamp` can be out of order and is kept only for provenance. The processor compares event time with processing time to reject old or future-dated live records. It also tracks the latest event time per key. An out-of-order event within the configured lateness limit updates the baseline but does not generate a retroactive alert; an older one is discarded. This is an arrival-time policy without Flink watermark buffering. The job does not revise an earlier alert after a late arrival.

An optional `source_event_id` identifies a record across retries. Without one, a hash of the validated event serves as its ID. Exact duplicates found in the bounded cache are skipped. Historical records marked `ingestion.mode=backfill` are skipped by the live job, so they cannot change its rolling state or emit live alerts. Rebuild historical state in an isolated job and keep live alert output disabled. Old Kafka data is also subject to the live-event age limit, so restarting with an empty state does not silently treat stale replay as today's market.

## Recovery and delivery

Flink checkpoints include keyed state and Kafka source positions; the local Compose setup stores them in the persistent `flink-state` volume. A running job can recover from a TaskManager failure using a completed checkpoint. For a planned stop, use `flink stop --savepointPath file:///opt/flink/state/savepoints <JOB_ID>`, then restore with `flink run -d -s <SAVEPOINT_PATH> -py /opt/processor/run_job.py`. A whole-stack restart automatically submits a fresh job using committed Kafka offsets; it does not restore rolling state. For exact state continuity, start only `jobmanager` and `taskmanager`, restore from a compatible V2 savepoint, then run the remaining Compose services. V1 savepoints are not compatible with the new topology.

The Kafka alert sink uses **at-least-once** delivery. Recovery around a checkpoint may publish the same alert again; its deterministic `alert_id` lets downstream consumers deduplicate. Fresh jobs without restored state use the configured Kafka group offset policy (`committed` by default, falling back to earliest). A Kafka offset alone does not restore a rolling baseline.

## Deploy an update

Stop the running job with a savepoint **before** replacing the Flink containers. For a compatible V2 code change, restore from the reported savepoint path:

```powershell
docker compose -f infra/compose.yml exec jobmanager flink list
docker compose -f infra/compose.yml exec jobmanager flink stop --savepointPath file:///opt/flink/state/savepoints <V2_JOB_ID>
docker compose -f infra/compose.yml up -d --no-deps --build --force-recreate jobmanager taskmanager
docker compose -f infra/compose.yml exec jobmanager flink run -d -s <V2_SAVEPOINT_PATH> -py /opt/processor/run_job.py
docker compose -f infra/compose.yml exec jobmanager flink list
```

The V1-to-V2 upgrade uses the same stop and rebuild steps, but submits V2 **without** `-s`: the V1 savepoint cannot initialize its new rolling state. Keep the V1 savepoint for rollback. Redpanda does not need a rebuild for a processor update. To change an existing three-partition `market.alerts` topic to one partition in a POC, delete and recreate only that topic; this discards its alerts but keeps `market-observations` and the broker volume. The full command sequence is in `processor/README.md`.
