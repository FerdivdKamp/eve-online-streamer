# EVE market processor

The PyFlink 2.2.1 DataStream job reads `market-observations`, validates market events, maintains a seven-day rolling price baseline in Flink keyed state, and publishes qualifying drops to `market.alerts`. Invalid records and decisions to skip records are logged by the TaskManager. The existing `VALID` diagnostic remains available.

## Run locally

Docker builds Flink/PyFlink 2.2.1 with Python 3.12 and the matching Kafka connector. If a V1 job is running, follow the upgrade note below before rebuilding. Run these commands from the repository root in PowerShell:

```powershell
docker compose -f infra/compose.yml up -d --build
docker exec eve-redpanda rpk topic list
```

Compose creates these topics automatically. If running without its `topics` service, create each topic only if it is missing:

```powershell
docker exec eve-redpanda rpk topic create market-observations -p 3 -r 1
docker exec eve-redpanda rpk topic create market.alerts -p 1 -r 1
```

Compose submits the market job automatically. Check its status:

```powershell
docker compose -f infra/compose.yml exec jobmanager flink list
```

The one-shot `job-submit` service waits for a TaskManager and skips submission if a market job is already active. For a manual submission after starting only the Flink services, use `docker compose -f infra/compose.yml exec jobmanager flink run -d -py /opt/processor/run_job.py` once. Open <http://localhost:8081/> for the job graph and checkpoints, <http://localhost:8080/> for Redpanda Console, and <http://localhost:3000/> for Grafana. Follow diagnostics with `docker compose -f infra/compose.yml logs -f taskmanager`. If Python `print()` lines are missing from Docker logs, inspect `/opt/flink/log/*taskexecutor*.out` inside the TaskManager.

To inspect alerts:

```powershell
docker exec eve-redpanda rpk topic consume market.alerts -n 1 -o start
```

The local Compose baseline requires three prior observations for the same region and item, and an alert requires the new price to be at least 30% below their average. A single sample event does not trigger one. For a repeatable integration check with a fresh item ID, run:

```powershell
.\processor\tools\send_alert_example.ps1
```

The script sends three 100 ISK observations followed by a 70 ISK observation at the default threshold. Its final line prints the item ID to look for in Redpanda Console. Set `PROCESSOR_DROP_THRESHOLD_PERCENT` in the invoking shell if the running processor uses another threshold. To watch the next alert on the command line, run `docker exec eve-redpanda rpk topic consume market.alerts -o end -n 1` in another terminal **before** running the script. The processor's pure calculation tests cover the same sequence without Kafka.

### Upgrading a running V1 job

Before rebuilding, get the V1 job ID and stop it with a savepoint:

```powershell
docker compose -f infra/compose.yml exec jobmanager flink list
docker compose -f infra/compose.yml exec jobmanager flink stop --savepointPath file:///opt/flink/state/savepoints <V1_JOB_ID>
```

Keep the reported V1 savepoint path for rollback only. There is no need to stop or rebuild Redpanda: its topics live in a separate volume. For a POC reset of an existing three-partition alert topic, delete **only that topic** and create it with one partition. This discards old alerts but retains `market-observations`:

```powershell
docker exec eve-redpanda rpk topic delete market.alerts
docker exec eve-redpanda rpk topic create market.alerts -p 1 -r 1
```

Then replace only the two Flink containers, submit V2 without the V1 savepoint, and verify the new job:

```powershell
docker compose -f infra/compose.yml up -d --no-deps --build --force-recreate jobmanager taskmanager
docker compose -f infra/compose.yml exec jobmanager flink run -d -py /opt/processor/run_job.py
docker compose -f infra/compose.yml exec jobmanager flink list
docker exec eve-redpanda rpk topic describe market.alerts
```

The V2 rolling baseline begins empty and becomes alert-eligible after the configured number of fresh observations per key. The source keeps the existing Kafka group and committed-offset policy; old records outside the live age limit are skipped. Do not run V1 and V2 simultaneously with the same group.

### Deploying a later V2 update

For a code change that preserves the V2 state schema and market key, stop the running job with a savepoint, rebuild only Flink, then restore that savepoint. Substitute the actual job ID and savepoint path reported by Flink:

```powershell
docker compose -f infra/compose.yml exec jobmanager flink list
docker compose -f infra/compose.yml exec jobmanager flink stop --savepointPath file:///opt/flink/state/savepoints <V2_JOB_ID>
docker compose -f infra/compose.yml up -d --no-deps --build --force-recreate jobmanager taskmanager
docker compose -f infra/compose.yml exec jobmanager flink run -d -s <V2_SAVEPOINT_PATH> -py /opt/processor/run_job.py
docker compose -f infra/compose.yml exec jobmanager flink list
```

Wait for a completed checkpoint in the Flink Web UI. Changes to the state schema, key shape, or operator topology may make the old savepoint incompatible; in that case, plan a fresh baseline and submit without `-s`. Do not reset Redpanda or the alert topic for an ordinary processor update.

## State, windows, and event identity

`event_timestamp` is the producer's publish time and the basis for the rolling window. `fetched_at` and the API's `source_timestamp` are provenance; the latter may be out of order. Processing time is used for freshness checks and recorded on alerts. The default Flink state key is `(region_id, item_id)`, matching the producer's `region_id:item_id` Kafka key. Set `PROCESSOR_KEY_DIMENSIONS` to a comma-separated selection of `station_id`, `market_side`, and `order_type` to split baselines further. For example, `PROCESSOR_KEY_DIMENSIONS=station_id,market_side`. Changing this setting changes keyed state identity; start a fresh baseline rather than restoring a savepoint made with a different key definition.

The job retains the current UTC hour and the preceding 168 hourly buckets by default. Each bucket stores a price sum and count. The average is weighted by observation count and is calculated **before** adding the incoming event. Bucket expiry is accurate to one hour, so a value at the oldest boundary may be retained for up to 59 minutes beyond an exact seven-day cutoff. At most `PROCESSOR_WINDOW_HOURS + 1` buckets are retained per active key. Alerts report both the exact window start and the earliest retained bucket. Flink-managed `ValueState` holds the buckets, latest event time, and a bounded set of seen observation IDs. An idle-key processing-time TTL is set to the window length plus one day.

The optional `source_event_id` is used for duplicate detection when present. Otherwise the job hashes the validated event; exact replays of the same value are suppressed. The seven-day ID cache has a configurable cap, so an evicted ID can be counted again under very high key volume. Alert IDs are deterministic hashes of the market key and observation ID. Alert payloads contain schema version, source and observation IDs, market identity, current price, previous average, signed percentage deviation, event and processing timestamps, window boundaries, and prior observation count. The Kafka sink provides **at-least-once** delivery. A crash near a checkpoint can duplicate an alert; consumers should deduplicate by `alert_id`.

## Late events, backfills, and replay

For each key, the job tracks the latest accepted event time. An event up to `PROCESSOR_ALLOWED_LATENESS_SECONDS` older than that time contributes to subsequent rolling baselines but does not emit a retroactive alert. Older events are skipped. The job does not buffer records behind Flink event-time watermarks; its lateness rule is applied on arrival. Live events more than `PROCESSOR_MAX_EVENT_AGE_SECONDS` behind processing time or more than `PROCESSOR_MAX_FUTURE_SKEW_SECONDS` ahead are also skipped. These checks prevent an old Kafka replay from silently changing the live baseline. They also mean an outage longer than the age limit causes backlog events to be skipped; adjust the limit deliberately if catch-up is needed.

Historical records can explicitly carry `{"ingestion":{"mode":"backfill"}}` or `{"ingestion":{"mode":"replay"}}`. The live job skips them entirely and counts them outside the live latency SLA. A historical rebuild should use an isolated group and state namespace and keep live alerts disabled; merely changing the Kafka starting offset is not a safe backfill procedure. Duplicate and late decisions are logged with a reason. The job does not revise alerts already emitted when a late event arrives.

Flink checkpoints capture Kafka source positions and keyed state. The `flink-state` Docker volume holds checkpoints and savepoints, and Redpanda's own volume retains broker data. A TaskManager failure can recover from a completed checkpoint while the job remains managed by Flink. For a planned stop, obtain the job ID with `flink list`, then use:

```powershell
docker compose -f infra/compose.yml exec jobmanager flink stop --savepointPath file:///opt/flink/state/savepoints <JOB_ID>
```

Resume from the reported path using `flink run -d -s <SAVEPOINT_PATH> -py /opt/processor/run_job.py`. A canceled job or a whole-stack restart does not automatically restore an arbitrary checkpoint; submit from a retained savepoint when continuity matters. Restore a V2 savepoint with the same state schema and key definition. Do not restore a V1 savepoint into this V2 topology. If starting fresh, Kafka's committed group offsets are used by default, falling back to earliest retained offsets when there are no commits. Replays require a separate group and alert destination or disabled alert output.

## Configuration

Set these environment variables on the `docker compose exec` command that submits the job, using `-e NAME=value` before `jobmanager`:

| Variable | Default | Meaning |
| --- | --- | --- |
| `PROCESSOR_BOOTSTRAP_SERVERS` | `redpanda:9092` | Broker inside Compose. |
| `PROCESSOR_TOPIC` | `market-observations` | Input topic. |
| `PROCESSOR_ALERT_TOPIC` | `market.alerts` | Alert output topic. |
| `PROCESSOR_GROUP_ID` | `eve-market-processor-v1` | Kafka group used for new starts. |
| `PROCESSOR_START_FROM` | `committed` | `committed`, `earliest`, or `latest` for new starts. |
| `PROCESSOR_PARALLELISM` | `3` | Flink operator parallelism. |
| `PROCESSOR_CHECKPOINT_MS` | `30000` | Checkpoint interval. |
| `PROCESSOR_PYTHON_BUNDLE_TIME_MS` | `100` | Maximum PyFlink bundle wait; lower values reduce latency but may reduce throughput. |
| `PROCESSOR_WINDOW_HOURS` | `168` | Number of hourly buckets. |
| `PROCESSOR_DROP_THRESHOLD_PERCENT` | `30` | Minimum drop from prior average; set in the Compose environment to change the processor and demo together. |
| `PROCESSOR_KEY_DIMENSIONS` | empty | Optional comma-separated extra baseline keys: `station_id`, `market_side`, `order_type`. |
| `PROCESSOR_MIN_OBSERVATIONS` | `3` in Compose (`10` in the processor package) | Prior observations required for an alert. Changing it requires a job restart; use a savepoint to keep the baseline. |
| `PROCESSOR_ALLOWED_LATENESS_SECONDS` | `600` | Accepted out-of-order delay per key. |
| `PROCESSOR_MAX_EVENT_AGE_SECONDS` | `900` | Maximum live-event age at processing. |
| `PROCESSOR_MAX_FUTURE_SKEW_SECONDS` | `60` | Maximum future event-time offset. |
| `PROCESSOR_MAX_DEDUP_ENTRIES` | `10000` | Per-key duplicate-ID cap. |

## Tests

The pure logic tests run without Flink using the available Python environment and Pydantic:

```powershell
python -m unittest discover -s processor/tests -v
```

For a local Poetry environment, select Python 3.12 before installing this package (`poetry -C processor env use 3.12`). PyFlink 2.2.1 has no Python 3.13 wheel.
