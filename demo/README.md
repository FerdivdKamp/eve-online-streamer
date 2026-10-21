# Demo scenario service

Compose starts this service at <http://localhost:8002/docs>. It publishes controlled observations directly to `market-observations`; it does not call or change the mock market API. It reuses the producer's event model and Kafka publisher. The normal producer remains a separate CLI program for polling the mock API.

The Swagger page exposes:

- `POST /scenarios/alert`: publishes 100 observations for one fresh synthetic item. Events 1–90 cost 100 ISK, event 91 costs 70 ISK by default, and events 92–100 return to 100 ISK. All events share one Kafka key and have unique source IDs. The run passes only when the Flink group's committed input offset covers all 100 events and exactly one matching alert, from event 91, appears in `market.alerts`.
- `POST /scenarios/load`: publishes stable-price observations across a chosen number of market keys at a chosen rate. Defaults are 5,000 events, 200 events per second, and 100 keys. The run passes when Flink commits all published input offsets. Use Grafana for throughput, lag, and checkpoint behavior during this run.
- `GET /runs/{run_id}`: returns `queued`, `running`, `passed`, or `failed`, plus progress and the final result. Only one scenario runs at a time; run results are kept in memory and disappear if the service restarts.

The alert scenario uses the local Compose processor's 30% drop threshold and three-observation minimum. It tests the threshold at exactly 70 against a 100 baseline. Set `PROCESSOR_DROP_THRESHOLD_PERCENT` in the Compose environment to change both the processor and the demo. If you change the Kafka group, update `DEMO_PROCESSOR_GROUP` in Compose. Both scenarios use fresh live event timestamps and synthetic item IDs to avoid old market state. A failed or timed-out run reports failure rather than claiming an alert was verified.

Example from PowerShell:

```powershell
$run = Invoke-RestMethod -Method Post http://localhost:8002/scenarios/alert
Invoke-RestMethod "http://localhost:8002/runs/$($run.run_id)"
```

To start a load run from Swagger, use `POST /scenarios/load` with a body such as `{"count":10000,"rate":500,"keys":500}`. Monitor <http://localhost:3000/> and <http://localhost:9090/targets>. This service is bound to localhost and intended only for the local demo.
