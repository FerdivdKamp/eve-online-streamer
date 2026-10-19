# Demo scenarios

The mock market API on port 8000 supplies source data to the normal producer. The separate [scenario service](http://localhost:8002/docs) publishes controlled events directly to Redpanda. Its Swagger page has two buttons:

- **POST /scenarios/alert:** sends 100 observations for one fresh item. The first 90 are priced at 100 ISK, event 91 is 70 ISK by default, and events 92–100 are 100 ISK. It checks that Flink commits all 100 and that exactly event 91 produces an alert.
- **POST /scenarios/load:** sends a configurable number of stable-price observations over multiple items at a selected events-per-second rate. The result reports publish throughput and whether Flink committed the input. Watch [Grafana](http://localhost:3000/) for throughput, consumer lag, and checkpoints.

Both POST endpoints return a `run_id`. Use **GET /runs/{run_id}** in the same Swagger page until the state reads `passed` or `failed`. The alert result includes the synthetic `item_id` and `alert_id`; find that message in the `market.alerts` topic in [Redpanda Console](http://localhost:8080/). The scenario service keeps results only in memory, and accepts one run at a time.

The normal producer remains a separate CLI tool that polls the mock API. The scenario service is for controlled tests and does not change the mock API or the processor's alert rule. The default processor threshold is 30%, so a 70 ISK observation against a 100 ISK average triggers at the boundary. `PROCESSOR_DROP_THRESHOLD_PERCENT` in the Compose environment sets the threshold for both services.
