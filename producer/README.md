# EVE market producer

Polls the mock EVE market API and publishes one JSON message per observation to Redpanda. The Kafka key is `region_id:item_id`. Each message keeps the market item fields, renames `timestamp` to `source_timestamp`, and adds UTC `fetched_at` and `event_timestamp` fields. Source timestamps are kept as received, even when out of order.

## Run locally

Start the mock API on port 8000 and Redpanda on port 9092, then from `producer/`:

```powershell
poetry install
Copy-Item .env.example .env
poetry run eve-producer --once
poetry run eve-producer --rate 20
poetry run eve-producer --once --max-events 50
```

Without `--once`, the producer polls continuously. `--max-events 50` stops after publishing 50 observations, including across multiple polls; combine it with `--once` to cap a single snapshot at 50. Defaults live in `src/eve_producer/config.ini`. Environment variables and a local `.env` file override those defaults; see `.env.example` for variable names. `--rate` overrides `EVENTS_PER_SECOND`, and `--rate 0` sends without throttling. `PAGE_SIZE` may be 1–100. `THRESHOLD` is sent to the API, which may filter its results. The producer does no price-drop detection or filtering.

## Test one connection at a time

With only the API running, fetch one complete snapshot and print raw observations as JSON Lines:

```powershell
poetry run eve-producer --api-only --max-events 50
```

With only Redpanda running, publish the included sample event and wait for delivery confirmation:

```powershell
poetry run eve-producer --kafka-only --event-file examples/sample_event.json
poetry run eve-producer --kafka-only --event-file examples/sample_event.json --max-events 50 --rate 0
```

You can replace `examples/sample_event.json` with any JSON file containing one complete market event. Kafka-only mode sends one event by default; `--max-events 50` repeats the sample 50 times and refreshes `event_timestamp` for each publication. `--api-only` never creates a Kafka client; `--kafka-only` never calls the API. The two flags cannot be used together.

Transient HTTP errors and timeouts are retried up to `MAX_RETRIES` times with bounded exponential backoff. Malformed responses and inconsistent pagination fail the run. Kafka queue-full errors are retried twice while polling for space. Messages are flushed after each complete poll and at shutdown; delivery errors cause a nonzero exit. Ctrl+C purges any pending Kafka messages so shutdown does not wait for an unreachable broker. The HTTP session and Kafka producer are closed on exit.

## Tests

```powershell
poetry run python -m unittest discover -s tests -v
```

