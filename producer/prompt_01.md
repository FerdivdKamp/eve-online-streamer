# Task: Implement EVE Market Data Producer

Implement a Python producer in `producer/` that polls a mock EVE Online market REST API and publishes individual market observations to Redpanda using the Kafka protocol.

Use the existing Poetry project and `src/eve_producer/` package.

## Requirements

1. **API ingestion**
   - Fetch data from `GET /api/market` using `requests`.
   - Support pagination (`page`, `per_page`) and configurable batch sizes.
   - Handle HTTP failures, timeouts, and retries with bounded exponential backoff.
   - Use the existing OpenAPI specification as reference.

2. **Event enrichment**
   - Preserve the original market data, including price, volume, item and location information.
   - Rename the API's `timestamp` to `source_timestamp`.
   - Add `event_timestamp` in UTC when each observation is published.
   - Add `fetched_at` in UTC when the API response is received.
   - Serialize events as JSON using Pydantic models.
   - Do not trust or enforce the API's `threshold` filtering; leave price-drop detection to downstream processing.

3. **Streaming**
   - Publish individual events to a configurable Redpanda topic using `confluent-kafka`.
   - Use `region_id:item_id` as the Kafka message key.
   - Support configurable throttling (`events_per_second`), including an unthrottled mode.
   - Handle Kafka delivery failures and shut down gracefully.
   - Allow continuous polling and a one-shot mode for testing.
   - Expose throttling as a CLI argument `--rate`, expressed in events per second.
    - Default to the configured `EVENTS_PER_SECOND` environment variable (default: 20).
    - Allow `--rate 0` to disable throttling and publish as fast as possible.
    - Use `argparse` for CLI argument parsing.
    - CLI arguments override environment configuration.
    - Apply throttling between individual Kafka publications, not between API requests.

4. **Configuration**
   - Use `pydantic-settings` and environment variables.
   - Configure API URL, page size, polling interval, threshold, Kafka broker, topic, and publishing rate.
   - Provide `.env.example` without credentials.

5. **Testing**
   - Unit tests for API pagination, HTTP errors, event enrichment, serialization, and throttling.
   - Mock HTTP requests and Kafka publishing; tests must work without running Redpanda.

## Design constraints

- Keep API retrieval, event models, throttling, and Kafka publishing separate.
- Prefer simple synchronous Python over asyncio for version 1.
- Do not implement market analytics or rolling averages in the producer.
- Preserve the original source timestamps, even if they are out of order.
- Keep the implementation minimal; avoid unnecessary abstractions.

## Deliverables

- Working producer with a CLI entry point (`poetry run eve-producer`).
- Updated Poetry dependencies.
- Unit tests.
- Short README explaining configuration and local execution.

Run the tests and report any remaining issues.