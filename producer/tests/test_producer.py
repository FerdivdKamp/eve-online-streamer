"""Unit tests for the market producer and its command-line modes."""

import json
import unittest
from copy import deepcopy
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from pydantic import ValidationError

from eve_producer.api import MarketClient
from eve_producer.cli import main, run, run_api_only, run_kafka_only
from eve_producer.config import Settings
from eve_producer.models import MarketEvent, MarketItem
from eve_producer.publisher import DeliveryError, MarketPublisher
from eve_producer.throttle import RateLimiter


ITEM = {
    "item_id": 34,
    "item_name": "Tritanium",
    "current_price": 4.5,
    "avg_price": 5.0,
    "price_change": -10.0,
    "volume": 100,
    "timestamp": "2020-01-01T00:00:00Z",
    "location": {
        "region_id": 10000002,
        "region_name": "The Forge",
        "station_id": 60003760,
        "station_name": "Jita",
    },
}


def response(page, total_pages=1, data=None):
    """Build a mocked API response for one market page."""
    result = Mock()
    result.status_code = 200
    result.raise_for_status.return_value = None
    result.json.return_value = {
        "data": [ITEM] if data is None else data,
        "pagination": {
            "current_page": page,
            "per_page": 1,
            "total_pages": total_pages,
            "total_items": total_pages,
        },
    }
    return result


class ConfigTests(unittest.TestCase):
    """Verify INI defaults and settings override precedence."""

    @patch.dict("os.environ", {}, clear=True)
    def test_ini_defaults(self):
        """Read default values from the packaged INI file."""
        settings = Settings(_env_file=None)
        self.assertEqual(settings.api_url, "http://localhost:8000/api/market")
        self.assertEqual(settings.threshold, 30)
        self.assertEqual(settings.events_per_second, 20)

    @patch.dict("os.environ", {"EVENTS_PER_SECOND": "7"}, clear=True)
    def test_environment_and_explicit_override(self):
        """Prefer constructor values, then environment, then INI defaults."""
        self.assertEqual(Settings(_env_file=None).events_per_second, 7)
        self.assertEqual(Settings(events_per_second=3, _env_file=None).events_per_second, 3)


class ApiTests(unittest.TestCase):
    """Check API pagination, retries, and response validation."""

    def client(self, session, max_retries=2):
        """Build a market client with a mocked HTTP session."""
        return MarketClient("http://api/api/market", 1, 90, 5, max_retries, 0.5, session)

    def test_pagination_and_response_time(self):
        """Fetch every page and attach a UTC response time."""
        session = Mock()
        session.get.side_effect = [response(1, 2), response(2, 2)]
        observations = list(self.client(session).observations())
        self.assertEqual(len(observations), 2)
        self.assertTrue(all(at.tzinfo == timezone.utc for _, at in observations))
        self.assertEqual(
            [call.kwargs["params"]["page"] for call in session.get.call_args_list], [1, 2]
        )
        self.assertEqual(session.get.call_args.kwargs["params"]["threshold"], 90)
        self.assertEqual(session.get.call_args.kwargs["timeout"], 5)

    @patch("eve_producer.api.time.sleep")
    def test_retries_timeout_and_server_error_with_bounded_backoff(self, sleep):
        """Retry transient failures with increasing bounded delays."""
        session = Mock()
        server_error = response(1)
        server_error.status_code = 503
        server_error.raise_for_status.side_effect = requests.HTTPError("unavailable")
        session.get.side_effect = [requests.Timeout(), server_error, response(1)]
        self.assertEqual(len(list(self.client(session).observations())), 1)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.5, 1.0])

    @patch("eve_producer.api.time.sleep")
    def test_client_error_is_not_retried(self, sleep):
        """Fail immediately for a nonretryable HTTP status."""
        session = Mock()
        bad = response(1)
        bad.status_code = 400
        bad.raise_for_status.side_effect = requests.HTTPError("bad request")
        session.get.return_value = bad
        with self.assertRaises(requests.HTTPError):
            list(self.client(session).observations())
        session.get.assert_called_once()
        sleep.assert_not_called()

    @patch("eve_producer.api.time.sleep")
    def test_timeout_retries_are_bounded(self, sleep):
        """Stop retrying after the configured attempt limit."""
        session = Mock()
        session.get.side_effect = requests.Timeout("slow")
        with self.assertRaises(requests.Timeout):
            list(self.client(session).observations())
        self.assertEqual(session.get.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_malformed_api_response_is_rejected(self):
        """Reject invalid JSON and missing required market fields."""
        invalid_item = deepcopy(ITEM)
        invalid_item.pop("location")
        for bad_response, expected_error in (
            (response(1, data=[invalid_item]), ValidationError),
            (response(1), ValueError),
        ):
            with self.subTest(expected_error=expected_error):
                session = Mock()
                session.get.return_value = bad_response
                if expected_error is ValueError:
                    bad_response.json.side_effect = ValueError("invalid JSON")
                with self.assertRaises(expected_error):
                    list(self.client(session).observations())
                session.get.assert_called_once()

    def test_inconsistent_pagination_is_rejected(self):
        """Reject pages that disagree with the requested page or size."""
        for field, invalid_value in (
            ("current_page", 2),
            ("per_page", 5),
            ("total_pages", 0),
            ("total_items", 5),
        ):
            with self.subTest(field=field):
                session = Mock()
                bad_response = response(1)
                bad_response.json.return_value["pagination"][field] = invalid_value
                session.get.return_value = bad_response
                with self.assertRaisesRegex(ValueError, "Inconsistent pagination"):
                    list(self.client(session).observations())

    def test_empty_zero_page_response_is_valid(self):
        """Accept an empty result with zero items and zero pages."""
        session = Mock()
        empty = response(1, total_pages=0, data=[])
        empty.json.return_value["pagination"]["total_items"] = 0
        session.get.return_value = empty
        self.assertEqual(list(self.client(session).observations()), [])


class EventTests(unittest.TestCase):
    """Check market event enrichment and serialization."""

    def test_enrichment_preserves_source_data_and_serializes(self):
        """Keep source fields and serialize UTC enrichment timestamps."""
        fetched_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
        event = MarketEvent.from_item(MarketItem.model_validate(ITEM), fetched_at)
        encoded = json.loads(event.model_dump_json())
        self.assertEqual(encoded["source_timestamp"], ITEM["timestamp"])
        self.assertNotIn("timestamp", encoded)
        self.assertEqual(encoded["current_price"], ITEM["current_price"])
        self.assertEqual(encoded["volume"], ITEM["volume"])
        self.assertEqual(encoded["location"], ITEM["location"])
        self.assertEqual(encoded["fetched_at"], "2026-01-02T00:00:00Z")
        self.assertTrue(encoded["event_timestamp"].endswith("Z"))


class ProducerPipelineTests(unittest.TestCase):
    """Verify observations pass from the API to Kafka without local filtering."""

    def test_observation_outside_api_threshold_is_published_unchanged(self):
        """Leave threshold decisions to Flink even when the API ignores them."""
        outside_threshold = {
            **ITEM,
            "current_price": 5.25,
            "avg_price": 5.0,
            "price_change": 5.0,
        }
        with patch("eve_producer.api.requests.Session") as session_class, patch(
            "eve_producer.cli.MarketPublisher"
        ) as publisher_class:
            session_class.return_value.get.return_value = response(1, data=[outside_threshold])

            run(Settings(threshold=30, page_size=1, _env_file=None), once=True, rate=0)

            requested = session_class.return_value.get.call_args.kwargs["params"]
            self.assertEqual(requested["threshold"], 30)
            publisher_class.return_value.publish.assert_called_once()
            event = publisher_class.return_value.publish.call_args.args[0]
            expected = {**outside_threshold, "source_timestamp": outside_threshold["timestamp"]}
            expected.pop("timestamp")
            self.assertEqual(
                event.model_dump(exclude={"fetched_at", "event_timestamp"}), expected
            )


class PublishingTests(unittest.TestCase):
    """Check Kafka publication and delivery handling."""

    def test_key_json_and_delivery_failure(self):
        """Use the region and item key and report callback failures."""
        producer = Mock()
        publisher = MarketPublisher("unused", "market", producer)
        event = MarketEvent.from_item(MarketItem.model_validate(ITEM), datetime.now(timezone.utc))
        publisher.publish(event)
        kwargs = producer.produce.call_args.kwargs
        self.assertEqual(kwargs["key"], "10000002:34")
        self.assertEqual(json.loads(kwargs["value"])["item_id"], 34)
        kwargs["on_delivery"]("broker failed", None)
        with self.assertRaises(DeliveryError):
            publisher.close()
        producer.close.assert_called_once()

    def test_flush_timeout_is_failure(self):
        """Treat undelivered messages after a flush as a failure."""
        producer = Mock()
        producer.flush.return_value = 1
        with self.assertRaises(DeliveryError):
            MarketPublisher("unused", "market", producer).close()
        producer.purge.assert_called_once()
        producer.poll.assert_called_once_with(0)
        producer.close.assert_called_once()

    def test_interrupt_during_flush_purges_before_close(self):
        """Stop waiting for delivery when Ctrl+C interrupts a flush."""
        producer = Mock()
        producer.flush.side_effect = KeyboardInterrupt()
        publisher = MarketPublisher("unused", "market", producer)

        with self.assertRaises(KeyboardInterrupt):
            publisher.close()

        producer.purge.assert_called_once()
        producer.poll.assert_called_once_with(0)
        producer.close.assert_called_once()

    def test_successful_delivery_is_confirmed_by_callback(self):
        """Accept a message only after Kafka confirms delivery."""
        producer = Mock()
        publisher = MarketPublisher("unused", "market", producer)
        event = MarketEvent.from_item(MarketItem.model_validate(ITEM), datetime.now(timezone.utc))
        publisher.publish(event)
        producer.flush.side_effect = lambda _timeout: producer.produce.call_args.kwargs[
            "on_delivery"
        ](None, Mock()) or 0
        publisher.close()
        self.assertEqual(publisher.pending, 0)
        producer.flush.assert_called_once_with(10)

    def test_poll_processes_successful_delivery_callback(self):
        """Accept a Kafka confirmation received during producer polling."""
        producer = Mock()
        producer.flush.return_value = 0
        publisher = MarketPublisher("unused", "market", producer)
        event = MarketEvent.from_item(MarketItem.model_validate(ITEM), datetime.now(timezone.utc))

        def poll(_timeout):
            """Confirm the message on the poll after it is queued."""
            if producer.produce.call_count:
                producer.produce.call_args.kwargs["on_delivery"](None, Mock())
            return 0

        producer.poll.side_effect = poll
        publisher.publish(event)
        self.assertEqual(producer.poll.call_count, 2)
        self.assertEqual(publisher.pending, 0)
        publisher.close()

    def test_missing_delivery_callback_is_failure(self):
        """Reject a zero flush count when no delivery callback arrived."""
        producer = Mock()
        publisher = MarketPublisher("unused", "market", producer)
        event = MarketEvent.from_item(MarketItem.model_validate(ITEM), datetime.now(timezone.utc))
        publisher.publish(event)
        producer.flush.return_value = 0
        with self.assertRaisesRegex(DeliveryError, "missing delivery confirmation"):
            publisher.close()

    def test_poll_retries_a_full_kafka_queue(self):
        """Poll for space and retry a transient full producer queue."""
        producer = Mock()
        producer.produce.side_effect = [BufferError("queue full"), None]
        producer.flush.return_value = 0
        publisher = MarketPublisher("unused", "market", producer)
        event = MarketEvent.from_item(MarketItem.model_validate(ITEM), datetime.now(timezone.utc))

        publisher.publish(event)

        self.assertEqual(producer.produce.call_count, 2)
        producer.poll.assert_any_call(1.0)
        self.assertEqual(publisher.pending, 1)
        producer.produce.call_args.kwargs["on_delivery"](None, Mock())
        publisher.close()
        self.assertEqual(publisher.pending, 0)
        producer.close.assert_called_once()

    def test_persistent_full_kafka_queue_fails_without_pending_message(self):
        """Stop after bounded queue retries and keep pending count accurate."""
        producer = Mock()
        producer.produce.side_effect = BufferError("queue full")
        producer.flush.return_value = 0
        publisher = MarketPublisher("unused", "market", producer)
        event = MarketEvent.from_item(MarketItem.model_validate(ITEM), datetime.now(timezone.utc))

        with self.assertRaisesRegex(DeliveryError, "queue remained full"):
            publisher.publish(event)

        self.assertEqual(producer.produce.call_count, 3)
        self.assertEqual(publisher.pending, 0)
        self.assertEqual([call.args for call in producer.poll.call_args_list].count((1.0,)), 2)
        publisher.close()
        producer.close.assert_called_once()

    def test_station_changes_keep_the_same_partition_key(self):
        """Keep station updates in event bodies while keying by region and item."""
        first = MarketItem.model_validate(ITEM)
        second_raw = deepcopy(ITEM)
        second_raw["location"]["station_id"] = 60003761
        second_raw["location"]["station_name"] = "New station"
        second = MarketItem.model_validate(second_raw)
        producer = Mock()
        producer.flush.return_value = 0
        publisher = MarketPublisher("unused", "market", producer)

        for item in (first, second):
            publisher.publish(MarketEvent.from_item(item, datetime.now(timezone.utc)))

        calls = producer.produce.call_args_list
        self.assertEqual([call.kwargs["key"] for call in calls], ["10000002:34"] * 2)
        locations = [json.loads(call.kwargs["value"])["location"] for call in calls]
        self.assertEqual([location["station_id"] for location in locations], [60003760, 60003761])
        self.assertEqual(locations[1]["station_name"], "New station")
        for call in calls:
            call.kwargs["on_delivery"](None, Mock())
        publisher.close()


class LifecycleTests(unittest.TestCase):
    """Verify transport cleanup after abnormal termination."""

    def test_run_closes_http_and_kafka_on_exception_or_ctrl_c(self):
        """Release both transports after an API error or keyboard interrupt."""
        for failure in (requests.Timeout("timed out"), KeyboardInterrupt()):
            with self.subTest(failure=type(failure).__name__), patch(
                "eve_producer.api.requests.Session"
            ) as session_class, patch("eve_producer.publisher.Producer") as producer_class:
                session_class.return_value.get.side_effect = failure
                producer_class.return_value.flush.return_value = 0

                with self.assertRaises(type(failure)):
                    run(Settings(max_retries=0, _env_file=None), once=True, rate=0)

                session_class.return_value.close.assert_called_once()
                if isinstance(failure, KeyboardInterrupt):
                    producer_class.return_value.flush.assert_not_called()
                    producer_class.return_value.purge.assert_called_once()
                else:
                    producer_class.return_value.flush.assert_called_once_with(10)
                producer_class.return_value.close.assert_called_once()

    def test_run_closes_http_and_kafka_after_publish_error(self):
        """Release both transports if Kafka rejects a publication."""
        with patch("eve_producer.api.requests.Session") as session_class, patch(
            "eve_producer.publisher.Producer"
        ) as producer_class:
            session_class.return_value.get.return_value = response(1)
            producer_class.return_value.produce.side_effect = ValueError("send failed")
            producer_class.return_value.flush.return_value = 0

            with self.assertRaisesRegex(ValueError, "send failed"):
                run(Settings(page_size=1, _env_file=None), once=True, rate=0)

            session_class.return_value.close.assert_called_once()
            producer_class.return_value.flush.assert_called_once_with(10)
            producer_class.return_value.close.assert_called_once()

    @patch("eve_producer.api.requests.Session")
    def test_api_only_closes_http_session_on_ctrl_c(self, session_class):
        """Close the API session when API-only mode is interrupted."""
        session_class.return_value.get.side_effect = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            run_api_only(Settings(_env_file=None))
        session_class.return_value.close.assert_called_once()


class ThrottleTests(unittest.TestCase):
    """Check per-event rate limiting and CLI overrides."""

    @patch("eve_producer.throttle.time.sleep")
    @patch("eve_producer.throttle.time.monotonic", side_effect=[0, 0.1, 0.5])
    def test_rate_waits_between_events(self, _clock, sleep):
        """Wait between consecutive events at a limited rate."""
        limiter = RateLimiter(2)
        limiter.wait()
        limiter.wait()
        sleep.assert_called_once_with(0.4)

    @patch("eve_producer.throttle.time.sleep")
    def test_zero_rate_is_unthrottled(self, sleep):
        """Skip sleeping when the configured rate is zero."""
        limiter = RateLimiter(0)
        limiter.wait()
        limiter.wait()
        sleep.assert_not_called()

    @patch("eve_producer.cli.MarketPublisher")
    @patch("eve_producer.cli.MarketClient")
    @patch("eve_producer.cli.RateLimiter")
    def test_cli_rate_override_applies_per_event(self, limiter_class, client_class, publisher_class):
        """Apply the CLI rate override to individual publications."""
        item = MarketItem.model_validate(ITEM)
        client_class.return_value.observations.return_value = iter(
            [(item, datetime.now(timezone.utc)), (item, datetime.now(timezone.utc))]
        )
        run(Settings(_env_file=None), once=True, rate=0)
        limiter_class.assert_called_once_with(0)
        self.assertEqual(limiter_class.return_value.wait.call_count, 2)
        self.assertEqual(publisher_class.return_value.publish.call_count, 2)

    @patch("eve_producer.cli.MarketPublisher")
    @patch("eve_producer.cli.MarketClient")
    def test_max_events_stops_and_closes_after_exact_count(self, client_class, publisher_class):
        """Publish exactly the requested count and close both transports."""
        item = MarketItem.model_validate(ITEM)
        fetched_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        client_class.return_value.observations.return_value = iter([(item, fetched_at)] * 100)

        run(Settings(_env_file=None), rate=0, max_events=50)

        self.assertEqual(publisher_class.return_value.publish.call_count, 50)
        publisher_class.return_value.close.assert_called_once()
        client_class.return_value.close.assert_called_once()

    @patch("eve_producer.cli.MarketPublisher")
    @patch("eve_producer.cli.MarketClient")
    @patch("eve_producer.cli.RateLimiter")
    @patch("eve_producer.cli.time.monotonic", side_effect=[0, 3.8])
    def test_max_events_logs_run_summary(self, _clock, _limiter, client_class, _publisher):
        """Report the effective rate, elapsed time, and event limit in order."""
        item = MarketItem.model_validate(ITEM)
        fetched_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        client_class.return_value.observations.return_value = iter([(item, fetched_at)] * 25)

        with self.assertLogs("eve_producer.cli", level="INFO") as captured:
            run(Settings(_env_file=None), once=True, rate=5, max_events=20)

        self.assertEqual(
            [record.getMessage() for record in captured.records],
            [
                "Producer started: rate=5, max_events=20",
                "Published 20 events in 3.8 seconds",
                "Event limit reached; stopping",
            ],
        )

    @patch("eve_producer.cli.MarketPublisher")
    @patch("eve_producer.cli.MarketClient")
    @patch("eve_producer.cli.RateLimiter")
    @patch("eve_producer.models.datetime")
    def test_event_timestamp_is_assigned_after_each_wait(
        self, model_datetime, limiter_class, client_class, publisher_class
    ):
        """Stamp each event after throttling, immediately before publication."""
        item = MarketItem.model_validate(ITEM)
        fetched_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        publish_times = [
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            datetime(2026, 1, 3, tzinfo=timezone.utc),
        ]
        client_class.return_value.observations.return_value = iter(
            [(item, fetched_at), (item, fetched_at)]
        )
        state = {"waits": 0, "stamps": 0}

        def wait():
            """Mark when the rate limiter releases an event."""
            state["waits"] += 1

        def now(tz):
            """Return a known publish time after the matching wait."""
            self.assertEqual(tz, timezone.utc)
            self.assertEqual(state["waits"], state["stamps"] + 1)
            value = publish_times[state["stamps"]]
            state["stamps"] += 1
            return value

        limiter_class.return_value.wait.side_effect = wait
        model_datetime.now.side_effect = now
        run(Settings(_env_file=None), once=True, rate=2)
        events = [call.args[0] for call in publisher_class.return_value.publish.call_args_list]
        self.assertEqual([event.event_timestamp for event in events], publish_times)
        self.assertTrue(all(event.fetched_at == fetched_at for event in events))
        self.assertEqual(state, {"waits": 2, "stamps": 2})


class SmokeModeTests(unittest.TestCase):
    """Check that each smoke mode uses only its requested connection."""

    @patch("eve_producer.cli.MarketPublisher")
    @patch("eve_producer.cli.MarketClient")
    def test_api_only_prints_data_without_kafka(self, client_class, publisher_class):
        """Print source JSON while leaving Kafka untouched."""
        client_class.return_value.observations.return_value = iter(
            [(MarketItem.model_validate(ITEM), datetime.now(timezone.utc))]
        )
        output = StringIO()
        with redirect_stdout(output):
            run_api_only(Settings(_env_file=None))
        self.assertEqual(json.loads(output.getvalue())["timestamp"], ITEM["timestamp"])
        publisher_class.assert_not_called()

    @patch("eve_producer.cli.MarketClient")
    def test_api_only_max_events_limits_output(self, client_class):
        """Print only the requested number of API observations."""
        item = MarketItem.model_validate(ITEM)
        client_class.return_value.observations.return_value = iter(
            [(item, datetime.now(timezone.utc))] * 5
        )
        output = StringIO()
        with redirect_stdout(output):
            run_api_only(Settings(_env_file=None), max_events=2)
        self.assertEqual(len(output.getvalue().splitlines()), 2)
        client_class.return_value.close.assert_called_once()

    @patch("eve_producer.cli.run")
    def test_cli_passes_max_events_to_producer(self, run_mock):
        """Pass the CLI message limit to the producer loop."""
        with patch("sys.argv", ["eve-producer", "--max-events", "50"]):
            main()
        self.assertEqual(run_mock.call_args.kwargs["max_events"], 50)

    def test_cli_rejects_nonpositive_max_events(self):
        """Reject limits that cannot publish any messages."""
        with patch("sys.argv", ["eve-producer", "--max-events", "0"]):
            with redirect_stderr(StringIO()), self.assertRaises(SystemExit) as error:
                main()
        self.assertEqual(error.exception.code, 2)

    @patch("eve_producer.cli.MarketClient")
    @patch("eve_producer.cli.MarketPublisher")
    @patch("eve_producer.cli.datetime")
    def test_kafka_only_publishes_sample_without_api(
        self, cli_datetime, publisher_class, client_class
    ):
        """Refresh the sample publish time without creating an API client."""
        sample = Path(__file__).resolve().parents[1] / "examples" / "sample_event.json"
        publish_time = datetime(2026, 10, 14, tzinfo=timezone.utc)
        cli_datetime.now.return_value = publish_time
        run_kafka_only(Settings(_env_file=None), sample)
        published = publisher_class.return_value.publish.call_args.args[0]
        self.assertEqual(published.item_id, 34)
        self.assertEqual(published.event_timestamp, publish_time)
        cli_datetime.now.assert_called_once_with(timezone.utc)
        publisher_class.return_value.close.assert_called_once()
        client_class.assert_not_called()

    @patch("eve_producer.cli.MarketPublisher")
    @patch("eve_producer.cli.datetime")
    def test_kafka_only_max_events_repeats_sample(self, cli_datetime, publisher_class):
        """Publish the requested sample count with a fresh time for each copy."""
        sample = Path(__file__).resolve().parents[1] / "examples" / "sample_event.json"
        publish_times = [
            datetime(2026, 10, 14, 10, second, tzinfo=timezone.utc) for second in range(3)
        ]
        cli_datetime.now.side_effect = publish_times

        run_kafka_only(Settings(_env_file=None), sample, max_events=3, rate=0)

        events = [call.args[0] for call in publisher_class.return_value.publish.call_args_list]
        self.assertEqual([event.event_timestamp for event in events], publish_times)
        publisher_class.return_value.close.assert_called_once()

    @patch("eve_producer.cli.run_kafka_only")
    def test_cli_passes_max_events_to_kafka_only(self, run_mock):
        """Pass the requested sample count and rate to Kafka-only mode."""
        with patch(
            "sys.argv",
            [
                "eve-producer", "--kafka-only", "--event-file", "examples/sample_event.json",
                "--max-events", "50", "--rate", "0",
            ],
        ):
            main()
        self.assertEqual(run_mock.call_args.kwargs["max_events"], 50)
        self.assertEqual(run_mock.call_args.kwargs["rate"], 0)

    @patch("eve_producer.publisher.Producer")
    def test_kafka_only_interrupt_purges_pending_delivery(self, producer_class):
        """Abort Kafka-only delivery promptly after Ctrl+C."""
        sample = Path(__file__).resolve().parents[1] / "examples" / "sample_event.json"
        producer_class.return_value.produce.side_effect = KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            run_kafka_only(Settings(_env_file=None), sample)

        producer_class.return_value.flush.assert_not_called()
        producer_class.return_value.purge.assert_called_once()
        producer_class.return_value.close.assert_called_once()

    @patch("eve_producer.publisher.Producer")
    def test_kafka_only_logs_graceful_shutdown_in_order(self, producer_class):
        """Report shutdown, flushing, closure, and stop in that order."""
        sample = Path(__file__).resolve().parents[1] / "examples" / "sample_event.json"
        producer = producer_class.return_value
        producer.flush.side_effect = lambda _timeout: producer.produce.call_args.kwargs[
            "on_delivery"
        ](None, Mock()) or 0
        with patch("sys.argv", ["eve-producer", "--kafka-only", "--event-file", str(sample)]):
            with self.assertLogs("eve_producer", level="INFO") as captured:
                main()

        self.assertEqual(
            [record.getMessage() for record in captured.records],
            [
                "Producer shutdown requested",
                "Flushing outstanding Kafka messages...",
                "Kafka producer closed, 0 messages pending",
                "Producer stopped",
            ],
        )

    @patch("eve_producer.publisher.Producer")
    def test_kafka_only_logs_discard_on_ctrl_c(self, producer_class):
        """Describe discarded messages without claiming they were flushed."""
        sample = Path(__file__).resolve().parents[1] / "examples" / "sample_event.json"
        producer_class.return_value.produce.side_effect = KeyboardInterrupt()
        with patch("sys.argv", ["eve-producer", "--kafka-only", "--event-file", str(sample)]):
            with self.assertLogs("eve_producer", level="INFO") as captured:
                main()

        self.assertEqual(
            [record.getMessage() for record in captured.records],
            [
                "Producer shutdown requested",
                "Discarding 1 pending Kafka message after shutdown request...",
                "Kafka producer closed, 0 messages pending",
                "Producer stopped",
            ],
        )


if __name__ == "__main__":
    unittest.main()
