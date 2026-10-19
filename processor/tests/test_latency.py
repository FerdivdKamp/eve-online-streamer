"""Pure tests for the live observation latency contract."""

import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eve_processor.config import ProcessorConfig
from eve_processor.latency import SLA_SECONDS, classify_latency, is_sla_success
from eve_processor.models import MarketObservation
from eve_processor.processing import classify, parse_observation
from eve_processor.rolling import RollingState, evaluate

SAMPLE = json.loads(
    (Path(__file__).resolve().parents[2] / "producer" / "examples" / "sample_event.json").read_text()
)
START = datetime(2026, 10, 19, 12, tzinfo=timezone.utc)
CONFIG = ProcessorConfig()


def observation(**changes):
    return MarketObservation.model_validate({**SAMPLE, "event_timestamp": START.isoformat(), **changes})


class LatencyTests(unittest.TestCase):
    def test_normal_observation_without_alert(self):
        event = observation()
        result = evaluate(event, RollingState(), CONFIG, START + timedelta(seconds=2))
        self.assertIsNone(result.alert)
        self.assertEqual(result.status, "accepted")
        sample = classify_latency(event, result.status, START + timedelta(seconds=2.5), CONFIG)
        self.assertEqual(sample.category, "live")
        self.assertEqual(sample.seconds, 2.5)

    def test_exact_five_second_boundary(self):
        event = observation()
        at_boundary = classify_latency(event, "accepted", START + timedelta(seconds=5), CONFIG)
        after_boundary = classify_latency(
            event, "accepted", START + timedelta(seconds=5, microseconds=1), CONFIG
        )
        self.assertEqual(at_boundary.seconds, SLA_SECONDS)
        self.assertTrue(is_sla_success(at_boundary, "accepted"))
        self.assertLess(SLA_SECONDS, after_boundary.seconds)
        self.assertFalse(is_sla_success(after_boundary, "accepted"))

    def test_backfill_replay_and_future_excluded(self):
        backfill = observation(ingestion={"mode": "backfill"})
        self.assertEqual(classify_latency(backfill, "backfill", START + timedelta(days=1), CONFIG).category,
                         "backfill")
        replay = observation(ingestion={"mode": "replay"})
        replay_result = evaluate(replay, RollingState(), CONFIG, START)
        self.assertEqual(replay_result.status, "replay")
        self.assertEqual(classify_latency(replay, replay_result.status, START, CONFIG).category, "replay")
        future = observation(event_timestamp=(START + timedelta(seconds=61)).isoformat())
        self.assertEqual(classify_latency(future, "future", START, CONFIG).category, "future")

    def test_stale_live_event_is_counted_as_slow(self):
        event = observation()
        result = evaluate(event, RollingState(), CONFIG, START + timedelta(hours=1))
        sample = classify_latency(event, result.status, START + timedelta(hours=1), CONFIG)
        self.assertEqual(result.status, "stale")
        self.assertEqual(sample.category, "live")
        self.assertEqual(sample.seconds, 3600)
        self.assertFalse(is_sla_success(sample, result.status))

    def test_small_future_skew_is_zero_latency(self):
        event = observation(event_timestamp=(START + timedelta(seconds=1)).isoformat())
        sample = classify_latency(event, "accepted", START, CONFIG)
        self.assertEqual(sample.seconds, 0)

    def test_invalid_timestamps_are_separate_from_other_invalid_records(self):
        for field, value in (("event_timestamp", "not-a-date"),
                             ("fetched_at", "not-a-date"),
                             ("source_timestamp", "not-a-date")):
            with self.subTest(field=field):
                raw = json.dumps({**SAMPLE, field: value})
                self.assertTrue(parse_observation(raw).invalid_timestamp)
                self.assertEqual(classify(raw)[0], "invalid_timestamp")
        self.assertEqual(classify('{"item_id":34}')[0], "invalid")


if __name__ == "__main__":
    unittest.main()
