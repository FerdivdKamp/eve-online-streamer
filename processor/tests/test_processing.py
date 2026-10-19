"""Unit coverage for observation parsing without a Flink runtime."""

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eve_processor.config import ProcessorConfig
from eve_processor.processing import classify, market_key, parse_observation


SAMPLE = json.loads(
    (Path(__file__).resolve().parents[2] / "producer" / "examples" / "sample_event.json").read_text()
)


class ProcessingTests(unittest.TestCase):
    """Validate the published contract and diagnostic policy."""

    def test_valid_observation_and_key(self) -> None:
        """Keep identity, timestamps, and producer key semantics."""
        result = parse_observation(json.dumps(SAMPLE))
        self.assertIsNone(result.error)
        self.assertEqual(market_key(result.observation), (10000002, 34))
        self.assertEqual(result.observation.source_timestamp, "2026-01-01T00:00:00Z")
        self.assertEqual(result.observation.event_timestamp.year, 2026)
        self.assertEqual(classify(json.dumps(SAMPLE))[0], "valid")

    def test_missing_fields(self) -> None:
        """The known manual test event must become a diagnostic."""
        result = parse_observation('{"item_id":34,"item_name":"Tritanium","current_price":6.97}')
        self.assertIsNone(result.observation)
        self.assertIn("volume", result.error)
        self.assertEqual(classify('{"item_id":34}')[0], "invalid")
        self.assertIn("sha256=", classify('{"item_id":34}')[1])

    def test_invalid_types(self) -> None:
        """Reject coerced IDs and prices."""
        changed = {**SAMPLE, "item_id": "34", "current_price": "4.5"}
        result = parse_observation(json.dumps(changed))
        self.assertIn("item_id", result.error)
        self.assertIn("current_price", result.error)

    def test_timestamp_parsing(self) -> None:
        """Require parseable and timezone-aware operational timestamps."""
        for name, value in (
            ("source_timestamp", "not-a-date"),
            ("event_timestamp", "2026-01-01T00:00:00"),
            ("fetched_at", "not-a-date"),
        ):
            with self.subTest(name=name):
                changed = {**SAMPLE, name: value}
                self.assertIn(name, parse_observation(json.dumps(changed)).error)

    def test_malformed_json_and_non_object(self) -> None:
        """Handle corrupt payloads without raising from the operator."""
        self.assertIn("invalid JSON", parse_observation("{").error)
        self.assertIn("must be an object", parse_observation("[]").error)

    def test_config_validation(self) -> None:
        """Reject unsupported offsets and impossible resources."""
        for values in (
            {"start_from": "middle"},
            {"parallelism": 0},
            {"checkpoint_interval_ms": 0},
            {"python_bundle_time_ms": 0},
            {"topic": " "},
            {"alert_topic": "market-observations"},
            {"window_hours": 0},
            {"drop_threshold_percent": 100},
            {"key_dimensions": ("unknown",)},
            {"key_dimensions": ("station_id", "station_id")},
            {"min_observations": 0},
        ):
            with self.subTest(values=values), self.assertRaises(ValueError):
                ProcessorConfig(**values)

    def test_default_and_environment_alert_settings(self) -> None:
        """Expose the default policy and explicit optional key dimensions."""
        self.assertEqual(ProcessorConfig().drop_threshold_percent, 30.0)
        self.assertEqual(ProcessorConfig().key_dimensions, ())
        with patch.dict(os.environ, {
            "PROCESSOR_DROP_THRESHOLD_PERCENT": "35",
            "PROCESSOR_KEY_DIMENSIONS": "station_id, market_side",
        }):
            config = ProcessorConfig.from_env()
        self.assertEqual(config.drop_threshold_percent, 35.0)
        self.assertEqual(config.key_dimensions, ("station_id", "market_side"))


if __name__ == "__main__":
    unittest.main()
