"""Unit tests for the state transition used by the Flink keyed operator."""

import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eve_processor.config import ProcessorConfig
from eve_processor.models import MarketObservation
from eve_processor.processing import parse_observation
from eve_processor.rolling import RollingState, evaluate, rolling_average

SAMPLE = json.loads(
    (Path(__file__).resolve().parents[2] / "producer" / "examples" / "sample_event.json").read_text()
)
START = datetime(2026, 10, 16, 10, tzinfo=timezone.utc)


def event(price: float, when: datetime, event_id: str, **changes) -> MarketObservation:
    """Build a current, uniquely identified observation."""
    data = {**SAMPLE, "current_price": price, "event_timestamp": when.isoformat(),
            "source_event_id": event_id, **changes}
    return MarketObservation.model_validate(data)


def step(price, when, event_id, state=None, config=None, **changes):
    """Evaluate with processing time equal to the event time by default."""
    return evaluate(
        event(price, when, event_id, **changes),
        state or RollingState(),
        config or ProcessorConfig(min_observations=2),
        when,
    )


class RollingTests(unittest.TestCase):
    """Check bounded state and alert rules without a Flink runtime."""

    def test_first_event_and_insufficient_history(self):
        """An event cannot alert until enough prior observations exist."""
        first = step(100, START, "first")
        self.assertIsNone(first.alert)
        self.assertEqual(rolling_average(first.state), (100, 1))
        second = step(50, START + timedelta(seconds=1), "second", first.state)
        self.assertIsNone(second.alert)
        self.assertEqual(rolling_average(second.state), (75, 2))

    def test_exact_threshold_and_clear_drop_use_prior_average(self):
        """Include the threshold boundary and exclude the incoming price."""
        state = step(100, START, "a").state
        state = step(100, START + timedelta(seconds=1), "b", state).state
        above_threshold = step(80, START + timedelta(seconds=2), "above", state)
        self.assertIsNone(above_threshold.alert)
        at_threshold = step(70, START + timedelta(seconds=2), "c", state)
        self.assertEqual(at_threshold.alert["previous_rolling_average"], 100)
        self.assertAlmostEqual(at_threshold.alert["percentage_deviation"], -30)
        self.assertEqual(at_threshold.alert["window"]["observation_count"], 2)
        self.assertEqual(at_threshold.alert["schema_version"], 1)
        drop = step(60, START + timedelta(seconds=3), "d", state)
        self.assertIsNotNone(drop.alert)

    def test_gradual_decline_uses_weighted_history(self):
        """Gradual declines follow the mean of prior observations."""
        state = RollingState()
        results = []
        for index, price in enumerate((100, 95, 90, 85, 80, 75)):
            result = step(price, START + timedelta(seconds=index), str(index), state)
            results.append(result)
            state = result.state
        self.assertTrue(all(result.alert is None for result in results))
        self.assertAlmostEqual(rolling_average(state)[0], 87.5)

    def test_expiry_and_state_round_trip(self):
        """State survives serialization and expires old hour buckets."""
        config = ProcessorConfig(window_hours=2, min_observations=1)
        old = step(100, START, "old", config=config).state
        restored = RollingState.from_json(old.to_json())
        boundary = step(100, START + timedelta(hours=2), "boundary", restored, config)
        self.assertEqual(rolling_average(boundary.state)[1], 2)
        newer = step(50, START + timedelta(hours=3), "new", restored, config)
        self.assertIsNone(newer.alert)
        self.assertEqual(rolling_average(newer.state), (50, 1))
        self.assertEqual(len(newer.state.seen), 1)

    def test_independent_market_keys(self):
        """Region and item define the default state key; extras are opt-in."""
        state_by_key = {}
        a = event(100, START, "a")
        b = event(100, START, "b", item_id=35)
        from eve_processor.rolling import rolling_key
        for observation in (a, b):
            key = rolling_key(observation)
            state_by_key[key] = evaluate(observation, RollingState(), ProcessorConfig(min_observations=1), START).state
        self.assertEqual(len(state_by_key), 2)
        station = event(100, START, "c", location={**SAMPLE["location"], "station_id": 42})
        self.assertEqual(rolling_key(a), rolling_key(station))
        self.assertNotEqual(rolling_key(a, ("station_id",)), rolling_key(station, ("station_id",)))
        side = event(100, START, "d", market_side="buy")
        self.assertEqual(rolling_key(a), rolling_key(side))
        self.assertNotEqual(rolling_key(a, ("market_side",)), rolling_key(side, ("market_side",)))

    def test_duplicate_and_out_of_order(self):
        """Duplicate events and bounded late arrivals follow their policies."""
        config = ProcessorConfig(min_observations=1, allowed_lateness_seconds=120)
        first = step(100, START, "first", config=config)
        duplicate = step(100, START, "first", first.state, config)
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(rolling_average(duplicate.state), (100, 1))
        latest = step(100, START + timedelta(seconds=60), "latest", first.state, config)
        late_observation = event(50, START + timedelta(seconds=30), "late")
        late = evaluate(late_observation, latest.state, config, START + timedelta(seconds=61))
        self.assertEqual(late.status, "late")
        self.assertIsNone(late.alert)
        self.assertEqual(rolling_average(late.state)[1], 3)
        too_late_observation = event(50, START - timedelta(seconds=121), "too-late")
        too_late = evaluate(too_late_observation, late.state, config, START)
        self.assertEqual(too_late.status, "too_late")
        self.assertEqual(rolling_average(too_late.state)[1], 3)

    def test_hash_identity_and_dedup_cap(self):
        """Hash events without source IDs and bound the seen-ID cache."""
        config = ProcessorConfig(min_observations=1, max_dedup_entries=2)
        first_observation = event(100, START, "a").model_copy(update={"source_event_id": None})
        first = evaluate(first_observation, RollingState(), config, START)
        duplicate = evaluate(first_observation, first.state, config, START)
        self.assertEqual(duplicate.status, "duplicate")
        state = step(100, START + timedelta(seconds=1), "b", first.state, config).state
        state = step(100, START + timedelta(seconds=2), "c", state, config).state
        self.assertEqual(len(state.seen), 2)

    def test_backfill_stale_and_future_are_isolated(self):
        """Backfill and invalid live times leave the live baseline intact."""
        state = step(100, START, "first").state
        config = ProcessorConfig(min_observations=1)
        backfill = event(50, START + timedelta(seconds=1), "history", ingestion={"mode": "backfill"})
        result = evaluate(backfill, state, config, START + timedelta(seconds=1))
        self.assertEqual(result.status, "backfill")
        self.assertEqual(rolling_average(result.state), (100, 1))
        stale = evaluate(event(50, START, "stale"), state, config, START + timedelta(hours=1))
        self.assertEqual(stale.status, "stale")
        future = evaluate(event(50, START + timedelta(minutes=2), "future"), state, config, START)
        self.assertEqual(future.status, "future")

    def test_alert_id_stable_after_restore(self):
        """A replay from restored state produces the same alert ID."""
        config = ProcessorConfig(min_observations=1)
        state = step(100, START, "first", config=config).state
        observation = event(70, START + timedelta(seconds=1), "drop")
        a = evaluate(observation, state, config, START + timedelta(seconds=1)).alert
        b = evaluate(observation, RollingState.from_json(state.to_json()), config,
                     START + timedelta(seconds=2)).alert
        self.assertEqual(a["alert_id"], b["alert_id"])
        self.assertEqual(a["source_event_id"], "drop")

    def test_malformed_and_invalid_prices(self):
        """Invalid input is classified before reaching rolling state."""
        self.assertIsNone(parse_observation("{").observation)
        for price in (-1, float("nan"), "100"):
            with self.subTest(price=price):
                raw = json.dumps({**SAMPLE, "current_price": price})
                self.assertIsNone(parse_observation(raw).observation)
        bad_station = {**SAMPLE, "location": {**SAMPLE["location"], "station_id": -1}}
        self.assertIsNone(parse_observation(json.dumps(bad_station)).observation)


if __name__ == "__main__":
    unittest.main()
