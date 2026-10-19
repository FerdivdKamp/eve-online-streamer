"""Pure, bounded rolling-price calculations used by the Flink keyed operator."""

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from hashlib import sha256

from .config import ProcessorConfig
from .models import MarketObservation

HOUR_MS = 3_600_000


@dataclass
class RollingState:
    """One market key's managed state, serialized as a Flink ValueState value."""

    buckets: dict[int, tuple[float, int]] = field(default_factory=dict)
    seen: dict[str, int] = field(default_factory=dict)
    latest_event_ms: int | None = None

    @classmethod
    def from_json(cls, value: str | None) -> "RollingState":
        """Load a checkpointed value or start with empty state."""
        if value is None:
            return cls()
        data = json.loads(value)
        return cls(
            buckets={int(k): (float(v[0]), int(v[1])) for k, v in data["buckets"].items()},
            seen={str(k): int(v) for k, v in data["seen"].items()},
            latest_event_ms=data["latest_event_ms"],
        )

    def to_json(self) -> str:
        """Serialize the state for a Flink ValueState descriptor."""
        return json.dumps({
            "buckets": self.buckets,
            "seen": self.seen,
            "latest_event_ms": self.latest_event_ms,
        }, separators=(",", ":"), sort_keys=True)


@dataclass(frozen=True)
class StepResult:
    """Outcome of evaluating one observation against a single market key."""

    state: RollingState
    status: str
    alert: dict | None = None


def rolling_key(observation: MarketObservation, dimensions: tuple[str, ...] = ()) -> tuple:
    """Group by region and item, with explicitly selected extra dimensions."""
    optional_values = {
        "station_id": observation.location.station_id if observation.location.station_id is not None else -1,
        "market_side": observation.market_side or "",
        "order_type": observation.order_type or "",
    }
    return (observation.location.region_id, observation.item_id) + tuple(
        optional_values[dimension] for dimension in dimensions
    )


def observation_id(observation: MarketObservation) -> str:
    """Use a source ID if supplied, otherwise fingerprint the complete event."""
    source = (
        f"source:{observation.source_event_id}"
        if observation.source_event_id is not None
        else "event:" + json.dumps(
            observation.model_dump(mode="json", exclude_none=True),
            sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        )
    )
    return sha256(source.encode("utf-8")).hexdigest()


def rolling_average(state: RollingState) -> tuple[float | None, int]:
    """Calculate an observation-weighted mean over the retained buckets."""
    total = sum(bucket_sum for bucket_sum, _ in state.buckets.values())
    count = sum(bucket_count for _, bucket_count in state.buckets.values())
    return (total / count if count else None), count


def percentage_deviation(price: float, average: float) -> float:
    """Calculate the signed percentage difference from a positive baseline."""
    if average <= 0:
        raise ValueError("rolling average must be positive")
    return (price - average) / average * 100


def evaluate(
    observation: MarketObservation,
    state: RollingState,
    config: ProcessorConfig,
    processing_time: datetime,
) -> StepResult:
    """Evaluate before adding the event; retain only bounded live history."""
    if processing_time.tzinfo is None or processing_time.utcoffset() is None:
        raise ValueError("processing_time must include a timezone")
    if observation.ingestion is not None and observation.ingestion.mode in {"backfill", "replay"}:
        return StepResult(state, observation.ingestion.mode)

    event_ms = int(observation.event_timestamp.timestamp() * 1000)
    now_ms = int(processing_time.timestamp() * 1000)
    if now_ms - event_ms > config.max_event_age_seconds * 1000:
        return StepResult(state, "stale")
    if event_ms - now_ms > config.max_future_skew_seconds * 1000:
        return StepResult(state, "future")
    latest_ms = state.latest_event_ms
    if latest_ms is not None and latest_ms - event_ms > config.allowed_lateness_seconds * 1000:
        return StepResult(state, "too_late")

    anchor_ms = max(event_ms, latest_ms) if latest_ms is not None else event_ms
    anchor_hour = anchor_ms // HOUR_MS * HOUR_MS
    cutoff_ms = anchor_hour - config.window_hours * HOUR_MS
    event_hour = event_ms // HOUR_MS * HOUR_MS
    if event_hour < cutoff_ms:
        return StepResult(state, "outside_window")

    event_id = observation_id(observation)
    if event_id in state.seen:
        return StepResult(state, "duplicate")

    next_state = RollingState(
        buckets={hour: value for hour, value in state.buckets.items() if hour >= cutoff_ms},
        seen={key: timestamp for key, timestamp in state.seen.items() if timestamp >= cutoff_ms},
        latest_event_ms=anchor_ms,
    )
    average, count = rolling_average(next_state)
    alert = None
    is_late = latest_ms is not None and event_ms < latest_ms
    if (
        not is_late
        and average is not None
        and average > 0
        and count >= config.min_observations
        and observation.current_price <= average * (1 - config.drop_threshold_percent / 100)
    ):
        key = rolling_key(observation, config.key_dimensions)
        alert_id = sha256(
            json.dumps([*key, event_id], separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        alert = {
            "schema_version": 1,
            "alert_id": alert_id,
            "observation_id": event_id,
            "source_event_id": observation.source_event_id,
            "region_id": key[0],
            "item_id": key[1],
            "station_id": observation.location.station_id,
            "market_side": observation.market_side,
            "order_type": observation.order_type,
            "current_price": observation.current_price,
            "previous_rolling_average": average,
            "percentage_deviation": percentage_deviation(observation.current_price, average),
            "event_timestamp": observation.event_timestamp.astimezone(timezone.utc).isoformat(),
            "processing_timestamp": processing_time.astimezone(timezone.utc).isoformat(),
            "window": {
                "start": (
                    observation.event_timestamp.astimezone(timezone.utc)
                    - timedelta(hours=config.window_hours)
                ).isoformat(),
                "bucket_start": datetime.fromtimestamp(cutoff_ms / 1000, tz=timezone.utc).isoformat(),
                "end": observation.event_timestamp.astimezone(timezone.utc).isoformat(),
                "hours": config.window_hours,
                "bucket_seconds": 3600,
                "observation_count": count,
            },
        }

    bucket_sum, bucket_count = next_state.buckets.get(event_hour, (0.0, 0))
    next_state.buckets[event_hour] = (bucket_sum + observation.current_price, bucket_count + 1)
    next_state.seen[event_id] = event_ms
    if len(next_state.seen) > config.max_dedup_entries:
        oldest = min(next_state.seen, key=lambda key: next_state.seen[key])
        del next_state.seen[oldest]
    return StepResult(next_state, "late" if is_late else "accepted", alert)
