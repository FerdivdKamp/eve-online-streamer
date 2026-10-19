"""Runtime configuration and validation for the processor."""

import os
from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True)
class ProcessorConfig:
    """Validated settings shared by source and Flink environment."""

    bootstrap_servers: str = "redpanda:9092"
    topic: str = "market-observations"
    group_id: str = "eve-market-processor-v1"
    start_from: str = "committed"
    parallelism: int = 3
    checkpoint_interval_ms: int = 30000
    python_bundle_time_ms: int = 100
    alert_topic: str = "market.alerts"
    window_hours: int = 168
    drop_threshold_percent: float = 30.0
    key_dimensions: tuple[str, ...] = ()
    min_observations: int = 10
    allowed_lateness_seconds: int = 600
    max_event_age_seconds: int = 900
    max_future_skew_seconds: int = 60
    max_dedup_entries: int = 10000

    def __post_init__(self) -> None:
        """Reject settings that would produce an unusable stream job."""
        for name in ("bootstrap_servers", "topic", "group_id", "alert_topic"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must not be empty")
        if self.start_from not in {"committed", "earliest", "latest"}:
            raise ValueError("start_from must be committed, earliest, or latest")
        if self.alert_topic == self.topic:
            raise ValueError("alert_topic must differ from the input topic")
        if self.parallelism < 1:
            raise ValueError("parallelism must be positive")
        if self.checkpoint_interval_ms < 1000:
            raise ValueError("checkpoint_interval_ms must be at least 1000")
        if self.python_bundle_time_ms < 1:
            raise ValueError("python_bundle_time_ms must be positive")
        if self.window_hours < 1:
            raise ValueError("window_hours must be positive")
        if not isfinite(self.drop_threshold_percent) or not 0 < self.drop_threshold_percent < 100:
            raise ValueError("drop_threshold_percent must be between 0 and 100")
        allowed_dimensions = {"station_id", "market_side", "order_type"}
        if (not isinstance(self.key_dimensions, tuple)
                or any(dimension not in allowed_dimensions for dimension in self.key_dimensions)
                or len(set(self.key_dimensions)) != len(self.key_dimensions)):
            raise ValueError("key_dimensions must contain unique station_id, market_side, or order_type values")
        if self.min_observations < 1:
            raise ValueError("min_observations must be positive")
        for name in ("allowed_lateness_seconds", "max_event_age_seconds", "max_future_skew_seconds"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must not be negative")
        if self.max_dedup_entries < 1:
            raise ValueError("max_dedup_entries must be positive")

    @classmethod
    def from_env(cls) -> "ProcessorConfig":
        """Read environment variables used by the submitted job."""
        return cls(
            bootstrap_servers=os.getenv("PROCESSOR_BOOTSTRAP_SERVERS", "redpanda:9092"),
            topic=os.getenv("PROCESSOR_TOPIC", "market-observations"),
            group_id=os.getenv("PROCESSOR_GROUP_ID", "eve-market-processor-v1"),
            start_from=os.getenv("PROCESSOR_START_FROM", "committed"),
            parallelism=int(os.getenv("PROCESSOR_PARALLELISM", "3")),
            checkpoint_interval_ms=int(os.getenv("PROCESSOR_CHECKPOINT_MS", "30000")),
            python_bundle_time_ms=int(os.getenv("PROCESSOR_PYTHON_BUNDLE_TIME_MS", "100")),
            alert_topic=os.getenv("PROCESSOR_ALERT_TOPIC", "market.alerts"),
            window_hours=int(os.getenv("PROCESSOR_WINDOW_HOURS", "168")),
            drop_threshold_percent=float(os.getenv("PROCESSOR_DROP_THRESHOLD_PERCENT", "30")),
            key_dimensions=tuple(
                dimension.strip() for dimension in os.getenv("PROCESSOR_KEY_DIMENSIONS", "").split(",")
                if dimension.strip()
            ),
            min_observations=int(os.getenv("PROCESSOR_MIN_OBSERVATIONS", "10")),
            allowed_lateness_seconds=int(os.getenv("PROCESSOR_ALLOWED_LATENESS_SECONDS", "600")),
            max_event_age_seconds=int(os.getenv("PROCESSOR_MAX_EVENT_AGE_SECONDS", "900")),
            max_future_skew_seconds=int(os.getenv("PROCESSOR_MAX_FUTURE_SKEW_SECONDS", "60")),
            max_dedup_entries=int(os.getenv("PROCESSOR_MAX_DEDUP_ENTRIES", "10000")),
        )
