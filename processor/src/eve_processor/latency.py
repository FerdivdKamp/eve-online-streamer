"""Classify producer-to-Flink evaluation latency for live observations."""

from dataclasses import dataclass
from datetime import datetime

from .config import ProcessorConfig
from .models import MarketObservation

SLA_SECONDS = 5.0
LATENCY_BUCKET_SECONDS = (0.5, 1, 2, 3, 4, 5, 7.5, 10, 20, 60, 300, 900)


@dataclass(frozen=True)
class LatencySample:
    """One observation's monitoring outcome after Flink evaluation."""

    category: str
    seconds: float | None = None


def classify_latency(
    observation: MarketObservation,
    status: str,
    completed_at: datetime,
    config: ProcessorConfig,
) -> LatencySample:
    """Exclude marked backfills and invalid clocks from the live SLA."""
    if completed_at.tzinfo is None or completed_at.utcoffset() is None:
        raise ValueError("completed_at must include a timezone")
    if observation.ingestion is not None and observation.ingestion.mode in {"backfill", "replay"}:
        return LatencySample(observation.ingestion.mode)
    seconds = (completed_at - observation.event_timestamp).total_seconds()
    if status == "future" or seconds < -config.max_future_skew_seconds:
        return LatencySample("future")
    return LatencySample("live", max(0.0, seconds))


def is_sla_success(sample: LatencySample, status: str) -> bool:
    """Count only timely live evaluations that were not rejected as stale."""
    return (
        sample.category == "live"
        and status != "stale"
        and sample.seconds is not None
        and sample.seconds <= SLA_SECONDS
    )
