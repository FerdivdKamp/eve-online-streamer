"""Pure parsing and routing logic for Kafka values."""

import json
from hashlib import sha256
from dataclasses import dataclass

from pydantic import ValidationError

from .models import MarketObservation


@dataclass(frozen=True)
class ParsedRecord:
    """One valid observation or a bounded diagnostic message."""

    observation: MarketObservation | None
    error: str | None


def parse_observation(raw: str) -> ParsedRecord:
    """Validate a Kafka JSON value without terminating the Flink job.

    Args:
        raw: UTF-8 JSON value decoded by KafkaSource.

    Returns:
        A valid observation or a concise validation diagnostic.
    """
    try:
        data = json.loads(raw)
    except (TypeError, ValueError) as exc:
        return ParsedRecord(None, f"invalid JSON: {exc}")
    if not isinstance(data, dict):
        return ParsedRecord(None, "JSON root must be an object")
    try:
        return ParsedRecord(MarketObservation.model_validate(data), None)
    except ValidationError as exc:
        details = "; ".join(
            f"{'.'.join(map(str, issue['loc']))}: {issue['msg']}"
            for issue in exc.errors()
        )
        return ParsedRecord(None, details[:1000])


def market_key(observation: MarketObservation) -> tuple[int, int]:
    """Match the producer's region:item Kafka key semantics."""
    return observation.location.region_id, observation.item_id


def classify(raw: str) -> tuple[str, str]:
    """Make a serializable valid/invalid envelope for DataStream branches."""
    result = parse_observation(raw)
    if result.observation is None:
        fingerprint = sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:12]
        preview = raw[:120].replace("\n", " ").replace("\r", " ")
        return "invalid", f"sha256={fingerprint} reason={result.error} preview={preview!r}"
    return "valid", result.observation.model_dump_json()
