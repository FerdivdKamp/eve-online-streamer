"""Synchronous command-line consumer for debugging market observations."""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition

DEFAULT_BROKER = "localhost:19092"
DEFAULT_TOPIC = "market-observations"
DEFAULT_GROUP = "eve-debug-consumer"
POLL_TIMEOUT_SECONDS = 0.5
logger = logging.getLogger(__name__)


def _is_number(value: Any) -> bool:
    """Check for a JSON number, excluding booleans.

    Args:
        value: Decoded JSON value.

    Returns:
        True when the value is an integer or float but not a boolean.
    """
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_event(payload: Any) -> None:
    """Check the required fields of a published market observation.

    Args:
        payload: Decoded JSON value to check.

    Raises:
        ValueError: If a required field is missing or has the wrong type.
    """
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")

    fields: dict[str, type | tuple[type, ...]] = {
        "item_id": int,
        "item_name": str,
        "volume": int,
        "source_timestamp": str,
        "fetched_at": str,
        "event_timestamp": str,
    }
    for name, expected_type in fields.items():
        if name not in payload or not isinstance(payload[name], expected_type):
            raise ValueError(f"{name} must be a {expected_type.__name__}")
        if expected_type is int and isinstance(payload[name], bool):
            raise ValueError(f"{name} must be an int")

    for name in ("current_price", "avg_price", "price_change"):
        if name not in payload or not _is_number(payload[name]):
            raise ValueError(f"{name} must be a number")

    location = payload.get("location")
    if not isinstance(location, dict):
        raise ValueError("location must be a JSON object")
    if not isinstance(location.get("region_id"), int) or isinstance(location["region_id"], bool):
        raise ValueError("location.region_id must be an int")
    if not isinstance(location.get("region_name"), str):
        raise ValueError("location.region_name must be a str")
    for name in ("station_id", "station_name"):
        value = location.get(name)
        expected = int if name == "station_id" else str
        if value is not None and (not isinstance(value, expected) or isinstance(value, bool)):
            raise ValueError(f"location.{name} must be a {expected.__name__} or null")

    for name in ("fetched_at", "event_timestamp"):
        try:
            datetime.fromisoformat(payload[name].replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{name} must be an ISO 8601 timestamp") from exc


def format_message(message: Message, validate: bool = False) -> str:
    """Decode one Kafka message into a printable JSON record.

    Args:
        message: Kafka message to inspect.
        validate: Check the market-event fields when true.

    Returns:
        A compact JSON record with Kafka metadata and the payload.

    Raises:
        ValueError: If the payload is missing, invalid JSON, or fails validation.
    """
    raw_value = message.value()
    if raw_value is None:
        raise ValueError("message has no JSON payload")
    payload = json.loads(raw_value.decode("utf-8"))
    if validate:
        validate_event(payload)

    _timestamp_type, timestamp_ms = message.timestamp()
    timestamp = (
        datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()
        if timestamp_ms is not None and timestamp_ms >= 0
        else None
    )
    raw_key = message.key()
    record = {
        "partition": message.partition(),
        "offset": message.offset(),
        "timestamp": timestamp,
        "key": raw_key.decode("utf-8", errors="replace") if raw_key is not None else None,
        "payload": payload,
    }
    return json.dumps(record, ensure_ascii=False, allow_nan=False)


def log_assignment(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
    """Log the partitions assigned when this consumer joins or rebalances.

    Args:
        _consumer: Kafka consumer supplied by the assignment callback.
        partitions: Topic partitions currently assigned to this consumer.
    """
    lines = [f"  {partition.topic}[{partition.partition}]" for partition in sorted(
        partitions, key=lambda partition: (partition.topic, partition.partition)
    )]
    logger.info("Assigned partitions:\n%s", "\n".join(lines) if lines else "  (none)")


def consume(
    *,
    broker: str = DEFAULT_BROKER,
    topic: str = DEFAULT_TOPIC,
    group: str = DEFAULT_GROUP,
    from_beginning: bool = False,
    max_messages: int | None = None,
    no_commit: bool = False,
    validate: bool = False,
    skip_invalid: bool = False,
) -> int:
    """Poll and print messages, committing each successful message synchronously.

    Args:
        broker: Kafka bootstrap address.
        topic: Topic to subscribe to.
        group: Persistent Kafka consumer-group ID.
        from_beginning: Start at the earliest offset if the group has no offset.
        max_messages: Stop after this many successful messages, if set.
        no_commit: Leave stored group offsets untouched.
        validate: Check market-event fields before printing.
        skip_invalid: Log invalid messages and continue past them.

    Returns:
        Number of messages printed.
    """
    if max_messages is not None and max_messages < 1:
        raise ValueError("max_messages must be positive")
    consumer = Consumer({
        "bootstrap.servers": broker,
        "group.id": group,
        "auto.offset.reset": "earliest" if from_beginning else "latest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })
    count = 0
    try:
        consumer.subscribe([topic], on_assign=log_assignment)
        while max_messages is None or count < max_messages:
            message = consumer.poll(POLL_TIMEOUT_SECONDS)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            try:
                record = format_message(message, validate=validate)
            except (ValueError, UnicodeError) as exc:
                logger.error(
                    "%s: topic=%s partition=%d offset=%d key=%r: %s",
                    "Validation failed" if validate else "Message decoding failed",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    message.key(),
                    exc,
                )
                if not skip_invalid:
                    raise
                if not no_commit:
                    consumer.commit(message=message, asynchronous=False)
                continue
            print(record, flush=True)
            if not no_commit:
                consumer.commit(message=message, asynchronous=False)
            count += 1
    finally:
        consumer.close()
    return count


def main() -> None:
    """Parse command-line options and run the debugging consumer."""
    parser = argparse.ArgumentParser(description="Inspect EVE market observations from Redpanda")
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka bootstrap address")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Topic to consume")
    parser.add_argument("--group", default=DEFAULT_GROUP, help="Persistent consumer-group ID")
    parser.add_argument("--from-beginning", action="store_true", help="Start earliest for a new group")
    parser.add_argument("--max-messages", type=int, help="Stop after this many messages")
    parser.add_argument("--no-commit", action="store_true", help="Do not advance group offsets")
    parser.add_argument("--validate", action="store_true", help="Check market-event fields")
    parser.add_argument("--skip-invalid", action="store_true", help="Log and skip invalid messages")
    args = parser.parse_args()
    if args.max_messages is not None and args.max_messages < 1:
        parser.error("--max-messages must be a positive integer")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        consume(
            broker=args.broker,
            topic=args.topic,
            group=args.group,
            from_beginning=args.from_beginning,
            max_messages=args.max_messages,
            no_commit=args.no_commit,
            validate=args.validate,
            skip_invalid=args.skip_invalid,
        )
    except KeyboardInterrupt:
        print("Consumer stopped", file=sys.stderr)
    except (KafkaException, ValueError, UnicodeError, OSError) as exc:
        print(f"Consumer failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
