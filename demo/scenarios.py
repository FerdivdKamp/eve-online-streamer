"""Publish repeatable market scenarios and verify Flink consumed them."""

import json
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from eve_producer.models import MarketEvent
from eve_producer.publisher import MarketPublisher
from eve_producer.throttle import RateLimiter


BROKER = os.getenv("DEMO_BROKER", "redpanda:9092")
INPUT_TOPIC = os.getenv("DEMO_INPUT_TOPIC", "market-observations")
ALERT_TOPIC = os.getenv("DEMO_ALERT_TOPIC", "market.alerts")
PROCESSOR_GROUP = os.getenv("DEMO_PROCESSOR_GROUP", "eve-market-processor-v1")
DROP_THRESHOLD_PERCENT = float(os.getenv("DEMO_DROP_THRESHOLD_PERCENT", "30"))
REGION_ID = 10000002
STATION_ID = 60003760


class TrackingPublisher(MarketPublisher):
    """Remember the next offset after each confirmed publication."""

    def __init__(self) -> None:
        super().__init__(BROKER, INPUT_TOPIC, Producer({
            "bootstrap.servers": BROKER,
            "enable.idempotence": True,
            "acks": "all",
        }))
        self.end_offsets: dict[int, int] = {}

    def _delivered(self, error, message) -> None:
        super()._delivered(error, message)
        if error is None:
            partition = message.partition()
            self.end_offsets[partition] = max(self.end_offsets.get(partition, 0), message.offset() + 1)


def new_item_id(seed: int, index: int = 0) -> int:
    """Use a fresh synthetic market identity while staying within Flink's INT key."""
    return 1_000_000_000 + (seed + index) % 1_000_000_000


def observation(run_id: str, index: int, item_id: int, price: float) -> MarketEvent:
    """Build a live observation with a unique source event ID."""
    now = datetime.now(timezone.utc)
    return MarketEvent.model_validate({
        "item_id": item_id,
        "item_name": f"Demo item {item_id}",
        "current_price": price,
        "avg_price": 100.0,
        "price_change": price - 100.0,
        "volume": 100,
        "source_timestamp": now.isoformat(),
        "fetched_at": now,
        "event_timestamp": now,
        "source_event_id": f"{run_id}-{index}",
        "market_side": "buy",
        "location": {
            "region_id": REGION_ID,
            "region_name": "The Forge",
            "station_id": STATION_ID,
            "station_name": "Jita",
        },
    })


def alert_consumer(run_id: str) -> Consumer:
    """Start at the end of each alert partition before publishing the run."""
    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": f"demo-alert-check-{run_id}",
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",
    })
    metadata = consumer.list_topics(ALERT_TOPIC, timeout=10)
    partitions = metadata.topics[ALERT_TOPIC].partitions
    assignments = []
    for partition in partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(ALERT_TOPIC, partition), timeout=10, cached=False)
        assignments.append(TopicPartition(ALERT_TOPIC, partition, high))
    consumer.assign(assignments)
    return consumer


def committed_past(admin: AdminClient, end_offsets: dict[int, int]) -> bool:
    """Check Flink's checkpointed Kafka positions for every touched partition."""
    request = ConsumerGroupTopicPartitions(
        PROCESSOR_GROUP,
        [TopicPartition(INPUT_TOPIC, partition) for partition in end_offsets],
    )
    result = admin.list_consumer_group_offsets([request], request_timeout=5)[PROCESSOR_GROUP].result(timeout=6)
    actual = {part.partition: part.offset for part in result.topic_partitions if part.topic == INPUT_TOPIC}
    return all(actual.get(partition, -1) >= offset for partition, offset in end_offsets.items())


def wait_for_processing(end_offsets: dict[int, int], timeout: float, consumer: Consumer | None = None, run_id: str | None = None) -> list[dict]:
    """Wait for Flink's committed offsets, collecting this run's alerts."""
    admin = AdminClient({"bootstrap.servers": BROKER})
    deadline = time.monotonic() + timeout
    alerts: list[dict] = []
    while time.monotonic() < deadline:
        if consumer is not None:
            message = consumer.poll(0.25)
            if message is not None:
                if message.error():
                    raise RuntimeError(str(message.error()))
                payload = json.loads(message.value())
                if str(payload.get("source_event_id", "")).startswith(f"{run_id}-"):
                    alerts.append(payload)
        else:
            time.sleep(0.25)
        if committed_past(admin, end_offsets):
            break
    else:
        raise TimeoutError("Flink did not commit the published input offsets before the timeout")

    if consumer is not None:
        # Once Flink committed the input, drain the alert topic through its
        # current high watermark before asserting that there is exactly one.
        assignments = consumer.assignment()
        limits = {
            part.partition: consumer.get_watermark_offsets(part, timeout=10, cached=False)[1]
            for part in assignments
        }
        drain_deadline = time.monotonic() + 10
        while time.monotonic() < drain_deadline:
            positions = {part.partition: part.offset for part in consumer.position(assignments)}
            if all(positions.get(partition, -1) >= high for partition, high in limits.items()):
                break
            message = consumer.poll(0.25)
            if message is not None:
                if message.error():
                    raise RuntimeError(str(message.error()))
                payload = json.loads(message.value())
                if str(payload.get("source_event_id", "")).startswith(f"{run_id}-"):
                    alerts.append(payload)
        else:
            raise TimeoutError("Could not drain the alert topic before asserting the result")
    return alerts


def run_alert(run_id: str, progress) -> dict:
    """Publish 90 baseline, one threshold, and nine recovery observations."""
    item_id = new_item_id(uuid4().int)
    consumer = alert_consumer(run_id)
    publisher = TrackingPublisher()
    try:
        for index in range(100):
            price = 100.0 * (1 - DROP_THRESHOLD_PERCENT / 100) if index == 90 else 100.0
            publisher.publish(observation(run_id, index, item_id, price))
            progress(index + 1)
        publisher.flush()
        alerts = wait_for_processing(publisher.end_offsets, 90, consumer, run_id)
        passed = len(alerts) == 1 and alerts[0].get("source_event_id") == f"{run_id}-90"
        return {
            "passed": passed,
            "item_id": item_id,
            "expected_alerts": 1,
            "observed_alerts": len(alerts),
            "alert_ids": [alert.get("alert_id") for alert in alerts],
            "detail": "Exactly event 91 alerted" if passed else "Expected exactly one alert from event 91",
        }
    finally:
        consumer.close()
        publisher.close(abort=True)


def run_load(run_id: str, count: int, rate: float, keys: int, progress) -> dict:
    """Publish a bounded multi-key load and wait for Flink to process it."""
    seed = uuid4().int
    limiter = RateLimiter(rate)
    publisher = TrackingPublisher()
    started = time.monotonic()
    try:
        for index in range(count):
            limiter.wait()
            item_id = new_item_id(seed, index % keys)
            publisher.publish(observation(run_id, index, item_id, 100.0))
            if (index + 1) % 100 == 0 or index + 1 == count:
                progress(index + 1)
        publisher.flush()
        publish_seconds = time.monotonic() - started
        wait_for_processing(publisher.end_offsets, 120)
        return {
            "passed": True,
            "published": count,
            "keys": keys,
            "publish_seconds": round(publish_seconds, 3),
            "achieved_publish_rate": round(count / max(publish_seconds, 0.001), 1),
            "detail": "Flink committed all published input offsets",
        }
    finally:
        publisher.close(abort=True)
