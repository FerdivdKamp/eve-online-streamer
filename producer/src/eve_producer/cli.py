import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from .api import MarketClient
from .config import Settings
from .models import MarketEvent
from .publisher import MarketPublisher
from .throttle import RateLimiter


def run(settings: Settings, once: bool = False, rate: float | None = None) -> None:
    """Poll and publish market data until one cycle or interruption.

    Args:
        settings: Validated producer configuration.
        once: Exit after one complete paginated poll.
        rate: Optional CLI override for events per second.
    """
    limiter = RateLimiter(settings.events_per_second if rate is None else rate)
    client = MarketClient(
        settings.api_url, settings.page_size, settings.threshold,
        settings.request_timeout, settings.max_retries, settings.retry_backoff,
    )
    try:
        publisher = MarketPublisher(settings.kafka_broker, settings.kafka_topic)
        try:
            while True:
                for item, fetched_at in client.observations():
                    limiter.wait()
                    publisher.publish(MarketEvent.from_item(item, fetched_at))
                if once:
                    break
                publisher.flush()
                time.sleep(settings.poll_interval)
        finally:
            publisher.close()
    finally:
        client.close()


def run_api_only(settings: Settings) -> None:
    """Fetch one full API snapshot and print raw observations as JSON Lines.

    Args:
        settings: Validated API configuration.
    """
    client = MarketClient(
        settings.api_url, settings.page_size, settings.threshold,
        settings.request_timeout, settings.max_retries, settings.retry_backoff,
    )
    try:
        for item, _fetched_at in client.observations():
            print(item.model_dump_json())
    finally:
        client.close()


def run_kafka_only(settings: Settings, event_file: Path) -> None:
    """Publish one saved event without contacting the market API.

    Args:
        settings: Validated Kafka configuration.
        event_file: Path to a JSON file containing one complete market event.
    """
    event = MarketEvent.model_validate_json(event_file.read_text(encoding="utf-8"))
    publisher = MarketPublisher(settings.kafka_broker, settings.kafka_topic)
    try:
        publisher.publish(event.model_copy(update={"event_timestamp": datetime.now(timezone.utc)}))
    finally:
        publisher.close()


def main() -> None:
    """Parse CLI options and run the market producer."""
    parser = argparse.ArgumentParser(description="Publish EVE market observations to Redpanda")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--api-only", action="store_true", help="Fetch once and print JSON Lines; no Kafka")
    modes.add_argument("--kafka-only", action="store_true", help="Publish one saved event; no API")
    parser.add_argument("--event-file", type=Path, help="JSON event file for --kafka-only")
    parser.add_argument("--once", action="store_true", help="Poll all pages once and exit")
    parser.add_argument("--rate", type=float, help="Events per second; 0 disables throttling")
    args = parser.parse_args()
    if args.kafka_only and args.event_file is None:
        parser.error("--kafka-only requires --event-file")
    if args.event_file is not None and not args.kafka_only:
        parser.error("--event-file requires --kafka-only")
    logging.basicConfig(level=logging.INFO)
    try:
        settings = Settings()
        if args.api_only:
            run_api_only(settings)
        elif args.kafka_only:
            run_kafka_only(settings, args.event_file)
        else:
            run(settings, once=args.once, rate=args.rate)
    except KeyboardInterrupt:
        logging.info("Stopped")
    except Exception:
        logging.exception("Producer failed")
        raise SystemExit(1)
