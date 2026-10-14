import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from .api import MarketClient
from .config import Settings
from .models import MarketEvent
from .publisher import MarketPublisher
from .throttle import RateLimiter

logger = logging.getLogger(__name__)


def run(
    settings: Settings,
    once: bool = False,
    rate: float | None = None,
    max_events: int | None = None,
) -> None:
    """Poll and publish market data until one cycle or interruption.

    Args:
        settings: Validated producer configuration.
        once: Exit after one complete paginated poll.
        rate: Optional CLI override for events per second.
        max_events: Stop after publishing this many observations.
    """
    if max_events is not None and max_events < 1:
        raise ValueError("max_events must be positive")
    limiter = RateLimiter(settings.events_per_second if rate is None else rate)
    client = MarketClient(
        settings.api_url, settings.page_size, settings.threshold,
        settings.request_timeout, settings.max_retries, settings.retry_backoff,
    )
    try:
        publisher = MarketPublisher(settings.kafka_broker, settings.kafka_topic)
        try:
            published = 0
            while True:
                for item, fetched_at in client.observations():
                    limiter.wait()
                    publisher.publish(MarketEvent.from_item(item, fetched_at))
                    published += 1
                    if max_events is not None and published >= max_events:
                        return
                if once:
                    break
                publisher.flush()
                time.sleep(settings.poll_interval)
        finally:
            logger.info("Producer shutdown requested")
            if sys.exc_info()[0] is KeyboardInterrupt:
                publisher.close(abort=True)
            else:
                publisher.close()
    finally:
        client.close()


def run_api_only(settings: Settings, max_events: int | None = None) -> None:
    """Fetch one full API snapshot and print raw observations as JSON Lines.

    Args:
        settings: Validated API configuration.
        max_events: Stop after printing this many observations.
    """
    if max_events is not None and max_events < 1:
        raise ValueError("max_events must be positive")
    client = MarketClient(
        settings.api_url, settings.page_size, settings.threshold,
        settings.request_timeout, settings.max_retries, settings.retry_backoff,
    )
    try:
        shown = 0
        for item, _fetched_at in client.observations():
            print(item.model_dump_json())
            shown += 1
            if max_events is not None and shown >= max_events:
                return
    finally:
        logger.info("Producer shutdown requested")
        client.close()


def run_kafka_only(
    settings: Settings,
    event_file: Path,
    max_events: int | None = None,
    rate: float | None = None,
) -> None:
    """Publish a saved event without contacting the market API.

    Args:
        settings: Validated Kafka configuration.
        event_file: Path to a JSON file containing one complete market event.
        max_events: Number of copies to publish; defaults to one.
        rate: Optional CLI override for events per second.
    """
    if max_events is not None and max_events < 1:
        raise ValueError("max_events must be positive")
    event = MarketEvent.model_validate_json(event_file.read_text(encoding="utf-8"))
    limiter = RateLimiter(settings.events_per_second if rate is None else rate)
    publisher = MarketPublisher(settings.kafka_broker, settings.kafka_topic)
    try:
        for _ in range(1 if max_events is None else max_events):
            limiter.wait()
            publisher.publish(event.model_copy(update={"event_timestamp": datetime.now(timezone.utc)}))
    finally:
        logger.info("Producer shutdown requested")
        if sys.exc_info()[0] is KeyboardInterrupt:
            publisher.close(abort=True)
        else:
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
    parser.add_argument("--max-events", type=int, help="Stop after this many observations")
    args = parser.parse_args()
    if args.max_events is not None and args.max_events < 1:
        parser.error("--max-events must be a positive integer")
    if args.kafka_only and args.event_file is None:
        parser.error("--kafka-only requires --event-file")
    if args.event_file is not None and not args.kafka_only:
        parser.error("--event-file requires --kafka-only")
    logging.basicConfig(level=logging.INFO)
    try:
        settings = Settings()
        if args.api_only:
            run_api_only(settings, max_events=args.max_events)
        elif args.kafka_only:
            run_kafka_only(settings, args.event_file, max_events=args.max_events, rate=args.rate)
        else:
            run(settings, once=args.once, rate=args.rate, max_events=args.max_events)
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("Producer failed")
        raise SystemExit(1)
    finally:
        logger.info("Producer stopped")
