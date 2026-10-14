from confluent_kafka import Producer

from .models import MarketEvent


class DeliveryError(RuntimeError):
    """Report a failed or incomplete Kafka delivery."""

    pass


class MarketPublisher:
    """Publish individual market events to a Kafka topic."""

    def __init__(self, broker: str, topic: str, producer: Producer | None = None) -> None:
        """Create a publisher for a broker and topic.

        Args:
            broker: Kafka bootstrap server address.
            topic: Destination topic name.
            producer: Optional Kafka producer, useful for testing.
        """
        self.producer = producer or Producer({"bootstrap.servers": broker})
        self.topic = topic
        self.errors: list[str] = []
        self.pending = 0

    def _delivered(self, error, _message) -> None:
        """Record the result of an asynchronous Kafka delivery.

        Args:
            error: Kafka delivery error, if any.
            _message: Delivered Kafka message, unused here.
        """
        self.pending -= 1
        if error is not None:
            self.errors.append(str(error))

    def publish(self, event: MarketEvent) -> None:
        """Queue one JSON event with its region and item key.

        Args:
            event: Enriched market observation to publish.
        """
        self.producer.poll(0)
        self._check_errors()
        for attempt in range(3):
            self.pending += 1
            try:
                self.producer.produce(
                    self.topic,
                    key=f"{event.location.region_id}:{event.item_id}",
                    value=event.model_dump_json(),
                    on_delivery=self._delivered,
                )
                break
            except BufferError as exc:
                self.pending -= 1
                if attempt == 2:
                    raise DeliveryError("Kafka queue remained full after 3 attempts") from exc
                self.producer.poll(1.0)
                self._check_errors()
            except Exception:
                self.pending -= 1
                raise
        self.producer.poll(0)
        self._check_errors()

    def _check_errors(self) -> None:
        """Raise the first recorded asynchronous delivery error."""
        if self.errors:
            raise DeliveryError(self.errors[0])

    def flush(self) -> None:
        """Flush queued messages and report incomplete deliveries."""
        remaining = self.producer.flush(10)
        self._check_errors()
        if remaining:
            raise DeliveryError(f"{remaining} Kafka message(s) undelivered after flush")
        if self.pending:
            raise DeliveryError(f"{self.pending} Kafka message(s) missing delivery confirmation")

    def close(self) -> None:
        """Confirm queued messages, then close the Kafka producer."""
        try:
            self.flush()
        finally:
            self.producer.close()
