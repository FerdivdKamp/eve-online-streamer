"""Limit the rate of market event publication."""

import time


class RateLimiter:
    """Space Kafka publications by a configured minimum interval."""

    def __init__(self, events_per_second: float) -> None:
        """Set the publication rate; zero disables waiting.

        Args:
            events_per_second: Maximum publications per second.
        """
        if events_per_second < 0:
            raise ValueError("events_per_second must be nonnegative")
        self.interval = 1 / events_per_second if events_per_second else 0
        self.next_at: float | None = None

    def wait(self) -> None:
        """Wait until the next publication slot is available."""
        if not self.interval:
            return
        now = time.monotonic()
        if self.next_at is not None and now < self.next_at:
            time.sleep(self.next_at - now)
            now = time.monotonic()
        self.next_at = now + self.interval
