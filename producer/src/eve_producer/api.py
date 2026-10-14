import time
from datetime import datetime, timezone
from collections.abc import Iterator

import requests

from .models import MarketItem, MarketPage


class MarketClient:
    """Fetch paginated market observations from the mock API."""

    def __init__(
        self, url: str, page_size: int, threshold: float, timeout: float,
        max_retries: int, retry_backoff: float, session: requests.Session | None = None,
    ) -> None:
        """Configure the API request and retry policy.

        Args:
            url: Full URL of the market endpoint.
            page_size: Number of observations requested per page.
            threshold: API threshold parameter, without local filtering.
            timeout: Per-request timeout in seconds.
            max_retries: Maximum retries after the first request.
            retry_backoff: Initial retry delay in seconds.
            session: Optional requests session, useful for testing.
        """
        self.url = url
        self.page_size = page_size
        self.threshold = threshold
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.session = session or requests.Session()

    def _fetch_page(self, page: int) -> tuple[MarketPage, datetime]:
        """Fetch one page, retrying transient failures.

        Args:
            page: One-based page number.

        Returns:
            Validated page and its UTC response time.
        """
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(
                    self.url,
                    params={"page": page, "per_page": self.page_size, "threshold": self.threshold},
                    timeout=self.timeout,
                )
                fetched_at = datetime.now(timezone.utc)
                response.raise_for_status()
                return MarketPage.model_validate(response.json()), fetched_at
            except (requests.Timeout, requests.ConnectionError) as exc:
                error = exc
            except requests.HTTPError as exc:
                if response.status_code != 429 and response.status_code < 500:
                    raise
                error = exc
            if attempt == self.max_retries:
                raise error
            time.sleep(min(self.retry_backoff * 2 ** min(attempt, 10), 30.0))
        raise AssertionError("unreachable")

    def observations(self) -> Iterator[tuple[MarketItem, datetime]]:
        """Yield every observation and its response time across all pages.

        Yields:
            A market item and the UTC time its page was received.
        """
        page = 1
        while True:
            result, fetched_at = self._fetch_page(page)
            pagination = result.pagination
            if (
                page == 1
                and pagination.total_pages == 0
                and pagination.total_items == 0
                and not result.data
                and pagination.current_page == 1
                and pagination.per_page == self.page_size
            ):
                return
            if (
                pagination.current_page != page
                or pagination.per_page != self.page_size
                or pagination.total_pages < page
                or pagination.total_pages
                != (pagination.total_items + pagination.per_page - 1) // pagination.per_page
                or (pagination.total_items == 0 and result.data)
            ):
                raise ValueError(f"Inconsistent pagination on page {page}: {pagination}")
            for item in result.data:
                yield item, fetched_at
            if page >= pagination.total_pages:
                break
            page += 1

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self.session.close()
