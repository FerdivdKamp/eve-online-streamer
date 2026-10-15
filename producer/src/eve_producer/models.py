"""Define market API responses and enriched Kafka events."""

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field


class MarketLocation(BaseModel):
    """Item location as supplied by the market API."""

    model_config = ConfigDict(extra="allow")

    region_id: int
    region_name: str
    station_id: int | None = None
    station_name: str | None = None


class MarketItem(BaseModel):
    """Unmodified market observation from the API."""

    model_config = ConfigDict(extra="allow")

    item_id: int
    item_name: str
    current_price: float
    avg_price: float
    price_change: float
    volume: int
    timestamp: str
    location: MarketLocation


class Pagination(BaseModel):
    """Pagination metadata for an API response."""

    current_page: int = Field(default=1, ge=1)
    per_page: int = Field(ge=1)
    total_pages: int = Field(ge=0)
    total_items: int = Field(ge=0)


class MarketPage(BaseModel):
    """Market observations and pagination metadata for one page."""

    data: list[MarketItem]
    pagination: Pagination


class MarketEvent(BaseModel):
    """Market observation enriched for Kafka publication."""

    model_config = ConfigDict(extra="allow")

    item_id: int
    item_name: str
    current_price: float
    avg_price: float
    price_change: float
    volume: int
    source_timestamp: str
    location: MarketLocation
    fetched_at: datetime
    event_timestamp: datetime

    @classmethod
    def from_item(cls, item: MarketItem, fetched_at: datetime) -> "MarketEvent":
        """Convert a source observation into a publishable event.

        Args:
            item: Source market observation.
            fetched_at: UTC time when its API response arrived.

        Returns:
            Event with the original source time and a new publish time.
        """
        data = item.model_dump()
        data["source_timestamp"] = data.pop("timestamp")
        return cls.model_validate(
            {**data, "fetched_at": fetched_at, "event_timestamp": datetime.now(timezone.utc)}
        )
