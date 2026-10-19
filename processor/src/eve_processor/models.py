"""The producer's published market event contract."""

from datetime import datetime
from math import isfinite
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator


class MarketLocation(BaseModel):
    """Market region and optional station metadata."""

    model_config = ConfigDict(extra="allow")

    region_id: int
    region_name: str
    station_id: int | None = None
    station_name: str | None = None

    @field_validator("region_id", "station_id", mode="before")
    @classmethod
    def require_integer(cls, value: object) -> object:
        """Reject JSON strings and booleans masquerading as IDs."""
        if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
            raise ValueError("must be an integer")
        if value is not None and value < 1:
            raise ValueError("must be positive")
        return value


class IngestionMetadata(BaseModel):
    """Optional marker that keeps historical backfills out of live state."""

    mode: Literal["live", "backfill", "replay"] = "live"


class MarketObservation(BaseModel):
    """Validated observation published by the existing producer."""

    model_config = ConfigDict(extra="allow")

    item_id: int
    item_name: str
    current_price: float
    avg_price: float
    price_change: float
    volume: int
    source_timestamp: str
    event_timestamp: datetime
    fetched_at: datetime
    location: MarketLocation
    source_event_id: str | None = None
    market_side: str | None = None
    order_type: str | None = None
    ingestion: IngestionMetadata | None = None

    @field_validator("item_id", "volume", mode="before")
    @classmethod
    def require_integer(cls, value: object) -> object:
        """Require actual JSON integers for identifiers and volume."""
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("must be an integer")
        return value

    @field_validator("item_id")
    @classmethod
    def require_positive_item_id(cls, value: int) -> int:
        """Keep item identity in the valid EVE ID range."""
        if value < 1:
            raise ValueError("must be positive")
        return value

    @field_validator("volume")
    @classmethod
    def require_nonnegative_volume(cls, value: int) -> int:
        """Reject negative transaction volume."""
        if value < 0:
            raise ValueError("must not be negative")
        return value

    @field_validator("current_price", "avg_price", "price_change", mode="before")
    @classmethod
    def require_number(cls, value: object) -> object:
        """Accept JSON numbers but reject numeric strings and booleans."""
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("must be a number")
        if not isfinite(value):
            raise ValueError("must be finite")
        return value

    @field_validator("current_price")
    @classmethod
    def require_nonnegative_price(cls, value: float) -> float:
        """Reject prices that cannot represent an observed market price."""
        if value < 0:
            raise ValueError("must not be negative")
        return value

    @field_validator("source_event_id", "market_side", "order_type", mode="before")
    @classmethod
    def require_optional_text(cls, value: object) -> object:
        """Keep optional identity fields explicit rather than coercing them."""
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise ValueError("must be a nonempty string or null")
        return value

    @field_validator("source_timestamp")
    @classmethod
    def validate_source_timestamp(cls, value: str) -> str:
        """Check the source timestamp while preserving its original text."""
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value

    @field_validator("event_timestamp", "fetched_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Reject ambiguous timestamps with no timezone."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        return value
