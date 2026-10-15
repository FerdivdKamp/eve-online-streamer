"""The producer's published market event contract."""

from datetime import datetime

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
        return value


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

    @field_validator("item_id", "volume", mode="before")
    @classmethod
    def require_integer(cls, value: object) -> object:
        """Require actual JSON integers for identifiers and volume."""
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("must be an integer")
        return value

    @field_validator("current_price", "avg_price", "price_change", mode="before")
    @classmethod
    def require_number(cls, value: object) -> object:
        """Accept JSON numbers but reject numeric strings and booleans."""
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("must be a number")
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
