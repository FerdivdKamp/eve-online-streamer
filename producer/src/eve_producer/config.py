from configparser import ConfigParser
from pathlib import Path
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


class IniSettingsSource(PydanticBaseSettingsSource):
    """Load packaged INI defaults for Pydantic settings."""

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        """Return an INI value for one settings field.

        Args:
            field: The Pydantic field being resolved.
            field_name: The settings field name.

        Returns:
            The raw value, field name, and complexity flag.
        """
        return self().get(field_name), field_name, False

    def __call__(self) -> dict[str, str]:
        """Read default values from the packaged config.ini file.

        Returns:
            INI options from the producer section.
        """
        parser = ConfigParser()
        path = Path(__file__).with_name("config.ini")
        if not parser.read(path, encoding="utf-8"):
            raise FileNotFoundError(f"Missing producer configuration: {path}")
        if not parser.has_section("producer"):
            raise ValueError(f"Missing [producer] section in {path}")
        return dict(parser.items("producer"))


class Settings(BaseSettings):
    """Producer settings resolved from CLI, environment, .env, and INI defaults."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_url: str
    page_size: int = Field(ge=1, le=100)
    poll_interval: float = Field(ge=0)
    threshold: float = Field(ge=1, le=90)
    kafka_broker: str
    kafka_topic: str
    events_per_second: float = Field(ge=0)
    request_timeout: float = Field(gt=0)
    max_retries: int = Field(ge=0)
    retry_backoff: float = Field(ge=0)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Place INI defaults below explicit and environment settings.

        Args:
            settings_cls: Settings type used to construct the INI source.
            init_settings: Explicit constructor arguments.
            env_settings: Environment variables.
            dotenv_settings: Values from the local .env file.
            file_secret_settings: Pydantic file secrets source.

        Returns:
            Settings sources ordered from highest to lowest priority.
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
            IniSettingsSource(settings_cls),
        )
