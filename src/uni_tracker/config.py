from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = Field(default="development", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    database_url: str = Field(
        default="postgresql+psycopg://uni_tracker:uni_tracker@localhost:5432/uni_tracker",
        alias="DATABASE_URL",
    )
    raw_storage_path: Path = Field(default=Path("artifacts/runtime"), alias="RAW_STORAGE_PATH")
    sync_courses_interval_minutes: int = Field(default=30, alias="SYNC_COURSES_INTERVAL_MINUTES")
    sync_contents_interval_minutes: int = Field(default=60, alias="SYNC_CONTENTS_INTERVAL_MINUTES")

    moodle_base_url: str = Field(alias="MOODLE_BASE_URL")
    moodle_username: str = Field(alias="MOODLE_USERNAME")
    moodle_password: str = Field(alias="MOODLE_PASSWORD")
    moodle_service: str = Field(default="moodle_mobile_app", alias="MOODLE_SERVICE")

    telegram_bot_token: str | None = Field(default=None, alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str | None = Field(default=None, alias="TELEGRAM_CHAT_ID")


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.raw_storage_path.mkdir(parents=True, exist_ok=True)
    return settings
