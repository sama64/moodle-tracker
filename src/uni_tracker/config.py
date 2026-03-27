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
    raw_storage_path: Path = Field(default=Path("data/uni-tracker/artifacts/runtime"), alias="RAW_STORAGE_PATH")
    sync_courses_interval_minutes: int = Field(default=30, alias="SYNC_COURSES_INTERVAL_MINUTES")
    sync_contents_interval_minutes: int = Field(default=60, alias="SYNC_CONTENTS_INTERVAL_MINUTES")
    file_download_limit_per_run: int = Field(default=5, alias="FILE_DOWNLOAD_LIMIT_PER_RUN")
    daily_digest_hour: int = Field(default=7, alias="DAILY_DIGEST_HOUR")
    stale_sync_threshold_hours: int = Field(default=6, alias="STALE_SYNC_THRESHOLD_HOURS")

    moodle_base_url: str = Field(alias="MOODLE_BASE_URL")
    moodle_username: str = Field(alias="MOODLE_USERNAME")
    moodle_password: str = Field(alias="MOODLE_PASSWORD")
    moodle_service: str = Field(default="moodle_mobile_app", alias="MOODLE_SERVICE")

    telegram_bot_token: str | None = Field(default=None, alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str | None = Field(default=None, alias="TELEGRAM_CHAT_ID")
    telegram_polling_enabled: bool = Field(default=True, alias="TELEGRAM_POLLING_ENABLED")
    telegram_polling_interval_seconds: int = Field(default=30, alias="TELEGRAM_POLLING_INTERVAL_SECONDS")
    enable_llm: bool = Field(default=False, alias="ENABLE_LLM")
    llm_body_char_limit: int = Field(default=12000, alias="LLM_BODY_CHAR_LIMIT")
    llm_request_max_attempts: int = Field(default=3, alias="LLM_REQUEST_MAX_ATTEMPTS")
    llm_retry_base_delay_seconds: float = Field(default=2.0, alias="LLM_RETRY_BASE_DELAY_SECONDS")
    llm_retry_max_delay_seconds: float = Field(default=30.0, alias="LLM_RETRY_MAX_DELAY_SECONDS")
    llm_retry_cooldown_minutes: int = Field(default=180, alias="LLM_RETRY_COOLDOWN_MINUTES")
    nvidia_api_key: str | None = Field(default=None, alias="NVIDIA_API_KEY")
    nvidia_api_url: str = Field(
        default="https://integrate.api.nvidia.com/v1/chat/completions",
        alias="NVIDIA_API_URL",
    )
    nvidia_model: str = Field(default="moonshotai/kimi-k2.5", alias="NVIDIA_MODEL")


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    try:
        settings.raw_storage_path.mkdir(parents=True, exist_ok=True)
    except OSError:
        fallback = Path("data/uni-tracker/artifacts/runtime")
        fallback.mkdir(parents=True, exist_ok=True)
        settings.raw_storage_path = fallback
    return settings
