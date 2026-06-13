"""
Horizon v4 — runtime configuration.
Small, dependency-light settings layer with sensible defaults.
"""
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

    app_name: str = "Horizon"
    app_version: str = "5.0.0"
    app_description: str = (
        "Horizon v5 — live career intelligence for 20+ tracks: engineering, internships, product, design, blockchain, quantum computing, game dev, and more."
    )
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"

    database_path: str = Field(default_factory=lambda: str(Path(__file__).parent / "horizon.db"))
    cache_ttl_hours: int = 24
    scraper_timeout_seconds: int = 15
    rate_limit_per_minute: int = 20
    max_roadmap_generation_seconds: int = 180

    warm_nlp_models: bool = False
    enable_optional_transformers: bool = False


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
