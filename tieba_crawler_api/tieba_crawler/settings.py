from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any

def _env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default

def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None or v == "":
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}

def _env_json(key: str, default: Any):
    v = os.getenv(key)
    if not v:
        return default
    try:
        return json.loads(v)
    except Exception:
        return default

@dataclass(frozen=True)
class Settings:
    # storage / db
    db_url: str
    data_dir: Path

    # auth (optional)
    bduss: str
    stoken: str

    # defaults
    default_forum: str

    # timezone used for week calculation
    timezone: str

    # crawler
    threads_rn: int
    initial_hours: int
    overlap_seconds: int
    max_pages: int

    # tieba client
    try_ws: bool
    request_attempts: int
    page_sleep_ms_min: int
    page_sleep_ms_max: int

    # weekly collection detection: {category: [keyword1, keyword2]}
    collection_rules: dict

    # relay
    relay_mode: str
    relay_max_posts: int
    relay_min_interval_seconds: int
    relay_max_text_chars: int
    relay_max_images: int
    relay_lookback_days: int

    @staticmethod
    def from_env() -> "Settings":
        collection_rules = _env_json("COLLECTION_RULES_JSON", {})
        # normalize to dict[str, list[str]]
        if not isinstance(collection_rules, dict):
            collection_rules = {}
        else:
            norm = {}
            for k, v in collection_rules.items():
                if isinstance(v, list):
                    norm[str(k)] = [str(x) for x in v if str(x).strip()]
                elif isinstance(v, str):
                    norm[str(k)] = [v]
            collection_rules = norm

        return Settings(
            db_url=os.getenv("DB_URL", "sqlite:///data/tieba.db"),
            data_dir=Path(os.getenv("DATA_DIR", "data")),
            bduss=os.getenv("BDUSS", ""),
            stoken=os.getenv("STOKEN", ""),
            default_forum=os.getenv("FORUM", ""),
            timezone=os.getenv("TIMEZONE", "Asia/Shanghai"),
            threads_rn=_env_int("THREADS_RN", 50),
            initial_hours=_env_int("INITIAL_HOURS", 24),
            overlap_seconds=_env_int("OVERLAP_SECONDS", 3600),
            max_pages=_env_int("MAX_PAGES", 200),
            try_ws=_env_bool("TRY_WS", False),
            request_attempts=_env_int("REQUEST_ATTEMPTS", 5),
            page_sleep_ms_min=_env_int("PAGE_SLEEP_MS_MIN", 200),
            page_sleep_ms_max=_env_int("PAGE_SLEEP_MS_MAX", 800),
            collection_rules = _env_json("COLLECTION_RULES_JSON", collection_rules),
            relay_mode=os.getenv("RELAY_MODE", "link"),
            relay_max_posts=_env_int("RELAY_MAX_POSTS", 2),
            relay_min_interval_seconds=_env_int("RELAY_MIN_INTERVAL_SECONDS", 120),
            relay_max_text_chars=_env_int("RELAY_MAX_TEXT_CHARS", 300),
            relay_max_images=_env_int("RELAY_MAX_IMAGES", 3),
            relay_lookback_days=_env_int("RELAY_LOOKBACK_DAYS", 21),
        )
