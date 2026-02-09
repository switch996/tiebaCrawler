"""Tieba crawler + lightweight API.

This package contains:
- Core crawler jobs (crawl threads, download images)
- SQLite repository
- Optional relay job to post labeled threads into weekly collection threads
- FastAPI app to expose everything to a frontend

The original crawler logic is kept intact; the API layer simply calls the same job functions.
"""

__all__ = [
    "settings",
    "logging_conf",
]
