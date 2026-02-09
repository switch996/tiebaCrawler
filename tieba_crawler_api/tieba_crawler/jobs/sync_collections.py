from __future__ import annotations

import logging
from typing import Optional

from tieba_crawler_api.tieba_crawler.db.repo import Repo
from tieba_crawler_api.tieba_crawler.settings import Settings
from tieba_crawler_api.tieba_crawler.tieba.mappers import detect_collection_from_title

log = logging.getLogger(__name__)

def sync_collections(*, settings: Settings, forum: str, days: int = 120, dry_run: bool = False) -> None:
    """Backfill collection metadata from existing threads in DB.

    Useful if you already had weekly collection threads in DB before upgrading to v0.2.
    """
    repo = Repo(settings=settings)
    repo.ensure_schema()

    now_ts = __import__("time").time()
    since_ts = int(now_ts) - int(days) * 86400

    rows = repo.conn().execute(
        """
        SELECT tid, title
        FROM threads
        WHERE fname=?
          AND create_time>=?
        ORDER BY create_time DESC
        """,
        (forum, since_ts),
    ).fetchall()

    updated = 0
    for r in rows:
        tid = int(r["tid"])
        title = r["title"] or ""
        is_coll, cat, y, w = detect_collection_from_title(title, settings.collection_rules)
        if not (is_coll and cat and y and w):
            continue
        if dry_run:
            log.info("[DRY] mark collection tid=%s category=%s year=%s week=%s title=%s", tid, cat, y, w, title)
            updated += 1
        else:
            repo.mark_thread_as_collection(tid, cat, int(y), int(w))
            updated += 1

    if not dry_run:
        repo.conn().commit()
    repo.close()
    log.info("sync_collections done. forum=%s updated=%s (dry_run=%s)", forum, updated, dry_run)
