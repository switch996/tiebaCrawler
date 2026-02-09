from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Optional

from tieba_crawler_api.tieba_crawler.db.repo import Repo
from tieba_crawler_api.tieba_crawler.settings import Settings
from tieba_crawler_api.tieba_crawler.tieba.client import TiebaAPI
from tieba_crawler_api.tieba_crawler.tieba.mappers import thread_to_row, image_tasks_from_thread, detect_collection_from_title

log = logging.getLogger(__name__)

async def crawl_threads(
    *,
    forum: str,
    settings: Settings,
    rn: Optional[int] = None,
    initial_hours: Optional[int] = None,
    overlap_seconds: Optional[int] = None,
    max_pages: Optional[int] = None,
) -> None:
    repo = Repo(settings=settings)
    repo.ensure_schema()

    rn = rn if rn is not None else settings.threads_rn
    initial_hours = initial_hours if initial_hours is not None else settings.initial_hours
    overlap_seconds = overlap_seconds if overlap_seconds is not None else settings.overlap_seconds
    max_pages = max_pages if max_pages is not None else settings.max_pages

    api = TiebaAPI(
        bduss=settings.bduss,
        stoken=settings.stoken,
        try_ws=settings.try_ws,
        request_attempts=settings.request_attempts,
    )

    now_ts = int(time.time())
    state = repo.get_forum_state(forum)
    last_crawl_ts = int(state["last_crawl_ts"]) if state and state["last_crawl_ts"] else None

    if last_crawl_ts is None:
        since_ts = now_ts - initial_hours * 3600
        log.info("First run for forum=%s since_ts=%s (initial_hours=%s)", forum, since_ts, initial_hours)
    else:
        since_ts = max(0, last_crawl_ts - overlap_seconds)
        log.info("Incremental run for forum=%s since_ts=%s (last=%s overlap=%s)", forum, since_ts, last_crawl_ts, overlap_seconds)

    pn = 1
    max_seen_ts = last_crawl_ts or 0

    total_threads_upsert = 0
    total_images_enqueued = 0
    total_collections_detected = 0

    while pn <= max_pages:
        if pn > 1:
            sleep_ms = random.randint(settings.page_sleep_ms_min, settings.page_sleep_ms_max)
            await asyncio.sleep(sleep_ms / 1000)

        threads = await api.get_threads_page_with_retry(forum, pn=pn, rn=rn)

        if getattr(threads, "err", None):
            raise threads.err

        objs = list(getattr(threads, "objs", []) or [])
        if not objs:
            log.info("No threads returned (forum=%s pn=%s). Stop.", forum, pn)
            break

        any_candidate = False
        all_old = True

        for th in objs:
            # don't let pinned threads affect stop condition
            if bool(getattr(th, "is_top", False)):
                continue

            any_candidate = True
            cts = int(getattr(th, "create_time", 0) or 0)
            if cts > max_seen_ts:
                max_seen_ts = cts

            if cts > since_ts:
                all_old = False

                row = thread_to_row(th)

                # auto-detect weekly collection threads from title
                title = row.get("title") or ""
                is_coll, coll_cat, y, w = detect_collection_from_title(title, settings.collection_rules)
                if is_coll and coll_cat and y and w:
                    row["thread_role"] = "collection"
                    row["category"] = coll_cat
                    row["collection_year"] = int(y)
                    row["collection_week"] = int(w)
                    total_collections_detected += 1

                repo.upsert_thread(row)
                total_threads_upsert += 1

                imgs = image_tasks_from_thread(th)
                for img in imgs:
                    repo.upsert_image_task(img)
                total_images_enqueued += len(imgs)

        repo.conn().commit()

        if not any_candidate:
            log.info("Page has only top threads? (forum=%s pn=%s). Continue.", forum, pn)
            pn += 1
            continue

        log.info(
            "Crawled forum=%s pn=%s threads=%s has_more=%s all_old=%s max_seen_ts=%s",
            forum,
            pn,
            len(objs),
            bool(getattr(threads, "has_more", False)),
            all_old,
            max_seen_ts,
        )

        if all_old:
            log.info("Reached history boundary (forum=%s pn=%s). Stop.", forum, pn)
            break

        if not bool(getattr(threads, "has_more", True)):
            log.info("threads.has_more is False (forum=%s pn=%s). Stop.", forum, pn)
            break

        pn += 1

    if max_seen_ts > 0:
        repo.set_forum_state(forum, last_crawl_ts=max_seen_ts)

    log.info(
        "Done crawl_threads forum=%s upserted=%s images_enqueued=%s collections_detected=%s last_crawl_ts=%s",
        forum,
        total_threads_upsert,
        total_images_enqueued,
        total_collections_detected,
        max_seen_ts,
    )
    repo.close()
