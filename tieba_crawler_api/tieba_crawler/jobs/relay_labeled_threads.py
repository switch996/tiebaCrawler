from __future__ import annotations

import asyncio
import logging
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from tieba_crawler.db.repo import Repo
from tieba_crawler.settings import Settings
from tieba_crawler.tieba.client import TiebaAPI

log = logging.getLogger(__name__)

def _fmt_ts(ts: int, tz: ZoneInfo) -> str:
    try:
        return datetime.fromtimestamp(ts, tz=tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)

def _iso_year_week(ts: int, tz: ZoneInfo) -> tuple[int, int]:
    dt = datetime.fromtimestamp(ts, tz=tz)
    iso = dt.isocalendar()
    return int(iso.year), int(iso.week)

def _bool_response_ok(resp: Any) -> bool:
    for attr in ("success", "is_success", "is_ok", "ok"):
        v = getattr(resp, attr, None)
        if isinstance(v, bool):
            return v
    for attr in ("errno", "err_no", "error_code", "code"):
        v = getattr(resp, attr, None)
        if isinstance(v, int):
            return v == 0
    return bool(resp)

def build_reply_content(
    *,
    source_tid: int,
    title: str,
    author_name: str,
    author_id: int,
    create_time: int,
    text: str,
    image_urls: List[str],
    mode: str,
    max_text_chars: int,
    max_images: int,
    tz: ZoneInfo,
) -> str:
    link = f"https://tieba.baidu.com/p/{source_tid}"
    author = author_name.strip() if author_name else f"uid:{author_id}"

    header = (
        f"【新帖收录】{(title or '').strip()}\n"
        f"作者：{author}\n"
        f"作者ID：{author_id}\n"
        f"时间：{_fmt_ts(create_time, tz)}\n"
        f"原帖链接：{link}\n"
        f"帖子ID：{source_tid}\n"
    )

    body_parts: List[str] = []
    t = (text or "").strip()

    if mode == "full":
        if t:
            if len(t) > max_text_chars:
                t = t[:max_text_chars] + "…"
            body_parts.append("\n正文摘录：\n" + t)
        if image_urls and max_images > 0:
            imgs = image_urls[:max_images]
            body_parts.append("\n图片链接：\n" + "\n".join(imgs))
    else:
        # link mode: keep it short
        if t:
            short_limit = min(max_text_chars, 120)
            if len(t) > short_limit:
                t = t[:short_limit] + "…"
            body_parts.append("\n摘要：\n" + t)

    content = header + "".join(body_parts)
    # keep conservative length
    return content.strip()[:1800]

async def relay_labeled_threads(
    *,
    settings: Settings,
    forum: str,
    category: Optional[str] = None,
    include_error: bool = False,
    dry_run: bool = False,
    mode: Optional[str] = None,
    max_posts: Optional[int] = None,
    min_interval_seconds: Optional[int] = None,
    max_text_chars: Optional[int] = None,
    max_images: Optional[int] = None,
    lookback_days: Optional[int] = None,
) -> None:
    # Only require auth when actually posting
    if not dry_run:
        if not settings.bduss:
            raise SystemExit(
                "BDUSS/STOKEN required for add_post. Please set them in .env "
                "(not required for --dry-run)"
            )

    mode = mode or settings.relay_mode
    max_posts = max_posts if max_posts is not None else settings.relay_max_posts
    min_interval_seconds = min_interval_seconds if min_interval_seconds is not None else settings.relay_min_interval_seconds
    max_text_chars = max_text_chars if max_text_chars is not None else settings.relay_max_text_chars
    max_images = max_images if max_images is not None else settings.relay_max_images
    lookback_days = lookback_days if lookback_days is not None else settings.relay_lookback_days

    if mode not in ("link", "full"):
        raise SystemExit("mode must be 'link' or 'full'")

    tz = ZoneInfo(settings.timezone)

    repo = Repo(settings=settings)
    repo.ensure_schema()

    reset_n = repo.reset_stuck_relay_posting()
    if reset_n:
        log.warning("Reset %s stuck relay tasks POSTING -> PENDING.", reset_n)

    # Step 1: enqueue relay tasks for labeled threads
    now_ts = int(time.time())
    lookback_since_ts = now_ts - int(lookback_days) * 86400

    candidates = repo.query_threads_for_relay_candidates(
        forum=forum,
        lookback_since_ts=lookback_since_ts,
        category=category,
        limit=5000,
    )

    enqueued = 0
    missing_target = 0

    for th in candidates:
        source_tid = int(th["tid"])
        cat = str(th["category"])
        cts = int(th["create_time"] or 0)
        y, w = _iso_year_week(cts, tz)

        target = repo.find_collection_thread(forum, cat, y, w)
        if not target:
            # don't create ERROR tasks; just wait until the collection thread exists
            missing_target += 1
            continue

        inserted = repo.insert_relay_task(
            source_tid=source_tid,
            target_tid=int(target["tid"]),
            target_forum=str(target["fname"]),
            category=cat,
            source_year=y,
            source_week=w,
        )
        if inserted:
            enqueued += 1

    repo.conn().commit()
    log.info(
        "Relay enqueue done: forum=%s category=%s candidates=%s enqueued=%s missing_target=%s lookback_days=%s",
        forum,
        category,
        len(candidates),
        enqueued,
        missing_target,
        lookback_days,
    )

    # Step 2: claim tasks to post
    tasks = repo.claim_relay_tasks(limit=max_posts, include_error=include_error, category=category)
    if not tasks:
        log.info("No relay tasks to post.")
        repo.close()
        return

    api = TiebaAPI(
        bduss=settings.bduss,
        stoken=settings.stoken,
        try_ws=False,
        request_attempts=1,
    )

    task_ids = [int(t["id"]) for t in tasks]

    if dry_run:
        log.warning("DRY RUN: printing %s tasks then releasing them back to PENDING.", len(tasks))
        for t in tasks:
            source_tid = int(t["source_tid"])
            content = build_reply_content(
                source_tid=source_tid,
                title=t.get("title") or "",
                author_name=t.get("author_name") or "",
                author_id=int(t.get("author_id") or 0),
                create_time=int(t.get("create_time") or 0),
                text=t.get("text") or "",
                mode=mode,
                max_text_chars=max_text_chars,
                max_images=max_images,
                tz=tz,
            )
            print("\n" + "-" * 60)
            print(f"TARGET: {t['target_forum']} tid={t['target_tid']}  (category={t.get('category')})")
            print(content)
        repo.release_relay_tasks(task_ids)
        repo.close()
        return

    # Step 3: post sequentially with strong rate limiting
    for idx, t in enumerate(tasks):
        task_id = int(t["id"])
        source_tid = int(t["source_tid"])
        target_tid = int(t["target_tid"])
        target_forum = str(t["target_forum"])

        content = build_reply_content(
            source_tid=source_tid,
            title=t.get("title") or "",
            author_name=t.get("author_name") or "",
            author_id=int(t.get("author_id") or 0),
            create_time=int(t.get("create_time") or 0),
            text=t.get("text") or "",
            mode=mode,
            max_text_chars=max_text_chars,
            max_images=max_images,
            tz=tz,
        )

        if not content.strip():
            repo.mark_relay_skipped(task_id, "Empty content after formatting")
            continue

        try:
            resp = await api.add_post_safe(target_forum, target_tid, content)
            ok = _bool_response_ok(resp)
            if ok:
                repo.mark_relay_done(task_id)
                log.info("Relay posted: source_tid=%s -> target_tid=%s (task_id=%s)", source_tid, target_tid, task_id)
            else:
                repo.mark_relay_error(task_id, f"add_post returned not-ok: {resp!r}")
                log.warning("Relay failed(not-ok): task_id=%s source_tid=%s resp=%r", task_id, source_tid, resp)
        except asyncio.TimeoutError as e:
            repo.mark_relay_error(task_id, f"TimeoutError (unknown if posted): {e!r}")
            log.warning("Relay timeout: task_id=%s source_tid=%s target_tid=%s", task_id, source_tid, target_tid)
        except Exception as e:
            repo.mark_relay_error(task_id, repr(e))
            log.exception("Relay exception: task_id=%s source_tid=%s target_tid=%s", task_id, source_tid, target_tid)

        # Strong rate limit
        if idx < len(tasks) - 1:
            jitter = random.uniform(0, 10)
            await asyncio.sleep(float(min_interval_seconds) + jitter)

    repo.close()
