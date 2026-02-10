from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware

from tieba_crawler.logging_conf import setup_logging
from tieba_crawler.settings import Settings
from tieba_crawler.api.auth import require_api_key
from tieba_crawler.api.job_manager import JobManager
from tieba_crawler.api.schemas import (
    CrawlThreadsRequest,
    ImageItem,
    JobResponse,
    RelayLabeledRequest,
    RelayTaskItem,
    BatchRequest,
    SyncCollectionsRequest,
    ThreadListItem,
)
from tieba_crawler.db.repo import Repo
from tieba_crawler.jobs.crawl_threads import crawl_threads
from tieba_crawler.jobs.relay_labeled_threads import relay_labeled_threads
from tieba_crawler.jobs.sync_collections import sync_collections

from tieba_crawler.tieba.mappers import utcnow_iso


def _load_env() -> None:
    """Load .env for local development.

    Set ENV_FILE to override the default.
    """
    env_file = os.getenv("ENV_FILE", ".env")
    if env_file and Path(env_file).exists():
        load_dotenv(env_file)


def _parse_cors_origins(raw: str) -> List[str]:
    raw = (raw or "").strip()
    if not raw:
        return []

    # Accept JSON list first, fallback to comma-separated.
    try:
        v = json.loads(raw)
        if isinstance(v, list):
            return [str(x) for x in v if str(x).strip()]
    except Exception:
        pass

    return [x.strip() for x in raw.split(",") if x.strip()]


def _parse_json_list(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    try:
        v = json.loads(raw)
        if isinstance(v, list):
            return [str(x) for x in v if str(x).strip()]
    except Exception:
        return None
    return None

def _row_to_thread_list_item(row: Dict[str, Any]) -> ThreadListItem:
    tags = _parse_json_list(row.get("tags_json"))
    return ThreadListItem(
        tid=int(row["tid"]),
        fname=row.get("fname"),
        title=row.get("title"),
        author_id=row.get("author_id"),
        author_name=row.get("author_name"),
        create_time=row.get("create_time"),
        last_time=row.get("last_time"),
        reply_num=row.get("reply_num"),
        view_num=row.get("view_num"),
        is_top=row.get("is_top"),
        is_good=row.get("is_good"),
        category=row.get("category"),
        tags=tags,
        thread_role=row.get("thread_role"),
        collection_year=row.get("collection_year"),
        collection_week=row.get("collection_week"),
    )


def _row_to_image_item(row: Dict[str, Any], *, request: Request, settings: Settings) -> ImageItem:
    local_path = row.get("local_path")
    return ImageItem(
        id=int(row["id"]),
        tid=int(row["tid"]),
        url=str(row.get("url") or ""),
        hash=row.get("hash"),
        origin_src=row.get("origin_src"),
        src=row.get("src"),
        big_src=row.get("big_src"),
        show_width=row.get("show_width"),
        show_height=row.get("show_height"),
        updated_at=row.get("updated_at"),
    )


def create_app() -> FastAPI:
    _load_env()
    setup_logging()

    settings = Settings.from_env()
    settings.data_dir.mkdir(parents=True, exist_ok=True)

    # Ensure schema on startup.
    repo = Repo(settings=settings)
    repo.ensure_schema()
    repo.close()

    app = FastAPI(title="Tieba Crawler API", version="0.2")

    # Store shared objects
    app.state.settings = settings
    app.state.jobs = JobManager()

    # CORS
    cors_origins = _parse_cors_origins(os.getenv("CORS_ORIGINS", ""))
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # --- basic ---
    @app.get("/health")
    def health() -> Dict[str, str]:
        return {"status": "ok"}

    # --- v1 router (optionally protected by API_KEY) ---
    router = APIRouter(prefix="/v1", dependencies=[Depends(require_api_key)])

    @router.get("/settings")
    def get_settings_endpoint(request: Request) -> Dict[str, Any]:
        s: Settings = request.app.state.settings
        # Do not return credentials.
        return {
            "db_url": s.db_url,
            "data_dir": str(s.data_dir),
            "default_forum": s.default_forum,
            "timezone": s.timezone,
            "crawler": {
                "threads_rn": s.threads_rn,
                "initial_hours": s.initial_hours,
                "overlap_seconds": s.overlap_seconds,
                "max_pages": s.max_pages,
            },
            "collection_rules": s.collection_rules,
            "relay": {
                "relay_mode": s.relay_mode,
                "relay_max_posts": s.relay_max_posts,
                "relay_min_interval_seconds": s.relay_min_interval_seconds,
                "relay_max_text_chars": s.relay_max_text_chars,
                "relay_max_images": s.relay_max_images,
                "relay_lookback_days": s.relay_lookback_days,
            },
        }

    # ------------------------
    # Threads / images / tasks
    # ------------------------

    @router.get("/threads", response_model=List[ThreadListItem])
    def list_threads(
            request: Request,
            forum: Optional[str] = Query(default=None),
            category: Optional[str] = Query(default=None),
            thread_role: Optional[str] = Query(default=None),
            filter: Optional[str] = Query(default=None, description="uncategorized | categorized | collection"),
            q: Optional[str] = Query(default=None),
            since_ts: Optional[int] = Query(default=None),
            until_ts: Optional[int] = Query(default=None),
            limit: int = Query(default=50, ge=1, le=200),
            offset: int = Query(default=0, ge=0),
            order: str = Query(default="create_time_desc"),
    ) -> List[ThreadListItem]:
        s: Settings = request.app.state.settings
        repo = Repo(settings=s)
        repo.ensure_schema()

        where: List[str] = []
        params: List[Any] = []

        if forum:
            where.append("fname=?")
            params.append(forum)
        elif s.default_forum:
            where.append("fname=?")
            params.append(s.default_forum)

        if category:
            where.append("category=?")
            params.append(category)

        if thread_role:
            where.append("thread_role=?")
            params.append(thread_role)

        if filter == "uncategorized":
            where.append("(category IS NULL OR category = '')")
            where.append("thread_role = 'normal'")
        elif filter == "categorized":
            where.append("category IS NOT NULL AND category != ''")
        elif filter == "collection":
            where.append("thread_role = 'collection'")
        elif filter == "new":
            where.append("process_status = 'new'")
        elif filter == "fetched":
            where.append("process_status = 'fetched'")

        if q:
            where.append("title LIKE ?")
            params.append(f"%{q}%")

        if since_ts is not None:
            where.append("create_time >= ?")
            params.append(int(since_ts))

        if until_ts is not None:
            where.append("create_time <= ?")
            params.append(int(until_ts))

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        if order == "create_time_asc":
            order_sql = "ORDER BY create_time ASC"
        else:
            order_sql = "ORDER BY create_time DESC"

        rows = repo.conn().execute(
            f"""
            SELECT tid, fname, title, author_id, author_name,
                   create_time, last_time, reply_num, view_num,
                   is_top, is_good,
                   category, tags_json,
                   thread_role, collection_year, collection_week
            FROM threads
            {where_sql}
            {order_sql}
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), int(offset)),
        ).fetchall()

        out: List[ThreadListItem] = []
        for r in rows:
            out.append(_row_to_thread_list_item(dict(r)))

        repo.close()
        return out

    @router.post("/threads/batch")
    def batch_update_threads(request: Request, payload: BatchRequest) -> Dict[str, Any]:
        s: Settings = request.app.state.settings
        repo = Repo(settings=s)
        repo.ensure_schema()

        updated = 0
        for item in payload.items:
            tags_json = json.dumps(item.tags, ensure_ascii=False) if item.tags else None

            # Auto-detect process_status if not explicitly set
            status = item.process_status
            if not status and (item.category and item.ai_reply_content):
                status = "processed"

            repo.conn().execute(
                """UPDATE threads
                   SET category         = COALESCE(?, category),
                       tags_json        = COALESCE(?, tags_json),
                       ai_reply_content = COALESCE(?, ai_reply_content),
                       process_status   = COALESCE(?, process_status),
                       updated_at       = ?
                   WHERE tid = ?""",
                (item.category, tags_json, item.ai_reply_content, status, utcnow_iso(), item.tid),
            )
            updated += 1

        repo.conn().commit()
        repo.close()
        return {"ok": True, "updated": updated}



    @router.get("/relay-tasks", response_model=List[RelayTaskItem])
    def list_relay_tasks(
        request: Request,
        status: Optional[str] = Query(default=None, description="PENDING | POSTING | DONE | ERROR | SKIPPED"),
        category: Optional[str] = Query(default=None),
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
    ) -> List[RelayTaskItem]:
        s: Settings = request.app.state.settings
        repo = Repo(settings=s)
        repo.ensure_schema()

        where: List[str] = []
        params: List[Any] = []

        if status:
            where.append("status=?")
            params.append(status)
        if category:
            where.append("category=?")
            params.append(category)

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        rows = repo.conn().execute(
            f"""
            SELECT id, source_tid, target_tid, target_forum,
                   category, source_year, source_week,
                   status, attempts, last_error, created_at, updated_at
            FROM relay_tasks
            {where_sql}
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), int(offset)),
        ).fetchall()

        out = [RelayTaskItem(**dict(r)) for r in rows]
        repo.close()
        return out

    @router.get("/stats")
    def get_stats(request: Request) -> Dict[str, Any]:
        s: Settings = request.app.state.settings
        repo = Repo(settings=s)
        repo.ensure_schema()

        # Note: scope stats to default forum if configured.
        params: List[Any] = []
        forum_where = ""
        if s.default_forum:
            forum_where = "WHERE fname=?"
            params.append(s.default_forum)

        threads_total = repo.conn().execute(
            f"SELECT COUNT(1) AS c FROM threads {forum_where}",
            tuple(params),
        ).fetchone()["c"]

        img_total = repo.conn().execute(
            """
            SELECT COUNT(1) AS c
            FROM images
                     JOIN threads ON threads.tid = images.tid
            """
            + ("WHERE threads.fname=?" if s.default_forum else ""),
            tuple(params),
        ).fetchone()["c"]

        relay_status = repo.conn().execute(
            """
            SELECT status, COUNT(1) AS c
            FROM relay_tasks
            GROUP BY status
            """,
        ).fetchall()

        cat_counts = repo.conn().execute(
            """
            SELECT category, COUNT(1) AS c
            FROM threads
            WHERE category IS NOT NULL AND category != ''
            """
            + (" AND fname=?" if s.default_forum else "")
            + " GROUP BY category ORDER BY c DESC",
            tuple(params),
        ).fetchall()

        repo.close()
        return {
            "forum": s.default_forum or None,
            "threads_total": int(threads_total or 0),
            "images_total": int(img_total or 0),
            "relay_tasks_by_status": {r["status"]: int(r["c"]) for r in relay_status},
            "threads_by_category": {str(r["category"]): int(r["c"]) for r in cat_counts},
        }

    # -----
    # Jobs
    # -----

    @router.post("/jobs/crawl-threads", response_model=JobResponse)
    async def job_crawl_threads(request: Request, payload: CrawlThreadsRequest) -> JobResponse:
        s: Settings = request.app.state.settings
        jobs: JobManager = request.app.state.jobs

        async def _run() -> Dict[str, Any]:
            await crawl_threads(
                forum=payload.forum,
                settings=s,
                rn=payload.rn,
                initial_hours=payload.initial_hours,
                overlap_seconds=payload.overlap_seconds,
                max_pages=payload.max_pages,
            )
            return {"ok": True}

        job = await jobs.create("crawl_threads", _run)
        return JobResponse(**job.to_dict())

    @router.post("/jobs/sync-collections", response_model=JobResponse)
    async def job_sync_collections(request: Request, payload: SyncCollectionsRequest) -> JobResponse:
        s: Settings = request.app.state.settings
        jobs: JobManager = request.app.state.jobs

        async def _run() -> Dict[str, Any]:
            # sync_collections is a sync function; run it off the event loop.
            await asyncio.to_thread(
                sync_collections,
                settings=s,
                forum=payload.forum,
                days=int(payload.days),
                dry_run=bool(payload.dry_run),
            )
            return {"ok": True, "dry_run": bool(payload.dry_run)}

        job = await jobs.create("sync_collections", _run)
        return JobResponse(**job.to_dict())

    @router.post("/jobs/relay-labeled", response_model=JobResponse)
    async def job_relay_labeled(request: Request, payload: RelayLabeledRequest) -> JobResponse:
        s: Settings = request.app.state.settings
        jobs: JobManager = request.app.state.jobs

        async def _run() -> Dict[str, Any]:
            await relay_labeled_threads(
                settings=s,
                forum=payload.forum,
                category=payload.category,
                include_error=bool(payload.include_error),
                dry_run=bool(payload.dry_run),
                mode=payload.mode,
                max_posts=payload.max_posts,
                min_interval_seconds=payload.min_interval_seconds,
                max_text_chars=payload.max_text_chars,
                max_images=payload.max_images,
                lookback_days=payload.lookback_days,
            )
            return {"ok": True, "dry_run": bool(payload.dry_run)}

        job = await jobs.create("relay_labeled_threads", _run)
        return JobResponse(**job.to_dict())

    @router.get("/jobs/{job_id}", response_model=JobResponse)
    async def get_job(request: Request, job_id: str) -> JobResponse:
        jobs: JobManager = request.app.state.jobs
        job = await jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        return JobResponse(**job.to_dict())

    @router.get("/jobs", response_model=List[JobResponse])
    async def list_jobs(request: Request, limit: int = Query(default=50, ge=1, le=200)) -> List[JobResponse]:
        jobs: JobManager = request.app.state.jobs
        items = await jobs.list(limit=int(limit))
        return [JobResponse(**j.to_dict()) for j in items]

    @router.post("/update")
    async def update_threads(
            request: Request,
            forum: Optional[str] = Query(default=None),
            max_pages: Optional[int] = Query(default=10),
    ) -> Dict[str, Any]:
        """Crawl new threads for a forum, then return stats."""
        s: Settings = request.app.state.settings
        target_forum = forum or s.default_forum
        if not target_forum:
            raise HTTPException(status_code=400, detail="Forum is required")

        await crawl_threads(
            forum=target_forum,
            settings=s,
            max_pages=max_pages,
        )

        # Return fresh stats
        repo = Repo(settings=s)
        repo.ensure_schema()
        count = repo.conn().execute(
            "SELECT COUNT(1) AS c FROM threads WHERE fname=?", (target_forum,)
        ).fetchone()["c"]
        repo.close()

        return {"ok": True, "forum": target_forum, "threads_total": count}

    app.include_router(router)
    return app


app = create_app()
