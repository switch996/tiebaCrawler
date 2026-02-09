from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles

from tieba_crawler.logging_conf import setup_logging
from tieba_crawler.settings import Settings
from tieba_crawler.api.auth import require_api_key
from tieba_crawler.api.job_manager import JobManager
from tieba_crawler.api.schemas import (
    CrawlThreadsRequest,
    DownloadImagesRequest,
    ImageItem,
    JobResponse,
    RelayLabeledRequest,
    RelayTaskItem,
    SetCategoryRequest,
    SyncCollectionsRequest,
    ThreadDetail,
    ThreadListItem,
)
from tieba_crawler.db.repo import Repo
from tieba_crawler.jobs.crawl_threads import crawl_threads
from tieba_crawler.jobs.download_images import download_images
from tieba_crawler.jobs.relay_labeled_threads import relay_labeled_threads
from tieba_crawler.jobs.sync_collections import sync_collections


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


def _public_url_for_local_path(*, request: Request, settings: Settings, local_path: Optional[str]) -> Optional[str]:
    if not local_path:
        return None

    try:
        p = Path(local_path)
        # normalize relative to data_dir
        data_dir = Path(settings.data_dir)
        if p.is_absolute():
            try:
                rel = p.relative_to(data_dir)
            except Exception:
                # Do not expose arbitrary absolute paths
                return None
        else:
            rel = p
        # /files is mounted to settings.data_dir
        rel_posix = rel.as_posix().lstrip("/")
        base = str(request.base_url).rstrip("/")
        return f"{base}/files/{rel_posix}"
    except Exception:
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
        status=str(row.get("status") or ""),
        local_path=local_path,
        public_url=_public_url_for_local_path(request=request, settings=settings, local_path=local_path),
        hash=row.get("hash"),
        origin_src=row.get("origin_src"),
        src=row.get("src"),
        big_src=row.get("big_src"),
        show_width=row.get("show_width"),
        show_height=row.get("show_height"),
        last_error=row.get("last_error"),
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

    # Serve DATA_DIR files (downloaded images) under /files
    app.mount("/files", StaticFiles(directory=str(settings.data_dir), html=False), name="files")

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
            "image_downloader": {
                "image_concurrency": s.image_concurrency,
                "image_attempts": s.image_attempts,
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
        forum: Optional[str] = Query(default=None, description="Forum name (fname)"),
        category: Optional[str] = Query(default=None),
        thread_role: Optional[str] = Query(default=None, description="normal | collection"),
        q: Optional[str] = Query(default=None, description="Search keyword in title"),
        since_ts: Optional[int] = Query(default=None, description="create_time >= since_ts"),
        until_ts: Optional[int] = Query(default=None, description="create_time <= until_ts"),
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
        order: str = Query(default="create_time_desc", description="create_time_desc | create_time_asc"),
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

    @router.get("/threads/{tid}", response_model=ThreadDetail)
    def get_thread_detail(request: Request, tid: int) -> ThreadDetail:
        s: Settings = request.app.state.settings
        repo = Repo(settings=s)
        repo.ensure_schema()

        row = repo.conn().execute(
            """
            SELECT *
            FROM threads
            WHERE tid=?
            LIMIT 1
            """,
            (int(tid),),
        ).fetchone()

        if not row:
            repo.close()
            raise HTTPException(status_code=404, detail=f"Thread not found: tid={tid}")

        d = dict(row)
        contents_raw = d.get("contents_json")
        try:
            contents = json.loads(contents_raw) if contents_raw else None
        except Exception:
            contents = None

        tags = _parse_json_list(d.get("tags_json"))

        img_rows = repo.conn().execute(
            """
            SELECT id, tid, url, status, local_path, hash, origin_src, src, big_src,
                   show_width, show_height, last_error, updated_at
            FROM images
            WHERE tid=?
            ORDER BY id ASC
            """,
            (int(tid),),
        ).fetchall()

        images = [_row_to_image_item(dict(ir), request=request, settings=s) for ir in img_rows]

        repo.close()

        return ThreadDetail(
            tid=int(d["tid"]),
            fid=d.get("fid"),
            fname=d.get("fname"),
            title=d.get("title"),
            author_id=d.get("author_id"),
            author_name=d.get("author_name"),
            agree=d.get("agree"),
            pid=d.get("pid"),
            create_time=d.get("create_time"),
            last_time=d.get("last_time"),
            reply_num=d.get("reply_num"),
            view_num=d.get("view_num"),
            is_top=d.get("is_top"),
            is_good=d.get("is_good"),
            is_help=d.get("is_help"),
            is_hide=d.get("is_hide"),
            is_share=d.get("is_share"),
            text=d.get("text"),
            contents=contents,
            category=d.get("category"),
            tags=tags,
            thread_role=d.get("thread_role"),
            collection_year=d.get("collection_year"),
            collection_week=d.get("collection_week"),
            updated_at=d.get("updated_at"),
            source_url=f"https://tieba.baidu.com/p/{int(d['tid'])}",
            images=images,
        )

    @router.post("/threads/{tid}/category")
    def set_thread_category(request: Request, tid: int, payload: SetCategoryRequest) -> Dict[str, Any]:
        s: Settings = request.app.state.settings
        repo = Repo(settings=s)
        repo.ensure_schema()

        repo.set_thread_category(int(tid), payload.category, tags_json=payload.tags_json())
        repo.close()
        return {"ok": True, "tid": int(tid), "category": payload.category, "tags": payload.tags}

    @router.get("/images", response_model=List[ImageItem])
    def list_images(
        request: Request,
        status: Optional[str] = Query(default=None, description="PENDING | DOWNLOADING | DONE | ERROR"),
        forum: Optional[str] = Query(default=None),
        tid: Optional[int] = Query(default=None),
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
    ) -> List[ImageItem]:
        s: Settings = request.app.state.settings
        repo = Repo(settings=s)
        repo.ensure_schema()

        where: List[str] = []
        params: List[Any] = []

        if status:
            where.append("images.status=?")
            params.append(status)
        if tid is not None:
            where.append("images.tid=?")
            params.append(int(tid))
        if forum:
            where.append("threads.fname=?")
            params.append(forum)
        elif s.default_forum:
            where.append("threads.fname=?")
            params.append(s.default_forum)

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        rows = repo.conn().execute(
            f"""
            SELECT images.id, images.tid, images.url, images.status, images.local_path,
                   images.hash, images.origin_src, images.src, images.big_src,
                   images.show_width, images.show_height, images.last_error, images.updated_at
            FROM images
            JOIN threads ON threads.tid = images.tid
            {where_sql}
            ORDER BY images.id DESC
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), int(offset)),
        ).fetchall()

        out = [_row_to_image_item(dict(r), request=request, settings=s) for r in rows]
        repo.close()
        return out

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

        img_status = repo.conn().execute(
            """
            SELECT images.status AS status, COUNT(1) AS c
            FROM images
            JOIN threads ON threads.tid = images.tid
            """
            + ("WHERE threads.fname=?" if s.default_forum else "")
            + " GROUP BY images.status",
            tuple(params),
        ).fetchall()

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
            "images_by_status": {r["status"]: int(r["c"]) for r in img_status},
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

    @router.post("/jobs/download-images", response_model=JobResponse)
    async def job_download_images(request: Request, payload: DownloadImagesRequest) -> JobResponse:
        s: Settings = request.app.state.settings
        jobs: JobManager = request.app.state.jobs

        async def _run() -> Dict[str, Any]:
            await download_images(
                settings=s,
                limit=payload.limit,
                concurrency=payload.concurrency,
                include_error=bool(payload.include_error),
            )
            return {"ok": True}

        job = await jobs.create("download_images", _run)
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

    app.include_router(router)
    return app


app = create_app()
