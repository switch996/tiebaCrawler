from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv

from tieba_crawler_api.tieba_crawler.logging_conf import setup_logging
from tieba_crawler_api.tieba_crawler.settings import Settings
from tieba_crawler_api.tieba_crawler.db.repo import Repo
from tieba_crawler_api.tieba_crawler.jobs.crawl_threads import crawl_threads
from tieba_crawler_api.tieba_crawler.jobs.download_images import download_images
from tieba_crawler_api.tieba_crawler.jobs.relay_labeled_threads import relay_labeled_threads
from tieba_crawler_api.tieba_crawler.jobs.sync_collections import sync_collections

def _apply_cli_overrides(settings: Settings, args: argparse.Namespace) -> Settings:
    data = settings.__dict__.copy()

    if getattr(args, "db_url", None):
        data["db_url"] = args.db_url
    if getattr(args, "data_dir", None):
        data["data_dir"] = Path(args.data_dir)

    if getattr(args, "bduss", None) is not None:
        data["bduss"] = args.bduss
    if getattr(args, "stoken", None) is not None:
        data["stoken"] = args.stoken

    if getattr(args, "timezone", None):
        data["timezone"] = args.timezone

    if getattr(args, "try_ws", None) is True:
        data["try_ws"] = True
    if getattr(args, "no_try_ws", None) is True:
        data["try_ws"] = False

    return Settings(**data)

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tieba-crawler")
    p.add_argument("--env-file", default=".env", help="Path to .env file (default: .env)")
    p.add_argument("--log-level", default=None, help="Logging level (INFO, DEBUG, ...)")
    p.add_argument("--db-url", default=None, help="Override DB_URL (e.g., sqlite:///data/tieba.db)")
    p.add_argument("--data-dir", default=None, help="Override DATA_DIR (e.g., data)")
    p.add_argument("--bduss", default=None, help="Override BDUSS")
    p.add_argument("--stoken", default=None, help="Override STOKEN")
    p.add_argument("--timezone", default=None, help="Override TIMEZONE (e.g., Asia/Shanghai)")

    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-db", help="Create DB schema")

    sub_crawl = sub.add_parser("crawl-threads", help="Crawl latest threads into DB")
    sub_crawl.add_argument("--forum", required=False, default=None, help="Forum name or fid")
    sub_crawl.add_argument("--rn", type=int, default=None, help="Page size (max 100)")
    sub_crawl.add_argument("--initial-hours", type=int, default=None, help="First run: look back N hours")
    sub_crawl.add_argument("--overlap-seconds", type=int, default=None, help="Incremental overlap window (seconds)")
    sub_crawl.add_argument("--max-pages", type=int, default=None, help="Safety page limit")
    sub_crawl.add_argument("--try-ws", action="store_true", help="Prefer websocket mode")
    sub_crawl.add_argument("--no-try-ws", action="store_true", help="Force HTTP mode (more stable)")

    sub_img = sub.add_parser("download-images", help="Download pending images from DB queue")
    sub_img.add_argument("--limit", type=int, default=200, help="How many tasks to claim in this run")
    sub_img.add_argument("--concurrency", type=int, default=None, help="Concurrent downloads (default from env)")
    sub_img.add_argument("--include-error", action="store_true", help="Also retry tasks in ERROR status")

    sub_set = sub.add_parser("set-category", help="Set category/tag for a thread (manual / testing)")
    sub_set.add_argument("--tid", type=int, required=True, help="Thread tid")
    sub_set.add_argument("--category", required=True, help="Category label, e.g. 交友贴")
    sub_set.add_argument("--tags-json", default=None, help='Optional JSON array string, e.g. ["交友贴","自拍"]')

    sub_sync = sub.add_parser("sync-collections", help="Backfill collection metadata from titles in DB")
    sub_sync.add_argument("--forum", required=False, default=None)
    sub_sync.add_argument("--days", type=int, default=120, help="Look back N days in DB")
    sub_sync.add_argument("--dry-run", action="store_true", help="Do not update DB; print only")

    sub_relay = sub.add_parser("relay-labeled", help="Reply labeled threads into weekly collection threads")
    sub_relay.add_argument("--forum", required=False, default=None)
    sub_relay.add_argument("--category", default=None, help="Only relay this category")
    sub_relay.add_argument("--mode", choices=["link", "full"], default=None)
    sub_relay.add_argument("--max-posts", type=int, default=None)
    sub_relay.add_argument("--min-interval", type=int, default=None)
    sub_relay.add_argument("--max-text", type=int, default=None)
    sub_relay.add_argument("--max-images", type=int, default=None)
    sub_relay.add_argument("--lookback-days", type=int, default=None)
    sub_relay.add_argument("--dry-run", action="store_true")
    sub_relay.add_argument("--include-error", action="store_true")

    return p

def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)

    load_dotenv(args.env_file)
    setup_logging(args.log_level)

    settings = Settings.from_env()
    settings = _apply_cli_overrides(settings, args)

    if args.cmd == "init-db":
        repo = Repo(settings=settings)
        repo.ensure_schema()
        repo.close()
        print("DB schema is ready.")
        return

    if args.cmd == "crawl-threads":
        forum = args.forum or settings.default_forum
        if not forum:
            raise SystemExit("Forum is required. Use --forum or set FORUM in env.")

        settings = _apply_cli_overrides(settings, args)
        asyncio.run(
            crawl_threads(
                forum=forum,
                settings=settings,
                rn=args.rn,
                initial_hours=args.initial_hours,
                overlap_seconds=args.overlap_seconds,
                max_pages=args.max_pages,
            )
        )
        return

    if args.cmd == "download-images":
        asyncio.run(
            download_images(
                settings=settings,
                limit=args.limit,
                concurrency=args.concurrency,
                include_error=args.include_error,
            )
        )
        return

    if args.cmd == "set-category":
        repo = Repo(settings=settings)
        repo.ensure_schema()
        repo.set_thread_category(args.tid, args.category, tags_json=args.tags_json)
        repo.close()
        print(f"Updated tid={args.tid} category={args.category}")
        return

    if args.cmd == "sync-collections":
        forum = args.forum or settings.default_forum
        if not forum:
            raise SystemExit("Forum is required. Use --forum or set FORUM in env.")
        sync_collections(settings=settings, forum=forum, days=args.days, dry_run=bool(args.dry_run))
        return

    if args.cmd == "relay-labeled":
        forum = args.forum or settings.default_forum
        if not forum:
            raise SystemExit("Forum is required. Use --forum or set FORUM in env.")
        asyncio.run(
            relay_labeled_threads(
                settings=settings,
                forum=forum,
                category=args.category,
                include_error=bool(args.include_error),
                dry_run=bool(args.dry_run),
                mode=args.mode,
                max_posts=args.max_posts,
                min_interval_seconds=args.min_interval,
                max_text_chars=args.max_text,
                max_images=args.max_images,
                lookback_days=args.lookback_days,
            )
        )
        return

    raise SystemExit(f"Unknown command: {args.cmd}")
