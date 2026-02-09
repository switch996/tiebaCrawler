from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from tieba_crawler.settings import Settings
from tieba_crawler.db.conn import connect_sqlite

log = logging.getLogger(__name__)

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

@dataclass
class Repo:
    settings: Settings = field(default_factory=Settings.from_env)
    _conn: sqlite3.Connection | None = None

    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = connect_sqlite(self.settings.db_url)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ---- schema & migrations ----
    def ensure_schema(self) -> None:
        """Create tables if missing + migrate columns if needed."""
        schema_path = Path(__file__).with_name("schema.sql")
        sql = schema_path.read_text(encoding="utf-8")
        self.conn().executescript(sql)
        self.conn().commit()
        self._migrate()
        self.conn().commit()

    def _table_columns(self, table: str) -> set[str]:
        try:
            rows = self.conn().execute(f"PRAGMA table_info({table})").fetchall()
        except sqlite3.OperationalError:
            return set()
        return {r["name"] for r in rows}

    def _ensure_column(self, table: str, col_name: str, col_def_sql: str) -> None:
        cols = self._table_columns(table)
        if col_name in cols:
            return
        self.conn().execute(f"ALTER TABLE {table} ADD COLUMN {col_def_sql}")
        log.warning("Migrated: added column %s to %s", col_name, table)

    def _migrate(self) -> None:
        # threads new fields (v0.1 -> v0.2)
        self._ensure_column("threads", "agree", "agree INTEGER DEFAULT 0")
        self._ensure_column("threads", "pid", "pid INTEGER DEFAULT 0")
        self._ensure_column("threads", "is_help", "is_help INTEGER DEFAULT 0")
        self._ensure_column("threads", "is_hide", "is_hide INTEGER DEFAULT 0")
        self._ensure_column("threads", "is_share", "is_share INTEGER DEFAULT 0")
        self._ensure_column("threads", "category", "category TEXT")
        self._ensure_column("threads", "tags_json", "tags_json TEXT")
        self._ensure_column("threads", "thread_role", "thread_role TEXT NOT NULL DEFAULT 'normal'")
        self._ensure_column("threads", "collection_year", "collection_year INTEGER")
        self._ensure_column("threads", "collection_week", "collection_week INTEGER")
        self._ensure_column("threads", "ai_reply_content", "ai_reply_content TEXT")
        self._ensure_column("threads", "process_status", "process_status TEXT NOT NULL DEFAULT 'new'")

        # relay_tasks extra columns if user had old relay version
        self._ensure_column("relay_tasks", "category", "category TEXT")
        self._ensure_column("relay_tasks", "source_year", "source_year INTEGER")
        self._ensure_column("relay_tasks", "source_week", "source_week INTEGER")
        self._ensure_column("relay_tasks", "attempts", "attempts INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("relay_tasks", "created_at", "created_at TEXT")
        # If created_at is null for old rows, it is fine.

    # -------- forum state --------
    def get_forum_state(self, forum: str) -> Optional[Dict[str, Any]]:
        row = self.conn().execute(
            "SELECT forum, last_crawl_ts, updated_at FROM forum_state WHERE forum=?",
            (forum,),
        ).fetchone()
        return dict(row) if row else None

    def set_forum_state(self, forum: str, last_crawl_ts: int, updated_at: Optional[str] = None) -> None:
        updated_at = updated_at or utcnow_iso()
        self.conn().execute(
            """
            INSERT INTO forum_state (forum, last_crawl_ts, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(forum) DO UPDATE SET
              last_crawl_ts=excluded.last_crawl_ts,
              updated_at=excluded.updated_at
            """,
            (forum, last_crawl_ts, updated_at),
        )
        self.conn().commit()

    # -------- threads --------
    def upsert_thread(self, t: Dict[str, Any]) -> None:
        """Upsert threads. Important: do NOT wipe AI labels on update.

        - category/tags_json: only update when excluded has non-empty value.
        - thread_role: only upgrade to 'collection' (never downgrade automatically).
        - collection_year/week: only update when excluded provides non-null.
        """
        self.conn().execute(
            """
            INSERT INTO threads (
              tid, fid, fname,
              title,
              author_id, author_name,
              agree, pid,
              create_time, last_time,
              reply_num, view_num,
              is_top, is_good, is_help, is_hide, is_share,
              text, contents_json, ai_reply_content, process_status,
              category, tags_json, thread_role, collection_year, collection_week,
              updated_at
            )
            VALUES (
              :tid, :fid, :fname,
              :title,
              :author_id, :author_name,
              :agree, :pid,
              :create_time, :last_time,
              :reply_num, :view_num,
              :is_top, :is_good, :is_help, :is_hide, :is_share,
              :text, :contents_json, :ai_reply_content, :process_status,
              :category, :tags_json, :thread_role, :collection_year, :collection_week,
              :updated_at
            )
            ON CONFLICT(tid) DO UPDATE SET
              fid=excluded.fid,
              fname=excluded.fname,
              title=excluded.title,
              author_id=excluded.author_id,
              author_name=excluded.author_name,
              agree=excluded.agree,
              pid=excluded.pid,
              create_time=excluded.create_time,
              last_time=excluded.last_time,
              reply_num=excluded.reply_num,
              view_num=excluded.view_num,
              is_top=excluded.is_top,
              is_good=excluded.is_good,
              is_help=excluded.is_help,
              is_hide=excluded.is_hide,
              is_share=excluded.is_share,
              text=excluded.text,
              contents_json=excluded.contents_json,
              ai_reply_content=COALESCE(excluded.ai_reply_content, threads.ai_reply_content),
              process_status=COALESCE(excluded.process_status, threads.process_status),
                
              category=CASE
                WHEN excluded.category IS NOT NULL AND excluded.category != '' THEN excluded.category
                ELSE threads.category
              END,
              tags_json=CASE
                WHEN excluded.tags_json IS NOT NULL AND excluded.tags_json != '' THEN excluded.tags_json
                ELSE threads.tags_json
              END,
              thread_role=CASE
                WHEN excluded.thread_role = 'collection' THEN 'collection'
                ELSE threads.thread_role
              END,
              collection_year=COALESCE(excluded.collection_year, threads.collection_year),
              collection_week=COALESCE(excluded.collection_week, threads.collection_week),

              updated_at=excluded.updated_at
            """,
            t,
        )

    def set_thread_category(self, tid: int, category: str, tags_json: Optional[str] = None) -> None:
        self.conn().execute(
            """
            UPDATE threads
            SET category=?, tags_json=COALESCE(?, tags_json), updated_at=?
            WHERE tid=?
            """,
            (category, tags_json, utcnow_iso(), tid),
        )
        self.conn().commit()

    def mark_thread_as_collection(self, tid: int, category: str, year: int, week: int) -> None:
        self.conn().execute(
            """
            UPDATE threads
            SET thread_role='collection',
                category=?,
                collection_year=?,
                collection_week=?,
                updated_at=?
            WHERE tid=?
            """,
            (category, year, week, utcnow_iso(), tid),
        )
        self.conn().commit()

    def find_collection_thread(self, forum: str, category: str, year: int, week: int) -> Optional[Dict[str, Any]]:
        row = self.conn().execute(
            """
            SELECT tid, fname, title, create_time
            FROM threads
            WHERE fname=?
              AND thread_role='collection'
              AND category=?
              AND collection_year=?
              AND collection_week=?
            ORDER BY create_time DESC
            LIMIT 1
            """,
            (forum, category, year, week),
        ).fetchone()
        return dict(row) if row else None

    def query_threads_for_relay_candidates(
        self,
        *,
        forum: str,
        lookback_since_ts: int,
        category: Optional[str] = None,
        limit: int = 2000,
    ) -> List[Dict[str, Any]]:
        where = ["fname=?", "create_time>=?", "thread_role!='collection'", "category IS NOT NULL", "category!=''"]
        params: List[Any] = [forum, lookback_since_ts]
        if category:
            where.append("category=?")
            params.append(category)

        sql = f"""
        SELECT tid, fname, title, author_id, author_name, create_time, text, category
        FROM threads
        WHERE {' AND '.join(where)}
        ORDER BY create_time ASC
        LIMIT ?
        """
        params.append(limit)
        rows = self.conn().execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]

    # -------- images --------
    def upsert_image_task(self, img: Dict[str, Any]) -> None:
        """Upsert an image task.

        If an existing row is already DONE, keep it DONE and keep its local_path.
        """
        self.conn().execute(
            """
            INSERT INTO images (
              tid, url, hash, origin_src, src, big_src,
              show_width, show_height, updated_at
            )
            VALUES (
              :tid, :url, :hash, :origin_src, :src, :big_src,
              :show_width, :show_height, :updated_at
            )
            ON CONFLICT(tid, url) DO UPDATE SET
              hash=COALESCE(excluded.hash, images.hash),
              origin_src=COALESCE(excluded.origin_src, images.origin_src),
              src=COALESCE(excluded.src, images.src),
              big_src=COALESCE(excluded.big_src, images.big_src),
              show_width=COALESCE(excluded.show_width, images.show_width),
              show_height=COALESCE(excluded.show_height, images.show_height),
              updated_at=excluded.updated_at
            """,
            img,
        )

    def get_image_urls_for_tid(self, tid: int, limit: int = 3) -> List[str]:
        rows = self.conn().execute(
            "SELECT url FROM images WHERE tid=? ORDER BY id ASC",
            (tid,),
        ).fetchall()
        seen = set()
        out: List[str] = []
        for r in rows:
            u = r["url"]
            if not u or u in seen:
                continue
            seen.add(u)
            out.append(u)
            if len(out) >= limit:
                break
        return out

    # -------- relay tasks --------
    def reset_stuck_relay_posting(self) -> int:
        cur = self.conn().execute(
            "UPDATE relay_tasks SET status='PENDING', updated_at=? WHERE status='POSTING'",
            (utcnow_iso(),),
        )
        self.conn().commit()
        return cur.rowcount

    def insert_relay_task(
        self,
        *,
        source_tid: int,
        target_tid: int,
        target_forum: str,
        category: str,
        source_year: int,
        source_week: int,
    ) -> bool:
        now = utcnow_iso()
        cur = self.conn().execute(
            """
            INSERT OR IGNORE INTO relay_tasks
              (source_tid, target_tid, target_forum, category, source_year, source_week, status, attempts, last_error, created_at, updated_at)
            VALUES
              (?, ?, ?, ?, ?, ?, 'PENDING', 0, NULL, ?, ?)
            """,
            (source_tid, target_tid, target_forum, category, source_year, source_week, now, now),
        )
        return cur.rowcount == 1

    def claim_relay_tasks(self, *, limit: int, include_error: bool = False, category: Optional[str] = None) -> List[Dict[str, Any]]:
        statuses = ["PENDING"]
        if include_error:
            statuses.append("ERROR")
        q_marks = ",".join(["?"] * len(statuses))

        where = [f"rt.status IN ({q_marks})"]
        params: List[Any] = [*statuses]

        if category:
            where.append("rt.category=?")
            params.append(category)

        conn = self.conn()
        with conn:  # transaction
            rows = conn.execute(
                f"""
                SELECT rt.id, rt.source_tid, rt.target_tid, rt.target_forum, rt.category, rt.source_year, rt.source_week, rt.attempts,
                       th.fname AS source_forum,
                       th.title, th.author_id, th.author_name, th.create_time, th.text
                FROM relay_tasks rt
                JOIN threads th ON th.tid = rt.source_tid
                WHERE {' AND '.join(where)}
                ORDER BY th.create_time ASC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()

            ids = [r["id"] for r in rows]
            if ids:
                id_marks = ",".join(["?"] * len(ids))
                conn.execute(
                    f"UPDATE relay_tasks SET status='POSTING', attempts=attempts+1, updated_at=? WHERE id IN ({id_marks})",
                    (utcnow_iso(), *ids),
                )
        return [dict(r) for r in rows]

    def release_relay_tasks(self, task_ids: List[int]) -> None:
        if not task_ids:
            return
        marks = ",".join(["?"] * len(task_ids))
        self.conn().execute(
            f"UPDATE relay_tasks SET status='PENDING', updated_at=? WHERE id IN ({marks})",
            (utcnow_iso(), *task_ids),
        )
        self.conn().commit()

    def mark_relay_done(self, task_id: int) -> None:
        self.conn().execute(
            "UPDATE relay_tasks SET status='DONE', last_error=NULL, updated_at=? WHERE id=?",
            (utcnow_iso(), task_id),
        )
        self.conn().commit()

    def mark_relay_skipped(self, task_id: int, reason: str) -> None:
        self.conn().execute(
            "UPDATE relay_tasks SET status='SKIPPED', last_error=?, updated_at=? WHERE id=?",
            (reason[:1000], utcnow_iso(), task_id),
        )
        self.conn().commit()

    def mark_relay_error(self, task_id: int, error: str) -> None:
        self.conn().execute(
            "UPDATE relay_tasks SET status='ERROR', last_error=?, updated_at=? WHERE id=?",
            (error[:1000], utcnow_iso(), task_id),
        )
        self.conn().commit()
