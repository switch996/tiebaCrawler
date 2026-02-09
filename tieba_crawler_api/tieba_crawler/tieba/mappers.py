from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

_YEAR_WEEK_RE = re.compile(r"(?P<year>\d{4})\s*年?\s*第\s*(?P<week>\d{1,2})\s*周")

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _safe_str(v: Any) -> str:
    return "" if v is None else str(v)

def contents_to_json(contents: Any) -> str:
    if contents is None:
        return json.dumps({}, ensure_ascii=False)

    imgs = []
    for img in getattr(contents, "imgs", []) or []:
        imgs.append(
            {
                "src": getattr(img, "src", None),
                "big_src": getattr(img, "big_src", None),
                "origin_src": getattr(img, "origin_src", None),
                "origin_size": getattr(img, "origin_size", None),
                "show_width": getattr(img, "show_width", None),
                "show_height": getattr(img, "show_height", None),
                "hash": getattr(img, "hash", None),
            }
        )

    payload = {
        "text": getattr(contents, "text", "") or "",
        "imgs": imgs,
    }
    return json.dumps(payload, ensure_ascii=False)

def parse_year_week_from_title(title: str) -> Tuple[Optional[int], Optional[int]]:
    if not title:
        return None, None
    m = _YEAR_WEEK_RE.search(title)
    if not m:
        return None, None
    try:
        y = int(m.group("year"))
        w = int(m.group("week"))
        if w < 1 or w > 53:
            return y, None
        return y, w
    except Exception:
        return None, None

def detect_collection_from_title(title: str, rules: dict) -> Tuple[bool, Optional[str], Optional[int], Optional[int]]:
    """Return (is_collection, category, year, week).

    Detection strategy:
    - Title must contain year/week like '2026第1周'
    - Title must match one of category keywords configured in settings.collection_rules
    """
    year, week = parse_year_week_from_title(title)
    if year is None or week is None:
        return False, None, None, None

    # rules: {category: [keyword1, keyword2]}
    for cat, keywords in (rules or {}).items():
        if not keywords:
            continue
        for kw in keywords:
            if kw and kw in title:
                return True, str(cat), year, week
    return False, None, year, week

def thread_to_row(th: Any) -> Dict[str, Any]:
    user = getattr(th, "user", None)
    author_name = ""
    if user is not None:
        author_name = getattr(user, "show_name", "") or getattr(user, "user_name", "") or ""

    contents = getattr(th, "contents", None)
    content_text = ""
    if contents is not None:
        content_text = getattr(contents, "text", "") or ""

    return {
        "tid": int(getattr(th, "tid", 0) or 0),
        "fid": int(getattr(th, "fid", 0) or 0),
        "fname": _safe_str(getattr(th, "fname", "")),
        "title": _safe_str(getattr(th, "title", "")),
        "author_id": int(getattr(th, "author_id", 0) or 0),
        "author_name": author_name,

        "agree": int(getattr(th, "agree", 0) or 0),
        "pid": int(getattr(th, "pid", 0) or 0),

        "create_time": int(getattr(th, "create_time", 0) or 0),
        "last_time": int(getattr(th, "last_time", 0) or 0),
        "reply_num": int(getattr(th, "reply_num", 0) or 0),
        "view_num": int(getattr(th, "view_num", 0) or 0),

        "is_top": 1 if bool(getattr(th, "is_top", False)) else 0,
        "is_good": 1 if bool(getattr(th, "is_good", False)) else 0,
        "is_help": 1 if bool(getattr(th, "is_help", False)) else 0,
        "is_hide": 1 if bool(getattr(th, "is_hide", False)) else 0,
        "is_share": 1 if bool(getattr(th, "is_share", False)) else 0,

        "text": content_text,
        "contents_json": contents_to_json(contents),
        "ai_reply_content": None,
        "process_status": "new",

        # labeling/routing fields (do not overwrite on upsert unless non-empty)
        "category": None,
        "tags_json": None,
        "thread_role": "normal",
        "collection_year": None,
        "collection_week": None,

        "updated_at": utcnow_iso(),
    }

def canonical_image_url(img: Any) -> str | None:
    # Prefer big_src (big image), then origin_src, then src
    for k in ("big_src", "origin_src", "src"):
        v = getattr(img, k, None)
        if v:
            return str(v)
    return None

def image_tasks_from_thread(th: Any) -> List[Dict[str, Any]]:
    tid = int(getattr(th, "tid", 0))
    contents = getattr(th, "contents", None)
    tasks: List[Dict[str, Any]] = []
    if not contents:
        return tasks

    for img in getattr(contents, "imgs", []) or []:
        url = canonical_image_url(img)
        if not url:
            continue
        tasks.append(
            {
                "tid": tid,
                "url": url,
                "hash": getattr(img, "hash", None),
                "origin_src": getattr(img, "origin_src", None),
                "src": getattr(img, "src", None),
                "big_src": getattr(img, "big_src", None),
                "show_width": int(getattr(img, "show_width", 0) or 0),
                "show_height": int(getattr(img, "show_height", 0) or 0),
                "status": "PENDING",
                "updated_at": utcnow_iso(),
            }
        )
    return tasks
