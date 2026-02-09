from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class JobResponse(BaseModel):
    job_id: str
    job_type: str
    status: str
    created_at: float
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    error: Optional[str] = None
    result: Optional[Any] = None


class CrawlThreadsRequest(BaseModel):
    forum: str = Field(..., description="Forum name or fid")
    rn: Optional[int] = Field(None, description="Threads per page (max 100)")
    initial_hours: Optional[int] = Field(None, description="First run: look back N hours")
    overlap_seconds: Optional[int] = Field(None, description="Incremental overlap window")
    max_pages: Optional[int] = Field(None, description="Safety limit")


class SyncCollectionsRequest(BaseModel):
    forum: str
    days: int = Field(120, ge=1, le=3650)
    dry_run: bool = False


class RelayLabeledRequest(BaseModel):
    forum: str
    category: Optional[str] = None
    include_error: bool = False
    dry_run: bool = False
    mode: Optional[str] = Field(None, description="link or full")
    max_posts: Optional[int] = Field(None, ge=1, le=100)
    min_interval_seconds: Optional[int] = Field(None, ge=0)
    max_text_chars: Optional[int] = Field(None, ge=0)
    max_images: Optional[int] = Field(None, ge=0)
    lookback_days: Optional[int] = Field(None, ge=1, le=3650)


class SetCategoryRequest(BaseModel):
    category: str
    tags: Optional[List[str]] = None

    def tags_json(self) -> Optional[str]:
        if self.tags is None:
            return None
        return json.dumps([str(x) for x in self.tags], ensure_ascii=False)


class ThreadListItem(BaseModel):
    tid: int
    fname: Optional[str] = None
    title: Optional[str] = None
    author_id: Optional[int] = None
    author_name: Optional[str] = None
    create_time: Optional[int] = None
    last_time: Optional[int] = None
    reply_num: Optional[int] = None
    view_num: Optional[int] = None
    is_top: Optional[int] = None
    is_good: Optional[int] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    thread_role: Optional[str] = None
    collection_year: Optional[int] = None
    collection_week: Optional[int] = None
    process_status: Optional[str] = None


class ImageItem(BaseModel):
    id: int
    tid: int
    url: str
    hash: Optional[str] = None
    origin_src: Optional[str] = None
    src: Optional[str] = None
    big_src: Optional[str] = None
    show_width: Optional[int] = None
    show_height: Optional[int] = None
    updated_at: Optional[str] = None


class ThreadDetail(BaseModel):
    tid: int
    fid: Optional[int] = None
    fname: Optional[str] = None
    title: Optional[str] = None
    author_id: Optional[int] = None
    author_name: Optional[str] = None
    agree: Optional[int] = None
    pid: Optional[int] = None
    create_time: Optional[int] = None
    last_time: Optional[int] = None
    reply_num: Optional[int] = None
    view_num: Optional[int] = None
    is_top: Optional[int] = None
    is_good: Optional[int] = None
    is_help: Optional[int] = None
    is_hide: Optional[int] = None
    is_share: Optional[int] = None
    text: Optional[str] = None
    contents: Optional[Dict[str, Any]] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    thread_role: Optional[str] = None
    collection_year: Optional[int] = None
    collection_week: Optional[int] = None
    updated_at: Optional[str] = None
    source_url: Optional[str] = None
    images: List[ImageItem] = Field(default_factory=list)


class RelayTaskItem(BaseModel):
    id: int
    source_tid: int
    target_tid: int
    target_forum: str
    category: Optional[str] = None
    source_year: Optional[int] = None
    source_week: Optional[int] = None
    status: str
    attempts: int
    last_error: Optional[str] = None
    created_at: str
    updated_at: str


class BatchItem(BaseModel):
    tid: int
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    ai_reply_content: Optional[str] = None
    process_status: Optional[str] = None

class BatchRequest(BaseModel):
    items: List[BatchItem] = Field(..., min_length=1, max_length=500)


