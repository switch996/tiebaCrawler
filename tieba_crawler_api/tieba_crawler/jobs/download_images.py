from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

import aiohttp

from tieba_crawler_api.tieba_crawler.db.repo import Repo
from tieba_crawler_api.tieba_crawler.settings import Settings

log = logging.getLogger(__name__)

_CT_EXT = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/bmp": ".bmp",
}

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _guess_ext(url: str, content_type: Optional[str]) -> str:
    try:
        path = urlparse(url).path
        suffix = Path(path).suffix.lower()
        if suffix in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}:
            return ".jpg" if suffix == ".jpeg" else suffix
    except Exception:
        pass

    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct in _CT_EXT:
            return _CT_EXT[ct]
    return ".jpg"

async def _download_to_file(
    session: aiohttp.ClientSession,
    url: str,
    dst_path: Path,
    *,
    attempts: int = 3,
    timeout_total: int = 30,
) -> Tuple[bool, str]:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dst_path.with_suffix(dst_path.suffix + ".part")

    last_err = ""
    for i in range(1, attempts + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=timeout_total)
            async with session.get(url, timeout=timeout) as resp:
                if resp.status >= 400:
                    raise aiohttp.ClientResponseError(
                        resp.request_info, resp.history, status=resp.status, message=await resp.text()
                    )
                ext = _guess_ext(url, resp.headers.get("Content-Type"))
                if dst_path.suffix != ext:
                    dst_path = dst_path.with_suffix(ext)
                    tmp_path = dst_path.with_suffix(dst_path.suffix + ".part")

                with open(tmp_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(64 * 1024):
                        f.write(chunk)
                os.replace(tmp_path, dst_path)
                return True, str(dst_path)
        except Exception as e:
            last_err = repr(e)
            await asyncio.sleep(min(2 ** (i - 1), 8) + (0.1 * i))
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
    return False, last_err

async def download_images(
    *,
    settings: Settings,
    limit: int = 200,
    concurrency: Optional[int] = None,
    include_error: bool = False,
) -> None:
    repo = Repo(settings=settings)
    repo.ensure_schema()

    reset_n = repo.reset_stuck_downloads()
    if reset_n:
        log.warning("Reset %s stuck DOWNLOADING tasks back to PENDING.", reset_n)

    concurrency = concurrency if concurrency is not None else settings.image_concurrency
    tasks = repo.claim_image_tasks(limit=limit, include_error=include_error)
    if not tasks:
        log.info("No image tasks to download.")
        repo.close()
        return

    base_dir = settings.data_dir / "images"
    sem = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(headers={"User-Agent": "tieba-crawler/0.2"}) as session:

        async def worker(task: Dict):
            async with sem:
                image_id = int(task["id"])
                tid = int(task["tid"])
                forum = task.get("forum") or "unknown_forum"
                url = task["url"]
                hash_ = task.get("hash") or _sha1(url)[:16]

                local_path = task.get("local_path")
                if local_path:
                    p = Path(local_path)
                    if p.exists():
                        repo.mark_image_done(image_id, str(p))
                        return

                dst_dir = base_dir / str(forum) / str(tid)
                dst_path = dst_dir / (hash_ + ".jpg")

                ok, info = await _download_to_file(
                    session,
                    url,
                    dst_path,
                    attempts=settings.image_attempts,
                    timeout_total=45,
                )

                if ok:
                    repo.mark_image_done(image_id, info)
                    log.info("Downloaded image id=%s tid=%s -> %s", image_id, tid, info)
                else:
                    repo.mark_image_error(image_id, info)
                    log.warning("Failed download image id=%s tid=%s url=%s err=%s", image_id, tid, url, info)

        await asyncio.gather(*(worker(t) for t in tasks))

    repo.close()
