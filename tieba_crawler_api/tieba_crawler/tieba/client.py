from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
import aiotieba as tb
from aiotieba import TimeoutConfig, ThreadSortType

log = logging.getLogger(__name__)

RETRYABLE_EXC = (
    asyncio.TimeoutError,
    TimeoutError,
    aiohttp.ClientError,
    OSError,
    ConnectionError,
)

def _default_timeout() -> TimeoutConfig:
    return TimeoutConfig(
        http_acquire_conn=6.0,
        http_connect=10.0,
        http_read=30.0,
        http_keepalive=30.0,
        ws_send=5.0,
        ws_read=20.0,
        ws_close=10.0,
        ws_keepalive=300.0,
        ws_heartbeat=30.0,
        dns_ttl=600,
    )

@dataclass(frozen=True)
class TiebaAPI:
    bduss: str = ""
    stoken: str = ""
    try_ws: bool = False
    request_attempts: int = 5
    timeout: TimeoutConfig = field(default_factory=_default_timeout)

    async def _get_threads_page(self, forum: str | int, pn: int, rn: int, *, try_ws: bool) -> tb.api.threads.Threads:
        async with tb.Client(self.bduss, self.stoken, try_ws=try_ws, timeout=self.timeout) as client:
            return await client.get_threads(
                forum,
                pn=pn,
                rn=rn,
                sort=ThreadSortType.CREATE,
                is_good=False,
            )

    async def get_threads_page_with_retry(self, forum: str | int, pn: int, rn: int) -> tb.api.threads.Threads:
        last_exc: Optional[BaseException] = None

        for attempt in range(1, self.request_attempts + 1):
            use_ws = self.try_ws and (attempt <= max(1, self.request_attempts // 2))

            try:
                threads = await self._get_threads_page(forum, pn, rn, try_ws=use_ws)

                # IMPORTANT: aiotieba sometimes returns an object with .err instead of raising
                err = getattr(threads, "err", None)
                if err:
                    raise err

                return threads

            except RETRYABLE_EXC as e:
                last_exc = e
                backoff = min(2 ** (attempt - 1), 30) + random.random()
                log.warning(
                    "get_threads error (forum=%s pn=%s attempt=%s/%s ws=%s sleep=%.2fs): %r",
                    forum,
                    pn,
                    attempt,
                    self.request_attempts,
                    use_ws,
                    backoff,
                    e,
                )
                await asyncio.sleep(backoff)

            except Exception:
                # Non-retryable: fail fast
                raise

        assert last_exc is not None
        raise last_exc

    # ---- posting ----
    async def _add_post(self, forum: str | int, tid: int, content: str, *, try_ws: bool) -> tb.api.response.BoolResponse:
        async with tb.Client(self.bduss, self.stoken, try_ws=try_ws, timeout=self.timeout) as client:
            return await client.add_post(forum, tid, content)

    async def add_post_safe(self, forum: str | int, tid: int, content: str):
        """Safer add_post:
        - Force HTTP (try_ws=False)
        - No automatic retry on timeout (unknown if posted)
        """
        try:
            resp = await self._add_post(forum, tid, content, try_ws=False)
            if getattr(resp, "err", None):
                raise resp.err
            return resp
        except asyncio.TimeoutError:
            raise
