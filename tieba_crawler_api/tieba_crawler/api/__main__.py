from __future__ import annotations

import os

import uvicorn


def main() -> None:
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))

    # NOTE: In-memory job tracking works best with a single worker.
    # If you want multiple workers, consider externalizing the job queue/status.
    workers = int(os.getenv("WORKERS", "1"))
    reload = os.getenv("RELOAD", "false").strip().lower() in {"1", "true", "yes", "y"}

    uvicorn.run(
        "tieba_crawler.api.main:app",
        host=host,
        port=port,
        reload=reload,
        workers=workers,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )


if __name__ == "__main__":
    main()
