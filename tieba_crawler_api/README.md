# Tieba Crawler API (FastAPI)

This is your original Tieba crawler project, **unchanged in behavior**, with an added **FastAPI** HTTP layer so your frontend can:

- Trigger existing jobs (crawl threads, download images, sync collections, relay labeled threads)
- Read data from the SQLite database (threads, images, relay tasks)
- Serve downloaded images over HTTP

The original job logic is preserved — the API just calls the same functions.

## 1) Install

```bash
pip install -r requirements.txt
```

## 2) Configure `.env`

Create a `.env` next to where you run the server, e.g.:

```dotenv
# Storage
DB_URL=sqlite:///data/tieba.db
DATA_DIR=data
FORUM=your_forum_name
TIMEZONE=Asia/Shanghai

# Optional: auth (needed for relay posting; not needed for crawling)
BDUSS=...
STOKEN=...

# Optional: protect API
API_KEY=change_me

# Optional: allow frontend origins (comma separated or JSON array)
# CORS_ORIGINS=http://localhost:3000,https://your.domain
# CORS_ORIGINS=["http://localhost:3000"]
```

## 3) Run API

```bash
python -m tieba_crawler.api
```

Or with uvicorn:

```bash
uvicorn tieba_crawler.api.main:app --host 0.0.0.0 --port 8000 --reload
```

Open API docs:
- Swagger UI: `http://localhost:8000/docs`
- OpenAPI JSON: `http://localhost:8000/openapi.json`

## 4) Serve downloaded images

Downloaded images are stored under:

```
DATA_DIR/images/<forum>/<tid>/<hash>.<ext>
```

The server mounts `DATA_DIR` at `/files`, so images become:

```
http://localhost:8000/files/images/<forum>/<tid>/<hash>.<ext>
```

## 5) Main endpoints

All endpoints are under `/v1` (and protected if `API_KEY` is set).

### Data
- `GET /v1/threads`
- `GET /v1/threads/{tid}`
- `POST /v1/threads/{tid}/category`
- `GET /v1/images`
- `GET /v1/relay-tasks`
- `GET /v1/stats`

### Jobs
These start background jobs and return a `job_id`:
- `POST /v1/jobs/crawl-threads`
- `POST /v1/jobs/download-images`
- `POST /v1/jobs/sync-collections`
- `POST /v1/jobs/relay-labeled`

Check job status:
- `GET /v1/jobs/{job_id}`
- `GET /v1/jobs`

## Notes

- Job statuses are tracked **in memory**. For best results, run **one worker** (`WORKERS=1`).
- SQLite is used exactly as in the original project.
