# LinkPlease PseudoGram Assignment

FastAPI + SQLite implementation for the LinkPlease Tech Intern assignment.

## Shortlist Highlights

- Webhook returns quickly because it only verifies and persists events.
- Durable queue lives in SQLite, not process memory.
- Database uniqueness constraints enforce idempotency for webhook redelivery and per-user/per-rule duplicate DMs.
- Sends are rate-limited to PseudoGram's 10 requests per rolling 60 seconds.
- `202 Accepted` is treated as pending; the app polls delivery status and retries later failures.
- `GET /stats` counts from source-of-truth tables instead of in-memory counters.
- `FAILURES.md` is honest and specific, as requested by the assignment.

The service implements:

- `POST /rules`
- `POST /webhook`
- `GET /stats`
- webhook HMAC verification support when `PSEUDOGRAM_API_KEY` is configured
- durable inbound event storage
- duplicate prevention by `(rule_id, user_id)`
- retrying DM sends on network errors, `429`, and `5xx`
- reconciliation of accepted DMs with `GET /v1/dm/{dm_id}`
- cancellation of unsent jobs for `comment.deleted`
- a rolling 10-per-60-second send limiter

## How It Works

`POST /webhook` only verifies, parses, and stores the event in SQLite, then returns immediately. A background event worker matches queued events against stored rules and creates delivery jobs. A send worker drains delivery jobs through PseudoGram with an `Idempotency-Key`. A status worker polls accepted DMs until they are delivered or reported failed. Failed accepted DMs get a new idempotency cycle and are sent again.

SQLite uniqueness constraints enforce the important idempotency rule:

- `inbound_events.event_id` ignores redelivered webhook events.
- `delivery_jobs UNIQUE(rule_id, recipient_user_id)` prevents the same user receiving the same rule twice.
- `duplicate_blocks UNIQUE(rule_id, recipient_user_id, comment_id)` keeps `/stats.duplicates_blocked` honest if the same duplicate arrives again.

## Local Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

Set `PSEUDOGRAM_API_KEY` in `.env` after applying for a key.

The app loads `.env` automatically for local development.
If you want to test `/webhook` before you have a key, set `VERIFY_WEBHOOK_SIGNATURES=false` locally only.
The Render config sets `STRICT_WEBHOOK_SIGNATURES=false` so simulator traffic is not dropped if its signature header differs from the written spec. Set `STRICT_WEBHOOK_SIGNATURES=true` only after confirming live simulator requests include the expected HMAC header.

Run locally:

```powershell
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Health check:

```powershell
curl http://127.0.0.1:8000/health
```

## Required Assignment Steps

Apply for the PseudoGram key:

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/apply \
  -H "Content-Type: application/json" \
  -d '{
    "name": "YOUR NAME",
    "email": "you@example.com",
    "phone": "+91...",
    "whatsapp": "+91...",
    "linkedin_url": "https://linkedin.com/in/you"
  }'
```

Generate the key:

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/keygen \
  -H "Content-Type: application/json" \
  -d '{"email": "you@example.com"}'
```

Create a rule against your deployed app:

```bash
curl -X POST https://YOUR-APP.example.com/rules \
  -H "Content-Type: application/json" \
  -d '{"keyword":"PRICE","dm_message":"Here is the price list: ..."}'
```

Start a simulation:

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/simulate/start \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_API_KEY" \
  -d '{"webhook_url":"https://YOUR-APP.example.com/webhook","count":500,"duration_seconds":10}'
```

Check local stats:

```bash
curl https://YOUR-APP.example.com/stats
```

Check PseudoGram truth:

```bash
curl https://pseudogram-api.onrender.com/v1/simulate/RUN_ID/truth \
  -H "X-API-Key: YOUR_API_KEY"
```

Submit:

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/submit \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_API_KEY" \
  -d '{
    "email": "you@example.com",
    "github_repo": "https://github.com/you/repo",
    "working_url": "https://YOUR-APP.example.com",
    "loom_url": "https://loom.com/share/...",
    "parts_completed": "A+B+C",
    "start_date": "2026-08-16"
  }'
```

## Deployment Notes

Use one web process and a persistent disk for SQLite. This repo includes both `Procfile` and `render.yaml`. On Render, use the Blueprint if possible; it configures:

- Python web service
- `/health` health check
- persistent disk mounted at `/data`
- `DATABASE_PATH=/data/linkplease.db`
- one service instance

Set this secret manually in Render:

- `PSEUDOGRAM_API_KEY`

Do not scale horizontally unless the rate limiter and queue are moved to Redis/Postgres. Without a persistent disk, a restart can erase queued jobs and stats.

If you deploy somewhere other than Render, set:

- `PSEUDOGRAM_API_KEY`
- `PSEUDOGRAM_API_BASE_URL=https://pseudogram-api.onrender.com`
- `DATABASE_PATH` to a persistent path
- `VERIFY_WEBHOOK_SIGNATURES=true`

The app intentionally fails closed when signature verification is enabled but `PSEUDOGRAM_API_KEY` is missing.

## Tests

```powershell
pytest
```

The tests cover rule matching, duplicate blocking, signature verification, deletion cancellation, and retry idempotency cycles.
