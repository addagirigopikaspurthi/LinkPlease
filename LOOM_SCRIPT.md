# Loom Outline

Target length: about 3 minutes.

## 0:00-0:30 - What I built

This is a FastAPI service with the required `/rules`, `/webhook`, and `/stats` endpoints. The webhook route only verifies and stores the event, then returns quickly. Background workers handle rule matching, DM sending, and delivery reconciliation.

## 0:30-1:30 - Tradeoff

I chose SQLite with uniqueness constraints instead of an in-memory queue or Redis. The upside is durability and simple deployment: if PseudoGram fails or the process restarts, queued events and delivery jobs are still on disk. The downside is that this version should run as one web process, because SQLite plus the in-process rate limiter is not a horizontally scalable design.

Show:

- `delivery_jobs UNIQUE(rule_id, recipient_user_id)`
- `inbound_events.event_id`
- `FAILURES.md`

## 1:30-2:20 - Handling hostile API behavior

Talk through:

- duplicate webhook event IDs are ignored
- multiple comments from the same user for the same rule are blocked
- `429` and `5xx` send responses are retried
- accepted DMs are polled until delivered or failed
- later failed DMs are retried with a new idempotency cycle
- `comment.deleted` cancels unsent jobs

## 2:20-3:00 - One More Week

With one more week, I would move the queue and rate limiter to Postgres or Redis so multiple workers can coordinate safely. I would also add a reconciliation dashboard that compares local stats to PseudoGram truth runs and alerts if a job is stuck in `queued` too long.
