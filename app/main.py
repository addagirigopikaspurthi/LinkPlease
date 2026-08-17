from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from . import database as db
from .config import Settings, get_settings


logger = logging.getLogger("linkplease")


class RuleCreate(BaseModel):
    keyword: str = Field(min_length=1)
    dm_message: str = Field(min_length=1)


class CleanupRequest(BaseModel):
    event_id: str = Field(min_length=1)
    comment_id: str = Field(min_length=1)


class RollingRateLimiter:
    def __init__(self, limit: int, window_seconds: int) -> None:
        self.limit = limit
        self.window_seconds = window_seconds
        self.timestamps: deque[float] = deque()
        self.blocked_until = 0.0

    async def acquire(self, stop_event: asyncio.Event) -> bool:
        loop = asyncio.get_running_loop()
        while not stop_event.is_set():
            now = loop.time()
            while self.timestamps and now - self.timestamps[0] >= self.window_seconds:
                self.timestamps.popleft()

            wait_for_block = max(0.0, self.blocked_until - now)
            if wait_for_block > 0:
                await _sleep_or_stop(stop_event, wait_for_block)
                continue

            if len(self.timestamps) < self.limit:
                self.timestamps.append(now)
                return True

            oldest = self.timestamps[0]
            await _sleep_or_stop(stop_event, max(0.1, self.window_seconds - (now - oldest)))
        return False

    def defer(self, seconds: float) -> None:
        self.blocked_until = max(self.blocked_until, asyncio.get_running_loop().time() + seconds)


def verify_signature(raw_body: bytes, header: str | None, secret: str) -> bool:
    if not header or not header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header)


def retry_delay(attempts: int, retry_after: float | None = None) -> float:
    if retry_after is not None:
        return max(0.5, retry_after)
    return min(300.0, float(2 ** min(attempts, 6)))


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except TimeoutError:
        pass


async def event_worker(settings: Settings, stop_event: asyncio.Event, wake_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        rows = await asyncio.to_thread(db.claim_inbound_events, settings.database_path, 100)
        if not rows:
            wake_event.clear()
            await _sleep_or_stop(wake_event, settings.worker_poll_seconds)
            continue

        for row in rows:
            try:
                await asyncio.to_thread(db.process_inbound_event, settings.database_path, row["event_id"])
            except Exception as exc:
                logger.exception("Failed to process inbound event %s: %s", row["event_id"], exc)
                await _sleep_or_stop(stop_event, 0.5)
        wake_event.set()


async def send_worker(settings: Settings, stop_event: asyncio.Event) -> None:
    limiter = RollingRateLimiter(settings.send_rate_limit, settings.send_rate_window_seconds)
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
        while not stop_event.is_set():
            job = await asyncio.to_thread(db.claim_due_delivery_job, settings.database_path)
            if job is None:
                await _sleep_or_stop(stop_event, settings.worker_poll_seconds)
                continue

            if not settings.api_key:
                await asyncio.to_thread(
                    db.reschedule_send,
                    settings.database_path,
                    job["id"],
                    "PSEUDOGRAM_API_KEY is not configured",
                    60.0,
                    settings.max_send_attempts,
                )
                continue

            if not await limiter.acquire(stop_event):
                return

            headers = {
                "X-API-Key": settings.api_key,
                "Idempotency-Key": db.idempotency_key(job),
            }
            payload = {
                "recipient_user_id": job["recipient_user_id"],
                "message": job["message"],
                "comment_id": job["comment_id"],
            }
            try:
                response = await client.post(
                    f"{settings.api_base_url}/v1/dm/send",
                    json=payload,
                    headers=headers,
                )
            except httpx.HTTPError as exc:
                await asyncio.to_thread(
                    db.reschedule_send,
                    settings.database_path,
                    job["id"],
                    f"network error: {exc}",
                    retry_delay(job["attempts"]),
                    settings.max_send_attempts,
                )
                continue

            if response.status_code == 202:
                body = response.json()
                dm_id = body.get("dm_id")
                if isinstance(dm_id, str) and dm_id:
                    await asyncio.to_thread(
                        db.mark_send_accepted,
                        settings.database_path,
                        job["id"],
                        dm_id,
                        settings.status_poll_seconds,
                    )
                else:
                    await asyncio.to_thread(
                        db.reschedule_send,
                        settings.database_path,
                        job["id"],
                        "202 response missing dm_id",
                        retry_delay(job["attempts"]),
                        settings.max_send_attempts,
                    )
            elif response.status_code == 429:
                retry_after = _retry_after_header(response.headers)
                limiter.defer(retry_delay(job["attempts"], retry_after))
                await asyncio.to_thread(
                    db.reschedule_send,
                    settings.database_path,
                    job["id"],
                    "rate limited by PseudoGram",
                    retry_delay(job["attempts"], retry_after),
                    settings.max_send_attempts,
                )
            elif response.status_code >= 500:
                await asyncio.to_thread(
                    db.reschedule_send,
                    settings.database_path,
                    job["id"],
                    f"PseudoGram server error {response.status_code}",
                    retry_delay(job["attempts"]),
                    settings.max_send_attempts,
                )
            elif response.status_code == 400:
                await asyncio.to_thread(
                    db.fail_delivery,
                    settings.database_path,
                    job["id"],
                    f"invalid request: {response.text[:500]}",
                )
            else:
                await asyncio.to_thread(
                    db.reschedule_send,
                    settings.database_path,
                    job["id"],
                    f"unexpected response {response.status_code}: {response.text[:500]}",
                    retry_delay(job["attempts"]),
                    settings.max_send_attempts,
                )


async def status_worker(settings: Settings, stop_event: asyncio.Event) -> None:
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
        while not stop_event.is_set():
            job = await asyncio.to_thread(
                db.claim_due_status_check,
                settings.database_path,
                settings.status_poll_seconds,
            )
            if job is None:
                await _sleep_or_stop(stop_event, settings.worker_poll_seconds)
                continue

            if not settings.api_key:
                await asyncio.to_thread(
                    db.keep_waiting_for_delivery,
                    settings.database_path,
                    job["id"],
                    60.0,
                    "PSEUDOGRAM_API_KEY is not configured",
                )
                continue

            try:
                response = await client.get(
                    f"{settings.api_base_url}/v1/dm/{job['dm_id']}",
                    headers={"X-API-Key": settings.api_key},
                )
            except httpx.HTTPError as exc:
                await asyncio.to_thread(
                    db.keep_waiting_for_delivery,
                    settings.database_path,
                    job["id"],
                    settings.status_poll_seconds,
                    f"status network error: {exc}",
                )
                continue

            if response.status_code != 200:
                await asyncio.to_thread(
                    db.keep_waiting_for_delivery,
                    settings.database_path,
                    job["id"],
                    retry_delay(job["status_checks"]),
                    f"status check failed with {response.status_code}",
                )
                continue

            status = response.json().get("status")
            if status == "delivered":
                await asyncio.to_thread(db.mark_status_delivered, settings.database_path, job["id"])
            elif status == "failed":
                await asyncio.to_thread(
                    db.retry_after_delivery_failure,
                    settings.database_path,
                    job["id"],
                    "PseudoGram reported delivery failed",
                    retry_delay(job["attempts"]),
                    settings.max_send_attempts,
                )
            else:
                await asyncio.to_thread(
                    db.keep_waiting_for_delivery,
                    settings.database_path,
                    job["id"],
                    settings.status_poll_seconds,
                    None,
                )


def _retry_after_header(headers: httpx.Headers) -> float | None:
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await asyncio.to_thread(db.init_db, resolved_settings.database_path)
        app.state.settings = resolved_settings
        app.state.stop_event = asyncio.Event()
        app.state.wake_event = asyncio.Event()
        app.state.worker_tasks = []
        if not resolved_settings.disable_workers:
            app.state.worker_tasks = [
                asyncio.create_task(event_worker(resolved_settings, app.state.stop_event, app.state.wake_event)),
                asyncio.create_task(send_worker(resolved_settings, app.state.stop_event)),
                asyncio.create_task(status_worker(resolved_settings, app.state.stop_event)),
            ]
        try:
            yield
        finally:
            app.state.stop_event.set()
            app.state.wake_event.set()
            await asyncio.gather(*app.state.worker_tasks, return_exceptions=True)

    app = FastAPI(title="LinkPlease PseudoGram Automation", lifespan=lifespan)
    app.state.settings = resolved_settings

    @app.get("/health")
    async def health(request: Request) -> dict[str, str]:
        settings_for_request: Settings = request.app.state.settings
        api_key_status = "configured" if settings_for_request.api_key else "missing"
        worker_status = "disabled" if settings_for_request.disable_workers else "enabled"
        return {"status": "ok", "api_key": api_key_status, "workers": worker_status}

    @app.post("/rules", status_code=201)
    async def create_rule(rule: RuleCreate, request: Request) -> dict[str, str]:
        settings_for_request: Settings = request.app.state.settings
        try:
            return await asyncio.to_thread(
                db.create_rule,
                settings_for_request.database_path,
                rule.keyword,
                rule.dm_message,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/webhook")
    async def webhook(request: Request) -> dict[str, Any]:
        settings_for_request: Settings = request.app.state.settings
        raw_body = await request.body()
        if settings_for_request.verify_webhook_signatures:
            if not settings_for_request.api_key:
                raise HTTPException(status_code=503, detail="webhook signature secret is not configured")
            signature = request.headers.get("X-PseudoGram-Signature")
            if not verify_signature(raw_body, signature, settings_for_request.api_key):
                raise HTTPException(status_code=401, detail="invalid webhook signature")

        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="invalid JSON") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="payload must be an object")

        try:
            inserted = await asyncio.to_thread(
                db.enqueue_webhook_event,
                settings_for_request.database_path,
                payload,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        request.app.state.wake_event.set()
        return {"ok": True, "duplicate_event": not inserted}

    @app.get("/stats")
    async def stats(request: Request) -> dict[str, int]:
        settings_for_request: Settings = request.app.state.settings
        return await asyncio.to_thread(db.get_stats, settings_for_request.database_path)

    @app.post("/admin/cleanup-comment")
    async def admin_cleanup_comment(cleanup: CleanupRequest, request: Request) -> dict[str, int]:
        settings_for_request: Settings = request.app.state.settings
        if not settings_for_request.api_key:
            raise HTTPException(status_code=503, detail="admin secret is not configured")

        supplied_key = request.headers.get("X-Admin-Key") or request.headers.get("X-API-Key")
        if not supplied_key or not hmac.compare_digest(supplied_key, settings_for_request.api_key):
            raise HTTPException(status_code=401, detail="invalid admin key")

        return await asyncio.to_thread(
            db.cleanup_comment_state,
            settings_for_request.database_path,
            cleanup.event_id,
            cleanup.comment_id,
        )

    return app


app = create_app()
