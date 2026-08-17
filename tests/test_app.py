from __future__ import annotations

import hmac
import hashlib
import json

from fastapi.testclient import TestClient

from app import database as db
from app.config import Settings
from app.main import create_app


def test_rule_matching_blocks_same_user_twice(tmp_path):
    settings = make_settings(tmp_path / "app.db", api_key=None, verify=False)
    app = create_app(settings)

    with TestClient(app) as client:
        response = client.post(
            "/rules",
            json={"keyword": "PRICE", "dm_message": "Here is the price list"},
        )
        assert response.status_code == 201

        first_event = comment_event("evt_1", "cmt_1", "usr_1", "what is the PRICE?")
        assert client.post("/webhook", json=first_event).status_code == 200
        process_all(settings.database_path)
        assert client.get("/stats").json() == {
            "sent": 0,
            "failed": 0,
            "queued": 1,
            "duplicates_blocked": 0,
        }

        duplicate_delivery = comment_event("evt_2", "cmt_2", "usr_1", "price again")
        assert client.post("/webhook", json=duplicate_delivery).status_code == 200
        process_all(settings.database_path)
        assert client.get("/stats").json() == {
            "sent": 0,
            "failed": 0,
            "queued": 1,
            "duplicates_blocked": 1,
        }

        redelivery = client.post("/webhook", json=first_event)
        assert redelivery.status_code == 200
        assert redelivery.json()["duplicate_event"] is True
        process_all(settings.database_path)
        assert client.get("/stats").json()["duplicates_blocked"] == 1


def test_keyword_matching_is_case_insensitive_and_anywhere(tmp_path):
    settings = make_settings(tmp_path / "app.db", api_key=None, verify=False)
    app = create_app(settings)

    with TestClient(app) as client:
        assert client.post("/rules", json={"keyword": "PrIcE", "dm_message": "price list"}).status_code == 201
        event = comment_event("evt_anywhere", "cmt_anywhere", "usr_1", "can you send xxpRiCexx?")
        assert client.post("/webhook", json=event).status_code == 200
        process_all(settings.database_path)

        assert client.get("/stats").json() == {
            "sent": 0,
            "failed": 0,
            "queued": 1,
            "duplicates_blocked": 0,
        }


def test_signature_validation_rejects_forged_webhook(tmp_path):
    settings = make_settings(tmp_path / "app.db", api_key="secret", verify=True)
    app = create_app(settings)
    payload = comment_event("evt_signed", "cmt_signed", "usr_1", "PRICE")
    raw_body = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    with TestClient(app) as client:
        rejected = client.post(
            "/webhook",
            content=raw_body,
            headers={"Content-Type": "application/json"},
        )
        assert rejected.status_code == 401

        signature = "sha256=" + hmac.new(b"secret", raw_body, hashlib.sha256).hexdigest()
        accepted = client.post(
            "/webhook",
            content=raw_body,
            headers={
                "Content-Type": "application/json",
                "X-PseudoGram-Signature": signature,
            },
        )
        assert accepted.status_code == 200


def test_signature_verification_fails_closed_without_secret(tmp_path):
    settings = make_settings(tmp_path / "app.db", api_key=None, verify=True)
    app = create_app(settings)

    with TestClient(app) as client:
        response = client.post("/webhook", json=comment_event("evt_1", "cmt_1", "usr_1", "PRICE"))
        assert response.status_code == 503


def test_deleted_comment_cancels_unsent_delivery(tmp_path):
    settings = make_settings(tmp_path / "app.db", api_key=None, verify=False)
    app = create_app(settings)

    with TestClient(app) as client:
        assert client.post("/rules", json={"keyword": "price", "dm_message": "menu"}).status_code == 201
        assert client.post("/webhook", json=comment_event("evt_1", "cmt_1", "usr_1", "PRICE")).status_code == 200
        process_all(settings.database_path)
        assert client.get("/stats").json()["queued"] == 1

        deleted_event = {
            "event_id": "evt_delete",
            "event_type": "comment.deleted",
            "sent_at": "2026-08-10T09:14:23.000Z",
            "data": {"comment_id": "cmt_1"},
        }
        assert client.post("/webhook", json=deleted_event).status_code == 200
        process_all(settings.database_path)
        assert client.get("/stats").json()["queued"] == 0


def test_deleted_comment_before_created_prevents_delivery(tmp_path):
    settings = make_settings(tmp_path / "app.db", api_key=None, verify=False)
    app = create_app(settings)

    with TestClient(app) as client:
        assert client.post("/rules", json={"keyword": "PRICE", "dm_message": "price list"}).status_code == 201
        deleted_event = {
            "event_id": "evt_delete_first",
            "event_type": "comment.deleted",
            "sent_at": "2026-08-10T09:14:20.000Z",
            "data": {"comment_id": "cmt_deleted_first"},
        }
        assert client.post("/webhook", json=deleted_event).status_code == 200
        assert client.post(
            "/webhook",
            json=comment_event("evt_create_late", "cmt_deleted_first", "usr_1", "PRICE please"),
        ).status_code == 200

        process_all(settings.database_path)
        assert client.get("/stats").json() == {
            "sent": 0,
            "failed": 0,
            "queued": 0,
            "duplicates_blocked": 0,
        }


def test_delivery_failure_uses_new_idempotency_cycle(tmp_path):
    database_path = str(tmp_path / "app.db")
    db.init_db(database_path)
    rule = db.create_rule(database_path, "PRICE", "price list")
    assert rule["rule_id"]
    assert db.enqueue_webhook_event(database_path, comment_event("evt_1", "cmt_1", "usr_1", "PRICE"))
    process_all(database_path)

    job = db.claim_due_delivery_job(database_path)
    assert job is not None
    assert db.idempotency_key(job).endswith(":cycle:1")

    db.mark_send_accepted(database_path, job["id"], "dm_1", 0)
    accepted = db.claim_due_status_check(database_path, 0)
    assert accepted is not None
    db.retry_after_delivery_failure(database_path, accepted["id"], "failed later", 0, max_attempts=12)

    retry = db.claim_due_delivery_job(database_path)
    assert retry is not None
    assert retry["send_cycle"] == 2
    assert db.idempotency_key(retry).endswith(":cycle:2")


def test_500_events_keep_exact_stats_for_duplicate_users(tmp_path):
    settings = make_settings(tmp_path / "app.db", api_key=None, verify=False)
    app = create_app(settings)

    with TestClient(app) as client:
        assert client.post("/rules", json={"keyword": "PRICE", "dm_message": "price list"}).status_code == 201
        for index in range(500):
            user_index = index % 100
            event = comment_event(
                f"evt_{index}",
                f"cmt_{index}",
                f"usr_{user_index}",
                "please send PRICE",
            )
            assert client.post("/webhook", json=event).status_code == 200

        process_all(settings.database_path)
        assert client.get("/stats").json() == {
            "sent": 0,
            "failed": 0,
            "queued": 100,
            "duplicates_blocked": 400,
        }


def test_delivered_job_counts_as_sent(tmp_path):
    database_path = str(tmp_path / "app.db")
    db.init_db(database_path)
    db.create_rule(database_path, "PRICE", "price list")
    assert db.enqueue_webhook_event(database_path, comment_event("evt_1", "cmt_1", "usr_1", "PRICE"))
    process_all(database_path)

    job = db.claim_due_delivery_job(database_path)
    assert job is not None
    db.mark_send_accepted(database_path, job["id"], "dm_1", 0)
    accepted = db.claim_due_status_check(database_path, 0)
    assert accepted is not None
    db.mark_status_delivered(database_path, accepted["id"])

    assert db.get_stats(database_path) == {
        "sent": 1,
        "failed": 0,
        "queued": 0,
        "duplicates_blocked": 0,
    }


def process_all(database_path: str) -> None:
    while True:
        rows = db.claim_inbound_events(database_path, 100)
        if not rows:
            return
        for row in rows:
            db.process_inbound_event(database_path, row["event_id"])


def comment_event(event_id: str, comment_id: str, user_id: str, text: str) -> dict:
    return {
        "event_id": event_id,
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": comment_id,
            "post_id": "post_44de1b",
            "text": text,
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {
                "user_id": user_id,
                "username": "creator.fan",
            },
        },
    }


def make_settings(database_path, api_key: str | None, verify: bool) -> Settings:
    return Settings(
        api_base_url="https://pseudogram-api.onrender.com",
        api_key=api_key,
        database_path=str(database_path),
        verify_webhook_signatures=verify,
        disable_workers=True,
        max_send_attempts=12,
        send_rate_limit=10,
        send_rate_window_seconds=60,
        worker_poll_seconds=0.01,
        status_poll_seconds=0.01,
    )
