from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


TERMINAL_STATUSES = {"delivered", "failed", "cancelled"}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utcnow().isoformat()


def iso_after(seconds: float) -> str:
    return (utcnow() + timedelta(seconds=seconds)).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def connect(database_path: str) -> sqlite3.Connection:
    path = Path(database_path)
    if path.parent and str(path.parent) not in {"", "."}:
        path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path), timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout = 30000")
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")
    return con


def init_db(database_path: str) -> None:
    now = iso_now()
    with connect(database_path) as con:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS rules (
                id TEXT PRIMARY KEY,
                keyword TEXT NOT NULL,
                keyword_lower TEXT NOT NULL,
                dm_message TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS inbound_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                received_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                processed_at TEXT,
                error TEXT
            );

            CREATE INDEX IF NOT EXISTS inbound_events_status_idx
                ON inbound_events(status, received_at);

            CREATE TABLE IF NOT EXISTS comments (
                comment_id TEXT PRIMARY KEY,
                post_id TEXT,
                text TEXT,
                user_id TEXT,
                username TEXT,
                created_at TEXT,
                deleted_at TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS delivery_jobs (
                id TEXT PRIMARY KEY,
                rule_id TEXT NOT NULL,
                recipient_user_id TEXT NOT NULL,
                comment_id TEXT NOT NULL,
                message TEXT NOT NULL,
                send_cycle INTEGER NOT NULL DEFAULT 1,
                dm_id TEXT,
                status TEXT NOT NULL DEFAULT 'queued',
                attempts INTEGER NOT NULL DEFAULT 0,
                status_checks INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(rule_id, recipient_user_id),
                FOREIGN KEY(rule_id) REFERENCES rules(id)
            );

            CREATE INDEX IF NOT EXISTS delivery_jobs_due_idx
                ON delivery_jobs(status, next_attempt_at, created_at);

            CREATE TABLE IF NOT EXISTS duplicate_blocks (
                id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                rule_id TEXT NOT NULL,
                recipient_user_id TEXT NOT NULL,
                comment_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(rule_id, recipient_user_id, comment_id)
            );
            """
        )
        con.execute("UPDATE inbound_events SET status = 'queued' WHERE status = 'processing'")
        con.execute(
            """
            UPDATE delivery_jobs
            SET status = 'retry',
                next_attempt_at = ?,
                updated_at = ?,
                last_error = 'process restarted during send'
            WHERE status = 'sending'
            """,
            (now, now),
        )


def create_rule(database_path: str, keyword: str, dm_message: str) -> dict[str, Any]:
    keyword = keyword.strip()
    dm_message = dm_message.strip()
    if not keyword:
        raise ValueError("keyword must not be empty")
    if not dm_message:
        raise ValueError("dm_message must not be empty")

    rule = {
        "rule_id": new_id("rule"),
        "keyword": keyword,
        "dm_message": dm_message,
    }
    with connect(database_path) as con:
        con.execute(
            """
            INSERT INTO rules(id, keyword, keyword_lower, dm_message, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (rule["rule_id"], keyword, keyword.lower(), dm_message, iso_now()),
        )
    return rule


def enqueue_webhook_event(database_path: str, payload: dict[str, Any]) -> bool:
    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("event_id is required")
    if not isinstance(event_type, str) or not event_type:
        raise ValueError("event_type is required")

    try:
        with connect(database_path) as con:
            con.execute(
                """
                INSERT INTO inbound_events(event_id, event_type, payload_json, received_at, status)
                VALUES (?, ?, ?, ?, 'queued')
                """,
                (event_id, event_type, json.dumps(payload, separators=(",", ":")), iso_now()),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def claim_inbound_events(database_path: str, limit: int = 50) -> list[dict[str, Any]]:
    now = iso_now()
    with connect(database_path) as con:
        con.execute("BEGIN IMMEDIATE")
        rows = con.execute(
            """
            SELECT event_id, payload_json
            FROM inbound_events
            WHERE status = 'queued'
            ORDER BY received_at
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        event_ids = [row["event_id"] for row in rows]
        con.executemany(
            "UPDATE inbound_events SET status = 'processing', error = NULL WHERE event_id = ?",
            [(event_id,) for event_id in event_ids],
        )
        con.commit()
    return [{"event_id": row["event_id"], "payload": json.loads(row["payload_json"])} for row in rows]


def process_inbound_event(database_path: str, event_id: str) -> None:
    with connect(database_path) as con:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute(
            "SELECT payload_json FROM inbound_events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if row is None:
            con.commit()
            return

        try:
            payload = json.loads(row["payload_json"])
            event_type = payload.get("event_type")
            if event_type == "comment.created":
                _process_comment_created(con, event_id, payload)
            elif event_type == "comment.deleted":
                _process_comment_deleted(con, payload)

            now = iso_now()
            con.execute(
                """
                UPDATE inbound_events
                SET status = 'processed', processed_at = ?, error = NULL
                WHERE event_id = ?
                """,
                (now, event_id),
            )
            con.commit()
        except Exception as exc:
            con.execute(
                """
                UPDATE inbound_events
                SET status = 'queued', error = ?
                WHERE event_id = ?
                """,
                (str(exc), event_id),
            )
            con.commit()
            raise


def _process_comment_created(con: sqlite3.Connection, event_id: str, payload: dict[str, Any]) -> None:
    data = payload.get("data") or {}
    sender = data.get("from") or {}
    comment_id = data.get("comment_id")
    text = data.get("text") or ""
    user_id = sender.get("user_id")
    if not isinstance(comment_id, str) or not comment_id:
        raise ValueError("comment.created missing comment_id")
    if not isinstance(user_id, str) or not user_id:
        raise ValueError("comment.created missing from.user_id")

    now = iso_now()
    con.execute(
        """
        INSERT INTO comments(
            comment_id, post_id, text, user_id, username, created_at, deleted_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
        ON CONFLICT(comment_id) DO UPDATE SET
            post_id = excluded.post_id,
            text = excluded.text,
            user_id = excluded.user_id,
            username = excluded.username,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at
        """,
        (
            comment_id,
            data.get("post_id"),
            text,
            user_id,
            sender.get("username"),
            data.get("created_at"),
            now,
        ),
    )

    comment = con.execute(
        "SELECT deleted_at FROM comments WHERE comment_id = ?",
        (comment_id,),
    ).fetchone()
    if comment and comment["deleted_at"]:
        return

    lower_text = text.lower()
    rules = con.execute(
        "SELECT id, keyword_lower, dm_message FROM rules ORDER BY created_at"
    ).fetchall()
    for rule in rules:
        if rule["keyword_lower"] not in lower_text:
            continue
        _insert_delivery_or_duplicate(con, event_id, rule, user_id, comment_id)


def _insert_delivery_or_duplicate(
    con: sqlite3.Connection,
    event_id: str,
    rule: sqlite3.Row,
    user_id: str,
    comment_id: str,
) -> None:
    now = iso_now()
    try:
        con.execute(
            """
            INSERT INTO delivery_jobs(
                id, rule_id, recipient_user_id, comment_id, message, send_cycle,
                status, attempts, status_checks, next_attempt_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 1, 'queued', 0, 0, ?, ?, ?)
            """,
            (
                new_id("job"),
                rule["id"],
                user_id,
                comment_id,
                rule["dm_message"],
                now,
                now,
                now,
            ),
        )
    except sqlite3.IntegrityError:
        existing = con.execute(
            """
            SELECT comment_id
            FROM delivery_jobs
            WHERE rule_id = ? AND recipient_user_id = ?
            """,
            (rule["id"], user_id),
        ).fetchone()
        if existing and existing["comment_id"] == comment_id:
            return
        con.execute(
            """
            INSERT OR IGNORE INTO duplicate_blocks(
                id, event_id, rule_id, recipient_user_id, comment_id, reason, created_at
            )
            VALUES (?, ?, ?, ?, ?, 'rule_user_already_has_delivery', ?)
            """,
            (new_id("dup"), event_id, rule["id"], user_id, comment_id, now),
        )


def _process_comment_deleted(con: sqlite3.Connection, payload: dict[str, Any]) -> None:
    data = payload.get("data") or {}
    comment_id = data.get("comment_id")
    if not isinstance(comment_id, str) or not comment_id:
        raise ValueError("comment.deleted missing comment_id")

    now = iso_now()
    con.execute(
        """
        INSERT INTO comments(comment_id, deleted_at, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(comment_id) DO UPDATE SET
            deleted_at = excluded.deleted_at,
            updated_at = excluded.updated_at
        """,
        (comment_id, now, now),
    )
    con.execute(
        """
        UPDATE delivery_jobs
        SET status = 'cancelled',
            updated_at = ?,
            last_error = 'comment was deleted before send'
        WHERE comment_id = ?
          AND status IN ('queued', 'retry')
        """,
        (now, comment_id),
    )


def claim_due_delivery_job(database_path: str) -> dict[str, Any] | None:
    now = iso_now()
    with connect(database_path) as con:
        con.execute("BEGIN IMMEDIATE")
        rows = con.execute(
            """
            SELECT j.*, c.deleted_at
            FROM delivery_jobs j
            LEFT JOIN comments c ON c.comment_id = j.comment_id
            WHERE j.status IN ('queued', 'retry')
              AND j.next_attempt_at <= ?
            ORDER BY j.created_at
            LIMIT 25
            """,
            (now,),
        ).fetchall()

        for row in rows:
            if row["deleted_at"]:
                con.execute(
                    """
                    UPDATE delivery_jobs
                    SET status = 'cancelled',
                        updated_at = ?,
                        last_error = 'comment was deleted before send'
                    WHERE id = ?
                    """,
                    (now, row["id"]),
                )
                continue
            con.execute(
                """
                UPDATE delivery_jobs
                SET status = 'sending',
                    attempts = attempts + 1,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, row["id"]),
            )
            con.commit()
            claimed = dict(row)
            claimed["attempts"] = row["attempts"] + 1
            claimed["status"] = "sending"
            return claimed

        con.commit()
    return None


def idempotency_key(job: dict[str, Any]) -> str:
    return f"linkplease:{job['id']}:cycle:{job['send_cycle']}"


def mark_send_accepted(database_path: str, job_id: str, dm_id: str, status_delay_seconds: float) -> None:
    now = iso_now()
    with connect(database_path) as con:
        con.execute(
            """
            UPDATE delivery_jobs
            SET status = 'accepted',
                dm_id = ?,
                next_attempt_at = ?,
                last_error = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (dm_id, iso_after(status_delay_seconds), now, job_id),
        )


def reschedule_send(database_path: str, job_id: str, error: str, delay_seconds: float, max_attempts: int) -> None:
    now = iso_now()
    with connect(database_path) as con:
        row = con.execute(
            "SELECT attempts FROM delivery_jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        if row is None:
            return
        if row["attempts"] >= max_attempts:
            con.execute(
                """
                UPDATE delivery_jobs
                SET status = 'failed',
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (error, now, job_id),
            )
        else:
            con.execute(
                """
                UPDATE delivery_jobs
                SET status = 'retry',
                    next_attempt_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (iso_after(delay_seconds), error, now, job_id),
            )


def fail_delivery(database_path: str, job_id: str, error: str) -> None:
    now = iso_now()
    with connect(database_path) as con:
        con.execute(
            """
            UPDATE delivery_jobs
            SET status = 'failed',
                last_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (error, now, job_id),
        )


def claim_due_status_check(database_path: str, next_check_delay_seconds: float) -> dict[str, Any] | None:
    now = iso_now()
    with connect(database_path) as con:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute(
            """
            SELECT *
            FROM delivery_jobs
            WHERE status = 'accepted'
              AND dm_id IS NOT NULL
              AND next_attempt_at <= ?
            ORDER BY updated_at
            LIMIT 1
            """,
            (now,),
        ).fetchone()
        if row is None:
            con.commit()
            return None
        con.execute(
            """
            UPDATE delivery_jobs
            SET status_checks = status_checks + 1,
                next_attempt_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (iso_after(next_check_delay_seconds), now, row["id"]),
        )
        con.commit()
        claimed = dict(row)
        claimed["status_checks"] = row["status_checks"] + 1
        return claimed


def mark_status_delivered(database_path: str, job_id: str) -> None:
    now = iso_now()
    with connect(database_path) as con:
        con.execute(
            """
            UPDATE delivery_jobs
            SET status = 'delivered',
                last_error = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (now, job_id),
        )


def keep_waiting_for_delivery(
    database_path: str,
    job_id: str,
    delay_seconds: float,
    note: str | None = None,
) -> None:
    now = iso_now()
    with connect(database_path) as con:
        con.execute(
            """
            UPDATE delivery_jobs
            SET status = 'accepted',
                next_attempt_at = ?,
                last_error = COALESCE(?, last_error),
                updated_at = ?
            WHERE id = ?
            """,
            (iso_after(delay_seconds), note, now, job_id),
        )


def retry_after_delivery_failure(
    database_path: str,
    job_id: str,
    error: str,
    delay_seconds: float,
    max_attempts: int,
) -> None:
    now = iso_now()
    with connect(database_path) as con:
        row = con.execute(
            "SELECT attempts FROM delivery_jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        if row is None:
            return
        if row["attempts"] >= max_attempts:
            con.execute(
                """
                UPDATE delivery_jobs
                SET status = 'failed',
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (error, now, job_id),
            )
        else:
            con.execute(
                """
                UPDATE delivery_jobs
                SET status = 'retry',
                    dm_id = NULL,
                    send_cycle = send_cycle + 1,
                    next_attempt_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (iso_after(delay_seconds), error, now, job_id),
            )


def get_stats(database_path: str) -> dict[str, int]:
    with connect(database_path) as con:
        sent = con.execute(
            "SELECT COUNT(*) AS n FROM delivery_jobs WHERE status = 'delivered'"
        ).fetchone()["n"]
        failed = con.execute(
            "SELECT COUNT(*) AS n FROM delivery_jobs WHERE status = 'failed'"
        ).fetchone()["n"]
        queued = con.execute(
            """
            SELECT COUNT(*) AS n
            FROM delivery_jobs
            WHERE status NOT IN ('delivered', 'failed', 'cancelled')
            """
        ).fetchone()["n"]
        duplicates = con.execute(
            "SELECT COUNT(*) AS n FROM duplicate_blocks"
        ).fetchone()["n"]
    return {
        "sent": int(sent),
        "failed": int(failed),
        "queued": int(queued),
        "duplicates_blocked": int(duplicates),
    }

