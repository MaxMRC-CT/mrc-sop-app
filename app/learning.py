from datetime import date, timedelta, datetime
from typing import Optional

from .db import get_connection


def seed_modules(passing_score: int = 80, recert_days: int = 365) -> int:
    conn = get_connection()
    cur = conn.cursor()
    sops = cur.execute("SELECT id, title FROM sops ORDER BY id").fetchall()
    count = 0
    for sop in sops:
        exists = cur.execute("SELECT id FROM training_modules WHERE sop_id = ?", (sop["id"],)).fetchone()
        if exists:
            continue
        cur.execute(
            """
            INSERT INTO training_modules (sop_id, title, passing_score, recert_days)
            VALUES (?, ?, ?, ?)
            """,
            (sop["id"], sop["title"], passing_score, recert_days),
        )
        count += 1
    conn.commit()
    conn.close()
    return count


def module_status(module_id: int, staff_id: int) -> tuple[str, Optional[str], Optional[int], Optional[str]]:
    conn = get_connection()
    cur = conn.cursor()
    module = cur.execute("SELECT recert_days FROM training_modules WHERE id = ?", (module_id,)).fetchone()
    if not module:
        conn.close()
        return "not_started", None, None, None

    last = cur.execute(
        """
        SELECT score, passed, attempted_at FROM training_attempts
        WHERE module_id = ? AND staff_id = ?
        ORDER BY attempted_at DESC
        LIMIT 1
        """,
        (module_id, staff_id),
    ).fetchone()

    last_pass = cur.execute(
        """
        SELECT attempted_at FROM training_attempts
        WHERE module_id = ? AND staff_id = ? AND passed = 1
        ORDER BY attempted_at DESC
        LIMIT 1
        """,
        (module_id, staff_id),
    ).fetchone()
    conn.close()

    if not last:
        return "not_started", None, None, None

    last_date = date.fromisoformat(last["attempted_at"].split(" ")[0])
    due_date = last_date + timedelta(days=module["recert_days"])
    if last["passed"] == 1 and date.today() <= due_date:
        return "passed", last["attempted_at"], last["score"], last_pass["attempted_at"] if last_pass else None
    return "due", last["attempted_at"], last["score"], last_pass["attempted_at"] if last_pass else None


def is_locked_out(module_id: int, staff_id: int, max_attempts: int, lockout_hours: int) -> tuple[bool, int]:
    conn = get_connection()
    cur = conn.cursor()
    window_start = (datetime.utcnow() - timedelta(hours=lockout_hours)).strftime("%Y-%m-%d %H:%M:%S")

    attempts = cur.execute(
        """
        SELECT passed, attempted_at FROM training_attempts
        WHERE module_id = ? AND staff_id = ? AND attempted_at >= ?
        ORDER BY attempted_at DESC
        """,
        (module_id, staff_id, window_start),
    ).fetchall()
    conn.close()

    if not attempts:
        return False, 0

    # If there is a pass within the window, no lockout
    if any(a["passed"] == 1 for a in attempts):
        return False, 0

    failures = sum(1 for a in attempts if a["passed"] == 0)
    return failures >= max_attempts, failures
