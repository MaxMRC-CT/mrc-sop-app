import os
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
INSTANCE_DIR = BASE_DIR / "instance"
DB_PATH = INSTANCE_DIR / "sops.db"


def get_connection() -> sqlite3.Connection:
    INSTANCE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS staff (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            normalized_name TEXT NOT NULL UNIQUE,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            category TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS acknowledgments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sop_id INTEGER NOT NULL,
            staff_id INTEGER NOT NULL,
            acknowledged_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (sop_id) REFERENCES sops(id),
            FOREIGN KEY (staff_id) REFERENCES staff(id),
            UNIQUE (sop_id, staff_id)
        );
        """
    )

    # Simple migration support for older databases
    columns = {row[1] for row in cur.execute("PRAGMA table_info(acknowledgments)").fetchall()}
    if "staff_id" not in columns:
        cur.execute("ALTER TABLE acknowledgments ADD COLUMN staff_id INTEGER")
    if "staff_name" in columns:
        # best-effort migration: create staff rows from existing names
        rows = cur.execute("SELECT DISTINCT staff_name FROM acknowledgments WHERE staff_name IS NOT NULL").fetchall()
        for row in rows:
            name = row[0].strip()
            if not name:
                continue
            normalized = name.lower()
            cur.execute(
                "INSERT OR IGNORE INTO staff (name, normalized_name) VALUES (?, ?)",
                (name, normalized),
            )
            staff_id = cur.execute(
                "SELECT id FROM staff WHERE normalized_name = ?",
                (normalized,),
            ).fetchone()[0]
            cur.execute(
                "UPDATE acknowledgments SET staff_id = ? WHERE staff_name = ?",
                (staff_id, name),
            )
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ack_unique ON acknowledgments(sop_id, staff_id)")

    conn.commit()
    conn.close()
