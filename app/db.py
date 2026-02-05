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


def _column_names(cur: sqlite3.Cursor, table: str) -> set[str]:
    return {row[1] for row in cur.execute(f"PRAGMA table_info({table})").fetchall()}


def _index_names(cur: sqlite3.Cursor, table: str) -> set[str]:
    return {row[1] for row in cur.execute(f"PRAGMA index_list({table})").fetchall()}


def init_db() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'staff',
            staff_id INTEGER,
            must_reset_password INTEGER NOT NULL DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (staff_id) REFERENCES staff(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS staff (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            normalized_name TEXT NOT NULL UNIQUE,
            staff_type TEXT,
            role TEXT,
            department TEXT,
            supervisor TEXT,
            hire_date TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS onboarding_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS onboarding_steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER NOT NULL,
            step_text TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (template_id) REFERENCES onboarding_templates(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS onboarding_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            staff_id INTEGER NOT NULL,
            template_id INTEGER NOT NULL,
            assigned_at TEXT NOT NULL DEFAULT (datetime('now')),
            due_date TEXT,
            FOREIGN KEY (staff_id) REFERENCES staff(id),
            FOREIGN KEY (template_id) REFERENCES onboarding_templates(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS onboarding_progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assignment_id INTEGER NOT NULL,
            step_id INTEGER NOT NULL,
            completed INTEGER NOT NULL DEFAULT 0,
            completed_at TEXT,
            FOREIGN KEY (assignment_id) REFERENCES onboarding_assignments(id),
            FOREIGN KEY (step_id) REFERENCES onboarding_steps(id)
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
            current_version INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_reviewed TEXT,
            source_file TEXT,
            content_clean TEXT
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sop_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sop_id INTEGER NOT NULL,
            version INTEGER NOT NULL,
            title TEXT NOT NULL,
            category TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            created_by INTEGER,
            FOREIGN KEY (sop_id) REFERENCES sops(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS acknowledgments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sop_id INTEGER NOT NULL,
            staff_id INTEGER NOT NULL,
            sop_version INTEGER NOT NULL DEFAULT 1,
            staff_name TEXT,
            signature_text TEXT,
            read_seconds INTEGER DEFAULT 0,
            acknowledged_at TEXT NOT NULL DEFAULT (datetime('now')),
            ip_address TEXT,
            user_agent TEXT,
            FOREIGN KEY (sop_id) REFERENCES sops(id),
            FOREIGN KEY (staff_id) REFERENCES staff(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_user_id INTEGER,
            action TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id INTEGER,
            details TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (actor_user_id) REFERENCES users(id)
        );
        """
    )

    # Learning Management
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS training_modules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sop_id INTEGER NOT NULL UNIQUE,
            title TEXT NOT NULL,
            description TEXT,
            passing_score INTEGER NOT NULL DEFAULT 80,
            recert_days INTEGER NOT NULL DEFAULT 365,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (sop_id) REFERENCES sops(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS training_questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            module_id INTEGER NOT NULL,
            question_text TEXT NOT NULL,
            option_a TEXT NOT NULL,
            option_b TEXT NOT NULL,
            option_c TEXT NOT NULL,
            option_d TEXT NOT NULL,
            correct_option TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (module_id) REFERENCES training_modules(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS training_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            module_id INTEGER NOT NULL,
            staff_id INTEGER NOT NULL,
            score INTEGER NOT NULL,
            passed INTEGER NOT NULL DEFAULT 0,
            attempted_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (module_id) REFERENCES training_modules(id),
            FOREIGN KEY (staff_id) REFERENCES staff(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS training_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            module_id INTEGER NOT NULL,
            staff_id INTEGER NOT NULL,
            assigned_at TEXT NOT NULL DEFAULT (datetime('now')),
            due_date TEXT,
            FOREIGN KEY (module_id) REFERENCES training_modules(id),
            FOREIGN KEY (staff_id) REFERENCES staff(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS training_assignment_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            staff_type TEXT NOT NULL UNIQUE,
            include_keywords TEXT,
            exclude_keywords TEXT
        );
        """
    )

    # Migration: users table additions
    user_columns = _column_names(cur, "users")
    if "must_reset_password" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN must_reset_password INTEGER NOT NULL DEFAULT 0")

    # Migration: staff profile fields
    staff_columns = _column_names(cur, "staff")
    for col in ["staff_type", "role", "department", "supervisor", "hire_date"]:
        if col not in staff_columns:
            cur.execute(f"ALTER TABLE staff ADD COLUMN {col} TEXT")

    # Seed training assignment rules if missing
    existing_rules = cur.execute("SELECT COUNT(*) AS c FROM training_assignment_rules").fetchone()
    if existing_rules and existing_rules["c"] == 0:
        cur.execute(
            "INSERT INTO training_assignment_rules (staff_type, include_keywords, exclude_keywords) VALUES (?, ?, ?)",
            ("clinical", "", ""),
        )
        cur.execute(
            "INSERT INTO training_assignment_rules (staff_type, include_keywords, exclude_keywords) VALUES (?, ?, ?)",
            ("non_clinical", "", "medication,treatment,clinical,admissions,assessment"),
        )

    # Migration: remove old unique constraint on acknowledgments (sop_id, staff_id)
    if "ack_unique" in _index_names(cur, "acknowledgments"):
        cur.execute("DROP INDEX IF EXISTS ack_unique")

    # Migration: older acknowledgments table might include staff_name but not staff_id
    ack_columns = _column_names(cur, "acknowledgments")
    if "staff_id" not in ack_columns:
        cur.execute("ALTER TABLE acknowledgments ADD COLUMN staff_id INTEGER")
    if "sop_version" not in ack_columns:
        cur.execute("ALTER TABLE acknowledgments ADD COLUMN sop_version INTEGER DEFAULT 1")
    if "signature_text" not in ack_columns:
        cur.execute("ALTER TABLE acknowledgments ADD COLUMN signature_text TEXT")
    if "read_seconds" not in ack_columns:
        cur.execute("ALTER TABLE acknowledgments ADD COLUMN read_seconds INTEGER DEFAULT 0")
    if "ip_address" not in ack_columns:
        cur.execute("ALTER TABLE acknowledgments ADD COLUMN ip_address TEXT")
    if "user_agent" not in ack_columns:
        cur.execute("ALTER TABLE acknowledgments ADD COLUMN user_agent TEXT")
    if "staff_name" not in ack_columns:
        cur.execute("ALTER TABLE acknowledgments ADD COLUMN staff_name TEXT")

    # Migration: older sops table may be missing versioning columns
    sop_columns = _column_names(cur, "sops")
    if "current_version" not in sop_columns:
        cur.execute("ALTER TABLE sops ADD COLUMN current_version INTEGER NOT NULL DEFAULT 1")
    if "last_reviewed" not in sop_columns:
        cur.execute("ALTER TABLE sops ADD COLUMN last_reviewed TEXT")
    if "source_file" not in sop_columns:
        cur.execute("ALTER TABLE sops ADD COLUMN source_file TEXT")
    if "content_clean" not in sop_columns:
        cur.execute("ALTER TABLE sops ADD COLUMN content_clean TEXT")

    # Migrate any legacy acknowledgments with staff_name to staff table
    rows = cur.execute(
        "SELECT DISTINCT staff_name FROM acknowledgments WHERE staff_name IS NOT NULL AND staff_name != ''"
    ).fetchall()
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

    conn.commit()
    conn.close()
