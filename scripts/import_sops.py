from pathlib import Path
import re
import sys

from docx import Document

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from app.db import get_connection, init_db

SOURCE_DIR = Path("/Users/maxbauscher/Desktop/MRC CT Policies & Procedures")

CATEGORY_MAP = {
    "Client Care (CC)": "Client Care",
    "Documentation (DO)": "Documentation",
    "Operations (OP)": "Operations",
    "Safety (SF)": "Safety",
}

CATEGORY_RULES = [
    ("Admissions", ["admission", "intake", "screening", "referral", "assessment", "eligibility"]),
    ("Medication", ["medication", "prescription", "controlled", "narcan", "drug destruction", "med log"]),
    ("Incident Reporting", ["incident", "injury", "report", "event"]),
    ("Safety", ["fire", "evacuation", "drill", "emergency", "crisis", "overdose", "contraband", "search"]),
    ("Compliance", ["roi", "release of information", "42 cfr", "part 2", "confidential", "hipaa"]),
    ("Staffing", ["staffing", "training", "orientation", "competency", "background check"]),
]


def docx_to_text(path: Path) -> str:
    doc = Document(str(path))
    parts = []
    for p in doc.paragraphs:
        t = (p.text or "").strip()
        if t:
            parts.append(t)
    return "\n\n".join(parts).strip()


def clean_title(filename: str) -> str:
    title = filename.replace(".docx", "").replace("_", " ").strip()
    title = re.sub(r"\s+", " ", title)
    title = re.sub(r"^(OP|DO|CC|SF)\.MRC\.CT\s+", "", title)
    title = re.sub(r"^[A-Z]{2}\.\d+\.MRC\.?\s*[-–—]?\s*", "", title)
    title = title.replace("MRCCT ", "").replace("MRC.CT ", "").strip()
    title = re.sub(r"\bP\s*&\s*P\b", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\bPolicy\s*&\s*Procedure(s)?\b", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\bPolicy\b", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\bProcedure\b", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\bSOP\b", "", title, flags=re.IGNORECASE)
    title = re.sub(r"[-–—]+", " ", title)
    title = re.sub(r"^\s*-\s*", "", title)
    title = re.sub(r"\s{2,}", " ", title).strip()
    return title


def infer_category(title: str, content: str) -> str:
    hay = f"{title}\n{content}".lower()
    for cat, keywords in CATEGORY_RULES:
        if any(k in hay for k in keywords):
            return cat
    return "Operations"


def extract_last_reviewed(content: str) -> str:
    m = re.search(r"last\s+reviewed:\s*([0-1]?\d/\d{4})", content, re.IGNORECASE)
    return m.group(1) if m else ""


def strip_admin_header_lines(text: str) -> str:
    t = re.sub(r"Last\s+Reviewed:\s*[^\n]*", "", text, flags=re.IGNORECASE)
    t = re.sub(r"Additional\s+Authority:\s*[^\n]*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"CARF\s+Reference:\s*[^\n]*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"Regulation:\s*[^\n]*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"Attachment\(s\):\s*[^\n]*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\n{2,}", "\n", t)
    return t.strip()


def ensure_columns(conn):
    cur = conn.cursor()
    cols = [r[1] for r in cur.execute("PRAGMA table_info(sops)").fetchall()]

    if "last_reviewed" not in cols:
        cur.execute("ALTER TABLE sops ADD COLUMN last_reviewed TEXT")
    if "source_file" not in cols:
        cur.execute("ALTER TABLE sops ADD COLUMN source_file TEXT")
    if "content_clean" not in cols:
        cur.execute("ALTER TABLE sops ADD COLUMN content_clean TEXT")

    conn.commit()


def import_sops():
    init_db()
    conn = get_connection()
    ensure_columns(conn)
    cur = conn.cursor()

    if not SOURCE_DIR.exists():
        raise RuntimeError(f"Policies folder not found: {SOURCE_DIR}")

    count = 0
    for doc_path in sorted(SOURCE_DIR.rglob("*.docx")):
        content = docx_to_text(doc_path)
        if not content:
            continue
        content_clean = strip_admin_header_lines(content)
        title = clean_title(doc_path.name)

        folder_category = CATEGORY_MAP.get(doc_path.parent.name)
        category = folder_category or infer_category(title, content)
        last_reviewed = extract_last_reviewed(content)

        existing = cur.execute("SELECT id FROM sops WHERE title = ?", (title,)).fetchone()
        if existing:
            cur.execute(
                """
                UPDATE sops
                SET category = ?, content = ?, content_clean = ?, updated_at = datetime('now'),
                    last_reviewed = ?, source_file = ?
                WHERE title = ?
                """,
                (category, content, content_clean, last_reviewed, str(doc_path), title),
            )
        else:
            cur.execute(
                """
                INSERT INTO sops (title, category, content, content_clean, last_reviewed, source_file)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (title, category, content, content_clean, last_reviewed, str(doc_path)),
            )

        count += 1

    conn.commit()

    # Normalize stored titles to cleaned versions
    rows = cur.execute("SELECT id, title FROM sops").fetchall()
    for row in rows:
        cleaned = clean_title(row["title"])
        if cleaned != row["title"]:
            cur.execute("UPDATE sops SET title = ?, updated_at = datetime('now') WHERE id = ?", (cleaned, row["id"]))

    # Deduplicate by cleaned title, merging acknowledgments to the kept SOP
    rows = cur.execute("SELECT id, title FROM sops").fetchall()
    groups = {}
    for row in rows:
        normalized = clean_title(row["title"])
        groups.setdefault(normalized, []).append(row["id"])

    for ids in groups.values():
        if len(ids) <= 1:
            continue
        keep_id = min(ids)
        drop_ids = [i for i in ids if i != keep_id]
        cur.execute(
            f"UPDATE acknowledgments SET sop_id = ? WHERE sop_id IN ({','.join('?' for _ in drop_ids)})",
            [keep_id, *drop_ids],
        )
        cur.execute(
            f"DELETE FROM sops WHERE id IN ({','.join('?' for _ in drop_ids)})",
            drop_ids,
        )

    conn.commit()
    conn.close()
    print(f"Imported/updated {count} SOPs from {SOURCE_DIR}")


if __name__ == "__main__":
    import_sops()
