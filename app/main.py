from fastapi import FastAPI, Request, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from typing import Optional

from .db import get_connection, init_db
from .seed import import_sops

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"

app = FastAPI(title="Mulligan Recovery Centers SOPs")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    conn = get_connection()
    cur = conn.cursor()
    count = cur.execute("SELECT COUNT(*) AS c FROM sops").fetchone()["c"]
    conn.close()
    if count == 0:
        import_sops()


@app.get("/")
def index(request: Request, q: Optional[str] = None, category: Optional[str] = None):
    conn = get_connection()
    cur = conn.cursor()

    query = "SELECT id, title, category, substr(content, 1, 220) AS snippet FROM sops WHERE 1=1"
    params: list[str] = []

    if q:
        query += " AND (lower(title) LIKE ? OR lower(content) LIKE ?)"
        q_like = f"%{q.lower()}%"
        params.extend([q_like, q_like])

    if category and category != "All":
        query += " AND category = ?"
        params.append(category)

    query += " ORDER BY title"

    sops = cur.execute(query, params).fetchall()
    categories = [row[0] for row in cur.execute("SELECT DISTINCT category FROM sops ORDER BY category").fetchall()]
    grouped = {}
    for row in sops:
        grouped.setdefault(row["category"], []).append(
            (row["id"], row["title"], row["category"], row["snippet"])
        )
    total = len(sops)
    conn.close()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "categories": categories,
            "grouped": grouped,
            "total": total,
            "q": q or "",
            "category": category or "All",
        },
    )


@app.get("/sop/{sop_id}")
def sop_detail(request: Request, sop_id: int):
    conn = get_connection()
    cur = conn.cursor()

    sop = cur.execute("SELECT * FROM sops WHERE id = ?", (sop_id,)).fetchone()
    if not sop:
        conn.close()
        return RedirectResponse(url="/", status_code=302)

    acknowledgments = cur.execute(
        """
        SELECT staff.name AS staff_name, acknowledgments.acknowledged_at
        FROM acknowledgments
        JOIN staff ON staff.id = acknowledgments.staff_id
        WHERE acknowledgments.sop_id = ?
        ORDER BY acknowledgments.acknowledged_at DESC
        """,
        (sop_id,),
    ).fetchall()

    conn.close()

    return templates.TemplateResponse(
        "sop_detail.html",
        {"request": request, "sop": sop, "acknowledgments": acknowledgments},
    )


@app.post("/sop/{sop_id}/ack")
def acknowledge(sop_id: int, staff_name: str = Form(...)):
    conn = get_connection()
    cur = conn.cursor()

    cleaned_name = staff_name.strip()
    normalized = cleaned_name.lower()

    cur.execute(
        "INSERT OR IGNORE INTO staff (name, normalized_name) VALUES (?, ?)",
        (cleaned_name, normalized),
    )
    staff_id = cur.execute(
        "SELECT id FROM staff WHERE normalized_name = ?",
        (normalized,),
    ).fetchone()[0]

    cur.execute(
        "INSERT OR IGNORE INTO acknowledgments (sop_id, staff_id) VALUES (?, ?)",
        (sop_id, staff_id),
    )
    conn.commit()
    conn.close()

    return RedirectResponse(url=f"/sop/{sop_id}", status_code=303)


@app.get("/admin")
def admin_list(request: Request):
    conn = get_connection()
    cur = conn.cursor()
    sops = cur.execute("SELECT id, title, category, updated_at FROM sops ORDER BY updated_at DESC").fetchall()
    conn.close()

    return templates.TemplateResponse("admin_list.html", {"request": request, "sops": sops})


@app.get("/admin/new")
def admin_new(request: Request):
    return templates.TemplateResponse("admin_new.html", {"request": request})


@app.post("/admin/new")
def admin_create(
    title: str = Form(...),
    category: str = Form(...),
    content: str = Form(...),
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sops (title, category, content) VALUES (?, ?, ?)",
        (title.strip(), category.strip(), content.strip()),
    )
    conn.commit()
    conn.close()

    return RedirectResponse(url="/admin", status_code=303)


@app.get("/admin/edit/{sop_id}")
def admin_edit(request: Request, sop_id: int):
    conn = get_connection()
    cur = conn.cursor()
    sop = cur.execute("SELECT * FROM sops WHERE id = ?", (sop_id,)).fetchone()
    conn.close()

    if not sop:
        return RedirectResponse(url="/admin", status_code=302)

    return templates.TemplateResponse("admin_edit.html", {"request": request, "sop": sop})


@app.post("/admin/edit/{sop_id}")
def admin_update(
    sop_id: int,
    title: str = Form(...),
    category: str = Form(...),
    content: str = Form(...),
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE sops
        SET title = ?, category = ?, content = ?, updated_at = datetime('now')
        WHERE id = ?
        """,
        (title.strip(), category.strip(), content.strip(), sop_id),
    )
    conn.commit()
    conn.close()

    return RedirectResponse(url="/admin", status_code=303)


@app.get("/admin/staff")
def admin_staff(request: Request):
    conn = get_connection()
    cur = conn.cursor()
    staff = cur.execute(
        "SELECT id, name, active, created_at FROM staff ORDER BY active DESC, name"
    ).fetchall()
    conn.close()
    return templates.TemplateResponse("admin_staff.html", {"request": request, "staff": staff})


@app.post("/admin/staff/new")
def admin_staff_new(name: str = Form(...)):
    conn = get_connection()
    cur = conn.cursor()
    cleaned = name.strip()
    normalized = cleaned.lower()
    cur.execute(
        "INSERT OR IGNORE INTO staff (name, normalized_name) VALUES (?, ?)",
        (cleaned, normalized),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/admin/staff", status_code=303)


@app.post("/admin/staff/{staff_id}/toggle")
def admin_staff_toggle(staff_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE staff SET active = CASE WHEN active = 1 THEN 0 ELSE 1 END WHERE id = ?", (staff_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/admin/staff", status_code=303)


@app.get("/compliance")
def compliance_dashboard(
    request: Request,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    conn = get_connection()
    cur = conn.cursor()

    staff = cur.execute("SELECT id, name FROM staff WHERE active = 1 ORDER BY name").fetchall()
    sops = cur.execute("SELECT id, title, category FROM sops ORDER BY title").fetchall()
    staff_count = len(staff)
    sop_count = len(sops)

    date_clause = ""
    date_params: list[str] = []
    if start_date:
        date_clause += " AND date(acknowledged_at) >= date(?)"
        date_params.append(start_date)
    if end_date:
        date_clause += " AND date(acknowledged_at) <= date(?)"
        date_params.append(end_date)

    ack_by_sop = {
        row["sop_id"]: row["ack_count"]
        for row in cur.execute(
            f"""
            SELECT acknowledgments.sop_id AS sop_id, COUNT(DISTINCT acknowledgments.staff_id) AS ack_count
            FROM acknowledgments
            JOIN staff ON staff.id = acknowledgments.staff_id AND staff.active = 1
            WHERE 1=1 {date_clause}
            GROUP BY acknowledgments.sop_id
            """,
            date_params,
        ).fetchall()
    }

    ack_by_category = {
        row["category"]: row["ack_count"]
        for row in cur.execute(
            f"""
            SELECT sops.category AS category, COUNT(DISTINCT acknowledgments.sop_id || '-' || acknowledgments.staff_id) AS ack_count
            FROM acknowledgments
            JOIN staff ON staff.id = acknowledgments.staff_id AND staff.active = 1
            JOIN sops ON sops.id = acknowledgments.sop_id
            WHERE 1=1 {date_clause}
            GROUP BY sops.category
            """,
            date_params,
        ).fetchall()
    }

    sop_counts_by_category = {
        row["category"]: row["sop_count"]
        for row in cur.execute(
            "SELECT category, COUNT(*) AS sop_count FROM sops GROUP BY category"
        ).fetchall()
    }

    staff_completion = []
    for person in staff:
        acked = cur.execute(
            f"""
            SELECT COUNT(DISTINCT acknowledgments.sop_id) AS acked
            FROM acknowledgments
            WHERE acknowledgments.staff_id = ? {date_clause}
            """,
            [person["id"], *date_params],
        ).fetchone()["acked"]
        completion = (acked / sop_count * 100) if sop_count else 0
        staff_completion.append(
            {"name": person["name"], "acked": acked, "total": sop_count, "percent": completion}
        )

    recent_acks = cur.execute(
        f"""
        SELECT staff.name AS staff_name, sops.title AS sop_title, acknowledgments.acknowledged_at
        FROM acknowledgments
        JOIN staff ON staff.id = acknowledgments.staff_id
        JOIN sops ON sops.id = acknowledgments.sop_id
        WHERE 1=1 {date_clause}
        ORDER BY acknowledgments.acknowledged_at DESC
        LIMIT 25
        """,
        date_params,
    ).fetchall()

    overdue = []
    if staff_count and sop_count:
        for sop in sops:
            missing_staff = cur.execute(
                f"""
                SELECT staff.name
                FROM staff
                WHERE staff.active = 1 AND staff.id NOT IN (
                    SELECT acknowledgments.staff_id FROM acknowledgments
                    WHERE acknowledgments.sop_id = ? {date_clause}
                )
                ORDER BY staff.name
                """,
                [sop["id"], *date_params],
            ).fetchall()
            if missing_staff:
                overdue.append(
                    {
                        "sop_title": sop["title"],
                        "missing": [row[0] for row in missing_staff],
                    }
                )

    total_required = staff_count * sop_count
    total_ack_pairs = sum(ack_by_sop.values())
    overall_percent = (total_ack_pairs / total_required * 100) if total_required else 0

    conn.close()

    return templates.TemplateResponse(
        "compliance.html",
        {
            "request": request,
            "staff_count": staff_count,
            "sop_count": sop_count,
            "overall_percent": overall_percent,
            "sops": sops,
            "ack_by_sop": ack_by_sop,
            "ack_by_category": ack_by_category,
            "sop_counts_by_category": sop_counts_by_category,
            "staff_completion": staff_completion,
            "recent_acks": recent_acks,
            "overdue": overdue,
            "start_date": start_date or "",
            "end_date": end_date or "",
        },
    )
