from datetime import date, datetime, timedelta
from threading import Thread
import csv
import io
import os
import re
from typing import Optional

from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import RedirectResponse, HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from pathlib import Path

from .db import get_connection, init_db
from .seed import import_sops
from .auth import authenticate, ensure_default_admin, get_current_user, require_admin, require_login, create_password_hash, verify_password

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"

REACK_DAYS = int(os.getenv("MRC_REACK_DAYS", "365"))
MIN_READ_SECONDS = int(os.getenv("MRC_MIN_READ_SECONDS", "10"))

app = FastAPI(title="Mulligan Recovery Centers SOPs")
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("MRC_SESSION_SECRET", "change-me-please"),
    same_site="lax",
)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def _highlight(text: str, query: str) -> str:
    if not query:
        return text
    pattern = re.compile(re.escape(query), re.IGNORECASE)
    return pattern.sub(lambda m: f"<mark class='bg-orange-400/40 text-white px-1 rounded'>{m.group(0)}</mark>", text)


templates.env.filters["highlight"] = _highlight


def _redirect_login() -> RedirectResponse:
    return RedirectResponse(url="/login", status_code=302)


def _log_action(actor_id: Optional[int], action: str, entity_type: str, entity_id: Optional[int], details: Optional[str] = None) -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO audit_log (actor_user_id, action, entity_type, entity_id, details) VALUES (?, ?, ?, ?, ?)",
        (actor_id, action, entity_type, entity_id, details),
    )
    conn.commit()
    conn.close()


def _client_info(request: Request) -> tuple[Optional[str], Optional[str]]:
    ip = request.client.host if request.client else None
    agent = request.headers.get("user-agent")
    return ip, agent


def _template(name: str, request: Request, context: dict) -> HTMLResponse:
    context["current_user"] = get_current_user(request)
    return templates.TemplateResponse(name, context)


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    ensure_default_admin()
    conn = get_connection()
    cur = conn.cursor()
    count = cur.execute("SELECT COUNT(*) AS c FROM sops").fetchone()["c"]
    conn.close()
    if count == 0:
        Thread(target=import_sops, daemon=True).start()


@app.get("/login")
def login_page(request: Request):
    return _template("login.html", request, {"request": request, "error": None})


@app.post("/login")
def login_action(request: Request, username: str = Form(...), password: str = Form(...)):
    user = authenticate(username.strip(), password)
    if not user:
        return _template("login.html", request, {"request": request, "error": "Invalid credentials"})
    request.session["user_id"] = user.id
    if user.must_reset_password == 1:
        return RedirectResponse(url="/account?force=1", status_code=303)
    return RedirectResponse(url="/", status_code=303)


@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@app.get("/account")
def account_page(request: Request):
    user = get_current_user(request)
    if not user:
        return _redirect_login()
    force = request.query_params.get("force") == "1" or user.must_reset_password == 1
    return _template("account.html", request, {"request": request, "error": None, "message": None, "force": force})


@app.post("/account")
def account_update(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
):
    user = get_current_user(request)
    if not user:
        return _redirect_login()

    if new_password != confirm_password:
        return _template("account.html", request, {"request": request, "error": "Passwords do not match", "message": None})
    if len(new_password) < 8:
        return _template("account.html", request, {"request": request, "error": "Password must be at least 8 characters", "message": None})

    conn = get_connection()
    cur = conn.cursor()
    row = cur.execute("SELECT password_hash FROM users WHERE id = ?", (user.id,)).fetchone()
    if not row or not verify_password(current_password, row["password_hash"]):
        conn.close()
        return _template("account.html", request, {"request": request, "error": "Current password is incorrect", "message": None})

    cur.execute(
        "UPDATE users SET password_hash = ?, must_reset_password = 0 WHERE id = ?",
        (create_password_hash(new_password), user.id),
    )
    conn.commit()
    conn.close()

    _log_action(user.id, "password_change", "user", user.id, "self-service")

    return _template("account.html", request, {"request": request, "error": None, "message": "Password updated.", "force": False})


@app.get("/")
def index(request: Request, q: Optional[str] = None, category: Optional[str] = None):
    if not get_current_user(request):
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()

    query = "SELECT id, title, category FROM sops WHERE 1=1"
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
    conn.close()

    grouped = {}
    for row in sops:
        grouped.setdefault(row["category"], []).append((row["id"], row["title"], row["category"]))
    total = len(sops)

    return _template(
        "index.html",
        request,
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
    user = get_current_user(request)
    if not user:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()

    sop = cur.execute("SELECT * FROM sops WHERE id = ?", (sop_id,)).fetchone()
    if not sop:
        conn.close()
        return RedirectResponse(url="/", status_code=302)

    acknowledgments = cur.execute(
        """
        SELECT staff.name AS staff_name, acknowledgments.acknowledged_at, acknowledgments.sop_version,
               acknowledgments.read_seconds
        FROM acknowledgments
        JOIN staff ON staff.id = acknowledgments.staff_id
        WHERE acknowledgments.sop_id = ?
        ORDER BY acknowledgments.acknowledged_at DESC
        """,
        (sop_id,),
    ).fetchall()

    conn.close()

    return _template(
        "sop_detail.html",
        request,
        {
            "request": request,
            "sop": sop,
            "acknowledgments": acknowledgments,
            "min_read_seconds": MIN_READ_SECONDS,
        },
    )


@app.get("/sop/{sop_id}/print")
def sop_print(request: Request, sop_id: int):
    user = get_current_user(request)
    if not user:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    sop = cur.execute("SELECT * FROM sops WHERE id = ?", (sop_id,)).fetchone()
    conn.close()

    if not sop:
        return RedirectResponse(url="/", status_code=302)

    return _template("sop_print.html", request, {"request": request, "sop": sop})


@app.post("/sop/{sop_id}/ack")
def acknowledge(
    request: Request,
    sop_id: int,
    signature_text: str = Form(...),
    read_seconds: int = Form(0),
):
    user = get_current_user(request)
    if not user:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()

    if not user.staff_id:
        conn.close()
        return RedirectResponse(url=f"/sop/{sop_id}", status_code=303)

    staff = cur.execute("SELECT name FROM staff WHERE id = ?", (user.staff_id,)).fetchone()
    sop = cur.execute("SELECT current_version FROM sops WHERE id = ?", (sop_id,)).fetchone()
    if not staff or not sop:
        conn.close()
        return RedirectResponse(url=f"/sop/{sop_id}", status_code=303)

    ip, agent = _client_info(request)

    if int(read_seconds) < MIN_READ_SECONDS:
        conn.close()
        return RedirectResponse(url=f"/sop/{sop_id}", status_code=303)

    cur.execute(
        """
        INSERT INTO acknowledgments (sop_id, staff_id, sop_version, staff_name, signature_text, read_seconds, ip_address, user_agent)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (sop_id, user.staff_id, sop["current_version"], staff["name"], signature_text.strip(), int(read_seconds), ip, agent),
    )
    conn.commit()
    conn.close()

    _log_action(user.id, "acknowledged", "sop", sop_id, f"signature={signature_text.strip()}")

    return RedirectResponse(url=f"/sop/{sop_id}", status_code=303)


@app.get("/admin")
def admin_list(request: Request):
    try:
        user = require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    sops = cur.execute("SELECT id, title, category, updated_at, current_version FROM sops ORDER BY updated_at DESC").fetchall()
    conn.close()

    return _template("admin_list.html", request, {"request": request, "sops": sops})


@app.get("/admin/new")
def admin_new(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return _redirect_login()
    return _template("admin_new.html", request, {"request": request})


@app.post("/admin/new")
def admin_create(
    request: Request,
    title: str = Form(...),
    category: str = Form(...),
    content: str = Form(...),
):
    try:
        user = require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sops (title, category, content) VALUES (?, ?, ?)",
        (title.strip(), category.strip(), content.strip()),
    )
    sop_id = cur.lastrowid
    cur.execute(
        "INSERT INTO sop_versions (sop_id, version, title, category, content, created_by) VALUES (?, ?, ?, ?, ?, ?)",
        (sop_id, 1, title.strip(), category.strip(), content.strip(), user.id),
    )
    conn.commit()
    conn.close()

    _log_action(user.id, "create", "sop", sop_id, title.strip())

    return RedirectResponse(url="/admin", status_code=303)


@app.get("/admin/edit/{sop_id}")
def admin_edit(request: Request, sop_id: int):
    try:
        require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    sop = cur.execute("SELECT * FROM sops WHERE id = ?", (sop_id,)).fetchone()
    conn.close()

    if not sop:
        return RedirectResponse(url="/admin", status_code=302)

    return _template("admin_edit.html", request, {"request": request, "sop": sop})


@app.post("/admin/edit/{sop_id}")
def admin_update(
    request: Request,
    sop_id: int,
    title: str = Form(...),
    category: str = Form(...),
    content: str = Form(...),
):
    try:
        user = require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    existing = cur.execute("SELECT * FROM sops WHERE id = ?", (sop_id,)).fetchone()
    if not existing:
        conn.close()
        return RedirectResponse(url="/admin", status_code=302)

    new_version = existing["current_version"] + 1
    cur.execute(
        """
        UPDATE sops
        SET title = ?, category = ?, content = ?, updated_at = datetime('now'), current_version = ?
        WHERE id = ?
        """,
        (title.strip(), category.strip(), content.strip(), new_version, sop_id),
    )
    cur.execute(
        "INSERT INTO sop_versions (sop_id, version, title, category, content, created_by) VALUES (?, ?, ?, ?, ?, ?)",
        (sop_id, new_version, title.strip(), category.strip(), content.strip(), user.id),
    )
    conn.commit()
    conn.close()

    _log_action(user.id, "update", "sop", sop_id, f"version={new_version}")

    return RedirectResponse(url="/admin", status_code=303)


@app.get("/admin/staff")
def admin_staff(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    staff = cur.execute(
        "SELECT id, name, active, created_at FROM staff ORDER BY active DESC, name"
    ).fetchall()
    conn.close()
    return _template("admin_staff.html", request, {"request": request, "staff": staff})


@app.post("/admin/staff/new")
def admin_staff_new(request: Request, name: str = Form(...)):
    try:
        user = require_admin(request)
    except PermissionError:
        return _redirect_login()

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

    _log_action(user.id, "create", "staff", None, cleaned)

    return RedirectResponse(url="/admin/staff", status_code=303)


@app.post("/admin/staff/import")
def admin_staff_import(request: Request, file: UploadFile = File(...)):
    try:
        user = require_admin(request)
    except PermissionError:
        return _redirect_login()

    content = file.file.read().decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(content))
    conn = get_connection()
    cur = conn.cursor()
    count = 0
    for row in reader:
        name = (row.get("name") or row.get("Name") or "").strip()
        if not name:
            continue
        normalized = name.lower()
        cur.execute(
            "INSERT OR IGNORE INTO staff (name, normalized_name) VALUES (?, ?)",
            (name, normalized),
        )
        count += 1
    conn.commit()
    conn.close()

    _log_action(user.id, "import", "staff", None, f"count={count}")

    return RedirectResponse(url="/admin/staff", status_code=303)


@app.post("/admin/staff/{staff_id}/toggle")
def admin_staff_toggle(request: Request, staff_id: int):
    try:
        user = require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE staff SET active = CASE WHEN active = 1 THEN 0 ELSE 1 END WHERE id = ?", (staff_id,))
    conn.commit()
    conn.close()

    _log_action(user.id, "toggle", "staff", staff_id, "active")

    return RedirectResponse(url="/admin/staff", status_code=303)


@app.get("/admin/users")
def admin_users(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    users = cur.execute(
        "SELECT users.id, users.username, users.role, users.active, staff.name AS staff_name "
        "FROM users LEFT JOIN staff ON staff.id = users.staff_id ORDER BY users.username"
    ).fetchall()
    staff = cur.execute("SELECT id, name FROM staff ORDER BY name").fetchall()
    conn.close()

    return _template("admin_users.html", request, {"request": request, "users": users, "staff": staff})


@app.post("/admin/users/new")
def admin_users_new(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    role: str = Form(...),
    staff_id: Optional[int] = Form(None),
    must_reset_password: Optional[str] = Form(None),
):
    try:
        user = require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    linked_staff_id = staff_id
    if role == "staff" and not linked_staff_id:
        name = username.strip()
        cur.execute(
            "INSERT OR IGNORE INTO staff (name, normalized_name) VALUES (?, ?)",
            (name, name.lower()),
        )
        linked_staff_id = cur.execute(
            "SELECT id FROM staff WHERE normalized_name = ?",
            (name.lower(),),
        ).fetchone()[0]
    reset_flag = 1 if must_reset_password else 0
    if role == "staff" and must_reset_password is None:
        reset_flag = 1
    cur.execute(
        "INSERT INTO users (username, password_hash, role, staff_id, must_reset_password) VALUES (?, ?, ?, ?, ?)",
        (username.strip(), create_password_hash(password), role, linked_staff_id, reset_flag),
    )
    conn.commit()
    conn.close()

    _log_action(user.id, "create", "user", None, username.strip())

    return RedirectResponse(url="/admin/users", status_code=303)


@app.post("/admin/users/{user_id}/toggle")
def admin_users_toggle(request: Request, user_id: int):
    try:
        user = require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET active = CASE WHEN active = 1 THEN 0 ELSE 1 END WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()

    _log_action(user.id, "toggle", "user", user_id, "active")

    return RedirectResponse(url="/admin/users", status_code=303)


@app.post("/admin/users/{user_id}/force-reset")
def admin_users_force_reset(request: Request, user_id: int):
    try:
        user = require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET must_reset_password = 1 WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()

    _log_action(user.id, "force_reset", "user", user_id, "must_reset_password=1")

    return RedirectResponse(url="/admin/users", status_code=303)


@app.post("/admin/users/{user_id}/password")
def admin_users_password(request: Request, user_id: int, new_password: str = Form(...)):
    try:
        user = require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    if len(new_password) < 8:
        conn.close()
        return RedirectResponse(url="/admin/users", status_code=303)
    cur.execute(
        "UPDATE users SET password_hash = ?, must_reset_password = 1 WHERE id = ?",
        (create_password_hash(new_password), user_id),
    )
    conn.commit()
    conn.close()

    _log_action(user.id, "password_reset", "user", user_id, "admin reset")

    return RedirectResponse(url="/admin/users", status_code=303)


@app.get("/admin/evidence/sop/{sop_id}")
def admin_evidence(request: Request, sop_id: int):
    try:
        require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    sop = cur.execute("SELECT * FROM sops WHERE id = ?", (sop_id,)).fetchone()
    versions = cur.execute(
        "SELECT version, created_at FROM sop_versions WHERE sop_id = ? ORDER BY version DESC",
        (sop_id,),
    ).fetchall()
    acknowledgments = cur.execute(
        """
        SELECT staff.name AS staff_name, acknowledgments.acknowledged_at, acknowledgments.sop_version,
               acknowledgments.read_seconds, acknowledgments.signature_text, acknowledgments.ip_address
        FROM acknowledgments
        JOIN staff ON staff.id = acknowledgments.staff_id
        WHERE acknowledgments.sop_id = ?
        ORDER BY acknowledgments.acknowledged_at DESC
        """,
        (sop_id,),
    ).fetchall()
    conn.close()

    if not sop:
        return RedirectResponse(url="/admin", status_code=302)

    return _template(
        "evidence.html",
        request,
        {"request": request, "sop": sop, "versions": versions, "acknowledgments": acknowledgments},
    )


@app.get("/admin/audit")
def admin_audit(
    request: Request,
    user: Optional[str] = None,
    action: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    try:
        require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    where = "WHERE 1=1"
    params: list[str] = []
    if user:
        where += " AND users.username = ?"
        params.append(user)
    if action:
        where += " AND audit_log.action = ?"
        params.append(action)
    if start_date:
        where += " AND date(audit_log.created_at) >= date(?)"
        params.append(start_date)
    if end_date:
        where += " AND date(audit_log.created_at) <= date(?)"
        params.append(end_date)

    logs = cur.execute(
        f"""
        SELECT audit_log.created_at, audit_log.action, audit_log.entity_type, audit_log.entity_id,
               audit_log.details, users.username
        FROM audit_log
        LEFT JOIN users ON users.id = audit_log.actor_user_id
        {where}
        ORDER BY audit_log.created_at DESC
        LIMIT 200
        """,
        params,
    ).fetchall()
    users = cur.execute("SELECT username FROM users ORDER BY username").fetchall()
    actions = cur.execute("SELECT DISTINCT action FROM audit_log ORDER BY action").fetchall()
    conn.close()

    return _template(
        "audit_log.html",
        request,
        {
            "request": request,
            "logs": logs,
            "users": users,
            "actions": actions,
            "selected_user": user or "",
            "selected_action": action or "",
            "start_date": start_date or "",
            "end_date": end_date or "",
        },
    )


@app.get("/admin/export/acknowledgments.csv")
def export_acknowledgments(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT acknowledgments.acknowledged_at, sops.title, sops.category, staff.name AS staff_name,
               acknowledgments.sop_version, acknowledgments.read_seconds, acknowledgments.ip_address
        FROM acknowledgments
        JOIN sops ON sops.id = acknowledgments.sop_id
        JOIN staff ON staff.id = acknowledgments.staff_id
        ORDER BY acknowledgments.acknowledged_at DESC
        """
    ).fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["acknowledged_at", "sop_title", "category", "staff_name", "sop_version", "read_seconds", "ip_address"])
    for row in rows:
        writer.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6]])

    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=acknowledgments.csv"})


@app.get("/admin/export/audit.csv")
def export_audit(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT audit_log.created_at, audit_log.action, audit_log.entity_type, audit_log.entity_id,
               audit_log.details, users.username
        FROM audit_log
        LEFT JOIN users ON users.id = audit_log.actor_user_id
        ORDER BY audit_log.created_at DESC
        """
    ).fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["created_at", "action", "entity_type", "entity_id", "details", "username"])
    for row in rows:
        writer.writerow([row[0], row[1], row[2], row[3], row[4], row[5]])

    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=audit_log.csv"})


@app.get("/admin/export/sops.csv")
def export_sops(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()
    rows = cur.execute("SELECT title, category, current_version, updated_at FROM sops ORDER BY title").fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["title", "category", "current_version", "updated_at"])
    for row in rows:
        writer.writerow([row[0], row[1], row[2], row[3]])

    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=sops.csv"})


@app.get("/compliance")
def compliance_dashboard(
    request: Request,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    if not get_current_user(request):
        return _redirect_login()

    conn = get_connection()
    cur = conn.cursor()

    staff = cur.execute("SELECT id, name FROM staff WHERE active = 1 ORDER BY name").fetchall()
    sops = cur.execute("SELECT id, title, category FROM sops ORDER BY title").fetchall()
    staff_count = len(staff)
    sop_count = len(sops)

    cutoff = date.today() - timedelta(days=REACK_DAYS)
    effective_start = cutoff
    if start_date:
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            if start > effective_start:
                effective_start = start
        except ValueError:
            pass

    date_clause = ""
    date_params: list[str] = []
    if effective_start:
        date_clause += " AND date(acknowledged_at) >= date(?)"
        date_params.append(str(effective_start))
    if end_date:
        date_clause += " AND date(acknowledged_at) <= date(?)"
        date_params.append(end_date)

    ack_by_sop = {
        row["sop_id"]: row["ack_count"]
        for row in cur.execute(
            f"""
            SELECT sop_id, COUNT(*) AS ack_count FROM (
                SELECT acknowledgments.sop_id AS sop_id, acknowledgments.staff_id AS staff_id,
                       MAX(acknowledged_at) AS last_ack
                FROM acknowledgments
                JOIN staff ON staff.id = acknowledgments.staff_id AND staff.active = 1
                WHERE 1=1 {date_clause}
                GROUP BY acknowledgments.sop_id, acknowledgments.staff_id
            )
            GROUP BY sop_id
            """,
            date_params,
        ).fetchall()
    }

    ack_by_category = {
        row["category"]: row["ack_count"]
        for row in cur.execute(
            f"""
            SELECT sops.category AS category, COUNT(*) AS ack_count FROM (
                SELECT acknowledgments.sop_id AS sop_id, acknowledgments.staff_id AS staff_id,
                       MAX(acknowledged_at) AS last_ack
                FROM acknowledgments
                JOIN staff ON staff.id = acknowledgments.staff_id AND staff.active = 1
                WHERE 1=1 {date_clause}
                GROUP BY acknowledgments.sop_id, acknowledgments.staff_id
            ) latest
            JOIN sops ON sops.id = latest.sop_id
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
            SELECT COUNT(*) AS acked FROM (
                SELECT acknowledgments.sop_id AS sop_id, MAX(acknowledged_at) AS last_ack
                FROM acknowledgments
                WHERE acknowledgments.staff_id = ? {date_clause}
                GROUP BY acknowledgments.sop_id
            )
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
                    GROUP BY acknowledgments.staff_id
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

    return _template(
        "compliance.html",
        request,
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
            "reack_days": REACK_DAYS,
        },
    )
