# Mulligan Recovery Centers SOP App

## Setup
1. Create a virtual environment and install deps (Python 3.9+):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Alternatively, install from `pyproject.toml`:

```bash
pip install -e .
```

2. Import SOPs from the Policies & Procedures folder:

```bash
python scripts/import_sops.py
```

3. Run the web app:

```bash
uvicorn app.main:app --reload
```

Open http://127.0.0.1:8000

## Login
- Default admin user is created on startup if none exists.
- Defaults can be overridden with env vars:

```
MRC_ADMIN_USERNAME=admin
MRC_ADMIN_PASSWORD=ChangeMe123!
MRC_SESSION_SECRET=change-me-please
```

**Change the default password immediately in production.**

## Features
- Search and filter SOPs
- SOP detail pages with staff acknowledgment
- Admin pages for SOP add/edit
- Staff roster management + CSV import
- User accounts with roles (admin/staff)
- SOP version history
- Evidence pack per SOP
- Compliance dashboard with:
  - Compliance rate by SOP and category
  - Staff completion rates
  - Recent acknowledgments
  - Overdue/missing acknowledgments
  - Date range filtering
- CSV exports for SOPs and acknowledgments

## Notes
- The SQLite database lives at `instance/sops.db`.
- Admin pages are at `/admin`.
- Compliance dashboard is at `/compliance`.
- If the folder path changes, update `SOURCE_DIR` in `app/seed.py` and `scripts/import_sops.py`.

## Render deploy notes
- The import runs automatically on startup if the database is empty.
- SOPs are loaded from the `policies/` folder in this repo.
