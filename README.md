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

If you use `pyenv`, a recommended version is already pinned in `.python-version`:

```bash
pyenv install -s 3.11.9
pyenv local 3.11.9
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

## Render deploy notes
- The import runs automatically on startup if the database is empty.
- SOPs are loaded from the `policies/` folder in this repo.

## Features
- Search and filter SOPs
- SOP detail pages with staff acknowledgment
- Admin pages for SOP add/edit
- Staff roster management
- Compliance dashboard with:
  - Compliance rate by SOP and category
  - Staff completion rates
  - Recent acknowledgments
  - Overdue/missing acknowledgments
  - Date range filtering

## Notes
- The SQLite database lives at `instance/sops.db`.
- Admin pages are at `/admin` (no authentication yet).
- Compliance dashboard is at `/compliance`.
- If the folder path changes, update `SOURCE_DIR` in `scripts/import_sops.py`.
