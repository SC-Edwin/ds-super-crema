# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Super Crema is a Streamlit-based Creative Intelligence Automation Platform for the Supercent Marketing Intelligence Team. It provides creative performance analytics (BigQuery-backed), bulk creative asset uploads to ad networks (Facebook/Meta, Unity Ads, Mintegral, Applovin), and links to external localization/video generation services.

Primary language is Python. UI text and comments are largely in Korean.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the app (default port 8501)
streamlit run app.py

# Run in devcontainer/codespaces mode (disables CORS/XSRF for preview)
streamlit run app.py --server.enableCORS false --server.enableXsrfProtection false

# Google Cloud auth (required for BigQuery access)
gcloud auth application-default login
```

There is no test suite, linter configuration, or build step.

## Architecture

**Entry point:** `app.py` — sets up Streamlit page config, applies custom dark theme CSS, handles authentication, and renders tab navigation.

**Authentication:** `modules/auth_simple.py` — dual login (Google OAuth + ID/password), cookie-based session persistence (7-day expiry), role-based access (admin/user). User database is hardcoded with hashed passwords.

**Main modules (loaded as tabs):**

- **Tab 1 — Performance Analytics** (`modules/visualization/main.py`): Queries BigQuery for creative performance predictions, renders interactive Plotly charts (bubble, bar, pie), supports week-based filtering, CSV export, and an AI recommendations modal. Data is cached with 5-minute TTL.

- **Tab 2 — Creative Upload** (`modules/upload_automation/main.py`): Multi-platform creative asset upload hub. Two modes: **Test** (operations team) and **Marketer**. Supports Google Drive folder import (parallel processing) and local file upload.

- **Tabs 3-5**: Placeholders or external redirects (localization → `creative-crema.web.app`).

**Upload tab layout** (under `modules/upload_automation/`):
- `ui/upload_tab.py` — Streamlit Creative Upload tab (widgets + `session_state`; entry `run`)
- `session/keys.py` — shared session key constants for that tab
- `application/` — use-case / validation without Streamlit (grows incrementally; e.g. `upload_validation.py`)
- `main.py` — thin re-exports `run` / `render_main_app` / `init_*` for `app.py` compatibility

**Ad platform modules** (under `modules/upload_automation/platforms/`):
- `platforms/meta/facebook_ads.py` + `platforms/meta/fb.py` — Meta/Facebook (admin + marketer)
- `platforms/unity/unity_ads.py` + `platforms/unity/uni.py` — Unity Ads (admin + marketer)
- `platforms/mintegral/mintegral.py` — Mintegral
- `platforms/applovin/applovin.py` — Applovin
- `platforms/google_ads/google_ads.py` + `platforms/google_ads/ga.py` — Google Ads API + marketer UI
- `config/game_manager.py` — game configuration management (runtime `games_config.json`)
- `utils/drive_import.py` — Google Drive video import with parallel processing
- `utils/devtools.py`, `utils/upload_logger.py` — developer debug panel and BigQuery audit logging
- `scripts/generate_refresh_token.py` — one-off CLI to mint Google Ads OAuth refresh token (not imported by the app)

Each platform package follows a similar pattern: settings panel, media library upload, creative set/campaign creation. Meta/Unity/Google keep a thick `*_ads`/`google_ads` module plus a marketer-facing sibling (`fb`, `uni`, `ga`); `fb` still imports Facebook Business SDK and orchestration, so it was not merged into `facebook_ads` in this pass.

## Key Patterns

- **Session state**: Heavy use of `st.session_state` for uploads, settings, remote video lists, and auth state. Query params preserve tab selection across page reloads.
- **Role-based rendering**: `is_marketer` flag controls which modules and features are shown.
- **Secrets**: API keys and credentials are stored in `.streamlit/secrets.toml` (not in repo), accessed via `st.secrets`.
- **Environment detection**: `STREAMLIT_ENV` variable distinguishes local/dev/main deployments.
- **`Past/` directories**: Contain archived historical versions of modules for reference.

## Configuration

- `requirements.txt` — Python dependencies
- `packages.txt` — system-level apt packages (libgl1 for OpenCV)
- `.streamlit/secrets.toml` — credentials (Google Cloud, OAuth, Meta, Unity, Mintegral, Applovin) — local only, gitignored
- `.devcontainer/devcontainer.json` — VS Code Codespaces setup (Python 3.11)
