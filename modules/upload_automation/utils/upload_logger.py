"""BigQuery event logger for the Creative Upload tab.

Logs user actions (login, uploads, errors, etc.) to
`roas-test-456808.data_check.crema_upload_log`.

Every public function is fail-safe — logging must never break the app.
The BigQuery insert runs on a background thread so it never blocks the UI.
"""
from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone

import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

TABLE_ID = "roas-test-456808.data_check.crema_upload_log"

# Module-level client — initialized once on first log_event() call from main thread.
_client: bigquery.Client | None = None


def _get_client() -> bigquery.Client:
    """Return the BigQuery client, creating it on first call."""
    global _client
    if _client is None:
        try:
            if "gcp_service_account" in st.secrets:
                creds = service_account.Credentials.from_service_account_info(
                    st.secrets["gcp_service_account"]
                )
                _client = bigquery.Client(
                    credentials=creds,
                    project=st.secrets["gcp_service_account"]["project_id"],
                )
            else:
                _client = bigquery.Client(project="roas-test-456808")
        except Exception as e:
            print(f"[upload_logger] client init failed: {e}")
            _client = bigquery.Client(project="roas-test-456808")
    return _client


def _session_id() -> str:
    """Get or create a session-scoped ID for correlating events."""
    if "log_session_id" not in st.session_state:
        st.session_state["log_session_id"] = uuid.uuid4().hex[:16]
    return st.session_state["log_session_id"]


def _insert_row(client: bigquery.Client, row: dict) -> None:
    """Insert a single row into BigQuery. Runs on a background thread."""
    try:
        print(f"[upload_logger] inserting {row.get('event_type')}...")
        errors = client.insert_rows_json(TABLE_ID, [row])
        if errors:
            print(f"[upload_logger] BQ insert errors: {errors}")
        else:
            print(f"[upload_logger] OK: {row.get('event_type')}")
    except Exception as e:
        print(f"[upload_logger] insert failed: {e}")


def log_event(
    event_type: str,
    *,
    mode: str | None = None,
    game: str | None = None,
    platform: str | None = None,
    upload_method: str | None = None,
    file_count: int | None = None,
    success_count: int | None = None,
    error_count: int | None = None,
    error_message: str | None = None,
    settings: dict | None = None,
    result: dict | None = None,
) -> None:
    """Insert one row into crema_upload_log. Never raises, never blocks."""
    try:
        # Capture everything from Streamlit context on the main thread
        client = _get_client()
        user_agent = "Unknown"
        try:
            user_agent = st.context.headers.get("User-Agent", "Unknown")
        except Exception:
            pass

        row = {
            "log_id": uuid.uuid4().hex,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "session_id": _session_id(),
            "user_email": st.session_state.get("user_email", "unknown"),
            "user_name": st.session_state.get("user_name"),
            "user_role": st.session_state.get("user_role"),
            "login_method": st.session_state.get("login_method"),
            "event_type": event_type,
            "mode": mode,
            "game": game,
            "platform": platform,
            "upload_method": upload_method,
            "file_count": file_count,
            "success_count": success_count,
            "error_count": error_count,
            "error_message": error_message[:2000] if error_message else None,
            "settings_json": (
                json.dumps(settings, default=str, ensure_ascii=False)[:5000]
                if settings
                else None
            ),
            "result_json": (
                json.dumps(result, default=str, ensure_ascii=False)[:5000]
                if result
                else None
            ),
            "user_agent": user_agent,
        }
        row = {k: v for k, v in row.items() if v is not None}

        # Fire and forget on background thread — pass the client directly
        threading.Thread(target=_insert_row, args=(client, row), daemon=True).start()
    except Exception as e:
        print(f"[upload_logger] {e}")
