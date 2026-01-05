# drive_import.py
from __future__ import annotations
import io
import os
import pathlib
import tempfile
from typing import List, Dict, Optional, Callable

import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials


#export GOOGLE_APPLICATION_CREDENTIALS="/Users/eader/Downloads/roas-test-456808-321ce7426bfb.json"
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# File type extensions
VIDEO_EXTS = {".mp4", ".mpeg4", ".mov", ".mkv"}
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}
PLAYABLE_EXTS = {".zip", ".html"}

def get_drive_service_from_secrets():
    """
    Create an authenticated Drive API client using service-account credentials.

    Priority:
      1) st.secrets["gcp_service_account"]  (if present)
      2) GOOGLE_CREDENTIALS or GCP_SERVICE_ACCOUNT_JSON env var (full JSON)
      3) GOOGLE_APPLICATION_CREDENTIALS env var (path to JSON key file)
    """
    import json
    import os

    info = None

    # 1) Try st.secrets safely (no KeyError)
    try:
        if "gcp_service_account" in st.secrets:
            info = dict(st.secrets["creative_gcp_key"])
    except Exception:
        # st.secrets may not exist outside Streamlit runtime
        pass

    # 2) Try JSON in environment variable
    if info is None:
        env_json = os.getenv("GOOGLE_CREDENTIALS") or os.getenv("GCP_SERVICE_ACCOUNT_JSON")
        if env_json:
            try:
                info = json.loads(env_json)
            except Exception as e:
                raise RuntimeError(
                    "GOOGLE_CREDENTIALS / GCP_SERVICE_ACCOUNT_JSON is set "
                    "but could not be parsed as JSON."
                ) from e

    # 3) Try path in GOOGLE_APPLICATION_CREDENTIALS
    if info is None:
        cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if cred_path:
            if not os.path.isfile(cred_path):
                raise RuntimeError(
                    f"GOOGLE_APPLICATION_CREDENTIALS is set to '{cred_path}', "
                    "but that file does not exist."
                )
            creds = Credentials.from_service_account_file(cred_path, scopes=DRIVE_SCOPES)
            return build("drive", "v3", credentials=creds, cache_discovery=False)

    if info is None:
        # Build a short diagnostics summary to help users understand why no creds were found.
        diag: list[str] = []
        try:
            if hasattr(st, "secrets") and isinstance(st.secrets, dict):
                try:
                    keys = list(st.secrets.keys())
                except Exception:
                    keys = ["(unreadable)"]
                diag.append(f"st.secrets present; keys: {keys}")
            else:
                diag.append("st.secrets: not present or not a dict")
        except Exception as _:
            diag.append("st.secrets: access raised an exception")

        env_json = bool(os.getenv("GOOGLE_CREDENTIALS") or os.getenv("GCP_SERVICE_ACCOUNT_JSON"))
        diag.append(f"GOOGLE_CREDENTIALS / GCP_SERVICE_ACCOUNT_JSON set: {env_json}")

        gapp = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if gapp:
            diag.append(f"GOOGLE_APPLICATION_CREDENTIALS='{gapp}'; exists: {os.path.isfile(gapp)}")
        else:
            diag.append("GOOGLE_APPLICATION_CREDENTIALS: not set")

        raise RuntimeError(
            "No Google service account credentials found.\n"
            "Possible fixes:\n"
            "  - Add your service account JSON as a nested table in .streamlit/secrets.toml under the key 'gcp_service_account' (run app with `streamlit run`).\n"
            "    Example (TOML):\n"
            "      [gcp_service_account]\n"
            "      type = \"service_account\"\n"
            "      project_id = \"...\"\n"
            "      private_key = '''-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----'''\n"
            "  - Or export the full JSON into GOOGLE_CREDENTIALS (env) or set GOOGLE_APPLICATION_CREDENTIALS to the path of the key file.\n\n"
            "Diagnostics:\n" + "\n".join(diag)
        )

    # If we got info (from st.secrets or env JSON), build the client from it
    creds = Credentials.from_service_account_info(info, scopes=DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def extract_drive_folder_id(url_or_id: str) -> str:
    """Accepts a Drive folder URL or raw ID and returns the folder ID."""
    s = (url_or_id or "").strip()
    if not s:
        raise ValueError("Provide a Google Drive folder URL or ID.")
    if "drive.google.com" in s and "/folders/" in s:
        return s.split("/folders/")[1].split("?")[0].split("/")[0]
    return s  # assume already an ID

def list_drive_files_in_folder(service, folder_id: str, file_type: str = "VIDEO") -> List[Dict]:
    """
    List *all* files in a folder (with pagination, supports shared drives),
    keep items that match the file_type.
    
    Args:
        service: Drive API service
        folder_id: Drive folder ID
        file_type: "VIDEO", "IMAGE", or "PLAYABLE"
    
    Returns:
        [{'id': ..., 'name': ...}, ...]
    """
    # Determine which extensions to look for
    if file_type == "VIDEO":
        allowed_exts = VIDEO_EXTS
        mime_prefix = "video/"
    elif file_type == "IMAGE":
        allowed_exts = IMAGE_EXTS
        mime_prefix = "image/"
    elif file_type == "PLAYABLE":
        allowed_exts = PLAYABLE_EXTS
        mime_prefix = None  # No specific mime type
    else:
        # Default to VIDEO
        allowed_exts = VIDEO_EXTS
        mime_prefix = "video/"
    
    items: List[Dict] = []
    page_token = None
    q = f"'{folder_id}' in parents and trashed=false"
    fields = "nextPageToken, files(id, name, mimeType)"

    while True:
        resp = service.files().list(
            q=q,
            spaces="drive",
            fields=fields,
            pageSize=1000,
            pageToken=page_token,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            corpora="allDrives",
            orderBy="name_natural"
        ).execute()
        for f in resp.get("files", []):
            name = f.get("name") or ""
            ext = pathlib.Path(name).suffix.lower()
            mime_type = f.get("mimeType", "")
            
            # Check if file matches type
            is_match = False
            if mime_prefix and mime_type.startswith(mime_prefix):
                is_match = True
            elif ext in allowed_exts:
                is_match = True
            
            if is_match:
                items.append({"id": f["id"], "name": name})
        
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return items


def list_drive_videos_in_folder(service, folder_id: str) -> List[Dict]:
    """
    List *all* video files in a folder (backward compatibility).
    Returns [{'id': ..., 'name': ...}, ...]
    """
    return list_drive_files_in_folder(service, folder_id, file_type="VIDEO")


def download_drive_file_to_tmp(service, file_id: str, filename_hint: Optional[str] = None, *, max_retries: int = 5) -> Dict:
    """
    Stream a Drive file directly to a temporary file on disk and return {'name','path'}.
    Retries transient errors with exponential backoff.
    """
    # We first query the metadata to get the name (cheaper than guessing)
    try:
        meta = service.files().get(fileId=file_id, fields="name", supportsAllDrives=True).execute()
        name = meta.get("name") or filename_hint or f"{file_id}.tmp"
    except Exception:
        name = filename_hint or f"{file_id}.tmp"

    # Determine suffix
    suffix = pathlib.Path(name).suffix or ".tmp"

    # Request media
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)

    # Stream to disk, not RAM
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        path = tmp.name
    fh = open(path, "wb")
    try:
        downloader = MediaIoBaseDownload(fh, request, chunksize=32*1024 * 1024)
        done = False
        attempt = 0
        while not done:
            try:
                status, done = downloader.next_chunk()
            except HttpError as e:
                attempt += 1
                if attempt > max_retries:
                    raise
                # basic exponential backoff
                import time
                time.sleep(min(2 ** attempt, 30))
            except Exception as e:
                attempt += 1
                if attempt > max_retries:
                    raise
                import time
                time.sleep(min(2 ** attempt, 30))
    finally:
        fh.close()

    return {"name": pathlib.Path(name).name, "path": path}

def import_drive_folder_files_parallel(
    folder_url_or_id: str,
    file_type: str = "VIDEO",
    max_workers: int = 6,
    on_progress: Optional[Callable[[int, int, str, Optional[str]], None]] = None,
) -> List[Dict]:
    """
    List & download all files of specified type in parallel.
    
    Args:
        folder_url_or_id: Drive folder URL or ID
        file_type: "VIDEO", "IMAGE", or "PLAYABLE"
        max_workers: Number of parallel download workers
        on_progress: Callback(done, total, file_name, error_message_or_None)
    
    Returns:
        Successfully downloaded [{'name','path'}, ...]
    """
    # Enumerate first with a single service (cheap calls)
    svc_list = get_drive_service_from_secrets()
    folder_id = extract_drive_folder_id(folder_url_or_id)
    files = list_drive_files_in_folder(svc_list, folder_id, file_type=file_type)
    total = len(files)
    done = 0
    results: List[Dict] = []
    errors: List[str] = []

    if total == 0:
        if on_progress: on_progress(0, 0, "", None)
        return results

    def _one(meta: Dict) -> Dict:
        # Create a fresh service per worker to avoid concurrency issues
        svc = get_drive_service_from_secrets()
        return download_drive_file_to_tmp(svc, meta["id"], filename_hint=meta.get("name"))

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_map = {ex.submit(_one, f): f for f in files}
        for fut in as_completed(fut_map):
            src = fut_map[fut]
            name = src.get("name", "(no name)")
            err_msg = None
            try:
                out = fut.result()
                results.append(out)
            except Exception as e:
                err_msg = str(e)
                errors.append(f"{name}: {err_msg}")
            finally:
                done += 1
                if on_progress:
                    on_progress(done, total, name, err_msg)

    if errors:
        import logging
        logging.warning("Some Drive downloads failed: %s", errors)

    return results


def import_drive_folder_videos_parallel(
    folder_url_or_id: str,
    max_workers: int = 6,
    on_progress: Optional[Callable[[int, int, str, Optional[str]], None]] = None,
) -> List[Dict]:
    """
    Backward compatibility wrapper for video import.
    Calls import_drive_folder_files_parallel with file_type="VIDEO".
    """
    return import_drive_folder_files_parallel(
        folder_url_or_id,
        file_type="VIDEO",
        max_workers=max_workers,
        on_progress=on_progress
    )