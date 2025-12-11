"""Facebook/Meta helpers for Creative 자동 업로드 Streamlit app."""

from __future__ import annotations

from typing import Dict, List, Any
from datetime import datetime, timedelta, timezone
import logging
import pathlib
import tempfile
import os

import requests
import streamlit as st

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Thumbnail extraction and upload helpers
# --------------------------------------------------------------------
def extract_thumbnail_from_video(video_path: str, output_path: str | None = None) -> str:
    """
    Extract thumbnail from video using opencv.
    Returns path to the saved thumbnail image.
    """
    try:
        import cv2
    except ImportError:
        raise RuntimeError(
            "opencv-python-headless is required for thumbnail extraction. "
            "Install it with: pip install opencv-python-headless"
        )
    
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {video_path}")
    
    try:
        # Get middle frame (or first frame if video is too short)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total_frames > 0:
            frame_number = max(0, total_frames // 2)
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
        
        ret, frame = cap.read()
        
        if not ret:
            # Fallback to first frame
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            ret, frame = cap.read()
        
        if not ret:
            raise RuntimeError(f"Cannot read frame from video: {video_path}")
        
        # Save thumbnail
        if output_path is None:
            import tempfile
            output_path = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False).name
        
        cv2.imwrite(output_path, frame)
        logger.info(f"Extracted thumbnail from {video_path} to {output_path}")
        return output_path
    finally:
        cap.release()

def upload_thumbnail_image(account: "AdAccount", image_path: str) -> str:
    """
    Upload thumbnail image to Facebook using Graph API directly (like video upload).
    Returns the image URL (required for video_data.image_url).
    """
    # Get access token
    if "facebook" in st.secrets:
        token = st.secrets["facebook"].get("access_token", "").strip()
    else:
        token = st.secrets.get("access_token", "").strip()
    
    if not token:
        raise RuntimeError("Missing access_token in st.secrets (check [facebook] section)")
    
    act_id = account.get_id()
    url = f"https://graph.facebook.com/v24.0/{act_id}/adimages"
    
    try:
        # Upload image using multipart/form-data (same approach as video upload)
        with open(image_path, 'rb') as f:
            files = {'file': (os.path.basename(image_path), f, 'image/jpeg')}
            data = {'access_token': token}
            
            response = requests.post(url, files=files, data=data, timeout=60)
            response.raise_for_status()
            
            result = response.json()
            
            # Log full response for debugging
            logger.debug(f"AdImage API response: {result}")
            
            # Extract image URL from response (required for video_data.image_url)
            # Response format: {"images": {"<hash>": {"hash": "<hash>", "url": "..."}}}
            images = result.get("images", {})
            if isinstance(images, dict):
                # Get the first (and only) image from the response
                for hash_key, image_data in images.items():
                    if isinstance(image_data, dict):
                        image_url = image_data.get("url")
                        image_hash = image_data.get("hash") or hash_key
                        if image_url:
                            logger.info(f"Uploaded thumbnail image {image_path} to Facebook, url: {image_url}")
                            return image_url
                        # Fallback: construct URL from hash if URL not provided
                        logger.warning(f"URL not found in response, constructing from hash: {image_hash}")
                        # Try common Facebook CDN URL pattern
                        image_url = f"https://scontent.xx.fbcdn.net/v/t45.5328-4/{image_hash}.jpg"
                        return image_url
                    else:
                        # If image_data is just a string (the hash itself)
                        image_hash = hash_key
                        # Construct URL from hash
                        image_url = f"https://scontent.xx.fbcdn.net/v/t45.5328-4/{image_hash}.jpg"
                        logger.info(f"Uploaded thumbnail image {image_path} to Facebook, constructed url from hash: {image_url}")
                        return image_url
            
            # Fallback: try to get hash directly and construct URL
            image_hash = result.get("hash")
            if image_hash:
                image_url = f"https://scontent.xx.fbcdn.net/v/t45.5328-4/{image_hash}.jpg"
                logger.info(f"Uploaded thumbnail image {image_path} to Facebook, constructed url from hash: {image_url}")
                return image_url
            
            raise RuntimeError(f"Failed to get image URL from AdImage response: {result}")
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_data = e.response.json()
                error_msg = error_data.get("error", {}).get("message", error_msg)
            except:
                error_msg = e.response.text[:200] if e.response.text else error_msg
        logger.error(f"Failed to upload thumbnail image {image_path}: {error_msg}")
        raise RuntimeError(f"Failed to upload thumbnail image: {error_msg}") from e
    except Exception as e:
        logger.error(f"Failed to upload thumbnail image {image_path}: {e}")
        raise

# --------------------------------------------------------------------
# Meta SDK and account helpers
# --------------------------------------------------------------------
try:
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    from facebook_business.adobjects.adset import AdSet
    from facebook_business.adobjects.adcreative import AdCreative
    from facebook_business.adobjects.ad import Ad
    from facebook_business.exceptions import FacebookRequestError
    FB_AVAILABLE = True
    FB_IMPORT_ERROR = ""
except Exception as _e:
    FB_AVAILABLE = False
    FB_IMPORT_ERROR = f"{type(_e).__name__}: {_e}"

def _require_fb() -> None:
    """Raise a clear error if the Facebook SDK is missing."""
    if not FB_AVAILABLE:
        raise RuntimeError(
            "facebook-business SDK not available. Install it with:\n"
            "  pip install facebook-business\n"
            f"Import error: {FB_IMPORT_ERROR}"
        )

COUNTRY_OPTIONS = {
    # Top Tier Markets
    "United States": "US",
    "Canada": "CA",
    "United Kingdom": "GB",
    "Australia": "AU",
    "Germany": "DE",
    "France": "FR",
    "Japan": "JP",
    "South Korea": "KR",
    
    # European Markets
    "Italy": "IT",
    "Spain": "ES",
    "Netherlands": "NL",
    "Sweden": "SE",
    "Switzerland": "CH",
    "Norway": "NO",
    "Denmark": "DK",
    "Finland": "FI",
    "Austria": "AT",
    "Belgium": "BE",
    "Poland": "PL",
    
    # Asia-Pacific
    "Singapore": "SG",
    "Hong Kong": "HK",
    "Taiwan": "TW",
    "Thailand": "TH",
    "Indonesia": "ID",
    "Malaysia": "MY",
    "Philippines": "PH",
    "Vietnam": "VN",
    "India": "IN",
    
    # Middle East
    "United Arab Emirates": "AE",
    "Saudi Arabia": "SA",
    "Israel": "IL",
    
    # Latin America
    "Brazil": "BR",
    "Mexico": "MX",
    "Argentina": "AR",
    "Chile": "CL",
    "Colombia": "CO",
}

# Reverse lookup for displaying selected countries
COUNTRY_CODE_TO_NAME = {code: name for name, code in COUNTRY_OPTIONS.items()}

# --------------------------------------------------------------------
# Date / timezone helpers
# --------------------------------------------------------------------
ASIA_SEOUL = timezone(timedelta(hours=9))

def next_sat_0900_kst(today: datetime | None = None) -> str:
    """
    Compute start_iso in KST:
      - start: next Saturday 09:00
    Returned string is ISO8601 with +09:00 offset.
    """
    now = (today or datetime.now(ASIA_SEOUL)).astimezone(ASIA_SEOUL)
    base = now.replace(hour=0, minute=0, second=0, microsecond=0)
    # Monday=0 ... Saturday=5, Sunday=6
    days_until_sat = (5 - base.weekday()) % 7 or 7
    start_dt = (base + timedelta(days=days_until_sat)).replace(hour=9, minute=0)
    return start_dt.isoformat()

# --------------------------------------------------------------------
# Settings helpers (store URL, budget, targeting)
# --------------------------------------------------------------------

def requires_special_compliance(countries: list[str]) -> dict:
    """
    Check if any countries require special compliance handling.
    
    Returns dict with:
    {
        "has_blocked": bool,
        "blocked": list of blocked country codes,
        "blocked_reasons": dict of {country: reason},
        "has_special": bool,
        "countries": list of countries with special compliance,
        "types": dict of {country: compliance_type}
    }
    """
    # Countries that CANNOT be targeted via API without manual setup
    BLOCKED_COUNTRIES = {
        "TW": {
            "name": "Taiwan",
            "reason": "Requires manual business verification in Meta Ads Manager",
            "details": "Taiwan law requires advertiser identity disclosure and business registration verification"
        }
    }
    
    # Countries that CAN be handled via API (for future expansion)
    COMPLIANCE_COUNTRIES = {
        # Add here if you implement support for other compliance countries
        # "KR": "KOREA_UNIVERSAL",  # Example: If South Korea needs special handling
    }
    
    blocked = [c for c in countries if c in BLOCKED_COUNTRIES]
    special_countries = [c for c in countries if c in COMPLIANCE_COUNTRIES]
    
    return {
        "has_blocked": bool(blocked),
        "blocked": blocked,
        "blocked_reasons": {c: BLOCKED_COUNTRIES[c]["reason"] for c in blocked},
        "blocked_details": {c: BLOCKED_COUNTRIES[c] for c in blocked},
        "has_special": bool(special_countries),
        "countries": special_countries,
        "types": {c: COMPLIANCE_COUNTRIES[c] for c in special_countries}
    }

def sanitize_store_url(raw: str) -> str:
    """
    Normalize store URLs for Meta:
      - Google Play: keep ?id=<package> only
      - App Store: drop query/fragment
      - Other hosts: return as-is
    """
    from urllib.parse import urlsplit, urlunsplit, parse_qs, urlencode

    if not raw:
        return raw

    parts = urlsplit(raw)
    host = parts.netloc.lower()

    # Google Play: MUST preserve 'id' param only
    if "play.google.com" in host:
        qs = parse_qs(parts.query)
        pkg = (qs.get("id") or [None])[0]
        if not pkg:
            raise ValueError(
                "Google Play URL must include ?id=<package>. "
                "Example: https://play.google.com/store/apps/details?id=io.supercent.weaponrpg"
            )
        new_query = urlencode({"id": pkg})
        return urlunsplit(
            (parts.scheme, parts.netloc, parts.path or "/store/apps/details", new_query, "")
        )

    # Apple App Store: keep path only
    if "apps.apple.com" in host:
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

    # Other hosts: unchanged
    return raw

def compute_budget_from_settings(files: list, settings: dict, fallback_per_video: int = 10) -> int:
    """
    Budget per day = (#eligible videos) × per-video budget.
    Counts only .mp4/.mpeg4.
    """
    allowed = {".mp4", ".mpeg4"}

    def _name(u):
        return getattr(u, "name", None) or (u.get("name") if isinstance(u, dict) else "")

    n_videos = sum(
        1 for u in (files or []) if pathlib.Path(_name(u)).suffix.lower() in allowed
    )
    per_video = int(settings.get("budget_per_video_usd", fallback_per_video))
    return max(1, n_videos * per_video) if n_videos else per_video

def dollars_to_minor(usd: float) -> int:
    """Convert USD → Meta 'minor' units (1 USD → 100)."""
    return int(round(usd * 100))

ANDROID_OS_CHOICES = {
    "None (any)": None,
    "6.0+": "Android_ver_6.0_and_above",
    "7.0+": "Android_ver_7.0_and_above",
    "8.0+": "Android_ver_8.0_and_above",
    "9.0+": "Android_ver_9.0_and_above",
    "10.0+": "Android_ver_10.0_and_above",
    "11.0+": "Android_ver_11.0_and_above",
    "12.0+": "Android_ver_12.0_and_above",
    "13.0+": "Android_ver_13.0_and_above",
    "14.0+": "Android_ver_14.0_and_above",
}

IOS_OS_CHOICES = {
    "None (any)": None,
    "11.0+": "iOS_ver_11.0_and_above",
    "12.0+": "iOS_ver_12.0_and_above",
    "13.0+": "iOS_ver_13.0_and_above",
    "14.0+": "iOS_ver_14.0_and_above",
    "15.0+": "iOS_ver_15.0_and_above",
    "16.0+": "iOS_ver_16.0_and_above",
    "17.0+": "iOS_ver_17.0_and_above",
    "18.0+": "iOS_ver_18.0_and_above",
}

OPT_GOAL_LABEL_TO_API = {
    "앱 설치수 극대화": "APP_INSTALLS",
    "앱 이벤트 수 극대화": "APP_EVENTS",
    "전환값 극대화": "VALUE",
    "링크 클릭수 극대화": "LINK_CLICKS",
}

def build_targeting_from_settings(countries: list[str], age_min: int, settings: dict) -> dict:
    """
    Build Meta targeting dict from UI settings.
    Automatically detects OS from Store URL if 'os_choice' matches or defaults.
    """
    # 1. Basic Targeting
    if isinstance(countries, str):
        countries = [countries]
    
    targeting = {
        "geo_locations": {"countries": countries},
        "age_min": max(13, int(age_min)),
    }

    # 2. Determine OS Strategy
    # Detect platform from URL to prevent "Mismatch" errors
    store_url = (settings.get("store_url") or "").lower().strip()
    target_platform = "Both"
    
    if "play.google.com" in store_url:
        target_platform = "Android"  # <--- FORCE ANDROID
    elif "apps.apple.com" in store_url:
        target_platform = "iOS"      # <--- FORCE iOS
    else:
        # Only fallback to dropdown if URL is ambiguous
        os_choice = settings.get("os_choice", "Both")
        if os_choice == "Android only": target_platform = "Android"
        elif os_choice == "iOS only": target_platform = "iOS"

    # 3. Build user_os list
    user_os = []
    
    # Get version limits from settings
    min_android = settings.get("min_android_os_token")
    min_ios = settings.get("min_ios_os_token")

    if target_platform == "Android":
        token = min_android or "Android_ver_6.0_and_above"
        user_os.append(token)
        
    elif target_platform == "iOS":
        token = min_ios or "iOS_ver_11.0_and_above"
        user_os.append(token)
        
    elif target_platform == "Both":
        # Only add specific versions if "Both" is genuinely allowed
        if min_android: user_os.append(min_android)
        if min_ios: user_os.append(min_ios)

    # 4. Apply to targeting
    if user_os:
        targeting["user_os"] = user_os
        
        # [Additional Safety] Explicitly set user_device to []
        # This tells the API "All mobile devices compatible with the OS"
        # and helps resolve the "Targeting Mismatch" error.
        if target_platform in ("Android", "iOS"):
            targeting["user_device"] = [] 

    return targeting


def make_ad_name(filename: str, prefix: str | None) -> str:
    """Build ad name from filename and optional prefix."""
    return f"{prefix.strip()}_{filename}" if prefix else filename

# --------------------------------------------------------------------
# Session-state helpers for FB settings
# --------------------------------------------------------------------
def _ensure_settings_state() -> None:
    if "settings" not in st.session_state:
        st.session_state.settings = {}

def get_fb_settings(game: str) -> dict:
    """Return per-game FB settings dict (creating container if needed)."""
    _ensure_settings_state()
    return st.session_state.settings.get(game, {})

# --------------------------------------------------------------------
# Default per-game App IDs + Store URLs
# --------------------------------------------------------------------
GAME_DEFAULTS: Dict[str, Dict[str, str]] = {
    "XP HERO": {
        "fb_app_id": "519275767201283",
        "store_url": "https://play.google.com/store/apps/details?id=io.supercent.weaponrpg",
    },
    "Dino Universe": {
        "fb_app_id": "1665399243918955",
        "store_url": "https://play.google.com/store/apps/details?id=io.supercent.ageofdinosaurs",
    },
    "Snake Clash": {
        "fb_app_id": "1205179980183812",
        "store_url": "https://play.google.com/store/apps/details?id=io.supercent.linkedcubic",
    },
    "Pizza Ready": {
        "fb_app_id": "1475920199615616",
        "store_url": "https://play.google.com/store/apps/details?id=io.supercent.pizzaidle",
    },
    "Cafe Life": {
        "fb_app_id": "1343040866909064",
        "store_url": "https://play.google.com/store/apps/details?id=com.fireshrike.h2",
    },
    "Suzy's Restaurant": {
        "fb_app_id": "836273807918279",
        "store_url": "https://play.google.com/store/apps/details?id=com.corestudiso.suzyrest",
    },
    "Office Life": {
        "fb_app_id": "1570824996873176",
        "store_url": "https://play.google.com/store/apps/details?id=com.funreal.corporatetycoon",
    },
    "Lumber Chopper": {
        "fb_app_id": "2824067207774178",
        "store_url": "https://play.google.com/store/apps/details?id=dasi.prs2.lumberchopper",
    },
    "Burger Please": {
        "fb_app_id": "2967105673598896",
        "store_url": "https://play.google.com/store/apps/details?id=io.supercent.burgeridle",
    },
    "Prison Life": {
        "fb_app_id": "6564765833603067",
        "store_url": "https://play.google.com/store/apps/details?id=io.supercent.prison",
    },
}

def init_fb_game_defaults() -> None:
    """
    Apply FB app_id/store_url defaults per game without overwriting
    what the user has already saved in st.session_state.settings.
    """
    _ensure_settings_state()
    for game, defaults in GAME_DEFAULTS.items():
        cur = st.session_state.settings.get(game, {}) or {}
        if not cur.get("fb_app_id") and defaults.get("fb_app_id"):
            cur["fb_app_id"] = defaults["fb_app_id"]
        if not cur.get("store_url") and defaults.get("store_url"):
            cur["store_url"] = defaults["store_url"]
        st.session_state.settings[game] = cur



def init_fb_from_secrets(ad_account_id: str | None = None) -> "AdAccount":
    """
    Initialize Meta SDK using access_token from st.secrets (preferably under [facebook]),
    and return an AdAccount (default: XP HERO account if none given).
    """
    _require_fb()
    
    # Try to get token from [facebook] section first, then root
    if "facebook" in st.secrets:
        token = st.secrets["facebook"].get("access_token", "").strip()
    else:
        token = st.secrets.get("access_token", "").strip()

    if not token:
        raise RuntimeError(
            "Missing 'access_token' in st.secrets.\n"
            "Please add it to .streamlit/secrets.toml under [facebook] section:\n"
            "[facebook]\n"
            "access_token = \"...\""
        )

    FacebookAdsApi.init(access_token=token)

    default_act_id = "act_692755193188182"  # XP HERO default
    act_id = ad_account_id or default_act_id
    return AdAccount(act_id)

def validate_page_binding(account: "AdAccount", page_id: str) -> dict:
    """
    Ensure page_id is numeric/readable and fetch IG actor (if present).
    Returns {'id','name','instagram_business_account_id'}.
    """
    _require_fb()
    from facebook_business.adobjects.page import Page

    pid = str(page_id).strip()
    if not pid.isdigit():
        raise RuntimeError(f"Page ID must be numeric. Got: {page_id!r}")
    try:
        p = Page(pid).api_get(fields=["id", "name", "instagram_business_account"])
    except Exception as e:
        raise RuntimeError(
            f"Page validation failed for PAGE_ID={pid}. "
            "Use a real Facebook Page ID and ensure the token can read it."
        ) from e
    iba = (p.get("instagram_business_account") or {}).get("id")
    return {"id": p["id"], "name": p["name"], "instagram_business_account_id": iba}

# --------------------------------------------------------------------
# File helpers for uploads
# --------------------------------------------------------------------
VERBOSE_UPLOAD_LOG = False

def _fname_any(u) -> str:
    """Return a filename for either a Streamlit UploadedFile or a {'name','path'} dict."""
    return getattr(u, "name", None) or (u.get("name") if isinstance(u, dict) else "")

def _dedupe_by_name(files):
    """Keep first occurrence of each filename (case-insensitive)."""
    seen = set()
    out = []
    for u in files or []:
        n = (_fname_any(u) or "").strip().lower()
        if n and n not in seen:
            seen.add(n)
            out.append(u)
    return out

def _save_uploadedfile_tmp(u) -> str:
    """
    Persist a video source to disk and return its path.
    Supports UploadedFile and {'name','path'} dicts.
    """
    if isinstance(u, dict) and "path" in u and "name" in u:
        return u["path"]
    if hasattr(u, "getbuffer"):
        suffix = pathlib.Path(u.name).suffix.lower() or ".mp4"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(u.getbuffer())
            return tmp.name
    raise ValueError("Unsupported video object type for saving.")

# --------------------------------------------------------------------
# Video status checking
# --------------------------------------------------------------------
def wait_for_video_ready(account: "AdAccount", video_id: str, max_wait: int = 300, progress_bar=None, progress_text: str = "") -> bool:
    """
    비디오가 ready 상태가 될 때까지 대기
    Returns True if ready, False if timeout
    """
    from facebook_business.adobjects.advideo import AdVideo
    import time
    
    video = AdVideo(video_id, api=account.get_api())
    start_time = time.time()
    check_count = 0
    
    while time.time() - start_time < max_wait:
        try:
            video.api_get(fields=["status"])
            status = video.get("status", "")
            
            check_count += 1
            elapsed = int(time.time() - start_time)
            
            # Progress bar 업데이트
            if progress_bar:
                estimated_progress = min(elapsed / max_wait, 0.95)
                progress_bar.progress(
                    estimated_progress,
                    text=f"{progress_text} ⏳ Processing... ({elapsed}s)"
                )
            
            if status == "ready":
                if progress_bar:
                    progress_bar.progress(1.0, text=f"{progress_text} ✅ Ready!")
                return True
            if status in ["failed", "error"]:
                if progress_bar:
                    progress_bar.progress(1.0, text=f"{progress_text} ❌ Failed")
                return False
                
            time.sleep(5)  # 5초마다 확인
        except Exception as e:
            logger.warning(f"Error checking video {video_id} status: {e}")
            time.sleep(5)
    
    if progress_bar:
        progress_bar.progress(1.0, text=f"{progress_text} ⚠️ Timeout")
    return False

# --------------------------------------------------------------------
# Resumable upload + ad creation
# --------------------------------------------------------------------
def upload_videos_create_ads(
    account: "AdAccount",
    *,
    page_id: str,
    adset_id: str,
    uploaded_files: list,
    ad_name_prefix: str | None = None,
    max_workers: int = 6,
    store_url: str | None = None,
    try_instagram: bool = True,
    settings: dict | None = None,
):
    """
    [Hybrid Mode]
    - Test Mode: Uploads every video as a separate ad (Original behavior).
    - Marketer Mode: Groups videos by name (3 sizes) & applies Multi-Text (New behavior).
    """
    from facebook_business.adobjects.adcreative import AdCreative
    from facebook_business.adobjects.ad import Ad
    from facebook_business.exceptions import FacebookRequestError
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    import re
    import pathlib
    import os

    # ------------------------------------------------------------------
    # 0. DETECT MODE
    # ------------------------------------------------------------------
    # If "creative_type" is in settings, it comes from the Marketer UI.
    is_marketer_mode = settings and "creative_type" in settings
    
    allowed = {".mp4", ".mpeg4"}
    def _fname_any(u) -> str:
        return getattr(u, "name", None) or (u.get("name") if isinstance(u, dict) else "")

    # ------------------------------------------------------------------
    # 1. VALIDATION & GROUPING (Only for Marketer Mode)
    # ------------------------------------------------------------------
    video_groups = {} 
    unique_files_to_upload = []
    
    if is_marketer_mode:
        # [Marketer Mode Logic] Group files by base name
        def _get_ratio_from_name(fname):
            lower = fname.lower()
            # Check for pixel dimensions first (1080x1080, 1920x1080, 1080x1920)
            if "1080x1080" in lower: return "1x1"
            if "1080x1920" in lower: return "9x16"
            if "1920x1080" in lower: return "16x9"
            # Then check for ratio strings
            if "9x16" in lower or "port" in lower or "story" in lower: return "9x16"
            if "16x9" in lower or "land" in lower or "wide" in lower: return "16x9"
            if "1x1" in lower or "sq" in lower or "feed" in lower: return "1x1"
            return "unknown"

        def _get_base_name(fname):
            # Remove both ratio strings and pixel dimensions
            base = re.sub(r'[_ -]?(1x1|9x16|16x9|sq|port|land|story|feed|wide|1080x1080|1920x1080|1080x1920)', '', fname, flags=re.IGNORECASE)
            base = pathlib.Path(base).stem
            return base.strip()

        seen_filenames = set()
        errors = []
        warnings = []

        for u in uploaded_files:
            fname = _fname_any(u)
            if pathlib.Path(fname).suffix.lower() not in allowed: continue
            
            base = _get_base_name(fname)
            ratio = _get_ratio_from_name(fname)
            
            if ratio == "unknown":
                errors.append(f"❌ '{fname}': Filename missing ratio (1x1, 9x16, 16x9).")
                continue

            if base not in video_groups: video_groups[base] = {}
            if ratio in video_groups[base]:
                errors.append(f"❌ '{base}' group has duplicate '{ratio}'.")
                continue
            
            video_groups[base][ratio] = u
            if fname not in seen_filenames:
                unique_files_to_upload.append(u)
                seen_filenames.add(fname)

        # Validation Checks
        required = {"1x1", "9x16", "16x9"}
        
        for base, files in video_groups.items():
            existing = set(files.keys())
            missing = required - existing
            
            # [STRICT] If any size is missing -> ERROR (Block Upload)
            if missing:
                errors.append(
                    f"❌ '{base}' group is incomplete.\n"
                    f"   • Found: {', '.join(existing)}\n"
                    f"   • Missing: {', '.join(missing)}\n"
                    f"   (All 3 sizes are required for Marketer Mode)"
                )

        # Stop immediately if ANY errors found
        if errors:
            st.error("### ⛔ Upload Blocked: Incomplete Assets")
            for e in errors:
                st.write(e)
            st.stop()  # STOPS EXECUTION HERE
        
        if warnings:
            with st.expander("⚠️ Grouping Warnings", expanded=True):
                for w in warnings: st.write(w)

    else:
        # [Test Mode Logic] Just dedupe files, no grouping
        seen = set()
        for u in uploaded_files:
            fname = _fname_any(u)
            if pathlib.Path(fname).suffix.lower() in allowed and fname not in seen:
                unique_files_to_upload.append(u)
                seen.add(fname)

    # ------------------------------------------------------------------
    # 2. UPLOAD FILES (Shared Logic)
    # ------------------------------------------------------------------
    def _save_uploadedfile_tmp(u) -> str:
        if isinstance(u, dict) and "path" in u: return u["path"]
        if hasattr(u, "getbuffer"):
            suffix = pathlib.Path(u.name).suffix.lower() or ".mp4"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(u.getbuffer())
                return tmp.name
        raise ValueError("Unsupported video object")

    def upload_video_resumable(path: str) -> str:
        if "facebook" in st.secrets:
            token = st.secrets["facebook"].get("access_token", "").strip()
        else:
            token = st.secrets.get("access_token", "").strip()
        
        act = account.get_id()
        base_url = f"https://graph.facebook.com/v24.0/{act}/advideos"
        file_size = os.path.getsize(path)

        def _post(data, files=None, max_retries=5):
            delays = [0, 2, 4, 8, 12]
            for i, d in enumerate(delays[:max_retries], 1):
                if d: time.sleep(d)
                try:
                    r = requests.post(base_url, data={**data, "access_token": token}, files=files, timeout=180)
                    if r.status_code >= 500: continue
                    j = r.json()
                    if "error" in j and j["error"].get("code") == 390 and i < max_retries: continue
                    if "error" in j: raise RuntimeError(j["error"].get("message"))
                    return j
                except Exception: pass
            raise RuntimeError("Upload failed")

        start_resp = _post({"upload_phase": "start", "file_size": str(file_size), "content_category": "VIDEO_GAMING"})
        sess_id, vid_id = start_resp["upload_session_id"], start_resp["video_id"]
        start_off, end_off = int(start_resp.get("start_offset", 0)), int(start_resp.get("end_offset", 0))

        with open(path, "rb") as f:
            while True:
                if start_off == end_off == file_size: break
                if end_off <= start_off:
                    tr = _post({"upload_phase": "transfer", "upload_session_id": sess_id, "start_offset": str(start_off)})
                    start_off, end_off = int(tr.get("start_offset", start_off)), int(tr.get("end_offset", end_off or file_size))
                    continue
                f.seek(start_off)
                chunk = f.read(end_off - start_off)
                tr = _post({"upload_phase": "transfer", "upload_session_id": sess_id, "start_offset": str(start_off)}, 
                           files={"video_file_chunk": ("chunk.bin", chunk, "application/octet-stream")})
                start_off, end_off = int(tr.get("start_offset", start_off + len(chunk))), int(tr.get("end_offset", end_off))

        try: _post({"upload_phase": "finish", "upload_session_id": sess_id})
        except: pass
        return vid_id

    # Execute Uploads
    persisted = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_save_uploadedfile_tmp, u): u for u in unique_files_to_upload}
        for fut in as_completed(futs):
            try: persisted.append({"name": _fname_any(futs[fut]), "path": fut.result()})
            except: pass

    uploads_map = {} 
    total_up = len(persisted)
    prog = st.progress(0, text=f"Uploading {total_up} videos...") if total_up else None
    
    def _upload_task(item):
        path = item["path"]
        thumb_url = None
        try:
            t_path = extract_thumbnail_from_video(path)
            thumb_url = upload_thumbnail_image(account, t_path)
            try: os.unlink(t_path)
            except: pass
        except: pass
        return {"name": item["name"], "video_id": upload_video_resumable(path), "thumbnail_url": thumb_url}

    done_up = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_upload_task, i): i for i in persisted}
        for fut in as_completed(futs):
            try:
                res = fut.result()
                uploads_map[res["name"]] = res
                done_up += 1
                if prog: prog.progress(int(done_up/total_up*100))
            except: pass
    if prog: prog.empty()

    # ------------------------------------------------------------------
    # 2.5. WAIT FOR VIDEOS TO BE READY (Marketer Mode only)
    # ------------------------------------------------------------------
    if is_marketer_mode:
        st.info(f"✅ {len(uploads_map)}개 비디오 업로드 완료. 광고 생성 시작...")

    # ------------------------------------------------------------------
    # 3. CREATE ADS (Branching Logic)
    # ------------------------------------------------------------------
    results = []
    api_errors = []
    
    ig_actor_id = None
    if try_instagram:
        ig_actor_id = st.session_state.get("ig_actor_id_from_page")

    # Helper: Determine Ad Name
    def _make_name(base):
        return f"{ad_name_prefix.strip()}_{base}" if ad_name_prefix else base

    if is_marketer_mode:
        # ==========================================
        # PATH A: MARKETER MODE (Grouping + Asset Feed)
        # ==========================================
        
        # Parse Text Settings
        raw_text = settings.get("primary_text", "")
        raw_head = settings.get("headline", "")
        cta = settings.get("call_to_action", "INSTALL_MOBILE_APP")
        
        p_texts = [t.strip() for t in raw_text.split('\n') if t.strip()] or [""]
        headlines = [h.strip() for h in raw_head.split('\n') if h.strip()] 

        # facebook_ads.py - PATH A (Marketer Mode)

        # facebook_ads.py - 섹션 3 PATH A

        def _create_marketer_ad(base_name, group_files):
            """Create Flexible ad with 3 video sizes"""
            # 1. Collect video IDs
            vids, thumbs = {}, {}
            for ratio, f_obj in group_files.items():
                fn = _fname_any(f_obj)
                if fn in uploads_map:
                    vids[ratio] = uploads_map[fn]["video_id"]
                    thumbs[ratio] = uploads_map[fn].get("thumbnail_url")
            
            if len(vids) < 3:
                return {"success": False, "error": f"{base_name}: Need all 3 sizes"}

            try:
                # 2. Parse settings
                raw_text = settings.get("primary_text", "")
                raw_head = settings.get("headline", "")
                cta = settings.get("call_to_action", "INSTALL_MOBILE_APP")
                
                p_texts = [t.strip() for t in raw_text.split('\n\n') if t.strip()] or [""]
                headlines = [h.strip() for h in raw_head.split('\n') if h.strip()] or [base_name]

                # 3. Build Asset Feed (실제 Facebook Ads 구조와 일치)
                # 3. Build Asset Feed (실제 Facebook Ads 구조와 일치)
                asset_feed = {
                    "ad_formats": ["SINGLE_VIDEO"],  # <--- [중요] 이 줄을 반드시 추가해야 합니다!
                    "videos": [
                        {"video_id": vids["1x1"], "thumbnail_url": thumbs.get("1x1", "")},
                        {"video_id": vids["9x16"], "thumbnail_url": thumbs.get("9x16", "")},
                        {"video_id": vids["16x9"], "thumbnail_url": thumbs.get("16x9", "")}
                    ],
                    "bodies": [{"text": t} for t in p_texts],
                    "titles": [{"text": h} for h in headlines],
                    "call_to_action_types": [cta],
                    "optimization_type": "DEGREES_OF_FREEDOM",
                }
                
                if store_url:
                    asset_feed["link_urls"] = [{"website_url": store_url}]

                params = {
                    "name": base_name,
                    "asset_feed_spec": asset_feed,
                    "object_story_spec": {"page_id": page_id}
                }
                
                # 4. Create Creative with Retry
                max_retries = 5
                creative = None
                
                for attempt in range(max_retries):
                    try:
                        creative = account.create_ad_creative(fields=[], params=params)
                        break
                        
                    except FacebookRequestError as e:
                        if e.api_error_code() == 100 and e.api_error_subcode() == 1885252 and attempt < max_retries - 1:
                            wait_sec = 15 * (attempt + 1)
                            time.sleep(wait_sec)
                            continue
                        raise
                
                if not creative:
                    return {"success": False, "error": f"{base_name}: Creative failed"}
                
                # 5. Create Ad
                ad = account.create_ad(fields=[], params={
                    "name": _make_name(base_name),
                    "adset_id": adset_id,
                    "creative": {"creative_id": creative["id"]},
                    "status": Ad.Status.active,
                })
                
                return {"success": True, "result": {"name": base_name, "ad_id": ad["id"]}}
                
            except Exception as e:
                return {"success": False, "error": f"{base_name}: {str(e)}"}

        # Run creation for all video groups
        total = len(video_groups)
        prog = st.progress(0, text=f"Creating {total} Flexible ads...")
        done = 0

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(_create_marketer_ad, b, f): b for b, f in video_groups.items()}
            for fut in as_completed(futs):
                res = fut.result()
                done += 1
                prog.progress(int(done/total*100), text=f"Creating ads... {done}/{total}")
                if res["success"]:
                    results.append(res["result"])
                else:
                    api_errors.append(res["error"])
        prog.empty()

    else:
        # ==========================================
        # PATH B: TEST MODE (One File = One Ad)
        # ==========================================
        
        def _create_test_ad(file_data):
            name = file_data["name"]
            vid_id = file_data["video_id"]
            thumb = file_data.get("thumbnail_url")
            
            try:
                # Standard Object Story (Simple)
                vd = {"video_id": vid_id, "title": name, "message": ""}
                if thumb: vd["image_url"] = thumb
                if store_url:
                    vd["call_to_action"] = {"type": "INSTALL_MOBILE_APP", "value": {"link": store_url}}
                
                spec = {"page_id": page_id, "video_data": vd}
                if ig_actor_id: spec["instagram_actor_id"] = ig_actor_id
                
                # Retry logic for IG actor issues
                def _do_create(s):
                    creative = account.create_ad_creative(fields=[], params={"name": name, "object_story_spec": s})
                    ad = account.create_ad(fields=[], params={
                        "name": _make_name(name),
                        "adset_id": adset_id,
                        "creative": {"creative_id": creative["id"]},
                        "status": Ad.Status.active
                    })
                    return ad["id"]

                try:
                    ad_id = _do_create(spec)
                except FacebookRequestError as e:
                    # If IG fails, try without IG actor
                    if "instagram" in str(e).lower() and ig_actor_id:
                        spec.pop("instagram_actor_id", None)
                        ad_id = _do_create(spec)
                    else: raise

                return {"success": True, "result": {"name": name, "ad_id": ad_id}}
            except Exception as e:
                return {"success": False, "error": f"{name}: {e}"}

        # Run Test Creation
        total = len(uploads_map)
        if total:
            prog = st.progress(0, text="Creating Standard Ads...")
            done = 0
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = {ex.submit(_create_test_ad, data): name for name, data in uploads_map.items()}
                for fut in as_completed(futs):
                    res = fut.result()
                    done += 1
                    prog.progress(int(done/total*100))
                    if res["success"]: results.append(res["result"])
                    else: api_errors.append(res["error"])
            prog.empty()

    if api_errors:
        st.error(f"{len(api_errors)} errors during creation:\n" + "\n".join([f"- {e}" for e in api_errors]))

    return results

# --------------------------------------------------------------------
# Ad set planning + creation
# --------------------------------------------------------------------
def _plan_upload(
    account: "AdAccount",
    *,
    campaign_id: str,
    adset_prefix: str,
    page_id: str,
    uploaded_files: list,
    settings: dict,
) -> dict:
    """
    Compute planned ad set name/budget/schedule/ad names from settings
    and available videos (local + remote_videos).
    """
    start_iso = settings.get("start_iso") or next_sat_0900_kst()
    end_iso = settings.get("end_iso")

    n = int(settings.get("suffix_number") or 1)
    
    # Convert to ordinal suffix (1st, 2nd, 3rd, 4th, etc.)
    if n % 10 == 1 and n % 100 != 11:
        suffix_str = f"{n}st"
    elif n % 10 == 2 and n % 100 != 12:
        suffix_str = f"{n}nd"
    elif n % 10 == 3 and n % 100 != 13:
        suffix_str = f"{n}rd"
    else:
        suffix_str = f"{n}th"
    
    # Add "_ai" suffix if AI checkbox is checked
    ai_suffix = "_ai" if settings.get("use_ai", False) else ""

    launch_date_suffix = ""
    if settings.get("add_launch_date"):
        try:
            dt = datetime.fromisoformat(start_iso)
            launch_date_suffix = "_" + dt.strftime("%y%m%d")
        except Exception:
            launch_date_suffix = ""

    adset_name = f"{adset_prefix}{ai_suffix}_{suffix_str}{launch_date_suffix}"

    allowed = {".mp4", ".mpeg4"}
    remote = st.session_state.remote_videos.get(settings.get("game_key", ""), []) or []

    def _name(u):
        return getattr(u, "name", None) or (u.get("name") if isinstance(u, dict) else "")

    def _is_video(u):
        return pathlib.Path(_name(u)).suffix.lower() in allowed

    vids_local = [u for u in (uploaded_files or []) if _is_video(u)]
    vids_all = _dedupe_by_name(vids_local + [rv for rv in remote if _is_video(rv)])

    budget_usd_per_day = compute_budget_from_settings(vids_all, settings)

    ad_name_prefix = (
        settings.get("ad_name_prefix") if settings.get("ad_name_mode") == "Prefix + filename" else None
    )
    ad_names = [make_ad_name(_name(u), ad_name_prefix) for u in vids_all]

    # CRITICAL: Get countries as list (backward compatible)
    countries = settings.get("countries", ["US"])
    if isinstance(countries, str):
        countries = [countries]  # Old format conversion

    return {
        "campaign_id": campaign_id,
        "adset_name": adset_name,
        "countries": countries,  # ← Changed from "country" (string) to "countries" (list)
        "age_min": int(settings.get("age_min", 18)),
        "budget_usd_per_day": int(budget_usd_per_day),
        "start_iso": start_iso,
        "end_iso": end_iso,
        "page_id": page_id,
        "n_videos": len(vids_all),
        "ad_names": ad_names,
        "campaign_name": settings.get("campaign_name"),
        "app_store": settings.get("app_store"),
        "opt_goal_label": settings.get("opt_goal_label"),
    }


def create_creativetest_adset(
    account: "AdAccount",
    *,
    campaign_id: str,
    adset_name: str,
    targeting: dict,
    daily_budget_usd: int,
    start_iso: str,
    optimization_goal: str,
    promoted_object: dict | None = None,
    end_iso: str | None = None,
) -> str:
    """
    Create an ACTIVE ad set for a creative test and return its ID.
    
    Note: Taiwan targeting requires manual business verification in Meta Ads Manager.
    This function will block Taiwan and provide clear guidance to users.
    """
    from facebook_business.adobjects.adset import AdSet
    
    # Check for Taiwan BEFORE creating the ad set
    countries = targeting.get("geo_locations", {}).get("countries", [])
    if "TW" in countries:
        raise RuntimeError(
            "❌ **Taiwan (TW) Targeting Not Supported via API**\n\n"
            "Meta requires manual business verification for Taiwan ads.\n\n"
            "**Why?** Taiwan has strict advertising disclosure laws requiring:\n"
            "• Business registration verification\n"
            "• Advertiser identity disclosure\n"
            "• Tax ID (統一編號) submission\n\n"
            "**Solutions:**\n"
            "1. ✅ Remove Taiwan from your country selection\n"
            "2. ✅ Complete Taiwan verification in Meta Business Settings first\n"
            "3. ✅ Create Taiwan ad sets manually in Meta Ads Manager\n"
            "4. ✅ Target Taiwan in a separate campaign\n\n"
            "All other countries (US, JP, KR, etc.) work fine via API."
        )

    params = {
        "name": adset_name,
        "campaign_id": campaign_id,
        "daily_budget": dollars_to_minor(daily_budget_usd),
        "billing_event": AdSet.BillingEvent.impressions,
        "optimization_goal": getattr(
            AdSet.OptimizationGoal,
            optimization_goal.lower(),
            AdSet.OptimizationGoal.app_installs,
        ),
        "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
        "targeting": targeting,
        "status": AdSet.Status.active,
        "start_time": start_iso,
    }

    if end_iso:
        params["end_time"] = end_iso
    if promoted_object:
        params["promoted_object"] = promoted_object

    adset = account.create_ad_set(fields=[], params=params)
    return adset["id"]

# --------------------------------------------------------------------
# Per-game mapping + main entry
# --------------------------------------------------------------------
FB_GAME_MAPPING: Dict[str, Dict[str, Any]] = {
    "XP HERO": {
        "account_id": "act_692755193188182",
        "campaign_id": "120218934861590118",
        "campaign_name": "weaponrpg_aos_facebook_us_creativetest",
        "adset_prefix": "weaponrpg_aos_facebook_us_creativetest",
        "page_id_key": "page_id_xp",
    },
    "Dino Universe": {
        "account_id": "act_1400645283898971",
        "campaign_id": "120203672340130431",
        "campaign_name": "ageofdinosaurs_aos_facebook_us_test_6th+",
        "adset_prefix": "ageofdinosaurs_aos_facebook_us_test",
        "page_id_key": "page_id_dino",
    },
    "Snake Clash": {
        "account_id": "act_837301614677763",
        "campaign_id": "120201313657080615",
        "campaign_name": "linkedcubic_aos_facebook_us_test_14th above",
        "adset_prefix": "linkedcubic_aos_facebook_us_test",
        "page_id_key": "page_id_snake",
    },
    "Pizza Ready": {
        "account_id": "act_939943337267153",
        "campaign_id": "120200161907250465",
        "campaign_name": "pizzaidle_aos_facebook_us_test_12th+",
        "adset_prefix": "pizzaidle_aos_facebook_us_test",
        "page_id_key": "page_id_pizza",
    },
    "Cafe Life": {
        "account_id": "act_1425841598550220",
        "campaign_id": "120231530818850361",
        "campaign_name": "cafelife_aos_facebook_us_creativetest",
        "adset_prefix": "cafelife_aos_facebook_us_creativetest",
        "page_id_key": "page_id_cafe",
    },
    "Suzy's Restaurant": {
        "account_id": "act_953632226485498",
        "campaign_id": "120217220153800643",
        "campaign_name": "suzyrest_aos_facebook_us_creativetest",
        "adset_prefix": "suzyrest_aos_facebook_us_creativetest",
        "page_id_key": "page_id_suzy",
    },
    "Office Life": {
        "account_id": "act_733192439468531",
        "campaign_id": "120228464454680636",
        "campaign_name": "corporatetycoon_aos_facebook_us_creativetest",
        "adset_prefix": "corporatetycoon_aos_facebook_us_creativetest",
        "page_id_key": "page_id_office",
    },
    "Lumber Chopper": {
        "account_id": "act_1372896617079122",
        "campaign_id": "120224569359980144",
        "campaign_name": "lumberchopper_aos_facebook_us_creativetest",
        "adset_prefix": "lumberchopper_aos_facebook_us_creativetest",
        "page_id_key": "page_id_lumber",
    },
    "Burger Please": {
        "account_id": "act_3546175519039834",
        "campaign_id": "120200361364790724",
        "campaign_name": "burgeridle_aos_facebook_us_test_30th+",
        "adset_prefix": "burgeridle_aos_facebook_us_test",
        "page_id_key": "page_id_burger",
    },
    "Prison Life": {
        "account_id": "act_510600977962388",
        "campaign_id": "120212520882120614",
        "campaign_name": "prison_aos_facebook_us_install_test",
        "adset_prefix": "prison_aos_facebook_us_install_test",
        "page_id_key": "page_id_prison",
    },
}

def upload_to_facebook(
    game_name: str,
    uploaded_files: list,
    settings: dict,
    *,
    simulate: bool = False,
) -> dict:
    """
    Main entry: create ad set + ads for a game using current settings.
    If simulate=True, just return the plan (no writes).
    """
    if game_name not in FB_GAME_MAPPING:
        raise ValueError(f"No FB mapping configured for game: {game_name}")

    cfg = FB_GAME_MAPPING[game_name]
    account = init_fb_from_secrets(cfg["account_id"])

    page_id_key = cfg.get("page_id_key")
    
    # 1. Try looking inside the [facebook] section (correct for your secrets.toml)
    if "facebook" in st.secrets and page_id_key in st.secrets["facebook"]:
        page_id = st.secrets["facebook"][page_id_key]
    # 2. Fallback: Try looking at the root level
    elif page_id_key in st.secrets:
        page_id = st.secrets[page_id_key]
    # 3. Error if not found in either
    else:
        raise RuntimeError(f"Missing {page_id_key!r} in st.secrets['facebook'] or st.secrets root for game {game_name}")

    # Validate page and capture IG actor
    page_check = validate_page_binding(account, page_id)
    ig_actor_id_from_page = page_check.get("instagram_business_account_id")

    # Extra safety: ensure page_id != ad account id
    try:
        acct_num = account.get_id().replace("act_", "")
        pid = str(page_id)
        if pid in (acct_num, f"act_{acct_num}"):
            raise RuntimeError(
                "Configured PAGE_ID equals the Ad Account ID. "
                "Set st.secrets[page_id_*] to your Facebook Page ID (NOT 'act_...')."
            )
        from facebook_business.adobjects.page import Page
        _probe = Page(pid).api_get(fields=["id", "name"])
        if not _probe or not _probe.get("id"):
            raise RuntimeError("Provided PAGE_ID is not readable with this token.")
    except Exception as _pg_err:
        raise RuntimeError(
            f"Page validation failed for PAGE_ID={page_id}. "
            "Use a real Facebook Page ID and ensure asset access from this ad account/token."
        ) from _pg_err

    # Build plan (no writes yet)
    settings = dict(settings or {})
    settings["campaign_name"] = cfg.get("campaign_name")
    plan = _plan_upload(
        account=account,
        campaign_id=cfg["campaign_id"],
        adset_prefix=cfg["adset_prefix"],
        page_id=str(page_id),
        uploaded_files=uploaded_files,
        settings=settings,
    )
    
    
    if simulate:
        return plan

    # CRITICAL FIX: Pass countries LIST instead of single country
    targeting = build_targeting_from_settings(
        countries=plan["countries"],  # ← Changed from country=plan["country"]
        age_min=plan["age_min"],
        settings=settings,
    )
    

    # Optimization goal + promoted_object
    opt_goal_label = settings.get("opt_goal_label") or "앱 설치수 극대화"
    opt_goal_api = OPT_GOAL_LABEL_TO_API.get(opt_goal_label, "APP_INSTALLS")

    store_label = settings.get("app_store")
    store_url = (settings.get("store_url") or "").strip()
    fb_app_id = (settings.get("fb_app_id") or "").strip()

    if store_url:
        store_url = sanitize_store_url(store_url)

    promoted_object = None
    if opt_goal_api in ("APP_INSTALLS", "APP_EVENTS", "VALUE"):
        if not store_url:
            raise RuntimeError(
                "App objective selected. Please enter a valid store URL in Settings "
                "(Google Play or App Store)."
            )
        promoted_object = {
            "object_store_url": store_url,
            **({"application_id": fb_app_id} if fb_app_id else {}),
        }

    adset_id = create_creativetest_adset(
        account=account,
        campaign_id=cfg["campaign_id"],
        adset_name=plan["adset_name"],
        targeting=targeting,
        daily_budget_usd=plan["budget_usd_per_day"],
        start_iso=plan["start_iso"],
        optimization_goal=opt_goal_api,
        promoted_object=promoted_object,
        end_iso=plan.get("end_iso"),
    )

    if not adset_id:
        raise RuntimeError(
            "Ad set was not created (no ID returned). Check the error above and fix settings/permissions."
        )
    

    ad_name_prefix = (
        settings.get("ad_name_prefix") if settings.get("ad_name_mode") == "Prefix + filename" else None
    )

    try:
        st.session_state["ig_actor_id_from_page"] = ig_actor_id_from_page
    except Exception:
        pass

    upload_videos_create_ads(
        account=account,
        page_id=str(page_id),
        adset_id=adset_id,
        uploaded_files=uploaded_files,
        ad_name_prefix=settings.get("dco_creative_name"), # 이름 통일됨
        store_url=store_url,
        try_instagram=True,
        settings=settings, 
    )

    plan["adset_id"] = adset_id
    return plan

# --------------------------------------------------------------------
# Settings panel UI (right column)
# --------------------------------------------------------------------
def render_facebook_settings_panel(container, game: str, idx: int) -> None:
    """
    Render the Facebook settings panel for a single game and save
    values into st.session_state.settings[game].
    
    Includes validation for Taiwan and other compliance countries.
    """
    _ensure_settings_state()
    cur = st.session_state.settings.get(game, {})

    with container:
        st.markdown(f"#### {game} Facebook Settings")

        suffix_number = st.number_input(
            "광고 세트 접미사 n(…_nth)",
            min_value=1,
            step=1,
            value=int(cur.get("suffix_number", 1)),
            help="Ad set will be named as <campaign_name>_<n>th or <campaign_name>_<n>th_YYMMDD",
            key=f"suffix_{idx}",
        )

        use_ai = st.checkbox(
            "AI",
            value=bool(cur.get("use_ai", False)),
            key=f"use_ai_{idx}",
            help="체크 시 광고 세트 이름에 '_ai'가 추가됩니다. 예: ..._creativetest_ai_nth",
        )

        app_store = st.selectbox(
            "모바일 앱 스토어",
            ["Google Play 스토어", "Apple App Store"],
            index=0 if cur.get("app_store", "Google Play 스토어") == "Google Play 스토어" else 1,
            key=f"appstore_{idx}",
        )

        fb_app_id = st.text_input(
            "Facebook App ID",
            value=cur.get("fb_app_id", ""),
            key=f"fbappid_{idx}",
            help="설치 추적을 연결하려면 FB App ID를 입력하세요(선택).",
        )
        
        store_url = st.text_input(
            "구글 스토어 URL",
            value=cur.get("store_url", ""),
            key=f"storeurl_{idx}",
            help="예) https://play.google.com/store/apps/details?id=... (쿼리스트링/트래킹 파라미터 제거 권장)",
        )

        opt_goal_label = st.selectbox(
            "성과 목표",
            list(OPT_GOAL_LABEL_TO_API.keys()),
            index=list(OPT_GOAL_LABEL_TO_API.keys()).index(cur.get("opt_goal_label", "앱 설치수 극대화")),
            key=f"optgoal_{idx}",
        )

        st.caption("기여 설정: 클릭 1일(기본), 참여한 조회/조회 없음 — Facebook에서 고정/제한될 수 있습니다.")

        budget_per_video_usd = st.number_input(
            "영상 1개당 일일 예산 (USD)",
            min_value=1,
            value=int(cur.get("budget_per_video_usd", 10)),
            key=f"budget_per_video_{idx}",
            help="총 일일 예산 = (업로드/선택된 영상 수) × 이 값",
        )

        default_start_iso = next_sat_0900_kst()
        start_iso = st.text_input(
            "시작 날짜/시간 (ISO, KST)",
            value=cur.get("start_iso", default_start_iso),
            help="예: 2025-11-15T00:00:00+09:00 (종료일은 자동으로 꺼지지 않도록 설정하지 않습니다)",
            key=f"start_{idx}",
        )

        launch_date_example = ""
        try:
            dt_preview = datetime.fromisoformat(start_iso.strip())
            launch_date_example = dt_preview.strftime("%y%m%d")
        except Exception:
            launch_date_example = ""

        add_launch_date = st.checkbox(
            "Launch 날짜 추가",
            value=bool(cur.get("add_launch_date", False)),
            key=f"add_launch_date_{idx}",
            help=(
                f"시작 날짜/시간의 날짜(YYMMDD)를 광고 세트 이름 끝에 추가합니다. "
                f"예: …_{int(suffix_number)}th_{launch_date_example or 'YYMMDD'}"
            ),
        )

        st.markdown("#### 타겟팅 설정")
        
        # Get previously saved countries (could be string or list)
        saved_countries = cur.get("countries", ["US"])
        if isinstance(saved_countries, str):
            saved_countries = [saved_countries]
        
        # Create default selection (convert codes to names)
        default_selection = [
            COUNTRY_CODE_TO_NAME.get(code, code) 
            for code in saved_countries 
            if code in COUNTRY_CODE_TO_NAME.values()
        ]
        
        # If no valid defaults, use US
        if not default_selection:
            default_selection = ["United States"]
        
        selected_country_names = st.multiselect(
            "타겟 국가 (여러 개 선택 가능)",
            options=sorted(COUNTRY_OPTIONS.keys()),
            default=default_selection,
            key=f"countries_{idx}",
            help="Meta 광고를 게재할 국가를 선택하세요. 여러 국가 선택 가능합니다."
        )
        
        # Convert selected names back to country codes
        selected_country_codes = [
            COUNTRY_OPTIONS[name] for name in selected_country_names
        ]
        
        # Show warning if no country selected
        if not selected_country_codes:
            st.warning("⚠️ 최소 1개 국가를 선택해주세요.")
            selected_country_codes = ["US"]  # Fallback
        
        # Check for compliance/blocked countries
        compliance_info = requires_special_compliance(selected_country_codes)
        
        # Show blocking error for Taiwan or other blocked countries
        if compliance_info["has_blocked"]:
            blocked_details = compliance_info["blocked_details"]
            
            st.error(
                "🚫 **다음 국가는 API를 통해 타겟팅할 수 없습니다:**\n\n" +
                "\n".join(
                    f"**{details['name']}**\n"
                    f"- 이유: {details['reason']}\n"
                    f"- 상세: {details['details']}\n"
                    for c, details in blocked_details.items()
                ) +
                "\n\n**해결방법:**\n"
                "1. 해당 국가를 선택 해제하고 다시 시도하세요\n"
                "2. 또는 Meta Ads Manager에서 수동으로 광고 세트를 생성하세요"
            )
            
            # Auto-remove blocked countries from selection
            original_count = len(selected_country_codes)
            selected_country_codes = [
                c for c in selected_country_codes 
                if c not in compliance_info["blocked"]
            ]
            
            removed_count = original_count - len(selected_country_codes)
            if removed_count > 0:
                removed_names = [
                    COUNTRY_CODE_TO_NAME.get(c, c) 
                    for c in compliance_info["blocked"]
                ]
                st.warning(f"⚠️ 자동 제거됨: {', '.join(removed_names)}")
            
            if not selected_country_codes:
                selected_country_codes = ["US"]
                st.info("ℹ️ 기본값으로 United States가 선택되었습니다.")
        
        # Show info for supported special compliance countries (future expansion)
        if compliance_info["has_special"]:
            special_names = [
                COUNTRY_CODE_TO_NAME.get(c, c) 
                for c in compliance_info["countries"]
            ]
            st.info(
                f"ℹ️ **규제 준수 알림**\n\n"
                f"선택한 국가에 특별 규정 준수가 필요합니다: {', '.join(special_names)}\n\n"
                f"다음 설정이 자동으로 적용됩니다:\n" +
                "\n".join(
                    f"- {COUNTRY_CODE_TO_NAME.get(c, c)}: {t}" 
                    for c, t in compliance_info["types"].items()
                )
            )
        
        # Display final selected countries
        final_names = [COUNTRY_CODE_TO_NAME.get(c, c) for c in selected_country_codes]
        st.success(f"✅ 선택된 국가: {', '.join(final_names)}")

        age_min = st.number_input(
            "최소 연령",
            min_value=13,
            value=int(cur.get("age_min", 18)),
            key=f"age_{idx}",
        )

        os_choice = st.selectbox(
            "Target OS",
            ["Both", "Android only", "iOS only"],
            index={"Both": 0, "Android only": 1, "iOS only": 2}[cur.get("os_choice", "Android only")],
            key=f"os_choice_{idx}",
        )

        if os_choice in ("Both", "Android only"):
            min_android_label = st.selectbox(
                "Min Android version",
                list(ANDROID_OS_CHOICES.keys()),
                index=list(ANDROID_OS_CHOICES.keys()).index(cur.get("min_android_label", "6.0+")),
                key=f"min_android_{idx}",
            )
        else:
            min_android_label = "None (any)"

        if os_choice in ("Both", "iOS only"):
            min_ios_label = st.selectbox(
                "Min iOS version",
                list(IOS_OS_CHOICES.keys()),
                index=list(IOS_OS_CHOICES.keys()).index(cur.get("min_ios_label", "None (any)")),
                key=f"min_ios_{idx}",
            )
        else:
            min_ios_label = "None (any)"

        min_android_os_token = (
            ANDROID_OS_CHOICES[min_android_label]
            if os_choice in ("Both", "Android only")
            else None
        )
        min_ios_os_token = (
            IOS_OS_CHOICES[min_ios_label]
            if os_choice in ("Both", "iOS only")
            else None
        )

        ad_name_mode = st.selectbox(
            "Ad name",
            ["Use video filename", "Prefix + filename"],
            index=1 if cur.get("ad_name_mode") == "Prefix + filename" else 0,
            key=f"adname_mode_{idx}",
        )
        
        ad_name_prefix = ""
        if ad_name_mode == "Prefix + filename":
            ad_name_prefix = st.text_input(
                "Ad name prefix",
                value=cur.get("ad_name_prefix", ""),
                key=f"adname_prefix_{idx}",
            )

        # Save settings with validated countries
        st.session_state.settings[game] = {
            "suffix_number": int(suffix_number),
            "use_ai": bool(use_ai),
            "add_launch_date": bool(add_launch_date),
            "app_store": app_store,
            "fb_app_id": fb_app_id.strip(),
            "store_url": store_url.strip(),
            "opt_goal_label": opt_goal_label,
            "budget_per_video_usd": int(budget_per_video_usd),
            "start_iso": start_iso.strip(),
            "countries": selected_country_codes,  # Validated and cleaned list
            "age_min": int(age_min),
            "os_choice": os_choice,
            "min_android_label": min_android_label,
            "min_ios_label": min_ios_label,
            "min_android_os_token": min_android_os_token,
            "min_ios_os_token": min_ios_os_token,
            "ad_name_mode": ad_name_mode,
            "ad_name_prefix": ad_name_prefix.strip(),
            "game_key": game,
        }