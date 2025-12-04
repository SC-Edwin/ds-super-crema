"""Unity Ads helpers for Creative 자동 업로드 Streamlit app."""

from __future__ import annotations

from typing import Dict, List, Any
from datetime import datetime, timedelta, timezone
import logging
import pathlib
import re
import os
import json

import time
import requests
import streamlit as st

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Unity config from secrets.toml
# --------------------------------------------------------------------
unity_cfg = st.secrets.get("unity", {}) or {}

UNITY_ORG_ID_DEFAULT = unity_cfg.get("organization_id", "")
UNITY_CLIENT_ID_DEFAULT = unity_cfg.get("client_id", "")
UNITY_CLIENT_SECRET_DEFAULT = unity_cfg.get("client_secret", "")
UNITY_AUTH_HEADER_DEFAULT = unity_cfg.get("authorization_header", "")

_raw_game_ids = unity_cfg.get("game_ids", {}) or {}
_raw_campaign_ids = unity_cfg.get("campaign_ids", {}) or {}

UNITY_GAME_IDS: Dict[str, str] = {
    str(game): str(title_id)
    for game, title_id in _raw_game_ids.items()
}

UNITY_CAMPAIGN_IDS: Dict[str, List[str]] = {}
for game, val in _raw_campaign_ids.items():
    if isinstance(val, dict) and "ids" in val:
        UNITY_CAMPAIGN_IDS[str(game)] = [str(x) for x in (val.get("ids") or [])]
    elif isinstance(val, (list, tuple)):
        UNITY_CAMPAIGN_IDS[str(game)] = [str(x) for x in val]
    elif isinstance(val, str):
        UNITY_CAMPAIGN_IDS[str(game)] = [val]

UNITY_BASE_URL = "https://services.api.unity.com/advertise/v1"

# --------------------------------------------------------------------
# Session-state helpers
# --------------------------------------------------------------------
def _ensure_unity_settings_state() -> None:
    if "unity_settings" not in st.session_state:
        st.session_state.unity_settings = {}

def get_unity_settings(game: str) -> Dict:
    _ensure_unity_settings_state()
    return st.session_state.unity_settings.get(game, {})

# --------------------------------------------------------------------
# Campaign Auto-Start Check
# --------------------------------------------------------------------

def _unity_get_campaign(*, org_id: str, title_id: str, campaign_id: str) -> dict:
    """Fetch campaign details including auto-start settings."""
    path = f"organizations/{org_id}/apps/{title_id}/campaigns/{campaign_id}"
    return _unity_get(path)


def check_campaign_auto_start(*, org_id: str, title_id: str, campaign_id: str) -> dict:
    """
    Check if a Unity campaign has auto-start enabled.
    
    Returns dict with:
        - campaign_id: str
        - campaign_name: str
        - auto_start_enabled: bool
        - auto_start_mode: str (if applicable, e.g., "ALWAYS_ON")
        - status: str (ACTIVE, PAUSED, etc.)
    """
    try:
        campaign = _unity_get_campaign(
            org_id=org_id, 
            title_id=title_id, 
            campaign_id=campaign_id
        )
        
        # Extract relevant fields
        result = {
            "campaign_id": campaign.get("id", campaign_id),
            "campaign_name": campaign.get("name", "Unknown"),
            "status": campaign.get("status", "UNKNOWN"),
            "auto_start_enabled": False,
            "auto_start_mode": None,
        }
        
        # Check for auto-start configuration
        # Unity API may use different field names depending on version
        # Common field names: autoStart, auto_start, autoStartEnabled, etc.
        
        if "autoStart" in campaign:
            auto_start = campaign["autoStart"]
            if isinstance(auto_start, bool):
                result["auto_start_enabled"] = auto_start
            elif isinstance(auto_start, dict):
                result["auto_start_enabled"] = auto_start.get("enabled", False)
                result["auto_start_mode"] = auto_start.get("mode")
        
        elif "auto_start" in campaign:
            result["auto_start_enabled"] = bool(campaign["auto_start"])
        
        elif "autoStartEnabled" in campaign:
            result["auto_start_enabled"] = bool(campaign["autoStartEnabled"])
        
        # Check delivery settings which may contain auto-start info
        if "deliverySettings" in campaign:
            delivery = campaign["deliverySettings"]
            if "autoStart" in delivery:
                result["auto_start_enabled"] = bool(delivery["autoStart"])
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to check campaign auto-start: {e}")
        return {
            "campaign_id": campaign_id,
            "campaign_name": "Unknown",
            "status": "ERROR",
            "auto_start_enabled": None,
            "auto_start_mode": None,
            "error": str(e)
        }


def verify_campaigns_auto_start(campaigns_config: dict) -> dict:
    """
    Check auto-start status for all configured campaigns.
    
    Args:
        campaigns_config: Dict like {
            "game_name": {
                "org_id": "...",
                "title_id": "...", 
                "campaign_ids": ["id1", "id2"]
            }
        }
    
    Returns:
        Dict with results per game/campaign
    """
    results = {}
    
    for game_name, config in campaigns_config.items():
        org_id = config.get("org_id")
        title_id = config.get("title_id")
        campaign_ids = config.get("campaign_ids", [])
        
        if not all([org_id, title_id, campaign_ids]):
            results[game_name] = {"error": "Missing configuration"}
            continue
        
        game_results = []
        for campaign_id in campaign_ids:
            check_result = check_campaign_auto_start(
                org_id=org_id,
                title_id=title_id,
                campaign_id=campaign_id
            )
            game_results.append(check_result)
        
        results[game_name] = game_results
    
    return results


# --------------------------------------------------------------------
# UI Helper - Display Auto-Start Status
# --------------------------------------------------------------------

def display_auto_start_status(game: str, unity_settings: dict) -> None:
    """
    Display auto-start status in Streamlit UI for a game's Unity campaign.
    Call this in render_unity_settings_panel or after campaign selection.
    """
    import streamlit as st
    
    org_id = unity_settings.get("org_id") or UNITY_ORG_ID_DEFAULT
    title_id = unity_settings.get("title_id") or UNITY_GAME_IDS.get(game, "")
    campaign_id = unity_settings.get("campaign_id") or ""
    
    if not all([org_id, title_id, campaign_id]):
        return
    
    try:
        status = check_campaign_auto_start(
            org_id=org_id,
            title_id=title_id,
            campaign_id=campaign_id
        )
        
        if "error" in status:
            st.warning(f"⚠️ Could not check auto-start: {status['error']}")
            return
        
        # Display status with appropriate icon
        auto_start = status.get("auto_start_enabled")
        
        if auto_start is None:
            st.info("ℹ️ Auto-start status: Unknown (check Unity dashboard)")
        elif auto_start:
            st.success(f"✅ Auto-start: **ENABLED** ({status.get('campaign_name')})")
            if status.get("auto_start_mode"):
                st.caption(f"Mode: {status['auto_start_mode']}")
        else:
            st.error(f"❌ Auto-start: **DISABLED** ({status.get('campaign_name')})")
            st.warning("⚠️ New creative packs will NOT automatically start delivery!")
        
        # Show campaign status
        campaign_status = status.get("status", "UNKNOWN")
        status_color = {
            "ACTIVE": "🟢",
            "PAUSED": "🟡", 
            "ARCHIVED": "⚫",
        }.get(campaign_status, "⚪")
        
        st.caption(f"{status_color} Campaign Status: {campaign_status}")
        
    except Exception as e:
        st.warning(f"Could not check auto-start status: {e}")


# --------------------------------------------------------------------
# Bulk Check for All Games
# --------------------------------------------------------------------

def check_all_games_auto_start() -> None:
    """
    Streamlit UI component to check auto-start for all configured games.
    Can be added as a sidebar option or in settings.
    """
    import streamlit as st
    
    if st.button("🔍 Check All Campaigns Auto-Start Status"):
        with st.spinner("Checking campaigns..."):
            # Build config from UNITY_GAME_IDS and UNITY_CAMPAIGN_IDS
            campaigns_config = {}
            
            for game in UNITY_GAME_IDS.keys():
                title_id = UNITY_GAME_IDS.get(game)
                campaign_ids = UNITY_CAMPAIGN_IDS.get(game, [])
                
                if title_id and campaign_ids:
                    campaigns_config[game] = {
                        "org_id": UNITY_ORG_ID_DEFAULT,
                        "title_id": title_id,
                        "campaign_ids": campaign_ids
                    }
            
            results = verify_campaigns_auto_start(campaigns_config)
            
            # Display results
            st.subheader("Unity Auto-Start Status Report")
            
            for game, game_results in results.items():
                with st.expander(f"📊 {game}", expanded=False):
                    if isinstance(game_results, dict) and "error" in game_results:
                        st.error(f"Error: {game_results['error']}")
                        continue
                    
                    for campaign in game_results:
                        if "error" in campaign:
                            st.warning(f"❌ {campaign.get('campaign_name', 'Unknown')}: {campaign['error']}")
                            continue
                        
                        auto_start = campaign.get("auto_start_enabled")
                        name = campaign.get("campaign_name", "Unknown")
                        status = campaign.get("status", "UNKNOWN")
                        
                        icon = "✅" if auto_start else "❌"
                        status_text = "ENABLED" if auto_start else "DISABLED"
                        
                        st.write(f"{icon} **{name}**")
                        st.caption(f"Auto-Start: {status_text} | Status: {status}")
                        
                        if not auto_start:
                            st.warning("⚠️ Auto-start is disabled for this campaign!")


# --------------------------------------------------------------------
# Integration with render_unity_settings_panel
# --------------------------------------------------------------------

def render_unity_settings_panel_with_autostart_check(right_col, game: str, idx: int) -> None:
    """
    Enhanced version of render_unity_settings_panel that includes auto-start check.
    
    Add this after the existing settings are rendered:
    """
    # ... (existing render_unity_settings_panel code) ...
    
    # Add at the end, before saving to session_state:
    with right_col:
        st.markdown("---")
        st.markdown("#### Campaign Status")
        
        # Get current settings
        unity_settings = st.session_state.unity_settings.get(game, {})
        
        # Display auto-start status
        display_auto_start_status(game, unity_settings)
        
        # Optional: Quick link to Unity dashboard
        campaign_id = unity_settings.get("campaign_id", "")
        if campaign_id:
            unity_dashboard_url = f"https://operate.dashboard.unity3d.com/advertising/campaigns/{campaign_id}"
            st.markdown(f"[🔗 Open in Unity Dashboard]({unity_dashboard_url})")


# --------------------------------------------------------------------
# Example Usage in Main Upload Flow
# --------------------------------------------------------------------

def upload_unity_creatives_to_campaign_with_check(*, game: str, videos: list, settings: dict) -> dict:
    """
    Enhanced version that checks auto-start before uploading.
    
    This is a wrapper around the existing upload_unity_creatives_to_campaign function.
    """
    import streamlit as st
    
    # Check auto-start first
    org_id = settings.get("org_id") or UNITY_ORG_ID_DEFAULT
    title_id = settings.get("title_id") or ""
    campaign_id = settings.get("campaign_id") or ""
    
    if all([org_id, title_id, campaign_id]):
        auto_start_status = check_campaign_auto_start(
            org_id=org_id,
            title_id=title_id,
            campaign_id=campaign_id
        )
        
        if not auto_start_status.get("auto_start_enabled"):
            st.warning(
                "⚠️ **Auto-Start is DISABLED** for this campaign!\n\n"
                "Creative packs will be uploaded but will NOT automatically start delivery. "
                "You'll need to manually enable them in the Unity dashboard."
            )
            
            # Optional: Add confirmation
            if not st.checkbox("I understand and want to proceed", key=f"autostart_confirm_{game}"):
                st.stop()
    
    # Proceed with regular upload
    from .unity_ads import upload_unity_creatives_to_campaign
    return upload_unity_creatives_to_campaign(game=game, videos=videos, settings=settings)

def check_all_games_auto_start() -> None:
    """
    Streamlit UI component to check auto-start for all configured games.
    """
    import streamlit as st
    
    with st.spinner("Checking campaigns..."):
        # Build config from UNITY_GAME_IDS and UNITY_CAMPAIGN_IDS
        campaigns_config = {}
        
        for game in UNITY_GAME_IDS.keys():
            title_id = UNITY_GAME_IDS.get(game)
            campaign_ids = UNITY_CAMPAIGN_IDS.get(game, [])
            
            if title_id and campaign_ids:
                campaigns_config[game] = {
                    "org_id": UNITY_ORG_ID_DEFAULT,
                    "title_id": title_id,
                    "campaign_ids": campaign_ids
                }
        
        results = verify_campaigns_auto_start(campaigns_config)
        
        # Display results
        st.subheader("Unity Auto-Start Status Report")
        
        for game, game_results in results.items():
            with st.expander(f"📊 {game}", expanded=False):
                if isinstance(game_results, dict) and "error" in game_results:
                    st.error(f"Error: {game_results['error']}")
                    continue
                
                for campaign in game_results:
                    if "error" in campaign:
                        st.warning(f"❌ {campaign.get('campaign_name', 'Unknown')}: {campaign['error']}")
                        continue
                    
                    auto_start = campaign.get("auto_start_enabled")
                    name = campaign.get("campaign_name", "Unknown")
                    status = campaign.get("status", "UNKNOWN")
                    
                    icon = "✅" if auto_start else "❌"
                    status_text = "ENABLED" if auto_start else "DISABLED"
                    
                    st.write(f"{icon} **{name}**")
                    st.caption(f"Auto-Start: {status_text} | Status: {status}")
                    
                    if not auto_start:
                        st.warning("⚠️ Auto-start is disabled for this campaign!")


def verify_campaigns_auto_start(campaigns_config: dict) -> dict:
    """
    Check auto-start status for all configured campaigns.
    """
    results = {}
    
    for game_name, config in campaigns_config.items():
        org_id = config.get("org_id")
        title_id = config.get("title_id")
        campaign_ids = config.get("campaign_ids", [])
        
        if not all([org_id, title_id, campaign_ids]):
            results[game_name] = {"error": "Missing configuration"}
            continue
        
        game_results = []
        for campaign_id in campaign_ids:
            check_result = check_campaign_auto_start(
                org_id=org_id,
                title_id=title_id,
                campaign_id=campaign_id
            )
            game_results.append(check_result)
        
        results[game_name] = game_results
    
    return results

# --------------------------------------------------------------------
# Unity settings UI
# --------------------------------------------------------------------
def render_unity_settings_panel(right_col, game: str, idx: int) -> None:
    _ensure_unity_settings_state()

    with right_col:
        st.markdown(f"#### {game} Unity Settings")
        cur = st.session_state.unity_settings.get(game, {})

        secret_title_id = str(UNITY_GAME_IDS.get(game, ""))
        secret_campaign_ids = UNITY_CAMPAIGN_IDS.get(game, []) or []
        default_campaign_id_val = secret_campaign_ids[0] if secret_campaign_ids else ""

        title_key = f"unity_title_{idx}"
        campaign_key = f"unity_campaign_{idx}"

        if st.session_state.get(title_key) == "" and secret_title_id:
            st.session_state[title_key] = secret_title_id
        if (not secret_campaign_ids and st.session_state.get(campaign_key) == "" and default_campaign_id_val):
            st.session_state[campaign_key] = default_campaign_id_val

        current_title_id = cur.get("title_id") or st.session_state.get(title_key) or secret_title_id
        current_campaign_id = cur.get("campaign_id") or st.session_state.get(campaign_key) or default_campaign_id_val

        default_org_id = cur.get("org_id") or UNITY_ORG_ID_DEFAULT
        default_client_id = cur.get("client_id") or UNITY_CLIENT_ID_DEFAULT
        default_client_secret = cur.get("client_secret") or UNITY_CLIENT_SECRET_DEFAULT

        unity_title_id = current_title_id
        st.session_state[title_key] = unity_title_id
        unity_campaign_id = current_campaign_id
        st.session_state[campaign_key] = unity_campaign_id
        unity_org_id = default_org_id
        st.session_state[f"unity_org_{idx}"] = unity_org_id
        unity_client_id = default_client_id
        st.session_state[f"unity_client_id_{idx}"] = unity_client_id
        unity_client_secret = default_client_secret
        st.session_state[f"unity_client_secret_{idx}"] = unity_client_secret

        unity_daily_budget = 0

        st.markdown("#### Playable 선택")
        drive_playables = [
            v for v in (st.session_state.remote_videos.get(game, []) if "remote_videos" in st.session_state else [])
            if "playable" in (v.get("name") or "").lower()
        ]
        drive_options = [p["name"] for p in drive_playables]
        prev_drive_playable = cur.get("selected_playable", "")

        selected_drive_playable = st.selectbox(
            "Drive에서 가져온 플레이어블",
            options=["(선택 안 함)"] + drive_options,
            index=(drive_options.index(prev_drive_playable) + 1) if prev_drive_playable in drive_options else 0,
            key=f"unity_playable_{idx}",
        )
        chosen_drive_playable = selected_drive_playable if selected_drive_playable != "(선택 안 함)" else ""

        existing_labels: List[str] = ["(선택 안 함)"]
        existing_id_by_label: Dict[str, str] = {}
        prev_existing_label = cur.get("existing_playable_label", "")

        try:
            org_for_list = (unity_org_id or UNITY_ORG_ID_DEFAULT).strip()
            title_for_list = (unity_title_id or secret_title_id).strip()

            if org_for_list and title_for_list:
                # STRICT FILTER: Playables only
                playable_creatives = _unity_list_playable_creatives(org_id=org_for_list, title_id=title_for_list)
                for cr in playable_creatives:
                    cr_id = str(cr.get("id") or "")
                    cr_name = cr.get("name") or "(no name)"
                    cr_type = cr.get("type", "")
                    if not cr_id: continue
                    label = f"{cr_name} ({cr_type}) [{cr_id}]"
                    existing_labels.append(label)
                    existing_id_by_label[label] = cr_id
        except Exception as e:
            st.info(f"Unity playable 목록을 불러오지 못했습니다: {e}")

        try:
            existing_default_idx = existing_labels.index(prev_existing_label)
        except ValueError:
            existing_default_idx = 0

        selected_existing_label = st.selectbox(
            "Unity에 이미 있는 playable",
            options=existing_labels,
            index=existing_default_idx,
            key=f"unity_existing_playable_{idx}",
        )

        existing_playable_id = ""
        if selected_existing_label != "(선택 안 함)":
            existing_playable_id = existing_id_by_label.get(selected_existing_label, "")

        st.warning(
            "Unity creative pack은 **9:16 영상 1개 + 16:9 영상 1개 + 1개의 playable** 조합을 기준으로 생성됩니다."
        )

        st.session_state.unity_settings[game] = {
            "title_id": (unity_title_id or "").strip(),
            "campaign_id": (unity_campaign_id or "").strip(),
            "org_id": (unity_org_id or "").strip(),
            "daily_budget_usd": int(unity_daily_budget),
            "selected_playable": chosen_drive_playable,
            "existing_playable_id": existing_playable_id,
            "existing_playable_label": selected_existing_label,
        }
        
        # === NEW: Add Auto-Start Check ===
        st.markdown("---")
        st.markdown("#### 📊 Campaign Status")
        
        unity_settings = st.session_state.unity_settings.get(game, {})
        display_auto_start_status(game, unity_settings)
# --------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------
ASIA_SEOUL = timezone(timedelta(hours=9))

def next_sat_0000_kst(today: datetime | None = None) -> str:
    now = (today or datetime.now(ASIA_SEOUL)).astimezone(ASIA_SEOUL)
    base = now.replace(hour=0, minute=0, second=0, microsecond=0)
    days_until_sat = (5 - base.weekday()) % 7 or 7
    start_dt = (base + timedelta(days=days_until_sat)).replace(hour=9, minute=0)
    return start_dt.isoformat()

def unity_creative_name_from_filename(filename: str) -> str:
    stem = pathlib.Path(filename).stem
    m = re.search(r"(\d{3})(?!.*\d)", stem)
    code = m.group(1) if m else "000"
    return f"video{code}"

# --------------------------------------------------------------------
# API Helpers
# --------------------------------------------------------------------
def _unity_headers() -> dict:
    if not UNITY_AUTH_HEADER_DEFAULT:
        raise RuntimeError("unity.authorization_header is missing in secrets.toml")
    return {"Authorization": UNITY_AUTH_HEADER_DEFAULT, "Content-Type": "application/json"}

def _unity_post(path: str, json_body: dict) -> dict:
    url = f"{UNITY_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    last_error: Exception | None = None

    for attempt in range(8):
        try:
            resp = requests.post(url, headers=_unity_headers(), json=json_body, timeout=60)
            
            if resp.status_code == 429:
                detail = resp.text[:400]
                if "quota" in detail.lower():
                    raise RuntimeError(f"Unity Quota Exceeded (STOPPING): {detail}")
                
                sleep_sec = 2 ** (attempt + 1)
                logger.warning(f"Unity 429 Rate Limit (attempt {attempt+1}/8). Sleeping {sleep_sec}s...")
                time.sleep(sleep_sec)
                continue

            if not resp.ok:
                raise RuntimeError(f"Unity POST {path} failed ({resp.status_code}): {resp.text[:400]}")

            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed: {e}. Retrying...")
            time.sleep(2)
            last_error = e

    raise last_error or RuntimeError(f"Unity POST {path} failed after retries.")

def _unity_put(path: str, json_body: dict) -> dict:
    url = f"{UNITY_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    resp = requests.put(url, headers=_unity_headers(), json=json_body, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Unity PUT {path} failed ({resp.status_code}): {resp.text[:400]}")
    return resp.json()

def _unity_get(path: str, params: dict | None = None) -> dict:
    url = f"{UNITY_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    resp = requests.get(url, headers=_unity_headers(), params=params or {}, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Unity GET {path} failed ({resp.status_code}): {resp.text[:400]}")
    return resp.json()

def _unity_delete(path: str) -> None:
    url = f"{UNITY_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    resp = requests.delete(url, headers=_unity_headers(), timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Unity DELETE {path} failed ({resp.status_code}): {resp.text[:400]}")

# --------------------------------------------------------------------
# Creative Helpers
# --------------------------------------------------------------------
def _unity_list_assigned_creative_packs(*, org_id: str, title_id: str, campaign_id: str) -> List[dict]:
    path = f"organizations/{org_id}/apps/{title_id}/campaigns/{campaign_id}/assigned-creative-packs"
    
    # 1. No limit param (as it caused 400 error)
    meta = _unity_get(path)

    # 2. Return the correct list structure
    if isinstance(meta, list): return meta
    if isinstance(meta, dict):
        # FIX: Check 'results' which is standard for this API
        if isinstance(meta.get("results"), list): return meta["results"]
        if isinstance(meta.get("items"), list): return meta["items"]
        if isinstance(meta.get("data"), list): return meta["data"]
        
        for v in meta.values():
            if isinstance(v, list): return v

    return []

def _unity_assign_creative_pack(*, org_id: str, title_id: str, campaign_id: str, creative_pack_id: str) -> None:
    path = f"organizations/{org_id}/apps/{title_id}/campaigns/{campaign_id}/assigned-creative-packs"
    _unity_post(path, {"id": creative_pack_id})

def _unity_unassign_creative_pack(*, org_id: str, title_id: str, campaign_id: str, assigned_creative_pack_id: str) -> None:
    path = f"organizations/{org_id}/apps/{title_id}/campaigns/{campaign_id}/assigned-creative-packs/{assigned_creative_pack_id}"
    _unity_delete(path)

def _unity_unassign_with_retry(*, org_id: str, title_id: str, campaign_id: str, assigned_creative_pack_id: str, max_retries: int = 3) -> None:
    """Unassign with exponential backoff on rate limit."""
    for attempt in range(max_retries):
        try:
            _unity_unassign_creative_pack(
                org_id=org_id,
                title_id=title_id,
                campaign_id=campaign_id,
                assigned_creative_pack_id=assigned_creative_pack_id
            )
            return  # Success
        except RuntimeError as e:
            if "429" in str(e) and attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)  # 2, 4, 8 seconds
                logger.warning(f"Rate limit hit, waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                raise  # Give up after retries

def _unity_get_creative(*, org_id: str, title_id: str, creative_id: str) -> dict:
    path = f"organizations/{org_id}/apps/{title_id}/creatives/{creative_id}"
    return _unity_get(path)

def _unity_create_video_creative(*, org_id: str, title_id: str, video_path: str, name: str, language: str = "en") -> str:
    if not os.path.isfile(video_path):
        raise RuntimeError(f"Video path does not exist: {video_path!r}")

    # FIX: Use original name for fileName, not temp file path
    display_filename = name
    if not display_filename.lower().endswith(".mp4"):
        display_filename += ".mp4"

    creative_info = {
        "name": name, 
        "language": language, 
        "video": {"fileName": display_filename}
    }
    
    url = f"{UNITY_BASE_URL.rstrip('/')}/organizations/{org_id}/apps/{title_id}/creatives"
    headers = {"Authorization": UNITY_AUTH_HEADER_DEFAULT}

    for attempt in range(8):
        try:
            with open(video_path, "rb") as f:
                files = {
                    "creativeInfo": (None, json.dumps(creative_info), "application/json"),
                    "videoFile": (display_filename, f, "video/mp4"),
                }
                resp = requests.post(url, headers=headers, files=files, timeout=300)

            if resp.status_code == 429:
                detail = (resp.text or "")[:400].lower()
                if "quota" in detail:
                    raise RuntimeError(f"Unity Quota Exceeded (STOPPING): {detail}")
                sleep_sec = 5 * (attempt + 1)
                time.sleep(sleep_sec)
                continue

            if not resp.ok:
                raise RuntimeError(f"Unity create creative failed ({resp.status_code}): {resp.text[:400]}")

            body = resp.json()
            return str(body.get("id") or body.get("creativeId"))
        except Exception as e:
            if "Quota Exceeded" in str(e): raise e
            time.sleep(5)

    raise RuntimeError("Unity create creative failed after multiple retries.")

def _unity_create_playable_creative(*, org_id: str, title_id: str, playable_path: str, name: str, language: str = "en") -> str:
    if not os.path.isfile(playable_path):
        raise RuntimeError(f"Playable path does not exist: {playable_path!r}")

    file_name = os.path.basename(playable_path)
    creative_info = {"name": name, "language": language, "playable": {"fileName": file_name}}
    url = f"{UNITY_BASE_URL.rstrip('/')}/organizations/{org_id}/apps/{title_id}/creatives"
    headers = {"Authorization": UNITY_AUTH_HEADER_DEFAULT}

    for attempt in range(8):
        try:
            with open(playable_path, "rb") as f:
                files = {
                    "creativeInfo": (None, json.dumps(creative_info), "application/json"),
                    "playableFile": (file_name, f, "text/html"),
                }
                resp = requests.post(url, headers=headers, files=files, timeout=300)

            if resp.status_code == 429:
                time.sleep(3 * (attempt + 1))
                continue

            if not resp.ok:
                raise RuntimeError(f"Unity create playable failed ({resp.status_code}): {resp.text[:400]}")

            body = resp.json()
            return str(body.get("id") or body.get("creativeId"))
        except Exception as e:
            time.sleep(3)

    raise RuntimeError("Unity create playable creative failed after retries.")

def _unity_create_creative_pack(*, org_id: str, title_id: str, pack_name: str, creative_ids: List[str], pack_type: str = "video") -> str:
    clean_ids = [str(x) for x in creative_ids if x]
    
    if len(clean_ids) < 2:
        raise RuntimeError(f"Not enough creative IDs to create a pack: {clean_ids}")

    payload = {
        "name": pack_name,
        "creativeIds": clean_ids,
        "type": pack_type,
    }

    path = f"organizations/{org_id}/apps/{title_id}/creative-packs"
    meta = _unity_post(path, payload)
    
    creative_pack_id = meta.get("id") or meta.get("creativePackId")
    if not creative_pack_id:
        raise RuntimeError(f"Unity creative pack response missing id: {meta}")

    return str(creative_pack_id)

def _unity_list_playable_creatives(*, org_id: str, title_id: str) -> List[dict]:
    path = f"organizations/{org_id}/apps/{title_id}/creatives"
    meta = _unity_get(path)

    items: List[dict] = []
    if isinstance(meta, list): items = meta
    elif isinstance(meta, dict):
        if isinstance(meta.get("items"), list): items = meta["items"]
        elif isinstance(meta.get("data"), list): items = meta["data"]
        else:
            for v in meta.values():
                if isinstance(v, list): items.extend(v)

    playables: List[dict] = []
    for cr in items:
        if not isinstance(cr, dict): continue
        t = (cr.get("type") or "").lower()
        if "playable" in t or "cpe" in t:
             playables.append(cr)

    return playables

# --------------------------------------------------------------------
# Main Helpers
# --------------------------------------------------------------------

def upload_unity_creatives_to_campaign(*, game: str, videos: List[Dict[str, Any]], settings: Dict[str, Any]) -> Dict[str, Any]:
    title_id = (settings.get("title_id") or "").strip() or str(UNITY_GAME_IDS.get(game, ""))
    campaign_id = (settings.get("campaign_id") or "").strip()
    if not campaign_id:
        ids_for_game = UNITY_CAMPAIGN_IDS.get(game) or []
        if ids_for_game: campaign_id = str(ids_for_game[0])
    
    org_id = (settings.get("org_id") or "").strip() or UNITY_ORG_ID_DEFAULT
    
    if not all([title_id, campaign_id, org_id]):
        raise RuntimeError("Unity Settings Missing for upload.")

    start_iso = next_sat_0000_kst()
    new_creative_pack_ids: List[str] = []
    errors: List[str] = []

    # 1. PLAYABLE HANDLING
    playable_name = settings.get("selected_playable") or ""
    existing_playable_id = settings.get("existing_playable_id") or ""
    playable_creative_id: str | None = None

    if playable_name:
        playable_item = next((v for v in (videos or []) if v.get("name") == playable_name), None)
        if playable_item:
            try:
                playable_creative_id = _unity_create_playable_creative(
                    org_id=org_id, title_id=title_id, playable_path=playable_item["path"], name=playable_name
                )
            except Exception as e:
                errors.append(f"Playable creation failed: {e}")
                playable_creative_id = None

    if not playable_creative_id and existing_playable_id:
        playable_creative_id = str(existing_playable_id)

    # Validate Playable ID
    if playable_creative_id:
        try:
            logger.info(f"Validating Playable ID: {playable_creative_id}")
            p_details = _unity_get_creative(org_id=org_id, title_id=title_id, creative_id=playable_creative_id)
            p_type = (p_details.get("type") or "").lower()
            
            if "playable" not in p_type and "cpe" not in p_type:
                error_msg = f"CRITICAL: Playable ID ({playable_creative_id}) is type '{p_type}'. Must be 'playable'."
                errors.append(error_msg)
                return {"game": game, "campaign_id": campaign_id, "errors": errors, "creative_ids": []}
        except Exception as e:
            errors.append(f"Could not validate Playable ID: {e}")
            return {"game": game, "campaign_id": campaign_id, "errors": errors, "creative_ids": []}
    else:
        errors.append("No Playable End Card selected.")
        return {"game": game, "campaign_id": campaign_id, "errors": errors, "creative_ids": []}

    # 2. VIDEO PAIRING
    # 2. VIDEO PAIRING
    subjects: dict[str, list[dict]] = {}
    for v in videos or []:
        n = v.get("name") or ""
        # IMPORTANT: Exclude files named "playable" from being uploaded as VIDEO creatives
        if "playable" in n.lower(): continue 
        base = n.split("_")[0] 
        subjects.setdefault(base, []).append(v)

    total_pairs = len(subjects)
    progress_bar = None
    processed_count = 0
    
    if total_pairs > 0:
        progress_bar = st.progress(0, text=f"Creative Pack 생성 준비 중 (0/{total_pairs})...")

    # 3. PROCESSING LOOP
    for base, items in subjects.items():
        time.sleep(2) # Throttle API calls

        portrait = next((x for x in items if "1080x1920" in (x.get("name") or "")), None)
        landscape = next((x for x in items if "1920x1080" in (x.get("name") or "")), None)

        if not portrait or not landscape:
            errors.append(f"{base}: Missing Portrait or Landscape video.")
            processed_count += 1
            if progress_bar:
                pct = int(processed_count / total_pairs * 100)
                progress_bar.progress(pct, text=f"Skipping {base} (Missing video) - {processed_count}/{total_pairs}")
            continue
        
        # --- CLEAN NAMING LOGIC ---
        clean_base = base.replace("_", "")
        raw_p_name = playable_name if playable_name else settings.get("existing_playable_label", "").split(" ")[0]
        clean_p = pathlib.Path(raw_p_name).stem.replace("_unityads", "").replace("_", "")
        final_pack_name = f"{clean_base}_{clean_p}"
        # --------------------------

        try:
            if progress_bar:
                progress_bar.progress(
                    int(processed_count / total_pairs * 100), 
                    text=f"Uploading videos for {base} ({processed_count + 1}/{total_pairs})..."
                )

            # Create Videos
            p_id = _unity_create_video_creative(
                org_id=org_id, title_id=title_id, video_path=portrait["path"], name=portrait["name"]
            )
            time.sleep(1) # Small gap between uploads
            l_id = _unity_create_video_creative(
                org_id=org_id, title_id=title_id, video_path=landscape["path"], name=landscape["name"]
            )

            pack_creatives = [p_id, l_id, playable_creative_id]
            
            # Create Pack "video+playable"
            pack_id = _unity_create_creative_pack(
                org_id=org_id,
                title_id=title_id,
                pack_name=final_pack_name, 
                creative_ids=pack_creatives,
                pack_type="video+playable"
            )
            new_creative_pack_ids.append(pack_id)

        except Exception as e:
            msg = str(e)
            if "Quota Exceeded" in msg:
                errors.append(f"FATAL: {msg}")
                break 
            logger.exception(f"Unity pack creation failed for {base}")
            errors.append(f"{base}: {msg}")

        finally:
            processed_count += 1
            if progress_bar:
                pct = int(processed_count / total_pairs * 100)
                progress_bar.progress(pct, text=f"Completed {processed_count}/{total_pairs} packs")

    if progress_bar: 
        progress_bar.empty()

    return {
        "game": game,
        "campaign_id": campaign_id,
        "start_iso": start_iso,
        "creative_ids": new_creative_pack_ids,
        "errors": errors,
        "removed_ids": [],
    }

def apply_unity_creative_packs_to_campaign(*, game: str, creative_pack_ids: List[str], settings: Dict[str, Any]) -> Dict[str, Any]:
    if not creative_pack_ids:
        raise RuntimeError("No creative pack IDs to apply.")

    title_id = (settings.get("title_id") or "").strip() or str(UNITY_GAME_IDS.get(game, ""))
    campaign_id = (settings.get("campaign_id") or "").strip()
    org_id = (settings.get("org_id") or "").strip() or UNITY_ORG_ID_DEFAULT

    if not all([title_id, campaign_id, org_id]):
         raise RuntimeError("Unity settings missing for apply step.")

    removed_ids: List[str] = []
    assigned_packs: List[str] = []
    errors: List[str] = []

    # --- PROGRESS BAR FOR APPLY STEP ---
    progress_bar = st.progress(0, text="Fetching existing assignments...")

    # 1. Unassign existing (Loop to handle pagination)
    max_loops = 20 # Safety limit
    loop_count = 0
    
    while loop_count < max_loops:
        try:
            assigned = _unity_list_assigned_creative_packs(org_id=org_id, title_id=title_id, campaign_id=campaign_id)
            if not assigned:
                break
                
            total_unassign = len(assigned)
            loop_count += 1
            
            for idx, item in enumerate(assigned):
                assigned_id = item.get("id") or item.get("assignedCreativePackId")
                if assigned_id:
                    # Update progress bar
                    progress_bar.progress(
                        int((idx + 1) / max(total_unassign, 1) * 50), 
                        text=f"Unassigning batch {loop_count}: {idx + 1}/{total_unassign}..."
                    )
                    
                    try:
                        _unity_unassign_with_retry(  # 새 함수 사용
                            org_id=org_id,
                            title_id=title_id,
                            campaign_id=campaign_id,
                            assigned_creative_pack_id=str(assigned_id)
                        )
                        removed_ids.append(str(assigned_id))
                        time.sleep(1.0)  # 0.2 → 1.0으로 증가
                    except Exception as e:
                        errors.append(f"Unassign error {assigned_id}: {e}")
            
            # Short sleep between pages
            time.sleep(1)
            
        except Exception as e:
            errors.append(f"List assigned error: {e}")
            break

    # 2. Assign new
    total_assign = len(creative_pack_ids)
    count_a = 0
    
    for pack_id in creative_pack_ids:
        count_a += 1
        pct = 50 + int(count_a / max(total_assign, 1) * 50)
        progress_bar.progress(pct, text=f"Assigning new packs {count_a}/{total_assign}...")
        
        try:
            _unity_assign_creative_pack(org_id=org_id, title_id=title_id, campaign_id=campaign_id, creative_pack_id=str(pack_id))
            assigned_packs.append(str(pack_id))
            time.sleep(0.5) 
        except Exception as e:
            errors.append(f"Assign error {pack_id}: {e}")

    progress_bar.empty()

    return {
        "game": game,
        "campaign_id": campaign_id,
        "assigned_packs": assigned_packs,
        "removed_assignments": removed_ids,
        "errors": errors,
    }

