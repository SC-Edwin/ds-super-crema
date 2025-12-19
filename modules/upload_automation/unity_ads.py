"""Unity Ads helpers for Creative 자동 업로드 Streamlit app."""

from __future__ import annotations

from typing import Dict, List, Any
from datetime import datetime, timedelta, timezone
import logging
import pathlib
import re
import os
import json
import hashlib

import time
import requests
import streamlit as st
from modules.upload_automation import devtools

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Unity config from secrets.toml
# --------------------------------------------------------------------
# --------------------------------------------------------------------
# Unity config from secrets.toml
# --------------------------------------------------------------------
# --------------------------------------------------------------------
# Unity config from secrets.toml
# --------------------------------------------------------------------
unity_cfg = st.secrets.get("unity", {}) or {}

UNITY_ORG_ID_DEFAULT = unity_cfg.get("organization_id", "")
UNITY_CLIENT_ID_DEFAULT = unity_cfg.get("client_id", "")
UNITY_CLIENT_SECRET_DEFAULT = unity_cfg.get("client_secret", "")
UNITY_AUTH_HEADER_DEFAULT = unity_cfg.get("authorization_header", "")

# Raw sections from secrets.toml
_raw_game_ids      = unity_cfg.get("game_ids", {}) or {}       # per-game app ids + maybe campaign-sets (XP HERO)
_raw_campaign_sets = unity_cfg.get("campaign_sets", {}) or {}  # per-game campaign-set IDs (Dino, Snake, Pizza…)
_raw_campaign_ids  = unity_cfg.get("campaign_ids", {}) or {}   # per-game campaign IDs

# --------------------------------------------------------------------
# App (title) IDs & Campaign-set IDs per game (multi-platform)
# --------------------------------------------------------------------
# We want:
#   UNITY_APP_IDS_ALL["XP HERO"]           = {"aos": "500230240", "ios": "500236189"}
#   UNITY_CAMPAIGN_SET_IDS_ALL["XP HERO"]  = {"aos": "67d0...",    "ios": "683d..."}
#
UNITY_APP_IDS_ALL: Dict[str, Dict[str, str]] = {}
UNITY_CAMPAIGN_SET_IDS_ALL: Dict[str, Dict[str, str]] = {}

# Defaults for operator mode:
# - UNITY_GAME_IDS[game]: default app (title) ID (AOS if present)
# - UNITY_CAMPAIGN_SET_IDS_DEFAULT[game]: default campaign-set ID (AOS if present)
UNITY_GAME_IDS: Dict[str, str] = {}
UNITY_CAMPAIGN_SET_IDS_DEFAULT: Dict[str, str] = {}

# Campaign IDs per game (operator mode default uses AOS entry)
UNITY_CAMPAIGN_IDS_ALL: Dict[str, Dict[str, List[str]]] = {}
UNITY_CAMPAIGN_IDS: Dict[str, List[str]] = {}

UNITY_BASE_URL = "https://services.api.unity.com/advertise/v1"

# --------------------------------------------------------------------
# Build derived maps from unity_cfg (for defaults)
# --------------------------------------------------------------------

# 1) App (title) IDs & campaign-set IDs per game (multi-platform)
for game, val in _raw_game_ids.items():
    gname = str(game)
    app_ids: Dict[str, str] = {}
    camp_sets: Dict[str, str] = {}

    if isinstance(val, dict):
        for k, v in val.items():
            key = str(k)
            sval = str(v)

            # App (title) IDs
            if key in ("aos_app_id", "android_app_id"):
                app_ids["aos"] = sval
            elif key in ("ios_app_id", "ios_appid"):
                app_ids["ios"] = sval

            # Campaign-set IDs in game_ids (XP HERO style)
            elif key in ("aos", "aos_campaign_set"):
                camp_sets["aos"] = sval
            elif key in ("ios", "ios_campaign_set"):
                camp_sets["ios"] = sval

    elif isinstance(val, str):
        # Old-style single ID: treat as AOS app ID
        app_ids["aos"] = str(val)

    if app_ids:
        UNITY_APP_IDS_ALL[gname] = app_ids
        # default app ID for operator mode (prefer AOS)
        UNITY_GAME_IDS[gname] = app_ids.get("aos") or next(iter(app_ids.values()))

    if camp_sets:
        existing = UNITY_CAMPAIGN_SET_IDS_ALL.get(gname, {})
        existing.update(camp_sets)
        UNITY_CAMPAIGN_SET_IDS_ALL[gname] = existing
        if gname not in UNITY_CAMPAIGN_SET_IDS_DEFAULT:
            UNITY_CAMPAIGN_SET_IDS_DEFAULT[gname] = camp_sets.get("aos") or next(
                iter(camp_sets.values())
            )

# 2) Campaign-set IDs from unity.campaign_sets (Dino, Snake, Pizza, ...)
for game, val in _raw_campaign_sets.items():
    gname = str(game)
    if not isinstance(val, dict):
        continue

    camp_sets: Dict[str, str] = {}
    for k, v in val.items():
        key = str(k)
        sval = str(v)
        if key in ("aos", "aos_campaign_set"):
            camp_sets["aos"] = sval
        elif key in ("ios", "ios_campaign_set"):
            camp_sets["ios"] = sval

    if not camp_sets:
        continue

    existing = UNITY_CAMPAIGN_SET_IDS_ALL.get(gname, {})
    existing.update(camp_sets)
    UNITY_CAMPAIGN_SET_IDS_ALL[gname] = existing

    if gname not in UNITY_CAMPAIGN_SET_IDS_DEFAULT:
        UNITY_CAMPAIGN_SET_IDS_DEFAULT[gname] = camp_sets.get("aos") or next(
            iter(camp_sets.values())
        )

# 3) Campaign IDs per game (operator-mode default uses AOS entry)
for game, val in _raw_campaign_ids.items():
    gname = str(game)
    if isinstance(val, dict):
        plat_map: Dict[str, List[str]] = {}
        for plat, v in val.items():
            if isinstance(v, (list, tuple)):
                plat_map[str(plat)] = [str(x) for x in v]
            elif isinstance(v, str):
                plat_map[str(plat)] = [v]
        if plat_map:
            UNITY_CAMPAIGN_IDS_ALL[gname] = plat_map
            # Default: prefer "aos", else first platform
            UNITY_CAMPAIGN_IDS[gname] = plat_map.get("aos") or next(iter(plat_map.values()))
    elif isinstance(val, (list, tuple)):
        lst = [str(x) for x in val]
        UNITY_CAMPAIGN_IDS_ALL[gname] = {"default": lst}
        UNITY_CAMPAIGN_IDS[gname] = lst
    elif isinstance(val, str):
        UNITY_CAMPAIGN_IDS_ALL[gname] = {"default": [val]}
        UNITY_CAMPAIGN_IDS[gname] = [val]
# --------------------------------------------------------------------
# Internal helpers to build & use maps
# --------------------------------------------------------------------


def _normalize_game_name(name: str) -> str:
    """Normalize game name for tolerant matching (remove spaces, lowercase)."""
    return "".join(str(name).split()).lower()



def get_unity_app_id(game: str, platform: str = "aos") -> str:
    """
    Return Unity app (title) ID for a given game + platform.
    """
    game_ids_section = unity_cfg.get("game_ids")
    
    # ❌ 삭제: isinstance(game_ids_section, dict) 체크
    if not game_ids_section:
        raise RuntimeError(f"❌ unity.game_ids is missing")
    
    # Exact key match
    if game in game_ids_section:
        block = game_ids_section[game]
        # ❌ 삭제: isinstance(block, dict) 체크
        
        key = "aos_app_id" if platform == "aos" else "ios_app_id"
        val = block.get(key)
        
        if val is not None:
            result = str(val).strip()
            if result:
                return result
    
    # Normalized key match
    target = _normalize_game_name(game)
    for k in game_ids_section:
        if _normalize_game_name(k) == target:
            v = game_ids_section[k]
            key = "aos_app_id" if platform == "aos" else "ios_app_id"
            val = v.get(key) if hasattr(v, 'get') else None
            
            if val is not None:
                result = str(val).strip()
                if result:
                    return result
    
    # Fallback
    legacy = UNITY_GAME_IDS.get(game)
    if legacy:
        return str(legacy).strip()
    
    raise RuntimeError(
        f"❌ No app_id for '{game}' [{platform}]\n"
        f"Available: {list(game_ids_section.keys()) if hasattr(game_ids_section, 'keys') else 'N/A'}"
    )

# ━━━ unity_ads.py에서 이 함수를 완전히 교체하세요 ━━━

def get_unity_campaign_set_id(game: str, platform: str = "aos") -> str:
    """
    Return Unity campaign-set ID for a given game + platform.
    """
    plat = "aos" if platform == "aos" else "ios"
    
    # 1) Check unity.game_ids (XP HERO)
    game_ids_section = unity_cfg.get("game_ids")
    
    if game_ids_section:
        # Exact key
        if game in game_ids_section:
            block = game_ids_section[game]
            
            # Try direct key: 'aos' or 'ios'
            val = block.get(plat) if hasattr(block, 'get') else None
            if val is not None:
                result = str(val).strip()
                if result:
                    return result
            
            # Try alternative: 'aos_campaign_set' or 'ios_campaign_set'
            val = block.get(f"{plat}_campaign_set") if hasattr(block, 'get') else None
            if val is not None:
                result = str(val).strip()
                if result:
                    return result
        
        # Normalized key
        target = _normalize_game_name(game)
        for k in game_ids_section:
            if _normalize_game_name(k) == target:
                v = game_ids_section[k]
                if hasattr(v, 'get'):
                    val = v.get(plat) or v.get(f"{plat}_campaign_set")
                    if val is not None:
                        result = str(val).strip()
                        if result:
                            return result
    
    # 2) Check unity.campaign_sets (other games)
    cs_section = unity_cfg.get("campaign_sets")
    
    if cs_section:
        # Exact key
        if game in cs_section:
            block = cs_section[game]
            
            if hasattr(block, 'get'):
                val = block.get(plat) or block.get(f"{plat}_campaign_set")
                if val is not None:
                    result = str(val).strip()
                    if result:
                        return result
        
        # Normalized key
        target = _normalize_game_name(game)
        for k in cs_section:
            if _normalize_game_name(k) == target:
                v = cs_section[k]
                if hasattr(v, 'get'):
                    val = v.get(plat) or v.get(f"{plat}_campaign_set")
                    if val is not None:
                        result = str(val).strip()
                        if result:
                            return result
    
    raise RuntimeError(
        f"❌ No campaign-set ID for '{game}' [{platform}]"
    )

def debug_unity_ids(game: str = "XP HERO") -> None:
    """
    Streamlit debug helper: shows exactly what unity_cfg contains
    and what IDs we resolve for a given game.
    """
    st.write("unity_cfg.game_ids keys:", list((unity_cfg.get("game_ids") or {}).keys()))
    st.write("unity_cfg.campaign_sets keys:", list((unity_cfg.get("campaign_sets") or {}).keys()))

    game_ids_section = unity_cfg.get("game_ids") or {}
    cs_section = unity_cfg.get("campaign_sets") or {}

    block_g = game_ids_section.get(game)
    block_cs = cs_section.get(game)

    st.write("game_ids block (raw, exact):", block_g)
    st.write("campaign_sets block (raw, exact):", block_cs)

    try:
        aos_app = get_unity_app_id(game, "aos")
    except Exception as e:
        aos_app = f"ERROR: {e}"

    try:
        aos_cs = get_unity_campaign_set_id(game, "aos")
    except Exception as e:
        aos_cs = f"ERROR: {e}"

    st.write("get_unity_app_id(game, 'aos'):", aos_app)
    st.write("get_unity_campaign_set_id(game, 'aos'):", aos_cs)



def _ensure_unity_settings_state() -> None:
    if "unity_settings" not in st.session_state:
        st.session_state.unity_settings = {}

def get_unity_settings(game: str) -> Dict:
    _ensure_unity_settings_state()
    return st.session_state.unity_settings.get(game, {})

# --------------------------------------------------------------------
# Unity settings UI
# --------------------------------------------------------------------
def render_unity_settings_panel(right_col, game: str, idx: int, is_marketer: bool = False) -> None:
    _ensure_unity_settings_state()

    with right_col:
        st.markdown(f"#### {game} Unity Settings")
        cur = st.session_state.unity_settings.get(game, {})

        # Test Mode: campaign_set_id(aos)를 title_id로 사용
        # Marketer Mode: 플랫폼에 따라 app_id를 title_id로 사용
        if is_marketer:
            # Marketer Mode: 플랫폼 선택에 따라 app_id 사용
            # settings에서 platform 정보를 가져오거나 기본값 "aos" 사용
            platform = cur.get("platform", "aos")
            try:
                secret_title_id = get_unity_app_id(game, platform)
            except Exception as e:
                logger.warning(f"Failed to get app ID for {game} ({platform}): {e}")
                # Fallback: UNITY_GAME_IDS 사용
                secret_title_id = str(UNITY_GAME_IDS.get(game, ""))
        else:
            # Test Mode: campaign_set_id (aos) 사용
            try:
                secret_title_id = get_unity_campaign_set_id(game, "aos")
            except Exception as e:
                logger.warning(f"Failed to get campaign set ID for {game}: {e}")
                secret_title_id = ""
        
        secret_campaign_ids = UNITY_CAMPAIGN_IDS.get(game, []) or []
        default_campaign_id_val = secret_campaign_ids[0] if secret_campaign_ids else ""

        title_key = f"unity_title_{idx}"
        campaign_key = f"unity_campaign_{idx}"

        if st.session_state.get(title_key) == "" and secret_title_id:
            st.session_state[title_key] = secret_title_id
        if secret_campaign_ids and not st.session_state.get(campaign_key) and default_campaign_id_val:
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
            campaign_for_list = (unity_campaign_id or "").strip()

            if org_for_list:
                # Marketer: App 레벨 전체 조회
                # Operator (Test Mode): Campaign Set ID를 title_id로 사용하여 App 레벨에서 playable 조회
                if is_marketer:
                    if not title_for_list:
                        st.warning("⚠️ Title ID가 설정되지 않았습니다.")
                        playable_creatives = []
                    else:
                        playable_creatives = _unity_list_playable_creatives(
                            org_id=org_for_list, 
                            title_id=title_for_list
                        )
                else:
                    # Test Mode: campaign set ID를 title_id로 사용
                    # unity.game_ids의 aos 값이 campaign set ID이므로 이를 title_id로 사용
                    try:
                        campaign_set_id = get_unity_campaign_set_id(game, "aos")
                        logger.info(f"Test Mode: Using campaign set ID as title_id: {campaign_set_id} for game: {game}")
                        with st.expander("🔍 Debug: Unity Playable 조회 정보", expanded=False):
                            st.write(f"**Game:** {game}")
                            st.write(f"**Org ID:** {org_for_list}")
                            st.write(f"**Campaign Set ID (title_id로 사용):** {campaign_set_id}")
                        playable_creatives = _unity_list_playable_creatives(
                            org_id=org_for_list,
                            title_id=campaign_set_id
                        )
                        logger.info(f"Found {len(playable_creatives)} playables using campaign set ID")
                        if len(playable_creatives) == 0:
                            st.info(f"ℹ️ Campaign Set ID `{campaign_set_id}`에서 playable을 찾지 못했습니다. Unity에 playable이 등록되어 있는지 확인하세요.")
                    except Exception as e:
                        logger.warning(f"Failed to get campaign set ID for {game}, error: {e}")
                        devtools.record_exception("Unity Campaign Set ID lookup failed", e)
                        st.error("❌ Campaign Set ID 조회 실패")
                        # Fallback: 기존 title_id 사용 (있는 경우)
                        if title_for_list:
                            logger.info(f"Fallback: Using title_id: {title_for_list}")
                            st.info(f"⚠️ Fallback: Title ID `{title_for_list}`를 사용합니다.")
                            try:
                                playable_creatives = _unity_list_playable_creatives(
                                    org_id=org_for_list,
                                    title_id=title_for_list
                                )
                            except Exception as e2:
                                logger.warning(f"Fallback also failed: {e2}")
                                playable_creatives = []
                                st.error(f"❌ Fallback도 실패: {e2}")
                        else:
                            playable_creatives = []
                            st.warning("⚠️ Title ID도 설정되지 않아 playable을 조회할 수 없습니다.")
                
                if playable_creatives:
                    for cr in playable_creatives:
                        cr_id = str(cr.get("id") or "")
                        cr_name = cr.get("name") or "(no name)"
                        cr_type = cr.get("type", "")
                        if not cr_id: continue
                        label = f"{cr_name} ({cr_type}) [{cr_id}]"
                        existing_labels.append(label)
                        existing_id_by_label[label] = cr_id
                else:
                    logger.info(f"No playables found for game: {game}, org: {org_for_list}")
        except Exception as e:
            error_msg = f"Unity playable 목록을 불러오지 못했습니다: {e}"
            logger.exception(error_msg)
            devtools.record_exception("Unity playable list load failed", e)
            st.error("❌ Unity playable 목록을 불러오지 못했습니다.")

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

# --------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------

def _get_upload_state_key(game: str, campaign_id: str) -> str:
    """Generate a unique key for tracking upload state per game/campaign."""
    return f"unity_upload_state_{game}_{campaign_id}"

def _init_upload_state(game: str, campaign_id: str, videos: List[Dict]) -> Dict:
    """
    Initialize or load upload state for resumability.
    
    State structure:
    {
        "video_creatives": {
            "video_filename.mp4": "creative_id_12345" or None
        },
        "playable_creative": "creative_id_67890" or None,
        "creative_packs": {
            "pack_name": "pack_id_abc" or None
        },
        "completed_packs": ["pack_id_1", "pack_id_2"],
        "total_expected": 10
    }
    """
    key = _get_upload_state_key(game, campaign_id)
    
    if key not in st.session_state:
        # Initialize new state
        state = {
            "video_creatives": {},
            "playable_creative": None,
            "creative_packs": {},
            "completed_packs": [],
            "total_expected": 0
        }
        
        # Pre-populate video names
        for v in videos or []:
            name = v.get("name", "")
            if "playable" not in name.lower():
                state["video_creatives"][name] = None
        
        st.session_state[key] = state
    
    return st.session_state[key]


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

def _extract_video_part_from_base(base: str) -> str:
    """
    Extract video part from base name (e.g., 'video001' from 'video001_1080x1920').
    Returns the part that starts with 'video' followed by digits.
    """
    # Try to find 'video' followed by digits
    m = re.search(r"(video\d+)", base, re.IGNORECASE)
    if m:
        return m.group(1).lower()
    # Fallback: use unity_creative_name_from_filename logic
    m = re.search(r"(\d{3})(?!.*\d)", base)
    code = m.group(1) if m else "000"
    return f"video{code}"

def _clean_playable_name_for_pack(playable_name_or_label: str) -> str:
    """
    Clean playable name for creative pack naming.
    
    Rules:
    1. If label format "name (type) [id]", extract just the name part
    2. If playable name starts with something before underscore (e.g., "hello_playablexxx"),
       remove everything before and including the first underscore (result: "playablexxx")
    3. Remove "_unityads.html" or ".html" suffix
    4. Remove all underscores
    5. Return cleaned name
    
    Examples:
    - "playable_003_escalater_감옥_unityads.html" -> "playable003escalater감옥"
    - "playable_003_escalater_감옥.html" -> "playable003escalater감옥"
    - "hello_playable_003" -> "playable003"
    - "playable_name (playable) [12345]" -> "playablename"
    """
    if not playable_name_or_label:
        return ""
    
    # Step 1: Extract name from label format "name (type) [id]"
    name = playable_name_or_label.split(" (")[0].strip()
    
    # Step 2: Remove file extension and suffixes (.html, _unityads.html, _Default Version, _Default Creative) - do this first
    name = re.sub(r"_unityads\.html$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\.html$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"_Default Version$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"_Default Creative$", "", name, flags=re.IGNORECASE)
    
    # Step 3: If there's text before the first underscore and 'playable' appears after it,
    # remove everything before and including the first underscore
    # Example: "hello_playable_003" -> "playable_003"
    # Find the first underscore, and if 'playable' (case-insensitive) appears after it,
    # remove everything up to and including that underscore
    first_underscore_idx = name.find("_")
    if first_underscore_idx >= 0:
        # Check if 'playable' appears after the first underscore
        after_underscore = name[first_underscore_idx + 1:]
        if "playable" in after_underscore.lower():
            # Remove everything before and including the first underscore
            name = after_underscore
    
    # Step 4: Remove all underscores
    name = name.replace("_", "")
    
    return name

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
    try:
        _unity_post(path, {"id": creative_pack_id})
    except Exception as e:
        error_str = str(e).lower()
        # Check if error is related to capacity/limit
        if any(keyword in error_str for keyword in ["limit", "maximum", "exceeded", "full", "capacity", "quota"]):
            raise RuntimeError("Creative pack 개수가 최대입니다. 사용하지 않는 creative을 제거해주세요.")
        raise  # Re-raise original error if not capacity-related

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
                error_text = resp.text[:400] if resp.text else ""
                # Check if error is related to capacity/limit
                error_lower = error_text.lower()
                if any(keyword in error_lower for keyword in ["limit", "maximum", "exceeded", "full", "capacity", "quota"]):
                    raise RuntimeError("Creative 개수가 최대입니다. 사용하지 않는 creative을 제거해주세요.")
                raise RuntimeError(f"Unity create creative failed ({resp.status_code}): {error_text}")

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
                error_text = resp.text[:400] if resp.text else ""
                # Check if error is related to capacity/limit
                error_lower = error_text.lower()
                if any(keyword in error_lower for keyword in ["limit", "maximum", "exceeded", "full", "capacity", "quota"]):
                    raise RuntimeError("Creative 개수가 최대입니다. 사용하지 않는 creative을 제거해주세요.")
                raise RuntimeError(f"Unity create playable failed ({resp.status_code}): {error_text}")

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
    try:
        meta = _unity_post(path, payload)
    except Exception as e:
        error_str = str(e).lower()
        # Check if error is related to capacity/limit
        if any(keyword in error_str for keyword in ["limit", "maximum", "exceeded", "full", "capacity", "quota"]):
            raise RuntimeError("Creative pack 개수가 최대입니다. 사용하지 않는 creative을 제거해주세요.")
        raise  # Re-raise original error if not capacity-related
    
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

def _unity_list_campaign_playables(*, org_id: str, title_id: str, campaign_id: str) -> List[dict]:
    """특정 Campaign의 할당된 Playable 크리에이티브 조회 (Operator용)"""
    try:
        # 1. 캠페인에 할당된 모든 Creative Pack 가져오기
        assigned_packs = _unity_list_assigned_creative_packs(
            org_id=org_id, 
            title_id=title_id, 
            campaign_id=campaign_id
        )
        
        playable_ids = set()
        
        # 2. 각 Pack의 creativeIds에서 Playable 추출
        for pack in assigned_packs:
            pack_id = pack.get("id")
            if not pack_id:
                continue
            
            # Pack 상세정보 조회
            pack_path = f"organizations/{org_id}/apps/{title_id}/creative-packs/{pack_id}"
            pack_detail = _unity_get(pack_path)
            
            creative_ids = pack_detail.get("creativeIds", [])
            for cid in creative_ids:
                playable_ids.add(str(cid))
        
        # 3. 각 Creative ID의 타입 확인 후 Playable만 필터링
        playables = []
        for cid in playable_ids:
            try:
                creative = _unity_get_creative(org_id=org_id, title_id=title_id, creative_id=cid)
                c_type = (creative.get("type") or "").lower()
                
                if "playable" in c_type or "cpe" in c_type:
                    playables.append(creative)
                    
            except Exception as e:
                logger.warning(f"Failed to fetch creative {cid}: {e}")
                continue
        
        return playables
        
    except Exception as e:
        logger.warning(f"Failed to list campaign playables: {e}")
        return []
        
def _save_upload_state(game: str, campaign_id: str, state: Dict):
    """Save upload state to session."""
    key = _get_upload_state_key(game, campaign_id)
    st.session_state[key] = state


def _clear_upload_state(game: str, campaign_id: str):
    """Clear upload state (call when upload completes successfully)."""
    key = _get_upload_state_key(game, campaign_id)
    if key in st.session_state:
        del st.session_state[key]


def _check_existing_creative(org_id: str, title_id: str, name: str) -> str | None:
    """
    Check if a creative with this name already exists in Unity.
    Returns creative_id if found, None otherwise.
    """
    try:
        path = f"organizations/{org_id}/apps/{title_id}/creatives"
        meta = _unity_get(path, params={"limit": 100})
        
        items = []
        if isinstance(meta, list):
            items = meta
        elif isinstance(meta, dict):
            items = meta.get("items") or meta.get("data") or []
        
        for creative in items:
            if creative.get("name") == name:
                return str(creative.get("id", ""))
        
        return None
    except Exception as e:
        logger.warning(f"Could not check existing creative: {e}")
        return None


def _check_existing_pack(org_id: str, title_id: str, pack_name: str) -> str | None:
    """
    Check if a creative pack with this name already exists.
    Returns pack_id if found, None otherwise.
    """
    try:
        path = f"organizations/{org_id}/apps/{title_id}/creative-packs"
        
        # ✅ limit을 늘리거나 pagination 처리
        # Unity API는 보통 최대 100개씩 반환하므로, 여러 번 요청해야 할 수 있음
        all_items = []
        offset = 0
        limit = 100
        
        while True:
            meta = _unity_get(path, params={"limit": limit, "offset": offset})
            
            items = []
            if isinstance(meta, list):
                items = meta
            elif isinstance(meta, dict):
                items = meta.get("items") or meta.get("data") or []
            
            if not items:
                break
                
            all_items.extend(items)
            
            # 더 이상 데이터가 없으면 중단
            if len(items) < limit:
                break
                
            offset += limit
        
        # ✅ 이름 비교를 더 정확하게 (trim, case-insensitive)
        pack_name_normalized = pack_name.strip().lower()
        
        for pack in all_items:
            existing_name = pack.get("name", "").strip().lower()
            if existing_name == pack_name_normalized:
                return str(pack.get("id", ""))
        
        return None
    except Exception as e:
        logger.warning(f"Could not check existing pack: {e}")
        return None

def _check_existing_pack_by_creatives(org_id: str, title_id: str, creative_ids: List[str]) -> tuple[str | None, str | None]:
    """
    Check if a creative pack already exists with the same creative IDs (video + playable combination).
    Returns (pack_id, pack_name) if found, (None, None) otherwise.
    """
    try:
        path = f"organizations/{org_id}/apps/{title_id}/creative-packs"
        meta = _unity_get(path, params={"limit": 100})
        
        items = []
        if isinstance(meta, list):
            items = meta
        elif isinstance(meta, dict):
            items = meta.get("items") or meta.get("data") or []
        
        # Normalize creative_ids to set for comparison (order doesn't matter)
        target_creative_set = set(str(cid) for cid in creative_ids if cid)
        
        for pack in items:
            pack_creative_ids = pack.get("creativeIds") or pack.get("creative_ids") or []
            pack_creative_set = set(str(cid) for cid in pack_creative_ids if cid)
            
            # Check if sets match (same video + playable combination)
            if pack_creative_set == target_creative_set:
                pack_id = str(pack.get("id", ""))
                pack_name = pack.get("name", "")
                return (pack_id, pack_name)
        
        return (None, None)
    except Exception as e:
        logger.warning(f"Could not check existing pack by creatives: {e}")
        return (None, None)
# --------------------------------------------------------------------
# Dry Run / Preview Functions
# --------------------------------------------------------------------
def preview_unity_upload(
    *,
    game: str,
    videos: List[Dict[str, Any]],
    settings: Dict[str, Any],
    is_marketer: bool = False
) -> Dict[str, Any]:
    """
    Preview what would happen if Unity upload is executed.
    Returns a dict with preview information without actually uploading.
    """
    # Get title_id: Test Mode는 campaign_set_id(aos), Marketer Mode는 플랫폼별 app_id 사용
    title_id = (settings.get("title_id") or "").strip()
    if not title_id:
        if is_marketer:
            # Marketer Mode: 플랫폼에 따라 app_id 사용
            platform = settings.get("platform", "aos")
            try:
                title_id = get_unity_app_id(game, platform)
            except Exception as e:
                logger.warning(f"Failed to get app ID for {game} ({platform}): {e}")
                # Fallback: UNITY_GAME_IDS 사용
                title_id = str(UNITY_GAME_IDS.get(game, ""))
        else:
            # Test Mode: campaign_set_id (aos) 사용
            try:
                title_id = get_unity_campaign_set_id(game, "aos")
            except Exception as e:
                logger.warning(f"Failed to get campaign set ID for {game}: {e}")
                raise RuntimeError(f"Missing title_id for {game}. Please set it in Unity settings.")
    
    campaign_id = (settings.get("campaign_id") or "").strip()
    if not campaign_id:
        ids_for_game = UNITY_CAMPAIGN_IDS.get(game) or []
        if ids_for_game:
            campaign_id = str(ids_for_game[0])
    
    org_id = (settings.get("org_id") or "").strip() or UNITY_ORG_ID_DEFAULT
    
    if not all([title_id, campaign_id, org_id]):
        missing = []
        if not title_id:
            missing.append("title_id")
        if not campaign_id:
            missing.append("campaign_id")
        if not org_id:
            missing.append("org_id")
        raise RuntimeError(f"Unity Settings Missing for preview. Missing: {', '.join(missing)}")
    
    # Get playable info
    playable_name = settings.get("selected_playable") or ""
    existing_playable_id = settings.get("existing_playable_id") or ""
    existing_playable_label = settings.get("existing_playable_label", "")
    
    # Group videos by base name
    subjects: dict[str, list[dict]] = {}
    for v in videos or []:
        n = v.get("name") or ""
        if "playable" in n.lower():
            continue
        base = n.split("_")[0]
        subjects.setdefault(base, []).append(v)
    
    # Generate preview pack names
    preview_packs = []
    for base, items in subjects.items():
        portrait = next((x for x in items if "1080x1920" in (x.get("name") or "")), None)
        landscape = next((x for x in items if "1920x1080" in (x.get("name") or "")), None)
        
        if not portrait or not landscape:
            continue
        
        # Generate pack name
        video_part = _extract_video_part_from_base(base)
        raw_p_name = playable_name if playable_name else existing_playable_label
        playable_part = _clean_playable_name_for_pack(raw_p_name)
        
        if playable_part:
            final_pack_name = f"{video_part}_{playable_part}"
        else:
            final_pack_name = f"{video_part}_playable"
        
        preview_packs.append({
            "pack_name": final_pack_name,
            "portrait_video": portrait.get("name"),
            "landscape_video": landscape.get("name"),
            "playable": playable_name or existing_playable_label or "(No playable selected)",
        })
    
    # Check currently assigned creative packs
    try:
        assigned_packs = _unity_list_assigned_creative_packs(
            org_id=org_id,
            title_id=title_id,
            campaign_id=campaign_id
        )
        current_assigned = [
            {
                "id": pack.get("id") or pack.get("assignedCreativePackId"),
                "name": pack.get("name", "Unknown"),
            }
            for pack in assigned_packs
        ]
    except Exception as e:
        logger.warning(f"Could not fetch assigned packs: {e}")
        current_assigned = []
    
    return {
        "game": game,
        "org_id": org_id,
        "title_id": title_id,
        "campaign_id": campaign_id,
        "total_packs_to_create": len(preview_packs),
        "preview_packs": preview_packs,
        "current_assigned_packs": current_assigned,
        "playable_info": {
            "selected_playable": playable_name,
            "existing_playable_id": existing_playable_id,
            "existing_playable_label": existing_playable_label,
        },
        "action_summary": {
            "will_create_packs": len(preview_packs),
            "will_unassign_existing": 0 if is_marketer else len(current_assigned),
            "will_assign_new": len(preview_packs),
            "is_marketer_mode": is_marketer,
        }
    }

# --------------------------------------------------------------------
# Main Helpers
# --------------------------------------------------------------------

def upload_unity_creatives_to_campaign(
    *, 
    game: str, 
    videos: List[Dict[str, Any]], 
    settings: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Upload Unity creatives and packs with resume support.
    
    If upload fails midway, calling again will:
    - Skip already uploaded creatives
    - Only upload missing items
    - Resume from where it left off
    """
    # Get title_id: Test Mode는 campaign_set_id(aos), Marketer Mode는 플랫폼별 app_id 사용
    title_id = (settings.get("title_id") or "").strip()
    if not title_id:
        # settings에서 platform 정보가 있으면 marketer mode로 간주
        platform = settings.get("platform", "aos")
        if platform in ("aos", "ios"):
            # Marketer Mode: 플랫폼에 따라 app_id 사용
            try:
                title_id = get_unity_app_id(game, platform)
            except Exception as e:
                logger.warning(f"Failed to get app ID for {game} ({platform}): {e}")
                # Fallback: UNITY_GAME_IDS 사용
                title_id = str(UNITY_GAME_IDS.get(game, ""))
        else:
            # Test Mode: campaign_set_id (aos) 사용
            try:
                title_id = get_unity_campaign_set_id(game, "aos")
            except Exception as e:
                logger.warning(f"Failed to get campaign set ID for {game}: {e}")
                raise RuntimeError(f"Missing title_id for {game}. Please set it in Unity settings.")
    
    campaign_id = (settings.get("campaign_id") or "").strip()
    if not campaign_id:
        ids_for_game = UNITY_CAMPAIGN_IDS.get(game) or []
        if ids_for_game:
            campaign_id = str(ids_for_game[0])
    
    org_id = (settings.get("org_id") or "").strip() or UNITY_ORG_ID_DEFAULT
    
    if not all([title_id, campaign_id, org_id]):
        missing = []
        if not title_id:
            missing.append("title_id (app ID)")
        if not campaign_id:
            missing.append("campaign_id")
        if not org_id:
            missing.append("org_id")
        raise RuntimeError(f"Unity Settings Missing for upload. Missing: {', '.join(missing)}")
    
    # 디버깅: 실제 사용되는 ID들 출력
    st.info(f"🔍 **Debug Info:**\n- Org ID: {org_id}\n- Title ID (App ID): {title_id}\n- Campaign ID: {campaign_id}")
    logger.info(f"Unity upload - org_id={org_id}, title_id={title_id}, campaign_id={campaign_id}")

    # Unity Ads creative limit per app (typically very high, but check for safety)
    # Unity Ads creative pack limit per campaign (typically 50)
    UNITY_CREATIVE_LIMIT = 1000  # Creative limit is usually very high
    UNITY_CREATIVE_PACK_LIMIT = 50  # Creative pack limit per campaign
    
    # Check creative capacity (optional, as limit is usually very high)
    # This check is mainly for API error handling
    try:
        # Estimate how many creatives will be created
        # Each video pair (portrait + landscape) = 2 creatives, plus 1 playable per pack
        # For simplicity, we'll check during creation and handle API errors
        pass  # Creative limit check skipped as Unity Ads creative limit is usually very high
    except Exception as e:
        logger.warning(f"Could not check creative capacity: {e}")

    start_iso = next_sat_0000_kst()
    errors: List[str] = []
    
    # Initialize upload state (loads existing if resuming)
    upload_state = _init_upload_state(game, campaign_id, videos)
    
    # Show resume info if applicable
    existing_packs = len([p for p in upload_state["completed_packs"] if p])
    if existing_packs > 0:
        st.info(
            f"📦 **Resuming Upload**\n\n"
            f"Found {existing_packs} previously created pack(s).\n"
            f"Will skip already uploaded items and continue from where we left off."
        )

    # ========================================
    # 1. PLAYABLE HANDLING
    # ========================================
    playable_name = settings.get("selected_playable") or ""
    existing_playable_id = settings.get("existing_playable_id") or ""
    playable_creative_id: str | None = upload_state.get("playable_creative")

    if not playable_creative_id:
        if playable_name:
            playable_item = next((v for v in (videos or []) if v.get("name") == playable_name), None)
            if playable_item:
                try:
                    # Check if already uploaded
                    playable_creative_id = _check_existing_creative(org_id, title_id, playable_name)
                    
                    if playable_creative_id:
                        st.info(f"✅ Found existing playable: {playable_name}")
                    else:
                        st.info(f"⬆️ Uploading playable: {playable_name}")
                        playable_creative_id = _unity_create_playable_creative(
                            org_id=org_id, 
                            title_id=title_id, 
                            playable_path=playable_item["path"], 
                            name=playable_name
                        )
                    
                    upload_state["playable_creative"] = playable_creative_id
                    _save_upload_state(game, campaign_id, upload_state)
                    
                except Exception as e:
                    errors.append(f"Playable creation failed: {e}")
                    playable_creative_id = None

        if not playable_creative_id and existing_playable_id:
            playable_creative_id = str(existing_playable_id)
            upload_state["playable_creative"] = playable_creative_id
            _save_upload_state(game, campaign_id, upload_state)

    # Validate Playable
    if playable_creative_id:
        try:
            p_details = _unity_get_creative(org_id=org_id, title_id=title_id, creative_id=playable_creative_id)
            p_type = (p_details.get("type") or "").lower()
            
            if "playable" not in p_type and "cpe" not in p_type:
                error_msg = f"CRITICAL: Playable ID ({playable_creative_id}) is type '{p_type}'. Must be 'playable'."
                errors.append(error_msg)
                return {"game": game, "campaign_id": campaign_id, "errors": errors, "creative_ids": upload_state["completed_packs"]}
        except Exception as e:
            errors.append(f"Could not validate Playable ID: {e}")
            return {"game": game, "campaign_id": campaign_id, "errors": errors, "creative_ids": upload_state["completed_packs"]}
    else:
        errors.append("No Playable End Card selected.")
        return {"game": game, "campaign_id": campaign_id, "errors": errors, "creative_ids": upload_state["completed_packs"]}

    # ========================================
    # 2. VIDEO PAIRING
    # ========================================
    subjects: dict[str, list[dict]] = {}
    for v in videos or []:
        n = v.get("name") or ""
        if "playable" in n.lower():
            continue
        base = n.split("_")[0]
        subjects.setdefault(base, []).append(v)

    total_pairs = len(subjects)
    upload_state["total_expected"] = total_pairs
    _save_upload_state(game, campaign_id, upload_state)
    
    if total_pairs == 0:
        st.warning("No video pairs found to upload.")
        return {
            "game": game,
            "campaign_id": campaign_id,
            "errors": errors,
            "creative_ids": upload_state["completed_packs"]
        }

    processed_count = 0
    progress_bar = st.progress(0, text=f"Starting upload... (0/{total_pairs})")
    
    # Status container for real-time updates
    status_container = st.empty()

    # ========================================
    # 3. PROCESSING LOOP
    # ========================================
    for base, items in subjects.items():
        portrait = next((x for x in items if "1080x1920" in (x.get("name") or "")), None)
        landscape = next((x for x in items if "1920x1080" in (x.get("name") or "")), None)

        if not portrait or not landscape:
            errors.append(f"{base}: Missing Portrait or Landscape video.")
            processed_count += 1
            progress_bar.progress(
                int(processed_count / total_pairs * 100),
                text=f"❌ Skipped {base} (Missing videos) - {processed_count}/{total_pairs}"
            )
            continue
        
        # Generate pack name
        # Extract video part (e.g., "video001")
        video_part = _extract_video_part_from_base(base)
        
        # Get playable name or label
        raw_p_name = playable_name if playable_name else settings.get("existing_playable_label", "")
        
        # Clean playable name according to rules
        playable_part = _clean_playable_name_for_pack(raw_p_name)
        
        # Final pack name: videoxxx_playable003escalater감옥 (underscore between video and playable)
        if playable_part:
            final_pack_name = f"{video_part}_{playable_part}"
        else:
            # Fallback if no playable name
            final_pack_name = f"{video_part}_playable"
        
        # Check if pack already exists
        if final_pack_name in upload_state["creative_packs"] and upload_state["creative_packs"][final_pack_name]:
            pack_id = upload_state["creative_packs"][final_pack_name]
            if pack_id not in upload_state["completed_packs"]:
                upload_state["completed_packs"].append(pack_id)
                _save_upload_state(game, campaign_id, upload_state)
            
            processed_count += 1
            progress_bar.progress(
                int(processed_count / total_pairs * 100),
                text=f"✅ Already uploaded: {base} - {processed_count}/{total_pairs}"
            )
            status_container.success(f"✅ Skipped (already exists): {final_pack_name}")
            continue

        try:
            progress_bar.progress(
                int(processed_count / total_pairs * 100),
                text=f"⬆️ Uploading {base} ({processed_count + 1}/{total_pairs})..."
            )
            
            # Upload portrait video (check if exists first)
            p_id = upload_state["video_creatives"].get(portrait["name"])
            if not p_id:
                p_id = _check_existing_creative(org_id, title_id, portrait["name"])
                
            if not p_id:
                status_container.info(f"⬆️ Uploading portrait: {portrait['name']}")
                p_id = _unity_create_video_creative(
                    org_id=org_id, 
                    title_id=title_id, 
                    video_path=portrait["path"], 
                    name=portrait["name"]
                )
                upload_state["video_creatives"][portrait["name"]] = p_id
                _save_upload_state(game, campaign_id, upload_state)
                time.sleep(1)
            else:
                status_container.success(f"✅ Found existing: {portrait['name']}")
            
            # Upload landscape video (check if exists first)
            l_id = upload_state["video_creatives"].get(landscape["name"])
            if not l_id:
                l_id = _check_existing_creative(org_id, title_id, landscape["name"])
                
            if not l_id:
                status_container.info(f"⬆️ Uploading landscape: {landscape['name']}")
                l_id = _unity_create_video_creative(
                    org_id=org_id, 
                    title_id=title_id, 
                    video_path=landscape["path"], 
                    name=landscape["name"]
                )
                upload_state["video_creatives"][landscape["name"]] = l_id
                _save_upload_state(game, campaign_id, upload_state)
                time.sleep(1)
            else:
                status_container.success(f"✅ Found existing: {landscape['name']}")

            pack_creatives = [p_id, l_id, playable_creative_id]
            
            # ✅ Check if pack already exists by name first
            pack_id = _check_existing_pack(org_id, title_id, final_pack_name)
            existing_pack_name = final_pack_name
            
            # ✅ 이름으로 찾았으면 명확하게 스킵
            if pack_id:
                status_container.warning(
                    f"⚠️ **Creative pack already exists with same name:**\n\n"
                    f"   - Pack Name: `{final_pack_name}`\n"
                    f"   - Pack ID: `{pack_id}`\n"
                    f"   - Skipping creation...\n"
                )
                logger.info(f"Skipping pack creation for {final_pack_name} - already exists ({pack_id})")
            else:
                # Also check if pack exists with same video + playable combination (for marketer mode)
                existing_pack_id, existing_pack_name = _check_existing_pack_by_creatives(
                    org_id, title_id, pack_creatives
                )
                if existing_pack_id:
                    pack_id = existing_pack_id
                    status_container.warning(
                        f"⚠️ **Creative pack already exists** with same video + playable combination:\n\n"
                        f"   - Existing Pack Name: `{existing_pack_name}`\n"
                        f"   - Existing Pack ID: `{existing_pack_id}`\n"
                        f"   - Skipping upload for: `{final_pack_name}`\n\n"
                        f"   Continuing with remaining uploads..."
                    )
                    logger.info(f"Skipping pack creation for {final_pack_name} - already exists as {existing_pack_name} ({existing_pack_id})")
            
            # ✅ pack_id가 있으면 생성하지 않고 기존 팩 사용
            if not pack_id:
                status_container.info(f"📦 Creating pack: {final_pack_name}")
                logger.info(f"Creating pack with org_id={org_id}, title_id={title_id}, pack_name={final_pack_name}, creative_ids={pack_creatives}")
                pack_id = _unity_create_creative_pack(
                    org_id=org_id,
                    title_id=title_id,
                    pack_name=final_pack_name,
                    creative_ids=pack_creatives,
                    pack_type="video+playable"
                )
                logger.info(f"✅ Created pack with ID: {pack_id}")
            else:
                # ✅ 기존 팩이 있으면 명확하게 표시
                if existing_pack_name != final_pack_name:
                    status_container.success(f"✅ Found existing pack with same video + playable: `{existing_pack_name}`")
                else:
                    status_container.success(f"✅ Found existing pack with same name: `{final_pack_name}`")
            
            upload_state["creative_packs"][final_pack_name] = pack_id
            upload_state["completed_packs"].append(pack_id)
            _save_upload_state(game, campaign_id, upload_state)
            
            status_container.success(f"✅ Completed: {final_pack_name}")
            time.sleep(0.5)

        except Exception as e:
            msg = str(e)
            if "Quota Exceeded" in msg or "429" in msg:
                errors.append(f"⚠️ Rate limit reached at {base}. Progress saved - you can retry!")
                status_container.error(
                    f"⚠️ **Rate Limit Reached**\n\n"
                    f"Progress saved: {len(upload_state['completed_packs'])}/{total_pairs} packs created.\n"
                    f"Click '크리에이티브/팩 생성' again to resume from where we left off."
                )
                break
            
            logger.exception(f"Unity pack creation failed for {base}")
            errors.append(f"{base}: {msg}")
            status_container.error(f"❌ Failed: {base} - {msg}")

        finally:
            processed_count += 1
            pct = int(processed_count / total_pairs * 100)
            completed = len(upload_state["completed_packs"])
            progress_bar.progress(
                pct, 
                text=f"Progress: {completed}/{total_pairs} packs created"
            )

    progress_bar.empty()
    status_container.empty()
    
    # Final summary
    total_created = len(upload_state["completed_packs"])
    
    if total_created == total_pairs:
        st.success(
            f"🎉 **Upload Complete!**\n\n"
            f"Successfully created **{total_created}/{total_pairs}** creative packs."
        )
        # Clear state on successful completion
        _clear_upload_state(game, campaign_id)
    elif total_created > 0:
        st.warning(
            f"⚠️ **Partial Upload**\n\n"
            f"Created **{total_created}/{total_pairs}** creative packs.\n"
            f"Click '크리에이티브/팩 생성' again to continue uploading remaining packs."
        )
    
    return {
        "game": game,
        "campaign_id": campaign_id,
        "start_iso": start_iso,
        "creative_ids": upload_state["completed_packs"],
        "errors": errors,
        "removed_ids": [],
        "total_created": total_created,
        "total_expected": total_pairs
    }

def apply_unity_creative_packs_to_campaign(*, game: str, creative_pack_ids: List[str], settings: Dict[str, Any], is_marketer: bool = False) -> Dict[str, Any]:
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

    # 1. Unassign existing (Only for Test Mode, not Marketer Mode)
    if not is_marketer:
        # Test Mode: Unassign existing packs first
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
    else:
        # Marketer Mode: Skip unassign, just show current assignments
        try:
            assigned = _unity_list_assigned_creative_packs(org_id=org_id, title_id=title_id, campaign_id=campaign_id)
            if assigned:
                st.info(f"ℹ️ Marketer Mode: {len(assigned)} existing pack(s) will remain assigned. New packs will be added.")
        except Exception as e:
            logger.warning(f"Could not fetch existing assignments: {e}")

    # 2. Assign new
    total_assign = len(creative_pack_ids)
    count_a = 0
    start_pct = 0 if is_marketer else 50  # Marketer mode starts at 0% since no unassign
    
    for pack_id in creative_pack_ids:
        count_a += 1
        pct = start_pct + int(count_a / max(total_assign, 1) * (100 - start_pct))
        progress_bar.progress(pct, text=f"Assigning new packs {count_a}/{total_assign}...")
        
        try:
            _unity_assign_creative_pack(org_id=org_id, title_id=title_id, campaign_id=campaign_id, creative_pack_id=str(pack_id))
            assigned_packs.append(str(pack_id))
            time.sleep(0.5) 
        except Exception as e:
            error_str = str(e).lower()
            # Check if error is related to capacity/limit
            if any(keyword in error_str for keyword in ["limit", "maximum", "exceeded", "full", "capacity", "quota"]):
                errors.append(f"Creative pack 개수가 최대입니다. 사용하지 않는 creative을 제거해주세요.")
            else:
                errors.append(f"Assign error {pack_id}: {e}")

    progress_bar.empty()

    return {
        "game": game,
        "campaign_id": campaign_id,
        "assigned_packs": assigned_packs,
        "removed_assignments": removed_ids,
        "errors": errors,
    }

