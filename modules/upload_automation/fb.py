"""Marketer-side Facebook helpers for Creative Auto-Upload.

Features:
1. Campaign/AdSet Selection
2. Ad Setup (Single Video / Dynamic)
3. Smart "Mimic" Defaults: 
   - Scans Ad Set for the "highest numbered" video ad.
   - Pre-fills Headline, Text, and CTA from that winner.
"""
from __future__ import annotations

# Standard library imports
import logging
import os
import pathlib
import re
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Third-party imports
import requests
import streamlit as st
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.advideo import AdVideo
from facebook_business.adobjects.campaign import Campaign
from facebook_business.exceptions import FacebookRequestError
    
# Local imports
from modules.upload_automation.utils import devtools
from modules.upload_automation.network.dto import RequestExecutionContextDTO
from modules.upload_automation.service.facebook import build_advideos_resumable_request
from modules.upload_automation.network.http_client import execute_request
from modules.upload_automation.network.retry_policies import build_no_retry_policy
from facebook_ads import (
    FB_GAME_MAPPING,
    GAME_DEFAULTS,
    OPT_GOAL_LABEL_TO_API,
    _plan_upload,
    build_targeting_from_settings,
    create_creativetest_adset,
    extract_thumbnail_from_video,
    init_fb_from_secrets,
    init_fb_game_defaults,
    make_ad_name,
    next_sat_0900_kst,
    sanitize_store_url,
    upload_thumbnail_image,
    upload_videos_create_ads,
    validate_page_binding,
)


logger = logging.getLogger(__name__)

# =========================================================
# Dev-only UI helpers (hide noisy status banners unless ?dev=1)
# =========================================================
def _dev_info(msg: str) -> None:
    if devtools.dev_enabled():
        st.info(msg)


def _dev_success(msg: str) -> None:
    if devtools.dev_enabled():
        st.success(msg)


def _dev_caption(msg: str) -> None:
    if devtools.dev_enabled():
        st.caption(msg)


def _dev_warning(msg: str) -> None:
    if devtools.dev_enabled():
        st.warning(msg)


def _dev_write(msg: str) -> None:
    if devtools.dev_enabled():
        st.write(msg)


# --- Constants ---
FB_CTA_OPTIONS = [
    "INSTALL_MOBILE_APP", "PLAY_GAME", "USE_APP", "DOWNLOAD", 
    "SHOP_NOW", "LEARN_MORE", "SIGN_UP", "WATCH_MORE", "NO_BUTTON"
]

# --- Helper Functions ---
# =========================================================
# Speed/Robustness Utilities
# =========================================================
_thread_local = threading.local()

def _get_session() -> requests.Session:
    """Returns a per-thread requests.Session for connection reuse (faster, fewer TLS handshakes)."""
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        _thread_local.session = s
    return s

def with_retry(fn, tries: int = 4, base_wait: float = 1.0, max_wait: float = 12.0):
    """Runs fn() with exponential backoff. Useful for transient FB/network errors."""
    wait = base_wait
    last_err = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if i == tries - 1:
                raise
            time.sleep(wait)
            wait = min(wait * 2.0, max_wait)
    raise last_err  # pragma: no cover

def wait_video_ready(video_id: str, timeout_s: int = 180, base_sleep: float = 1.0) -> None:
    """
    Polls Facebook video processing status until ready (removes fixed sleep).
    This reduces total time and also lowers 'video still processing' errors.
    """
    start = time.time()
    sleep_s = base_sleep
    while True:
        v = AdVideo(video_id).api_get(fields=["status"])
        status = str(v.get("status", "")).upper()

        # Be permissive across accounts/api versions:
        # READY / FINISHED / COMPLETED 같은 키워드가 보이면 통과
        if any(k in status for k in ["READY", "FINISHED", "COMPLETED"]):
            return

        if time.time() - start > timeout_s:
            raise TimeoutError(f"Video not ready in {timeout_s}s: {video_id} (status={status})")

        time.sleep(sleep_s)
        sleep_s = min(sleep_s * 1.5, 8.0)

def _extract_number_from_name(name: str) -> int:
    """
    Extracts the largest integer found in a string to determine 'version'.
    Returns -1 if no number is found.
    Ex: 'Video_Ad_105_Final' -> 105
    """
    matches = re.findall(r'\d+', name)
    if not matches:
        return -1
    # Return the largest number found to be safe, or just the first if preferred
    return max([int(m) for m in matches])


def _build_video_ranges_label(nums: list[int]) -> str:
    """
    Build a label like:
      - [481, 483, 484, 485, 486, 487, 488, 489] -> "video481, video483-489"
      - [100, 101, 102, 103, 104, 123] -> "video123, video100-104"

    Rule:
    - Split into consecutive ranges.
    - Choose the "main" range as the longest (ties -> smaller start wins).
    - Put other ranges first (sorted by start desc), main range last.
    """
    nums = sorted(set(int(x) for x in (nums or []) if x is not None))
    if not nums:
        return ""

    ranges: list[tuple[int, int, int]] = []
    start = prev = nums[0]
    for n in nums[1:]:
        if n == prev + 1:
            prev = n
            continue
        ranges.append((start, prev, prev - start + 1))
        start = prev = n
    ranges.append((start, prev, prev - start + 1))

    main = max(ranges, key=lambda r: (r[2], -r[0]))
    others = [r for r in ranges if r != main]
    others.sort(key=lambda r: r[0], reverse=True)

    def _fmt(a: int, b: int) -> str:
        return f"video{a}" if a == b else f"video{a}-{b}"

    parts = [_fmt(a, b) for (a, b, _) in others] + [_fmt(main[0], main[1])]
    return ", ".join(parts)

# --- Rate Limit Helper ---

_RATE_LIMIT_CODES = {17, 32, 4}  # User limit, API too many calls, App limit
_RATE_LIMIT_COOLDOWN_SECONDS = 300  # 5분 쿨다운

def _is_rate_limit_error(e: Exception) -> bool:
    """Check if a Facebook API error is a rate limit error."""
    if isinstance(e, FacebookRequestError):
        return e.api_error_code() in _RATE_LIMIT_CODES
    return False


def _set_rate_limit_cooldown() -> None:
    """Rate limit이 발생하면 글로벌 쿨다운 설정 — 모든 FB API 호출을 일시 차단."""
    st.session_state["fb_rate_limit_until"] = time.time() + _RATE_LIMIT_COOLDOWN_SECONDS


def _is_in_cooldown() -> bool:
    """현재 rate limit 쿨다운 상태인지 확인."""
    until = st.session_state.get("fb_rate_limit_until", 0)
    if time.time() < until:
        return True
    return False


def _cooldown_remaining() -> int:
    """남은 쿨다운 시간(초)."""
    until = st.session_state.get("fb_rate_limit_until", 0)
    remaining = int(until - time.time())
    return max(0, remaining)


def _handle_rate_limit(e: Exception, context: str) -> None:
    """Rate limit 에러 처리: 쿨다운 설정 + 사용자 경고."""
    _set_rate_limit_cooldown()
    remaining = _cooldown_remaining()
    minutes = remaining // 60
    seconds = remaining % 60
    st.warning(
        f"⚠️ Facebook API 호출 한도에 도달했습니다 ({context}). "
        f"{minutes}분 {seconds}초 후 자동으로 재시도됩니다. 페이지를 새로고침하지 마세요."
    )


# --- Cached Data Fetchers ---

@st.cache_data(ttl=600, show_spinner=False)
def fetch_active_campaigns_cached(account_id: str) -> list[dict]:
    """Fetch ACTIVE campaigns. Rate limit 에러는 re-raise하여 캐싱 방지."""
    try:
        account = init_fb_from_secrets(account_id)
        campaigns = account.get_campaigns(
            fields=[Campaign.Field.name, Campaign.Field.id],
            params={"effective_status": ["ACTIVE"], "limit": 100}
        )
        return [{"id": c["id"], "name": c["name"]} for c in campaigns]
    except Exception as e:
        if _is_rate_limit_error(e):
            raise  # re-raise → @st.cache_data가 빈 결과를 캐싱하지 않음
        logger.error(f"Error fetching campaigns: {e}")
        return []

@st.cache_data(ttl=600, show_spinner=False)
def fetch_active_adsets_cached(account_id: str, campaign_id: str) -> list[dict]:
    """Fetch adsets (excluding DELETED/ARCHIVED). Rate limit 에러는 re-raise하여 캐싱 방지."""
    try:
        campaign = Campaign(campaign_id)
        adsets_all = campaign.get_ad_sets(
            fields=[AdSet.Field.name, AdSet.Field.id, AdSet.Field.effective_status],
            params={"limit": 100}
        )
        filtered = []
        for a in adsets_all:
            status = str(a.get("effective_status", "")).upper()
            if status not in ["DELETED", "ARCHIVED"]:
                filtered.append({"id": a["id"], "name": a["name"]})
        return filtered
    except Exception as e:
        if _is_rate_limit_error(e):
            raise  # re-raise → @st.cache_data가 빈 결과를 캐싱하지 않음
        logger.error(f"Error fetching adsets: {e}")
        return []

@st.cache_data(ttl=600, show_spinner=False)
def fetch_latest_ad_creative_defaults(adset_id: str) -> dict:
    """Fetch ad creative defaults from the highest-numbered active ad in the adset."""
    try:
        adset = AdSet(adset_id)
        # Fetch ads
        ads = adset.get_ads(
            fields=[Ad.Field.name, Ad.Field.creative],
            # Template Source Auto (highest): active ads only
            params={"limit": 100, "effective_status": ["ACTIVE"]}
        )
        
        if not ads: return {}

        # Sort by Highest Number
        candidate_ads = []
        for ad in ads:
            num = _extract_number_from_name(ad['name'])
            if num > -1: candidate_ads.append((num, ad))
        
        if not candidate_ads: return {}

        candidate_ads.sort(key=lambda x: x[0], reverse=True)
        target_ad_data = candidate_ads[0][1] # The Winner
        
        # Fetch Creative Data
        c_id = target_ad_data['creative']['id']
        c_data = AdCreative(c_id).api_get(fields=[
            AdCreative.Field.asset_feed_spec,       # Dynamic
            AdCreative.Field.object_story_spec,     # Standard
            AdCreative.Field.body, 
            AdCreative.Field.title, 
            AdCreative.Field.call_to_action_type,
        ])
        
        # --- Extraction Logic ---
        primary_texts = []
        headlines = []
        cta = "INSTALL_MOBILE_APP"
        store_url = ""  # <--- New Field

        # 1. Check Dynamic (Asset Feed)
        ad_formats = []
        full_asset_feed_spec = None
        if c_data.get('asset_feed_spec'):
            afs = c_data['asset_feed_spec']
            # Convert Facebook API object to plain dict for serialization
            def _make_serializable(obj):
                """Convert object to pickle-serializable types"""
                if obj is None:
                    return None
                elif isinstance(obj, (str, int, float, bool)):
                    return obj
                elif isinstance(obj, dict):
                    return {str(k): _make_serializable(v) for k, v in obj.items()}
                elif isinstance(obj, (list, tuple)):
                    return [_make_serializable(item) for item in obj]
                elif hasattr(obj, '__dict__'):
                    # Facebook API object - convert to dict
                    try:
                        return _make_serializable(dict(obj))
                    except:
                        return str(obj)
                else:
                    return str(obj)
            
            try:
                # Force conversion to dict first
                if hasattr(afs, '__dict__'):
                    afs_dict = dict(afs)
                elif isinstance(afs, dict):
                    afs_dict = afs
                else:
                    afs_dict = {}
                full_asset_feed_spec = _make_serializable(afs_dict)
            except Exception as e:
                logger.warning(f"Could not serialize asset_feed_spec: {e}")
                full_asset_feed_spec = {}
            
            # Extract ad_formats safely
            if isinstance(afs, dict):
                ad_formats = list(afs.get('ad_formats', [])) if afs.get('ad_formats') else []
                bodies = afs.get('bodies', [])
                titles = afs.get('titles', [])
                link_urls = afs.get('link_urls', [])
            elif hasattr(afs, 'get'):
                ad_formats = list(afs.get('ad_formats', [])) if afs.get('ad_formats') else []
                bodies = afs.get('bodies', [])
                titles = afs.get('titles', [])
                link_urls = afs.get('link_urls', [])
            else:
                ad_formats = []
                bodies = []
                titles = []
                link_urls = []
            
            primary_texts = [b.get('text') for b in bodies if b.get('text')]
            headlines = [t.get('text') for t in titles if t.get('text')]
            
            # Extract URL & CTA from link_urls
            if link_urls:
                found_cta = link_urls[0].get('call_to_action_type')
                found_url = link_urls[0].get('website_url') # <--- Get URL
                if found_cta: cta = found_cta
                if found_url: store_url = found_url

        # 2. Check Standard (Object Story)
        if not primary_texts:
            # Direct fields
            if c_data.get('body'): primary_texts.append(c_data['body'])
            if c_data.get('title'): headlines.append(c_data['title'])
            
            story_spec = c_data.get('object_story_spec', {})
            video_data = story_spec.get('video_data', {})
            
            if video_data.get('message'): primary_texts.append(video_data['message'])
            if video_data.get('title'): headlines.append(video_data['title'])
            
            # Extract CTA & URL from video_data
            cta_obj = video_data.get('call_to_action', {})
            if cta_obj:
                if cta_obj.get('type'): cta = cta_obj['type']
                if cta_obj.get('value', {}).get('link'): store_url = cta_obj['value']['link'] # <--- Get URL

        return {
            "primary_texts": list(dict.fromkeys(primary_texts)),
            "headlines": list(dict.fromkeys(headlines)),
            "call_to_action": cta,
            "store_url": store_url, # <--- Return it
            "source_ad_name": target_ad_data['name'],
            "ad_formats": ad_formats,  # ad_formats 추가
            "full_asset_feed_spec": full_asset_feed_spec,  # 전체 구조 확인용
        }

    except Exception as e:
        if _is_rate_limit_error(e):
            raise  # re-raise → @st.cache_data가 빈 결과를 캐싱하지 않음
        logger.warning(f"Could not fetch ad defaults: {e}")
        return {}

@st.cache_data(ttl=600, show_spinner=False)
def fetch_ads_in_adset(adset_id: str) -> list[dict]:
    """
    Fetch all ads in an adset and return list with name and creative data.
    Returns: [{"id": "...", "name": "...", "number": 123}, ...]
    """
    try:
        adset = AdSet(adset_id)
        ads = adset.get_ads(
            fields=[Ad.Field.name, Ad.Field.id],
            # Template Source: active ads only
            params={"limit": 100, "effective_status": ["ACTIVE"]}
        )

        result = []
        for ad in ads:
            num = _extract_number_from_name(ad['name'])
            result.append({
                "id": ad["id"],
                "name": ad["name"],
                "number": num
            })

        # Sort by number (highest first)
        result.sort(key=lambda x: x["number"], reverse=True)
        return result

    except Exception as e:
        if _is_rate_limit_error(e):
            raise  # re-raise → @st.cache_data가 빈 결과를 캐싱하지 않음
        logger.error(f"Error fetching ads: {e}")
        return []

@st.cache_data(ttl=600, show_spinner=False)
def fetch_adset_store_url_cached(adset_id: str) -> str:
    """Fetch store URL from AdSet's promoted_object (cached)."""
    try:
        adset = AdSet(adset_id)
        adset_data = adset.api_get(fields=["promoted_object"])
        promoted_obj = adset_data.get("promoted_object", {})
        return promoted_obj.get("object_store_url", "")
    except Exception as e:
        if _is_rate_limit_error(e):
            raise  # re-raise → @st.cache_data가 빈 결과를 캐싱하지 않음
        logger.warning(f"Could not fetch AdSet store URL: {e}")
        return ""

@st.cache_data(ttl=600, show_spinner=False)
def fetch_ad_creative_by_ad_id(ad_id: str) -> dict:
    """Fetch ad creative data by ad ID."""
    try:
        ad = Ad(ad_id)
        ad_data = ad.api_get(fields=[Ad.Field.name, Ad.Field.creative])
        
        c_id = ad_data['creative']['id']
        c_data = AdCreative(c_id).api_get(fields=[
            AdCreative.Field.asset_feed_spec,
            AdCreative.Field.object_story_spec,
            AdCreative.Field.body,
            AdCreative.Field.title,
            AdCreative.Field.call_to_action_type,
        ])
        
        # --- Extraction Logic (Same as fetch_latest_ad_creative_defaults) ---
        primary_texts = []
        headlines = []
        cta = "INSTALL_MOBILE_APP"
        store_url = ""
        
        # 1. Check Dynamic (Asset Feed)
        if c_data.get('asset_feed_spec'):
            afs = c_data['asset_feed_spec']
            
            # Extract bodies, titles, link_urls
            if isinstance(afs, dict):
                bodies = afs.get('bodies', [])
                titles = afs.get('titles', [])
                link_urls = afs.get('link_urls', [])
            elif hasattr(afs, 'get'):
                bodies = afs.get('bodies', [])
                titles = afs.get('titles', [])
                link_urls = afs.get('link_urls', [])
            else:
                bodies = []
                titles = []
                link_urls = []
            
            primary_texts = [b.get('text') for b in bodies if b.get('text')]
            headlines = [t.get('text') for t in titles if t.get('text')]
            
            # Extract URL & CTA from link_urls
            if link_urls:
                found_cta = link_urls[0].get('call_to_action_type')
                found_url = link_urls[0].get('website_url')
                if found_cta: cta = found_cta
                if found_url: store_url = found_url

        # 2. Check Standard (Object Story)
        if not primary_texts:
            # Direct fields
            if c_data.get('body'): primary_texts.append(c_data['body'])
            if c_data.get('title'): headlines.append(c_data['title'])
            
            story_spec = c_data.get('object_story_spec', {})
            video_data = story_spec.get('video_data', {})
            
            if video_data.get('message'): primary_texts.append(video_data['message'])
            if video_data.get('title'): headlines.append(video_data['title'])
            
            # Extract CTA & URL from video_data
            cta_obj = video_data.get('call_to_action', {})
            if cta_obj:
                if cta_obj.get('type'): cta = cta_obj['type']
                if cta_obj.get('value', {}).get('link'): store_url = cta_obj['value']['link']

        # 3. Check top-level call_to_action_type
        if c_data.get('call_to_action_type'):
            cta = c_data['call_to_action_type']

        return {
            "primary_texts": list(dict.fromkeys(primary_texts)),
            "headlines": list(dict.fromkeys(headlines)),
            "call_to_action": cta,
            "store_url": store_url,
            "source_ad_name": ad_data['name'],
        }
        
    except Exception as e:
        if _is_rate_limit_error(e):
            raise  # re-raise → @st.cache_data가 빈 결과를 캐싱하지 않음
        logger.warning(f"Could not fetch ad creative: {e}")
        return {}


# --- UI Renderer ---

def render_facebook_settings_panel(container, game: str, idx: int, **kwargs) -> None:
    """Render Facebook settings with Smart Defaults logic."""
    with container:
        st.markdown(f"#### {game} Settings")

        # --- Global rate limit cooldown guard ---
        if _is_in_cooldown():
            remaining = _cooldown_remaining()
            minutes = remaining // 60
            seconds = remaining % 60
            st.warning(
                f"⚠️ Facebook API 호출 한도 쿨다운 중입니다. "
                f"{minutes}분 {seconds}초 후에 다시 시도해주세요."
            )
            # 쿨다운 중에는 캐시된 데이터만 사용 — 새 API 호출 차단
            # 이전에 선택한 캠페인/AdSet 정보가 session_state에 있으면 표시
            prev_campaign = st.session_state.get(f"fb_c_{idx}")
            prev_adset = st.session_state.get(f"fb_a_{idx}")
            if prev_campaign:
                st.info(f"📌 마지막 선택: Campaign `{prev_campaign}` / AdSet `{prev_adset or 'N/A'}`")
            return

        # 1. Config & Selection
        cfg = FB_GAME_MAPPING.get(game)
        account_id = cfg["account_id"]

        # Campaign Select
        try:
            campaigns = fetch_active_campaigns_cached(account_id)
        except FacebookRequestError as e:
            if _is_rate_limit_error(e):
                _handle_rate_limit(e, "campaigns 조회")
                return
            raise
        if not campaigns: return
        
        c_opts = [f"{c['name']} ({c['id']})" for c in campaigns]
        c_ids = [c["id"] for c in campaigns]
        
        c_key = f"fb_c_{idx}"
        def_c_idx = 0
        if st.session_state.get(c_key) in c_ids: 
            def_c_idx = c_ids.index(st.session_state[c_key])
        
        sel_c_lbl = st.selectbox("Select Campaign", c_opts, index=def_c_idx, key=f"sel_c_{idx}")
        sel_c_id = c_ids[c_opts.index(sel_c_lbl)]
        st.session_state[c_key] = sel_c_id
        
        # AdSet Select
        try:
            adsets = fetch_active_adsets_cached(account_id, sel_c_id)
        except FacebookRequestError as e:
            if _is_rate_limit_error(e):
                _handle_rate_limit(e, "adsets 조회")
                return
            raise
        if not adsets: return
        
        a_opts = [f"{a['name']} ({a['id']})" for a in adsets]
        a_ids = [a["id"] for a in adsets]
        
        a_key = f"fb_a_{idx}"
        def_a_idx = 0
        if st.session_state.get(a_key) in a_ids: 
            def_a_idx = a_ids.index(st.session_state[a_key])

        sel_a_lbl = st.selectbox("Select Ad Set", a_opts, index=def_a_idx, key=f"sel_a_{idx}")
        sel_a_id = a_ids[a_opts.index(sel_a_lbl)]
        
        # Reset fetch flag if AdSet changes
        # Reset fetch flag if AdSet changes
        if st.session_state.get(f"prev_fb_a_{idx}") != sel_a_id:
            st.session_state[f"defaults_fetched_{idx}"] = False
            # Reset primary texts and headlines when AdSet changes
            st.session_state.pop(f"primary_texts_{idx}", None)
            st.session_state.pop(f"headlines_{idx}", None)
            # ✅ Clear all template cache when AdSet changes
            st.session_state.pop(f"template_source_{idx}", None)
            for key in list(st.session_state.keys()):
                if key.startswith(f"defaults_fetched_") and f"_{idx}" in key:
                    st.session_state.pop(key, None)
                if key.startswith(f"mimic_data_") and f"_{idx}" in key:
                    st.session_state.pop(key, None)
        st.session_state[f"prev_fb_a_{idx}"] = sel_a_id
        st.session_state[a_key] = sel_a_id

        st.divider()

        # ====================================================================
        # TEMPLATE SOURCE SELECTION
        # ====================================================================
        st.markdown("**📋 Template Source**")

        # Fetch ad list
        try:
            ads_in_adset = fetch_ads_in_adset(sel_a_id)
        except FacebookRequestError as e:
            if _is_rate_limit_error(e):
                _handle_rate_limit(e, "ads 목록 조회")
                return
            raise

        # Build options: [빈칸] + [Highest Number (Auto)] + [All Ads]
        template_options = ["빈칸 (Empty)"]

        if ads_in_adset:
            highest_ad = ads_in_adset[0]  # Already sorted by number desc
            template_options.append(f"🏆 {highest_ad['name']} (Auto)")
            
            # Add all other ads
            for ad in ads_in_adset:
                template_options.append(f"📄 {ad['name']}")

        # Get current selection
        template_key = f"template_source_{idx}"
        current_selection = st.session_state.get(template_key, template_options[1] if len(template_options) > 1 else template_options[0])

        # Selectbox
        selected_template = st.selectbox(
            "Select Template Source",
            options=template_options,
            index=template_options.index(current_selection) if current_selection in template_options else 0,
            key=f"template_sel_{idx}",
            help="Choose which ad to copy text/headlines/CTA from, or select 빈칸 for empty values"
        )

        st.session_state[template_key] = selected_template


        prev_template_key = f"prev_template_{idx}"
        tpl_ver_key = f"tpl_ver_{idx}"

        # init
        if tpl_ver_key not in st.session_state:
            st.session_state[tpl_ver_key] = 0

        if st.session_state.get(prev_template_key) != selected_template:
            # Template changed -> bump version so widget keys change (forces UI refresh)
            st.session_state[tpl_ver_key] += 1

            # Reset stored lists (so they re-seed from new defaults)
            st.session_state.pop(f"primary_texts_{idx}", None)
            st.session_state.pop(f"headlines_{idx}", None)

            # (optional legacy) clear any signatures if you still use them
            st.session_state.pop(f"headline_template_sig_{idx}", None)
            st.session_state.pop(f"primary_text_template_sig_{idx}", None)

            st.session_state[prev_template_key] = selected_template

        # ====================================================================
        # LOAD TEMPLATE DATA
        # ====================================================================
        defaults = {}

        try:
            if selected_template == "빈칸 (Empty)":
                # Empty template - no defaults but keep store URL from AdSet
                st.info("ℹ️ Using empty template (no text/headlines/CTA will be copied)")

                # ✅ Fetch store URL from AdSet's promoted_object (cached via @st.cache_data)
                store_url = fetch_adset_store_url_cached(sel_a_id)

                defaults = {
                    "primary_texts": [],
                    "headlines": [],
                    "call_to_action": "INSTALL_MOBILE_APP",
                    "store_url": store_url,
                    "source_ad_name": "Empty Template"
                }

            elif selected_template.startswith("🏆"):
                # Auto mode - highest number
                defaults_flag = f"defaults_fetched_auto_{idx}"

                if not st.session_state.get(defaults_flag, False):
                    with st.spinner("Loading template from highest ad..."):
                        defaults = fetch_latest_ad_creative_defaults(sel_a_id)
                        st.session_state[f"mimic_data_auto_{idx}"] = defaults
                    st.session_state[defaults_flag] = True
                else:
                    defaults = st.session_state.get(f"mimic_data_auto_{idx}", {})

            elif selected_template.startswith("📄"):
                # Specific ad selected
                ad_name = selected_template.replace("📄 ", "")
                selected_ad = next((a for a in ads_in_adset if a["name"] == ad_name), None)

                if selected_ad:
                    defaults_flag = f"defaults_fetched_{selected_ad['id']}_{idx}"

                    if not st.session_state.get(defaults_flag, False):
                        with st.spinner(f"Loading template from {ad_name}..."):
                            defaults = fetch_ad_creative_by_ad_id(selected_ad['id'])
                            st.session_state[f"mimic_data_{selected_ad['id']}_{idx}"] = defaults
                            st.session_state[defaults_flag] = True
                    else:
                        defaults = st.session_state.get(f"mimic_data_{selected_ad['id']}_{idx}", {})

        except FacebookRequestError as e:
            if _is_rate_limit_error(e):
                _handle_rate_limit(e, "template 데이터 조회")
                return
            logger.warning(f"Could not fetch template data: {e}")

        # ====================================================================
        # PREPARE DEFAULT VALUES (rest stays the same)
        # ====================================================================
        val_text = ""
        val_headline = ""
        val_cta_idx = 0
        source_msg = ""
        h_lines = []
        p_texts = []

        if defaults:
            p_texts = defaults.get("primary_texts", [])
            val_text = "\n\n".join(p_texts) if p_texts else ""
            
            h_lines = defaults.get("headlines", [])
            val_headline = h_lines[0] if h_lines else ""
            
            # [NEW] Get Store URL
            val_store_url = defaults.get("store_url", "")
            
            fetched_cta = defaults.get("call_to_action", "INSTALL_MOBILE_APP")
            if fetched_cta in FB_CTA_OPTIONS:
                val_cta_idx = FB_CTA_OPTIONS.index(fetched_cta)
            
            source_msg = f"✨ Loaded from: **{defaults.get('source_ad_name')}**"
            
            # Display ad_formats if available
            ad_formats = defaults.get('ad_formats', [])
            if ad_formats:
                source_msg += f"\n\n📋 **ad_formats**: `{ad_formats}`"
                
                # Display full asset_feed_spec in expander for debugging
                # if defaults.get('full_asset_feed_spec'):
                #     with st.expander("🔍 View Full asset_feed_spec (Debug)", expanded=False):
                #         spec = defaults.get('full_asset_feed_spec')
                #         # Ensure it's a dict before passing to st.json
                #         if isinstance(spec, dict):
                #             st.json(spec)
                #         else:
                #             st.code(str(spec), language='text')
        # 2. Ad Setup
        st.caption("Ad Setup")
        
        # Ad Format & Ad Name
        col_d1, col_d2 = st.columns(2)
        dco_aspect_ratio = col_d1.selectbox(
            "Ad Format", 
            ["단일 영상", "다이내믹-single video", "다이내믹-1x1", "다이내믹-9x16", "다이내믹-16:9"], 
            key=f"dco_r_{idx}"
        )
        
        # Ad Name은 다이내믹일 때만 표시
        if dco_aspect_ratio.startswith("다이내믹"):
            ad_name_input = col_d2.text_input("Ad Name", key=f"dco_n_{idx}")
        else:
            # 단일 영상일 때는 Ad Name 숨김 (기본값 사용)
            ad_name_input = ""
            col_d2.empty()  # 빈 공간 유지
        st.markdown("**Ad Name Customization** (Optional)")
        
        col_pre, col_suf = st.columns(2)
        
        with col_pre:
            use_prefix = st.checkbox("Add Prefix", key=f"use_prefix_{idx}")
            if use_prefix:
                prefix_text = st.text_input(
                    "Prefix", 
                    key=f"prefix_text_{idx}",
                    placeholder="e.g., a",
                    help="Result: a_video164"
                )
            else:
                prefix_text = ""
        
        with col_suf:
            use_suffix = st.checkbox("Add Suffix", key=f"use_suffix_{idx}")
            if use_suffix:
                suffix_text = st.text_input(
                    "Suffix", 
                    key=f"suffix_text_{idx}",
                    placeholder="e.g., a",
                    help="Result: video164_a"
                )
            else:
                suffix_text = ""
        
        # Preview
        if use_prefix or use_suffix:
            preview_name = ""
            if use_prefix and prefix_text:
                preview_name = f"{prefix_text}_"
            preview_name += "videoxxx"
            if use_suffix and suffix_text:
                preview_name += f"_{suffix_text}"
            st.caption(f"📝 Preview: `{preview_name}`")

        st.divider()

        # 3. Ad Creative Inputs
        col_head, col_info = st.columns([1, 2])
        col_head.caption("Creative Elements")
        if source_msg:
            col_info.info(source_msg, icon="🤖")

        # ✅ Primary Text - 태그 형태로 개별 관리
        st.markdown("**Primary Text**")

        primary_texts_key = f"primary_texts_{idx}"

        # 템플릿이 바뀌었는지 확인
        current_template = st.session_state.get(f"template_source_{idx}", "")
        stored_template = st.session_state.get(f"loaded_template_for_pt_{idx}", "")

        # 조건: 템플릿이 바뀌었거나 Session State가 비어있으면 초기화
        if stored_template != current_template or primary_texts_key not in st.session_state:
            if p_texts:
                st.session_state[primary_texts_key] = p_texts.copy()
            else:
                st.session_state[primary_texts_key] = [""]
            st.session_state[f"loaded_template_for_pt_{idx}"] = current_template

        primary_texts_list = st.session_state[primary_texts_key]
                
        # Display each primary text as editable tag
        for i, text in enumerate(primary_texts_list):
            col_text, col_del = st.columns([10, 1])
            with col_text:
                tpl_ver = st.session_state.get(f"tpl_ver_{idx}", 0)

                updated_text = st.text_input(
                    f"Primary Text {i+1}",
                    value=text,
                    key=f"pt_{idx}_{tpl_ver}_{i}",
                    label_visibility="collapsed",
                    placeholder="Tell people what your ad is about" if not text else None
                )
                primary_texts_list[i] = updated_text
            with col_del:
                if st.button("❌", key=f"pt_del_{idx}_{i}", help="Delete this text"):
                    primary_texts_list.pop(i)
                    st.session_state[primary_texts_key] = primary_texts_list.copy()
                    st.rerun()
        
        # Add new primary text button
        tpl_ver = st.session_state.get(f"tpl_ver_{idx}", 0)
        if st.button("➕ Add Primary Text", key=f"pt_add_{idx}_{tpl_ver}"):
            primary_texts_list.append("")
            st.session_state[primary_texts_key] = primary_texts_list.copy()
            st.rerun()
        
        # Join primary texts with double newline for backward compatibility
        primary_text = "\n\n".join([t.strip() for t in primary_texts_list if t.strip()])

        # ✅ Headlines - 태그 형태로 개별 관리
        # ✅ Headlines
        st.markdown("**Headlines**")

        headlines_key = f"headlines_{idx}"

        # 템플릿이 바뀌었는지 확인
        current_template = st.session_state.get(f"template_source_{idx}", "")
        stored_template = st.session_state.get(f"loaded_template_for_hl_{idx}", "")

        # 조건: 템플릿이 바뀌었거나 Session State가 비어있으면 초기화
        if stored_template != current_template or headlines_key not in st.session_state:
            if h_lines:
                st.session_state[headlines_key] = h_lines.copy()
            else:
                st.session_state[headlines_key] = [""]
            st.session_state[f"loaded_template_for_hl_{idx}"] = current_template

        headlines_list = st.session_state[headlines_key]
        
        # Display each headline as editable tag
        for i, headline_text in enumerate(headlines_list):
            col_head, col_del = st.columns([10, 1])
            with col_head:
                tpl_ver = st.session_state.get(f"tpl_ver_{idx}", 0)

                updated_headline = st.text_input(
                    f"Headline {i+1}",
                    value=headline_text,
                    key=f"hl_{idx}_{tpl_ver}_{i}",
                    label_visibility="collapsed",
                    placeholder="Write a short headline" if not headline_text else None
                )
                headlines_list[i] = updated_headline
            with col_del:
                if st.button("❌", key=f"hl_del_{idx}_{i}", help="Delete this headline"):
                    headlines_list.pop(i)
                    st.session_state[headlines_key] = headlines_list.copy()
                    st.rerun()
        
        # Add new headline button
        tpl_ver = st.session_state.get(f"tpl_ver_{idx}", 0)
        if st.button("➕ Add Headline", key=f"hl_add_{idx}_{tpl_ver}"):
            st.session_state[headlines_key].append("")
            st.rerun()
        
        # ✅ 루프 후 최신 값으로 업데이트
        st.session_state[headlines_key] = headlines_list
        
        # Join headlines with newline for backward compatibility
        headline = "\n".join([h.strip() for h in headlines_list if h.strip()])

        # CTA
        call_to_action = st.selectbox(
            "Call to Action", 
            FB_CTA_OPTIONS, 
            index=val_cta_idx,
            key=f"cta_{idx}"
        )

        # ✅ 처음 렌더링될 때만 default OFF 주입 (이후엔 유저 선택 유지)
        _multi_key = f"multi_ads_optin_{idx}"
        if _multi_key not in st.session_state:
            st.session_state[_multi_key] = False  # default = OFF

        multi_advertiser_ads_opt_in = st.checkbox(
            "Multi-advertiser ads 사용하기 (같은 유닛에 다른 광고와 함께 노출될 수 있음)",
            key=_multi_key,
        )

        # Final Save
        # ✅ UI에서 관리하는 "리스트"를 그대로 저장 (빈값 포함 허용)
        _clean_keep_empty = lambda xs: [x if x is not None else "" for x in (xs or [])]

        st.session_state.settings[game] = {
            "campaign_id": sel_c_id,
            "adset_id": sel_a_id,
            "creative_type": "Dynamic Creative",
            "dco_aspect_ratio": dco_aspect_ratio,
            "dco_creative_name": ad_name_input,
            "single_creative_name": None,

            # ✅ backward compatibility (기존 로직용 문자열도 유지)
            "primary_text": primary_text,     # "\n\n" join된 문자열
            "headline": headline,             # "\n" join된 문자열

            # ✅ NEW: 업로드 로직에서는 이 리스트를 우선 사용
            "primary_texts": _clean_keep_empty(primary_texts_list),   # 빈칸 포함
            "headlines": _clean_keep_empty(headlines_list),           # 빈칸 포함

            "call_to_action": call_to_action,

            # ✅ NEW: template에서 가져온 store_url을 settings에도 저장 (Marketer mode에서 그대로 쓰기 좋음)
            "store_url": defaults.get("store_url", "") if defaults else (st.session_state.get("store_url") or ""),

            "use_prefix": use_prefix,
            "prefix_text": prefix_text.strip() if use_prefix else "",
            "use_suffix": use_suffix,
            "suffix_text": suffix_text.strip() if use_suffix else "",
            "multi_advertiser_ads_opt_in": bool(multi_advertiser_ads_opt_in),
        }


        # --------------------------------------------------------------------
# Main Execution Function (Add this to the bottom of fb.py)
# --------------------------------------------------------------------

def upload_to_facebook(
    game_name: str,
    uploaded_files: list,
    settings: dict,
) -> dict:
    """
    Marketer Mode: 선택된 AdSet에 바로 업로드
    Test Mode: 새 AdSet 생성 후 업로드
    """
    
    if game_name not in FB_GAME_MAPPING:
        raise ValueError(f"No FB mapping configured for game: {game_name}")

    cfg = FB_GAME_MAPPING[game_name]
    account = init_fb_from_secrets(cfg["account_id"])

    # Page ID 가져오기
    page_id_key = cfg.get("page_id_key")
    if "facebook" in st.secrets and page_id_key in st.secrets["facebook"]:
        page_id = st.secrets["facebook"][page_id_key]
    elif page_id_key in st.secrets:
        page_id = st.secrets[page_id_key]
    else:
        raise RuntimeError(f"Missing {page_id_key} in secrets.")

    # Validate Page
    page_check = validate_page_binding(account, page_id)

    settings = dict(settings or {})
    
    # ✅ Marketer Mode: 선택된 AdSet 확인
    selected_adset_id = settings.get("adset_id")
    
    if selected_adset_id:
        # ========================================
        # MARKETER MODE: 선택된 AdSet에 바로 업로드
        # ========================================
        _dev_info("📌 Marketer Mode: 선택된 Ad Set에 업로드")
        
        # Store URL 가져오기
        game_defaults = GAME_DEFAULTS.get(game_name, {})
        user_store_url = (settings.get("store_url") or "").strip()
        
        # [SMART URL LOGIC] - 기존 코드 유지
        target_campaign_name = cfg.get("campaign_name", "").lower()
        is_ios_campaign = "ios" in target_campaign_name
        
        if is_ios_campaign:
            default_url = game_defaults.get("store_url_ios", "")
            if not default_url: default_url = game_defaults.get("store_url", "")
        else:
            default_url = game_defaults.get("store_url_aos", "")
            if not default_url: default_url = game_defaults.get("store_url", "")
        
        store_url = user_store_url if user_store_url else default_url
        
        if store_url:
            store_url = sanitize_store_url(store_url)
        
        # ✅ 핵심: upload_videos_to_library_and_create_single_ads 사용
        # game_name을 settings에 추가하여 전달
        settings_with_game = dict(settings)
        settings_with_game["game_name"] = game_name
        result = upload_videos_to_library_and_create_single_ads(
            account=account,
            page_id=str(page_id),
            adset_id=selected_adset_id,
            uploaded_files=uploaded_files,
            settings=settings_with_game,
            store_url=store_url,
            max_workers=6
        )
        
        # result가 None인 경우 처리
        if result is None:
            result = {
                "ads": [],
                "errors": ["업로드 결과를 가져올 수 없습니다."],
                "total_created": 0,
                "uploads_map": {}
            }
        
        return {
            "campaign_id": settings.get("campaign_id"),
            "adset_id": selected_adset_id,
            "adset_name": "(Selected Ad Set)",
            "page_id": str(page_id),
            "n_videos": len(uploaded_files),
            "ads_created": result.get("total_created", 0),
            "errors": result.get("errors", [])
        }
    
    # TEST MODE: Create new creativetest adset
    ui_campaign_id = settings.get("campaign_id")
    final_campaign_id = ui_campaign_id if ui_campaign_id else cfg["campaign_id"]

    # [FIX] Construct Ad Set Prefix dynamically if possible, or use config
    # If we are in "Marketer Mode" (UI selection), we might want to adopt the campaign name?
    # For now, let's stick to the config prefix but acknowledge the campaign ID change.
    
    # Also, ensure we pass the correct App Store URL if missing
    if not settings.get("store_url"):
        # (Your existing smart URL logic runs later, but _plan_upload might need it for budgeting? 
        # Actually _plan_upload doesn't use URL, so it's fine).
        pass

    plan = _plan_upload(
        account=account,
        campaign_id=final_campaign_id, # <--- FIXED: Uses UI selection
        adset_prefix=cfg["adset_prefix"], 
        page_id=str(page_id),
        uploaded_files=uploaded_files,
        settings=settings,
    )

    # 3. Create the Ad Set (Targeting & Optimization)
    
    # Build Targeting Spec (Country, OS, Age)
    targeting = build_targeting_from_settings(
        countries=plan["countries"],
        age_min=plan["age_min"],
        settings=settings,
    )

    # Determine Optimization Goal
    opt_goal_label = settings.get("opt_goal_label") or "앱 설치수 극대화"
    opt_goal_api = OPT_GOAL_LABEL_TO_API.get(opt_goal_label, "APP_INSTALLS")

    # Determine Promoted Object (App Store URL)
    # --- SMART URL LOGIC START ---
    
    # 1. Get User Input first
    # -----------------------------------------------------------
    # [SMART URL LOGIC] 캠페인 이름 기반 자동 스토어 링크 선택
    # -----------------------------------------------------------
    
    # 1. 사용자 입력 우선 확인
    user_store_url = (settings.get("store_url") or "").strip()
    user_app_id = (settings.get("fb_app_id") or "").strip()
    
    # 2. 게임 기본값 딕셔너리 가져오기
    game_defaults = GAME_DEFAULTS.get(game_name, {})
    
    # 3. OS 판단 로직 (우선순위: 캠페인 이름 > 설정값)
    # FB_GAME_MAPPING에 정의된 캠페인 이름 가져오기
    target_campaign_name = cfg.get("campaign_name", "").lower()
    
    is_ios_campaign = False
    
    if "ios" in target_campaign_name:
        is_ios_campaign = True
    elif "aos" in target_campaign_name:
        is_ios_campaign = False
    else:
        # 캠페인 이름에 OS 정보가 없을 경우, 설정값(os_choice)을 확인
        if settings.get("os_choice") == "iOS only":
            is_ios_campaign = True

    # 4. 판단된 OS에 맞는 URL 가져오기
    if is_ios_campaign:
        # iOS일 경우
        default_url = game_defaults.get("store_url_ios", "")
        # iOS 전용 링크가 없으면 공통 링크(store_url) 시도
        if not default_url: default_url = game_defaults.get("store_url", "")
    else:
        # AOS(Android)일 경우
        default_url = game_defaults.get("store_url_aos", "")
        # AOS 전용 링크가 없으면 공통 링크(store_url) 시도
        if not default_url: default_url = game_defaults.get("store_url", "")

    default_app_id = game_defaults.get("fb_app_id", "")

    # 5. 최종 결정 (사용자 입력이 있으면 무조건 그것을 사용)
    store_url = user_store_url if user_store_url else default_url
    fb_app_id = user_app_id if user_app_id else default_app_id

    # 6. URL 정리 (트래킹 파라미터 제거 등)
    if store_url:
        store_url = sanitize_store_url(store_url)
        
    # --- SMART URL LOGIC END ---

    promoted_object = None
    if opt_goal_api in ("APP_INSTALLS", "APP_EVENTS", "VALUE"):
        if not store_url:
            raise RuntimeError("App objective selected but Store URL is missing.")
        promoted_object = {
            "object_store_url": store_url,
            **({"application_id": fb_app_id} if fb_app_id else {}),
        }

    # Execute Ad Set Creation
    adset_id = create_creativetest_adset(
        account=account,
        campaign_id=final_campaign_id,
        adset_name=plan["adset_name"],
        targeting=targeting,
        daily_budget_usd=plan["budget_usd_per_day"],
        start_iso=plan["start_iso"],
        optimization_goal=opt_goal_api,
        promoted_object=promoted_object,
        end_iso=plan.get("end_iso"),
    )

    if not adset_id:
        raise RuntimeError("Ad set creation failed (no ID returned).")

    # 4. Upload Videos & Create Ads
    # [CRITICAL] This calls our new logic for Grouping/Multi-Text
    ad_name_prefix = settings.get("ad_name_prefix") if settings.get("ad_name_mode") == "Prefix + filename" else None

    upload_videos_create_ads(
        account=account,
        page_id=str(page_id),
        adset_id=adset_id,
        uploaded_files=uploaded_files,
        ad_name_prefix=ad_name_prefix,
        store_url=store_url,
        try_instagram=False,
        settings=settings,  # <--- WE MUST PASS SETTINGS HERE
    )

    plan["adset_id"] = adset_id
    return plan

    # fb.py 하단에 추가

# fb.py 최하단 (upload_to_facebook 함수 아래에 추가)

def upload_videos_to_library_and_create_single_ads(
    account,
    page_id: str,
    adset_id: str,
    uploaded_files: list,
    settings: dict,
    store_url: str = None,
    max_workers: int = 6
) -> dict:
    """
    1. Upload videos to Ad Library (with original filename as title)
    2. Create Single Video Ads (단일 영상) or Flexible Ads (다이내믹)
    """
    
    # Ad Format 확인
    dco_aspect_ratio = settings.get("dco_aspect_ratio", "단일 영상")
    is_dynamic_single_video = (dco_aspect_ratio == "다이내믹-single video")
    is_dynamic_1x1 = (dco_aspect_ratio == "다이내믹-1x1")
    is_dynamic_9x16 = (dco_aspect_ratio == "다이내믹-9x16")
    is_dynamic_16x9 = (dco_aspect_ratio == "다이내믹-16:9")
    
    if is_dynamic_single_video:
        # 다이내믹-single video 모드로 처리
        return _upload_dynamic_single_video_ads(
            account, page_id, adset_id, uploaded_files,
            settings, store_url, max_workers
        )
    
    if is_dynamic_1x1:
        # 다이내믹-1x1 모드로 처리
        # game_name은 파일명에서 추출 시도 후 fallback으로만 사용 (없어도 진행)
        game_name = (settings.get("game_name") or "").strip()
        return _upload_dynamic_1x1_ads(
            account, page_id, adset_id, uploaded_files,
            settings, store_url, max_workers, game_name
        )

    if is_dynamic_16x9:
        # 다이내믹-16:9 모드로 처리
        # game_name은 파일명에서 추출 시도 후 fallback으로만 사용 (없어도 진행)
        game_name = (settings.get("game_name") or "").strip()
        return _upload_dynamic_16x9_ads(
            account, page_id, adset_id, uploaded_files,
            settings, store_url, max_workers, game_name
        )

    if is_dynamic_9x16:
        # 다이내믹-9x16 모드로 처리
        # game_name은 파일명에서 추출 시도 후 fallback으로만 사용 (없어도 진행)
        game_name = (settings.get("game_name") or "").strip()
        return _upload_dynamic_9x16_ads(
            account, page_id, adset_id, uploaded_files,
            settings, store_url, max_workers, game_name
        )
    
    # 기존 단일 영상 로직 그대로 실행 (아래 코드는 변경 없음)
    # st.write("🔧 **DEBUG: upload_videos_to_library_and_create_single_ads 실행 중**")
    # st.write(f"- 업로드된 파일 수: {len(uploaded_files)}")
    # st.write(f"- Ad Set ID: {adset_id}")
    # st.write(f"- Settings: {settings.keys()}")
    
    # Prefix/Suffix 설정 확인
    use_prefix = settings.get("use_prefix", False)
    prefix_text = settings.get("prefix_text", "").strip()
    use_suffix = settings.get("use_suffix", False)
    suffix_text = settings.get("suffix_text", "").strip()
    
    # st.write(f"- Prefix: {'✅ ' + prefix_text if use_prefix else '❌'}")
    # st.write(f"- Suffix: {'✅ ' + suffix_text if use_suffix else '❌'}")
    try:
        adset = AdSet(adset_id)
        adset_data = adset.api_get(fields=["promoted_object"])
        promoted_obj = adset_data.get("promoted_object", {})
        adset_store_url = promoted_obj.get("object_store_url", "")
        
        if adset_store_url:
            _dev_info(f"📌 Ad Set의 Store URL: {adset_store_url[:60]}...")
            # ✅ Ad Set URL을 최우선으로 사용 (일치 보장)
            if not store_url:
                store_url = adset_store_url
                _dev_success("✅ Ad Set URL을 사용합니다 (일치 보장)")
        else:
            _dev_warning("⚠️ Ad Set에 promoted_object가 없습니다")
    except Exception as e:
        devtools.record_exception("Ad Set 조회 실패", e)
        _dev_warning(f"⚠️ Ad Set 조회 실패: {e}")
    # ====================================================================
    # STEP 0: Get template from highest video in AdSet
    # ====================================================================
    _dev_info("🔍 AdSet에서 템플릿 정보 가져오는 중...")
    template = fetch_latest_ad_creative_defaults(adset_id)

    # ✅ 디버그 출력
    # st.write("**🔍 Debug: Template Data**")
    # st.json({
    #     "primary_texts": template.get("primary_texts", []),
    #     "headlines": template.get("headlines", []),
    #     "cta": template.get("call_to_action", ""),
    #     "store_url": template.get("store_url", "")[:50] if template.get("store_url") else ""
    # })

    # ✅ 모든 Primary Text 복사
    default_primary_texts = []
    if template.get("primary_texts") and len(template["primary_texts"]) > 0:
        default_primary_texts = template["primary_texts"]
        # st.write(f"✅ Loaded {len(default_primary_texts)} primary texts from template")
    elif settings.get("primary_text"):
        text = settings["primary_text"].strip()
        default_primary_texts = [t.strip() for t in text.split('\n\n') if t.strip()] if text else []
        # st.write(f"✅ Loaded {len(default_primary_texts)} primary texts from settings")
    else:
        _dev_warning("⚠️ No primary texts found in template or settings!")

    # ✅ 모든 Primary Text 복사 (배열 그대로)
    default_primary_texts = []
    if template.get("primary_texts") and len(template["primary_texts"]) > 0:
        default_primary_texts = template["primary_texts"]
    elif settings.get("primary_text"):
        # Settings에서 온 경우 '\n\n'로 split
        text = settings["primary_text"].strip()
        default_primary_texts = [t.strip() for t in text.split('\n\n') if t.strip()] if text else []
    
    # 디버그 출력은 선택적으로
    if default_primary_texts:
        _dev_write(f"✅ Loaded {len(default_primary_texts)} primary texts")
    else:
        _dev_warning("⚠️ No primary texts found in template or settings!")

    # ✅ 모든 Headline 복사 (배열 그대로)
    default_headlines = []
    if template.get("headlines") and len(template["headlines"]) > 0:
        # "New Game"을 빈 문자열로 변환
        default_headlines = ["" if h.strip().lower() == "new game" else h for h in template["headlines"]]
    elif settings.get("headline"):
        # Settings에서 온 경우 '\n'로 split
        headline = settings["headline"].strip()
        default_headlines = ["" if h.strip().lower() == "new game" else h.strip() for h in headline.split('\n') if h.strip()] if headline else []

    # ✅ CTA 우선순위: UI(settings) > template > default
    default_cta = (settings.get("call_to_action") or "").strip()
    if not default_cta:
        default_cta = (template.get("call_to_action") or "").strip()
    if not default_cta:
        default_cta = "INSTALL_MOBILE_APP"

    # ✅ Store URL 결정 순서:
    # AdSet의 promoted_object URL이 있으면 무조건 사용 (일치 보장 필수)
    # 없을 때만 다른 소스 사용
    
    if adset_store_url:
        # AdSet URL을 최우선으로 사용 (일치 보장)
        final_store_url = sanitize_store_url(adset_store_url)
        _dev_info(f"✅ Ad Set의 Store URL 사용: {final_store_url[:50]}...")
    else:
        # AdSet URL이 없을 때만 다른 소스 사용
        final_store_url = store_url  # 인자로 받은 값
        
        if not final_store_url and settings.get("store_url"):
            final_store_url = settings["store_url"]
        elif not final_store_url and template.get("store_url"):
            final_store_url = template["store_url"]

    if final_store_url:
        final_store_url = sanitize_store_url(final_store_url)
    
    # 결과 출력
    if devtools.dev_enabled():
        st.success(f"✅ 템플릿 로드 완료 (from: {template.get('source_ad_name', 'N/A')})")

        if default_primary_texts:
            st.caption(f"📝 Primary Texts: {len(default_primary_texts)}개")
            with st.expander("Primary Text 목록 보기", expanded=False):
                for idx, text in enumerate(default_primary_texts, 1):
                    st.write(f"{idx}. {text[:80]}...")
        else:
            _dev_warning("⚠️ Primary Text 없음")

        if default_headlines:
            st.caption(f"📰 Headlines: {len(default_headlines)}개")
            with st.expander("Headline 목록 보기", expanded=False):
                for idx, h in enumerate(default_headlines, 1):
                    st.write(f"{idx}. {h}")
        else:
            _dev_warning("⚠️ Headline 없음")

        st.caption(f"🎯 CTA: {default_cta}")

        if final_store_url:
            st.caption(f"🔗 Store URL: {final_store_url[:50]}...")

    if not final_store_url:
        st.error("❌ Store URL이 없습니다! 앱 설치 광고는 URL이 필수입니다.")
    
    use_prefix = settings.get("use_prefix", False)
    prefix_text = settings.get("prefix_text", "").strip()
    use_suffix = settings.get("use_suffix", False)
    suffix_text = settings.get("suffix_text", "").strip()

    def _build_ad_name(video_num: str) -> str:
        """Build ad name with optional prefix/suffix"""
        name_parts = []
        
        if use_prefix and prefix_text:
            name_parts.append(prefix_text)
        
        name_parts.append(video_num)
        
        if use_suffix and suffix_text:
            name_parts.append(suffix_text)
        
        return "_".join(name_parts)
    
    # ====================================================================
    # STEP 1: Group videos by base name (video164, video165, ...)
    # ====================================================================
    def _extract_video_number(fname):
        """Extract video number from filename (e.g., video164)"""
        match = re.search(r'video(\d+)', fname.lower())
        return f"video{match.group(1)}" if match else None
    def _extract_resolution(fname):
        """Extract resolution from filename (e.g., 1080x1080)"""
        if "1080x1080" in fname.lower():
            return "1080x1080"
        elif "1920x1080" in fname.lower():
            return "1920x1080"
        elif "1080x1920" in fname.lower():
            return "1080x1920"
        return None
    
    video_groups = {}
    
    for u in uploaded_files:
        fname = getattr(u, "name", None) or u.get("name", "")
        if not fname: 
            continue
        
        video_num = _extract_video_number(fname)
        resolution = _extract_resolution(fname)
        
        if not video_num:
            st.warning(f"⚠️ 파일명 형식 오류: {fname} (video 번호 누락)")
            continue
        
        if not resolution:
            st.warning(f"⚠️ 해상도 인식 실패: {fname} (1080x1080, 1920x1080, 1080x1920 필요)")
            continue
        
        if video_num not in video_groups:
            video_groups[video_num] = {}
        
        video_groups[video_num][resolution] = u

    # Dev-only: grouping debug
    if devtools.dev_enabled():
        st.write("📦 **그룹화 결과:**")
        for video_num, resolutions in video_groups.items():
            st.write(f"- {video_num}: {list(resolutions.keys())}")
    
    # ✅ 해상도 우선순위에 따라 최적 비디오 선택
    valid_groups = {}
    RESOLUTION_PRIORITY = ["1080x1080", "1920x1080", "1080x1920"]
    
    for video_num, files in video_groups.items():
        selected_resolution = None
        selected_file = None
        
        # 우선순위대로 해상도 찾기
        for res in RESOLUTION_PRIORITY:
            if res in files:
                selected_resolution = res
                selected_file = files[res]
                break
        
        if selected_resolution:
            valid_groups[video_num] = {
                "resolution": selected_resolution,
                "file": selected_file
            }
            # 우선순위 정보 표시
            if selected_resolution != "1080x1080":
                _dev_info(f"ℹ️ {video_num}: 1080x1080 없음, {selected_resolution} 사용")
        else:
            st.error(f"❌ {video_num}: 사용 가능한 해상도 없음 (1080x1080, 1920x1080, 1080x1920 필요)")
    if not valid_groups:
        raise RuntimeError("❌ 유효한 비디오 그룹이 없습니다. 각 video는 1080x1080, 1920x1080, 또는 1080x1920 해상도가 필요합니다.")

    # Dev-only: selected-resolution debug
    if devtools.dev_enabled():
        st.write("✅ **최종 선택된 비디오:**")
        for video_num, data in valid_groups.items():
            st.write(f"- {video_num}: {data['resolution']}")

    _dev_success(f"✅ {len(valid_groups)}개 비디오 검증 완료")

    # 해상도별 통계 표시
    resolution_stats = {}
    for vg in valid_groups.values():
        res = vg["resolution"]
        resolution_stats[res] = resolution_stats.get(res, 0) + 1

    if devtools.dev_enabled():
        st.caption("📊 사용된 해상도:")
        for res, count in sorted(resolution_stats.items()):
            st.caption(f"  - {res}: {count}개")
    
    if not valid_groups:
        raise RuntimeError("❌ 유효한 비디오 그룹이 없습니다. 각 video는 1080x1080 해상도가 필요합니다.")
    
    _dev_success(f"✅ {len(valid_groups)}개 비디오 검증 완료 (1080x1080만 사용)")
    
    # ====================================================================
    # STEP 2: Upload videos to Ad Library (with original filename)
    # ====================================================================
    def _save_tmp(u):
        if isinstance(u, dict) and "path" in u:
            return {"name": u["name"], "path": u["path"]}
        if hasattr(u, "getbuffer"):
            suffix = pathlib.Path(u.name).suffix.lower() or ".mp4"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(u.getbuffer())
                return {"name": u.name, "path": tmp.name}
        raise ValueError("Unsupported video object")
    
    def _upload_video_with_title(path: str, title: str) -> tuple:
        """Upload video with original filename as title"""
        if "facebook" in st.secrets:
            token = st.secrets["facebook"].get("access_token", "").strip()
        else:
            token = st.secrets.get("access_token", "").strip()
        
        act = account.get_id()
        file_size = os.path.getsize(path)
        
        def _post(data, files=None):
            sess = _get_session()

            def _do():
                req = build_advideos_resumable_request(
                    account_id=act,
                    data={**data, "access_token": token},
                    files=files,
                    timeout=180,
                )
                ctx = RequestExecutionContextDTO(session=sess)
                r = execute_request(req, build_no_retry_policy(), context=ctx)
                j = r.json()
                if "error" in j:
                    raise RuntimeError(j["error"].get("message") or "Facebook video upload failed")
                return j

            return with_retry(_do, tries=4, base_wait=1.0)
        
        # Start upload
        start_resp = _post({
            "upload_phase": "start",
            "file_size": str(file_size),
            "title": title,
            "content_category": "VIDEO_GAMING"
        })
        
        sess_id = start_resp["upload_session_id"]
        vid_id = start_resp["video_id"]
        start_off = int(start_resp.get("start_offset", 0))
        end_off = int(start_resp.get("end_offset", 0))
        
        # Upload chunks
        with open(path, "rb") as f:
            while True:
                if start_off == end_off == file_size: 
                    break
                if end_off <= start_off:
                    tr = _post({
                        "upload_phase": "transfer",
                        "upload_session_id": sess_id,
                        "start_offset": str(start_off)
                    })
                    start_off = int(tr.get("start_offset", start_off))
                    end_off = int(tr.get("end_offset", end_off or file_size))
                    continue
                
                f.seek(start_off)
                chunk = f.read(end_off - start_off)
                tr = _post(
                    {
                        "upload_phase": "transfer",
                        "upload_session_id": sess_id,
                        "start_offset": str(start_off)
                    },
                    files={"video_file_chunk": ("chunk.bin", chunk, "application/octet-stream")}
                )
                start_off = int(tr.get("start_offset", start_off + len(chunk)))
                end_off = int(tr.get("end_offset", end_off))
        
        # Finish
        try: 
            _post({
                "upload_phase": "finish", 
                "upload_session_id": sess_id,
                "title": title
            })
        except: 
            pass
        
        return vid_id, None
    
    # ====================================================================
    # STEP 2+3: 그룹별 병렬 처리
    # ====================================================================
        # ====================================================================
    # STEP 2+3: PIPELINE (Upload -> Ready -> Create) with concurrency
    # ====================================================================

    # --- Small retry helper (crash-less) ---
    def _with_retry(fn, *, retries=3, base_sleep=2, retry_codes=(1885252, 80004, 2, 4, 17, 32)):
        """
        Runs fn() with retry/backoff for transient FB/network errors.
        - retries: max attempts
        - base_sleep: seconds for exponential backoff base
        - retry_codes: FB error codes that are usually transient
        """
        last_err = None
        for attempt in range(retries):
            try:
                return fn()
            except FacebookRequestError as e:
                last_err = e
                code = e.api_error_code()
                msg = e.api_error_message()
                # retry only for known transient codes
                if code in retry_codes and attempt < retries - 1:
                    time.sleep(base_sleep * (2 ** attempt))
                    continue
                raise RuntimeError(f"Facebook API Error [{code}] {msg}")
            except Exception as e:
                last_err = e
                if attempt < retries - 1:
                    time.sleep(base_sleep * (2 ** attempt))
                    continue
                raise
        raise last_err

    def _wait_video_ready(vid_id: str, timeout_s=180, poll_s=3) -> bool:
        """
        Polls video status until it's ready (or timeout).
        Helps remove fixed sleeps and reduces failures (e.g. 1885252).
        """
        start = time.time()
        v = AdVideo(vid_id)
        while time.time() - start < timeout_s:
            try:
                info = v.api_get(fields=["status", "id"])
                status = info.get("status", "")
                
                # Facebook API는 status를 문자열 또는 딕셔너리로 반환할 수 있음
                if isinstance(status, dict):
                    # 딕셔너리인 경우
                    video_status = (status.get("video_status") or status.get("status") or "").upper()
                    if any(k in video_status for k in ["READY", "FINISHED", "COMPLETE", "SUCCESS"]):
                        return True
                else:
                    # 문자열인 경우
                    status_str = str(status).upper()
                    if any(k in status_str for k in ["READY", "FINISHED", "COMPLETE", "SUCCESS"]):
                        return True
                        
            except Exception as e:
                # 예외를 로깅하여 디버깅 가능하게 함
                logger.warning(f"Video status check failed for {vid_id}: {e}")
                # 계속 폴링
            time.sleep(poll_s)
        return False

    # --- Stage A: upload single chosen file per group (video + thumbnail) ---
    def _stage_upload_one(video_num: str, group_data: dict) -> dict:
        """
        Uploads 1 video (selected resolution) + thumbnail, returns payload for creation stage.
        Does NOT call st.* inside thread (safer).
        """
        resolution = group_data["resolution"]
        f_obj = group_data["file"]

        fname = getattr(f_obj, "name", None) or f_obj.get("name", "")
        match = re.search(r'(video\d+)', fname.lower())
        base_video_num = match.group(1) if match else video_num
        ad_name = _build_ad_name(base_video_num)
            
        # save temp
        file_data = _save_tmp(f_obj)
        video_path = file_data["path"]

        # thumbnail (best-effort)
        thumb_url = None
        def _thumb_job():
            try:
                thumb_path = extract_thumbnail_from_video(video_path)
                url = upload_thumbnail_image(account, thumb_path)
                try:
                    os.unlink(thumb_path)
                except:
                    pass
                return url
            except Exception:
                return None

        thumb_url = _thumb_job()

        # upload video (retry)
        def _upload_job():
            return _upload_video_with_title(video_path, fname)[0]  # returns (vid_id, None)
        vid_id = _with_retry(_upload_job, retries=3, base_sleep=2)

        return {
            "ok": True,
            "video_num": video_num,
            "resolution": resolution,
            "fname": fname,
            "ad_name": ad_name,
            "vid_id": vid_id,
            "thumb_url": thumb_url,
        }

    # --- Stage B: wait until ready (parallel) ---
    def _stage_wait_ready(item: dict) -> dict:
        """
        Waits video processing ready.
        """
        vid_id = item["vid_id"]
        ready = _wait_video_ready(vid_id, timeout_s=240, poll_s=3)
        item["ready"] = bool(ready)
        return item

    # --- Stage C: create creative + ad (parallel, only if ready) ---
    def _stage_create_ad(item: dict) -> dict:
        """
        Creates creative + ad for a ready video.
        Uses template texts/headlines/CTA/store_url resolved earlier.
        """
        if not item.get("ready"):
            return {"ok": False, "error": f"{item['ad_name']}: video not ready in time", "item": item}

        ad_name = item["ad_name"]
        vid_id = item["vid_id"]
        resolution = item["resolution"]
        thumb_url = item.get("thumb_url")  # 실제로는 image_hash 값

        # Prepare texts (filter empties)
        final_primary_texts = [t.strip() for t in (default_primary_texts or []) if (t or "").strip()]
        final_headlines = [h.strip() for h in (default_headlines or []) if (h or "").strip() and h.strip().lower() != "new game"]
        final_cta = default_cta if default_cta else "INSTALL_MOBILE_APP"
                    
        # Build video_data safely (avoid sending empty fields)
        video_data = {"video_id": vid_id}
        if final_headlines:
            title = (final_headlines[0] or "").strip()
            if title:
                video_data["title"] = title
        if final_primary_texts:
            msg = "\n\n".join(final_primary_texts).strip()
            if msg:
                video_data["message"] = msg

        # Must have URL
        if not final_store_url:
            return {"ok": False, "error": f"{ad_name}: Store URL Missing", "item": item}

        # ✅ 수정: image_url 대신 image_hash 사용
        # thumb_url은 실제로 upload_thumbnail_image에서 반환한 hash 값
        if thumb_url:
            # thumb_url이 실제로 URL인지 hash인지 확인
            if thumb_url.startswith(("http://", "https://")):
                video_data["image_url"] = thumb_url
            else:
                # hash 값인 경우
                video_data["image_hash"] = thumb_url
        else:
            # thumb_url이 없으면 에러 반환 (Meta API 요구사항)
            return {"ok": False, "error": f"{ad_name}: Thumbnail missing (image_hash or image_url required)", "item": item}

        video_data["call_to_action"] = {
            "type": final_cta,
            "value": {"link": final_store_url},
        }

        # Multi-ads opt-in
        multi_opt_in = bool(settings.get("multi_advertiser_ads_opt_in", False))
        multi_enroll_status = "OPT_IN" if multi_opt_in else "OPT_OUT"

        # IG identity (optional)
        ig_actor_id = (settings.get("instagram_actor_id") or "").strip()

        object_story_spec = {
            "page_id": str(page_id),
            "video_data": video_data,
        }
        if ig_actor_id:
            object_story_spec["instagram_actor_id"] = ig_actor_id

        creative_params = {
            "name": ad_name,
            "actor_id": str(page_id),
            "object_story_spec": object_story_spec,
            "contextual_multi_ads": {"enroll_status": multi_enroll_status},
        }
        if ig_actor_id:
            creative_params["instagram_actor_id"] = ig_actor_id

        # Create creative + ad with retry
        def _create_creative():
            cr = account.create_ad_creative(fields=[], params=creative_params)
            return cr["id"]

        creative_id = _with_retry(_create_creative, retries=3, base_sleep=2)

        def _create_ad():
            ad_params = {
                "name": ad_name,
                        "adset_id": adset_id,
                "creative": {"creative_id": creative_id},
                "status": Ad.Status.active,
            }
            resp = account.create_ad(fields=[], params=ad_params)
            ad_id = resp.get("id")
            if not ad_id:
                raise RuntimeError(f"API 응답에 Ad ID 없음: {resp}")
            return ad_id

        ad_id = _with_retry(_create_ad, retries=3, base_sleep=2)
        
        return {
            "ok": True,
            "result": {
                "name": ad_name,
                "ad_id": ad_id,
                "creative_id": creative_id,
                "resolution": resolution,
                            "used_values": {
                                "primary_texts_count": len(final_primary_texts),
                                "headlines_count": len(final_headlines),
                    "cta": final_cta,
                },
            },
        }

    # ====================================================================
    # PIPELINE EXECUTION
    # ====================================================================
    results = []
    errors = []

    total = len(valid_groups)
    prog = st.progress(0, text=f"🚀 Upload stage... 0/{total}")

    # Tune concurrency
    upload_workers = min(int(max_workers or 6), 6)      # uploading is heavy; don't go too high
    ready_workers = min(upload_workers, 6)
    create_workers = min(upload_workers, 6)

    # ---- Stage A: Upload in parallel
    uploaded_items = []
    done = 0

    with ThreadPoolExecutor(max_workers=upload_workers) as ex:
        futs = {ex.submit(_stage_upload_one, vn, vdata): vn for vn, vdata in valid_groups.items()}
        for fut in as_completed(futs):
            done += 1
            prog.progress(int(done / total * 100), text=f"🚀 Upload stage... {done}/{total}")
            try:
                item = fut.result()
                uploaded_items.append(item)
            except Exception as e:
                vn = futs[fut]
                errors.append(f"{vn}: upload failed - {e}")

    prog.empty()

    # quick summary in UI (main thread)
    ok_uploads = [x for x in uploaded_items if x.get("ok")]
    st.info(f"📤 Upload complete: {len(ok_uploads)}/{total} succeeded")

    if not ok_uploads:
        return {"ads": [], "errors": errors or ["No uploads succeeded"], "total_created": 0, "uploads_map": {}}

    # ---- Stage B: Ready wait in parallel
    prog = st.progress(0, text=f"⏳ Waiting ready... 0/{len(ok_uploads)}")
    ready_items = []
    done = 0
    with ThreadPoolExecutor(max_workers=ready_workers) as ex:
        futs = {ex.submit(_stage_wait_ready, item): item for item in ok_uploads}
        for fut in as_completed(futs):
            done += 1
            # 진행률 계산 수정 (0-100 범위)
            progress_pct = int((done / len(ok_uploads)) * 100) if ok_uploads else 0
            prog.progress(progress_pct / 100, text=f"⏳ Waiting ready... {done}/{len(ok_uploads)}")
            try:
                ready_items.append(fut.result())
            except Exception as e:
                it = futs[fut]
                errors.append(f"{it.get('ad_name','unknown')}: ready check failed - {e}")
                logger.error(f"Video ready check failed: {e}", exc_info=True)
    prog.empty()

    ready_ok = [x for x in ready_items if x.get("ready")]
    ready_fail = [x for x in ready_items if not x.get("ready")]
    if ready_fail:
        for x in ready_fail:
            errors.append(f"{x.get('ad_name')}: video not ready (timeout)")

    st.info(f"✅ Ready: {len(ready_ok)}/{len(ok_uploads)}")

    if not ready_ok:
        return {"ads": [], "errors": errors, "total_created": 0, "uploads_map": {}}

    # ---- Stage C: Create in parallel (only ready)
    prog = st.progress(0, text=f"🎨 Creating ads... 0/{len(ready_ok)}")
    done = 0
    with ThreadPoolExecutor(max_workers=create_workers) as ex:
        futs = {ex.submit(_stage_create_ad, item): item for item in ready_ok}
        for fut in as_completed(futs):
            done += 1
            prog.progress(int(done / len(ready_ok) * 100), text=f"🎨 Creating ads... {done}/{len(ready_ok)}")
            try:
                out = fut.result()
                if out.get("ok"):
                    results.append(out["result"])
                else:
                    errors.append(out.get("error", "Unknown create error"))
            except Exception as e:
                it = futs[fut]
                errors.append(f"{it.get('ad_name','unknown')}: create failed - {e}")
    prog.empty()

    # UI reporting (main thread)
    # - Normal mode: keep UI clean (no final report block). Show only a concise error if needed.
    # - Dev mode: show full details (created ads + error list).
    if errors and not devtools.dev_enabled():
        st.error(str(errors[0]))

    if devtools.dev_enabled():
        st.write("---")
        st.write("### 📊 최종 결과")

        success_with_ad = [r for r in results if r.get("ad_id")]
        if success_with_ad:
            st.success(f"✅ Ad 생성 완료: {len(success_with_ad)}개")
            with st.expander("생성된 Ad 목록 보기", expanded=True):
                for r in success_with_ad:
                    st.write(f"- **{r['name']}**: Ad ID `{r['ad_id']}` ({r.get('resolution','N/A')})")

        if errors:
            st.error(f"❌ 실패: {len(errors)}개")
            with st.expander("실패 항목 보기"):
                for e in errors:
                    st.write(f"- {e}")

    return {
        "ads": results,
        "errors": errors,
        "total_created": len(results),
        "uploads_map": {}
    }

def upload_all_videos_to_media_library(
    account,
    uploaded_files: list,
    max_workers: int = 6
) -> dict:
    """
    Upload all videos to Account Media Library with original filenames.
    No ad creation - just video storage.
    
    Returns:
        {
            "uploaded": [{"name": "video164.mp4", "video_id": "123..."}],
            "errors": ["video165.mp4: Upload failed"]
        }
    """
    # Helper: Save uploaded file to temp
    def _save_tmp(u):
        if isinstance(u, dict) and "path" in u:
            return {"name": u["name"], "path": u["path"]}
        if hasattr(u, "getbuffer"):
            suffix = pathlib.Path(u.name).suffix.lower() or ".mp4"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(u.getbuffer())
                return {"name": u.name, "path": tmp.name}
        raise ValueError("Unsupported video object")
    
    # Helper: Upload video with original filename as title
    def _upload_video_with_title(path: str, title: str) -> str:
        """Upload video to media library with title"""
        if "facebook" in st.secrets:
            token = st.secrets["facebook"].get("access_token", "").strip()
        else:
            token = st.secrets.get("access_token", "").strip()
        
        act = account.get_id()
        file_size = os.path.getsize(path)
        
        def _post(data, files=None):
            req = build_advideos_resumable_request(
                account_id=act,
                data={**data, "access_token": token},
                files=files,
                timeout=180,
            )
            r = execute_request(req, build_no_retry_policy())
            j = r.json()
            if "error" in j: 
                raise RuntimeError(j["error"].get("message"))
            return j
        
        # Start upload
        start_resp = _post({
            "upload_phase": "start",
            "file_size": str(file_size),
            "title": title,
            "content_category": "VIDEO_GAMING"
        })
        
        sess_id = start_resp["upload_session_id"]
        vid_id = start_resp["video_id"]
        start_off = int(start_resp.get("start_offset", 0))
        end_off = int(start_resp.get("end_offset", 0))
        
        # Upload chunks
        with open(path, "rb") as f:
            while True:
                if start_off == end_off == file_size: 
                    break
                if end_off <= start_off:
                    tr = _post({
                        "upload_phase": "transfer",
                        "upload_session_id": sess_id,
                        "start_offset": str(start_off)
                    })
                    start_off = int(tr.get("start_offset", start_off))
                    end_off = int(tr.get("end_offset", end_off or file_size))
                    continue
                    
                f.seek(start_off)
                chunk = f.read(end_off - start_off)
                tr = _post(
                    {
                        "upload_phase": "transfer",
                        "upload_session_id": sess_id,
                        "start_offset": str(start_off)
                    },
                    files={"video_file_chunk": ("chunk.bin", chunk, "application/octet-stream")}
                )
                start_off = int(tr.get("start_offset", start_off + len(chunk)))
                end_off = int(tr.get("end_offset", end_off))
        
        # Finish
        try: 
            _post({
                "upload_phase": "finish", 
                "upload_session_id": sess_id,
                "title": title
            })
        except: 
            pass
        
        return vid_id
    
    # Process files
    persisted = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_save_tmp, u): u for u in uploaded_files}
        for fut in as_completed(futs):
            try: 
                fname = getattr(futs[fut], "name", None) or futs[fut].get("name", "")
                persisted.append({"name": fname, "path": fut.result()["path"]})
            except: 
                pass
    
    # Upload videos
    uploaded = []
    errors = []
    total = len(persisted)
    
    prog = st.progress(0, text=f"📤 Uploading to Media Library... 0/{total}")
    done = 0
    
    def _upload_task(item):
        try:
            vid_id = _upload_video_with_title(item["path"], item["name"])
            return {"success": True, "name": item["name"], "video_id": vid_id}
        except Exception as e:
            return {"success": False, "name": item["name"], "error": str(e)}
    
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_upload_task, item): item for item in persisted}
        
        for fut in as_completed(futs):
            res = fut.result()
            done += 1
            prog.progress(int(done / total * 100), text=f"📤 Uploading... {done}/{total}")
            
            if res["success"]:
                uploaded.append({"name": res["name"], "video_id": res["video_id"]})
            else:
                errors.append(f"{res['name']}: {res['error']}")
    
    prog.empty()
    
    return {
        "uploaded": uploaded,
        "errors": errors,
        "total": len(uploaded),
        "failed": len(errors)
    }


# fb.py 하단에 추가

def _upload_dynamic_single_video_ads(
    account, page_id: str, adset_id: str, uploaded_files: list,
    settings: dict, store_url: str, max_workers: int
) -> dict:
    """
    다이내믹-single video 모드:
    - 각 video 그룹에 3개 사이즈 필수 (1080x1080, 1920x1080, 1080x1920)
    - 모든 비디오를 하나의 Flexible Ad에 통합
    """
    logger = logging.getLogger(__name__)
    
    # ====================================================================
    # STEP 0: 템플릿 로드
    # ====================================================================
    st.info("📋 AdSet에서 템플릿 정보 가져오는 중...")
    template = fetch_latest_ad_creative_defaults(adset_id)
    
    # Primary Texts
    default_primary_texts = []
    if template.get("primary_texts"):
        default_primary_texts = [pt.strip() for pt in template["primary_texts"] if pt.strip()]
    elif settings.get("primary_text"):
        text = settings["primary_text"].strip()
        default_primary_texts = [t.strip() for t in text.split('\n\n') if t.strip()]
    

    # Headlines
    default_headlines = []
    if template.get("headlines"):
        # "New Game" 제외하고 유효한 headline만 수집
        for h in template["headlines"]:
            cleaned = h.strip()
            if cleaned and cleaned.lower() != "new game":
                default_headlines.append(cleaned)
    elif settings.get("headline"):
        headline = settings["headline"].strip()
        default_headlines = [h.strip() for h in headline.split('\n') if h.strip()]

    # ✅ 검증 전에 디버그 출력
    # st.write(f"🔍 DEBUG: Template headlines: {template.get('headlines', [])}")
    # st.write(f"🔍 DEBUG: Filtered headlines: {default_headlines}")
    # st.write(f"🔍 DEBUG: Settings headline: {settings.get('headline', 'N/A')}")

    # ✅ 텍스트는 "없어도" 진행 (빈칸 업로드 허용)
    # - 단, 실제 API에는 빈 문자열은 넣지 않도록 아래에서 필터링함
    if default_primary_texts is None:
        default_primary_texts = []
    if default_headlines is None:
        default_headlines = []
        
    # ✅ CTA 우선순위: UI(settings) > template > default
    default_cta = (settings.get("call_to_action") or "").strip()
    if not default_cta:
        default_cta = (template.get("call_to_action") or "").strip()
    if not default_cta:
        default_cta = "INSTALL_MOBILE_APP"
    
    # Store URL
    final_store_url = ""
    try:
        adset = AdSet(adset_id)
        adset_data = adset.api_get(fields=["promoted_object"])
        promoted_obj = adset_data.get("promoted_object", {})
        adset_store_url = promoted_obj.get("object_store_url", "")
        
        if adset_store_url:
            final_store_url = sanitize_store_url(adset_store_url)
            st.info(f"✅ AdSet의 Store URL 사용: {final_store_url[:60]}...")
        else:
            st.warning("⚠️ AdSet에 promoted_object가 없습니다")
    except Exception as e:
        st.warning(f"⚠️ AdSet 조회 실패: {e}")
    
    if not final_store_url:
        if store_url:
            final_store_url = sanitize_store_url(store_url)
        elif settings.get("store_url"):
            final_store_url = sanitize_store_url(settings["store_url"])
    
    if not final_store_url:
        raise RuntimeError("❌ Store URL이 없습니다!")
    if not final_store_url.startswith("http"):
        raise RuntimeError(f"❌ 유효하지 않은 Store URL: {final_store_url}")
    
    _dev_success("✅ 템플릿 로드 완료")
    _dev_caption(f"📝 Primary Texts: {len(default_primary_texts)}개")
    _dev_caption(f"📰 Headlines: {len(default_headlines)}개")
    _dev_caption(f"🎯 CTA: {default_cta}")
    _dev_caption(f"🔗 Store URL: {final_store_url[:50]}...")
    
    # Prefix/Suffix
    use_prefix = settings.get("use_prefix", False)
    prefix_text = settings.get("prefix_text", "").strip()
    use_suffix = settings.get("use_suffix", False)
    suffix_text = settings.get("suffix_text", "").strip()
    
    def _build_ad_name(video_num: str) -> str:
        name_parts = []
        if use_prefix and prefix_text:
            name_parts.append(prefix_text)
        name_parts.append(video_num)
        if use_suffix and suffix_text:
            name_parts.append(suffix_text)
        return "_".join(name_parts)
    
    # ====================================================================
    # STEP 1: 비디오 그룹화
    # ====================================================================
    def _extract_video_number(fname):
        match = re.search(r'video(\d+)', fname.lower())
        return f"video{match.group(1)}" if match else None
    
    def _extract_resolution(fname):
        if "1080x1080" in fname.lower():
            return "1080x1080"
        elif "1920x1080" in fname.lower():
            return "1920x1080"
        elif "1080x1920" in fname.lower():
            return "1080x1920"
        return None
    
    video_groups = {}
    unrecognized_files = []  # ✅ 인식되지 않은 파일 추적
    
    for u in uploaded_files:
        fname = getattr(u, "name", None) or u.get("name", "")
        if not fname: 
            continue
        
        video_num = _extract_video_number(fname)
        resolution = _extract_resolution(fname)
        
        if not video_num:
            continue  # video 번호가 없으면 스킵
        
        if not resolution:
            # ✅ 인식되지 않은 해상도 경고
            unrecognized_files.append(fname)
            continue
        
        if video_num not in video_groups:
            video_groups[video_num] = {}
        
        # ✅ 중복 해상도 체크
        if resolution in video_groups[video_num]:
            st.warning(f"⚠️ {video_num}: {resolution} 해상도가 중복됩니다. 마지막 파일만 사용됩니다.")
            st.caption(f"   - 기존: {getattr(video_groups[video_num][resolution], 'name', 'N/A')}")
            st.caption(f"   - 새 파일: {fname}")
        
        video_groups[video_num][resolution] = u
    
    # ✅ 인식되지 않은 파일 경고
    if unrecognized_files:
        st.warning(f"⚠️ 인식되지 않은 해상도 파일 {len(unrecognized_files)}개:")
        for fname in unrecognized_files:
            st.caption(f"   - {fname} (1080x1080, 1920x1080, 1080x1920만 지원)")
    
    # 3개 사이즈 검증
    valid_groups = {}
    REQUIRED_SIZES = ["1080x1080", "1920x1080", "1080x1920"]
    
    for video_num, files in video_groups.items():
        missing = [size for size in REQUIRED_SIZES if size not in files]
        if missing:
            st.error(f"❌ {video_num}: 필수 해상도 누락 - {', '.join(missing)}")
            st.caption(f"   현재 있는 해상도: {', '.join(files.keys())}")
        else:
            valid_groups[video_num] = files
            st.success(f"✅ {video_num}: 3개 사이즈 모두 확인")
    
    if not valid_groups:
        raise RuntimeError("❌ 유효한 비디오 그룹이 없습니다.")
    
    st.info(f"📦 {len(valid_groups)}개 비디오 그룹을 하나의 Flexible Ad로 생성합니다...")
    
    # ====================================================================
    # STEP 2: 모든 비디오 업로드
    # ====================================================================
    def _save_tmp(u):
        if isinstance(u, dict) and "path" in u:
            return {"name": u["name"], "path": u["path"]}
        if hasattr(u, "getbuffer"):
            suffix = pathlib.Path(u.name).suffix.lower() or ".mp4"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(u.getbuffer())
                return {"name": u.name, "path": tmp.name}
        raise ValueError("Unsupported video object")
    
    def _upload_video_with_title(path: str, title: str) -> str:
        if "facebook" in st.secrets:
            token = st.secrets["facebook"].get("access_token", "").strip()
        else:
            token = st.secrets.get("access_token", "").strip()
        
        act = account.get_id()
        file_size = os.path.getsize(path)
        
        def _post(data, files=None):
            req = build_advideos_resumable_request(
                account_id=act,
                data={**data, "access_token": token},
                files=files,
                timeout=180,
            )
            r = execute_request(req, build_no_retry_policy())
            j = r.json()
            if "error" in j: 
                raise RuntimeError(j["error"].get("message"))
            return j
        
        start_resp = _post({
            "upload_phase": "start",
            "file_size": str(file_size),
            "title": title,
            "content_category": "VIDEO_GAMING"
        })
        
        sess_id = start_resp["upload_session_id"]
        vid_id = start_resp["video_id"]
        start_off = int(start_resp.get("start_offset", 0))
        end_off = int(start_resp.get("end_offset", 0))
        
        with open(path, "rb") as f:
            while True:
                if start_off == end_off == file_size: 
                    break
                if end_off <= start_off:
                    tr = _post({"upload_phase": "transfer", "upload_session_id": sess_id, "start_offset": str(start_off)})
                    start_off = int(tr.get("start_offset", start_off))
                    end_off = int(tr.get("end_offset", end_off or file_size))
                    continue
                
                f.seek(start_off)
                chunk = f.read(end_off - start_off)
                tr = _post(
                    {"upload_phase": "transfer", "upload_session_id": sess_id, "start_offset": str(start_off)},
                    files={"video_file_chunk": ("chunk.bin", chunk, "application/octet-stream")}
                )
                start_off = int(tr.get("start_offset", start_off + len(chunk)))
                end_off = int(tr.get("end_offset", end_off))
        
        try: 
            _post({"upload_phase": "finish", "upload_session_id": sess_id, "title": title})
        except: 
            pass
        
        return vid_id
    
    # 모든 비디오 업로드 (병렬 처리)
    all_video_ids = {}
    thumb_urls = {}

    tasks = []
    for video_num, group_files in valid_groups.items():
        all_video_ids[video_num] = {}
        for size in REQUIRED_SIZES:
            f_obj = group_files[size]
            fname = getattr(f_obj, "name", None) or f_obj.get("name", "")
            tasks.append((video_num, size, f_obj, fname))

    total_uploads = len(tasks)
    prog = st.progress(0, text=f"📤 비디오 업로드 중... 0/{total_uploads}")
    done = 0

    def _upload_one(video_num: str, size: str, f_obj, fname: str):
        """Uploads one video; also prepares one thumbnail per video_num (from 1080x1080)."""
        file_data = _save_tmp(f_obj)

        # 썸네일은 video_num당 1번만, 그리고 1080x1080에서만 시도
        if size == "1080x1080" and video_num not in thumb_urls:
            try:
                thumb_path = extract_thumbnail_from_video(file_data["path"])
                thumb_urls[video_num] = upload_thumbnail_image(account, thumb_path)
                try:
                    os.unlink(thumb_path)
                except:
                    pass
            except Exception:
                thumb_urls[video_num] = None

        vid_id = _upload_video_with_title(file_data["path"], fname)
        return (video_num, size, vid_id)

    # 업로드는 너무 많은 병렬이 오히려 불안정할 수 있으니 3~4 추천
    upload_workers = min(4, max(2, total_uploads))
    errors = []

    with ThreadPoolExecutor(max_workers=upload_workers) as ex:
        futs = {
            ex.submit(with_retry, lambda vn=vn, sz=sz, fo=fo, fn=fname: _upload_one(vn, sz, fo, fn), 4, 1.0): (vn, sz, fname)
            for (vn, sz, fo, fname) in tasks
        }
        for fut in as_completed(futs):
            vn, sz, fname = futs[fut]
            try:
                video_num, size, vid_id = fut.result()
                all_video_ids[video_num][size] = vid_id
            except Exception as e:
                errors.append(f"{vn}/{sz}/{fname}: {e}")
            finally:
                done += 1
                prog.progress(int(done / total_uploads * 100), text=f"📤 비디오 업로드 중... {done}/{total_uploads}")

    prog.empty()

    if errors:
        raise RuntimeError("Upload failed for some videos:\n" + "\n".join(errors))

    st.success(f"✅ {total_uploads}개 비디오 업로드 완료")
    
    # 비디오 처리 완료 대기 (고정 sleep 제거)
    st.info("⏳ 업로드된 비디오 처리 완료 대기 중(wait_video_ready)...")

    all_vids = []
    for vn in all_video_ids:
        for sz in all_video_ids[vn]:
            all_vids.append(all_video_ids[vn][sz])

    # 병렬 폴링(너무 세게 치지 않도록 workers 제한)
    errs = []
    with ThreadPoolExecutor(max_workers=min(6, max(2, len(all_vids)))) as ex:
        futs = {ex.submit(wait_video_ready, vid, 300, 1.0): vid for vid in all_vids}
        for fut in as_completed(futs):
            vid = futs[fut]
            try:
                fut.result()
            except Exception as e:
                errs.append(f"{vid}: {e}")

    if errs:
        raise RuntimeError("Some videos did not become ready:\n" + "\n".join(errs))
        
    # ====================================================================
    # STEP 3: 그룹별로 Flexible Ad 생성 (video166은 1개, video167도 1개 ...)
    # ====================================================================
    ads_created = []
    errors = []

    # ✅ IG actor id: Streamlit에서 선택된 값 사용
    ig_actor_id = (settings.get("instagram_actor_id") or "").strip()

    for video_num in sorted(all_video_ids.keys()):
        try:
            # 3사이즈 video_id만 이 그룹에 포함
            videos = [{"video_id": all_video_ids[video_num][size]} for size in REQUIRED_SIZES]

            # Ad 이름: video166 기준 (prefix/suffix 적용)
            ad_name = _build_ad_name(video_num)

            # ✅ 텍스트: text_type당 최대 5개 제한(Flexible Ad Format 제한)
            final_primary_texts = []
            for pt in (default_primary_texts or []):
                pt = (pt or "").strip()
                if pt:
                    final_primary_texts.append(pt)
            final_primary_texts = final_primary_texts[:5]

            final_headlines = []
            for hl in (default_headlines or []):
                hl = (hl or "").strip()
                if hl and hl.lower() != "new game":
                    final_headlines.append(hl)
            final_headlines = final_headlines[:5]

            texts = (
                [{"text": t, "text_type": "primary_text"} for t in final_primary_texts]
                + [{"text": t, "text_type": "headline"} for t in final_headlines]
            )

            # ✅ group payload (texts가 비면 아예 키를 빼서 보냄)
            group_payload = {
                "videos": videos,
                "call_to_action": {
                    "type": default_cta,
                    "value": {"link": final_store_url}
                }
            }
            if texts:
                group_payload["texts"] = texts

            # ✅ inline creative: 첫 그룹의 첫 video_id와 동일하게 맞춤
            inline_video_data = {
                "video_id": videos[0]["video_id"],
                "call_to_action": {
                    "type": default_cta,
                    "value": {"link": final_store_url}
                },
            }

            # ✅ 썸네일 제공
            thumb_url = thumb_urls.get(video_num)
            if thumb_url:
                # ✅ 수정: image_url 대신 image_hash 사용
                # thumb_url은 실제로 upload_thumbnail_image에서 반환한 hash 값
                if thumb_url.startswith(("http://", "https://")):
                    inline_video_data["image_url"] = thumb_url
                else:
                    inline_video_data["image_hash"] = thumb_url
            else:
                raise RuntimeError("썸네일(image_hash) 생성 실패: object_story_spec.video_data에 필요함")

            # ✅ Object Story Spec 구성 (Instagram 연결 포함)
            inline_object_story_spec = {
                "page_id": str(page_id),
                "video_data": inline_video_data
            }
            
            # ✅ Instagram account 연결 (Use Facebook Page)
            if ig_actor_id:
                inline_object_story_spec["instagram_actor_id"] = ig_actor_id

            # ✅ Multi-advertiser ads 토글
            multi_opt_in = bool(settings.get("multi_advertiser_ads_opt_in", False))
            multi_enroll_status = "OPT_IN" if multi_opt_in else "OPT_OUT"

            # ✅ Creative 구성 (Instagram 포함)
            creative_config = {
                "name": ad_name,
                "actor_id": str(page_id),  # Facebook Page identity
                "object_story_spec": inline_object_story_spec,
                "contextual_multi_ads": {"enroll_status": multi_enroll_status},
            }
            
            # ✅ Instagram account를 Creative 레벨에 추가
            if ig_actor_id:
                creative_config["instagram_actor_id"] = ig_actor_id

            ad_params = {
                "name": ad_name,
                "adset_id": adset_id,
                "creative": creative_config,
                "creative_asset_groups_spec": {
                    "groups": [group_payload]
                },
                "status": Ad.Status.active,
            }

            ad_response = account.create_ad(fields=[], params=ad_params)
            ad_id = ad_response.get("id")
            if not ad_id:
                raise RuntimeError(f"Ad 생성 응답에 id가 없습니다: {ad_response}")

            st.success(f"✅ Flexible Ad 생성 완료: {ad_name} / {ad_id}")
            ads_created.append({
                "name": ad_name,
                "ad_id": ad_id,
                "creative_id": None,
                "video_groups": [video_num],
                "total_videos": len(videos)
            })

        except Exception as e:
            errors.append(f"{video_num}: {e}")
            st.error(f"❌ {video_num} Flexible Ad 생성 실패: {e}")

    return {
        "ads": ads_created,
        "errors": errors,
        "total_created": len(ads_created)
    }


def _upload_dynamic_1x1_ads(
    account, page_id: str, adset_id: str, uploaded_files: list,
    settings: dict, store_url: str, max_workers: int, game_name: str
) -> dict:
    """
    다이내믹-1x1 모드:
    - 모든 비디오가 1080x1080 사이즈여야 함
    - 모든 비디오가 같은 게임이어야 함
    - 최대 10개 비디오
    - 하나의 Flexible Ad 생성
    """
    logger = logging.getLogger(__name__)
    
    # ====================================================================
    # STEP 0: 템플릿 로드
    # ====================================================================
    st.info("📋 AdSet에서 템플릿 정보 가져오는 중...")
    template = fetch_latest_ad_creative_defaults(adset_id)
    
    # Primary Texts
    default_primary_texts = []
    if template.get("primary_texts"):
        default_primary_texts = [pt.strip() for pt in template["primary_texts"] if pt.strip()]
    elif settings.get("primary_text"):
        text = settings["primary_text"].strip()
        default_primary_texts = [t.strip() for t in text.split('\n\n') if t.strip()]

    # Headlines
    default_headlines = []
    if template.get("headlines"):
        for h in template["headlines"]:
            cleaned = h.strip()
            if cleaned and cleaned.lower() != "new game":
                default_headlines.append(cleaned)
    elif settings.get("headline"):
        headline = settings["headline"].strip()
        default_headlines = [h.strip() for h in headline.split('\n') if h.strip()]

    if default_primary_texts is None:
        default_primary_texts = []
    if default_headlines is None:
        default_headlines = []
        
    # CTA 우선순위: UI(settings) > template > default
    default_cta = (settings.get("call_to_action") or "").strip()
    if not default_cta:
        default_cta = (template.get("call_to_action") or "").strip()
    if not default_cta:
        default_cta = "INSTALL_MOBILE_APP"
    
    # Store URL
    final_store_url = ""
    try:
        adset = AdSet(adset_id)
        adset_data = adset.api_get(fields=["promoted_object"])
        promoted_obj = adset_data.get("promoted_object", {})
        adset_store_url = promoted_obj.get("object_store_url", "")
        
        if adset_store_url:
            final_store_url = sanitize_store_url(adset_store_url)
            st.info(f"✅ AdSet의 Store URL 사용: {final_store_url[:60]}...")
        else:
            st.warning("⚠️ AdSet에 promoted_object가 없습니다")
    except Exception as e:
        st.warning(f"⚠️ AdSet 조회 실패: {e}")
    
    if not final_store_url:
        if store_url:
            final_store_url = sanitize_store_url(store_url)
        elif settings.get("store_url"):
            final_store_url = sanitize_store_url(settings["store_url"])
    
    if not final_store_url:
        raise RuntimeError("❌ Store URL이 없습니다!")
    if not final_store_url.startswith("http"):
        raise RuntimeError(f"❌ 유효하지 않은 Store URL: {final_store_url}")
    
    _dev_success("✅ 템플릿 로드 완료")
    _dev_caption(f"📝 Primary Texts: {len(default_primary_texts)}개")
    _dev_caption(f"📰 Headlines: {len(default_headlines)}개")
    _dev_caption(f"🎯 CTA: {default_cta}")
    _dev_caption(f"🔗 Store URL: {final_store_url[:50]}...")
    
    # Prefix/Suffix
    use_prefix = settings.get("use_prefix", False)
    prefix_text = settings.get("prefix_text", "").strip()
    use_suffix = settings.get("use_suffix", False)
    suffix_text = settings.get("suffix_text", "").strip()
    
    # ====================================================================
    # STEP 1: 비디오 검증 (1080x1080만, 개수 체크)
    # ====================================================================
    def _extract_video_number(fname):
        match = re.search(r'video(\d+)', fname.lower())
        return f"video{match.group(1)}" if match else None
    
    def _extract_resolution(fname):
        if "1080x1080" in fname.lower():
            return "1080x1080"
        return None
    
    valid_videos = []
    user_msg = "비디오사이즈가 1x1(1080x1080)가 맞는지 체크해주세요"

    for u in uploaded_files:
        fname = getattr(u, "name", None) or u.get("name", "")
        if not fname:
            continue

        video_num = _extract_video_number(fname)
        if not video_num:
            # 파일명 디테일은 노출하지 않고 안내만
            raise RuntimeError("파일명이 videoxxx 형식인지 확인해주세요")

        resolution = _extract_resolution(fname)
        if not resolution or resolution != "1080x1080":
            raise RuntimeError(user_msg)

        valid_videos.append({"video_num": video_num, "file": u, "fname": fname})
    
    # 3. 개수 체크 (10개 이하)
    if len(valid_videos) > 10:
        raise RuntimeError("❌ 다이내믹 광고는 10개이상의 동영상을 수용할 수 없습니다")
    
    if not valid_videos:
        raise RuntimeError(user_msg)
    
    _dev_success(f"✅ {len(valid_videos)}개 비디오 검증 완료 (1080x1080)")
    
    # ====================================================================
    # STEP 2: 모든 비디오 업로드
    # ====================================================================
    def _save_tmp(u):
        if isinstance(u, dict) and "path" in u:
            return {"name": u["name"], "path": u["path"]}
        if hasattr(u, "getbuffer"):
            suffix = pathlib.Path(u.name).suffix.lower() or ".mp4"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(u.getbuffer())
                return {"name": u.name, "path": tmp.name}
        raise ValueError("Unsupported video object")
    
    def _upload_video_with_title(path: str, title: str) -> str:
        if "facebook" in st.secrets:
            token = st.secrets["facebook"].get("access_token", "").strip()
        else:
            token = st.secrets.get("access_token", "").strip()
        
        act = account.get_id()
        file_size = os.path.getsize(path)
        
        def _post(data, files=None):
            req = build_advideos_resumable_request(
                account_id=act,
                data={**data, "access_token": token},
                files=files,
                timeout=180,
            )
            r = execute_request(req, build_no_retry_policy())
            j = r.json()
            if "error" in j: 
                raise RuntimeError(j["error"].get("message"))
            return j
        
        start_resp = _post({
            "upload_phase": "start",
            "file_size": str(file_size),
            "title": title,
            "content_category": "VIDEO_GAMING"
        })
        
        sess_id = start_resp["upload_session_id"]
        vid_id = start_resp["video_id"]
        start_off = int(start_resp.get("start_offset", 0))
        end_off = int(start_resp.get("end_offset", 0))
        
        with open(path, "rb") as f:
            while True:
                if start_off == end_off == file_size: 
                    break
                if end_off <= start_off:
                    tr = _post({"upload_phase": "transfer", "upload_session_id": sess_id, "start_offset": str(start_off)})
                    start_off = int(tr.get("start_offset", start_off))
                    end_off = int(tr.get("end_offset", end_off or file_size))
                    continue
                
                f.seek(start_off)
                chunk = f.read(end_off - start_off)
                tr = _post(
                    {"upload_phase": "transfer", "upload_session_id": sess_id, "start_offset": str(start_off)},
                    files={"video_file_chunk": ("chunk.bin", chunk, "application/octet-stream")}
                )
                start_off = int(tr.get("start_offset", start_off + len(chunk)))
                end_off = int(tr.get("end_offset", end_off))
        
        try: 
            _post({"upload_phase": "finish", "upload_session_id": sess_id, "title": title})
        except: 
            pass
        
        return vid_id
    
    # 모든 비디오 업로드 (병렬 처리)
    all_video_ids = {}
    thumb_urls = {}
    
    tasks = []
    for vid_data in valid_videos:
        video_num = vid_data["video_num"]
        f_obj = vid_data["file"]
        fname = vid_data["fname"]
        all_video_ids[video_num] = {}
        tasks.append((video_num, f_obj, fname))
    
    total_uploads = len(tasks)
    
    # 통합 프로그래스바 생성
    overall_prog = st.progress(0, text="🚀 전체 진행 중... 0%")
    status_text = st.empty()
    
    def _update_progress(stage: str, current: int, total: int, stage_pct: int, base_pct: int = 0):
        """전체 진행 상황 업데이트"""
        # stage_pct: 이 단계가 전체에서 차지하는 비율 (0-100)
        # base_pct: 이전 단계까지의 진행률
        stage_progress = int((current / total) * stage_pct) if total > 0 else 0
        overall_pct = base_pct + stage_progress
        overall_prog.progress(overall_pct / 100, text=f"🚀 {stage}... {current}/{total} ({overall_pct}%)")
        status_text.text(f"📊 현재 단계: {stage} | 진행률: {overall_pct}%")
    
    def _upload_one(video_num: str, f_obj, fname: str):
        """Uploads one video; also prepares one thumbnail."""
        file_data = _save_tmp(f_obj)
        
        # 썸네일 생성
        if video_num not in thumb_urls:
            try:
                thumb_path = extract_thumbnail_from_video(file_data["path"])
                thumb_urls[video_num] = upload_thumbnail_image(account, thumb_path)
                try:
                    os.unlink(thumb_path)
                except:
                    pass
            except Exception:
                thumb_urls[video_num] = None
        
        vid_id = _upload_video_with_title(file_data["path"], fname)
        return (video_num, vid_id)
    
    # ====================================================================
    # STEP 2-1: 비디오 업로드 (0-40%)
    # ====================================================================
    upload_workers = min(4, max(2, total_uploads))
    upload_errors = []
    
    with ThreadPoolExecutor(max_workers=upload_workers) as ex:
        futs = {
            ex.submit(_upload_one, vn, fo, fname): (vn, fname)
            for (vn, fo, fname) in tasks
        }
        
        done = 0
        for fut in as_completed(futs):
            done += 1
            _update_progress("📤 비디오 업로드", done, total_uploads, 40, 0)
            try:
                video_num, vid_id = fut.result()
                all_video_ids[video_num] = vid_id
            except Exception as e:
                vn, fname = futs[fut]
                upload_errors.append(f"{fname}: {e}")
                st.error(f"❌ {fname} 업로드 실패: {e}")
    
    if upload_errors:
        overall_prog.empty()
        status_text.empty()
        raise RuntimeError("Upload failed for some videos:\n" + "\n".join(upload_errors))
    
    # ====================================================================
    # STEP 2-2: 비디오 처리 완료 대기 (40-80%)
    # ====================================================================
    all_vids = list(all_video_ids.values())
    errs = []
    done = 0
    with ThreadPoolExecutor(max_workers=min(6, max(2, len(all_vids)))) as ex:
        futs = {ex.submit(wait_video_ready, vid, 300, 1.0): vid for vid in all_vids}
        for fut in as_completed(futs):
            done += 1
            _update_progress("⏳ 비디오 처리 대기", done, len(all_vids), 40, 40)
            vid = futs[fut]
            try:
                fut.result()
            except Exception as e:
                errs.append(f"{vid}: {e}")
    
    if errs:
        overall_prog.empty()
        status_text.empty()
        raise RuntimeError("Some videos did not become ready:\n" + "\n".join(errs))
    
    # ====================================================================
    # STEP 3: Ad 이름 생성
    # ====================================================================
    def _extract_game_name_from_filename(fname):
        """
        파일명에서 게임 이름 추출
        예: video100_gamename_en_37s_1080x1080.mp4 -> gamename
        """
        # 패턴: video숫자_게임이름_언어코드_길이_해상도
        # 예: video100_suzyrest_en_37s_1080x1080.mp4
        match = re.search(r'video\d+_(.+?)_[a-z]{2}_\d+s_', fname.lower())
        if match:
            return match.group(1)
        return None
    
    # 모든 비디오 파일명에서 게임 이름 추출
    extracted_game_names = []
    for vid_data in valid_videos:
        fname = vid_data["fname"]
        game_name_from_file = _extract_game_name_from_filename(fname)
        if game_name_from_file:
            extracted_game_names.append(game_name_from_file)
    
    # 가장 많이 나온 게임 이름 사용 (또는 첫 번째)
    if extracted_game_names:
        # 가장 많이 나온 것 사용
        from collections import Counter
        game_name_counter = Counter(extracted_game_names)
        most_common_game_name = game_name_counter.most_common(1)[0][0]
        game_name_clean = most_common_game_name
        st.info(f"📝 파일명에서 추출한 게임 이름: {game_name_clean}")
    else:
        # 추출 실패 시 기존 로직 사용 (game_name 파라미터)
        game_name_clean = re.sub(r'[^\w]', '', game_name.lower())
        st.warning(f"⚠️ 파일명에서 게임 이름을 추출할 수 없어 기본값 사용: {game_name_clean}")
    
    video_numbers = []
    for vid_data in valid_videos:
        video_num = vid_data["video_num"]
        match = re.search(r'video(\d+)', video_num.lower())
        if match:
            video_numbers.append(int(match.group(1)))
    
    if not video_numbers:
        raise RuntimeError("❌ 비디오 번호를 추출할 수 없습니다.")
    
    video_label = _build_video_ranges_label(video_numbers)
    if not video_label:
        raise RuntimeError("❌ 비디오 번호를 추출할 수 없습니다.")
    
    # Ad 이름 생성
    ad_name_setting = settings.get("dco_creative_name", "").strip()
    if ad_name_setting:
        # 사용자가 Ad Name을 설정한 경우
        ad_name = ad_name_setting
    else:
        # 기본 Ad 이름 생성
        ad_name = f"{video_label}_{game_name_clean}_flexible_정방"
    
    # Prefix/Suffix 적용
    if use_prefix and prefix_text:
        ad_name = f"{prefix_text}_{ad_name}"
    if use_suffix and suffix_text:
        ad_name = f"{ad_name}_{suffix_text}"
    
    # ====================================================================
    # STEP 4: 하나의 Flexible Ad 생성 (80-100%)
    # ====================================================================
    _update_progress("🎨 Flexible Ad 생성", 0, 1, 20, 80)
    try:
        # 모든 비디오를 하나의 그룹으로
        videos = [{"video_id": vid_id} for vid_id in all_video_ids.values()]
        
        # 텍스트 필터링 (Flexible Ad Format 제한: text_type당 최대 5개)
        final_primary_texts = []
        for pt in (default_primary_texts or []):
            pt = (pt or "").strip()
            if pt:
                final_primary_texts.append(pt)
        final_primary_texts = final_primary_texts[:5]

        final_headlines = []
        for hl in (default_headlines or []):
            hl = (hl or "").strip()
            if hl and hl.lower() != "new game":
                final_headlines.append(hl)
        final_headlines = final_headlines[:5]

        texts = (
            [{"text": t, "text_type": "primary_text"} for t in final_primary_texts]
            + [{"text": t, "text_type": "headline"} for t in final_headlines]
        )
        
        # group payload
        group_payload = {
            "videos": videos,
            "call_to_action": {
                "type": default_cta,
                "value": {"link": final_store_url}
            }
        }
        if texts:
            group_payload["texts"] = texts
        
        # inline creative: 첫 번째 video_id 사용
        inline_video_data = {
            "video_id": videos[0]["video_id"],
            "call_to_action": {
                "type": default_cta,
                "value": {"link": final_store_url}
            },
        }
        
        # 썸네일 제공 (첫 번째 비디오의 썸네일 사용)
        first_video_num = valid_videos[0]["video_num"]
        thumb_url = thumb_urls.get(first_video_num)
        if thumb_url:
            # ✅ 수정: image_url 대신 image_hash 사용
            # thumb_url은 실제로 upload_thumbnail_image에서 반환한 hash 값
            if thumb_url.startswith(("http://", "https://")):
                inline_video_data["image_url"] = thumb_url
            else:
                inline_video_data["image_hash"] = thumb_url
        else:
            raise RuntimeError("썸네일(image_hash) 생성 실패: object_story_spec.video_data에 필요함")
        
        # Object Story Spec 구성
        inline_object_story_spec = {
            "page_id": str(page_id),
            "video_data": inline_video_data
        }
        
        # IG identity (optional)
        ig_actor_id = (settings.get("instagram_actor_id") or "").strip()
        if ig_actor_id:
            inline_object_story_spec["instagram_actor_id"] = ig_actor_id
        
        # Multi-advertiser ads 토글
        multi_opt_in = bool(settings.get("multi_advertiser_ads_opt_in", False))
        multi_enroll_status = "OPT_IN" if multi_opt_in else "OPT_OUT"

        # Creative 구성
        creative_config = {
            "name": ad_name,
            "actor_id": str(page_id),
            "object_story_spec": inline_object_story_spec,
            "contextual_multi_ads": {"enroll_status": multi_enroll_status},
        }
        
        if ig_actor_id:
            creative_config["instagram_actor_id"] = ig_actor_id
        
        ad_params = {
            "name": ad_name,
            "adset_id": adset_id,
            "creative": creative_config,
            "creative_asset_groups_spec": {
                "groups": [group_payload]
            },
            "status": Ad.Status.active,
        }
        
        ad_response = account.create_ad(fields=[], params=ad_params)
        ad_id = ad_response.get("id")
        if not ad_id:
            raise RuntimeError(f"Ad 생성 응답에 id가 없습니다: {ad_response}")
        
        # 완료
        _update_progress("✅ 완료", 1, 1, 20, 80)
        overall_prog.progress(1.0, text="✅ 모든 작업 완료!")
        status_text.empty()
        
        st.success(f"✅ Flexible Ad 생성 완료: {ad_name} / {ad_id}")
        
        return {
            "ads": [{
                "name": ad_name,
                "ad_id": ad_id,
                "creative_id": None,
                "video_groups": [vid_data["video_num"] for vid_data in valid_videos],
                "total_videos": len(videos)
            }],
            "errors": [],
            "total_created": 1
        }
        
    except Exception as e:
        overall_prog.empty()
        status_text.empty()
        error_msg = f"Flexible Ad 생성 실패: {e}"
        st.error(f"❌ {error_msg}")
        return {
            "ads": [],
            "errors": [error_msg],
            "total_created": 0
        }


def _upload_dynamic_16x9_ads(
    account, page_id: str, adset_id: str, uploaded_files: list,
    settings: dict, store_url: str, max_workers: int, game_name: str
) -> dict:
    """
    다이내믹-16:9 모드:
    - 모든 비디오가 1920x1080 사이즈여야 함
    - 최대 10개 비디오
    - 하나의 Flexible Ad 생성
    - Ad name suffix: 가로
    """
    logger = logging.getLogger(__name__)

    # ====================================================================
    # STEP 0: 템플릿 로드
    # ====================================================================
    st.info("📋 AdSet에서 템플릿 정보 가져오는 중...")
    template = fetch_latest_ad_creative_defaults(adset_id)

    # Primary Texts
    default_primary_texts = []
    if template.get("primary_texts"):
        default_primary_texts = [pt.strip() for pt in template["primary_texts"] if pt.strip()]
    elif settings.get("primary_text"):
        text = settings["primary_text"].strip()
        default_primary_texts = [t.strip() for t in text.split("\n\n") if t.strip()]

    # Headlines
    default_headlines = []
    if template.get("headlines"):
        for h in template["headlines"]:
            cleaned = h.strip()
            if cleaned and cleaned.lower() != "new game":
                default_headlines.append(cleaned)
    elif settings.get("headline"):
        headline = settings["headline"].strip()
        default_headlines = [h.strip() for h in headline.split("\n") if h.strip()]

    if default_primary_texts is None:
        default_primary_texts = []
    if default_headlines is None:
        default_headlines = []

    # CTA 우선순위: UI(settings) > template > default
    default_cta = (settings.get("call_to_action") or "").strip()
    if not default_cta:
        default_cta = (template.get("call_to_action") or "").strip()
    if not default_cta:
        default_cta = "INSTALL_MOBILE_APP"

    # Store URL
    final_store_url = ""
    try:
        adset = AdSet(adset_id)
        adset_data = adset.api_get(fields=["promoted_object"])
        promoted_obj = adset_data.get("promoted_object", {})
        adset_store_url = promoted_obj.get("object_store_url", "")

        if adset_store_url:
            final_store_url = sanitize_store_url(adset_store_url)
            st.info(f"✅ AdSet의 Store URL 사용: {final_store_url[:60]}...")
        else:
            st.warning("⚠️ AdSet에 promoted_object가 없습니다")
    except Exception as e:
        st.warning(f"⚠️ AdSet 조회 실패: {e}")

    if not final_store_url:
        if store_url:
            final_store_url = sanitize_store_url(store_url)
        elif settings.get("store_url"):
            final_store_url = sanitize_store_url(settings["store_url"])

    if not final_store_url:
        raise RuntimeError("❌ Store URL이 없습니다!")
    if not final_store_url.startswith("http"):
        raise RuntimeError(f"❌ 유효하지 않은 Store URL: {final_store_url}")

    _dev_success("✅ 템플릿 로드 완료")
    _dev_caption(f"📝 Primary Texts: {len(default_primary_texts)}개")
    _dev_caption(f"📰 Headlines: {len(default_headlines)}개")
    _dev_caption(f"🎯 CTA: {default_cta}")
    _dev_caption(f"🔗 Store URL: {final_store_url[:50]}...")

    # Prefix/Suffix
    use_prefix = settings.get("use_prefix", False)
    prefix_text = settings.get("prefix_text", "").strip()
    use_suffix = settings.get("use_suffix", False)
    suffix_text = settings.get("suffix_text", "").strip()

    # ====================================================================
    # STEP 1: 비디오 검증 (1920x1080만, 개수 체크)
    # ====================================================================
    def _extract_video_number(fname):
        match = re.search(r"video(\d+)", fname.lower())
        return f"video{match.group(1)}" if match else None

    def _extract_resolution(fname):
        if "1920x1080" in fname.lower():
            return "1920x1080"
        return None

    valid_videos = []
    user_msg = "비디오사이즈가 16x9(1920x1080)가 맞는지 체크해주세요"

    for u in uploaded_files:
        fname = getattr(u, "name", None) or u.get("name", "")
        if not fname:
            continue

        video_num = _extract_video_number(fname)
        if not video_num:
            raise RuntimeError("파일명이 videoxxx 형식인지 확인해주세요")

        resolution = _extract_resolution(fname)
        if not resolution or resolution != "1920x1080":
            raise RuntimeError(user_msg)

        valid_videos.append({"video_num": video_num, "file": u, "fname": fname})

    # 3. 개수 체크 (10개 이하)
    if len(valid_videos) > 10:
        raise RuntimeError("❌ 다이내믹 광고는 10개이상의 동영상을 수용할 수 없습니다")

    if not valid_videos:
        raise RuntimeError(user_msg)

    _dev_success(f"✅ {len(valid_videos)}개 비디오 검증 완료 (1920x1080)")

    # ====================================================================
    # STEP 2: 모든 비디오 업로드 (다이내믹-1x1과 동일)
    # ====================================================================
    def _save_tmp(u):
        if isinstance(u, dict) and "path" in u:
            return {"name": u["name"], "path": u["path"]}
        if hasattr(u, "getbuffer"):
            suffix = pathlib.Path(u.name).suffix.lower() or ".mp4"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(u.getbuffer())
                return {"name": u.name, "path": tmp.name}
        raise ValueError("Unsupported video object")

    def _upload_video_with_title(path: str, title: str) -> str:
        if "facebook" in st.secrets:
            token = st.secrets["facebook"].get("access_token", "").strip()
        else:
            token = st.secrets.get("access_token", "").strip()

        act = account.get_id()
        file_size = os.path.getsize(path)

        def _post(data, files=None):
            req = build_advideos_resumable_request(
                account_id=act,
                data={**data, "access_token": token},
                files=files,
                timeout=180,
            )
            r = execute_request(req, build_no_retry_policy())
            j = r.json()
            if "error" in j:
                raise RuntimeError(j["error"].get("message"))
            return j

        start_resp = _post(
            {
                "upload_phase": "start",
                "file_size": str(file_size),
                "title": title,
                "content_category": "VIDEO_GAMING",
            }
        )

        sess_id = start_resp["upload_session_id"]
        vid_id = start_resp["video_id"]
        start_off = int(start_resp.get("start_offset", 0))
        end_off = int(start_resp.get("end_offset", 0))

        with open(path, "rb") as f:
            while True:
                if start_off == end_off == file_size:
                    break
                if end_off <= start_off:
                    tr = _post(
                        {"upload_phase": "transfer", "upload_session_id": sess_id, "start_offset": str(start_off)}
                    )
                    start_off = int(tr.get("start_offset", start_off))
                    end_off = int(tr.get("end_offset", end_off or file_size))
                    continue

                f.seek(start_off)
                chunk = f.read(end_off - start_off)
                tr = _post(
                    {"upload_phase": "transfer", "upload_session_id": sess_id, "start_offset": str(start_off)},
                    files={"video_file_chunk": ("chunk.bin", chunk, "application/octet-stream")},
                )
                start_off = int(tr.get("start_offset", start_off + len(chunk)))
                end_off = int(tr.get("end_offset", end_off))

        try:
            _post({"upload_phase": "finish", "upload_session_id": sess_id, "title": title})
        except Exception:
            pass

        return vid_id

    all_video_ids = {}
    thumb_urls = {}

    tasks = []
    for vid_data in valid_videos:
        video_num = vid_data["video_num"]
        f_obj = vid_data["file"]
        fname = vid_data["fname"]
        all_video_ids[video_num] = {}
        tasks.append((video_num, f_obj, fname))

    total_uploads = len(tasks)

    # 통합 프로그래스바
    overall_prog = st.progress(0, text="🚀 전체 진행 중... 0%")
    status_text = st.empty()

    def _update_progress(stage: str, current: int, total: int, stage_pct: int, base_pct: int = 0):
        stage_progress = int((current / total) * stage_pct) if total > 0 else 0
        overall_pct = base_pct + stage_progress
        overall_prog.progress(overall_pct / 100, text=f"🚀 {stage}... {current}/{total} ({overall_pct}%)")
        status_text.text(f"📊 현재 단계: {stage} | 진행률: {overall_pct}%")

    def _upload_one(video_num: str, f_obj, fname: str):
        file_data = _save_tmp(f_obj)

        if video_num not in thumb_urls:
            try:
                thumb_path = extract_thumbnail_from_video(file_data["path"])
                thumb_urls[video_num] = upload_thumbnail_image(account, thumb_path)
                try:
                    os.unlink(thumb_path)
                except Exception:
                    pass
            except Exception:
                thumb_urls[video_num] = None

        vid_id = _upload_video_with_title(file_data["path"], fname)
        return (video_num, vid_id)

    # STEP 2-1: 업로드 (0-40)
    upload_workers = min(4, max(2, total_uploads))
    upload_errors = []

    with ThreadPoolExecutor(max_workers=upload_workers) as ex:
        futs = {ex.submit(_upload_one, vn, fo, fname): (vn, fname) for (vn, fo, fname) in tasks}

        done = 0
        for fut in as_completed(futs):
            done += 1
            _update_progress("📤 비디오 업로드", done, total_uploads, 40, 0)
            try:
                video_num, vid_id = fut.result()
                all_video_ids[video_num] = vid_id
            except Exception as e:
                vn, fname = futs[fut]
                upload_errors.append(f"{fname}: {e}")
                st.error(f"❌ {fname} 업로드 실패: {e}")

    if upload_errors:
        overall_prog.empty()
        status_text.empty()
        raise RuntimeError("Upload failed for some videos:\n" + "\n".join(upload_errors))

    # STEP 2-2: ready 대기 (40-80)
    all_vids = list(all_video_ids.values())
    errs = []
    done = 0
    with ThreadPoolExecutor(max_workers=min(6, max(2, len(all_vids)))) as ex:
        futs = {ex.submit(wait_video_ready, vid, 300, 1.0): vid for vid in all_vids}
        for fut in as_completed(futs):
            done += 1
            _update_progress("⏳ 비디오 처리 대기", done, len(all_vids), 40, 40)
            vid = futs[fut]
            try:
                fut.result()
            except Exception as e:
                errs.append(f"{vid}: {e}")

    if errs:
        overall_prog.empty()
        status_text.empty()
        raise RuntimeError("Some videos did not become ready:\n" + "\n".join(errs))

    # ====================================================================
    # STEP 3: Ad 이름 생성 (다이내믹-1x1과 동일, suffix만 '가로')
    # ====================================================================
    def _extract_game_name_from_filename(fname):
        match = re.search(r"video\d+_(.+?)_[a-z]{2}_\d+s_", fname.lower())
        if match:
            return match.group(1)
        return None

    extracted_game_names = []
    for vid_data in valid_videos:
        fname = vid_data["fname"]
        game_name_from_file = _extract_game_name_from_filename(fname)
        if game_name_from_file:
            extracted_game_names.append(game_name_from_file)

    if extracted_game_names:
        from collections import Counter

        game_name_counter = Counter(extracted_game_names)
        game_name_clean = game_name_counter.most_common(1)[0][0]
        st.info(f"📝 파일명에서 추출한 게임 이름: {game_name_clean}")
    else:
        game_name_clean = re.sub(r"[^\w]", "", (game_name or "").lower())
        st.warning(f"⚠️ 파일명에서 게임 이름을 추출할 수 없어 기본값 사용: {game_name_clean}")

    video_numbers = []
    for vid_data in valid_videos:
        video_num = vid_data["video_num"]
        match = re.search(r"video(\d+)", video_num.lower())
        if match:
            video_numbers.append(int(match.group(1)))

    if not video_numbers:
        raise RuntimeError("❌ 비디오 번호를 추출할 수 없습니다.")

    video_label = _build_video_ranges_label(video_numbers)
    if not video_label:
        raise RuntimeError("❌ 비디오 번호를 추출할 수 없습니다.")

    ad_name_setting = settings.get("dco_creative_name", "").strip()
    if ad_name_setting:
        ad_name = ad_name_setting
    else:
        ad_name = f"{video_label}_{game_name_clean}_flexible_가로"

    if use_prefix and prefix_text:
        ad_name = f"{prefix_text}_{ad_name}"
    if use_suffix and suffix_text:
        ad_name = f"{ad_name}_{suffix_text}"

    # ====================================================================
    # STEP 4: 하나의 Flexible Ad 생성 (80-100%)
    # ====================================================================
    _update_progress("🎨 Flexible Ad 생성", 0, 1, 20, 80)
    try:
        videos = [{"video_id": vid_id} for vid_id in all_video_ids.values()]

        # 텍스트 필터링 (Flexible Ad Format 제한: text_type당 최대 5개)
        final_primary_texts = []
        for pt in (default_primary_texts or []):
            pt = (pt or "").strip()
            if pt:
                final_primary_texts.append(pt)
        final_primary_texts = final_primary_texts[:5]

        final_headlines = []
        for hl in (default_headlines or []):
            hl = (hl or "").strip()
            if hl and hl.lower() != "new game":
                final_headlines.append(hl)
        final_headlines = final_headlines[:5]

        texts = (
            [{"text": t, "text_type": "primary_text"} for t in final_primary_texts]
            + [{"text": t, "text_type": "headline"} for t in final_headlines]
        )

        group_payload = {
            "videos": videos,
            "call_to_action": {"type": default_cta, "value": {"link": final_store_url}},
        }
        if texts:
            group_payload["texts"] = texts

        inline_video_data = {
            "video_id": videos[0]["video_id"],
            "call_to_action": {"type": default_cta, "value": {"link": final_store_url}},
        }

        first_video_num = valid_videos[0]["video_num"]
        thumb_url = thumb_urls.get(first_video_num)
        if thumb_url:
            # ✅ 수정: image_url 대신 image_hash 사용
            # thumb_url은 실제로 upload_thumbnail_image에서 반환한 hash 값
            if thumb_url.startswith(("http://", "https://")):
                inline_video_data["image_url"] = thumb_url
            else:
                inline_video_data["image_hash"] = thumb_url
        else:
            raise RuntimeError("썸네일(image_hash) 생성 실패: object_story_spec.video_data에 필요함")

        inline_object_story_spec = {"page_id": str(page_id), "video_data": inline_video_data}

        ig_actor_id = (settings.get("instagram_actor_id") or "").strip()
        if ig_actor_id:
            inline_object_story_spec["instagram_actor_id"] = ig_actor_id

        multi_opt_in = bool(settings.get("multi_advertiser_ads_opt_in", False))
        multi_enroll_status = "OPT_IN" if multi_opt_in else "OPT_OUT"

        creative_config = {
            "name": ad_name,
            "actor_id": str(page_id),
            "object_story_spec": inline_object_story_spec,
            "contextual_multi_ads": {"enroll_status": multi_enroll_status},
        }
        if ig_actor_id:
            creative_config["instagram_actor_id"] = ig_actor_id

        ad_params = {
            "name": ad_name,
            "adset_id": adset_id,
            "creative": creative_config,
            "creative_asset_groups_spec": {"groups": [group_payload]},
            "status": Ad.Status.active,
        }

        ad_response = account.create_ad(fields=[], params=ad_params)
        ad_id = ad_response.get("id")
        if not ad_id:
            raise RuntimeError(f"Ad 생성 응답에 id가 없습니다: {ad_response}")

        _update_progress("✅ 완료", 1, 1, 20, 80)
        overall_prog.progress(1.0, text="✅ 모든 작업 완료!")
        status_text.empty()

        st.success(f"✅ Flexible Ad 생성 완료: {ad_name} / {ad_id}")

        return {
            "ads": [
                {
                    "name": ad_name,
                    "ad_id": ad_id,
                    "creative_id": None,
                    "video_groups": [vid_data["video_num"] for vid_data in valid_videos],
                    "total_videos": len(videos),
                }
            ],
            "errors": [],
            "total_created": 1,
        }

    except Exception as e:
        overall_prog.empty()
        status_text.empty()
        error_msg = f"Flexible Ad 생성 실패: {e}"
        st.error(f"❌ {error_msg}")
        return {"ads": [], "errors": [error_msg], "total_created": 0}


def _upload_dynamic_9x16_ads(
    account, page_id: str, adset_id: str, uploaded_files: list,
    settings: dict, store_url: str, max_workers: int, game_name: str
) -> dict:
    """
    다이내믹-9x16 모드:
    - 모든 비디오가 1080x1920 사이즈여야 함
    - 최대 10개 비디오
    - 하나의 Flexible Ad 생성
    - Ad name suffix: 세로
    """
    logger = logging.getLogger(__name__)

    # ====================================================================
    # STEP 0: 템플릿 로드
    # ====================================================================
    st.info("📋 AdSet에서 템플릿 정보 가져오는 중...")
    template = fetch_latest_ad_creative_defaults(adset_id)

    # Primary Texts
    default_primary_texts = []
    if template.get("primary_texts"):
        default_primary_texts = [pt.strip() for pt in template["primary_texts"] if pt.strip()]
    elif settings.get("primary_text"):
        text = settings["primary_text"].strip()
        default_primary_texts = [t.strip() for t in text.split("\n\n") if t.strip()]

    # Headlines
    default_headlines = []
    if template.get("headlines"):
        for h in template["headlines"]:
            cleaned = h.strip()
            if cleaned and cleaned.lower() != "new game":
                default_headlines.append(cleaned)
    elif settings.get("headline"):
        headline = settings["headline"].strip()
        default_headlines = [h.strip() for h in headline.split("\n") if h.strip()]

    if default_primary_texts is None:
        default_primary_texts = []
    if default_headlines is None:
        default_headlines = []

    # CTA 우선순위: UI(settings) > template > default
    default_cta = (settings.get("call_to_action") or "").strip()
    if not default_cta:
        default_cta = (template.get("call_to_action") or "").strip()
    if not default_cta:
        default_cta = "INSTALL_MOBILE_APP"

    # Store URL
    final_store_url = ""
    try:
        adset = AdSet(adset_id)
        adset_data = adset.api_get(fields=["promoted_object"])
        promoted_obj = adset_data.get("promoted_object", {})
        adset_store_url = promoted_obj.get("object_store_url", "")

        if adset_store_url:
            final_store_url = sanitize_store_url(adset_store_url)
            st.info(f"✅ AdSet의 Store URL 사용: {final_store_url[:60]}...")
        else:
            st.warning("⚠️ AdSet에 promoted_object가 없습니다")
    except Exception as e:
        st.warning(f"⚠️ AdSet 조회 실패: {e}")

    if not final_store_url:
        if store_url:
            final_store_url = sanitize_store_url(store_url)
        elif settings.get("store_url"):
            final_store_url = sanitize_store_url(settings["store_url"])

    if not final_store_url:
        raise RuntimeError("❌ Store URL이 없습니다!")
    if not final_store_url.startswith("http"):
        raise RuntimeError(f"❌ 유효하지 않은 Store URL: {final_store_url}")

    _dev_success("✅ 템플릿 로드 완료")
    _dev_caption(f"📝 Primary Texts: {len(default_primary_texts)}개")
    _dev_caption(f"📰 Headlines: {len(default_headlines)}개")
    _dev_caption(f"🎯 CTA: {default_cta}")
    _dev_caption(f"🔗 Store URL: {final_store_url[:50]}...")

    # Prefix/Suffix
    use_prefix = settings.get("use_prefix", False)
    prefix_text = settings.get("prefix_text", "").strip()
    use_suffix = settings.get("use_suffix", False)
    suffix_text = settings.get("suffix_text", "").strip()

    # ====================================================================
    # STEP 1: 비디오 검증 (1080x1920만, 개수 체크)
    # ====================================================================
    def _extract_video_number(fname):
        match = re.search(r"video(\d+)", fname.lower())
        return f"video{match.group(1)}" if match else None

    def _extract_resolution(fname):
        if "1080x1920" in fname.lower():
            return "1080x1920"
        return None

    valid_videos = []
    user_msg = "비디오사이즈가 9x16(1080x1920)가 맞는지 체크해주세요"

    for u in uploaded_files:
        fname = getattr(u, "name", None) or u.get("name", "")
        if not fname:
            continue

        video_num = _extract_video_number(fname)
        if not video_num:
            raise RuntimeError("파일명이 videoxxx 형식인지 확인해주세요")

        resolution = _extract_resolution(fname)
        if not resolution or resolution != "1080x1920":
            raise RuntimeError(user_msg)

        valid_videos.append({"video_num": video_num, "file": u, "fname": fname})

    # 3. 개수 체크 (10개 이하)
    if len(valid_videos) > 10:
        raise RuntimeError("❌ 다이내믹 광고는 10개이상의 동영상을 수용할 수 없습니다")

    if not valid_videos:
        raise RuntimeError(user_msg)

    _dev_success(f"✅ {len(valid_videos)}개 비디오 검증 완료 (1080x1920)")

    # ====================================================================
    # STEP 2: 모든 비디오 업로드 (다이내믹-1x1과 동일)
    # ====================================================================
    def _save_tmp(u):
        if isinstance(u, dict) and "path" in u:
            return {"name": u["name"], "path": u["path"]}
        if hasattr(u, "getbuffer"):
            suffix = pathlib.Path(u.name).suffix.lower() or ".mp4"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(u.getbuffer())
                return {"name": u.name, "path": tmp.name}
        raise ValueError("Unsupported video object")

    def _upload_video_with_title(path: str, title: str) -> str:
        if "facebook" in st.secrets:
            token = st.secrets["facebook"].get("access_token", "").strip()
        else:
            token = st.secrets.get("access_token", "").strip()

        act = account.get_id()
        file_size = os.path.getsize(path)

        def _post(data, files=None):
            req = build_advideos_resumable_request(
                account_id=act,
                data={**data, "access_token": token},
                files=files,
                timeout=180,
            )
            r = execute_request(req, build_no_retry_policy())
            j = r.json()
            if "error" in j:
                raise RuntimeError(j["error"].get("message"))
            return j

        start_resp = _post(
            {
                "upload_phase": "start",
                "file_size": str(file_size),
                "title": title,
                "content_category": "VIDEO_GAMING",
            }
        )

        sess_id = start_resp["upload_session_id"]
        vid_id = start_resp["video_id"]
        start_off = int(start_resp.get("start_offset", 0))
        end_off = int(start_resp.get("end_offset", 0))

        with open(path, "rb") as f:
            while True:
                if start_off == end_off == file_size:
                    break
                if end_off <= start_off:
                    tr = _post(
                        {"upload_phase": "transfer", "upload_session_id": sess_id, "start_offset": str(start_off)}
                    )
                    start_off = int(tr.get("start_offset", start_off))
                    end_off = int(tr.get("end_offset", end_off or file_size))
                    continue

                f.seek(start_off)
                chunk = f.read(end_off - start_off)
                tr = _post(
                    {"upload_phase": "transfer", "upload_session_id": sess_id, "start_offset": str(start_off)},
                    files={"video_file_chunk": ("chunk.bin", chunk, "application/octet-stream")},
                )
                start_off = int(tr.get("start_offset", start_off + len(chunk)))
                end_off = int(tr.get("end_offset", end_off))

        try:
            _post({"upload_phase": "finish", "upload_session_id": sess_id, "title": title})
        except Exception:
            pass

        return vid_id

    all_video_ids = {}
    thumb_urls = {}

    tasks = []
    for vid_data in valid_videos:
        video_num = vid_data["video_num"]
        f_obj = vid_data["file"]
        fname = vid_data["fname"]
        all_video_ids[video_num] = {}
        tasks.append((video_num, f_obj, fname))

    total_uploads = len(tasks)

    overall_prog = st.progress(0, text="🚀 전체 진행 중... 0%")
    status_text = st.empty()

    def _update_progress(stage: str, current: int, total: int, stage_pct: int, base_pct: int = 0):
        stage_progress = int((current / total) * stage_pct) if total > 0 else 0
        overall_pct = base_pct + stage_progress
        overall_prog.progress(overall_pct / 100, text=f"🚀 {stage}... {current}/{total} ({overall_pct}%)")
        status_text.text(f"📊 현재 단계: {stage} | 진행률: {overall_pct}%")

    def _upload_one(video_num: str, f_obj, fname: str):
        file_data = _save_tmp(f_obj)

        if video_num not in thumb_urls:
            try:
                thumb_path = extract_thumbnail_from_video(file_data["path"])
                thumb_urls[video_num] = upload_thumbnail_image(account, thumb_path)
                try:
                    os.unlink(thumb_path)
                except Exception:
                    pass
            except Exception:
                thumb_urls[video_num] = None

        vid_id = _upload_video_with_title(file_data["path"], fname)
        return (video_num, vid_id)

    # STEP 2-1: 업로드 (0-40)
    upload_workers = min(4, max(2, total_uploads))
    upload_errors = []

    with ThreadPoolExecutor(max_workers=upload_workers) as ex:
        futs = {ex.submit(_upload_one, vn, fo, fname): (vn, fname) for (vn, fo, fname) in tasks}

        done = 0
        for fut in as_completed(futs):
            done += 1
            _update_progress("📤 비디오 업로드", done, total_uploads, 40, 0)
            try:
                video_num, vid_id = fut.result()
                all_video_ids[video_num] = vid_id
            except Exception as e:
                vn, fname = futs[fut]
                upload_errors.append(f"{fname}: {e}")
                st.error(f"❌ {fname} 업로드 실패: {e}")

    if upload_errors:
        overall_prog.empty()
        status_text.empty()
        raise RuntimeError("Upload failed for some videos:\n" + "\n".join(upload_errors))

    # STEP 2-2: ready 대기 (40-80)
    all_vids = list(all_video_ids.values())
    errs = []
    done = 0
    with ThreadPoolExecutor(max_workers=min(6, max(2, len(all_vids)))) as ex:
        futs = {ex.submit(wait_video_ready, vid, 300, 1.0): vid for vid in all_vids}
        for fut in as_completed(futs):
            done += 1
            _update_progress("⏳ 비디오 처리 대기", done, len(all_vids), 40, 40)
            vid = futs[fut]
            try:
                fut.result()
            except Exception as e:
                errs.append(f"{vid}: {e}")

    if errs:
        overall_prog.empty()
        status_text.empty()
        raise RuntimeError("Some videos did not become ready:\n" + "\n".join(errs))

    # ====================================================================
    # STEP 3: Ad 이름 생성 (다이내믹-1x1과 동일, suffix만 '세로')
    # ====================================================================
    def _extract_game_name_from_filename(fname):
        match = re.search(r"video\d+_(.+?)_[a-z]{2}_\d+s_", fname.lower())
        if match:
            return match.group(1)
        return None

    extracted_game_names = []
    for vid_data in valid_videos:
        fname = vid_data["fname"]
        game_name_from_file = _extract_game_name_from_filename(fname)
        if game_name_from_file:
            extracted_game_names.append(game_name_from_file)

    if extracted_game_names:
        from collections import Counter

        game_name_counter = Counter(extracted_game_names)
        game_name_clean = game_name_counter.most_common(1)[0][0]
        st.info(f"📝 파일명에서 추출한 게임 이름: {game_name_clean}")
    else:
        game_name_clean = re.sub(r"[^\w]", "", (game_name or "").lower())
        st.warning(f"⚠️ 파일명에서 게임 이름을 추출할 수 없어 기본값 사용: {game_name_clean}")

    video_numbers = []
    for vid_data in valid_videos:
        video_num = vid_data["video_num"]
        match = re.search(r"video(\d+)", video_num.lower())
        if match:
            video_numbers.append(int(match.group(1)))

    if not video_numbers:
        raise RuntimeError("❌ 비디오 번호를 추출할 수 없습니다.")

    video_label = _build_video_ranges_label(video_numbers)
    if not video_label:
        raise RuntimeError("❌ 비디오 번호를 추출할 수 없습니다.")

    ad_name_setting = settings.get("dco_creative_name", "").strip()
    if ad_name_setting:
        ad_name = ad_name_setting
    else:
        ad_name = f"{video_label}_{game_name_clean}_flexible_세로"

    if use_prefix and prefix_text:
        ad_name = f"{prefix_text}_{ad_name}"
    if use_suffix and suffix_text:
        ad_name = f"{ad_name}_{suffix_text}"

    # ====================================================================
    # STEP 4: 하나의 Flexible Ad 생성 (80-100%)
    # ====================================================================
    _update_progress("🎨 Flexible Ad 생성", 0, 1, 20, 80)
    try:
        videos = [{"video_id": vid_id} for vid_id in all_video_ids.values()]

        # 텍스트 필터링 (Flexible Ad Format 제한: text_type당 최대 5개)
        final_primary_texts = []
        for pt in (default_primary_texts or []):
            pt = (pt or "").strip()
            if pt:
                final_primary_texts.append(pt)
        final_primary_texts = final_primary_texts[:5]

        final_headlines = []
        for hl in (default_headlines or []):
            hl = (hl or "").strip()
            if hl and hl.lower() != "new game":
                final_headlines.append(hl)
        final_headlines = final_headlines[:5]

        texts = (
            [{"text": t, "text_type": "primary_text"} for t in final_primary_texts]
            + [{"text": t, "text_type": "headline"} for t in final_headlines]
        )

        group_payload = {
            "videos": videos,
            "call_to_action": {"type": default_cta, "value": {"link": final_store_url}},
        }
        if texts:
            group_payload["texts"] = texts

        inline_video_data = {
            "video_id": videos[0]["video_id"],
            "call_to_action": {"type": default_cta, "value": {"link": final_store_url}},
        }

        first_video_num = valid_videos[0]["video_num"]
        thumb_url = thumb_urls.get(first_video_num)
        if thumb_url:
            # ✅ 수정: image_url 대신 image_hash 사용
            # thumb_url은 실제로 upload_thumbnail_image에서 반환한 hash 값
            if thumb_url.startswith(("http://", "https://")):
                inline_video_data["image_url"] = thumb_url
            else:
                inline_video_data["image_hash"] = thumb_url
        else:
            raise RuntimeError("썸네일(image_hash) 생성 실패: object_story_spec.video_data에 필요함")

        inline_object_story_spec = {"page_id": str(page_id), "video_data": inline_video_data}

        ig_actor_id = (settings.get("instagram_actor_id") or "").strip()
        if ig_actor_id:
            inline_object_story_spec["instagram_actor_id"] = ig_actor_id

        multi_opt_in = bool(settings.get("multi_advertiser_ads_opt_in", False))
        multi_enroll_status = "OPT_IN" if multi_opt_in else "OPT_OUT"

        creative_config = {
            "name": ad_name,
            "actor_id": str(page_id),
            "object_story_spec": inline_object_story_spec,
            "contextual_multi_ads": {"enroll_status": multi_enroll_status},
        }
        if ig_actor_id:
            creative_config["instagram_actor_id"] = ig_actor_id

        ad_params = {
            "name": ad_name,
            "adset_id": adset_id,
            "creative": creative_config,
            "creative_asset_groups_spec": {"groups": [group_payload]},
            "status": Ad.Status.active,
        }

        ad_response = account.create_ad(fields=[], params=ad_params)
        ad_id = ad_response.get("id")
        if not ad_id:
            raise RuntimeError(f"Ad 생성 응답에 id가 없습니다: {ad_response}")

        _update_progress("✅ 완료", 1, 1, 20, 80)
        overall_prog.progress(1.0, text="✅ 모든 작업 완료!")
        status_text.empty()

        st.success(f"✅ Flexible Ad 생성 완료: {ad_name} / {ad_id}")

        return {
            "ads": [
                {
                    "name": ad_name,
                    "ad_id": ad_id,
                    "creative_id": None,
                    "video_groups": [vid_data["video_num"] for vid_data in valid_videos],
                    "total_videos": len(videos),
                }
            ],
            "errors": [],
            "total_created": 1,
        }

    except Exception as e:
        overall_prog.empty()
        status_text.empty()
        error_msg = f"Flexible Ad 생성 실패: {e}"
        st.error(f"❌ {error_msg}")
        return {"ads": [], "errors": [error_msg], "total_created": 0}