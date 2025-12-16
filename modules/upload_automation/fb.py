"""Marketer-side Facebook helpers for Creative Auto-Upload.

Features:
1. Campaign/AdSet Selection
2. Ad Setup (Single Video / Dynamic)
3. Smart "Mimic" Defaults: 
   - Scans Ad Set for the "highest numbered" video ad.
   - Pre-fills Headline, Text, and CTA from that winner.
"""
from __future__ import annotations

import streamlit as st
import logging
import re
import os
import pathlib
import tempfile

# Import FB SDK objects
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.exceptions import FacebookRequestError
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
    

# Import base module (Assuming this exists in your project)
from facebook_ads import (
    GAME_DEFAULTS,
    FB_GAME_MAPPING,
    init_fb_from_secrets,
    _plan_upload,
    build_targeting_from_settings,
    create_creativetest_adset,
    sanitize_store_url,
    next_sat_0900_kst,
    init_fb_game_defaults,
    make_ad_name,
    validate_page_binding,
    upload_videos_create_ads,
    OPT_GOAL_LABEL_TO_API,
    extract_thumbnail_from_video,      # ✅ 추가
    upload_thumbnail_image,            # ✅ 추가
)


logger = logging.getLogger(__name__)

# --- Constants ---
FB_CTA_OPTIONS = [
    "INSTALL_MOBILE_APP", "PLAY_GAME", "USE_APP", "DOWNLOAD", 
    "SHOP_NOW", "LEARN_MORE", "SIGN_UP", "WATCH_MORE", "NO_BUTTON"
]

# --- Helper Functions ---

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

# --- Cached Data Fetchers ---

@st.cache_data(ttl=300, show_spinner=False)
def fetch_active_campaigns_cached(account_id: str) -> list[dict]:
    """Fetch ACTIVE campaigns."""
    try:
        account = init_fb_from_secrets(account_id)
        campaigns = account.get_campaigns(
            fields=[Campaign.Field.name, Campaign.Field.id],
            params={"effective_status": ["ACTIVE"], "limit": 100}
        )
        return [{"id": c["id"], "name": c["name"]} for c in campaigns]
    except Exception as e:
        logger.error(f"Error fetching campaigns: {e}")
        return []

@st.cache_data(ttl=300, show_spinner=False)
def fetch_active_adsets_cached(account_id: str, campaign_id: str) -> list[dict]:
    """Fetch adsets (excluding DELETED/ARCHIVED)."""
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
        logger.error(f"Error fetching adsets: {e}")
        return []

@st.cache_data(ttl=600, show_spinner=False)
def fetch_latest_ad_creative_defaults(adset_id: str) -> dict:
    """
    Fetches the highest numbered ad and extracts Text, Headline, CTA, AND Store URL.
    """
    try:
        adset = AdSet(adset_id)
        # Fetch ads
        ads = adset.get_ads(
            fields=[Ad.Field.name, Ad.Field.creative],
            params={"limit": 100, "effective_status": ["ACTIVE", "PAUSED", "ARCHIVED"]}
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
        logger.warning(f"Could not fetch ad defaults: {e}")
        return {}
# --- UI Renderer ---

def render_facebook_settings_panel(container, game: str, idx: int) -> None:
    """Render Facebook settings with Smart Defaults logic."""
    with container:
        st.markdown(f"#### {game} Settings")
        
        # 1. Config & Selection
        cfg = FB_GAME_MAPPING.get(game)
        account_id = cfg["account_id"]
        
        # Campaign Select
        campaigns = fetch_active_campaigns_cached(account_id)
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
        adsets = fetch_active_adsets_cached(account_id, sel_c_id)
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
        if st.session_state.get(f"prev_fb_a_{idx}") != sel_a_id:
             st.session_state[f"defaults_fetched_{idx}"] = False
             # Reset primary texts and headlines when AdSet changes
             st.session_state.pop(f"primary_texts_{idx}", None)
             st.session_state.pop(f"headlines_{idx}", None)
        st.session_state[f"prev_fb_a_{idx}"] = sel_a_id
        st.session_state[a_key] = sel_a_id

        st.divider()

        # --- SMART MIMIC LOGIC (Simplified) ---
        defaults = {}
        defaults_flag = f"defaults_fetched_{idx}"
        
        # Always fetch defaults regardless of format selection
        if not st.session_state.get(defaults_flag, False):
             with st.spinner(f"Mimicking highest number ad..."):
                defaults = fetch_latest_ad_creative_defaults(sel_a_id)
                st.session_state[f"mimic_data_{idx}"] = defaults 
                st.session_state[defaults_flag] = True
        else:
            defaults = st.session_state.get(f"mimic_data_{idx}", {})

        # Prep Default Values
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
            if defaults.get('full_asset_feed_spec'):
                with st.expander("🔍 View Full asset_feed_spec (Debug)", expanded=False):
                    spec = defaults.get('full_asset_feed_spec')
                    # Ensure it's a dict before passing to st.json
                    if isinstance(spec, dict):
                        st.json(spec)
                    else:
                        st.code(str(spec), language='text')

        # 2. Ad Setup
        st.caption("Ad Setup")
        
        # Ad Name
        col_d1, col_d2 = st.columns(2)
        dco_aspect_ratio = col_d1.selectbox("Ratio (For Preview)", ["1:1", "9:16", "16:9"], key=f"dco_r_{idx}")
        ad_name_input = col_d2.text_input("Ad Name", key=f"dco_n_{idx}")

        st.divider()

        # 3. Ad Creative Inputs
        col_head, col_info = st.columns([1, 2])
        col_head.caption("Creative Elements")
        if source_msg:
            col_info.info(source_msg, icon="🤖")

        # ✅ Primary Text - 태그 형태로 개별 관리
        st.markdown("**Primary Text**")
        
        # Initialize session state for primary texts
        primary_texts_key = f"primary_texts_{idx}"
        if primary_texts_key not in st.session_state:
            # Load from defaults or existing settings
            if p_texts:
                st.session_state[primary_texts_key] = p_texts.copy()
            elif defaults:
                # Try to split existing text
                existing = defaults.get("primary_texts", [])
                if existing:
                    st.session_state[primary_texts_key] = existing.copy()
                else:
                    st.session_state[primary_texts_key] = [""]
            else:
                st.session_state[primary_texts_key] = [""]
        
        primary_texts_list = st.session_state[primary_texts_key]
        
        # Display each primary text as editable tag
        for i, text in enumerate(primary_texts_list):
            col_text, col_del = st.columns([10, 1])
            with col_text:
                updated_text = st.text_input(
                    f"Primary Text {i+1}",
                    value=text,
                    key=f"pt_{idx}_{i}",
                    label_visibility="collapsed",
                    placeholder="Tell people what your ad is about" if not text else None
                )
                primary_texts_list[i] = updated_text
            with col_del:
                if st.button("❌", key=f"pt_del_{idx}_{i}", help="Delete this text"):
                    primary_texts_list.pop(i)
                    st.session_state[primary_texts_key] = primary_texts_list
                    st.rerun()
        
        # Add new primary text button
        if st.button("➕ Add Primary Text", key=f"pt_add_{idx}"):
            primary_texts_list.append("")
            st.session_state[primary_texts_key] = primary_texts_list
            st.rerun()
        
        # Join primary texts with double newline for backward compatibility
        primary_text = "\n\n".join([t.strip() for t in primary_texts_list if t.strip()])

        # ✅ Headlines - 태그 형태로 개별 관리
        st.markdown("**Headlines**")
        
        # Initialize session state for headlines
        headlines_key = f"headlines_{idx}"
        if headlines_key not in st.session_state:
            # Load from defaults or existing settings
            if h_lines:
                st.session_state[headlines_key] = h_lines.copy()
            elif defaults:
                existing = defaults.get("headlines", [])
                if existing:
                    st.session_state[headlines_key] = existing.copy()
                else:
                    st.session_state[headlines_key] = [""]
            else:
                st.session_state[headlines_key] = [""]
        
        headlines_list = st.session_state[headlines_key]
        
        # Display each headline as editable tag
        for i, headline_text in enumerate(headlines_list):
            col_head, col_del = st.columns([10, 1])
            with col_head:
                updated_headline = st.text_input(
                    f"Headline {i+1}",
                    value=headline_text,
                    key=f"hl_{idx}_{i}",
                    label_visibility="collapsed",
                    placeholder="Write a short headline" if not headline_text else None
                )
                headlines_list[i] = updated_headline
            with col_del:
                if st.button("❌", key=f"hl_del_{idx}_{i}", help="Delete this headline"):
                    headlines_list.pop(i)
                    st.session_state[headlines_key] = headlines_list
                    st.rerun()
        
        # Add new headline button
        if st.button("➕ Add Headline", key=f"hl_add_{idx}"):
            headlines_list.append("")
            st.session_state[headlines_key] = headlines_list
            st.rerun()
        
        # Join headlines with newline for backward compatibility
        headline = "\n".join([h.strip() for h in headlines_list if h.strip()])

        # CTA
        call_to_action = st.selectbox(
            "Call to Action", 
            FB_CTA_OPTIONS, 
            index=val_cta_idx,
            key=f"cta_{idx}"
        )

        # Final Save
        st.session_state.settings[game] = {
            "campaign_id": sel_c_id,
            "adset_id": sel_a_id,
            "creative_type": "Dynamic Creative",
            "dco_aspect_ratio": dco_aspect_ratio,
            "dco_creative_name": ad_name_input,
            "single_creative_name": None,
            "primary_text": primary_text,
            "headline": headline,
            "call_to_action": call_to_action,
        }


        # --------------------------------------------------------------------
# Main Execution Function (Add this to the bottom of fb.py)
# --------------------------------------------------------------------

def upload_to_facebook(
    game_name: str,
    uploaded_files: list,
    settings: dict,
    *,
    simulate: bool = False,
) -> dict:
    """
    Main entry point called by main.py.
    Orchestrates the entire flow: Auth -> Plan -> Create Ad Set -> Upload Videos -> Create Ads.
    """
    
    # 1. Validation & Setup
    if game_name not in FB_GAME_MAPPING:
        raise ValueError(f"No FB mapping configured for game: {game_name}")

    cfg = FB_GAME_MAPPING[game_name]
    account = init_fb_from_secrets(cfg["account_id"])

    # Resolve Page ID
    page_id_key = cfg.get("page_id_key")
    if "facebook" in st.secrets and page_id_key in st.secrets["facebook"]:
        page_id = st.secrets["facebook"][page_id_key]
    elif page_id_key in st.secrets:
        page_id = st.secrets[page_id_key]
    else:
        raise RuntimeError(f"Missing {page_id_key} in secrets.")

    # Validate Page & Get Instagram Actor
    page_check = validate_page_binding(account, page_id)
    ig_actor_id_from_page = page_check.get("instagram_business_account_id")
    
    # Store IG actor in session for the uploader to use
    if ig_actor_id_from_page:
        st.session_state["ig_actor_id_from_page"] = ig_actor_id_from_page

    settings = dict(settings or {})
    
    # MARKETER MODE: Use selected adset if available
    selected_adset_id = settings.get("adset_id")
    if selected_adset_id:
        # Get store URL
        game_defaults = GAME_DEFAULTS.get(game_name, {})
        store_url = (settings.get("store_url") or "").strip()
        if not store_url:
            campaign_id = settings.get("campaign_id", "")
            if "ios" in str(campaign_id).lower():
                store_url = game_defaults.get("store_url_ios", game_defaults.get("store_url", ""))
            else:
                store_url = game_defaults.get("store_url_aos", game_defaults.get("store_url", ""))
        if store_url:
            store_url = sanitize_store_url(store_url)
        
        # Upload directly to selected adset
        ad_name_prefix = settings.get("ad_name_prefix") if settings.get("ad_name_mode") == "Prefix + filename" else None
        upload_videos_create_ads(
            account=account,
            page_id=str(page_id),
            adset_id=selected_adset_id,
            uploaded_files=uploaded_files,
            ad_name_prefix=ad_name_prefix,
            store_url=store_url,
            try_instagram=True,
            settings=settings,
        )
        
        return {
            "campaign_id": settings.get("campaign_id"),
            "adset_id": selected_adset_id,
            "adset_name": "(Selected Ad Set)",
            "page_id": str(page_id),
            "n_videos": len(uploaded_files),
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
    if simulate:
        return plan

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
        try_instagram=True,
        settings=settings,  # <--- WE MUST PASS SETTINGS HERE
    )

    plan["adset_id"] = adset_id
    return plan

    # fb.py 하단에 추가

# fb.py 최하단 (upload_to_facebook 함수 아래에 추가)

def upload_videos_to_library_and_create_single_ads(
    account,  # ✅ 타입 힌트 제거 (순환 참조 방지)
    page_id: str,
    adset_id: str,
    uploaded_files: list,
    settings: dict,
    store_url: str = None,  # ✅ 추가
    max_workers: int = 6
) -> dict:
    """
    1. Upload videos to Ad Library (with original filename as title)
    2. Create Single Video Ads with placement-specific videos
    """
    # ✅ 필요한 모듈들을 함수 내부에서 import
    import os
    import pathlib
    import tempfile
    import requests
    import re
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from facebook_business.adobjects.adcreative import AdCreative
    from facebook_business.adobjects.ad import Ad
    
    # facebook_ads 모듈에서 필요한 함수들 import
    from facebook_ads import (
        extract_thumbnail_from_video,
        upload_thumbnail_image,
        sanitize_store_url
    )
    
    # ====================================================================
    # STEP 0: Get template from highest video in AdSet
    # ====================================================================
    st.info("🔍 AdSet에서 템플릿 정보 가져오는 중...")
    template = fetch_latest_ad_creative_defaults(adset_id)

    # ✅ 디버그 출력
    st.write("**🔍 Debug: Template Data**")
    st.json({
        "primary_texts": template.get("primary_texts", []),
        "headlines": template.get("headlines", []),
        "cta": template.get("call_to_action", ""),
        "store_url": template.get("store_url", "")[:50] if template.get("store_url") else ""
    })

    # ✅ 모든 Primary Text 복사
    default_primary_texts = []
    if template.get("primary_texts") and len(template["primary_texts"]) > 0:
        default_primary_texts = template["primary_texts"]
        st.write(f"✅ Loaded {len(default_primary_texts)} primary texts from template")
    elif settings.get("primary_text"):
        text = settings["primary_text"].strip()
        default_primary_texts = [t.strip() for t in text.split('\n\n') if t.strip()] if text else []
        st.write(f"✅ Loaded {len(default_primary_texts)} primary texts from settings")
    else:
        st.warning("⚠️ No primary texts found in template or settings!")

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
        st.write(f"✅ Loaded {len(default_primary_texts)} primary texts")
    else:
        st.warning("⚠️ No primary texts found in template or settings!")

    # ✅ 모든 Headline 복사 (배열 그대로)
    default_headlines = []
    if template.get("headlines") and len(template["headlines"]) > 0:
        default_headlines = template["headlines"]
    elif settings.get("headline"):
        # Settings에서 온 경우 '\n'로 split
        headline = settings["headline"].strip()
        default_headlines = [h.strip() for h in headline.split('\n') if h.strip()] if headline else []

    # ✅ CTA 복사 (템플릿 우선, 없으면 세팅, 없으면 기본값)
    default_cta = "INSTALL_MOBILE_APP"
    if template.get("call_to_action"):
        default_cta = template["call_to_action"]
    elif settings.get("call_to_action"):
        default_cta = settings["call_to_action"]

    # ✅ Store URL 결정 순서:
    # 1. 함수 인자로 전달받은 값 (upload_to_facebook에서 계산된 값)
    # 2. 템플릿(기존 광고)에서 가져온 값
    # 3. settings(UI 입력)에서 가져온 값
    
    final_store_url = store_url  # 인자로 받은 값 우선

    if not final_store_url and template.get("store_url"):
        final_store_url = template["store_url"]
    elif not final_store_url and settings.get("store_url"):
        final_store_url = settings["store_url"]

    if final_store_url:
        final_store_url = sanitize_store_url(final_store_url)
    
    # 결과 출력
    st.success(f"✅ 템플릿 로드 완료 (from: {template.get('source_ad_name', 'N/A')})")

    if default_primary_texts:
        st.caption(f"📝 Primary Texts: {len(default_primary_texts)}개")
        with st.expander("Primary Text 목록 보기", expanded=False):
            for idx, text in enumerate(default_primary_texts, 1):
                st.write(f"{idx}. {text[:80]}...")
    else:
        st.warning("⚠️ Primary Text 없음")

    if default_headlines:
        st.caption(f"📰 Headlines: {len(default_headlines)}개")
        with st.expander("Headline 목록 보기", expanded=False):
            for idx, h in enumerate(default_headlines, 1):
                st.write(f"{idx}. {h}")
    else:
        st.warning("⚠️ Headline 없음")

    st.caption(f"🎯 CTA: {default_cta}")

    if final_store_url:
        st.caption(f"�� Store URL: {final_store_url[:50]}...")
    else:
        st.error("❌ Store URL이 없습니다! 앱 설치 광고는 URL이 필수입니다.")
    
    # ====================================================================
    # STEP 1: Group videos by base name (video164, video165, ...)
    # ====================================================================
    def _extract_video_number(fname):
        """Extract video number from filename (e.g., video164)"""
        match = re.search(r'video(\d+)', fname.lower())
        return f"video{match.group(1)}" if match else None
    
    # Group files by video number (1080x1080만 필터링)
    video_groups = {}
    
    for u in uploaded_files:
        fname = getattr(u, "name", None) or u.get("name", "")
        if not fname: continue
        
        video_num = _extract_video_number(fname)
        
        # ✅ 1080x1080만 처리
        if "1080x1080" not in fname.lower():
            continue  # 1080x1080이 아닌 파일은 스킵
        
        if not video_num:
            st.warning(f"⚠️ 파일명 형식 오류: {fname} (video 번호 누락)")
            continue
        
        if video_num not in video_groups:
            video_groups[video_num] = {}
        
        video_groups[video_num]["1080x1080"] = u
    
    # ✅ 1080x1080만 필수로 변경
    valid_groups = {}
    required_ratio = "1080x1080"  # 단일 해상도만 필요
    
    for video_num, files in video_groups.items():
        if required_ratio in files:
            valid_groups[video_num] = {required_ratio: files[required_ratio]}
        else:
            st.error(f"❌ {video_num}: 1080x1080 해상도가 필요합니다")
    
    if not valid_groups:
        raise RuntimeError("❌ 유효한 비디오 그룹이 없습니다. 각 video는 1080x1080 해상도가 필요합니다.")
    
    st.success(f"✅ {len(valid_groups)}개 비디오 검증 완료 (1080x1080만 사용)")
    
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
        base_url = f"https://graph.facebook.com/v24.0/{act}/advideos"
        file_size = os.path.getsize(path)
        
        def _post(data, files=None):
            r = requests.post(base_url, data={**data, "access_token": token}, files=files, timeout=180)
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
        
        return vid_id, None
    
    # ====================================================================
    # STEP 2+3: 그룹별 병렬 처리
    # ====================================================================
    def _process_one_group(video_num: str, group_files: dict) -> dict:
        """한 그룹 처리: 업로드 → 대기 → 광고 생성 (1080x1080만 사용)"""
        import time
        
        try:
            # 1. 비디오 업로드 (1080x1080만)
            if "1080x1080" not in group_files:
                return {"success": False, "error": f"{video_num}: 1080x1080 비디오가 필요합니다"}
            
            f_obj = group_files["1080x1080"]
            fname = getattr(f_obj, "name", None) or f_obj.get("name", "")
            
            # 파일 저장 및 업로드
            file_data = _save_tmp(f_obj)
            vid_id, _ = _upload_video_with_title(file_data["path"], fname)
            
            # 2. 대기
            time.sleep(20)
            
            # 3. 광고 생성 (재시도 포함)
            for attempt in range(3):
                try:
                    # ✅ 텍스트 준비 및 검증
                    # Primary Texts
                    if default_primary_texts:
                        final_primary_texts = [t.strip() for t in default_primary_texts if t.strip()] or [""]
                    else:
                        final_primary_texts = [""]
                    
                    # Headlines
                    if default_headlines:
                        final_headlines = [h.strip() for h in default_headlines if h.strip()] or [video_num]
                    else:
                        final_headlines = [video_num]
                    
                    # CTA
                    final_cta = default_cta if default_cta else "INSTALL_MOBILE_APP"
                    
                    # ✅ Creative Params (단일 비디오, placement 무시)
                    creative_params = {
                        "name": video_num,
                        "object_story_spec": {"page_id": page_id},
                        "asset_feed_spec": {
                            "videos": [
                                {"video_id": vid_id}  # ✅ 1080x1080 하나만
                            ],
                            "bodies": [{"text": text} for text in final_primary_texts],
                            "titles": [{"text": h} for h in final_headlines],
                            "call_to_action_types": [final_cta],
                            "ad_formats": ["AUTOMATIC_FORMAT"],
                            "optimization_type": "PLACEMENT",  # ✅ ASSET_CUSTOMIZATION → PLACEMENT
                            # ✅ asset_customization_rules 제거 - placement별 비디오 지정 불필요
                        }
                    }
                    
                    # Store URL 필수 주입
                    if final_store_url:
                        creative_params["asset_feed_spec"]["link_urls"] = [{
                            "website_url": final_store_url,
                        }]
                    else:
                        return {"success": False, "error": f"{video_num}: ❌ Store URL Missing"}
                    
                    # Create Creative
                    creative = account.create_ad_creative(fields=[], params=creative_params)
                    
                    # Create Ad
                    ad = account.create_ad(fields=[], params={
                        "name": video_num,
                        "adset_id": adset_id,
                        "creative": {"creative_id": creative["id"]},
                        "status": Ad.Status.active
                    })
                    
                    return {
                        "success": True,
                        "result": {
                            "name": video_num,
                            "ad_id": ad["id"],
                            "creative_id": creative["id"],
                            "used_values": {
                                "primary_texts_count": len(final_primary_texts),
                                "headlines_count": len(final_headlines),
                                "cta": final_cta
                            }
                        }
                    }
                    
                except Exception as e:
                    error_str = str(e)
                    
                    # Error 1885252 체크
                    if "1885252" in error_str and attempt < 2:
                        wait_time = 15 * (attempt + 1)
                        logger.info(f"⏳ {video_num}: 비디오 처리 중, {wait_time}초 후 재시도... ({attempt+1}/3)")
                        time.sleep(wait_time)
                        continue
                    
                    raise
            
            return {"success": False, "error": f"{video_num}: 최대 재시도 횟수 초과"}
            
        except Exception as e:
            return {"success": False, "error": f"{video_num}: {str(e)}"}
    
    # ====================================================================
    # 병렬 실행
    # ====================================================================
    results = []
    errors = []
    total = len(valid_groups)
    
    prog = st.progress(0, text=f"🚀 비디오 처리 중... 0/{total}")
    done = 0
    
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(_process_one_group, vn, vf): vn for vn, vf in valid_groups.items()}
        
        for fut in as_completed(futs):
            res = fut.result()
            done += 1
            
            prog.progress(
                int(done / total * 100),
                text=f"🚀 비디오 처리 중... {done}/{total}"
            )
            
            if res["success"]:
                results.append(res["result"])
            else:
                errors.append(res["error"])
    
    prog.empty()
    
    return {
        "ads": results,
        "errors": errors,
        "total_created": len(results),
        "uploads_map": {}
    }