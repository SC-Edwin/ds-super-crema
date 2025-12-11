"""Marketer-side Facebook helpers for Creative Auto-Upload.

Features:
1. Campaign/AdSet Selection
2. Ad Setup (Single Video / Dynamic)
3. Smart "Mimic" Defaults: 
   - Scans Ad Set for the "highest numbered" video ad.
   - Pre-fills Headline, Text, and CTA from that winner.
4. Advantage+ Settings Control
"""
from __future__ import annotations

import streamlit as st
import logging
import re

# Import FB SDK objects
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.exceptions import FacebookRequestError

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
)


logger = logging.getLogger(__name__)

# --- Constants ---
FB_CTA_OPTIONS = [
    "INSTALL_MOBILE_APP", "PLAY_GAME", "USE_APP", "DOWNLOAD", 
    "SHOP_NOW", "LEARN_MORE", "SIGN_UP", "WATCH_MORE", "NO_BUTTON"
]

FB_ADVANTAGE_ENHANCEMENTS = [
    "standard_enhancements",      # Visual touch-ups, brightness/contrast
    "music",                      # Add Music
    "image_template",             # Image Templates (4:5, 9:16 auto-crop)
    "video_cropping",             # Automatic Cropping
    "text_optimizations",         # Text Swapping
    "relevant_comments",          # Highlight Relevant Comments
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
        if c_data.get('asset_feed_spec'):
            afs = c_data['asset_feed_spec']
            bodies = afs.get('bodies', [])
            primary_texts = [b.get('text') for b in bodies if b.get('text')]
            titles = afs.get('titles', [])
            headlines = [t.get('text') for t in titles if t.get('text')]
            
            # Extract URL & CTA from link_urls
            link_urls = afs.get('link_urls', [])
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
            "source_ad_name": target_ad_data['name']
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

        # Primary Text Input
        # (Already working correctly)
        primary_text = st.text_area(
            "Primary Text", 
            value=val_text, 
            height=150, 
            key=f"txt_{idx}",
            help=f"Loaded {len(p_texts)} text options. Separate multiple options with blank lines."
        )

        # Headline Input [UPDATED]
        # Now joins ALL headlines with ' || ' or newlines so you can see them all
        
        val_headline_display = ""
        if h_lines:
            # We use a distinct separator so you can differentiate them easily
            # For Dynamic Creative, usually, these are distinct options.
            val_headline_display = "\n".join(h_lines)

        headline = st.text_area(  # Changed from text_input to text_area
            "Headlines", 
            value=val_headline_display,
            height=100, # Give it some height to show multiple lines
            key=f"head_{idx}",
            help=f"Loaded {len(h_lines)} headlines. Edit them here (one per line)."
        )

        # CTA & Advantage+
        col_cta, col_adv = st.columns([1, 1])
        
        with col_cta:
            call_to_action = st.selectbox(
                "Call to Action", 
                FB_CTA_OPTIONS, 
                index=val_cta_idx,
                key=f"cta_{idx}"
            )

        with col_adv:
            st.markdown("**Advantage+ Settings**")
            use_adv_plus = st.checkbox("Enable Advantage+", value=True, key=f"adv_on_{idx}")
            
            selected_enhancements = []
            if use_adv_plus:
                selected_enhancements = st.multiselect(
                    "Active Enhancements",
                    options=FB_ADVANTAGE_ENHANCEMENTS,
                    default=["standard_enhancements", "image_template", "music"],
                    key=f"adv_opts_{idx}"
                )

        # Final Save
        st.session_state.settings[game] = {
            "campaign_id": sel_c_id,
            "adset_id": sel_a_id,
            "creative_type": "Dynamic Creative",  # 무조건 Dynamic 로직을 타게 함
            "dco_aspect_ratio": dco_aspect_ratio,
            "dco_creative_name": ad_name_input,   # 이름 필드 통일
            "single_creative_name": None,         # 사용 안 함
            "primary_text": primary_text,
            "headline": headline,
            "call_to_action": call_to_action,
            "advantage_plus_enabled": use_adv_plus,
            "advantage_plus_features": selected_enhancements
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