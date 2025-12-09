"""Marketer-side Facebook helpers for Creative 자동 업로드.

Overrides specific UI/logic from facebook_ads.py for the 'Marketer' mode:
1. Simplified Settings UI (Campaign -> Ad Set -> Creative Type).
2. Uses the selected Ad Set ID directly.
3. Auto-optimizes ad set (cleans up low performers).
4. Clones settings (headline/text) from existing ads.
"""
from __future__ import annotations

import streamlit as st
import logging
import time
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import FB SDK objects
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.ad import Ad
from facebook_business.exceptions import FacebookRequestError

# Import everything from the base module
import facebook_ads as fb_ops  # 'from . import' 제거
from facebook_ads import (     # 'from .facebook_ads'에서 점(.) 제거
    FB_GAME_MAPPING,
    GAME_DEFAULTS,
    OPT_GOAL_LABEL_TO_API,
    init_fb_from_secrets,
    validate_page_binding,
    _plan_upload,
    build_targeting_from_settings,
    create_creativetest_adset,
    sanitize_store_url,
    next_sat_0900_kst,
    init_fb_game_defaults,
    make_ad_name,
)

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# 1. Cached Data Fetchers
# -------------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_active_campaigns_cached(account_id: str) -> list[dict]:
    """Fetch ACTIVE campaigns for the given ad account."""
    try:
        account = init_fb_from_secrets(account_id)
        campaigns = account.get_campaigns(
            fields=[Campaign.Field.name, Campaign.Field.id],
            params={"effective_status": ["ACTIVE"], "limit": 100}
        )
        return [{"id": c["id"], "name": c["name"]} for c in campaigns]
    except Exception as e:
        print(f"Error fetching campaigns for {account_id}: {e}")
        return []

@st.cache_data(ttl=300, show_spinner=False)
def fetch_active_adsets_cached(account_id: str, campaign_id: str) -> list[dict]:
    """Fetch ACTIVE adsets for the given campaign."""
    try:
        account = init_fb_from_secrets(account_id)
        campaign = Campaign(campaign_id)
        adsets = campaign.get_ad_sets(
            fields=[AdSet.Field.name, AdSet.Field.id],
            params={"effective_status": ["ACTIVE"], "limit": 100}
        )
        return [{"id": a["id"], "name": a["name"]} for a in adsets]
    except Exception as e:
        print(f"Error fetching adsets for campaign {campaign_id}: {e}")
        return []

# -------------------------------------------------------------------------
# 2. Adset Capacity Management
# -------------------------------------------------------------------------
def _check_and_free_adset_capacity(account: AdAccount, adset_id: str, game: str, idx: int) -> None:
    """
    Check if adset has enough capacity for new creatives.
    If not, automatically delete low-spending creatives.
    """
    from datetime import datetime, timedelta
    
    ADSET_CREATIVE_LIMIT = 50  # Facebook adset creative limit
    
    try:
        # Get current active ads count
        adset = AdSet(adset_id)
        current_ads = adset.get_ads(
            fields=[Ad.Field.id, Ad.Field.created_time],
            params={"effective_status": ["ACTIVE"], "limit": 100}
        )
        current_count = len(current_ads)
        
        # Calculate how many creatives will be uploaded
        remote_list = st.session_state.get("remote_videos", {}).get(game, [])
        creative_type = st.session_state.get("settings", {}).get(game, {}).get("creative_type", "단일 영상")
        
        if creative_type == "단일 영상":
            # Count video groups (each group = 1 creative)
            def _get_base_name(filename: str) -> str:
                return filename.split("_")[0] if "_" in filename else filename.split(".")[0]
            
            def _get_video_size(filename: str) -> str | None:
                if "1080x1080" in filename or "1920x1080" in filename or "1080x1920" in filename:
                    return True
                return None
            
            video_groups = set()
            for item in remote_list:
                name = getattr(item, "name", None) or (item.get("name") if isinstance(item, dict) else None)
                if name and _get_video_size(name):
                    video_groups.add(_get_base_name(name))
            new_creatives_count = len(video_groups)
        else:
            # Dynamic: 1 creative
            new_creatives_count = 1
        
        total_after_upload = current_count + new_creatives_count
        
        # If capacity is sufficient, no action needed
        if total_after_upload <= ADSET_CREATIVE_LIMIT:
            return
        
        # Need to free up space
        needed_space = total_after_upload - ADSET_CREATIVE_LIMIT
        
        # Get all active ads with spending data
        all_ads = adset.get_ads(
            fields=[Ad.Field.id, Ad.Field.created_time, Ad.Field.name],
            params={"effective_status": ["ACTIVE"], "limit": 100}
        )
        
        # Get spending data for each ad (last 14 days)
        now = datetime.now()
        date_14d_ago = (now - timedelta(days=14)).strftime("%Y-%m-%d")
        date_7d_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        
        ads_with_spending = []
        for ad in all_ads:
            ad_id = ad["id"]
            created_time = ad.get("created_time", "")
            
            # Skip ads created in last 7 days
            if created_time:
                try:
                    created_dt = datetime.fromisoformat(created_time.replace("Z", "+00:00"))
                    if (now - created_dt.replace(tzinfo=None)).days < 7:
                        continue
                except:
                    pass
            
            try:
                # Get 14-day spending (use string field name instead of Insights.Field)
                insights = Ad(ad_id).get_insights(
                    fields=["spend"],
                    params={"time_range": {"since": date_14d_ago, "until": "today"}}
                )
                spend_14d = float(insights[0].get("spend", 0)) if insights else 0.0
                
                # Get 7-day spending
                insights_7d = Ad(ad_id).get_insights(
                    fields=["spend"],
                    params={"time_range": {"since": date_7d_ago, "until": "today"}}
                )
                spend_7d = float(insights_7d[0].get("spend", 0)) if insights_7d else 0.0
                
                ads_with_spending.append({
                    "id": ad_id,
                    "name": ad.get("name", ""),
                    "spend_14d": spend_14d,
                    "spend_7d": spend_7d,
                    "created_time": created_time,
                })
            except Exception as e:
                logger.warning(f"Could not get spending for ad {ad_id}: {e}")
                continue
        
        # Step 1: Delete ads with < $1 spending in last 14 days
        deleted_count = 0
        ads_to_delete_14d = [a for a in ads_with_spending if a["spend_14d"] < 1.0]
        ads_to_delete_14d.sort(key=lambda x: x["spend_14d"])  # Delete lowest spending first
        
        for ad_info in ads_to_delete_14d:
            if deleted_count >= needed_space:
                break
            try:
                ad = Ad(ad_info["id"])
                ad.api_update(params={"status": Ad.Status.deleted})
                deleted_count += 1
                logger.info(f"Deleted ad {ad_info['id']} ({ad_info['name']}) - 14d spend: ${ad_info['spend_14d']:.2f}")
            except Exception as e:
                logger.warning(f"Failed to delete ad {ad_info['id']}: {e}")
        
        # Step 2: If still need space, delete ads with < $1 spending in last 7 days
        if deleted_count < needed_space:
            ads_to_delete_7d = [a for a in ads_with_spending if a["spend_7d"] < 1.0 and a["id"] not in [x["id"] for x in ads_to_delete_14d[:deleted_count]]]
            ads_to_delete_7d.sort(key=lambda x: x["spend_7d"])
            
            for ad_info in ads_to_delete_7d:
                if deleted_count >= needed_space:
                    break
                try:
                    ad = Ad(ad_info["id"])
                    ad.api_update(params={"status": Ad.Status.deleted})
                    deleted_count += 1
                    logger.info(f"Deleted ad {ad_info['id']} ({ad_info['name']}) - 7d spend: ${ad_info['spend_7d']:.2f}")
                except Exception as e:
                    logger.warning(f"Failed to delete ad {ad_info['id']}: {e}")
        
        if deleted_count > 0:
            st.info(f"ℹ️ {deleted_count}개의 저성과 광고를 삭제하여 공간을 확보했습니다.")
            # Clear cache to refresh ad count
            st.cache_data.clear()
    except Exception as e:
        logger.error(f"Error in adset capacity check: {e}")
        # Don't raise, just log

# -------------------------------------------------------------------------
# 3. Template Data Fetcher (Clones from existing ads)
# -------------------------------------------------------------------------
@st.cache_data(ttl=0, show_spinner=False)
def fetch_reference_creative_data(_account: AdAccount, adset_id: str) -> dict:
    """
    Fetch ALL headlines/texts/CTA from the MOST RECENT active ad in the adset.
    Returns a dict with 'headline' (list), 'message' (list), 'call_to_action' keys.
    Note: _account has leading underscore to exclude from cache hashing.
    """
    try:
        adset = AdSet(adset_id)
        # Get most recent active ad (sorted by creation time)
        ads = adset.get_ads(
            fields=[Ad.Field.id, Ad.Field.status, Ad.Field.effective_status, Ad.Field.created_time],
            params={"effective_status": ["ACTIVE"], "limit": 10}
        )
        
        if not ads:
            return {}
        
        # Sort by created_time descending to get most recent
        ads_sorted = sorted(ads, key=lambda x: x.get("created_time", ""), reverse=True)
        ad_id = ads_sorted[0]["id"]
        creative_id = Ad(ad_id).api_get(fields=[Ad.Field.creative])["creative"]["id"]
        # Get creative with all fields including asset_feed_spec details
        creative = AdCreative(creative_id).api_get(
            fields=[
                AdCreative.Field.object_story_spec,
                AdCreative.Field.asset_feed_spec,  # For flexible format creatives
                AdCreative.Field.body,
                AdCreative.Field.title,
                AdCreative.Field.link_url,
            ]
        )
        
        # Debug: Log what we got
        logger.info(f"Creative {creative_id} structure: has object_story_spec={bool(creative.get('object_story_spec'))}, has asset_feed_spec={bool(creative.get('asset_feed_spec'))}")
        
        # Check both object_story_spec and asset_feed_spec
        spec = creative.get("object_story_spec", {})
        asset_feed = creative.get("asset_feed_spec", {})
        video_data = spec.get("video_data", {})
        link_data = spec.get("link_data", {})
        
        # Debug: Log structure
        logger.info(f"Creative structure - asset_feed keys: {list(asset_feed.keys()) if asset_feed else 'None'}")
        if asset_feed:
            logger.info(f"asset_feed.get('titles'): {asset_feed.get('titles')}")
            logger.info(f"asset_feed.get('bodies'): {asset_feed.get('bodies')}")
            logger.info(f"asset_feed.get('headlines'): {asset_feed.get('headlines')}")
            logger.info(f"asset_feed.get('messages'): {asset_feed.get('messages')}")
        
        # Get ALL headlines (not just first one)
        # PRIORITY: asset_feed_spec first (this is where multiple headlines come from in Dynamic/Flexible creatives)
        # Note: Facebook API uses 'titles' in asset_feed_spec, not 'headlines'
        headlines = []
        if asset_feed.get("titles"):  # Facebook uses 'titles' in asset_feed_spec
            titles_raw = asset_feed["titles"]
            if isinstance(titles_raw, list):
                headlines.extend([str(h) for h in titles_raw if h])
            else:
                headlines.append(str(titles_raw))
        elif asset_feed.get("headlines"):  # Fallback to 'headlines' if exists
            headlines_raw = asset_feed["headlines"]
            if isinstance(headlines_raw, list):
                headlines.extend([str(h) for h in headlines_raw if h])
            else:
                headlines.append(str(headlines_raw))
        
        # Fallback: From object_story_spec (if asset_feed_spec doesn't have headlines)
        if not headlines:
            if video_data.get("title"):
                if isinstance(video_data["title"], list):
                    headlines.extend([str(h) for h in video_data["title"] if h])
                else:
                    headlines.append(str(video_data["title"]))
            if link_data.get("name"):
                if isinstance(link_data["name"], list):
                    headlines.extend([str(h) for h in link_data["name"] if h])
                else:
                    headlines.append(str(link_data["name"]))
            # From creative title (last fallback)
            if creative.get("title") and not headlines:
                if isinstance(creative["title"], list):
                    headlines.extend([str(h) for h in creative["title"] if h])
                else:
                    headlines.append(str(creative["title"]))
        
        # Get ALL messages (primary text)
        # PRIORITY: asset_feed_spec first (this is where multiple messages come from in Dynamic/Flexible creatives)
        # Note: Facebook API uses 'bodies' in asset_feed_spec, not 'messages'
        messages = []
        if asset_feed.get("bodies"):  # Facebook uses 'bodies' in asset_feed_spec
            bodies_raw = asset_feed["bodies"]
            if isinstance(bodies_raw, list):
                messages.extend([str(m) for m in bodies_raw if m])
            else:
                messages.append(str(bodies_raw))
        elif asset_feed.get("messages"):  # Fallback to 'messages' if exists
            messages_raw = asset_feed["messages"]
            if isinstance(messages_raw, list):
                messages.extend([str(m) for m in messages_raw if m])
            else:
                messages.append(str(messages_raw))
        
        # Fallback: From object_story_spec (if asset_feed_spec doesn't have messages)
        if not messages:
            if video_data.get("message"):
                if isinstance(video_data["message"], list):
                    messages.extend([str(m) for m in video_data["message"] if m])
                else:
                    messages.append(str(video_data["message"]))
            if link_data.get("message"):
                if isinstance(link_data["message"], list):
                    messages.extend([str(m) for m in link_data["message"] if m])
                else:
                    messages.append(str(link_data["message"]))
            # From creative body (last fallback)
            if creative.get("body") and not messages:
                if isinstance(creative["body"], list):
                    messages.extend([str(m) for m in creative["body"] if m])
                else:
                    messages.append(str(creative["body"]))
        
        # Get CTA from both sources
        cta = video_data.get("call_to_action") or link_data.get("call_to_action") or asset_feed.get("call_to_action")
        
        result = {}
        # Store ALL headlines (remove duplicates while preserving order)
        if headlines:
            seen = set()
            unique_headlines = []
            for h in headlines:
                h_str = str(h).strip()
                if h_str and h_str not in seen:
                    seen.add(h_str)
                    unique_headlines.append(h_str)
            result["headline"] = unique_headlines
            logger.info(f"Found {len(unique_headlines)} unique headlines: {unique_headlines}")
        else:
            logger.warning(f"No headlines found in creative {creative_id}")
        
        # Store ALL messages (remove duplicates while preserving order)
        if messages:
            seen = set()
            unique_messages = []
            for m in messages:
                m_str = str(m).strip()
                if m_str and m_str not in seen:
                    seen.add(m_str)
                    unique_messages.append(m_str)
            result["message"] = unique_messages
            logger.info(f"Found {len(unique_messages)} unique messages: {unique_messages}")
        else:
            logger.warning(f"No messages found in creative {creative_id}")
        
        if cta:
            # Convert CTA to a fully serializable dict
            if isinstance(cta, dict):
                # Deep copy and convert all values to basic types
                cta_serializable = {}
                for k, v in cta.items():
                    k_str = str(k)
                    if isinstance(v, dict):
                        # Nested dict - convert all values to strings
                        cta_serializable[k_str] = {str(k2): str(v2) if v2 is not None else None for k2, v2 in v.items()}
                    elif isinstance(v, (list, tuple)):
                        # List - convert to list of strings
                        cta_serializable[k_str] = [str(item) for item in v]
                    else:
                        # Primitive type - convert to string
                        cta_serializable[k_str] = str(v) if v is not None else None
                result["call_to_action"] = cta_serializable
            else:
                result["call_to_action"] = str(cta)
            
        return result
    except Exception as e:
        logger.warning(f"Could not fetch reference creative data: {e}")
        return {}

# -------------------------------------------------------------------------
# 3. Cleanup Logic (Optimization)
# -------------------------------------------------------------------------
def cleanup_low_performing_ads(
    account: AdAccount,
    adset_id: str,
    new_files_count: int,
    *,
    min_spend_threshold_usd: float = 10.0,
    min_impressions: int = 1000,
) -> None:
    """
    Before uploading new ads, pause low-performing ads in the adset.
    Only pauses ads that have spent >= threshold and have low performance.
    """
    try:
        adset = AdSet(adset_id)
        ads = adset.get_ads(
            fields=[
                Ad.Field.id,
                Ad.Field.name,
                Ad.Field.status,
                Ad.Field.effective_status,
            ],
            params={"effective_status": ["ACTIVE"], "limit": 100}
        )
        
        if not ads:
            return
        
        # Get insights for performance check
        from facebook_business.adobjects.adsinsights import AdsInsights
        
        insights = account.get_insights(
            fields=[
                AdsInsights.Field.ad_id,
                AdsInsights.Field.spend,
                AdsInsights.Field.impressions,
                AdsInsights.Field.ctr,
            ],
            params={
                "level": "ad",
                "time_range": {"since": (time.time() - 7 * 24 * 3600), "until": time.time()},
                "filtering": [{"field": "ad.id", "operator": "IN", "value": [a["id"] for a in ads]}],
            }
        )
        
        insights_by_ad = {ins["ad_id"]: ins for ins in insights}
        
        to_pause = []
        for ad in ads:
            ad_id = ad["id"]
            ins = insights_by_ad.get(ad_id, {})
            spend = float(ins.get("spend", 0))
            impressions = int(ins.get("impressions", 0))
            
            if spend >= min_spend_threshold_usd and impressions >= min_impressions:
                ctr = float(ins.get("ctr", 0))
                if ctr < 0.5:  # Low CTR threshold
                    to_pause.append(ad_id)
        
        if to_pause and len(ads) - len(to_pause) >= new_files_count:
            for ad_id in to_pause:
                try:
                    Ad(ad_id).api_update(params={"status": Ad.Status.paused})
                    logger.info(f"Paused low-performing ad: {ad_id}")
                except Exception as e:
                    logger.warning(f"Failed to pause ad {ad_id}: {e}")
    except Exception as e:
        logger.warning(f"Cleanup check failed: {e}")

# -------------------------------------------------------------------------
# 4. Settings Panel UI
# -------------------------------------------------------------------------
def render_facebook_settings_panel(container, game: str, idx: int) -> None:
    """Render simplified Facebook settings for Marketer mode."""
    from modules.upload_automation import game_manager

    with container:
        st.markdown(f"#### {game} Facebook Settings")

        cfg = FB_GAME_MAPPING.get(game)
        if not cfg:
            st.error(f"No Facebook configuration found for {game}")
            return

        account_id = cfg["account_id"]
        campaigns = fetch_active_campaigns_cached(account_id)
        
        if not campaigns:
            st.warning("No active campaigns found.")
            return
        
        campaign_options = [f"{c['name']} ({c['id']})" for c in campaigns]
        campaign_ids = [c["id"] for c in campaigns]
        
        campaign_key = f"fb_campaign_{idx}"
        prev_campaign = st.session_state.get(campaign_key, "")
        default_campaign_idx = 0
        if prev_campaign:
            try:
                default_campaign_idx = campaign_ids.index(prev_campaign)
            except ValueError:
                pass
        
        selected_campaign_label = st.selectbox(
            "캠페인 선택",
            options=campaign_options,
            index=default_campaign_idx,
            key=f"fb_campaign_select_{idx}",
        )
        selected_campaign_id = campaign_ids[campaign_options.index(selected_campaign_label)]
        st.session_state[campaign_key] = selected_campaign_id
        
        adsets = fetch_active_adsets_cached(account_id, selected_campaign_id)
        if not adsets:
            st.warning("No active ad sets found in this campaign.")
            return
        
        adset_options = [f"{a['name']} ({a['id']})" for a in adsets]
        adset_ids = [a["id"] for a in adsets]
        
        adset_key = f"fb_adset_{idx}"
        prev_adset = st.session_state.get(adset_key, "")
        default_adset_idx = 0
        if prev_adset:
            try:
                default_adset_idx = adset_ids.index(prev_adset)
            except ValueError:
                pass
        
        selected_adset_label = st.selectbox(
            "광고 세트 선택",
            options=adset_options,
            index=default_adset_idx,
            key=f"fb_adset_select_{idx}",
        )
        selected_adset_id = adset_ids[adset_options.index(selected_adset_label)]
        st.session_state[adset_key] = selected_adset_id
        
        # Check adset capacity and auto-delete if needed
        if selected_adset_id:
            try:
                account = init_fb_from_secrets(cfg["account_id"])
                _check_and_free_adset_capacity(account, selected_adset_id, game, idx)
            except Exception as e:
                logger.warning(f"Failed to check adset capacity: {e}")
                # Don't block UI if check fails
        
        creative_type = st.radio(
            "Creative 타입",
            options=["단일 영상", "다이나믹"],
            index=0,
            key=f"fb_creative_type_{idx}",
        )
        
        store_url = st.text_input(
            "Store URL (Optional)",
            value=st.session_state.get(f"fb_store_url_{idx}", ""),
            key=f"fb_store_url_input_{idx}",
            placeholder="https://play.google.com/store/apps/details?id=...",
        )
        
        if creative_type == "다이나믹":
            dco_aspect_ratio = st.selectbox(
                "Aspect Ratio",
                options=["1:1", "4:5", "9:16", "16:9"],
                index=0,
                key=f"fb_dco_ratio_{idx}",
            )
            dco_creative_name = st.text_input(
                "Creative Name (Optional)",
                value=st.session_state.get(f"fb_dco_name_{idx}", ""),
                key=f"fb_dco_name_input_{idx}",
            )
        else:
            dco_aspect_ratio = None
            dco_creative_name = None
        
        st.session_state.settings[game] = {
            "campaign_id": selected_campaign_id,
            "adset_id": selected_adset_id,
            "creative_type": creative_type,
            "store_url": store_url,
            "dco_aspect_ratio": dco_aspect_ratio,
            "dco_creative_name": dco_creative_name,
        }

# -------------------------------------------------------------------------
# 5. Dry Run / Preview Function
# -------------------------------------------------------------------------
def preview_facebook_upload(
    game_name: str,
    uploaded_files: list,
    settings: dict,
) -> dict:
    """
    Preview what would happen if upload is executed.
    Returns a dict with preview information without actually uploading.
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
    
    target_campaign_id = settings.get("campaign_id")
    target_adset_id = settings.get("adset_id")
    creative_type = settings.get("creative_type", "단일 영상")
    
    if not target_campaign_id:
        raise RuntimeError("캠페인이 선택되지 않았습니다.")
    if not target_adset_id:
        raise RuntimeError("광고 세트가 선택되지 않았습니다.")
    
    # Fetch template data (same as real upload)
    template_data = fetch_reference_creative_data(account, target_adset_id)
    headlines_found = template_data.get("headline") or []
    messages_found = template_data.get("message") or []
    cta_found = template_data.get("call_to_action")
    
    single_headline = headlines_found[0] if headlines_found else "New Game"
    single_message = messages_found[0] if messages_found else ""
    
    store_url = (settings.get("store_url") or "").strip()
    ad_name_prefix = (
        settings.get("ad_name_prefix") if settings.get("ad_name_mode") == "Prefix + filename" else None
    )
    
    # Get video file names
    def _fname_any(u):
        return getattr(u, "name", None) or (u.get("name") if isinstance(u, dict) else None)
    
    video_names = [_fname_any(u) for u in uploaded_files if _fname_any(u)]
    
    # Preview creatives that would be created
    preview_creatives = []
    if creative_type == "다이나믹":
        # Single creative with multiple videos
        creative_name = settings.get("dco_creative_name") or f"Flexible_{len(video_names)}vids_{video_names[0] if video_names else 'Creative'}"
        preview_creatives.append({
            "name": creative_name,
            "type": "Dynamic (Flexible Format)",
            "videos": video_names,
            "headline": headlines_found,  # All headlines
            "message": messages_found,     # All messages
            "cta": cta_found,
            "aspect_ratio": settings.get("dco_aspect_ratio"),
        })
    else:
        # 단일 영상 모드: 3가지 사이즈 검증 및 그룹화
        def _get_base_name(filename: str) -> str:
            return filename.split("_")[0] if "_" in filename else filename.split(".")[0]
        
        def _get_video_size(filename: str) -> str | None:
            if "1080x1080" in filename:
                return "1080x1080"
            elif "1920x1080" in filename:
                return "1920x1080"
            elif "1080x1920" in filename:
                return "1080x1920"
            return None
        
        # Group by base name
        video_groups: dict[str, dict[str, str]] = {}
        for name in video_names:
            base_name = _get_base_name(name)
            size = _get_video_size(name)
            if not size:
                continue
            if base_name not in video_groups:
                video_groups[base_name] = {}
            video_groups[base_name][size] = name
        
        # Validate: Each group must have all 3 sizes
        required_sizes = {"1080x1080", "1920x1080", "1080x1920"}
        errors = []
        valid_groups = {}
        for base_name, sizes in video_groups.items():
            missing = required_sizes - set(sizes.keys())
            if missing:
                errors.append(f"{base_name}의 사이즈를 확인하세요. 누락된 사이즈: {', '.join(missing)}")
            else:
                valid_groups[base_name] = sizes
        
        if errors:
            return {
                "error": "\n".join(errors),
                "preview_creatives": [],
                "current_ad_count": 0,
            }
        
        # One creative per group (with 3 videos)
        for base_name, sizes in valid_groups.items():
            ad_name = make_ad_name(base_name, ad_name_prefix)
            preview_creatives.append({
                "name": base_name,
                "ad_name": ad_name,
                "type": "Single Video (3 sizes)",
                "videos": {
                    "1080x1080": sizes["1080x1080"],
                    "1920x1080": sizes["1920x1080"],
                    "1080x1920": sizes["1080x1920"],
                },
                "placements": {
                    "1080x1080": ["Feed", "In-stream ads for Reels"],
                    "1080x1920": ["Stories", "Status", "Reels", "Search results", "Apps and sites"],
                    "1920x1080": ["Facebook search results"],
                },
                "placements_kr": {
                    "1080x1080": ["피드", "릴스 익스트림 광고"],
                    "1080x1920": ["스토리", "상태", "릴스", "검색결과", "앱및사이트"],
                    "1920x1080": ["Facebook 검색 결과"],
                },
                "headline": headlines_found,  # All headlines
                "message": messages_found,     # All messages
                "cta": cta_found,
            })
    
    # Get current ad count in adset and capacity info
    ADSET_CREATIVE_LIMIT = 50
    current_ad_count = 0
    capacity_info = {
        "current_count": 0,
        "limit": ADSET_CREATIVE_LIMIT,
        "available_slots": 0,
        "new_creatives_count": len(preview_creatives),
        "will_exceed": False,
        "ads_to_delete": []
    }
    
    try:
        from datetime import datetime, timedelta
        
        adset = AdSet(target_adset_id)
        current_ads = adset.get_ads(
            fields=[Ad.Field.id, Ad.Field.created_time, Ad.Field.name],
            params={"effective_status": ["ACTIVE"], "limit": 100}
        )
        current_ad_count = len(current_ads)
        capacity_info["current_count"] = current_ad_count
        capacity_info["available_slots"] = ADSET_CREATIVE_LIMIT - current_ad_count
        
        total_after_upload = current_ad_count + len(preview_creatives)
        capacity_info["will_exceed"] = total_after_upload > ADSET_CREATIVE_LIMIT
        
        # If capacity will be exceeded, calculate which ads would be deleted
        if capacity_info["will_exceed"]:
            needed_space = total_after_upload - ADSET_CREATIVE_LIMIT
            
            # Get spending data for ads
            now = datetime.now()
            date_14d_ago = (now - timedelta(days=14)).strftime("%Y-%m-%d")
            date_7d_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
            
            ads_with_spending = []
            for ad in current_ads:
                ad_id = ad["id"]
                created_time = ad.get("created_time", "")
                
                # Skip ads created in last 7 days
                if created_time:
                    try:
                        created_dt = datetime.fromisoformat(created_time.replace("Z", "+00:00"))
                        if (now - created_dt.replace(tzinfo=None)).days < 7:
                            continue
                    except:
                        pass
                
                try:
                    # Get 14-day spending
                    insights = Ad(ad_id).get_insights(
                        fields=["spend"],
                        params={"time_range": {"since": date_14d_ago, "until": "today"}}
                    )
                    spend_14d = float(insights[0].get("spend", 0)) if insights else 0.0
                    
                    # Get 7-day spending
                    insights_7d = Ad(ad_id).get_insights(
                        fields=["spend"],
                        params={"time_range": {"since": date_7d_ago, "until": "today"}}
                    )
                    spend_7d = float(insights_7d[0].get("spend", 0)) if insights_7d else 0.0
                    
                    ads_with_spending.append({
                        "id": ad_id,
                        "name": ad.get("name", ""),
                        "spend_14d": spend_14d,
                        "spend_7d": spend_7d,
                        "created_time": created_time,
                    })
                except Exception as e:
                    logger.warning(f"Could not get spending for ad {ad_id}: {e}")
                    continue
            
            # Step 1: Ads with < $1 spending in last 14 days
            ads_to_delete_14d = [a for a in ads_with_spending if a["spend_14d"] < 1.0]
            ads_to_delete_14d.sort(key=lambda x: x["spend_14d"])
            
            # Step 2: Ads with < $1 spending in last 7 days (if still need space)
            ads_to_delete_7d = [a for a in ads_with_spending if a["spend_7d"] < 1.0 and a["id"] not in [x["id"] for x in ads_to_delete_14d]]
            ads_to_delete_7d.sort(key=lambda x: x["spend_7d"])
            
            # Combine and limit to needed space
            all_ads_to_delete = ads_to_delete_14d + ads_to_delete_7d
            capacity_info["ads_to_delete"] = all_ads_to_delete[:needed_space]
            
    except Exception as e:
        logger.warning(f"Could not fetch capacity info: {e}")
    
    return {
        "campaign_id": target_campaign_id,
        "adset_id": target_adset_id,
        "page_id": str(page_id),
        "creative_type": creative_type,
        "n_videos": len(video_names),
        "current_ad_count": current_ad_count,
        "preview_creatives": preview_creatives,
        "capacity_info": capacity_info,
        "template_source": {
            "headlines_found": len(headlines_found),
            "messages_found": len(messages_found),
            "headline_example": single_headline,
            "message_example": single_message[:50] + "..." if len(single_message) > 50 else single_message,
            "cta": cta_found,
        },
        "store_url": store_url,
    }

    # -------------------------------------------------------------------------
# 5. Specialized Upload Function (Clones Settings + PAC Support)
    # -------------------------------------------------------------------------
def upload_videos_create_ads_cloned(
    account: AdAccount,
    *,
    page_id: str,
    adset_id: str,
    uploaded_files: list,
    ad_name_prefix: str | None = None,
    store_url: str | None = None,
    try_instagram: bool = True,
    template_data: dict | None = None,
    use_flexible_format: bool = False,
    target_aspect_ratio: str | None = None,
    creative_name_manual: str | None = None,
):
    """Upload videos and create ads, cloning settings from existing ads."""
    from facebook_business.adobjects.advideo import AdVideo
    from facebook_business.adobjects.page import Page
    import pathlib
    
    allowed = {".mp4", ".mpeg4"}
    def _is_video(u):
        n = fb_ops._fname_any(u) or "video.mp4"
        return pathlib.Path(n).suffix.lower() in allowed

    videos = fb_ops._dedupe_by_name([u for u in (uploaded_files or []) if _is_video(u)])
    
    if not videos:
        st.warning("No video files to upload.")
        return []
        
    # Extract template data - use ALL headlines and messages
    template = template_data or {}
    headlines_list = template.get("headline") or ["New Game"]
    messages_list = template.get("message") or []
    orig_cta = template.get("call_to_action")
    
    # Ensure lists (not just first one)
    if not isinstance(headlines_list, list):
        headlines_list = [headlines_list] if headlines_list else ["New Game"]
    if not isinstance(messages_list, list):
        messages_list = [messages_list] if messages_list else []
    
    target_link = store_url
    
    # -------------------------------------------------------------------------
    # BRANCH A: FLEXIBLE FORMAT (Dynamic Creative)
    # -------------------------------------------------------------------------
    if use_flexible_format:
        # 1. Persist to temp
        persisted = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {}
            for u in videos:
                f = ex.submit(fb_ops._save_uploadedfile_tmp, u)
                futs[f] = fb_ops._fname_any(u)
        
        for fut, nm in futs.items():
            try:
                p = fut.result()
                persisted.append({"name": nm, "path": p})
            except Exception as e:
                st.error(f"File prep failed {nm}: {e}")

    # 2. Upload Videos
    uploads = []
    total = len(persisted)
    progress = st.progress(0, text="Uploading videos (Marketer Mode)...")
    
    for i, item in enumerate(persisted):
        try:
            v = account.create_ad_video(params={
                "file": item["path"], 
                "content_category": "VIDEO_GAMING"
            })
            uploads.append({"name": item["name"], "video_id": v["id"]})
        except Exception as e:
            st.error(f"Upload failed for {item['name']}: {e}")
            progress.progress((i + 1) / total)
        
        progress.empty()
        
        # Wait for all videos to have thumbnails (improved)
        if uploads:
            from facebook_business.adobjects.advideo import AdVideo
            import time
            logger.info(f"Waiting for thumbnails for {len(uploads)} videos...")
            
            video_ids = [u["video_id"] for u in uploads]
            ready = {vid: False for vid in video_ids}
            deadline = time.time() + 600  # 10 minutes timeout
            start_time = time.time()
            
            while time.time() < deadline:
                all_done = True
                for vid in video_ids:
                    if ready[vid]:
                        continue
                    try:
                        info = AdVideo(vid).api_get(fields=["status", "thumbnails", "picture", "processing_progress"])
                        status = info.get("status")
                        has_pic = bool(info.get("picture"))
                        has_thumbs = bool(info.get("thumbnails"))
                        progress = info.get("processing_progress", 0)
                        
                        if (has_pic or has_thumbs) and status in ("READY", "PUBLISHED"):
                            ready[vid] = True
                        elif status == "PROCESSING" and progress < 100:
                            all_done = False
                        else:
                            all_done = False
                    except Exception as e:
                        all_done = False
                        logger.warning(f"Error checking video {vid}: {e}")
                
                if all_done:
                    elapsed = time.time() - start_time
                    logger.info(f"All {len(video_ids)} videos have thumbnails after {elapsed:.1f}s")
                    break
                
                time.sleep(5)
            
            not_ready = [vid for vid, is_ready in ready.items() if not is_ready]
            if not_ready:
                elapsed = time.time() - start_time
                logger.warning(f"Timeout after {elapsed:.1f}s: {len(not_ready)} videos still don't have thumbnails")
                st.warning(f"⚠️ {len(not_ready)} video(s) still processing thumbnails. Some creatives may have gray thumbnails.")

        # 3. Create ONE Flexible Format Creative
        results = []
        api_errors = []
        
        ig_actor_id = None
        try:
            from facebook_business.adobjects.page import Page
            p = Page(page_id).api_get(fields=["instagram_business_account"])
            ig_actor_id = p.get("instagram_business_account", {}).get("id")
        except: pass

        try:
            # Build asset_feed_spec with ALL headlines and messages
            video_assets = [{"video_id": u["video_id"]} for u in uploads]
            
            asset_feed_spec = {
                "video_assets": video_assets,
                "headlines": headlines_list,  # Use ALL headlines
                "messages": messages_list,     # Use ALL messages
            }
            
            if orig_cta:
                asset_feed_spec["call_to_action"] = orig_cta
            
            if target_aspect_ratio:
                # Map ratio string to API value
                ratio_map = {
                    "1:1": "1:1",
                    "4:5": "4:5",
                    "9:16": "9:16",
                    "16:9": "16:9",
                }
                asset_feed_spec["aspect_ratio"] = ratio_map.get(target_aspect_ratio, "1:1")
            
            # Basic Page Spec
            object_story_spec = {
                "page_id": page_id,
            }
            if try_instagram and ig_actor_id:
                object_story_spec["instagram_actor_id"] = ig_actor_id

            # Create ONE Creative
            # Create ONE Creative
            # Use manual name if provided, else auto-generate
            if creative_name_manual:
                creative_name = creative_name_manual
            else:
                base_name = uploads[0]["name"]
                creative_name = f"Flexible_{len(uploads)}vids_{base_name}"
            
            # FIX: asset_feed_spec is a SIBLING of object_story_spec
            creative = account.create_ad_creative(params={
                "name": creative_name,
                "object_story_spec": object_story_spec,
                "asset_feed_spec": asset_feed_spec
            })
            
            # Create ONE Ad
            ad_name = make_ad_name(f"Flexible_{len(uploads)}Items", ad_name_prefix)
            account.create_ad(params={
                "name": ad_name,
                "adset_id": adset_id,
                "creative": {"creative_id": creative["id"]},
                "status": Ad.Status.active,
            })
            
            results.append({"name": creative_name, "creative_id": creative["id"]})
        except Exception as e:
            api_errors.append(str(e))

        if api_errors:
            st.error("Some ads failed to create:\n" + "\n".join(api_errors))
        
        return results

    # -------------------------------------------------------------------------
    # BRANCH B: SINGLE FORMAT (Standard) - 1 Creative per Video Group (3 sizes)
    # -------------------------------------------------------------------------
    else:
        def _get_base_name(filename: str) -> str:
            """Extract base name from filename (e.g., 'video462_1080x1080.mp4' -> 'video462')"""
            return filename.split("_")[0] if "_" in filename else filename.split(".")[0]
        
        def _get_video_size(filename: str) -> str | None:
            """Extract size from filename. Returns '1080x1080', '1920x1080', '1080x1920', or None"""
            if "1080x1080" in filename:
                return "1080x1080"
            elif "1920x1080" in filename:
                return "1920x1080"
            elif "1080x1920" in filename:
                return "1080x1920"
            return None
        
        def _create_creative_with_3_videos(base_name: str, videos_by_size: dict[str, dict]) -> dict:
            """Create 1 creative with 3 videos for different placements"""
            try:
                from facebook_business.adobjects.advideo import AdVideo
                
                # Get video IDs and thumbnails
                video_1x1 = videos_by_size["1080x1080"]
                video_9x16 = videos_by_size["1080x1920"]
                video_16x9 = videos_by_size["1920x1080"]
                
                # Get thumbnails for all videos (with retries like test mode)
                def _get_thumbnail(video_id: str, name: str) -> str:
                    max_retries = 3
                    thumbnail_url = None
                    
                    for attempt in range(max_retries):
                        try:
                            vinfo = AdVideo(video_id).api_get(fields=["status", "thumbnails", "picture", "processing_progress"])
                            status = vinfo.get("status")
                            thumbnail_url = vinfo.get("picture")
                            
                            # If no picture, try to get from thumbnails
                            if not thumbnail_url:
                                thumbnails = vinfo.get("thumbnails")
                                if thumbnails and isinstance(thumbnails, list) and len(thumbnails) > 0:
                                    thumbnail_url = thumbnails[0].get("uri") if isinstance(thumbnails[0], dict) else None
                            
                            # Check if video is ready (has thumbnail and status is READY or PUBLISHED)
                            if thumbnail_url and status in ("READY", "PUBLISHED"):
                                break
                            
                            # If still processing and not last attempt, wait and retry
                            if attempt < max_retries - 1:
                                wait_time = 30 * (attempt + 1)  # 30s, 60s, 90s
                                logger.info(f"Video {video_id} ({name}) thumbnail not ready (status: {status}), waiting {wait_time}s (attempt {attempt + 1}/{max_retries})...")
                                time.sleep(wait_time)
                                
                        except Exception as e:
                            if attempt < max_retries - 1:
                                wait_time = 30 * (attempt + 1)
                                logger.warning(f"Error getting thumbnail for {video_id} ({name}): {e}, retrying in {wait_time}s...")
                                time.sleep(wait_time)
                            else:
                                raise
                    
                    if not thumbnail_url:
                        raise RuntimeError(f"Video {video_id} ({name}) has no thumbnail after {max_retries} attempts")
                    
                    return thumbnail_url
                
                thumb_1x1 = _get_thumbnail(video_1x1["video_id"], video_1x1["name"])
                thumb_9x16 = _get_thumbnail(video_9x16["video_id"], video_9x16["name"])
                thumb_16x9 = _get_thumbnail(video_16x9["video_id"], video_16x9["name"])
                
                # Prepare CTA
                final_cta = None
                if orig_cta:
                     final_cta = orig_cta.copy()
                     if target_link and "value" in final_cta:
                         final_cta["value"]["link"] = target_link
                elif target_link:
                     final_cta = {"type": "INSTALL_MOBILE_APP", "value": {"link": target_link}}

                # Create asset_feed_spec with 3 videos for different placements
                asset_feed_spec = {
                    "video_assets": [
                        {
                            "video_id": video_1x1["video_id"],
                            "image_url": thumb_1x1,
                            "placements": ["feed", "reels_extreme_ads"]
                        },
                        {
                            "video_id": video_9x16["video_id"],
                            "image_url": thumb_9x16,
                            "placements": ["story", "status", "reels", "search_results", "apps_and_sites"]
                        },
                        {
                            "video_id": video_16x9["video_id"],
                            "image_url": thumb_16x9,
                            "placements": ["facebook_search_results"]
                        }
                    ],
                    "headlines": headlines_list,  # All headlines
                    "messages": messages_list,     # All messages (primary text)
                    "call_to_action": final_cta 
                }
                
                # Create object_story_spec
                object_story_spec = {"page_id": page_id}
                if try_instagram and ig_actor_id:
                    object_story_spec["instagram_actor_id"] = ig_actor_id
                    
                # Create creative
                creative = account.create_ad_creative(params={
                    "name": base_name,
                    "object_story_spec": object_story_spec,
                    "asset_feed_spec": asset_feed_spec
                })
                
                # Create ad
                ad = account.create_ad(params={
                    "name": make_ad_name(base_name, ad_name_prefix),
                    "adset_id": adset_id,
                    "creative": {"creative_id": creative["id"]},
                    "status": Ad.Status.active,
                })
                
                return {"name": base_name, "ad_id": ad["id"], "creative_id": creative["id"]}
            except Exception as e:
                return {"name": base_name, "error": str(e)}

    # 1. Persist to temp
    persisted = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {}
        for u in videos:
            f = ex.submit(fb_ops._save_uploadedfile_tmp, u)
            futs[f] = fb_ops._fname_any(u)
        
        for fut, nm in futs.items():
            try:
                p = fut.result()
                persisted.append({"name": nm, "path": p})
            except Exception as e:
                st.error(f"File prep failed {nm}: {e}")

        # 2. Upload Videos (with size validation before upload)
        # 1.5. Validate video sizes BEFORE uploading
        video_groups_pre: dict[str, dict[str, dict]] = {}
        for item in persisted:
            base_name = _get_base_name(item["name"])
            size = _get_video_size(item["name"])
            if not size:
                continue
            if base_name not in video_groups_pre:
                video_groups_pre[base_name] = {}
            video_groups_pre[base_name][size] = item
        
        # Validate: Each group must have all 3 sizes
        required_sizes = {"1080x1080", "1920x1080", "1080x1920"}
        errors = []
        valid_groups_pre = {}
        for base_name, sizes in video_groups_pre.items():
            missing = required_sizes - set(sizes.keys())
            if missing:
                errors.append(f"{base_name}의 사이즈를 확인하세요. 누락된 사이즈: {', '.join(missing)}")
            else:
                valid_groups_pre[base_name] = sizes
        
        if errors:
            st.error("\n".join(errors))
            return []
        
        # Upload all videos
        uploads = []
        total = sum(len(group) for group in valid_groups_pre.values())
        progress = st.progress(0, text="Uploading videos (Marketer Mode)...")
        uploaded_count = 0
        
        for base_name, group in valid_groups_pre.items():
            for size, item in group.items():
                try:
                    v = account.create_ad_video(params={
                        "file": item["path"], 
                        "content_category": "VIDEO_GAMING"
                    })
                    uploads.append({"name": item["name"], "video_id": v["id"], "base_name": base_name, "size": size})
                    uploaded_count += 1
                    progress.progress(uploaded_count / total)
                except Exception as e:
                    st.error(f"Upload failed for {item['name']}: {e}")
        
        progress.empty()
        
        # Wait for all videos to have thumbnails (same as test mode)
        if uploads:
            from facebook_business.adobjects.advideo import AdVideo
            import time
            logger.info(f"Waiting for thumbnails for {len(uploads)} videos...")
            
            video_ids = [u["video_id"] for u in uploads]
            ready = {vid: False for vid in video_ids}
            deadline = time.time() + 600  # 10 minutes timeout (same as test mode)
            start_time = time.time()
            sleep_s = 5  # Same as test mode
            
            while time.time() < deadline:
                all_done = True
                for vid in video_ids:
                    if ready[vid]:
                        continue
                    try:
                        # Check video status and thumbnails (same fields as test mode)
                        info = AdVideo(vid).api_get(fields=["status", "thumbnails", "picture", "processing_progress"])
                        status = info.get("status")
                        has_pic = bool(info.get("picture"))
                        has_thumbs = bool(info.get("thumbnails"))
                        progress = info.get("processing_progress", 0)
                        
                        # Video is ready if it has picture or thumbnails, and status is READY or PUBLISHED (same logic as test mode)
                        if (has_pic or has_thumbs) and status in ("READY", "PUBLISHED"):
                            ready[vid] = True
                        elif status == "PROCESSING" and progress < 100:
                            # Still processing, wait more
                            all_done = False
                        else:
                            # No thumbnail yet, keep waiting
                            all_done = False
                    except Exception as e:
                        # If we can't get info, assume not ready yet
                        all_done = False
                        logger.warning(f"Error checking video {vid}: {e}")
                
                if all_done:
                    elapsed = time.time() - start_time
                    logger.info(f"All {len(video_ids)} videos have thumbnails after {elapsed:.1f}s")
                    break
                
                time.sleep(sleep_s)
            
            # Log which videos are ready/not ready (same as test mode)
            not_ready = [vid for vid, is_ready in ready.items() if not is_ready]
            if not_ready:
                elapsed = time.time() - start_time
                logger.warning(f"Timeout after {elapsed:.1f}s: {len(not_ready)} videos still don't have thumbnails: {not_ready}")
                st.warning(f"⚠️ {len(not_ready)} video(s) still processing thumbnails. Some creatives may have gray thumbnails.")
            else:
                elapsed = time.time() - start_time
                logger.info(f"All videos ready after {elapsed:.1f}s")

        # 3. Group uploaded videos by base name and create 1 creative per group
        video_groups: dict[str, dict[str, dict]] = {}
        for up in uploads:
            base_name = up.get("base_name") or _get_base_name(up["name"])
            size = up.get("size") or _get_video_size(up["name"])
            if not size:
                continue
            if base_name not in video_groups:
                video_groups[base_name] = {}
            video_groups[base_name][size] = up
        
        # Create creatives
        results = []
        api_errors = []
        
        ig_actor_id = None
        try:
            from facebook_business.adobjects.page import Page
            p = Page(page_id).api_get(fields=["instagram_business_account"])
            ig_actor_id = p.get("instagram_business_account", {}).get("id")
        except: pass

        template = template_data or {}
        
        # Extract ALL Lists (not just first one)
        headlines_list = template.get("headline") or ["New Game"]
        messages_list = template.get("message") or []

        # CTA Logic
        orig_cta = template.get("call_to_action")
        target_link = store_url

        # Create creatives for each valid group
        for base_name, videos_by_size in video_groups.items():
            res = _create_creative_with_3_videos(base_name, videos_by_size)
            if "error" in res:
                api_errors.append(f"{res['name']}: {res['error']}")
            else:
                results.append(res)

        if api_errors:
            st.error("Some ads failed to create:\n" + "\n".join(api_errors))
            
        return results

# -------------------------------------------------------------------------
# 6. Main Entry Point
# -------------------------------------------------------------------------
def upload_to_facebook(
    game_name: str,
    uploaded_files: list,
    settings: dict,
    *,
    simulate: bool = False,
) -> dict:
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
    
    validate_page_binding(account, page_id)

    target_campaign_id = settings.get("campaign_id")
    target_adset_id = settings.get("adset_id")
    creative_type = settings.get("creative_type", "단일 영상")

    if not target_campaign_id: raise RuntimeError("캠페인이 선택되지 않았습니다.")
    if not target_adset_id: raise RuntimeError("광고 세트가 선택되지 않았습니다.")

    plan = {
        "campaign_id": target_campaign_id,
        "adset_id": target_adset_id,
        "adset_name": "(Existing Ad Set)",
        "page_id": str(page_id),
        "n_videos": len(uploaded_files),
        "creative_type": creative_type
    }
    if simulate: return plan

    # 4. Cleanup Logic
    if creative_type == "단일 영상":
        try:
            cleanup_low_performing_ads(
                account=account, 
                adset_id=target_adset_id, 
                new_files_count=len(uploaded_files)
            )
        except RuntimeError as re:
            raise re
        except Exception as e:
            st.warning(f"Optimization check failed: {e}")

    # 5. Fetch Template
    template_data = fetch_reference_creative_data(account, target_adset_id)
    headlines_found = template_data.get("headline") or []
    messages_found = template_data.get("message") or []
    
    if headlines_found or messages_found:
        h_preview = headlines_found[0] if headlines_found else "None"
        m_preview = messages_found[0][:30] + "..." if messages_found else "None"
        
        st.info(f"📋 Copying settings from existing ad:\n"
                f"- Headlines: {len(headlines_found)} found (e.g. '{h_preview}')\n"
                f"- Messages: {len(messages_found)} found (e.g. '{m_preview}')")
    else:
        st.warning("⚠️ No existing active ads found to copy settings from. Using defaults.")

    ad_name_prefix = (
        settings.get("ad_name_prefix") if settings.get("ad_name_mode") == "Prefix + filename" else None
    )
    store_url = (settings.get("store_url") or "").strip()

    # 6. Upload

    # Determine mode flag
    is_flexible = (creative_type == "다이나믹")
    target_ratio_val = settings.get("dco_aspect_ratio") if is_flexible else None
    manual_creative_name = settings.get("dco_creative_name") if is_flexible else None

    upload_videos_create_ads_cloned(
        account=account,
        page_id=str(page_id),
        adset_id=target_adset_id,
        uploaded_files=uploaded_files,
        ad_name_prefix=ad_name_prefix,
        store_url=store_url,
        template_data=template_data,
        use_flexible_format=is_flexible,
        target_aspect_ratio=target_ratio_val,
        creative_name_manual=manual_creative_name # << Pass it here
    )

    plan["adset_id"] = target_adset_id
    return plan
