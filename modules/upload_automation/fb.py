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
    extract_thumbnail_from_video,
    upload_thumbnail_image,
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

def _is_rate_limit_error(e: Exception) -> bool:
    """Check if exception is a Facebook API rate limit error (code 17)."""
    if isinstance(e, FacebookRequestError):
        try:
            error_code = e.api_error_code()
            if error_code == 17:  # User request limit reached
                return True
        except:
            pass
        # Also check error message
        error_str = str(e).lower()
        if "request limit" in error_str or "code 17" in error_str or "error_subcode 2446079" in error_str:
            return True
    return False

@st.cache_data(ttl=300, show_spinner=False)
def fetch_active_adsets_cached(account_id: str, campaign_id: str) -> list[dict]:
    """Fetch adsets for the given campaign (including ACTIVE, PAUSED, etc. - excluding DELETED)."""
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            account = init_fb_from_secrets(account_id)
            campaign = Campaign(campaign_id)
            
            # First, try to get all adsets without status filter to see what we have
            logger.info(f"Fetching adsets for campaign {campaign_id} (account: {account_id})")
            adsets_all = campaign.get_ad_sets(
                fields=[AdSet.Field.name, AdSet.Field.id, AdSet.Field.effective_status],
                params={"limit": 100}
            )
            
            # Convert to list immediately to avoid iterator exhaustion
            adsets_list = list(adsets_all)
            logger.info(f"Campaign {campaign_id} fetched {len(adsets_list)} total adsets")
            
            # Log all statuses found and filter in one pass
            status_counts = {}
            filtered = []
            for a in adsets_list:
                status = a.get("effective_status", "UNKNOWN")
                status_counts[status] = status_counts.get(status, 0) + 1
                
                # Filter out DELETED and ARCHIVED
                status_upper = str(status).upper() if status else ""
                if status_upper not in ["DELETED", "ARCHIVED"]:
                    adset_id = a.get("id")
                    adset_name = a.get("name", "Unknown")
                    if adset_id:
                        filtered.append({"id": adset_id, "name": adset_name})
                    else:
                        logger.warning(f"AdSet missing ID: {a}")
                else:
                    logger.debug(f"AdSet {a.get('id')} excluded (status: {status_upper})")
            
            logger.info(f"Campaign {campaign_id} adsets by status: {status_counts}")
            logger.info(f"Campaign {campaign_id} returning {len(filtered)} adsets (excluding DELETED/ARCHIVED)")
            
            # If no adsets found but we have adsets_list, log details for debugging
            if not filtered and adsets_list:
                logger.warning(f"Campaign {campaign_id}: Found {len(adsets_list)} adsets but all were filtered out. Status breakdown: {status_counts}")
                logger.warning(f"First few adset details: {[{'id': a.get('id'), 'name': a.get('name'), 'status': a.get('effective_status')} for a in adsets_list[:3]]}")
            
            return filtered
        except Exception as e:
            if _is_rate_limit_error(e) and attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff: 5s, 10s, 20s
                logger.warning(f"Rate limit hit fetching adsets for campaign {campaign_id}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Error fetching adsets for campaign {campaign_id}: {e}", exc_info=True)
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
        # Get current ads count (Facebook adset creative limit includes ALL ads except DELETED)
        adset = AdSet(adset_id)
        # Handle pagination to get ALL ads with retry logic
        all_ads = []
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                ads_iterator = adset.get_ads(
                    fields=[Ad.Field.id, Ad.Field.created_time, Ad.Field.status],
                    params={"limit": 100}  # Remove effective_status filter to count all ads
                )
                # Iterate through all pages
                for ad in ads_iterator:
                    all_ads.append(ad)
                break  # Success
            except Exception as e:
                if _is_rate_limit_error(e) and attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f"Rate limit hit fetching ads for adset {adset_id}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.warning(f"Cleanup check failed: {e}")
                    return  # Don't fail the whole upload if cleanup check fails
        # Filter out DELETED ads (they don't count towards limit)
        # Also log status distribution for debugging
        status_counts = {}
        for ad in all_ads:
            status = ad.get("status", "UNKNOWN")
            status_counts[status] = status_counts.get(status, 0) + 1
        logger.info(f"AdSet {adset_id} ads by status: {status_counts}")
        
        current_ads = [ad for ad in all_ads if ad.get("status") != "DELETED"]
        current_count = len(current_ads)
        logger.info(f"AdSet {adset_id} total ads (excluding DELETED): {current_count}")
        
        # Calculate how many creatives will be uploaded
        remote_list = st.session_state.get("remote_videos", {}).get(game, [])
        creative_type = st.session_state.get("settings", {}).get(game, {}).get("creative_type", "단일 영상")
        dco_aspect_ratio = st.session_state.get("settings", {}).get(game, {}).get("dco_aspect_ratio")
        
        # Helper function to count video groups (each group = 1 creative with 3 sizes)
        def _get_base_name(filename: str) -> str:
            return filename.split("_")[0] if "_" in filename else filename.split(".")[0]
        
        def _get_video_size(filename: str) -> str | None:
            if "1080x1080" in filename or "1920x1080" in filename or "1080x1920" in filename:
                return True
            return None
        
        if creative_type == "단일 영상":
            # Count video groups (each group = 1 creative)
            video_groups = set()
            for item in remote_list:
                name = getattr(item, "name", None) or (item.get("name") if isinstance(item, dict) else None)
                if name and _get_video_size(name):
                    video_groups.add(_get_base_name(name))
            new_creatives_count = len(video_groups)
        elif creative_type == "다이나믹" and dco_aspect_ratio == "single video":
            # Dynamic single video: count video groups (each group = 1 creative with 3 sizes)
            video_groups = set()
            for item in remote_list:
                name = getattr(item, "name", None) or (item.get("name") if isinstance(item, dict) else None)
                if name and _get_video_size(name):
                    video_groups.add(_get_base_name(name))
            new_creatives_count = len(video_groups)
        else:
            # Dynamic regular mode (1:1, 16:9, 9:16): 1 creative for all videos
            new_creatives_count = 1
        
        total_after_upload = current_count + new_creatives_count
        
        # If capacity is sufficient, no action needed
        if total_after_upload <= ADSET_CREATIVE_LIMIT:
            return
        
        # Need to free up space
        needed_space = total_after_upload - ADSET_CREATIVE_LIMIT
        
        # Get all active ads with spending data (with retry logic)
        active_ads = []
        for attempt in range(max_retries):
            try:
                active_ads_iterator = adset.get_ads(
                    fields=[Ad.Field.id, Ad.Field.created_time, Ad.Field.name],
                    params={"effective_status": ["ACTIVE"], "limit": 100}
                )
                active_ads = list(active_ads_iterator)
                break  # Success
            except Exception as e:
                if _is_rate_limit_error(e) and attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f"Rate limit hit fetching active ads for adset {adset_id}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.warning(f"Could not fetch active ads for cleanup: {e}")
                    return  # Don't fail the whole upload if cleanup check fails
        all_ads = active_ads
        
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
                # Get 14-day spending (use string field name instead of Insights.Field) with retry
                spend_14d = 0.0
                spend_7d = 0.0
                for attempt in range(max_retries):
                    try:
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
                        break  # Success
                    except Exception as e:
                        if _is_rate_limit_error(e) and attempt < max_retries - 1:
                            wait_time = retry_delay * (2 ** attempt)
                            logger.warning(f"Rate limit hit fetching insights for ad {ad_id}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})...")
                            time.sleep(wait_time)
                            continue
                        else:
                            raise  # Re-raise if not rate limit or last attempt
                
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
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
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
            # Get ALL headlines (not just first one)
            # PRIORITY: asset_feed_spec first (this is where multiple headlines come from in Dynamic/Flexible creatives)
            # Note: Facebook API uses 'titles' in asset_feed_spec, not 'headlines'
            headlines = []
            if asset_feed.get("titles"):  # Facebook uses 'titles' in asset_feed_spec
                titles_raw = asset_feed["titles"]
                if isinstance(titles_raw, list):
                    for h in titles_raw:
                        if isinstance(h, dict) and "text" in h:
                            text = h.get("text", "")  # strip() 제거 - 공백도 의미있을 수 있음
                            headlines.append(text)  # 빈 문자열도 포함
                        elif h is not None:  # None만 제외
                            headlines.append(str(h))
                else:
                    if isinstance(titles_raw, dict) and "text" in titles_raw:
                        text = titles_raw.get("text", "").strip()
                        if text:
                            headlines.append(text)
                    else:
                        headlines.append(str(titles_raw))
            elif asset_feed.get("headlines"):  # Fallback to 'headlines' if exists
                headlines_raw = asset_feed["headlines"]
                if isinstance(headlines_raw, list):
                    for h in headlines_raw:
                        if isinstance(h, dict) and "text" in h:
                            text = h.get("text", "").strip()
                            if text:
                                headlines.append(text)
                        elif h:
                            headlines.append(str(h))
                else:
                    if isinstance(headlines_raw, dict) and "text" in headlines_raw:
                        text = headlines_raw.get("text", "").strip()
                        if text:
                            headlines.append(text)
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
                    for m in bodies_raw:
                        if isinstance(m, dict) and "text" in m:
                            text = m.get("text", "")  # strip() 제거
                            messages.append(text)  # 빈 문자열도 포함
                        elif m is not None:  # None만 제외
                            messages.append(str(m))
                else:
                    if isinstance(bodies_raw, dict) and "text" in bodies_raw:
                        text = bodies_raw.get("text", "").strip()
                        if text:
                            messages.append(text)
                    else:
                        messages.append(str(bodies_raw))
            elif asset_feed.get("messages"):  # Fallback to 'messages' if exists
                messages_raw = asset_feed["messages"]
                if isinstance(messages_raw, list):
                    for m in messages_raw:
                        if isinstance(m, dict) and "text" in m:
                            text = m.get("text", "").strip()
                            if text:
                                messages.append(text)
                        elif m:
                            messages.append(str(m))
                else:
                    if isinstance(messages_raw, dict) and "text" in messages_raw:
                        text = messages_raw.get("text", "").strip()
                        if text:
                            messages.append(text)
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
            # Store creative_id for display
            result["creative_id"] = creative_id
            # Store ALL headlines (remove duplicates while preserving order)
            # Include empty strings as they may be valid placeholders
            if headlines:
                seen = set()
                unique_headlines = []
                for h in headlines:
                    h_str = str(h).strip() if isinstance(h, str) else str(h)
                    # Include empty strings - they may be valid placeholders
                    if h_str not in seen:
                        seen.add(h_str)
                        unique_headlines.append(h_str)
                result["headline"] = unique_headlines
                logger.info(f"Found {len(unique_headlines)} unique headlines: {unique_headlines}")
            else:
                logger.warning(f"No headlines found in creative {creative_id}")
            
            # Store ALL messages (remove duplicates while preserving order)
            # Include empty strings as they may be valid placeholders
            if messages:
                seen = set()
                unique_messages = []
                for m in messages:
                    m_str = str(m).strip() if isinstance(m, str) else str(m)
                    # Include empty strings - they may be valid placeholders
                    if m_str not in seen:
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
            
            # Copy ALL other video_data fields (app_link, application_id, object_id, etc.)
            # Exclude fields we already handle separately (title, message, call_to_action, video_id, image_url)
            excluded_fields = {"title", "message", "call_to_action", "video_id", "image_url"}
            video_data_other = {}
            for key, value in video_data.items():
                if key not in excluded_fields and value is not None:
                    # Deep copy to avoid reference issues
                    if isinstance(value, dict):
                        video_data_other[key] = {str(k): str(v) if v is not None else None for k, v in value.items()}
                    elif isinstance(value, (list, tuple)):
                        video_data_other[key] = [str(item) for item in value]
                    else:
                        video_data_other[key] = str(value) if value is not None else None
            
            if video_data_other:
                result["video_data_other"] = video_data_other
                logger.info(f"Found additional video_data fields: {list(video_data_other.keys())}")
            
            # Also copy asset_feed_spec other fields if any
            # Exclude asset_customization_rules as they reference specific asset labels that may not exist
            # Exclude ad_formats as it must be set explicitly, not copied from existing creative
            asset_feed_other = {}
            excluded_asset_fields = {"titles", "headlines", "bodies", "messages", "call_to_action", "video_assets", "videos", "asset_customization_rules", "ad_formats"}
            for key, value in asset_feed.items():
                if key not in excluded_asset_fields and value is not None:
                    if isinstance(value, dict):
                        asset_feed_other[key] = {str(k): str(v) if v is not None else None for k, v in value.items()}
                    elif isinstance(value, (list, tuple)):
                        asset_feed_other[key] = [str(item) for item in value]
                    else:
                        asset_feed_other[key] = str(value) if value is not None else None
            
            if asset_feed_other:
                result["asset_feed_other"] = asset_feed_other
                logger.info(f"Found additional asset_feed_spec fields: {list(asset_feed_other.keys())}")
                
            return result
        except Exception as e:
            if _is_rate_limit_error(e) and attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)
                logger.warning(f"Rate limit hit fetching reference creative data for adset {adset_id}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(wait_time)
                continue
            else:
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
        from datetime import datetime, timedelta
        
        # Convert timestamps to YYYY-MM-DD format
        now = datetime.now()
        date_7d_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        today = now.strftime("%Y-%m-%d")
        
        insights = account.get_insights(
            fields=[
                AdsInsights.Field.ad_id,
                AdsInsights.Field.spend,
                AdsInsights.Field.impressions,
                AdsInsights.Field.ctr,
            ],
            params={
                "level": "ad",
                "time_range": {"since": date_7d_ago, "until": today},
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
            # Add button to clear cache and retry
            if st.button("🔄 Clear Cache & Retry", key=f"clear_adset_cache_{idx}"):
                st.cache_data.clear()
                st.rerun()
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

        if creative_type == "다이나믹":
            dco_aspect_ratio = st.selectbox(
                "Aspect Ratio",
                options=["1:1", "9:16", "16:9", "single video"],
                index=0, 
                key=f"fb_dco_ratio_{idx}",
            )
            
            # Show creative name input for all 다이나믹 modes
            if dco_aspect_ratio == "single video":
                # For single video mode, show per-video creative title input
                st.info("💡 Single Video 모드: 각 비디오당 3개 사이즈(1080x1080, 1920x1080, 1080x1920)가 필요합니다.")
                dco_creative_name = st.text_input(
                    "Creative Title (Optional, 기본값: videoxxx)",
                    value=st.session_state.get(f"fb_dco_name_{idx}", ""),
                    key=f"fb_dco_name_input_{idx}",
                    help="비워두면 비디오 이름에서 자동 추출됩니다 (예: video263 → video263)"
                )
            else:
                dco_creative_name = st.text_input(
                    "Creative Name (Optional)",
                    value=st.session_state.get(f"fb_dco_name_{idx}", ""),
                    key=f"fb_dco_name_input_{idx}",
                )
            single_creative_name = None
        else:
            dco_aspect_ratio = None
            dco_creative_name = None
            single_creative_name = st.text_input(
                "Creative Title (Optional, 기본값: videoxxx)",
                value=st.session_state.get(f"fb_single_name_{idx}", ""),
                key=f"fb_single_name_input_{idx}",
                help="비워두면 비디오 이름에서 자동 추출됩니다 (예: video263 → video263)"
            )
        
        st.session_state.settings[game] = {
            "campaign_id": selected_campaign_id,
            "adset_id": selected_adset_id,
            "creative_type": creative_type,
            "store_url": "",  # Store URL removed from UI - will use from template or empty
            "dco_aspect_ratio": dco_aspect_ratio,
            "dco_creative_name": dco_creative_name,
            "single_creative_name": single_creative_name if creative_type == "단일 영상" else None,
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
    errors = []  # Initialize errors list for all modes
    valid_groups = {}  # Initialize valid_groups for all modes
    
    if creative_type == "다이나믹":
        # Validate video sizes based on aspect ratio
        dco_aspect_ratio = settings.get("dco_aspect_ratio")
        
        if dco_aspect_ratio and dco_aspect_ratio != "single video":
            # Regular aspect ratio mode (1:1, 16:9, 9:16) - validate sizes
            required_size_map = {
                "1:1": "1080x1080",
                "16:9": "1920x1080",
                "9:16": "1080x1920",
            }
            required_size = required_size_map.get(dco_aspect_ratio)
            suffix_map = {
                "1:1": "_정방",
                "16:9": "_가로",
                "9:16": "_세로",
            }
            name_suffix = suffix_map.get(dco_aspect_ratio, "_정방")
            
            def _get_video_size_from_name(filename: str) -> str | None:
                """Extract size from filename"""
                if "1080x1080" in filename:
                    return "1080x1080"
                elif "1920x1080" in filename:
                    return "1920x1080"
                elif "1080x1920" in filename:
                    return "1080x1920"
                return None
            
            # Validate video sizes
            invalid_videos = []
            valid_video_names = []
            for name in video_names:
                size = _get_video_size_from_name(name)
                if size != required_size:
                    invalid_videos.append(f"{name} (expected {required_size}, found {size or 'unknown'})")
                else:
                    valid_video_names.append(name)
            
            if invalid_videos:
                return {
                    "campaign_id": target_campaign_id,
                    "adset_id": target_adset_id,
                    "error": f"다이나믹 모드 ({dco_aspect_ratio}): 다음 비디오의 사이즈를 확인하세요:\n" + "\n".join(invalid_videos),
                    "preview_creatives": [],
                    "current_ad_count": 0,
                    "creative_type": creative_type,
                    "n_videos": len(video_names),
                    "capacity_info": {
                        "current_count": 0,
                        "limit": 50,
                        "available_slots": 0,
                        "new_creatives_count": 0,
                        "will_exceed": False,
                        "ads_to_delete": []
                    },
                    "template_source": {
                        "headlines_found": len(headlines_found),
                        "messages_found": len(messages_found),
                        "headline_example": single_headline,
                        "message_example": single_message[:50] + "..." if len(single_message) > 50 else single_message,
                        "cta": cta_found,
                    },
                    "store_url": store_url,
                }
            
            # Generate creative name using same logic as actual upload
            def _extract_video_number(filename: str) -> int | None:
                """Extract number from video name (e.g., 'video001' -> 1, 'video12' -> 12)"""
                import re
                match = re.search(r'video\s*(\d+)', filename, re.IGNORECASE)
                if match:
                    return int(match.group(1))
                return None
            
            def _generate_creative_name(video_names: list[str], game_name: str | None, suffix: str) -> str:
                """Generate creative name based on video number patterns"""
                video_numbers = []
                other_videos = []
                
                for name in video_names:
                    num = _extract_video_number(name)
                    if num is not None:
                        video_numbers.append((num, name))
                    else:
                        other_videos.append(name)
                
                # Sort by number
                video_numbers.sort(key=lambda x: x[0])
                
                parts = []
                
                # Add non-numbered videos first
                for name in other_videos:
                    base = name.rsplit('.', 1)[0] if '.' in name else name
                    parts.append(base)
                
                # Group consecutive numbers
                if video_numbers:
                    ranges = []
                    current_start = video_numbers[0][0]
                    current_end = video_numbers[0][0]
                    
                    for i in range(1, len(video_numbers)):
                        if video_numbers[i][0] == current_end + 1:
                            current_end = video_numbers[i][0]
                        else:
                            if current_start == current_end:
                                ranges.append(f"video{current_start:03d}")
                            else:
                                ranges.append(f"video{current_start:03d}-{current_end:03d}")
                            current_start = video_numbers[i][0]
                            current_end = video_numbers[i][0]
                    
                    if current_start == current_end:
                        ranges.append(f"video{current_start:03d}")
                    else:
                        ranges.append(f"video{current_start:03d}-{current_end:03d}")
                    
                    parts.extend(ranges)
                
                # Build final name
                name_parts = [p for p in parts if p]
                if not name_parts:
                    base_name = video_names[0].rsplit('.', 1)[0] if video_names else "video"
                    creative_name = f"{base_name}_{len(video_names)}vids"
                else:
                    video_part = ",".join(name_parts)
                    game_part = game_name or "game"
                    creative_name = f"{video_part}_{game_part}_flexible{suffix}"
                
                return creative_name
            
            # Get game name from game_name parameter
            creative_name = settings.get("dco_creative_name")
            if not creative_name:
                creative_name = _generate_creative_name(valid_video_names, game_name, name_suffix)
            
            # Create preview creative for dynamic mode (1:1, 16:9, 9:16)
            # In dynamic mode, all videos are combined into one flexible format creative
            preview_creatives.append({
                "name": creative_name,
                "type": f"Dynamic Creative ({dco_aspect_ratio})",
                "videos": valid_video_names,  # List of all video names
                "aspect_ratio": dco_aspect_ratio,
                "headline": headlines_found,  # All headlines
                "message": messages_found,     # All messages
                "cta": cta_found,
            })
        elif dco_aspect_ratio == "single video":
            # single video mode - validate 3 sizes per video and create one creative per group
            def _get_base_name_from_filename(filename: str) -> str:
                """Extract base name from filename (e.g., 'video462_ageofdinosaurs_en_45s_1080x1080_brown_251205.mp4' -> 'video462')"""
                # Extract only the part before the first underscore (e.g., "video462")
                if "_" in filename:
                    base = filename.split("_")[0]
                else:
                    # If no underscore, use filename without extension
                    base = filename.rsplit('.', 1)[0] if '.' in filename else filename
                return base
            
            def _get_video_size_from_name(filename: str) -> str | None:
                """Extract size from filename"""
                if "1080x1080" in filename:
                    return "1080x1080"
                elif "1920x1080" in filename:
                    return "1920x1080"
                elif "1080x1920" in filename:
                    return "1080x1920"
                return None
            
            # Group videos by base name
            video_groups: dict[str, dict[str, str]] = {}
            for name in video_names:
                base_name = _get_base_name_from_filename(name)
                size = _get_video_size_from_name(name)
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
                    "campaign_id": target_campaign_id,
                    "adset_id": target_adset_id,
                    "error": "\n".join(errors),
                    "preview_creatives": [],
                    "current_ad_count": 0,
                    "creative_type": creative_type,
                    "n_videos": len(video_names),
                    "capacity_info": {
                        "current_count": 0,
                        "limit": 50,
                        "available_slots": 0,
                        "new_creatives_count": 0,
                        "will_exceed": False,
                        "ads_to_delete": []
                    },
                    "template_source": {
                        "headlines_found": len(headlines_found),
                        "messages_found": len(messages_found),
                        "headline_example": single_headline,
                        "message_example": single_message[:50] + "..." if len(single_message) > 50 else single_message,
                        "cta": cta_found,
                    },
                    "store_url": store_url,
                }
            
            # Create one creative per group (with 3 videos)
            for base_name, sizes in valid_groups.items():
                # Use base_name as creative name (e.g., "video263")
                creative_name = settings.get("dco_creative_name") or base_name
                
                preview_creatives.append({
                    "name": creative_name,
                    "type": "Single Video (Flexible Format, 3 sizes)",
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
    elif creative_type == "단일 영상":
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
                "campaign_id": target_campaign_id,
                "adset_id": target_adset_id,
                "error": "\n".join(errors),
                "preview_creatives": [],
                "current_ad_count": 0,
                "creative_type": creative_type,
                "n_videos": len(video_names),
                "capacity_info": {
                    "current_count": 0,
                    "limit": 50,
                    "available_slots": 0,
                    "new_creatives_count": 0,
                    "will_exceed": False,
                    "ads_to_delete": []
                },
                "template_source": {
                    "headlines_found": len(headlines_found),
                    "messages_found": len(messages_found),
                    "headline_example": single_headline,
                    "message_example": single_message[:50] + "..." if len(single_message) > 50 else single_message,
                    "cta": cta_found,
                },
                "store_url": store_url,
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
    else:
        # Unknown creative type
        pass
    
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
        # Facebook adset creative limit includes ALL ads (ACTIVE, PAUSED, etc.) except DELETED
        # So we need to count all ads, not just ACTIVE ones
        # Handle pagination to get ALL ads
        current_ads = []
        ads_iterator = adset.get_ads(
            fields=[Ad.Field.id, Ad.Field.created_time, Ad.Field.name, Ad.Field.status],
            params={"limit": 100}  # Remove effective_status filter to count all ads
        )
        # Iterate through all pages
        for ad in ads_iterator:
            current_ads.append(ad)
        # Filter out DELETED ads (they don't count towards limit)
        # Also log status distribution for debugging
        status_counts = {}
        for ad in current_ads:
            status = ad.get("status", "UNKNOWN")
            status_counts[status] = status_counts.get(status, 0) + 1
        logger.info(f"AdSet {target_adset_id} ads by status: {status_counts}")
        
        current_ads = [ad for ad in current_ads if ad.get("status") != "DELETED"]
        current_ad_count = len(current_ads)
        logger.info(f"AdSet {target_adset_id} total ads (excluding DELETED): {current_ad_count}")
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
# 4.5. Wait for Videos to be Ready
# -------------------------------------------------------------------------
def _wait_for_videos_ready(account: AdAccount, video_ids: list[str], *, timeout_s: int = 300, sleep_s: int = 5) -> list[str]:
    """
    Wait for all videos to be ready (status READY or PUBLISHED) before creating creatives.
    Handles rate limit errors gracefully.
    Returns list of video IDs that are ready.
    """
    from facebook_business.adobjects.advideo import AdVideo
    from facebook_business.exceptions import FacebookRequestError
    import time
    import warnings
    
    if not video_ids:
        return []
    
    ready = {vid: False for vid in video_ids}
    deadline = time.time() + timeout_s
    consecutive_rate_limits = 0
    max_rate_limits = 5
    
    logger.info(f"Waiting for {len(video_ids)} videos to be ready (timeout: {timeout_s}s)...")
    
    while time.time() < deadline:
        all_done = True
        rate_limit_in_this_loop = False
        
        for vid in video_ids:
            if ready[vid]:
                continue
            try:
                # Suppress warnings about thumbnails field
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    info = AdVideo(vid).api_get(fields=["status", "picture"])
                status = info.get("status")
                has_pic = bool(info.get("picture"))
                
                # Video is ready if status is READY or PUBLISHED
                # Also accept other statuses that might indicate readiness
                if status in ("READY", "PUBLISHED"):
                    ready[vid] = True
                    logger.info(f"Video {vid} is ready (status: {status})")
                elif status == "PROCESSING":
                    all_done = False
                    logger.debug(f"Video {vid} is still processing...")
                elif status in ("FAILED", "ERROR"):
                    # Video failed, mark as ready to avoid infinite wait (will fail during creative creation)
                    logger.warning(f"Video {vid} has status {status}, marking as ready to proceed (will fail during creative creation)")
                    ready[vid] = True
                else:
                    # Unknown status, log and wait more
                    all_done = False
                    logger.debug(f"Video {vid} status: {status} (not ready yet, waiting...)")
                
                consecutive_rate_limits = 0
                
            except FacebookRequestError as e:
                error_code = None
                try:
                    error_code = e.api_error_code()
                except:
                    pass
                
                if error_code == 4 or "rate limit" in str(e).lower():
                    rate_limit_in_this_loop = True
                    consecutive_rate_limits += 1
                    if consecutive_rate_limits >= max_rate_limits:
                        ready_list = [vid for vid, is_ready in ready.items() if is_ready]
                        logger.warning(f"Too many rate limits, returning {len(ready_list)}/{len(video_ids)} ready videos")
                        return ready_list
                else:
                    logger.warning(f"Error checking video {vid}: {e}")
                    all_done = False
                    
            except Exception as e:
                logger.warning(f"Error checking video {vid}: {e}")
                all_done = False
        
        if all_done:
            logger.info(f"All {len(video_ids)} videos are ready!")
            return [vid for vid, is_ready in ready.items() if is_ready]
        
        # Rate limit handling
        if rate_limit_in_this_loop:
            sleep_time = min(sleep_s * 2, 30)  # Longer sleep for rate limits
        else:
            sleep_time = sleep_s
        
        time.sleep(sleep_time)
    
    # Timeout reached
    ready_list = [vid for vid, is_ready in ready.items() if is_ready]
    logger.warning(f"Timeout reached: {len(ready_list)}/{len(video_ids)} videos are ready.")
    return ready_list

# -------------------------------------------------------------------------
# 4.6. Resumable Video Upload Helper
# -------------------------------------------------------------------------
def _upload_video_resumable(account: AdAccount, path: str) -> str:
    """
    Chunked upload to /{act_id}/advideos using the official 3-phase protocol.
    Retries transient errors and verifies total bytes sent before finishing.
    """
    import requests
    import os
    
    # Try to get token from [facebook] section first, then root
    if "facebook" in st.secrets:
        token = st.secrets["facebook"].get("access_token", "").strip()
    else:
        token = st.secrets.get("access_token", "").strip()
    
    if not token:
        raise RuntimeError("Missing access_token in st.secrets (check [facebook] section)")
    
    act = account.get_id()
    base = f"https://graph.facebook.com/v24.0/{act}/advideos"
    file_size = os.path.getsize(path)
    
    def _post(data, files=None, max_retries=5):
        delays = [0, 2, 4, 8, 12]
        last = None
        for i, d in enumerate(delays[:max_retries], 1):
            if d:
                time.sleep(d)
            try:
                r = requests.post(
                    base,
                    data={**data, "access_token": token},
                    files=files,
                    timeout=300,  # Increased timeout for large files
                )
                if r.status_code >= 500:
                    last = RuntimeError(f"HTTP {r.status_code}: {r.text[:400]}")
                    continue
                j = r.json()
                if "error" in j:
                    code = j["error"].get("code")
                    if code in (390,) and i < max_retries:
                        last = RuntimeError(j["error"].get("message"))
                        continue
                    raise RuntimeError(j["error"].get("message", str(j["error"])))
                return j
            except Exception as e:
                last = e
        raise last or RuntimeError("advideos POST failed")
    
    start_resp = _post(
        {"upload_phase": "start", "file_size": str(file_size), "content_category": "VIDEO_GAMING"}
    )
    upload_session_id = start_resp["upload_session_id"]
    video_id = start_resp["video_id"]
    start_offset = int(start_resp.get("start_offset", 0))
    end_offset = int(start_resp.get("end_offset", 0))
    
    sent_bytes = 0
    
    with open(path, "rb") as f:
        while True:
            if start_offset == end_offset == file_size:
                break
            
            if end_offset <= start_offset:
                tr = _post(
                    {
                        "upload_phase": "transfer",
                        "upload_session_id": upload_session_id,
                        "start_offset": str(start_offset),
                    }
                )
                start_offset = int(tr.get("start_offset", start_offset))
                end_offset = int(tr.get("end_offset", end_offset or file_size))
                continue
            
            to_read = end_offset - start_offset
            f.seek(start_offset)
            chunk = f.read(to_read)
            if not chunk or len(chunk) != to_read:
                raise RuntimeError(f"Read {len(chunk) if chunk else 0} bytes; expected {to_read}.")
            
            files = {"video_file_chunk": ("chunk.bin", chunk, "application/octet-stream")}
            tr = _post(
                {
                    "upload_phase": "transfer",
                    "upload_session_id": upload_session_id,
                    "start_offset": str(start_offset),
                },
                files=files,
            )
            
            sent_bytes += to_read
            new_start = int(tr.get("start_offset", start_offset + to_read))
            new_end = int(tr.get("end_offset", end_offset))
            
            start_offset, end_offset = new_start, new_end
            if start_offset > file_size:
                start_offset = file_size
            if end_offset > file_size:
                end_offset = file_size
    
    if sent_bytes != file_size:
        raise RuntimeError(f"Uploaded bytes ({sent_bytes}) != file size ({file_size}).")
    
    try:
        _post({"upload_phase": "finish", "upload_session_id": upload_session_id})
        return video_id
    except Exception:
        logger.warning(f"Resumable finish failed for {os.path.basename(path)} — trying fallback upload once.")
        v = account.create_ad_video(params={"file": path, "content_category": "VIDEO_GAMING"})
        return v["id"]

def _clone_creative_with_new_video(
    account: AdAccount,
    reference_creative_id: str,
    video_ids_by_size: dict[str, str],
    thumbnails_by_size: dict[str, str],
    new_creative_name: str,
    store_url: str | None = None 
) -> str:
    """기존 Creative를 완벽히 복제하고 비디오만 교체 (Advantage+ 포함)"""
    from facebook_business.adobjects.adcreative import AdCreative
    import copy
    
    # 1. 모든 필드 가져오기 (Advantage+ 포함!)
    ref_creative = AdCreative(reference_creative_id).api_get(fields=[
        AdCreative.Field.object_story_spec,
        AdCreative.Field.asset_feed_spec,
        AdCreative.Field.degrees_of_freedom_spec,
    ])
    
    new_params = {"name": new_creative_name}
    
    # 2. asset_feed_spec 복제 (Single Image or Video)
    if ref_creative.get("asset_feed_spec"):
        feed_spec = copy.deepcopy(ref_creative["asset_feed_spec"])
        
        if "videos" in feed_spec and isinstance(feed_spec["videos"], list):
            for i, video_obj in enumerate(feed_spec["videos"]):
                labels = video_obj.get("adlabels", [])
                
                # Label 추출
                label_names = []
                for label in labels:
                    if isinstance(label, dict):
                        label_names.append(str(label.get("name", "")).lower())
                    else:
                        label_names.append(str(label).lower())
                
                label_str = " ".join(label_names)
                
                new_video_id = None
                new_thumb = None
                
                # 개선된 Placement 매칭
                if any(kw in label_str for kw in ["feed", "reels_extreme", "1080x1080"]):
                    new_video_id = video_ids_by_size.get("1080x1080")
                    new_thumb = thumbnails_by_size.get("1080x1080")
                elif any(kw in label_str for kw in ["story", "status", "reels", "search", "apps_and_sites", "1080x1920"]):
                    new_video_id = video_ids_by_size.get("1080x1920")
                    new_thumb = thumbnails_by_size.get("1080x1920")
                elif any(kw in label_str for kw in ["instream", "facebook_search", "1920x1080"]):
                    new_video_id = video_ids_by_size.get("1920x1080")
                    new_thumb = thumbnails_by_size.get("1920x1080")
                else:
                    # Fallback: Index 기반
                    sizes = ["1080x1080", "1080x1920", "1920x1080"]
                    if i < len(sizes):
                        new_video_id = video_ids_by_size.get(sizes[i])
                        new_thumb = thumbnails_by_size.get(sizes[i])
                
                if new_video_id:
                    video_obj["video_id"] = new_video_id
                    if new_thumb:
                        video_obj["thumbnail_url"] = new_thumb
        
        new_params["asset_feed_spec"] = feed_spec
    
    # 3. object_story_spec 복제 (Standard Video)
    elif ref_creative.get("object_story_spec"):
        spec = copy.deepcopy(ref_creative["object_story_spec"])
        
        if "video_data" in spec:
            spec["video_data"]["video_id"] = video_ids_by_size.get("1080x1080")
            if thumbnails_by_size.get("1080x1080"):
                spec["video_data"]["image_url"] = thumbnails_by_size.get("1080x1080")
        
        new_params["object_story_spec"] = spec
    
    # 4. Advantage+ 설정 복제
    if ref_creative.get("degrees_of_freedom_spec"):
        new_params["degrees_of_freedom_spec"] = copy.deepcopy(
            ref_creative["degrees_of_freedom_spec"]
        )
        logger.info("✅ Copied Advantage+ Creative settings")
    
    if store_url:
        # asset_feed_spec에서 URL 덮어쓰기
        if "asset_feed_spec" in new_params:
            feed_spec = new_params["asset_feed_spec"]
            
            # call_to_action 필드 업데이트
            if "call_to_action" in feed_spec:
                if isinstance(feed_spec["call_to_action"], dict):
                    if "value" not in feed_spec["call_to_action"]:
                        feed_spec["call_to_action"]["value"] = {}
                    feed_spec["call_to_action"]["value"]["link"] = store_url
            
            # videos 내부 URL 업데이트 (혹시 있다면)
            if "videos" in feed_spec:
                for video_obj in feed_spec["videos"]:
                    if "call_to_action" in video_obj:
                        if isinstance(video_obj["call_to_action"], dict):
                            if "value" not in video_obj["call_to_action"]:
                                video_obj["call_to_action"]["value"] = {}
                            video_obj["call_to_action"]["value"]["link"] = store_url
        
        # object_story_spec에서 URL 덮어쓰기
        if "object_story_spec" in new_params:
            spec = new_params["object_story_spec"]
            if "video_data" in spec:
                if "call_to_action" in spec["video_data"]:
                    if isinstance(spec["video_data"]["call_to_action"], dict):
                        if "value" not in spec["video_data"]["call_to_action"]:
                            spec["video_data"]["call_to_action"]["value"] = {}
                        spec["video_data"]["call_to_action"]["value"]["link"] = store_url
        
        logger.info(f"✅ Updated Store URL to: {store_url}")

    # 5. Creative 생성
    new_creative = account.create_ad_creative(params=new_params)
    return new_creative["id"]
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
    game_name: str | None = None,
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
    headlines_list = template.get("headline") or []
    messages_list = template.get("message") or []
    orig_cta = template.get("call_to_action")
    video_data_other = template.get("video_data_other", {})  # All other video_data fields (app_link, etc.)
    asset_feed_other = template.get("asset_feed_other", {})  # All other asset_feed_spec fields
    
    # Ensure lists (not just first one)
    if not isinstance(headlines_list, list):
        headlines_list = [headlines_list] if headlines_list else []
    if not isinstance(messages_list, list):
        messages_list = [messages_list] if messages_list else []
    
    # Keep empty strings as-is - user may have intentionally set them to empty
    # Don't filter or replace with defaults - respect user's choice
    logger.info(f"Using headlines from template: {headlines_list} (including empty strings if any)")
    logger.info(f"Using messages from template: {messages_list} (including empty strings if any)")
    
    # If headlines_list is completely empty (not just empty strings), we still need at least one for API
    # But if it contains empty strings, use them as-is
    if not headlines_list:
        # Only use default if list is completely empty (not provided at all)
        headlines_list = [""]  # Use empty string, not "New Game"
        logger.info("No headlines provided in template, using empty string")
    
    # Convert headlines and messages to Facebook API format (objects with 'text' field)
    def _format_text_list(text_list):
        """Convert list of strings to list of objects with 'text' field for Facebook API"""
        if not text_list:
            return []
        result = []
        for item in text_list:
            if isinstance(item, dict):
                # Already in object format, use as-is
                result.append(item)
            elif isinstance(item, str):
                # Convert string to object with 'text' field
                result.append({"text": item})
            else:
                # Convert to string first
                result.append({"text": str(item)})
        return result
    
    titles_formatted = _format_text_list(headlines_list)
    bodies_formatted = _format_text_list(messages_list)
    
    # Facebook API requires at least one body, so add empty one if none provided
    if not bodies_formatted:
        bodies_formatted = [{"text": ""}]
    
    target_link = store_url
    
    # -------------------------------------------------------------------------
    # BRANCH A: FLEXIBLE FORMAT (Dynamic Creative)
    # -------------------------------------------------------------------------
    if use_flexible_format:
        # Helper function to extract video size from filename
        def _get_video_size_from_name(filename: str) -> str | None:
            """Extract size from filename"""
            if "1080x1080" in filename:
                return "1080x1080"
            elif "1920x1080" in filename:
                return "1920x1080"
            elif "1080x1920" in filename:
                return "1080x1920"
            return None
        
        # Helper function to extract base name (without size suffix)
        def _get_base_name_from_filename(filename: str) -> str:
            """Extract base name from filename (e.g., 'video462_ageofdinosaurs_en_45s_1080x1080_brown_251205.mp4' -> 'video462')"""
            # Extract only the part before the first underscore (e.g., "video462")
            if "_" in filename:
                base = filename.split("_")[0]
            else:
                # If no underscore, use filename without extension
                base = filename.rsplit('.', 1)[0] if '.' in filename else filename
            return base
        
        # Handle single video mode (requires 3 sizes per video)
        if target_aspect_ratio == "single video":
            # Group videos by base name
            video_groups: dict[str, dict[str, dict]] = {}
            for u in videos:
                name = fb_ops._fname_any(u) or ""
                base_name = _get_base_name_from_filename(name)
                size = _get_video_size_from_name(name)
                if not size:
                    continue
                if base_name not in video_groups:
                    video_groups[base_name] = {}
                video_groups[base_name][size] = u
            
            # Validate: Each group must have all 3 sizes
            required_sizes = {"1080x1080", "1920x1080", "1080x1920"}
            errors = []
            valid_groups = {}

            st.write("### 🔍 디버깅 정보")
            st.write(f"- **Ad Set ID**: `{adset_id}`")
            st.write(f"- **업로드된 비디오**: {len(uploads)}개")
            st.write(f"- **비디오 그룹**: {len(video_groups)}개")

            for base_name, sizes in video_groups.items():
                missing = required_sizes - set(sizes.keys())
                if missing:
                    errors.append(f"{base_name}의 사이즈를 확인하세요. 누락된 사이즈: {', '.join(missing)}")
                else:
                    valid_groups[base_name] = sizes
            
            if errors:
                st.error("\n".join(errors))
                return []
        
            # Process single video mode (will be handled after video upload)
            # Store groups for later processing
            single_video_groups = valid_groups
            name_suffix = ""  # No suffix for single video mode
        else:
            # Regular aspect ratio mode (1:1, 16:9, 9:16)
            required_size_map = {
                "1:1": "1080x1080",
                "16:9": "1920x1080",
                "9:16": "1080x1920",
            }
            required_size = required_size_map.get(target_aspect_ratio, "1080x1080")
            suffix_map = {
                "1:1": "_정방",
                "16:9": "_가로",
                "9:16": "_세로",
            }
            name_suffix = suffix_map.get(target_aspect_ratio, "_정방")
            
            # Check all videos have correct size
            invalid_videos = []
            for u in videos:
                name = fb_ops._fname_any(u) or ""
                size = _get_video_size_from_name(name)
                if size != required_size:
                    invalid_videos.append(f"{name} (expected {required_size}, found {size or 'unknown'})")
            
            if invalid_videos:
                st.error(f"다이나믹 모드 ({target_aspect_ratio}): 다음 비디오의 사이즈를 확인하세요:\n" + "\n".join(invalid_videos))
                return []

            single_video_groups = None
        
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

        # 2. Upload Videos with Thumbnails (OPTIMIZED: 다이나믹 모드에서는 첫 번째 비디오 썸네일만 추출)
        uploads = []
        total = len(persisted)
        progress = st.progress(0, text="Uploading videos with thumbnails (Marketer Mode)...")
        
        # 다이나믹 모드 (1:1, 16:9, 9:16)인 경우: 첫 번째 비디오의 썸네일만 추출
        shared_thumbnail_url = None
        if use_flexible_format and target_aspect_ratio in ["1:1", "16:9", "9:16"]:
            if persisted:
                first_item = persisted[0]
                try:
                    thumbnail_path = extract_thumbnail_from_video(first_item["path"])
                    shared_thumbnail_url = upload_thumbnail_image(account, thumbnail_path)
                    logger.info(f"Extracted and uploaded shared thumbnail for 다이나믹 모드: {shared_thumbnail_url}")
                    # Clean up
                    import os
                    if os.path.exists(thumbnail_path):
                        os.unlink(thumbnail_path)
                except Exception as e:
                    logger.warning(f"Failed to extract thumbnail for first video: {e}. Continuing without thumbnail.")
        
        def _upload_one_with_thumbnail(item, thumbnail_url_to_use=None):
            """Upload one video with its thumbnail (parallelized) using resumable upload"""
            thumbnail_path = None
            thumbnail_url = thumbnail_url_to_use  # Use provided thumbnail or extract new one
            
            # If no thumbnail provided, extract from this video (for 단일 영상 & single video modes)
            if thumbnail_url is None:
                try:
                    # Extract and upload thumbnail before uploading video
                    thumbnail_path = extract_thumbnail_from_video(item["path"])
                    thumbnail_url = upload_thumbnail_image(account, thumbnail_path)
                    logger.info(f"Extracted and uploaded thumbnail for {item['name']}: {thumbnail_url}")
                except Exception as e:
                    logger.warning(f"Failed to extract/upload thumbnail for {item['name']}: {e}. Continuing without thumbnail.")
                    # Continue without thumbnail - will use Facebook's auto-generated one
            
            # Upload video using resumable upload (handles large files and timeouts)
            video_id = _upload_video_resumable(account, item["path"])
            
            # Clean up temporary thumbnail file
            if thumbnail_path:
                import os
                try:
                    if os.path.exists(thumbnail_path):
                        os.unlink(thumbnail_path)
                except Exception:
                    pass
            
            return {
                "name": item["name"],
                "video_id": video_id,
                "thumbnail_url": thumbnail_url
            }
        
        # Upload in parallel (same as test mode)
        done = 0
        if total:
            with ThreadPoolExecutor(max_workers=6) as ex:
                # 다이나믹 모드(1:1, 16:9, 9:16)인 경우: 모든 비디오에 같은 썸네일 URL 사용
                if shared_thumbnail_url:
                    future_to_item = {ex.submit(_upload_one_with_thumbnail, item, shared_thumbnail_url): item for item in persisted}
                else:
                    # 단일 영상 & 다이나믹-single video: 각 사이즈별로 썸네일 추출
                    future_to_item = {ex.submit(_upload_one_with_thumbnail, item, None): item for item in persisted}
                
                for fut in as_completed(future_to_item):
                    item = future_to_item[fut]
                    name = item["name"]
                    try:
                        res = fut.result()
                        uploads.append(res)
                        done += 1
                        if progress is not None:
                            pct = int(done / total * 100)
                            progress.progress(pct, text=f"Uploading {done}/{total} videos…")
                    except Exception as e:
                        st.error(f"Upload failed for {name}: {e}")
    
        progress.empty()
        
        # For single video mode, we need to map uploaded videos back to groups
        if target_aspect_ratio == "single video" and single_video_groups:
            # Re-map groups with uploaded video IDs
            uploaded_by_name = {u["name"]: u for u in uploads}
            updated_groups = {}
            for base_name, size_dict in single_video_groups.items():
                updated_group = {}
                for size, video_file in size_dict.items():
                    # Find matching uploaded video
                    matching_name = next((n for n in uploaded_by_name.keys() if base_name in n and size in n), None)
                    if matching_name:
                        updated_group[size] = uploaded_by_name[matching_name]
                if len(updated_group) == 3:  # All 3 sizes present
                    updated_groups[base_name] = updated_group
            single_video_groups = updated_groups
        
        # Wait for videos to be ready before creating creatives
        # Facebook requires videos to be in READY or PUBLISHED status before use
        st.info("🚀 광고 생성 중... (비디오 처리는 자동으로 완료됩니다)")

        # 3. Create Flexible Format Creatives
        results = []
        api_errors = []
        
        ig_actor_id = None
        try:
            from facebook_business.adobjects.page import Page
            p = Page(page_id).api_get(fields=["instagram_business_account"])
            ig_actor_id = p.get("instagram_business_account", {}).get("id")
        except: pass

        # Handle single video mode separately
        if target_aspect_ratio == "single video" and single_video_groups:
            # Create one flexible creative per video group (3 sizes per creative)
            from facebook_business.adobjects.advideo import AdVideo
            import time
            
            # Get thumbnail from uploaded data (we already extracted and uploaded them)
            def _get_thumbnail_from_upload(video_id: str) -> str | None:
                """Get thumbnail URL from uploads list"""
                for u in uploads:
                    if u.get("video_id") == video_id:
                        return u.get("thumbnail_url")
                return None
            
            # Process each video group
            for base_name, size_dict in single_video_groups.items():
                try:
                    # Get uploaded videos for this group (already mapped)
                    video_1x1 = size_dict.get("1080x1080")
                    video_9x16 = size_dict.get("1080x1920")
                    video_16x9 = size_dict.get("1920x1080")
                    
                    if not all([video_1x1, video_9x16, video_16x9]):
                        api_errors.append(f"{base_name}: Missing videos for one or more sizes")
                        continue
                    
                    # Get thumbnails from uploaded data
                    thumb_1x1 = _get_thumbnail_from_upload(video_1x1["video_id"])
                    thumb_9x16 = _get_thumbnail_from_upload(video_9x16["video_id"])
                    thumb_16x9 = _get_thumbnail_from_upload(video_16x9["video_id"])
                    
                    # Prepare CTA
                    final_cta = None
                    if orig_cta:
                        final_cta = orig_cta.copy()
                        if target_link and "value" in final_cta:
                            final_cta["value"]["link"] = target_link
                    elif target_link:
                        final_cta = {"type": "INSTALL_MOBILE_APP", "value": {"link": target_link}}
                    
                    # Build asset_feed_spec with 3 videos for different placements (like 단일 영상)
                    # Facebook API uses 'videos' (array of video objects) not 'video_assets'
                    # Each video object can have video_id, image_url, and adlabels for placement targeting
                    videos_list = []
                    for video_info, thumb, placements in [
                        (video_1x1, thumb_1x1, ["feed", "reels_extreme_ads"]),
                        (video_9x16, thumb_9x16, ["story", "status", "reels", "search_results", "apps_and_sites"]),
                        (video_16x9, thumb_16x9, ["facebook_search_results"])
                    ]:
                        video_obj = {
                            "video_id": video_info["video_id"]
                        }
                        if thumb:  # Only add image_url if thumbnail is available
                            video_obj["thumbnail_url"] = thumb
                        
                        # Note: placements are handled via asset_customization_rules, not in video object
                        # Copy all other video_data fields (app_link, application_id, etc.)
                        for key, value in video_data_other.items():
                            if key not in video_obj:  # Don't override existing fields
                                video_obj[key] = value
                        
                        videos_list.append(video_obj)
                    
                    asset_feed_spec = {
                        "videos": videos_list,  # Facebook API uses 'videos' not 'video_assets'
                        "titles": titles_formatted,  # Facebook API requires objects with 'text' field
                        "bodies": bodies_formatted,
                        "ad_formats": ["SINGLE_VIDEO"]     # Facebook API requires objects with 'text' field
                        # Note: call_to_action is NOT allowed in asset_feed_spec for flexible format
                        # Use call_to_action_types instead (should be in asset_feed_other)
                    }
                    
                    # Copy all other asset_feed_spec fields
                    import json
                    for key, value in asset_feed_other.items():
                        # Explicitly exclude ad_formats - it must be set explicitly, not copied
                        if key not in asset_feed_spec and key != "ad_formats":  # Don't override existing fields
                            # Special handling for additional_data: must be JSON object, not string
                            if key == "additional_data":
                                if isinstance(value, str):
                                    # Try to parse string representation of AdAssetFeedAdditionalData
                                    # Format: "<AdAssetFeedAdditionalData> {...}"
                                    try:
                                        # Extract JSON part from string like "<AdAssetFeedAdditionalData> {...}"
                                        if "{" in value:
                                            json_str = value[value.index("{"):]
                                            asset_feed_spec[key] = json.loads(json_str)
                                        else:
                                            # Skip if can't parse
                                            logger.warning(f"Could not parse additional_data: {value}")
                                            continue
                                    except (json.JSONDecodeError, ValueError) as e:
                                        logger.warning(f"Could not parse additional_data as JSON: {e}")
                                        continue
                                elif isinstance(value, dict):
                                    asset_feed_spec[key] = value
                                else:
                                    # Skip if not dict or parseable string
                                    continue
                            else:
                                asset_feed_spec[key] = value
                    
                    # Create object_story_spec
                    object_story_spec = {"page_id": page_id}
                    
                    
                    # Generate creative name
                    if creative_name_manual:
                        creative_name = creative_name_manual
                    else:
                        # Default: extract base name (e.g., "video263")
                        creative_name = base_name
                    
                    # Create creative with retry for video processing errors
                    import time  # Import time for sleep in retry logic
                    max_retries = 3
                    retry_delay = 10
                    creative = None
                    for attempt in range(max_retries):
                        try:
                            creative = account.create_ad_creative(params={
                                "name": creative_name,
                                "object_story_spec": object_story_spec,
                                "asset_feed_spec": asset_feed_spec
                            })
                            break  # Success
                        except FacebookRequestError as e:
                            error_subcode = None
                            try:
                                error_subcode = e.api_error_subcode()
                            except:
                                pass
                            
                            # Check if it's a video processing error (1885252)
                            if error_subcode == 1885252 and attempt < max_retries - 1:
                                logger.warning(f"Video not ready yet for {base_name}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})...")
                                time.sleep(retry_delay)
                                retry_delay *= 2  # Exponential backoff
                                continue
                            else:
                                raise  # Re-raise if not retryable or last attempt
                        
                    # Create ad
                    ad_name = make_ad_name(creative_name, ad_name_prefix)
                    account.create_ad(params={
                        "name": ad_name,
                        "adset_id": adset_id,
                        "creative": {"creative_id": creative["id"]},
                        "status": Ad.Status.active,
                    })

                    results.append({"name": creative_name, "creative_id": creative["id"]})
                except Exception as e:
                    api_errors.append(f"{base_name}: {str(e)}")
        elif use_flexible_format:
            # Regular flexible format mode (1:1, 16:9, 9:16)
            try:
                # Build asset_feed_spec with ALL headlines and messages
                # Use shared thumbnail URL for all videos (already extracted from first video)
                shared_thumb = uploads[0].get("thumbnail_url") if uploads else None
                videos_list = []
                for u in uploads:
                    video_obj = {"video_id": u["video_id"]}
                    if shared_thumb:  # Add shared thumbnail to all videos
                        video_obj["thumbnail_url"] = shared_thumb
                    
                    # Copy all other video_data fields (app_link, application_id, etc.)
                    for key, value in video_data_other.items():
                        if key not in video_obj:  # Don't override existing fields
                            video_obj[key] = value
                    
                    videos_list.append(video_obj)
                
                asset_feed_spec = {
                    "videos": videos_list,
                    "titles": titles_formatted,
                    "bodies": bodies_formatted,
                    "ad_formats": ["SINGLE_VIDEO"],  # ← 추가
                }
                
                if target_aspect_ratio:
                    # Map ratio string to API value
                    ratio_map = {
                        "1:1": "1:1",
                        "9:16": "9:16",
                        "16:9": "16:9",
                    }
                    asset_feed_spec["aspect_ratio"] = ratio_map.get(target_aspect_ratio, "1:1")
                
                # Copy all other asset_feed_spec fields
                import json
                for key, value in asset_feed_other.items():
                    # Explicitly exclude ad_formats - it must be set explicitly, not copied
                    if key not in asset_feed_spec and key != "ad_formats":  # Don't override existing fields
                        # Special handling for additional_data: must be JSON object, not string
                        if key == "additional_data":
                            if isinstance(value, str):
                                # Try to parse string representation of AdAssetFeedAdditionalData
                                # Format: "<AdAssetFeedAdditionalData> {...}"
                                try:
                                    # Extract JSON part from string like "<AdAssetFeedAdditionalData> {...}"
                                    if "{" in value:
                                        json_str = value[value.index("{"):]
                                        asset_feed_spec[key] = json.loads(json_str)
                                    else:
                                        # Skip if can't parse
                                        logger.warning(f"Could not parse additional_data: {value}")
                                        continue
                                except (json.JSONDecodeError, ValueError) as e:
                                    logger.warning(f"Could not parse additional_data as JSON: {e}")
                                    continue
                            elif isinstance(value, dict):
                                asset_feed_spec[key] = value
                            else:
                                # Skip if not dict or parseable string
                                continue
                        else:
                            asset_feed_spec[key] = value
                
                # Basic Page Spec
                object_story_spec = {
                    "page_id": page_id,
                }
                

                # Create ONE Creative
                # Use manual name if provided, else auto-generate based on video naming pattern
                if creative_name_manual:
                    creative_name = creative_name_manual
                else:
                    # Generate creative name based on video naming pattern
                    def _extract_video_number(filename: str) -> int | None:
                        """Extract number from video name (e.g., 'video001' -> 1, 'video12' -> 12)"""
                        import re
                        # Try to find pattern like video001, video1, video12, etc.
                        match = re.search(r'video\s*(\d+)', filename, re.IGNORECASE)
                        if match:
                            return int(match.group(1))
                        return None
                    
                    def _generate_creative_name(video_names: list[str], game_name: str | None, suffix: str) -> str:
                        """Generate creative name based on video number patterns"""
                        video_numbers = []
                        other_videos = []
                        
                        for name in video_names:
                            num = _extract_video_number(name)
                            if num is not None:
                                video_numbers.append((num, name))
                            else:
                                other_videos.append(name)
                        
                        # Sort by number
                        video_numbers.sort(key=lambda x: x[0])
                        
                        parts = []
                        
                        # Add non-numbered videos first
                        for name in other_videos:
                            # Extract base name without extension
                            base = name.rsplit('.', 1)[0] if '.' in name else name
                            parts.append(base)
                        
                        # Group consecutive numbers
                        if video_numbers:
                            ranges = []
                            current_start = video_numbers[0][0]
                            current_end = video_numbers[0][0]
                            
                            for i in range(1, len(video_numbers)):
                                if video_numbers[i][0] == current_end + 1:
                                    # Consecutive
                                    current_end = video_numbers[i][0]
                                else:
                                    # Gap found, save current range
                                    if current_start == current_end:
                                        ranges.append(f"video{current_start:03d}")
                                    else:
                                        ranges.append(f"video{current_start:03d}-{current_end:03d}")
                                    current_start = video_numbers[i][0]
                                    current_end = video_numbers[i][0]
                            
                            # Add last range
                            if current_start == current_end:
                                ranges.append(f"video{current_start:03d}")
                            else:
                                ranges.append(f"video{current_start:03d}-{current_end:03d}")
                            
                            parts.extend(ranges)
                        
                        # Build final name
                        name_parts = [p for p in parts if p]
                        if not name_parts:
                            # Fallback if no pattern found
                            base_name = video_names[0].rsplit('.', 1)[0] if video_names else "video"
                            creative_name = f"{base_name}_{len(video_names)}vids"
                        else:
                            video_part = ",".join(name_parts)
                            game_part = game_name or "game"
                            creative_name = f"{video_part}_{game_part}_flexible{suffix}"
                        
                        return creative_name
                    
                    video_names = [u["name"] for u in uploads]
                    creative_name = _generate_creative_name(video_names, game_name, name_suffix)
                
                # FIX: asset_feed_spec is a SIBLING of object_story_spec
                # Create creative with retry for video processing errors
                import time  # Import time for sleep in retry logic
                max_retries = 3
                retry_delay = 10
                creative = None
                for attempt in range(max_retries):
                    try:
                        creative = account.create_ad_creative(params={
                            "name": creative_name,
                            "object_story_spec": object_story_spec,
                            "asset_feed_spec": asset_feed_spec
                        })
                        break  # Success
                    except FacebookRequestError as e:
                        error_subcode = None
                        try:
                            error_subcode = e.api_error_subcode()
                        except:
                            pass
                        
                        # Check if it's a video processing error (1885252)
                        if error_subcode == 1885252 and attempt < max_retries - 1:
                            logger.warning(f"Video not ready yet for {creative_name}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})...")
                            time.sleep(retry_delay)
                            retry_delay *= 2  # Exponential backoff
                            continue
                        else:
                            raise  # Re-raise if not retryable or last attempt
                
                # Create ONE Ad
                ad_name = make_ad_name(f"Flexible_{len(uploads)}Items", ad_name_prefix)
                account.create_ad(params={
                    "name": ad_name,
                    "adset_id": adset_id,
                    "creative": {"creative_id": creative["id"]},
                    "status": Ad.Status.active,
                })

                results.append({"name": creative_name, "creative_id": creative["id"]})
            # ✅ 추가: 상세 에러 정보
            except FacebookRequestError as e:
                error_msg = f"**{base_name}** 광고 생성 실패:\n"
                error_msg += f"  - Error Code: {e.api_error_code()}\n"
                error_msg += f"  - Error Subcode: {e.api_error_subcode()}\n"
                error_msg += f"  - Error Type: {e.api_error_type()}\n"
                error_msg += f"  - Message: {e.api_error_message()}\n"
                
                # Get full response body if available
                if e.body():
                    try:
                        import json
                        body = json.loads(e.body()) if isinstance(e.body(), str) else e.body()
                        if "error" in body:
                            error_info = body["error"]
                            if "error_user_title" in error_info:
                                error_msg += f"  - User Title: {error_info['error_user_title']}\n"
                            if "error_user_msg" in error_info:
                                error_msg += f"  - User Message: {error_info['error_user_msg']}\n"
                    except:
                        pass
                
                api_errors.append(error_msg)
                logger.error(f"Facebook API error for {base_name}: {error_msg}")
                
            except Exception as e:
                error_msg = f"**{base_name}** 예상치 못한 에러:\n  - {str(e)}\n  - Type: {type(e).__name__}"
                api_errors.append(error_msg)
                logger.error(f"Unexpected error for {base_name}: {e}", exc_info=True)

            # Display errors
            if api_errors:
                st.error("⚠️ **광고 생성 중 에러 발생**\n\n" + "\n\n".join(api_errors))
                logger.error(f"Total {len(api_errors)} ads failed")

            # Debug info
            st.write(f"**생성 결과**: {len(results)}개 성공, {len(api_errors)}개 실패")
            logger.info(f"Ad creation complete: {len(results)} succeeded, {len(api_errors)} failed")

            return results

    # -------------------------------------------------------------------------
    # BRANCH B: SINGLE FORMAT (Standard) - 1 Creative per Video Group (3 sizes)
    # -------------------------------------------------------------------------
    # -------------------------------------------------------------------------
# BRANCH B: SINGLE FORMAT - 기존 Creative 완벽 복제
# -------------------------------------------------------------------------
        else:
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
            
            # 2. Upload all videos with thumbnails
            uploads = []
            total = sum(len(group) for group in valid_groups_pre.values())
            progress = st.progress(0, text="Uploading videos with thumbnails (Marketer Mode)...")
            
            def _upload_one_with_thumbnail(item, base_name, size):
                thumbnail_path = None
                thumbnail_url = None
                
                try:
                    thumbnail_path = extract_thumbnail_from_video(item["path"])
                    thumbnail_url = upload_thumbnail_image(account, thumbnail_path)
                    logger.info(f"Extracted and uploaded thumbnail for {item['name']}: {thumbnail_url}")
                except Exception as e:
                    logger.warning(f"Failed to extract/upload thumbnail for {item['name']}: {e}. Continuing without thumbnail.")
                
                video_id = _upload_video_resumable(account, item["path"])
                
                if thumbnail_path:
                    import os
                    try:
                        if os.path.exists(thumbnail_path):
                            os.unlink(thumbnail_path)
                    except Exception:
                        pass
                
                return {
                    "name": item["name"],
                    "video_id": video_id,
                    "base_name": base_name,
                    "size": size,
                    "thumbnail_url": thumbnail_url
                }
            
            # Upload in parallel
            done = 0
            if total:
                with ThreadPoolExecutor(max_workers=6) as ex:
                    future_to_item = {}
                    for base_name, group in valid_groups_pre.items():
                        for size, item in group.items():
                            fut = ex.submit(_upload_one_with_thumbnail, item, base_name, size)
                            future_to_item[fut] = (base_name, size, item["name"])
                    
                    for fut in as_completed(future_to_item):
                        base_name, size, name = future_to_item[fut]
                        try:
                            res = fut.result()
                            uploads.append(res)
                            done += 1
                            if progress is not None:
                                pct = int(done / total * 100)
                                progress.progress(pct, text=f"Uploading {done}/{total} videos…")
                        except Exception as e:
                            st.error(f"Upload failed for {name}: {e}")
            
            progress.empty()
            
            # 디버깅: 업로드 결과 확인
            logger.info(f"Video upload complete: {len(uploads)} videos uploaded")
            st.write(f"### 📤 비디오 업로드 완료")
            st.write(f"**업로드된 비디오 수**: {len(uploads)}개")
            if uploads:
                st.write("**업로드된 비디오 목록 (최대 5개)**:")
                for i, up in enumerate(uploads[:5], 1):
                    st.write(f"  {i}. {up.get('name')} → base_name: {up.get('base_name')}, size: {up.get('size')}, video_id: {up.get('video_id')}")
                if len(uploads) > 5:
                    st.write(f"  ... 외 {len(uploads) - 5}개")
            else:
                st.error("⚠️ **비디오가 업로드되지 않았습니다!**")
                logger.error("No videos were uploaded successfully")
                return []  # Early return if no uploads
            
            # Wait for videos to be ready
            logger.info(f"Waiting for {len(uploads)} videos to be ready...")
            video_ids = [u["video_id"] for u in uploads]
            ready_videos = _wait_for_videos_ready(account, video_ids, timeout_s=300)
            
            if not ready_videos:
                logger.warning(f"None of the {len(video_ids)} videos became ready within timeout.")
            else:
                not_ready = [vid for vid in video_ids if vid not in ready_videos]
                if not_ready:
                    logger.warning(f"{len(not_ready)} videos are not ready yet: {not_ready}.")
                logger.info(f"Proceeding to create creatives for {len(uploads)} videos ({len(ready_videos)}/{len(video_ids)} ready)")

            # 3. Group uploaded videos by base name
            video_groups: dict[str, dict[str, dict]] = {}
            for up in uploads:
                base_name = up.get("base_name") or _get_base_name(up["name"])
                size = up.get("size") or _get_video_size(up["name"])
                if not size:
                    logger.warning(f"Video {up.get('name')} has no recognized size, skipping")
                    continue
                if base_name not in video_groups:
                    video_groups[base_name] = {}
                video_groups[base_name][size] = up
            
            # 디버깅: 그룹화 결과 확인
            logger.info(f"Video groups after processing: {len(video_groups)} groups")
            for base_name, sizes_dict in video_groups.items():
                logger.info(f"  - {base_name}: {len(sizes_dict)} sizes - {list(sizes_dict.keys())}")
            
            # Display video groups (after grouping is complete)
            st.write("### 🎬 업로드된 비디오 그룹")
            st.write(f"**총 그룹 수**: {len(video_groups)}개")
            st.write(f"**업로드된 비디오 수**: {len(uploads)}개")

            if not video_groups:
                error_msg = "⚠️ **비디오 그룹이 생성되지 않았습니다!**\n\n"
                error_msg += "**가능한 원인**:\n"
                error_msg += "1. 업로드된 비디오 파일명에 사이즈 정보(1080x1080, 1920x1080, 1080x1920)가 없습니다\n"
                error_msg += "2. 모든 비디오가 사이즈 추출에 실패했습니다\n\n"
                error_msg += "**업로드 정보**:\n"
                for i, up in enumerate(uploads, 1):
                    error_msg += f"  {i}. {up.get('name')} → base_name: {up.get('base_name')}, size: {up.get('size')}\n"
                st.error(error_msg)
                logger.error(f"No video groups created from {len(uploads)} uploads")
                return []

            for base_name, sizes_dict in video_groups.items():
                st.write(f"**{base_name}**:")
                for size, up_info in sizes_dict.items():
                    st.write(f"  - {size}: video_id=`{up_info['video_id']}`")
                
                # Check if group has all 3 sizes
                if len(sizes_dict) != 3:
                    st.warning(f"⚠️ {base_name}은(는) 3개 사이즈가 필요한데 {len(sizes_dict)}개만 있습니다!")

            # 4. Get reference Creative ID
            st.write("### 🔍 Reference Creative 검색 중...")
            st.write(f"**Target Ad Set ID**: `{adset_id}`")

            reference_creative_id = None
            max_retries = 3
            retry_delay = 5

            for attempt in range(max_retries):
                try:
                    logger.info(f"[Attempt {attempt + 1}/{max_retries}] Fetching ads from Ad Set: {adset_id}")
                    
                    adset = AdSet(adset_id)
                    
                    # Step 1: Get ALL ads first (no status filter) to see what's there
                    all_ads = adset.get_ads(
                        fields=[Ad.Field.id, Ad.Field.name, Ad.Field.status, Ad.Field.effective_status, Ad.Field.created_time],
                        params={"limit": 50}
                    )
                    
                    all_ads_list = list(all_ads)
                    logger.info(f"Found {len(all_ads_list)} total ads in Ad Set {adset_id}")
                    
                    # Show what we found
                    if all_ads_list:
                        st.write(f"**발견된 광고**: {len(all_ads_list)}개")
                        
                        # Count by status
                        status_counts = {}
                        for ad in all_ads_list:
                            status = ad.get("effective_status", "UNKNOWN")
                            status_counts[status] = status_counts.get(status, 0) + 1
                        
                        st.write("**상태별 광고 수**:")
                        for status, count in status_counts.items():
                            st.write(f"  - {status}: {count}개")
                        
                        # Show first 5 ads
                        st.write("**광고 샘플 (최대 5개)**:")
                        for i, ad in enumerate(all_ads_list[:5]):
                            st.write(f"  {i+1}. ID: `{ad['id']}` | Status: `{ad.get('effective_status')}` | Name: `{ad.get('name', 'N/A')}`")
                    else:
                        st.warning(f"⚠️ Ad Set `{adset_id}`에 광고가 하나도 없습니다!")
                        logger.warning(f"No ads found in Ad Set {adset_id}")
                    
                    # Step 2: Filter for ACTIVE ads and get creative
                    active_ads = [a for a in all_ads_list if a.get("effective_status") == "ACTIVE"]
                    
                    if not active_ads:
                        logger.warning(f"No ACTIVE ads found in ad set {adset_id} (attempt {attempt + 1}/{max_retries})")
                        # status_counts is only defined if all_ads_list is not empty
                        if all_ads_list:
                            logger.info(f"Available statuses: {list(status_counts.keys())}")
                        
                        if attempt < max_retries - 1:
                            st.info(f"재시도 중... ({attempt + 1}/{max_retries})")
                            time.sleep(retry_delay)
                            continue
                        break
                    
                    # Step 3: Get creative for the most recent active ad
                    st.write(f"**Active 광고**: {len(active_ads)}개 발견!")
                    
                    # Sort by created_time (newest first)
                    active_ads_sorted = sorted(active_ads, key=lambda x: x.get("created_time", ""), reverse=True)
                    reference_ad = active_ads_sorted[0]
                    
                    # Fetch the creative
                    reference_ad_obj = Ad(reference_ad["id"])
                    reference_ad_data = reference_ad_obj.api_get(fields=[Ad.Field.creative])
                    reference_creative_id = reference_ad_data["creative"]["id"]
                    
                    logger.info(f"✅ Reference Creative ID: {reference_creative_id} (from ad {reference_ad['id']})")
                    st.success(f"✅ **템플릿 광고 찾음!**")
                    st.write(f"  - Ad ID: `{reference_ad['id']}`")
                    st.write(f"  - Ad Name: `{reference_ad.get('name', 'N/A')}`")
                    st.write(f"  - Creative ID: `{reference_creative_id}`")
                    
                    break  # Success!
                    
                except FacebookRequestError as e:
                    error_code = e.api_error_code()
                    error_subcode = e.api_error_subcode()
                    
                    logger.error(f"Facebook API Error: Code={error_code}, Subcode={error_subcode}, Message={e.api_error_message()}")
                    st.error(f"❌ Facebook API 에러: {e.api_error_message()}")
                    
                    if _is_rate_limit_error(e) and attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt)
                        logger.warning(f"Rate limit hit, retrying in {wait_time}s...")
                        st.warning(f"⏳ Rate limit 에러. {wait_time}초 후 재시도...")
                        time.sleep(wait_time)
                        continue
                    else:
                        break
                        
                except Exception as e:
                    logger.error(f"Unexpected error fetching reference creative: {e}", exc_info=True)
                    st.error(f"❌ 예상치 못한 에러: {str(e)}")
                    break

            # Final check
            if not reference_creative_id:
                st.error(
                    f"⚠️ **Ad Set에서 Active 광고를 찾을 수 없습니다**\n\n"
                    f"**Ad Set ID**: `{adset_id}`\n\n"
                    f"**확인사항**:\n"
                    f"1. Facebook Ads Manager에서 이 Ad Set ID가 맞는지 확인\n"
                    f"2. 이 Ad Set에 정말 ACTIVE 상태의 광고가 있는지 확인\n"
                    f"3. Access Token 권한 확인\n\n"
                    f"**다음 단계**:\n"
                    f"- 위의 디버깅 정보를 확인하여 실제 광고 상태를 파악하세요\n"
                    f"- Ad Set에 Active 광고가 없다면, 수동으로 1개 생성 후 재시도하세요"
                )
                logger.error(f"Failed to find reference creative after {max_retries} attempts")
                return []

            # 5. Create creatives by cloning reference
            results = []
            api_errors = []
            
            creative_title_override = creative_name_manual
            
            # Debug: Log video groups info
            logger.info(f"Starting creative creation for {len(video_groups)} video groups")
            st.write(f"### 🎯 Creative 생성 시작")
            st.write(f"**비디오 그룹 수**: {len(video_groups)}개")
            
            if not video_groups:
                error_msg = "⚠️ **비디오 그룹이 없습니다!**\n\n"
                error_msg += "Creative를 생성할 수 없습니다."
                st.error(error_msg)
                logger.error("No video groups to create creatives for")
                return []
            
            for base_name, videos_by_size in video_groups.items():
                logger.info(f"Processing video group: {base_name} with {len(videos_by_size)} sizes")
                st.write(f"**처리 중**: {base_name} ({len(videos_by_size)}개 사이즈)")
                
                if len(videos_by_size) != 3:
                    error_msg = f"{base_name}: 3개 사이즈가 필요합니다 (현재 {len(videos_by_size)}개)"
                    api_errors.append(error_msg)
                    logger.warning(error_msg)
                    st.warning(f"⚠️ {error_msg}")
                    continue
                
                video_ids = {
                    "1080x1080": videos_by_size["1080x1080"]["video_id"],
                    "1920x1080": videos_by_size["1920x1080"]["video_id"],
                    "1080x1920": videos_by_size["1080x1920"]["video_id"],
                }
                
                thumbnails = {
                    "1080x1080": videos_by_size["1080x1080"].get("thumbnail_url"),
                    "1920x1080": videos_by_size["1920x1080"].get("thumbnail_url"),
                    "1080x1920": videos_by_size["1080x1920"].get("thumbnail_url"),
                }
                
                final_title = creative_title_override if creative_title_override else base_name
                
                try:
                    new_creative_id = _clone_creative_with_new_video(
                        account=account,
                        reference_creative_id=reference_creative_id,
                        video_ids_by_size=video_ids,
                        thumbnails_by_size=thumbnails,
                        new_creative_name=final_title,
                        store_url=store_url
                    )
                    
                    ad = account.create_ad(params={
                        "name": make_ad_name(final_title, ad_name_prefix),
                        "adset_id": adset_id,
                        "creative": {"creative_id": new_creative_id},
                        "status": Ad.Status.active,
                    })
                    
                    results.append({
                        "name": final_title,
                        "ad_id": ad["id"],
                        "creative_id": new_creative_id
                    })
                    
                # ✅ 수정 후
                except FacebookRequestError as e:
                    error_msg = f"{base_name}:\n"
                    error_msg += f"  Message: {e.api_error_message()}\n"
                    error_msg += f"  Code: {e.api_error_code()}\n"
                    error_msg += f"  Subcode: {e.api_error_subcode()}\n"
                    error_msg += f"  Type: {e.api_error_type()}\n"
                    if e.body():
                        error_msg += f"  Response: {e.body()}"
                    api_errors.append(error_msg)
                    logger.error(f"Failed to create ad for {base_name}: {error_msg}")
                except Exception as e:
                    error_msg = f"{base_name}: {str(e)}"
                    api_errors.append(error_msg)
                    logger.error(f"Unexpected error for {base_name}: {e}")

            # Display errors and results after processing all groups
            if api_errors:
                error_display = "⚠️ **광고 생성 실패**\n\n" + "\n\n".join(api_errors)
                st.error(error_display)
                logger.error(f"Total {len(api_errors)} ads failed to create")

            # 디버깅 정보 추가
            logger.info(f"Created {len(results)} ads successfully, {len(api_errors)} failed")
            st.write(f"**결과**: {len(results)}개 성공, {len(api_errors)}개 실패")
            
            # Debug: Log detailed info when no results
            if not results:
                error_summary = f"⚠️ **모든 광고 생성 실패**\n\n"
                error_summary += f"- 시도한 그룹 수: {len(video_groups)}\n"
                error_summary += f"- 실패 수: {len(api_errors)}\n"
                error_summary += f"- Reference Creative ID: {reference_creative_id or 'N/A'}\n\n"
                
                # Log video groups details
                error_summary += "**비디오 그룹 상세**:\n"
                for base_name, sizes_dict in video_groups.items():
                    error_summary += f"- {base_name}: {len(sizes_dict)}개 사이즈\n"
                    for size, up_info in sizes_dict.items():
                        error_summary += f"  - {size}: video_id={up_info.get('video_id', 'N/A')}\n"
                
                if api_errors:
                    error_summary += "\n**에러 내역**:\n" + "\n\n".join(api_errors)
                else:
                    error_summary += "\n**에러 내역이 기록되지 않았습니다.**\n"
                    error_summary += "가능한 원인:\n"
                    error_summary += "1. 모든 그룹이 3개 사이즈를 가지지 않음\n"
                    error_summary += "2. Creative 생성 중 예외가 발생했지만 로그되지 않음\n"
                    error_summary += "3. Reference Creative를 찾지 못함\n"
                
                st.error(error_summary)
                logger.error(f"All {len(video_groups)} video groups failed to create ads. Errors: {api_errors}")
                logger.error(f"Video groups details: {[(name, len(sizes)) for name, sizes in video_groups.items()]}")

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

    # 4. Cleanup Logic (모든 Creative Type에 적용)
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
    
    # ✅ 추가
    # Show what we found (even if empty strings)
    h_preview = f"'{headlines_found[0]}'" if headlines_found else "(없음)"
    m_preview = f"'{messages_found[0][:50]}...'" if messages_found and len(messages_found[0]) > 50 else (f"'{messages_found[0]}'" if messages_found else "(없음)")

    st.info(f"📋 Reference Creative에서 설정 복사:\n"
            f"- Headlines: {len(headlines_found)}개 - {h_preview}\n"
            f"- Messages: {len(messages_found)}개 - {m_preview}\n"
            f"- Creative ID: {template_data.get('creative_id', 'N/A')}")

    # Warning only if both are truly missing (empty list, not empty string)
    if not headlines_found and not messages_found:
        st.warning("⚠️ Headline과 Message가 비어있습니다. 다른 설정(CTA, URL 등)은 복사됩니다.")
    ad_name_prefix = (
        settings.get("ad_name_prefix") if settings.get("ad_name_mode") == "Prefix + filename" else None
    )
    store_url = (settings.get("store_url") or "").strip()

    # 6. Upload

    # Determine mode flag
    is_flexible = (creative_type == "다이나믹")
    target_ratio_val = settings.get("dco_aspect_ratio") if is_flexible else None
    if is_flexible:
        manual_creative_name = settings.get("dco_creative_name")
    else:
        manual_creative_name = settings.get("single_creative_name")

    results = upload_videos_create_ads_cloned(
        account=account,
        page_id=str(page_id),
        adset_id=target_adset_id,
        uploaded_files=uploaded_files,
        ad_name_prefix=ad_name_prefix,
        store_url=store_url,
        template_data=template_data,
        use_flexible_format=is_flexible,
        target_aspect_ratio=target_ratio_val,
        creative_name_manual=manual_creative_name,
        game_name=game_name
    )

    if not results:
        raise RuntimeError("광고 생성 실패: upload_videos_create_ads_cloned returned empty results")
    
    plan["adset_id"] = target_adset_id
    plan["created_ads_count"] = len(results)  # ← 추가 정보
    plan["created_ads"] = results  # ← 생성된 광고 목록
    
    return plan
