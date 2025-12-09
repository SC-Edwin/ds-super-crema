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
# 2. Template Data Fetcher (Clones from existing ads)
# -------------------------------------------------------------------------
@st.cache_data(ttl=0, show_spinner=False)
def fetch_reference_creative_data(account: AdAccount, adset_id: str) -> dict:
    """
    Fetch headline/text/CTA from an existing active ad in the adset.
    Returns a dict with 'headline', 'message', 'call_to_action' keys.
    """
    try:
        adset = AdSet(adset_id)
        ads = adset.get_ads(
            fields=[Ad.Field.id, Ad.Field.status, Ad.Field.effective_status],
            params={"effective_status": ["ACTIVE"], "limit": 1}
        )
        
        if not ads:
            return {}
        
        ad_id = ads[0]["id"]
        creative_id = Ad(ad_id).api_get(fields=[Ad.Field.creative])["creative"]["id"]
        creative = AdCreative(creative_id).api_get(
            fields=[
                AdCreative.Field.object_story_spec,
                AdCreative.Field.body,
                AdCreative.Field.title,
            ]
        )
        
        spec = creative.get("object_story_spec", {})
        video_data = spec.get("video_data", {})
        link_data = spec.get("link_data", {})
        
        headline = video_data.get("title") or link_data.get("name") or creative.get("title")
        message = video_data.get("message") or link_data.get("message") or creative.get("body")
        cta = video_data.get("call_to_action") or link_data.get("call_to_action")
        
        result = {}
        if headline:
            result["headline"] = [headline] if isinstance(headline, str) else headline
        if message:
            result["message"] = [message] if isinstance(message, str) else message
        if cta:
            result["call_to_action"] = cta
            
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
    if not page_id_key or page_id_key not in st.secrets:
        raise RuntimeError(f"Missing {page_id_key!r} in st.secrets")
    page_id = st.secrets[page_id_key]
    
    target_campaign_id = settings.get("campaign_id")
    target_adset_id = settings.get("adset_id")
    creative_type = settings.get("creative_type", "단일 영상")
    
    if not target_campaign_id:
        raise RuntimeError("캠페인이 선택되지 않았습니다.")
    if not target_adset_id:
        raise RuntimeError("광고 세트가 선택되지 않았습니다.")
    
    # Fetch template data
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
            "headline": single_headline,
            "message": single_message,
            "cta": cta_found,
            "aspect_ratio": settings.get("dco_aspect_ratio"),
        })
    else:
        # One creative per video
        for name in video_names:
            ad_name = make_ad_name(name, ad_name_prefix)
            preview_creatives.append({
                "name": name,
                "ad_name": ad_name,
                "type": "Single Video",
                "headline": single_headline,
                "message": single_message,
                "cta": cta_found,
            })
    
    # Get current ad count in adset
    try:
        adset = AdSet(target_adset_id)
        current_ads = adset.get_ads(
            fields=[Ad.Field.id],
            params={"effective_status": ["ACTIVE"], "limit": 100}
        )
        current_ad_count = len(current_ads)
    except Exception as e:
        logger.warning(f"Could not fetch current ads: {e}")
        current_ad_count = 0
    
    return {
        "campaign_id": target_campaign_id,
        "adset_id": target_adset_id,
        "page_id": str(page_id),
        "creative_type": creative_type,
        "n_videos": len(video_names),
        "current_ad_count": current_ad_count,
        "preview_creatives": preview_creatives,
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
    
    # Extract template data
    template = template_data or {}
    headlines_list = template.get("headline") or ["New Game"]
    messages_list = template.get("message") or []
    orig_cta = template.get("call_to_action")
    
    single_headline = headlines_list[0] if headlines_list else "New Game"
    single_message = messages_list[0] if messages_list else ""
    
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
            # Build asset_feed_spec
            video_assets = [{"video_id": u["video_id"]} for u in uploads]
            
            asset_feed_spec = {
                "video_assets": video_assets,
            }
            
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
    # BRANCH B: SINGLE FORMAT (Standard) - 1 Ad per Video
    # -------------------------------------------------------------------------
    else:
        def _create_ad_process(up):
            name = up["name"]
            vid = up["video_id"]
            
            try:
                from facebook_business.adobjects.advideo import AdVideo
                vinfo = AdVideo(vid).api_get(fields=["status", "thumbnails", "picture"])
                thumb = vinfo.get("picture")
                if not thumb:
                    # Try thumbnails array
                    thumbs = vinfo.get("thumbnails")
                    if thumbs and isinstance(thumbs, list) and len(thumbs) > 0:
                        thumb = thumbs[0].get("uri") if isinstance(thumbs[0], dict) else None
                
                # If still no thumbnail, wait and retry
                if not thumb:
                    logger.warning(f"Video {vid} ({name}) has no thumbnail, waiting 30s and retrying...")
                    time.sleep(30)
                    try:
                        vinfo = AdVideo(vid).api_get(fields=["status", "thumbnails", "picture"])
                        thumb = vinfo.get("picture")
                        if not thumb:
                            thumbs = vinfo.get("thumbnails")
                            if thumbs and isinstance(thumbs, list) and len(thumbs) > 0:
                                thumb = thumbs[0].get("uri") if isinstance(thumbs[0], dict) else None
                    except Exception as e:
                        logger.warning(f"Retry failed for video {vid}: {e}")
                
                if not thumb:
                    raise RuntimeError(f"Video {vid} ({name}) has no thumbnail after retry")
                
                # Fix CTA for Single Spec
                final_cta = None
                if orig_cta:
                     final_cta = orig_cta.copy()
                     if target_link and "value" in final_cta:
                         final_cta["value"]["link"] = target_link
                elif target_link:
                     final_cta = {"type": "INSTALL_MOBILE_APP", "value": {"link": target_link}}

                video_data = {
                    "video_id": vid,
                    "image_url": thumb,
                    "title": single_headline,    
                    "message": single_message,   
                    "call_to_action": final_cta 
                }
                spec = {"page_id": page_id, "video_data": video_data}
                if try_instagram and ig_actor_id: spec["instagram_actor_id"] = ig_actor_id
                    
                creative = account.create_ad_creative(params={"name": name, "object_story_spec": spec})
                ad = account.create_ad(params={
                    "name": make_ad_name(name, ad_name_prefix),
                    "adset_id": adset_id,
                    "creative": {"creative_id": creative["id"]},
                    "status": Ad.Status.active,
                })
                return {"name": name, "ad_id": ad["id"], "creative_id": creative["id"]}
            except Exception as e:
                return {"name": name, "error": str(e)}

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

        # 3. Create Ads (Branching Logic)
        results = []
        api_errors = []
        
        ig_actor_id = None
        try:
            from facebook_business.adobjects.page import Page
            p = Page(page_id).api_get(fields=["instagram_business_account"])
            ig_actor_id = p.get("instagram_business_account", {}).get("id")
        except: pass

        template = template_data or {}
        
        # Extract Lists
        headlines_list = template.get("headline") or ["New Game"]
        messages_list = template.get("message") or []
        
        # Extract Single Strings (for Branch B fallback)
        # This prevents the list-vs-string error
        single_headline = headlines_list[0] if headlines_list else "New Game"
        single_message = messages_list[0] if messages_list else ""

        # CTA Logic
        orig_cta = template.get("call_to_action")
        target_link = store_url

        with ThreadPoolExecutor(max_workers=5) as ex:
            future_to_video = {ex.submit(_create_ad_process, up): up for up in uploads}
            for fut in as_completed(future_to_video):
                res = fut.result()
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
    if not page_id_key or page_id_key not in st.secrets:
        raise RuntimeError(f"Missing {page_id_key!r} in st.secrets")
    page_id = st.secrets[page_id_key]
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
