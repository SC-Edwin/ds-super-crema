"""
Google Ads API Operations Module
App Campaign (UAC) 전용 — 영상/플레이어블 소재 관리
"""
from __future__ import annotations

import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta

import streamlit as st

logger = logging.getLogger(__name__)

# ── Google Ads Client ────────────────────────────────────────────────

def _get_client():
    """
    Initialize GoogleAdsClient from st.secrets.
    Expects [google_ads] section with:
      developer_token, client_id, client_secret, refresh_token, customer_id
    """
    from google.ads.googleads.client import GoogleAdsClient

    cfg = st.secrets["google_ads"]
    credentials = {
        "developer_token": cfg["developer_token"],
        "client_id": cfg["client_id"],
        "client_secret": cfg["client_secret"],
        "refresh_token": cfg["refresh_token"],
        "use_proto_plus": True,
    }
    # MCC (Manager) account requires login_customer_id header
    if cfg.get("login_customer_id"):
        credentials["login_customer_id"] = str(cfg["login_customer_id"]).replace("-", "")
    client = GoogleAdsClient.load_from_dict(credentials)
    return client


def _customer_id() -> str:
    """Return customer_id from secrets (stripped of dashes)."""
    raw = st.secrets["google_ads"]["customer_id"]
    return str(raw).replace("-", "")


# ── Query Helpers ────────────────────────────────────────────────────

def list_campaigns(game: str = None) -> List[Dict]:
    """
    Fetch all App campaigns (MULTI_CHANNEL or APP type).
    If game is provided, filters by game_mapping codename in campaign name.
    Returns list of {id, name, status, type}.
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")
    customer_id = _customer_id()

    query = """
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type
        FROM campaign
        WHERE campaign.advertising_channel_type = 'MULTI_CHANNEL'
        AND campaign.status = 'ENABLED'
        ORDER BY campaign.name
    """
    results = []
    try:
        response = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in response:
            for row in batch.results:
                results.append({
                    "id": str(row.campaign.id),
                    "name": row.campaign.name,
                    "status": row.campaign.status.name,
                    "type": row.campaign.advertising_channel_type.name,
                })
    except Exception as e:
        logger.error(f"Failed to list campaigns: {e}")
        raise

    # Filter by game codename if mapping exists
    if game:
        mapping = st.secrets.get("google_ads", {}).get("game_mapping", {})
        codename = mapping.get(game, "").lower()
        if codename:
            results = [c for c in results if codename in c["name"].lower()]

    return results


def list_ad_groups_with_spend(campaign_id: str, days: int = 7) -> List[Dict]:
    """
    Fetch all ad groups in a campaign with spend for the last N days.
    Returns list sorted by spend DESC:
      {id, name, status, spend_micros, spend, video_count}
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")
    customer_id = _customer_id()

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    query = f"""
        SELECT
            ad_group.id,
            ad_group.name,
            ad_group.status,
            metrics.cost_micros
        FROM ad_group
        WHERE campaign.id = {campaign_id}
        AND ad_group.status != 'REMOVED'
        AND segments.date BETWEEN '{start_date}' AND '{end_date}'
    """
    # Aggregate spend per ad group
    spend_map: Dict[str, Dict] = {}
    try:
        response = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in response:
            for row in batch.results:
                ag_id = str(row.ad_group.id)
                if ag_id not in spend_map:
                    spend_map[ag_id] = {
                        "id": ag_id,
                        "name": row.ad_group.name,
                        "status": row.ad_group.status.name,
                        "spend_micros": 0,
                    }
                spend_map[ag_id]["spend_micros"] += row.metrics.cost_micros
    except Exception as e:
        logger.error(f"Failed to list ad groups: {e}")
        raise

    results = list(spend_map.values())
    for r in results:
        r["spend"] = r["spend_micros"] / 1_000_000  # micros → dollars
    results.sort(key=lambda x: x["spend_micros"], reverse=True)

    # Fetch low-performing video counts per ad group
    low_counts = _get_low_performing_counts(campaign_id)
    for r in results:
        r["low_count"] = low_counts.get(r["id"], 0)

    return results


def _get_low_performing_counts(campaign_id: str) -> Dict[str, int]:
    """
    Count LOW-performing video assets per ad group using ad_group_ad_asset_view.
    Returns {ad_group_id: count_of_low_videos}.
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")
    customer_id = _customer_id()

    query = f"""
        SELECT
            ad_group.id,
            ad_group_ad_asset_view.performance_label,
            ad_group_ad_asset_view.field_type
        FROM ad_group_ad_asset_view
        WHERE campaign.id = {campaign_id}
        AND ad_group_ad_asset_view.field_type = 'YOUTUBE_VIDEO'
    """
    counts: Dict[str, int] = {}
    try:
        response = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in response:
            for row in batch.results:
                ag_id = str(row.ad_group.id)
                label = row.ad_group_ad_asset_view.performance_label.name
                if label == "LOW":
                    counts[ag_id] = counts.get(ag_id, 0) + 1
    except Exception as e:
        logger.warning(f"Failed to get low-performing counts: {e}")
    return counts


def list_ad_group_videos(campaign_id: str, ad_group_id: str) -> List[Dict]:
    """
    List video assets in an ad group's AppAd.
    Returns list of {asset_id, name, performance_label, youtube_video_id}.

    performance_label: BEST, GOOD, LOW, LEARNING, UNSPECIFIED, UNKNOWN
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")
    customer_id = _customer_id()

    query = f"""
        SELECT
            ad_group_ad.ad.id,
            ad_group_ad.ad.app_ad.videos,
            ad_group_ad.ad.app_ad.headlines,
            ad_group_ad.ad_strength
        FROM ad_group_ad
        WHERE ad_group.id = {ad_group_id}
        AND campaign.id = {campaign_id}
        AND ad_group_ad.status != 'REMOVED'
    """
    videos = []
    try:
        response = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in response:
            for row in batch.results:
                app_ad = row.ad_group_ad.ad.app_ad
                for video_asset in app_ad.videos:
                    asset_rn = video_asset.asset
                    videos.append({
                        "asset_resource_name": asset_rn,
                        "ad_id": str(row.ad_group_ad.ad.id),
                    })
    except Exception as e:
        logger.error(f"Failed to list ad group videos: {e}")
        raise

    # Enrich with asset details + performance
    if videos:
        videos = _enrich_video_assets(videos)
    return videos


def _enrich_video_assets(videos: List[Dict]) -> List[Dict]:
    """Fetch asset name + performance label for each video asset."""
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")
    customer_id = _customer_id()

    resource_names = list({v["asset_resource_name"] for v in videos})
    # Build condition
    rn_list = ", ".join(f"'{rn}'" for rn in resource_names)

    query = f"""
        SELECT
            asset.resource_name,
            asset.name,
            asset.youtube_video_asset.youtube_video_id,
            asset.youtube_video_asset.youtube_video_title,
            asset.type
        FROM asset
        WHERE asset.resource_name IN ({rn_list})
    """
    asset_info: Dict[str, Dict] = {}
    try:
        response = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in response:
            for row in batch.results:
                yt_title = getattr(
                    row.asset.youtube_video_asset, "youtube_video_title", ""
                ) or ""
                asset_name = row.asset.name or ""
                display_name = yt_title if yt_title else asset_name
                asset_info[row.asset.resource_name] = {
                    "name": display_name,
                    "youtube_video_id": getattr(
                        row.asset.youtube_video_asset, "youtube_video_id", ""
                    ),
                    "youtube_video_title": yt_title,
                    "type": row.asset.type_.name,
                }
    except Exception as e:
        logger.warning(f"Failed to enrich video assets: {e}")

    # Also fetch performance labels via ad_group_asset
    perf_map = _get_asset_performance_labels(resource_names)

    for v in videos:
        rn = v["asset_resource_name"]
        info = asset_info.get(rn, {})
        v["name"] = info.get("name", "")
        v["youtube_video_id"] = info.get("youtube_video_id", "")
        v["youtube_video_title"] = info.get("youtube_video_title", "")
        v["performance_label"] = perf_map.get(rn, "UNSPECIFIED")
    return videos


def _get_asset_performance_labels(asset_resource_names: List[str]) -> Dict[str, str]:
    """
    Get performance labels for assets.
    Returns {asset_resource_name: "LOW" | "GOOD" | "BEST" | "LEARNING" | ...}
    """
    if not asset_resource_names:
        return {}

    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")
    customer_id = _customer_id()

    rn_list = ", ".join(f"'{rn}'" for rn in asset_resource_names)
    query = f"""
        SELECT
            ad_group_asset.asset,
            ad_group_asset.performance_label
        FROM ad_group_asset
        WHERE ad_group_asset.asset IN ({rn_list})
    """
    perf_map: Dict[str, str] = {}
    try:
        response = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in response:
            for row in batch.results:
                perf_map[row.ad_group_asset.asset] = (
                    row.ad_group_asset.performance_label.name
                )
    except Exception as e:
        logger.warning(f"Failed to get performance labels: {e}")
    return perf_map


# ── Asset Upload ─────────────────────────────────────────────────────

def upload_video_asset(video_bytes: bytes, display_name: str) -> str:
    """
    Upload a raw video file to Google Ads.
    Google auto-hosts it on the account's YouTube ad storage channel
    (내 동영상 광고 저장 채널).
    Returns the asset resource name.
    """
    client = _get_client()
    asset_service = client.get_service("AssetService")
    customer_id = _customer_id()

    asset_operation = client.get_type("AssetOperation")
    asset = asset_operation.create
    asset.name = display_name
    asset.type_ = client.enums.AssetTypeEnum.YOUTUBE_VIDEO
    asset.youtube_video_asset.youtube_video_id = ""  # empty = auto-upload
    asset.video_asset.data = video_bytes

    try:
        response = asset_service.mutate_assets(
            customer_id=customer_id,
            operations=[asset_operation],
        )
        resource_name = response.results[0].resource_name
        logger.info(f"Uploaded video asset: {resource_name} ({display_name})")
        return resource_name
    except Exception as e:
        logger.error(f"Failed to upload video asset: {e}")
        raise


def upload_video_asset_by_youtube_id(youtube_video_id: str, display_name: str) -> str:
    """
    Create a video asset referencing a YouTube video.
    Returns the asset resource name.
    """
    client = _get_client()
    asset_service = client.get_service("AssetService")
    customer_id = _customer_id()

    asset_operation = client.get_type("AssetOperation")
    asset = asset_operation.create
    asset.name = display_name
    asset.type_ = client.enums.AssetTypeEnum.YOUTUBE_VIDEO
    asset.youtube_video_asset.youtube_video_id = youtube_video_id

    try:
        response = asset_service.mutate_assets(
            customer_id=customer_id,
            operations=[asset_operation],
        )
        resource_name = response.results[0].resource_name
        logger.info(f"Created video asset: {resource_name} ({display_name})")
        return resource_name
    except Exception as e:
        logger.error(f"Failed to create video asset: {e}")
        raise


def upload_html5_asset(html5_bytes: bytes, display_name: str) -> str:
    """
    Upload an HTML5 (playable) asset.
    Returns the asset resource name.
    """
    client = _get_client()
    asset_service = client.get_service("AssetService")
    customer_id = _customer_id()

    asset_operation = client.get_type("AssetOperation")
    asset = asset_operation.create
    asset.name = display_name
    asset.type_ = client.enums.AssetTypeEnum.MEDIA_BUNDLE
    asset.media_bundle_asset.data = html5_bytes

    try:
        response = asset_service.mutate_assets(
            customer_id=customer_id,
            operations=[asset_operation],
        )
        resource_name = response.results[0].resource_name
        logger.info(f"Created HTML5 asset: {resource_name} ({display_name})")
        return resource_name
    except Exception as e:
        logger.error(f"Failed to create HTML5 asset: {e}")
        raise


# ── Category Auto-Detection ──────────────────────────────────────────

LANGUAGE_CODES = [
    "en", "fr", "jp", "cn", "kr", "th", "vn", "de", "es", "pt",
    "id", "tr", "ar", "ru", "it", "hi", "ms", "pl", "nl", "sv",
]


def _auto_detect_category(filename: str) -> str:
    """
    Detect creative category from filename.
    Returns: 'normal' | 'localized' | 'AI' | 'influencer'

    Rules:
    - Influencer: starts with "500-" or "500_"
    - AI: contains "eli" as a standalone underscore-separated segment
    - Localized: contains a short language code (en, fr, jp, ...) as a segment
    - Normal: default (video123 pattern etc.)
    """
    name = filename.lower()
    stem = name.rsplit(".", 1)[0]  # remove extension
    parts = stem.split("_")

    # Influencer: starts with "500-" or "500_"
    if stem.startswith("500-") or stem.startswith("500_"):
        return "influencer"

    # AI: contains "eli" as a standalone segment
    # Also check hyphen-separated parts (e.g., "some-eli-concept")
    all_parts = []
    for p in parts:
        all_parts.extend(p.split("-"))
    if "eli" in all_parts:
        return "AI"

    # Localized: contains a short language code as a segment
    if any(code in parts for code in LANGUAGE_CODES):
        return "localized"

    # Normal: default
    return "normal"


# ── Asset Library Queries ────────────────────────────────────────────

def list_video_assets(game_codename: str = None) -> List[Dict]:
    """
    Fetch YOUTUBE_VIDEO assets from the account's asset library.
    Optionally filter by game codename in asset name.
    Returns [{resource_name, name, youtube_video_id, category}]
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")
    customer_id = _customer_id()

    query = """
        SELECT
            asset.resource_name,
            asset.name,
            asset.youtube_video_asset.youtube_video_id,
            asset.youtube_video_asset.youtube_video_title,
            asset.type
        FROM asset
        WHERE asset.type = 'YOUTUBE_VIDEO'
    """
    results = []
    try:
        response = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in response:
            for row in batch.results:
                name = row.asset.name
                yt_title = getattr(
                    row.asset.youtube_video_asset, "youtube_video_title", ""
                ) or ""
                # youtube_video_title에 원본 파일명이 들어있으면 우선 사용
                display_name = yt_title if yt_title else name
                results.append({
                    "resource_name": row.asset.resource_name,
                    "name": display_name,
                    "youtube_video_id": getattr(
                        row.asset.youtube_video_asset, "youtube_video_id", ""
                    ),
                    "youtube_video_title": yt_title,
                    "category": _auto_detect_category(display_name),
                })
    except Exception as e:
        logger.error(f"Failed to list video assets: {e}")
        raise

    # Filter by game codename if provided
    if game_codename:
        codename = game_codename.lower()
        results = [a for a in results if codename in a["name"].lower()]

    return results


def _category_from_ad_group_name(ag_name: str) -> str:
    """Detect category from an ad group name."""
    lower = ag_name.lower()
    # Split on underscores, hyphens, AND spaces
    # e.g., "tier1_hybrid_AI assets #2" → ["tier1", "hybrid", "ai", "assets", "#2"]
    parts = lower.replace("-", "_").replace(" ", "_").split("_")
    if "ai" in parts or "eli" in parts:
        return "AI"
    if "influencer" in parts or "500" in parts:
        return "influencer"
    for code in LANGUAGE_CODES:
        if code in parts:
            return "localized"
    return "normal"


def list_campaign_video_assets(campaign_id: str) -> List[Dict]:
    """
    Fetch YOUTUBE_VIDEO assets linked to ad group ads within a campaign.
    Categorizes each video based on which ad group it belongs to.
    Returns [{resource_name, name, youtube_video_id, category, ad_groups}]
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")
    customer_id = _customer_id()

    query = f"""
        SELECT
            asset.resource_name,
            asset.name,
            asset.youtube_video_asset.youtube_video_id,
            asset.youtube_video_asset.youtube_video_title,
            ad_group.name
        FROM ad_group_ad_asset_view
        WHERE campaign.id = {campaign_id}
            AND asset.type = 'YOUTUBE_VIDEO'
    """
    # Track all ad groups per video (a video can be in multiple ad groups)
    video_map: Dict[str, Dict] = {}
    try:
        response = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in response:
            for row in batch.results:
                rn = row.asset.resource_name
                ag_name = row.ad_group.name
                if rn not in video_map:
                    asset_name = row.asset.name or ""
                    yt_id = getattr(
                        row.asset.youtube_video_asset, "youtube_video_id", ""
                    )
                    yt_title = getattr(
                        row.asset.youtube_video_asset, "youtube_video_title", ""
                    ) or ""
                    # youtube_video_title에 원본 파일명이 들어있으면 우선 사용
                    display_name = yt_title if yt_title else asset_name
                    video_map[rn] = {
                        "resource_name": rn,
                        "name": display_name,
                        "youtube_video_id": yt_id,
                        "youtube_video_title": yt_title,
                        "ad_groups": [],
                    }
                video_map[rn]["ad_groups"].append(ag_name)
    except Exception as e:
        logger.error(f"Failed to list campaign video assets: {e}")
        raise

    # Determine category from ad group names (not filename)
    results = []
    for vid in video_map.values():
        # Use the most specific category found among its ad groups
        categories_found = [_category_from_ad_group_name(ag) for ag in vid["ad_groups"]]
        # Priority: AI > influencer > localized > normal
        if "AI" in categories_found:
            vid["category"] = "AI"
        elif "influencer" in categories_found:
            vid["category"] = "influencer"
        elif "localized" in categories_found:
            vid["category"] = "localized"
        else:
            vid["category"] = "normal"
        results.append(vid)

    return results


def list_playable_assets(game_codename: str = None) -> List[Dict]:
    """
    Fetch MEDIA_BUNDLE assets from the account's asset library.
    Optionally filter by game codename in asset name.
    Returns [{resource_name, name, category}]
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")
    customer_id = _customer_id()

    query = """
        SELECT
            asset.resource_name,
            asset.name,
            asset.type
        FROM asset
        WHERE asset.type = 'MEDIA_BUNDLE'
    """
    results = []
    try:
        response = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in response:
            for row in batch.results:
                name = row.asset.name
                results.append({
                    "resource_name": row.asset.resource_name,
                    "name": name,
                    "category": _auto_detect_category(name),
                })
    except Exception as e:
        logger.error(f"Failed to list playable assets: {e}")
        raise

    # Filter by game codename if provided
    if game_codename:
        codename = game_codename.lower()
        results = [a for a in results if codename in a["name"].lower()]

    return results


def _ad_group_name_parts(name: str) -> List[str]:
    """Split ad group name into parts for keyword matching."""
    return name.lower().replace("-", "_").replace(" ", "_").split("_")


def filter_ad_groups_by_category(
    ad_groups: List[Dict], category: str
) -> List[Dict]:
    """
    Filter ad groups by category keyword in their name.
    Uses parts-based matching (split on _/- /space) to avoid false positives.
    - normal: all ad groups EXCEPT those matching localized/AI/influencer
    - localized: ad groups with language keywords in name
    - AI: ad groups with "ai" or "eli" in name parts
    - influencer: ad groups with "influencer" or "500" in name parts
    """
    def _is_ai(ag):
        parts = _ad_group_name_parts(ag["name"])
        return "ai" in parts or "eli" in parts

    def _is_influencer(ag):
        parts = _ad_group_name_parts(ag["name"])
        return "influencer" in parts or "500" in parts

    def _is_localized(ag):
        parts = _ad_group_name_parts(ag["name"])
        localized_keywords = LANGUAGE_CODES + ["localized", "local"]
        return any(kw in parts for kw in localized_keywords)

    if category == "normal":
        return [
            ag for ag in ad_groups
            if not _is_ai(ag) and not _is_influencer(ag) and not _is_localized(ag)
        ]
    elif category == "localized":
        return [ag for ag in ad_groups if _is_localized(ag)]
    elif category == "AI":
        return [ag for ag in ad_groups if _is_ai(ag)]
    elif category == "influencer":
        return [ag for ag in ad_groups if _is_influencer(ag)]
    return ad_groups


# ── Ad Group Ad Mutation (AppAd) ─────────────────────────────────────

def get_app_ad_resource(campaign_id: str, ad_group_id: str) -> Optional[Dict]:
    """
    Get the AppAd resource for an ad group.
    Returns {ad_resource_name, video_assets: [...], html5_assets: [...]}.
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")
    customer_id = _customer_id()

    query = f"""
        SELECT
            ad_group_ad.resource_name,
            ad_group_ad.ad.resource_name,
            ad_group_ad.ad.app_ad.videos,
            ad_group_ad.ad.app_ad.html5_media_bundles
        FROM ad_group_ad
        WHERE ad_group.id = {ad_group_id}
        AND campaign.id = {campaign_id}
        AND ad_group_ad.status != 'REMOVED'
        LIMIT 1
    """
    try:
        response = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in response:
            for row in batch.results:
                app_ad = row.ad_group_ad.ad.app_ad
                return {
                    "ad_group_ad_resource_name": row.ad_group_ad.resource_name,
                    "ad_resource_name": row.ad_group_ad.ad.resource_name,
                    "video_assets": [v.asset for v in app_ad.videos],
                    "html5_assets": [h.asset for h in app_ad.html5_media_bundles],
                }
    except Exception as e:
        logger.error(f"Failed to get AppAd: {e}")
        raise
    return None


def mutate_app_ad_videos(
    ad_resource_name: str,
    new_video_assets: List[str],
) -> bool:
    """
    Replace ALL video assets in an AppAd with the given list.
    ad_resource_name: the ad's resource name
    new_video_assets: list of asset resource names to set
    Returns True on success.
    """
    client = _get_client()
    ad_service = client.get_service("AdService")
    customer_id = _customer_id()

    ad_operation = client.get_type("AdOperation")
    ad = ad_operation.update
    ad.resource_name = ad_resource_name

    # Clear and re-set videos
    for asset_rn in new_video_assets:
        ad_info = client.get_type("AdVideoAsset")
        ad_info.asset = asset_rn
        ad.app_ad.videos.append(ad_info)

    # Set field mask to only update videos
    field_mask = client.get_type("FieldMask")
    field_mask.paths.append("app_ad.videos")
    ad_operation.update_mask.CopyFrom(field_mask)

    try:
        ad_service.mutate_ads(
            customer_id=customer_id,
            operations=[ad_operation],
        )
        logger.info(f"Updated AppAd videos: {ad_resource_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to mutate AppAd videos: {e}")
        raise


def add_playable_to_app_ad(
    ad_resource_name: str,
    current_html5_assets: List[str],
    new_html5_asset: str,
) -> bool:
    """
    Add an HTML5 (playable) asset to an AppAd.
    Preserves existing playables.
    """
    all_html5 = list(current_html5_assets) + [new_html5_asset]

    client = _get_client()
    ad_service = client.get_service("AdService")
    customer_id = _customer_id()

    ad_operation = client.get_type("AdOperation")
    ad = ad_operation.update
    ad.resource_name = ad_resource_name

    for asset_rn in all_html5:
        bundle = client.get_type("AdMediaBundleAsset")
        bundle.asset = asset_rn
        ad.app_ad.html5_media_bundles.append(bundle)

    field_mask = client.get_type("FieldMask")
    field_mask.paths.append("app_ad.html5_media_bundles")
    ad_operation.update_mask.CopyFrom(field_mask)

    try:
        ad_service.mutate_ads(
            customer_id=customer_id,
            operations=[ad_operation],
        )
        logger.info(f"Added playable to AppAd: {ad_resource_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to add playable: {e}")
        raise


# ── Distribution Logic ───────────────────────────────────────────────

def distribute_videos(
    campaign_id: str,
    new_video_assets: List[str],
    exception_map: Dict[str, str],
    ad_groups: Optional[List[Dict]] = None,
) -> Dict:
    """
    Main orchestration: distribute new videos across ad groups.

    Args:
        campaign_id: Google Ads campaign ID
        new_video_assets: ordered list of asset resource names (highest priority first)
        exception_map: {asset_resource_name: "localized" | "AI" | "influencer" | None}
        ad_groups: optional pre-fetched ad groups (sorted by spend DESC)

    Returns:
        {
            "actions": [
                {
                    "ad_group_id": str,
                    "ad_group_name": str,
                    "removed": [asset_resource_name, ...],
                    "added": [asset_resource_name, ...],
                    "type": "normal" | "exception"
                },
                ...
            ],
            "unplaced": [asset_resource_name, ...]  # videos that couldn't be placed
        }
    """
    if ad_groups is None:
        ad_groups = list_ad_groups_with_spend(campaign_id)

    actions = []
    remaining = list(new_video_assets)

    # Step 1: Separate exception vs normal videos
    exception_videos: Dict[str, List[str]] = {}  # type → [asset_rns]
    normal_videos = []
    for asset_rn in new_video_assets:
        tag = exception_map.get(asset_rn)
        if tag:
            exception_videos.setdefault(tag, []).append(asset_rn)
        else:
            normal_videos.append(asset_rn)

    # Step 2: Handle exception videos
    exception_ag_ids = set()
    for exc_type, exc_assets in exception_videos.items():
        # Find ad group whose name contains the exception keyword
        matching_ags = [
            ag for ag in ad_groups
            if exc_type.lower() in ag["name"].lower()
        ]
        if not matching_ags:
            logger.warning(f"No ad group found for exception type '{exc_type}'")
            continue

        target_ag = matching_ags[0]
        exception_ag_ids.add(target_ag["id"])

        # Get current videos in this ad group
        current_videos = list_ad_group_videos(campaign_id, target_ag["id"])
        low_performers = [
            v for v in current_videos
            if v.get("performance_label") == "LOW"
        ]

        to_replace = min(len(low_performers), len(exc_assets))
        removed = [v["asset_resource_name"] for v in low_performers[:to_replace]]
        added = exc_assets[:to_replace]

        # Build new video list
        current_rns = [v["asset_resource_name"] for v in current_videos]
        new_rns = [rn for rn in current_rns if rn not in removed] + added

        actions.append({
            "ad_group_id": target_ag["id"],
            "ad_group_name": target_ag["name"],
            "removed": removed,
            "added": added,
            "new_video_list": new_rns,
            "type": "exception",
            "exception_type": exc_type,
        })

        # Remove placed videos from remaining
        for rn in added:
            if rn in remaining:
                remaining.remove(rn)

    # Step 3: Handle normal videos (spend-based distribution)
    normal_remaining = [rn for rn in normal_videos if rn in remaining]

    for ag in ad_groups:
        if not normal_remaining:
            break
        if ag["id"] in exception_ag_ids:
            continue

        current_videos = list_ad_group_videos(campaign_id, ag["id"])
        low_performers = [
            v for v in current_videos
            if v.get("performance_label") == "LOW"
        ]

        to_replace = min(len(low_performers), len(normal_remaining))
        if to_replace == 0:
            continue

        removed = [v["asset_resource_name"] for v in low_performers[:to_replace]]
        added = normal_remaining[:to_replace]

        current_rns = [v["asset_resource_name"] for v in current_videos]
        new_rns = [rn for rn in current_rns if rn not in removed] + added

        actions.append({
            "ad_group_id": ag["id"],
            "ad_group_name": ag["name"],
            "removed": removed,
            "added": added,
            "new_video_list": new_rns,
            "type": "normal",
        })

        normal_remaining = normal_remaining[to_replace:]
        for rn in added:
            if rn in remaining:
                remaining.remove(rn)

    return {
        "actions": actions,
        "unplaced": remaining,
    }


def execute_distribution(campaign_id: str, plan: Dict) -> Dict:
    """
    Execute a distribution plan (from distribute_videos).
    Returns {success: int, failed: int, errors: [str]}.
    """
    success = 0
    failed = 0
    errors = []

    for action in plan["actions"]:
        ag_id = action["ad_group_id"]
        try:
            app_ad = get_app_ad_resource(campaign_id, ag_id)
            if not app_ad:
                errors.append(f"No AppAd found for ad group {ag_id}")
                failed += 1
                continue

            mutate_app_ad_videos(
                app_ad["ad_resource_name"],
                action["new_video_list"],
            )
            success += 1
            logger.info(
                f"Ad group {action['ad_group_name']}: "
                f"removed {len(action['removed'])}, added {len(action['added'])}"
            )
        except Exception as e:
            errors.append(f"Ad group {action.get('ad_group_name', ag_id)}: {e}")
            failed += 1

    return {"success": success, "failed": failed, "errors": errors}


def add_playable_to_all_ad_groups(
    campaign_id: str, playable_asset_rn: str
) -> Dict:
    """
    Add a playable (HTML5) asset to ALL ad groups in a campaign.
    Returns {success: int, failed: int, errors: [str]}.

    DEPRECATED: prefer per-category distribution via ga.py category tabs.
    """
    ad_groups = list_ad_groups_with_spend(campaign_id)
    success = 0
    failed = 0
    errors = []

    for ag in ad_groups:
        try:
            app_ad = get_app_ad_resource(campaign_id, ag["id"])
            if not app_ad:
                errors.append(f"No AppAd found for ad group {ag['name']}")
                failed += 1
                continue

            add_playable_to_app_ad(
                app_ad["ad_resource_name"],
                app_ad["html5_assets"],
                playable_asset_rn,
            )
            success += 1
        except Exception as e:
            errors.append(f"Ad group {ag['name']}: {e}")
            failed += 1

    return {"success": success, "failed": failed, "errors": errors}
