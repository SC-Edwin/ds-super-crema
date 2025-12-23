"""Applovin helpers for Creative 자동 업로드.

- Lets the user pick:
  1) Campaign selection
  2) Creative upload settings

- TODO: Implement Applovin API integration
"""

from __future__ import annotations
from typing import Dict, List
import logging
import streamlit as st

import requests
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed


logger = logging.getLogger(__name__)


APPLOVIN_BASE_URL = "https://api.ads.axon.ai/manage/v1"

def _get_api_config():
    """Get Applovin API configuration from secrets."""
    return {
        "api_key": st.secrets["applovin"]["campaign_management_api_key"],
        "account_id": st.secrets["applovin"]["account_id"],
        "game_mapping": dict(st.secrets["applovin"].get("game_mapping", {}))
    }

# =========================================================
# Settings State Management
# =========================================================

def _ensure_applovin_settings_state():
    """Initialize Applovin settings in session state."""
    if "applovin_settings" not in st.session_state:
        st.session_state.applovin_settings = {}

def get_applovin_settings(game: str) -> Dict:
    """Get Applovin settings for a game."""
    _ensure_applovin_settings_state()
    return st.session_state.applovin_settings.get(game, {})

# =========================================================
# API Functions
# =========================================================

@st.cache_data(ttl=300)  # 5분 캐시
def get_campaigns(game: str = None) -> List[Dict]:
    """
    Fetch all LIVE campaigns with parallel requests (cached).
    """
    try:
        config = _get_api_config()
        headers = {"Authorization": config["api_key"]}
        account_id = config["account_id"]
        
        # 먼저 첫 페이지로 전체 페이지 수 추정
        params = {"account_id": account_id, "page": 1, "size": 100}
        response = requests.get(
            f"{APPLOVIN_BASE_URL}/campaign/list",
            headers=headers,
            params=params,
            timeout=30
        )
        response.raise_for_status()
        first_page = response.json()
        campaigns = first_page if isinstance(first_page, list) else first_page.get("results", [])
        
        if len(campaigns) < 100:
            # 1페이지로 끝
            all_campaigns = campaigns
        else:
            # 여러 페이지 병렬 처리
            all_campaigns = list(campaigns)
            
            def fetch_page(page_num):
                params = {"account_id": account_id, "page": page_num, "size": 100}
                resp = requests.get(
                    f"{APPLOVIN_BASE_URL}/campaign/list",
                    headers=headers,
                    params=params,
                    timeout=30
                )
                resp.raise_for_status()
                data = resp.json()
                return data if isinstance(data, list) else data.get("results", [])
            
            # 최대 20페이지까지 병렬 요청
            with ThreadPoolExecutor(max_workers=5) as executor:
                page = 2
                while page <= 20:  # 최대 2000개
                    # 5페이지씩 묶어서 병렬 요청
                    batch_pages = range(page, min(page + 5, 21))
                    futures = {executor.submit(fetch_page, p): p for p in batch_pages}
                    
                    batch_results = []
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            if result:
                                batch_results.append((futures[future], result))
                        except Exception as e:
                            logger.error(f"Campaign page {futures[future]} failed: {e}")
                    
                    # 페이지 순서대로 정렬
                    batch_results.sort(key=lambda x: x[0])
                    
                    # 결과 추가
                    has_more = False
                    for page_num, result in batch_results:
                        all_campaigns.extend(result)
                        if len(result) == 100:
                            has_more = True
                    
                    if not has_more:
                        break
                    
                    page += 5
                    logger.info(f"Fetched campaigns up to page {page-1}, total: {len(all_campaigns)}")
        
        logger.info(f"Total campaigns fetched: {len(all_campaigns)}")
        
        # LIVE만 필터링
        all_campaigns = [c for c in all_campaigns if c.get("status") == "LIVE"]
        logger.info(f"After LIVE filter: {len(all_campaigns)}")
        
        # 게임별 필터링
        if game and "game_mapping" in config:
            keyword = config["game_mapping"].get(game, "").lower()
            if keyword:
                all_campaigns = [
                    c for c in all_campaigns 
                    if keyword in c.get("name", "").lower()
                ]
                logger.info(f"After game filter ({keyword}): {len(all_campaigns)}")
        
        return all_campaigns
        
    except Exception as e:
        logger.error(f"Failed to fetch campaigns: {e}", exc_info=True)
        st.error(f"Campaign 목록을 가져오는데 실패했습니다: {e}")
        return []

@st.cache_data(ttl=300)  # 5분 캐시
def get_assets(game: str = None) -> Dict[str, List[Dict]]:
    """
    Fetch all assets with parallel requests (cached).
    """
    try:
        config = _get_api_config()
        headers = {"Authorization": config["api_key"]}
        account_id = config["account_id"]
        
        # 먼저 첫 페이지로 전체 페이지 수 추정
        params = {"account_id": account_id, "page": 1, "size": 100}
        response = requests.get(
            f"{APPLOVIN_BASE_URL}/asset/list",
            headers=headers,
            params=params,
            timeout=30
        )
        response.raise_for_status()
        first_page = response.json()
        assets = first_page if isinstance(first_page, list) else first_page.get("results", [])
        
        if len(assets) < 100:
            # 1페이지로 끝
            all_assets = assets
        else:
            # 여러 페이지 병렬 처리
            all_assets = list(assets)
            
            def fetch_page(page_num):
                params = {"account_id": account_id, "page": page_num, "size": 100}
                resp = requests.get(
                    f"{APPLOVIN_BASE_URL}/asset/list",
                    headers=headers,
                    params=params,
                    timeout=30
                )
                resp.raise_for_status()
                data = resp.json()
                return data if isinstance(data, list) else data.get("results", [])
            
            # 최대 60페이지까지 병렬 요청 (5~10개씩 동시)
            with ThreadPoolExecutor(max_workers=5) as executor:
                page = 2
                while page <= 60:  # 최대 6000개
                    # 5페이지씩 묶어서 병렬 요청
                    batch_pages = range(page, min(page + 5, 61))
                    futures = {executor.submit(fetch_page, p): p for p in batch_pages}
                    
                    batch_results = []
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            if result:
                                batch_results.append((futures[future], result))
                        except Exception as e:
                            logger.error(f"Page {futures[future]} failed: {e}")
                    
                    # 페이지 순서대로 정렬
                    batch_results.sort(key=lambda x: x[0])
                    
                    # 결과 추가
                    has_more = False
                    for page_num, result in batch_results:
                        all_assets.extend(result)
                        if len(result) == 100:
                            has_more = True
                    
                    if not has_more:
                        break
                    
                    page += 5
                    logger.info(f"Fetched up to page {page-1}, total: {len(all_assets)}")
        
        logger.info(f"Total assets fetched: {len(all_assets)}")
        
        # ACTIVE만 필터링
        all_assets = [a for a in all_assets if a.get("status") == "ACTIVE"]
        
        # 게임별 필터링
        if game and "game_mapping" in config:
            package_keyword = config["game_mapping"].get(game, "").lower()
            if package_keyword:
                all_assets = [
                    a for a in all_assets
                    if package_keyword in a.get("name", "").lower()
                ]
                logger.info(f"Filtered to {len(all_assets)} assets for {game}")
        
        # Videos와 Playables 분리
        videos = [a for a in all_assets if a.get("resource_type") == "VIDEO"]
        playables = [a for a in all_assets if a.get("resource_type") == "HTML"]
        
        logger.info(f"Split: {len(videos)} videos, {len(playables)} playables")
        
        return {
            "videos": videos,
            "playables": playables
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch Applovin assets: {e}", exc_info=True)
        st.error(f"Applovin asset 목록을 가져오는데 실패했습니다: {e}")
        return {"videos": [], "playables": []}

# =========================================================
# UI Renderer
# =========================================================

def render_applovin_settings_panel(container, game: str, idx: int, is_marketer: bool = True) -> None:
    """Render Applovin settings panel with campaign and asset selection."""
    _ensure_applovin_settings_state()
    cur = get_applovin_settings(game) or {}
    
    with container:
        st.markdown(f"#### {game} Applovin Settings")
        
        # Fetch campaigns for this game
        campaigns = get_campaigns(game=game)
        
        if not campaigns:
            st.warning("⚠️ Campaign을 불러올 수 없습니다.")
            return
        
        # Campaign selection
        campaign_options = {
            f"{c.get('name', 'Unnamed')} (ID: {c.get('id', 'N/A')})": c.get('id')
            for c in campaigns
        }
        
        current_campaign_id = cur.get("campaign_id", "")
        default_idx = 0
        if current_campaign_id:
            for i, cid in enumerate(campaign_options.values()):
                if str(cid) == str(current_campaign_id):
                    default_idx = i
                    break
        
        selected_campaign = st.selectbox(
            "Campaign 선택",
            options=list(campaign_options.keys()),
            index=default_idx,
            key=f"applovin_campaign_{idx}",
        )
        
        campaign_id = campaign_options[selected_campaign]
        
        # Create or Import Creative
        creative_action = st.selectbox(
            "Create/Import Creative",
            options=["Create", "Import"],
            index=0 if cur.get("creative_action") != "Import" else 1,
            key=f"applovin_creative_action_{idx}",
        )
        
        # Create 선택 시 Videos와 Playables 드롭다운 표시
        selected_video_id = None
        selected_playable_id = None
        
        if creative_action == "Create":
            assets = get_assets(game=game)
            
            # Videos 드롭다운
            if assets["videos"]:
                video_options = {
                    f"{v['name']} (ID: {v['id']})": v['id']
                    for v in assets["videos"]
                }
                selected_video = st.selectbox(
                    "Video 선택",
                    options=list(video_options.keys()),
                    key=f"applovin_video_{idx}",
                )
                selected_video_id = video_options[selected_video]
            else:
                st.warning(f"⚠️ {game}에 해당하는 Video asset이 없습니다.")
            
            # Playables 드롭다운
            if assets["playables"]:
                playable_options = {
                    f"{p['name']} (ID: {p['id']})": p['id']
                    for p in assets["playables"]
                }
                selected_playable = st.selectbox(
                    "Playable (HTML) 선택",
                    options=list(playable_options.keys()),
                    key=f"applovin_playable_{idx}",
                )
                selected_playable_id = playable_options[selected_playable]
            else:
                st.warning(f"⚠️ {game}에 해당하는 Playable asset이 없습니다.")
        
        # Save settings
        st.session_state.applovin_settings[game] = {
            "campaign_id": str(campaign_id),
            "creative_action": creative_action,
            "video_id": selected_video_id,
            "playable_id": selected_playable_id,
        }
# =========================================================
# Upload Logic
# =========================================================

def upload_to_applovin(game: str, videos: List[Dict], settings: Dict) -> Dict:
    """
    Upload videos to Applovin.
    
    Args:
        game: Game name
        videos: List of video dictionaries (from Drive import)
        settings: Applovin settings dictionary
        
    Returns:
        Dict with success status, message, and errors
    """
    logger.info(f"Uploading {len(videos)} videos to Applovin for {game}")
    
    # TODO: Implement Applovin API integration
    # This is a placeholder implementation
    
    campaign_id = settings.get("campaign_id", "")
    ad_group_id = settings.get("ad_group_id", "")
    creative_type = settings.get("creative_type", "Video")
    
    if not campaign_id:
        return {
            "success": False,
            "error": "Campaign ID가 필요합니다.",
            "errors": ["Campaign ID를 입력해주세요."]
        }
    
    # Placeholder: 실제 Applovin API 호출은 여기에 구현
    logger.warning(f"Applovin upload not yet implemented. Would upload {len(videos)} videos to campaign {campaign_id}")
    
    return {
        "success": False,
        "error": "Applovin upload 기능은 아직 구현되지 않았습니다.",
        "errors": ["Applovin API 통합이 필요합니다."],
        "message": f"{len(videos)}개의 비디오를 업로드할 준비가 되었습니다."
    }

