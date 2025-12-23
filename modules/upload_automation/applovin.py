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

def get_campaigns(game: str = None) -> List[Dict]:
    """
    Fetch campaigns from Applovin API, optionally filtered by game.
    
    Args:
        game: Optional game name to filter campaigns by name pattern
    """
    try:
        config = _get_api_config()
        headers = {"Authorization": config["api_key"]}
        params = {"account_id": config["account_id"]}
        
        response = requests.get(
            f"{APPLOVIN_BASE_URL}/campaign/list",
            headers=headers,
            params=params,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        
        if isinstance(data, list):
            campaigns = data
        else:
            campaigns = data.get("results", [])
        
        # 게임별 필터링
        if game and "game_mapping" in config:
            campaign_keyword = config["game_mapping"].get(game, "").lower()
            if campaign_keyword:
                campaigns = [
                    c for c in campaigns 
                    if campaign_keyword in c.get("name", "").lower()
                ]
                logger.info(f"Filtered to {len(campaigns)} campaigns for {game} (keyword: {campaign_keyword})")
        
        logger.info(f"Fetched {len(campaigns)} campaigns from Applovin")
        return campaigns
        
    except Exception as e:
        logger.error(f"Failed to fetch Applovin campaigns: {e}", exc_info=True)
        st.error(f"Applovin campaign 목록을 가져오는데 실패했습니다: {e}")
        return []

# =========================================================
# UI Renderer
# =========================================================

def render_applovin_settings_panel(container, game: str, idx: int, is_marketer: bool = True) -> None:
    """Render Applovin settings panel with campaign selection."""
    _ensure_applovin_settings_state()
    cur = get_applovin_settings(game) or {}
    
    with container:
        st.markdown(f"#### {game} Applovin Settings")
        
        # Fetch campaigns
        campaigns = get_campaigns(game=game)
        
        if not campaigns:
            st.warning("⚠️ Campaign을 불러올 수 없습니다. API 설정을 확인해주세요.")
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
            help="업로드할 Campaign을 선택하세요."
        )
        
        campaign_id = campaign_options[selected_campaign]
        
        # ✅ Campaign 선택 시 현재 탭 유지
        if st.session_state.get(f"prev_applovin_campaign_{idx}") != campaign_id:
            st.query_params["tab"] = game
            st.session_state[f"prev_applovin_campaign_{idx}"] = campaign_id
        
        # Creative Type
        creative_type = st.selectbox(
            "Creative Type",
            options=["Video", "Image"],
            index=0,
            key=f"applovin_creative_type_{idx}",
            help="업로드할 크리에이티브 타입"
        )
        
        # ✅ Creative Type 선택 시에도 현재 탭 유지 (선택사항)
        if st.session_state.get(f"prev_applovin_creative_type_{idx}") != creative_type:
            st.query_params["tab"] = game
            st.session_state[f"prev_applovin_creative_type_{idx}"] = creative_type
        
        # Save settings
        st.session_state.applovin_settings[game] = {
            "campaign_id": str(campaign_id),
            "creative_type": creative_type,
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

