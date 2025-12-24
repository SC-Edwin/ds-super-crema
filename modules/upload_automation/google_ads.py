"""Marketer-side Google Ads helpers for Creative 자동 업로드.

- Lets the marketer pick:
  1) Campaign selection
  2) Ad group selection
  3) Creative upload settings

- TODO: Implement Google Ads API integration
"""

from __future__ import annotations
from typing import Dict, List
import logging
import streamlit as st

logger = logging.getLogger(__name__)

# =========================================================
# Settings State Management
# =========================================================

def _ensure_google_ads_settings_state():
    """Initialize Google Ads settings in session state."""
    if "google_ads_settings" not in st.session_state:
        st.session_state.google_ads_settings = {}

def get_google_ads_settings(game: str) -> Dict:
    """Get Google Ads settings for a game."""
    _ensure_google_ads_settings_state()
    return st.session_state.google_ads_settings.get(game, {})

# =========================================================
# UI Renderer
# =========================================================

def render_google_ads_settings_panel(container, game: str, idx: int, is_marketer: bool = True) -> None:
    """
    Render Google Ads settings panel for marketer mode.
    
    Args:
        container: Streamlit container to render into
        game: Game name
        idx: Tab index for unique keys
        is_marketer: Whether in marketer mode (default True)
    """
    _ensure_google_ads_settings_state()
    cur = get_google_ads_settings(game) or {}
    
    with container:
        st.markdown(f"#### {game} Google Ads Settings")
        
        if is_marketer:
            st.info("🚧 Google Ads 설정 패널 (구현 예정)")
        else:
            st.info("🚧 Google Ads 설정 패널 (Test Mode)")
        
        # Campaign ID 입력
        campaign_id = st.text_input(
            "Campaign ID",
            value=cur.get("campaign_id", ""),
            key=f"google_campaign_id_{idx}",
            help="Google Ads Campaign ID를 입력하세요."
        )
        
        # Ad Group ID 입력
        ad_group_id = st.text_input(
            "Ad Group ID",
            value=cur.get("ad_group_id", ""),
            key=f"google_ad_group_id_{idx}",
            help="Google Ads Ad Group ID를 입력하세요 (선택사항)."
        )
        
        # Creative Type 선택
        creative_type = st.selectbox(
            "Creative Type",
            options=["Video", "Image", "Responsive Display"],
            index=0 if cur.get("creative_type", "Video") == "Video" else 1,
            key=f"google_creative_type_{idx}",
            help="업로드할 크리에이티브 타입을 선택하세요."
        )
        
        # Settings 저장
        st.session_state.google_ads_settings[game] = {
            "campaign_id": campaign_id,
            "ad_group_id": ad_group_id,
            "creative_type": creative_type,
        }

# =========================================================
# Upload Logic
# =========================================================

def upload_to_google_ads(game: str, videos: List[Dict], settings: Dict) -> Dict:
    """
    Upload videos to Google Ads.
    
    Args:
        game: Game name
        videos: List of video dictionaries (from Drive import)
        settings: Google Ads settings dictionary
        
    Returns:
        Dict with success status, message, and errors
    """
    logger.info(f"Uploading {len(videos)} videos to Google Ads for {game}")
    
    # TODO: Implement Google Ads API integration
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
    
    # Placeholder: 실제 Google Ads API 호출은 여기에 구현
    logger.warning(f"Google Ads upload not yet implemented. Would upload {len(videos)} videos to campaign {campaign_id}")
    
    return {
        "success": False,
        "error": "Google Ads upload 기능은 아직 구현되지 않았습니다.",
        "errors": ["Google Ads API 통합이 필요합니다."],
        "message": f"{len(videos)}개의 비디오를 업로드할 준비가 되었습니다."
    }

