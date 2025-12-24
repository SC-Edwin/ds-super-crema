"""Marketer-side Mintegral helpers for Creative 자동 업로드.

- Lets the marketer pick:
  1) Campaign selection
  2) Ad group selection
  3) Creative upload settings

- TODO: Implement Mintegral API integration
"""

from __future__ import annotations
from typing import Dict, List
import logging
import streamlit as st

logger = logging.getLogger(__name__)

# =========================================================
# Settings State Management
# =========================================================

def _ensure_mintegral_settings_state():
    """Initialize Mintegral settings in session state."""
    if "mintegral_settings" not in st.session_state:
        st.session_state.mintegral_settings = {}

def get_mintegral_settings(game: str) -> Dict:
    """Get Mintegral settings for a game."""
    _ensure_mintegral_settings_state()
    return st.session_state.mintegral_settings.get(game, {})

# =========================================================
# UI Renderer
# =========================================================

def render_mintegral_settings_panel(container, game: str, idx: int, is_marketer: bool = True) -> None:
    """
    Render Mintegral settings panel for marketer mode.
    
    Args:
        container: Streamlit container to render into
        game: Game name
        idx: Tab index for unique keys
        is_marketer: Whether in marketer mode (default True)
    """
    _ensure_mintegral_settings_state()
    cur = get_mintegral_settings(game) or {}
    
    with container:
        st.markdown(f"#### {game} Mintegral Settings")
        
        if is_marketer:
            st.info("🚧 Mintegral 설정 패널 (구현 예정)")
        else:
            st.info("🚧 Mintegral 설정 패널 (Test Mode)")
        
        # Campaign ID 입력
        campaign_id = st.text_input(
            "Campaign ID",
            value=cur.get("campaign_id", ""),
            key=f"mintegral_campaign_id_{idx}",
            help="Mintegral Campaign ID를 입력하세요."
        )
        
        # Ad Group ID 입력
        ad_group_id = st.text_input(
            "Ad Group ID",
            value=cur.get("ad_group_id", ""),
            key=f"mintegral_ad_group_id_{idx}",
            help="Mintegral Ad Group ID를 입력하세요 (선택사항)."
        )
        
        # Creative Type 선택
        creative_type = st.selectbox(
            "Creative Type",
            options=["Video", "Image", "Responsive Display"],
            index=0 if cur.get("creative_type", "Video") == "Video" else 1,
            key=f"mintegral_creative_type_{idx}",
            help="업로드할 크리에이티브 타입을 선택하세요."
        )
        
        # Settings 저장
        st.session_state.mintegral_settings[game] = {
            "campaign_id": campaign_id,
            "ad_group_id": ad_group_id,
            "creative_type": creative_type,
        }

# =========================================================
# Upload Logic
# =========================================================

def upload_to_mintegral(game: str, videos: List[Dict], settings: Dict) -> Dict:
    """
    Upload videos to Mintegral.
    
    Args:
        game: Game name
        videos: List of video dictionaries (from Drive import)
        settings: Mintegral settings dictionary
        
    Returns:
        Dict with success status, message, and errors
    """
    logger.info(f"Uploading {len(videos)} videos to Mintegral for {game}")
    
    # TODO: Implement Mintegral API integration
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
    
    # Placeholder: 실제 Mintegral API 호출은 여기에 구현
    logger.warning(f"Mintegral upload not yet implemented. Would upload {len(videos)} videos to campaign {campaign_id}")
    
    return {
        "success": False,
        "error": "Mintegral upload 기능은 아직 구현되지 않았습니다.",
        "errors": ["Mintegral API 통합이 필요합니다."],
        "message": f"{len(videos)}개의 비디오를 업로드할 준비가 되었습니다."
    }

