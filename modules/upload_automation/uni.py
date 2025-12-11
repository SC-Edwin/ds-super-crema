"""Marketer-side Unity helpers for Creative 자동 업로드.

- Lets the marketer pick:
  1) 플랫폼 (AOS / iOS) per game
  2) Live campaigns inside that campaign-set (via Unity API)

- Uses:
  - UNITY_GAME_IDS: per-game default app (title) ID (fallback)
  - UNITY_APP_IDS_ALL: per-game, per-platform app IDs
  - get_unity_campaign_set_id(game, platform): per-game campaign-set ID
"""


from __future__ import annotations

from typing import Dict, List
import logging

import streamlit as st

import unity_ads  # ← 이 줄 확인
from unity_ads import (
    UNITY_ORG_ID_DEFAULT,
    get_unity_app_id,        # ← 이 함수들이 제대로 import됐는지 확인
    get_unity_campaign_set_id,
    _unity_get,
    _unity_list_playable_creatives,
    get_unity_settings as _get_unity_settings,
    _ensure_unity_settings_state,
    preview_unity_upload as _preview_unity_upload,
    apply_unity_creative_packs_to_campaign as _apply_unity_creative_packs_to_campaign,
    upload_unity_creatives_to_campaign as _upload_unity_creatives_to_campaign,
)

# Re-export for compatibility
def get_unity_settings(game: str) -> Dict:
    """Re-export from unity_ads for compatibility."""
    return _get_unity_settings(game)

def preview_unity_upload(*, game: str, videos: List[Dict], settings: Dict, is_marketer: bool = True) -> Dict:
    """Re-export from unity_ads for compatibility. Default is_marketer=True for marketer mode."""
    return _preview_unity_upload(game=game, videos=videos, settings=settings, is_marketer=is_marketer)

def apply_unity_creative_packs_to_campaign(*, game: str, creative_pack_ids: List[str], settings: Dict, is_marketer: bool = True) -> Dict:
    """Re-export from unity_ads for compatibility. Default is_marketer=True for marketer mode."""
    return _apply_unity_creative_packs_to_campaign(game=game, creative_pack_ids=creative_pack_ids, settings=settings, is_marketer=is_marketer)

def upload_unity_creatives_to_campaign(*, game: str, videos: List[Dict], settings: Dict) -> Dict:
    """Re-export from unity_ads for compatibility."""
    return _upload_unity_creatives_to_campaign(game=game, videos=videos, settings=settings)

logger = logging.getLogger(__name__)
UNITY_BASE_URL = "https://services.api.unity.com/advertise/v1"
# -------------------------------------------------------------------------
# Small helper: pick app (title) ID for game + platform
# -------------------------------------------------------------------------
# ━━━ uni.py에서 이 함수를 완전히 교체하세요 ━━━





# -------------------------------------------------------------------------
# 1. Fetch campaigns per game + platform (AOS / iOS)
# -------------------------------------------------------------------------
# ━━━ 수정 후 (에러 처리 강화) ━━━
@st.cache_data(ttl=0, show_spinner=False)
def fetch_unity_campaigns(game: str, platform: str = "aos") -> List[Dict]:
    """
    시도 1: Apps 레벨에서 캠페인 조회
    
    API: GET /organizations/{orgId}/apps/{appId}/campaigns
    """
    try:
        org_id = (UNITY_ORG_ID_DEFAULT or "").strip()
        app_id = get_unity_campaign_set_id(game, platform)  # 실제로는 App ID
        
        if not org_id or not app_id:
            raise RuntimeError("❌ Missing org_id or app_id")

        path = f"organizations/{org_id}/apps/{app_id}/campaigns"
        
        st.write(f"🔍 Trying: {UNITY_BASE_URL}/{path}")
        
        meta = _unity_get(path)
        
        # 응답 파싱
        items: List[Dict] = []
        if isinstance(meta, list):
            items = meta
        elif isinstance(meta, dict):
            for key in ("results", "items", "data", "campaigns"):
                if isinstance(meta.get(key), list):
                    items = meta[key]
                    break
        
        # Campaign 정보 추출
        campaigns: List[Dict] = []
        for c in items:
            if not isinstance(c, dict):
                continue
                
            cid = str(c.get("id") or c.get("campaignId") or "")
            name = c.get("name") or "(no name)"
            status = (c.get("status") or "").upper()

            if cid:
                campaigns.append({
                    "id": cid,
                    "name": name,
                    "status": status
                })

        return campaigns
        
    except Exception as e:
        logger.exception(f"Unity API error: fetch_unity_campaigns({game}, {platform})")
        raise RuntimeError(f"Unity 캠페인 조회 실패: {str(e)}")


# -------------------------------------------------------------------------
# 2. List playables per game + platform (for info)
# -------------------------------------------------------------------------
@st.cache_data(ttl=0, show_spinner=False)
def fetch_playables_for_game(game: str, platform: str = "aos") -> List[Dict]:
    try:
        org_id = (UNITY_ORG_ID_DEFAULT or "").strip()
        app_id = get_unity_campaign_set_id(game, platform)
        
        path = f"organizations/{org_id}/apps/{app_id}/creatives"
        
        st.write(f"🔍 Trying: {UNITY_BASE_URL}/{path}")
        
        meta = _unity_get(path)
        
        # Playable만 필터링
        items = []
        if isinstance(meta, list):
            items = meta
        elif isinstance(meta, dict):
            for key in ("results", "items", "data", "creatives"):
                if isinstance(meta.get(key), list):
                    items = meta[key]
                    break
        
        playables = []
        for cr in items:
            if not isinstance(cr, dict):
                continue
            t = (cr.get("type") or "").lower()
            if "playable" in t or "cpe" in t:
                playables.append(cr)
        
        return playables
        
    except Exception as e:
        logger.exception(f"Unity API error: fetch_playables_for_game({game}, {platform})")
        raise RuntimeError(f"Playable 조회 실패: {str(e)}")

# -------------------------------------------------------------------------
# 3. Marketer-mode Unity settings panel
# -------------------------------------------------------------------------
def render_unity_settings_panel(container, game: str, idx: int, is_marketer: bool = True) -> None:
    """
    Marketer-mode Unity panel (all games)
    Allows campaign selection and creative/creative pack upload for all games in marketer mode.
    """
    _ensure_unity_settings_state()
    cur = get_unity_settings(game) or {}

    with container:
        st.markdown(f"#### {game} Unity Settings (Marketer)")

        # 1) 플랫폼 선택
        current_platform = cur.get("platform", "aos")
        platform = st.radio(
            "플랫폼",
            options=["aos", "ios"],
            index=0 if current_platform == "aos" else 1,
            horizontal=True,
            key=f"unity_mkt_platform_{idx}",
        )

        # 2) 캠페인 목록 가져오기
        try:
            campaigns = fetch_unity_campaigns(game, platform)
        except Exception as e:
            st.error(f"Unity 캠페인 목록을 불러오는 중 오류: {e}")
            campaigns = []

        selected_campaign_id = cur.get("campaign_id") or ""

        if campaigns:
            labels = [f"{c['name']} ({c['id']})" for c in campaigns]
            ids = [c["id"] for c in campaigns]
            
            default_idx = ids.index(selected_campaign_id) if selected_campaign_id in ids else 0
            
            sel_label = st.selectbox(
                "캠페인 선택",
                options=labels,
                index=default_idx,
                key=f"unity_mkt_campaign_{idx}",
            )
            selected_campaign_id = ids[labels.index(sel_label)]
            st.caption(f"선택된 캠페인 ID: `{selected_campaign_id}`")
        else:
            st.info("선택된 플랫폼의 Unity 캠페인을 찾을 수 없습니다.")

        # 3) Playable 선택
        st.markdown("#### Playable 선택")
        
        playable_options = ["(선택 안 함)"]
        playable_id_map = {}
        prev_playable_id = cur.get("existing_playable_id", "")

        try:
            playables = fetch_playables_for_game(game, platform=platform)
            
            if playables:
                st.caption(f"✅ {platform.upper()} 앱에 등록된 Playable: {len(playables)}개")
                
                for p in playables:
                    p_id = str(p.get("id", ""))
                    p_name = p.get("name", "(no name)")
                    p_type = p.get("type", "")
                    
                    if p_id:
                        label = f"{p_name} ({p_type}) [{p_id[:8]}...]"
                        playable_options.append(label)
                        playable_id_map[label] = p_id
            else:
                st.caption(f"⚠️ {platform.upper()} 앱에 등록된 Playable이 없습니다.")
                
        except Exception as e:
            st.error(f"Playable 목록 조회 실패: {e}")

        default_playable_idx = 0
        if prev_playable_id and prev_playable_id in playable_id_map.values():
            for idx_p, (label, p_id) in enumerate(playable_id_map.items(), start=1):
                if p_id == prev_playable_id:
                    default_playable_idx = idx_p
                    break

        selected_playable_label = st.selectbox(
            "사용할 Playable 선택",
            options=playable_options,
            index=default_playable_idx,
            key=f"unity_mkt_playable_{idx}",
        )

        selected_playable_id = playable_id_map.get(selected_playable_label, "")
        if selected_playable_id:
            st.success(f"선택된 Playable ID: `{selected_playable_id}`")

        # 4) 상태 저장 (Campaign Set ID 기준)
        campaign_set_id = get_unity_campaign_set_id(game, platform)

        cur.update({
            "platform": platform,
            "org_id": UNITY_ORG_ID_DEFAULT,
            "campaign_set_id": campaign_set_id,  # ← Campaign Set ID 저장
            "campaign_id": selected_campaign_id,
            "existing_playable_id": selected_playable_id,
            "existing_playable_label": selected_playable_label,
        })
        
        st.session_state.unity_settings[game] = cur

        if selected_campaign_id:
            st.success(f"Target Unity Campaign: `{selected_campaign_id}` ({platform.upper()})")