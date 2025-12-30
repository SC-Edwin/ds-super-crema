"""Marketer-side Mintegral helpers for Creative 자동 업로드.

- Creative Set Settings:
  1) Upload Creative Set - Upload new creatives to new or existing sets
  2) Copy Creative Set - Copy creative sets to other offers

- Supports:
  - Creative filtering by game name (via secrets.toml mapping)
  - Multi-select for Image/Video/Playable creatives
  - Offer selection with game filtering
  - Auto-naming: {game}_{YYMMDD}
"""

from __future__ import annotations
from typing import Dict, List, Optional, Callable
import logging
import streamlit as st
import requests
import hashlib
import time
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

MINTEGRAL_BASE_URL = "https://ss-api.mintegral.com/api/open/v1"
MINTEGRAL_STORAGE_URL = "https://ss-storage-api.mintegral.com/api/open/v1"

# =========================================================
# API Configuration
# =========================================================

def _get_api_config():
    """Get Mintegral API configuration from secrets."""
    if "mintegral" not in st.secrets:
        raise RuntimeError(
            "Missing [mintegral] section in secrets.toml.\n"
            "Please add:\n"
            "[mintegral]\n"
            "access_key = \"your_access_key\"\n"
            "api_key = \"your_api_key\""
        )
    return {
        "access_key": st.secrets["mintegral"]["access_key"],
        "api_key": st.secrets["mintegral"]["api_key"]
    }

def _get_game_mapping(game: str) -> str:
    """Get game short name from secrets.toml mapping."""
    if "mintegral" in st.secrets and "game_mappings" in st.secrets["mintegral"]:
        mapping = st.secrets["mintegral"]["game_mappings"].get(game)
        
        # 리스트면 그대로 반환, 문자열이면 리스트로 변환
        if isinstance(mapping, list):
            return mapping
        elif isinstance(mapping, str):
            return [mapping]
        else:
            return [game.lower().replace(" ", "")]
    # Fallback mapping if not in secrets
    fallback = {
    "XP HERO": ["weaponrpg"],
    "Dino Universe": ["dinouniverse"],
    "Snake Clash": ["snakeclash"],
    "Pizza Ready": ["pizzaready"],
    "Cafe Life": ["cafelife"],
    "Suzy's Restaurant": ["suzyrest"],
    "Office Life": ["officelife"],
    "Lumber Chopper": ["lumberchop"],
    "Burger Please": ["burgerplease"],
    "Prison Life": ["prisonlife"]
    }
    return fallback.get(game, [game.lower().replace(" ", "")])

def _generate_token(api_key: str) -> tuple[str, int]:
    """Generate token for Mintegral API authentication."""
    timestamp = int(time.time())
    token = hashlib.md5(f"{api_key}{hashlib.md5(str(timestamp).encode()).hexdigest()}".encode()).hexdigest()
    return token, timestamp

def _get_auth_headers():
    """Get authentication headers for Mintegral API."""
    config = _get_api_config()
    token, timestamp = _generate_token(config["api_key"])
    return {
        "access-key": config["access_key"],
        "token": token,
        "timestamp": str(timestamp),
        "Content-Type": "application/json"
    }

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
# API Functions
# =========================================================
@st.cache_data(ttl=300)
def get_creatives(creative_type: Optional[str] = None, game_filter: Optional[List[str]] = None, max_pages: int = 3) -> List[Dict]:
    """
    Fetch creatives from Mintegral API with parallel pagination.
    
    Args:
        creative_type: Type filter (IMAGE, VIDEO, PLAYABLE)
        game_filter: List of game short names to filter creatives (OR condition)
        max_pages: Maximum number of pages to fetch (default 3 = 600 items)
    
    Returns:
        List of creative dictionaries
    
    Note:
        Results are cached for 5 minutes. Uses parallel requests for faster loading.
    """
    
    def fetch_page(page: int) -> List[Dict]:
        """Fetch a single page of creatives."""
        try:
            headers = _get_auth_headers()
            params = {"page": page, "limit": 200}
            
            if creative_type:
                params["creative_type"] = creative_type
            
            response = requests.get(
                f"{MINTEGRAL_BASE_URL}/creatives/source",
                headers=headers,
                params=params,
                timeout=15
            )
            response.raise_for_status()
            
            data = response.json()
            if data.get("code") != 200:
                logger.error(f"Page {page}: Failed - {data.get('msg')}")
                return []
            
            return data.get("data", {}).get("list", [])
        except Exception as e:
            logger.error(f"Page {page}: Error - {e}")
            return []
    
    try:
        all_creatives = []
        
        # 병렬로 여러 페이지 동시 요청 (최대 5개 worker)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(fetch_page, page): page for page in range(1, max_pages + 1)}
            
            for future in as_completed(futures):
                creatives = future.result()
                if creatives:
                    all_creatives.extend(creatives)
        
        logger.info(f"Total creatives before filtering: {len(all_creatives)}")
        
        # 필터링: 여러 키워드 중 하나라도 포함되면 OK
        if game_filter:
            all_creatives = [
                c for c in all_creatives 
                if any(gf.lower() in c.get("creative_name", "").lower() for gf in game_filter)
            ]
        
        logger.info(f"Fetched {len(all_creatives)} creatives (type: {creative_type}, game: {game_filter})")
        return all_creatives
        
    except Exception as e:
        logger.error(f"Failed to fetch Mintegral creatives: {e}", exc_info=True)
        st.error(f"Mintegral creative 목록을 가져오는데 실패했습니다: {e}")
        return []

@st.cache_data(ttl=300)
def get_offers(game_filter: Optional[List[str]] = None, max_pages: int = 3) -> List[Dict]:
    """
    Fetch offers from Mintegral API with parallel pagination.
    
    Args:
        game_filter: List of game short names to filter offers (OR condition)
        max_pages: Maximum number of pages to fetch (default 3 = 600 items)
    
    Returns:
        List of offer dictionaries
    
    Note:
        Results are cached for 5 minutes. Uses parallel requests for faster loading.
    """
    
    def fetch_page(page: int, api_filter: Optional[str]) -> List[Dict]:
        """Fetch a single page of offers."""
        try:
            headers = _get_auth_headers()
            params = {"page": page, "limit": 200}
            
            if api_filter:
                params["offer_name"] = api_filter
            
            response = requests.get(
                f"{MINTEGRAL_BASE_URL}/offers",
                headers=headers,
                params=params,
                timeout=15
            )
            response.raise_for_status()
            
            data = response.json()
            if data.get("code") != 200:
                logger.error(f"Page {page}: Failed - {data.get('msg')}")
                return []
            
            return data.get("data", {}).get("list", [])
        except Exception as e:
            logger.error(f"Page {page}: Error - {e}")
            return []
    
    try:
        # API 필터링은 첫 번째 키워드만 사용 (API는 단일 검색만 지원)
        api_filter = game_filter[0] if game_filter and len(game_filter) > 0 else None
        
        all_offers = []
        
        # 병렬로 여러 페이지 동시 요청 (최대 5개 worker)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(fetch_page, page, api_filter): page for page in range(1, max_pages + 1)}
            
            for future in as_completed(futures):
                offers = future.result()
                if offers:
                    all_offers.extend(offers)
        
        # 클라이언트에서 추가 필터링 (모든 키워드 체크)
        if game_filter and len(game_filter) > 1:
            all_offers = [
                o for o in all_offers 
                if any(gf.lower() in o.get("offer_name", "").lower() for gf in game_filter)
            ]
        
        logger.info(f"Fetched {len(all_offers)} offers (game: {game_filter})")
        return all_offers
        
    except Exception as e:
        logger.error(f"Failed to fetch Mintegral offers: {e}", exc_info=True)
        st.error(f"Mintegral offer 목록을 가져오는데 실패했습니다: {e}")
        return []

def _get_default_creative_set_name(game: str) -> str:
    """Generate default creative set name: {game_short}_{YYMMDD}"""
    short_names = _get_game_mapping(game)
    short_name = short_names[0] if short_names else game.lower().replace(" ", "")  # 첫 번째 이름 사용
    date_str = datetime.now().strftime("%y%m%d")
    return f"{short_name}_{date_str}"
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
        
        # Creative Set Setting dropdown
        setting_mode = st.selectbox(
            "Creative Set Setting",
            options=["Upload Creative Set", "Copy Creative Set"],
            key=f"mintegral_setting_mode_{idx}",
            help="Upload: 새 Creative Set 생성 또는 기존 Creative 추가\nCopy: 다른 Offer로 Creative Set 복사"
        )
        
        if setting_mode == "Upload Creative Set":
            _render_upload_creative_set(game, idx, cur)
        else:
            _render_copy_creative_set(game, idx, cur)

def _render_upload_creative_set(game: str, idx: int, cur: Dict) -> None:
    """Render Upload Creative Set UI."""
    
    # Creative Set Name with auto-generated default
    default_name = _get_default_creative_set_name(game)
    creative_set_name = st.text_input(
        "Creative Set Name",
        value=cur.get("creative_set_name", default_name),
        key=f"mintegral_creative_set_name_{idx}",
        help=f"비워두면 자동으로 {default_name}로 설정됩니다"
    )
    
    # Use default if empty
    if not creative_set_name.strip():
        creative_set_name = default_name
    
    st.markdown("---")
    st.markdown("**Add Existing Creatives**")
    
    game_short = _get_game_mapping(game)
    
    # 세션 상태에 페이지 수 저장
    if f"mintegral_video_pages_{idx}" not in st.session_state:
        st.session_state[f"mintegral_video_pages_{idx}"] = 20
    if f"mintegral_image_pages_{idx}" not in st.session_state:
        st.session_state[f"mintegral_image_pages_{idx}"] = 20  
    if f"mintegral_playable_pages_{idx}" not in st.session_state:
        st.session_state[f"mintegral_playable_pages_{idx}"] = 5
    
    # Initialize selected lists
    selected_image_md5s = []
    selected_video_md5s = []
    selected_playable_md5s = []

    # ========== Image Creatives ==========
    with st.expander("📷 Image Creatives", expanded=False):
        # Load button
        if st.button("🔍 Load Images", key=f"load_images_{idx}"):
            image_pages = st.session_state.get(f"mintegral_image_pages_{idx}", 20)
            with st.spinner(f"Loading images... ({image_pages}페이지)"):
                images = get_creatives(
                    creative_type="IMAGE", 
                    game_filter=game_short, 
                    max_pages=image_pages
                )
                st.session_state[f"mintegral_images_data_{idx}"] = images
        
        # Display loaded data
        images = st.session_state.get(f"mintegral_images_data_{idx}", [])
        
        if images:
            st.caption(f"📊 총 {len(images)}개 표시 (최대 {st.session_state.get(f'mintegral_image_pages_{idx}', 20) * 200}개 중 필터링)")
            
            image_options = {f"{c['creative_name']} ({c['resolution']})": c['creative_md5'] 
                        for c in images}
            selected_images = st.multiselect(
                "Select Images",
                options=list(image_options.keys()),
                key=f"mintegral_images_{idx}",
                help=f"Image 크리에이티브 선택"
            )
            selected_image_md5s = [image_options[name] for name in selected_images]
            
            # "더 보기" 버튼
            col1, col2 = st.columns([1, 3])
            with col1:
                if st.button("➕ 더 보기 (10페이지)", key=f"load_more_images_{idx}"):
                    st.session_state[f"mintegral_image_pages_{idx}"] = st.session_state.get(f"mintegral_image_pages_{idx}", 20) + 10
                    # 즉시 재로드 추가 ← HERE
                    image_pages = st.session_state[f"mintegral_image_pages_{idx}"]
                    with st.spinner(f"Loading more images... ({image_pages}페이지)"):
                        images = get_creatives(
                            creative_type="IMAGE", 
                            game_filter=game_short, 
                            max_pages=image_pages
                        )
                        st.session_state[f"mintegral_images_data_{idx}"] = images
                    st.cache_data.clear()
                    st.rerun()
            with col2:
                st.caption(f"💡 원하는 image가 없으면 '더 보기' 클릭")
        else:
            st.info("Click 'Load Images' to see available images")
        
    # ========== Video Creatives ==========
    with st.expander("🎥 Video Creatives", expanded=False):
        # Load button
        if st.button("🔍 Load Videos", key=f"load_videos_{idx}"):
            video_pages = st.session_state.get(f"mintegral_video_pages_{idx}", 20)
            with st.spinner(f"Loading videos... ({video_pages}페이지)"):
                videos = get_creatives(
                    creative_type="VIDEO", 
                    game_filter=game_short, 
                    max_pages=video_pages
                )
                st.session_state[f"mintegral_videos_data_{idx}"] = videos
        
        # Display loaded data
        videos = st.session_state.get(f"mintegral_videos_data_{idx}", [])
        
        if videos:
            st.caption(f"📊 총 {len(videos)}개 표시 (최대 {st.session_state.get(f'mintegral_video_pages_{idx}', 20) * 200}개 중 필터링)")
            
            video_options = {f"{c['creative_name']} ({c['resolution']})": c['creative_md5'] 
                            for c in videos}
            selected_videos = st.multiselect(
                "Select Videos",
                options=list(video_options.keys()),
                key=f"mintegral_videos_{idx}",
                help=f"Video 크리에이티브 선택"
            )
            selected_video_md5s = [video_options[name] for name in selected_videos]
            
            # "더 보기" 버튼
            col1, col2 = st.columns([1, 3])
            with col1:
                if st.button("➕ 더 보기 (10페이지)", key=f"load_more_videos_{idx}"):
                    st.session_state[f"mintegral_video_pages_{idx}"] = st.session_state.get(f"mintegral_video_pages_{idx}", 20) + 10
                    # 즉시 재로드 추가 ← HERE
                    video_pages = st.session_state[f"mintegral_video_pages_{idx}"]
                    with st.spinner(f"Loading more videos... ({video_pages}페이지)"):
                        videos = get_creatives(
                            creative_type="VIDEO", 
                            game_filter=game_short, 
                            max_pages=video_pages
                        )
                        st.session_state[f"mintegral_videos_data_{idx}"] = videos
                    st.cache_data.clear()
                    st.rerun()
            with col2:
                st.caption(f"💡 원하는 video가 없으면 '더 보기' 클릭")
        else:
            st.info("Click 'Load Videos' to see available videos")
    
    # ========== Playable Creatives ==========
    with st.expander("🎮 Playable Creatives", expanded=False):
        # Load button
        if st.button("🔍 Load Playables", key=f"load_playables_{idx}"):
            playable_pages = st.session_state.get(f"mintegral_playable_pages_{idx}", 5)
            with st.spinner(f"Loading playables... ({playable_pages}페이지)"):
                playables = get_creatives(
                    creative_type="PLAYABLE", 
                    game_filter=game_short, 
                    max_pages=playable_pages
                )
                st.session_state[f"mintegral_playables_data_{idx}"] = playables
        
        # Display loaded data
        playables = st.session_state.get(f"mintegral_playables_data_{idx}", [])
        
        if playables:
            st.caption(f"📊 총 {len(playables)}개 표시 (최대 {st.session_state.get(f'mintegral_playable_pages_{idx}', 5) * 200}개 중 필터링)")
            
            playable_options = {c['creative_name']: c['creative_md5'] for c in playables}
            selected_playables = st.multiselect(
                "Select Playables",
                options=list(playable_options.keys()),
                key=f"mintegral_playables_{idx}",
                help=f"Playable 크리에이티브 선택"
            )
            selected_playable_md5s = [playable_options[name] for name in selected_playables]
            
            # "더 보기" 버튼
            col1, col2 = st.columns([1, 3])
            with col1:
                if st.button("➕ 더 보기 (5페이지)", key=f"load_more_playables_{idx}"):
                    st.session_state[f"mintegral_playable_pages_{idx}"] = st.session_state.get(f"mintegral_playable_pages_{idx}", 5) + 5
                    # 즉시 재로드 추가 ← HERE
                    playable_pages = st.session_state[f"mintegral_playable_pages_{idx}"]
                    with st.spinner(f"Loading more playables... ({playable_pages}페이지)"):
                        playables = get_creatives(
                            creative_type="PLAYABLE", 
                            game_filter=game_short, 
                            max_pages=playable_pages
                        )
                        st.session_state[f"mintegral_playables_data_{idx}"] = playables
                    st.cache_data.clear()
                    st.rerun()
            with col2:
                st.caption(f"💡 원하는 playable이 없으면 '더 보기' 클릭")
        else:
            st.info("Click 'Load Playables' to see available playables")
    
    st.markdown("---")

    # Apply in Offer dropdown
    st.markdown("**Apply in Offer**")
    with st.spinner("Loading offers..."):
        offers = get_offers(game_filter=game_short, max_pages=5)

    selected_offer_id = None
    selected_offer_name = None
    if offers:
        offer_options = {f"{o['offer_name']} (ID: {o['offer_id']})": o['offer_id'] 
                        for o in offers}
        selected_offer = st.selectbox(
            "Select Offer",
            options=list(offer_options.keys()),
            key=f"mintegral_offer_{idx}",
            help=f"Creative Set을 적용할 Offer 선택"
        )
        selected_offer_id = offer_options[selected_offer]
        selected_offer_name = selected_offer.split(" (ID:")[0]
    else:
        st.warning(f"'{game_short}' 필터링된 Offer가 없습니다")

    # Add Product Icon button (Offer 선택 후에만 활성화)
    if selected_offer_id:
        if st.button(
            "Add Product Icon",
            key=f"mintegral_add_icon_{idx}",
            use_container_width=True,
            type="primary"
        ):
            # Get existing creative sets for this offer to find the icon
            try:
                with st.spinner("🔍 Searching for product icon in offer..."):
                    headers = _get_auth_headers()
                    params = {"offer_id": selected_offer_id, "page": 1, "limit": 10}
                    
                    response = requests.get(
                        f"{MINTEGRAL_BASE_URL}/creative_sets",
                        headers=headers,
                        params=params,
                        timeout=15
                    )
                    response.raise_for_status()
                    data = response.json()
                    
                    if data.get("code") == 200:
                        creative_sets = data.get("data", {}).get("list", [])
                        
                        # Search for 512x512 icon in any creative set
                        found_icon = None
                        for creative_set in creative_sets:
                            for creative in creative_set.get("creatives", []):
                                if (creative.get("creative_type") == "IMAGE" and 
                                    creative.get("dimension") == "512x512"):
                                    found_icon = {
                                        "md5": creative["creative_md5"],
                                        "name": creative["creative_name"]
                                    }
                                    break
                            if found_icon:
                                break
                        
                        if found_icon:
                            # Save to session state
                            st.session_state[f"mintegral_icon_{idx}"] = found_icon
                            st.success(f"✅ Found: {found_icon['name']}")
                        else:
                            st.warning(f"⚠️ No 512x512 icon found in offer's creative sets")
                    else:
                        st.error(f"❌ API Error: {data.get('msg')}")
                        
            except Exception as e:
                st.error(f"❌ Search error: {e}")
                logger.error(f"Icon search error: {e}", exc_info=True)
        
        # Show selected icon with X button
        if f"mintegral_icon_{idx}" in st.session_state:
            icon_data = st.session_state[f"mintegral_icon_{idx}"]
            
            col1, col2 = st.columns([4, 1])
            with col1:
                st.info(f"📷 **Product Icon:** {icon_data['name']}")
            with col2:
                if st.button("❌", key=f"mintegral_remove_icon_{idx}"):
                    del st.session_state[f"mintegral_icon_{idx}"]
                    st.rerun()

    # Save settings
    st.session_state.mintegral_settings[game] = {
        "mode": "upload",
        "creative_set_name": creative_set_name,
        "selected_images": selected_image_md5s,
        "selected_videos": selected_video_md5s,
        "selected_playables": selected_playable_md5s,
        "selected_offer_id": selected_offer_id,
        "selected_offer_name": selected_offer_name,
        "product_icon_md5": st.session_state.get(f"mintegral_icon_{idx}", {}).get("md5"),
    }

def _render_copy_creative_set(game: str, idx: int, cur: Dict) -> None:
    """Render Copy Creative Set UI."""
    
    st.markdown("**Select Creative Sets to Copy**")
    
    game_short = _get_game_mapping(game)
    
    # Session state key for creative sets data
    cache_key = f"mintegral_copy_creative_sets_data_{idx}"
    
    # Load button to fetch creative sets
    if st.button("🔍 Load Creative Sets", key=f"load_copy_creative_sets_{idx}"):
        with st.spinner("Loading creative sets..."):
            try:
                # Get all offers for this game
                offers = get_offers(game_filter=game_short, max_pages=5)
                
                if not offers:
                    st.warning(f"'{game_short}' 필터링된 Offer가 없습니다")
                    return
                
                # Fetch creative sets from all offers IN PARALLEL
                def fetch_creative_sets_for_offer(offer: Dict) -> List[Dict]:
                    """Fetch creative sets for a single offer."""
                    offer_id = offer["offer_id"]
                    offer_name = offer["offer_name"]
                    
                    try:
                        headers = _get_auth_headers()
                        params = {"offer_id": offer_id, "page": 1, "limit": 50}
                        response = requests.get(
                            f"{MINTEGRAL_BASE_URL}/creative_sets",
                            headers=headers,
                            params=params,
                            timeout=15
                        )
                        response.raise_for_status()
                        data = response.json()
                        
                        if data.get("code") == 200:
                            creative_sets = data.get("data", {}).get("list", [])
                            # Add offer info to each creative set
                            for cs in creative_sets:
                                cs["source_offer_id"] = offer_id
                                cs["source_offer_name"] = offer_name
                            return creative_sets
                        return []
                    except Exception as e:
                        logger.warning(f"Failed to fetch creative sets from offer {offer_id}: {e}")
                        return []
                
                all_creative_sets = []
                
                # Parallel fetch with ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = {executor.submit(fetch_creative_sets_for_offer, offer): offer for offer in offers}
                    
                    for future in as_completed(futures):
                        creative_sets = future.result()
                        if creative_sets:
                            all_creative_sets.extend(creative_sets)
                
                # Save to session state
                st.session_state[cache_key] = {
                    "creative_sets": all_creative_sets,
                    "offers": offers
                }
                
            except Exception as e:
                st.error(f"Creative Set 목록을 불러오는데 실패했습니다: {e}")
                logger.error(f"Failed to load creative sets for copy: {e}", exc_info=True)
    
    # Display loaded data from session state
    cached_data = st.session_state.get(cache_key)
    
    if not cached_data:
        st.info("Click 'Load Creative Sets' to see available creative sets")
        return
    
    all_creative_sets = cached_data["creative_sets"]
    offers = cached_data["offers"]
    
    if not all_creative_sets:
        st.info("이 게임에 생성된 Creative Set이 없습니다")
        return
    
    st.caption(f"📊 총 {len(all_creative_sets)}개 Creative Set 표시")
    
    # Create options: "creative_set_name (Offer: offer_name)"
    creative_set_options = {
        f"{cs['creative_set_name']} (Offer: {cs['source_offer_name']})": {
            "creative_set_name": cs["creative_set_name"],
            "offer_id": cs["source_offer_id"],
            "ad_outputs": cs.get("ad_outputs", []),
            "geos": cs.get("geos", ["ALL"]),
            "creatives": cs.get("creatives", [])
        }
        for cs in all_creative_sets
    }
    
    # Multi-select dropdown
    selected_sets = st.multiselect(
        "Select Creative Sets",
        options=list(creative_set_options.keys()),
        key=f"mintegral_copy_creative_sets_{idx}",
        help="복사할 Creative Set을 선택하세요 (여러 개 선택 가능)"
    )
    
    # Show selected sets info
    if selected_sets:
        st.markdown("**선택된 Creative Sets:**")
        for set_name in selected_sets:
            cs_info = creative_set_options[set_name]
            st.write(f"• {cs_info['creative_set_name']} (Creative 개수: {len(cs_info['creatives'])})")
    
    st.markdown("---")

    # Target Offer Selection (Multi-select)
    st.markdown("**Copy to Offers**")
    st.caption("Creative Set을 복사할 대상 Offer들을 선택하세요 (여러 개 선택 가능)")

    target_offer_options = {
        f"{o['offer_name']} (ID: {o['offer_id']})": o['offer_id'] 
        for o in offers
    }

    selected_target_offers = st.multiselect(
        "Target Offers",
        options=list(target_offer_options.keys()),
        key=f"mintegral_copy_target_offers_{idx}",
        help="Creative Set을 복사할 대상 Offer들을 선택하세요"
    )

    # Convert to list of offer IDs
    target_offer_ids = [target_offer_options[name] for name in selected_target_offers]

    # Show selected target offers
    if selected_target_offers:
        st.markdown("**복사 대상 Offers:**")
        for offer_name in selected_target_offers:
            st.write(f"• {offer_name}")

    # Save settings
    st.session_state.mintegral_settings[game] = {
        "mode": "copy",
        "selected_creative_sets": [creative_set_options[name] for name in selected_sets],
        "target_offer_ids": target_offer_ids,
        "target_offer_names": [name.split(" (ID:")[0] for name in selected_target_offers]
    }
        

# =========================================================
# Upload Logic
# =========================================================
def upload_creative_to_library(file_path: str, creative_type: str = "VIDEO", original_filename: str = None) -> Dict:
    """
    Upload a creative file to Mintegral library.
    
    Args:
        file_path: Local path to the file
        creative_type: "VIDEO", "IMAGE", or "PLAYABLE"
        original_filename: Original filename (optional, uses file_path basename if not provided)
    """
    try:
        headers = _get_auth_headers()
        headers_no_content_type = {k: v for k, v in headers.items() if k != "Content-Type"}
        
        # Determine endpoint
        if creative_type == "PLAYABLE":
            url = f"{MINTEGRAL_STORAGE_URL}/playable/upload"
        else:
            url = f"{MINTEGRAL_STORAGE_URL}/creatives/upload"
        
        # Use original filename if provided, otherwise use basename
        filename = original_filename or os.path.basename(file_path)
        
        # Open and upload file
        with open(file_path, 'rb') as f:
            files = {'file': (filename, f)}  # ← 수정: 원본 파일명 사용
            response = requests.post(url, headers=headers_no_content_type, files=files, timeout=300)
        
        response.raise_for_status()
        data = response.json()
        
        if data.get("code") != 200:
            return {
                "success": False,
                "error": data.get("msg", "Upload failed")
            }
        
        return {
            "success": True,
            "creative_md5": data["data"]["creative_md5"],
            "creative_name": data["data"]["creative_name"]
        }
        
    except Exception as e:
        logger.error(f"Failed to upload creative: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

def batch_upload_from_drive(
    folder_url: str,
    game: str,
    creative_type: str = "VIDEO",
    on_progress: Optional[Callable] = None
) -> Dict:
    """
    Import videos from Google Drive and upload to Mintegral library.
    
    Args:
        folder_url: Google Drive folder URL
        game: Game name
        creative_type: "VIDEO", "IMAGE", or "PLAYABLE"
        on_progress: Callback function(current, total, filename, status)
    
    Returns:
        Dict with success count, failed count, and errors
    """
    from modules.upload_automation import drive_import
    
    try:
        # Download from Drive
        st.info("📥 Downloading files from Google Drive...")
        files = drive_import.import_drive_folder_videos_parallel(
            folder_url,
            max_workers=4
        )
        
        if not files:
            return {
                "success": False,
                "error": "No files found in Drive folder"
            }
        
        # Upload to Mintegral
        total = len(files)
        success_count = 0
        failed_count = 0
        errors = []
        
        for idx, file_info in enumerate(files, 1):
            filename = file_info["name"]
            filepath = file_info["path"]
            
            if on_progress:
                on_progress(idx, total, filename, "uploading")
            
            result = upload_creative_to_library(filepath, creative_type)
            
            if result.get("success"):
                success_count += 1
                if on_progress:
                    on_progress(idx, total, filename, "success")
            else:
                failed_count += 1
                error_msg = result.get("error", "Unknown error")
                errors.append(f"{filename}: {error_msg}")
                if on_progress:
                    on_progress(idx, total, filename, f"failed: {error_msg}")
            
            # Clean up temp file
            try:
                os.unlink(filepath)
            except:
                pass
        
        return {
            "success": True,
            "total": total,
            "success_count": success_count,
            "failed_count": failed_count,
            "errors": errors
        }
        
    except Exception as e:
        logger.error(f"Batch upload failed: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

def batch_upload_to_library(files: List[Dict], max_workers: int = 3, on_progress: Optional[Callable] = None) -> Dict:
    """
    Upload multiple files to Mintegral library in parallel.
    
    Args:
        files: List of {"name": ..., "path": ...}
        max_workers: Number of parallel uploads (default 3)
        on_progress: Optional callback(filename, success, error_msg)
    """
    import pathlib
    
    def upload_one(file_info: Dict) -> Dict:
        """Upload a single file."""
        filename = file_info["name"]
        filepath = file_info["path"]
        
        # Auto-detect type
        ext = pathlib.Path(filename).suffix.lower()
        if ext in ['.mp4', '.mov', '.mkv', '.mpeg4']:
            creative_type = "VIDEO"
        elif ext in ['.png', '.jpg', '.jpeg', '.gif', '.webp']:
            creative_type = "IMAGE"
        elif ext in ['.zip', '.html']:
            creative_type = "PLAYABLE"
        else:
            creative_type = "VIDEO"
        
        result = upload_creative_to_library(filepath, creative_type, filename)
        return {
            "filename": filename,
            "success": result.get("success", False),
            "error": result.get("error")
        }
    
    results = []
    errors = []
    success_count = 0
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(upload_one, f): f for f in files}
        
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
                
                if result["success"]:
                    success_count += 1
                    logger.info(f"✅ Uploaded: {result['filename']}")
                    if on_progress:
                        on_progress(result['filename'], True, None)  # ← 추가
                else:
                    failed_count += 1
                    if result["error"]:
                        errors.append(f"{result['filename']}: {result['error']}")
                        logger.error(f"❌ Failed: {result['filename']} - {result['error']}")
                    if on_progress:
                        on_progress(result['filename'], False, result['error'])  # ← 추가
            except Exception as e:
                failed_count += 1
                file_info = futures[future]
                error_msg = f"{file_info['name']}: {str(e)}"
                errors.append(error_msg)
                logger.error(f"❌ Exception: {error_msg}")
                if on_progress:
                    on_progress(file_info['name'], False, str(e))  # ← 추가
    
    return {
        "total": len(files),
        "success": success_count,
        "failed": failed_count,
        "errors": errors,
        "results": results
    }


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
    logger.info(f"Uploading to Mintegral for {game} with settings: {settings}")
    
    mode = settings.get("mode", "upload")
    
    if mode == "upload":
        return _upload_creative_set(game, videos, settings)
    elif mode == "copy":
        return _copy_creative_sets(game, settings)
    
    return {
        "success": False,
        "error": "알 수 없는 모드입니다.",
        "errors": [f"Unknown mode: {mode}"]
    }

def _upload_creative_set(game: str, videos: List[Dict], settings: Dict) -> Dict:
    """Upload creative set to Mintegral."""
    
    # Step 1: API Config 체크
    try:
        config = _get_api_config()
        logger.info(f"🔑 API Config check:")
        logger.info(f"   - access_key exists: {bool(config.get('access_key'))}")
        logger.info(f"   - api_key exists: {bool(config.get('api_key'))}")
        logger.info(f"   - access_key length: {len(config.get('access_key', ''))}")
    except Exception as e:
        logger.error(f"❌ Failed to load API config: {e}", exc_info=True)
        return {
            "success": False,
            "error": f"❌ API 설정 로드 실패: {str(e)}",
            "errors": [str(e)]
        }
    
    # Step 2: 네트워크 연결 테스트
    try:
        logger.info("🌐 Testing network connection to Mintegral API...")
        test_response = requests.get("https://ss-api.mintegral.com", timeout=5)
        logger.info(f"✅ Network test OK: {test_response.status_code}")
    except Exception as e:
        logger.error(f"❌ Cannot reach Mintegral API: {e}", exc_info=True)
        return {
            "success": False,
            "error": f"❌ Mintegral API 접속 실패: {str(e)}",
            "errors": [f"Network error: {str(e)}"]
        }
    try:
        test_response = requests.get("https://ss-api.mintegral.com", timeout=5)
        logger.info(f"Network test: {test_response.status_code}")
    except Exception as e:
        logger.error(f"Cannot reach Mintegral API: {e}")
    # Validate required settings
    offer_id = settings.get("selected_offer_id")
    if not offer_id:
        return {
            "success": False,
            "error": "Offer를 선택해주세요.",
            "errors": ["Offer ID가 필요합니다."]
        }
    
    creative_set_name = settings.get("creative_set_name", "")
    if not creative_set_name:
        creative_set_name = _get_default_creative_set_name(game)
    
    # Collect all selected creatives (Images, Videos, Playables)
    all_creatives_md5 = []
    all_creatives_md5.extend(settings.get("selected_images", []))
    all_creatives_md5.extend(settings.get("selected_videos", []))
    all_creatives_md5.extend(settings.get("selected_playables", []))

    # ✅ 중복 제거 (순서 유지)
    seen = set()
    unique_md5 = []
    for md5 in all_creatives_md5:
        if md5 not in seen:
            seen.add(md5)
            unique_md5.append(md5)
    all_creatives_md5 = unique_md5

    # Add Product Icon if provided (and not duplicate)
    product_icon_md5 = settings.get("product_icon_md5")
    if product_icon_md5:
        if product_icon_md5 not in seen:
            all_creatives_md5.insert(0, product_icon_md5)
            logger.info(f"Product icon added to creative set: {product_icon_md5}")
        else:
            logger.info(f"Product icon already selected, skipping duplicate")

    logger.info(f"📊 Total unique creatives: {len(all_creatives_md5)}")

    if not all_creatives_md5:
        return {
            "success": False,
            "error": "선택된 Creative가 없습니다.",
            "errors": ["최소 1개 이상의 Creative를 선택해주세요."]
        }

    try:
        all_images = get_creatives(creative_type="IMAGE", game_filter=None, max_pages=5)
        all_videos = get_creatives(creative_type="VIDEO", game_filter=None, max_pages=5)
        all_playables = get_creatives(creative_type="PLAYABLE", game_filter=None, max_pages=5)
        
        # Build MD5 -> {name, type} mapping
        md5_info = {}
        for creative in all_images:
            md5_info[creative["creative_md5"]] = {
                "name": creative["creative_name"],
                "type": "IMAGE",
                "dimension": creative.get("dimension", "")
            }
        for creative in all_videos:
            md5_info[creative["creative_md5"]] = {
                "name": creative["creative_name"],
                "type": "VIDEO",
                "dimension": creative.get("dimension", "")
            }
        for creative in all_playables:
            md5_info[creative["creative_md5"]] = {
                "name": creative["creative_name"],
                "type": "PLAYABLE",
                "dimension": creative.get("dimension", "")
            }
        
        # Build creatives array with names
        creatives_payload = []
        has_image = False
        has_video = False
        has_playable = False
        
        for md5 in all_creatives_md5:
            info = md5_info.get(md5)
            if not info:
                logger.warning(f"Creative MD5 not found: {md5}")
                continue
            
            creatives_payload.append({
                "creative_md5": md5,
                "creative_name": info["name"]
            })
            
            # Track creative types
            if info["type"] == "IMAGE":
                has_image = True
            elif info["type"] == "VIDEO":
                has_video = True
            elif info["type"] == "PLAYABLE":
                has_playable = True
        
        logger.info(f"Built {len(creatives_payload)} creatives with names")
        logger.info(f"Creative types: IMAGE={has_image}, VIDEO={has_video}, PLAYABLE={has_playable}")

    except Exception as e:
        logger.error(f"Failed to fetch creative names: {e}")
        return {
            "success": False,
            "error": f"Creative 정보 조회 실패: {str(e)}",
            "errors": [str(e)]
        }
    
    # Build ad_outputs dynamically based on selected creatives
    ad_outputs = []

    # Collect available dimensions
    available_images = {}  # dimension -> count
    available_videos = {}  # dimension -> count
    has_playable = False

    for md5 in all_creatives_md5:
        info = md5_info.get(md5)
        if not info:
            continue
        
        if info["type"] == "IMAGE":
            dim = info["dimension"]
            available_images[dim] = available_images.get(dim, 0) + 1
        elif info["type"] == "VIDEO":
            dim = info["dimension"]
            available_videos[dim] = available_videos.get(dim, 0) + 1
        elif info["type"] == "PLAYABLE":
            has_playable = True

    logger.info(f"📊 Available creatives:")
    logger.info(f"   - Images: {available_images}")
    logger.info(f"   - Videos: {available_videos}")
    logger.info(f"   - Playable: {has_playable}")

    # Ad Output mapping (based on Mintegral documentation)
    # 111: Native - Image + Icon (any size)
    # 121: Interstitial - Image (any size)
    # 122: Interstitial - Image (768x1024 or 1200x627/628)
    # 131: Banner - Image (320x50 or 640x120)
    # 132: Banner - Image (768x1024 or 1200x627/628)
    # 211: Native - Video Portrait (1080x1920)
    # 212: Native - Video Landscape (1920x1080)
    # 213: Native - Video Square (1080x1080)
    # 221: Interstitial - Video (any)
    # 231: Banner - Video (any)
    # 311: Playable

    # Auto-generate ad_outputs based on available creatives
    if available_images:
        ad_outputs.append(111)  # Native always supported
        ad_outputs.append(121)  # Interstitial always supported
        
        # Banner 131 (320x50 or 640x120)
        if "320x50" in available_images or "640x120" in available_images:
            ad_outputs.append(131)
        
        # Banner 132 and Interstitial 122 (768x1024 or 1200x627/628)
        if any(dim in available_images for dim in ["768x1024", "1200x627", "1200x628"]):
            ad_outputs.append(122)
            ad_outputs.append(132)

    if available_videos:
        ad_outputs.append(221)  # Interstitial video
        ad_outputs.append(231)  # Banner video
        
        # Native video based on dimensions
        if "1080x1920" in available_videos:
            ad_outputs.append(211)
        if "1920x1080" in available_videos:
            ad_outputs.append(212)
        if "1080x1080" in available_videos:
            ad_outputs.append(213)

    if has_playable:
        ad_outputs.append(311)

    if not ad_outputs:
        return {
            "success": False,
            "error": "선택된 Creative로는 ad_output을 생성할 수 없습니다.",
            "errors": ["No valid ad_outputs can be generated from selected creatives"]
        }

    logger.info(f"✅ Auto-generated ad_outputs: {ad_outputs}")
    
    # API Request
    try:
        headers = _get_auth_headers()
        payload = {
            "creative_set_name": creative_set_name,
            "offer_id": int(offer_id),
            "geos": ["ALL"],
            "ad_outputs": ad_outputs,  # ← 자동 선택된 값
            "creatives": creatives_payload
        }
        
        logger.info(f"📤 Sending API request:")
        logger.info(f"   - Payload: {payload}")
        
        response = requests.post(
            f"{MINTEGRAL_BASE_URL}/creative_set",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        # 디버깅: 응답 로깅
        logger.info(f"📥 API Response:")
        logger.info(f"   - Status Code: {response.status_code}")
        logger.info(f"   - Response Text: {response.text}")
        
        response.raise_for_status()
        data = response.json()
        
        if data.get("code") != 200:
            error_msg = data.get("msg", "Creative Set 생성 실패")
            error_detail = data.get("data")  # ← 추가: data 필드 확인
            logger.error(f"Creative Set creation failed: {error_msg}")
            logger.error(f"Error details: {error_detail}")  # ← 추가
            return {
                "success": False,
                "error": error_msg,
                "errors": [f"{error_msg} - {error_detail}" if error_detail else error_msg]
            }
            
        logger.info(f"✅ Creative Set created: {creative_set_name} with {len(creatives_payload)} creatives")
        
        return {
            "success": True,
            "message": f"Creative Set '{creative_set_name}'이(가) 성공적으로 생성되었습니다! ({len(creatives_payload)}개 creative)",
            "creative_set_name": creative_set_name,
            "offer_id": offer_id,
            "total_creatives": len(creatives_payload)
        }
        
    except requests.exceptions.ConnectionError as e:
        logger.error(f"❌ Connection Error: {e}", exc_info=True)
        return {
            "success": False,
            "error": f"❌ API 연결 실패 (네트워크 차단 가능성): {str(e)}",
            "errors": [str(e)]
        }
    except requests.exceptions.Timeout as e:
        logger.error(f"❌ Timeout: {e}", exc_info=True)
        return {
            "success": False,
            "error": f"❌ API 타임아웃 (30초 초과): {str(e)}",
            "errors": [str(e)]
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Request Error: {e}", exc_info=True)
        return {
            "success": False,
            "error": f"❌ API 요청 실패: {str(e)}",
            "errors": [str(e)]
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return {
            "success": False,
            "error": f"예상치 못한 오류: {str(e)}",
            "errors": [str(e)]
        }

def _copy_creative_sets(game: str, settings: Dict) -> Dict:
    """Copy creative sets to target offers."""
    
    selected_sets = settings.get("selected_creative_sets", [])
    target_offer_ids = settings.get("target_offer_ids", [])
    
    if not selected_sets or not target_offer_ids:
        return {
            "success": False,
            "error": "Creative Set 또는 Target Offer가 선택되지 않았습니다.",
            "errors": []
        }
    
    total_copies = len(selected_sets) * len(target_offer_ids)
    success_count = 0
    failed_count = 0
    errors = []
    
    logger.info(f"Copying {len(selected_sets)} creative set(s) to {len(target_offer_ids)} offer(s)")
    
    for creative_set in selected_sets:
        creative_set_name = creative_set["creative_set_name"]
        ad_outputs = creative_set["ad_outputs"]
        geos = creative_set["geos"]
        creatives = creative_set["creatives"]
        
        # Build creatives payload (only MD5 and name)
        creatives_payload = [
            {
                "creative_md5": c["creative_md5"],
                "creative_name": c["creative_name"]
            }
            for c in creatives
        ]
        
        for target_offer_id in target_offer_ids:
            try:
                headers = _get_auth_headers()
                payload = {
                    "creative_set_name": creative_set_name,
                    "offer_id": int(target_offer_id),
                    "geos": geos,
                    "ad_outputs": ad_outputs,
                    "creatives": creatives_payload
                }
                
                response = requests.post(
                    f"{MINTEGRAL_BASE_URL}/creative_set",
                    headers=headers,
                    json=payload,
                    timeout=30
                )
                
                response.raise_for_status()
                data = response.json()

                logger.info(f"📥 API Response for Offer {target_offer_id}:")
                logger.info(f"   - Status Code: {response.status_code}")
                logger.info(f"   - Response: {response.text}")

                if data.get("code") == 200:
                    success_count += 1
                    logger.info(f"✅ Copied '{creative_set_name}' to Offer {target_offer_id}")
                else:
                    failed_count += 1
                    error_msg = data.get("msg") or data.get("message") or "Unknown error"
                    error_detail = data.get("data")  # ← 추가
                    
                    if error_detail:
                        full_error = f"{error_msg} - {error_detail}"
                        errors.append(f"Offer {target_offer_id}: {full_error}")
                        logger.error(f"❌ Failed to copy to Offer {target_offer_id}: {full_error}")
                    else:
                        errors.append(f"Offer {target_offer_id}: {error_msg}")
                        logger.error(f"❌ Failed to copy to Offer {target_offer_id}: {error_msg}")
                    
            except Exception as e:
                failed_count += 1
                errors.append(f"Offer {target_offer_id}: {str(e)}")
                logger.error(f"❌ Exception copying to Offer {target_offer_id}: {e}")

# REPLACE THIS BLOCK WITH ABOVE IF WE WANT TO ADD _COPY1,....COPY9 WHEN THERE IS AN EXISTING CREATIVE SET NAME IN THE OFFER WE ARE TRYING TO COPY
# for target_offer_id in target_offer_ids:
#     # Try with original name, then auto-rename if duplicate
#     base_name = creative_set_name
#     attempt = 0
#     final_name = base_name
    
#     while attempt < 10:
#         try:
#             headers = _get_auth_headers()
#             payload = {
#                 "creative_set_name": final_name,  # ← Use unique name
#                 "offer_id": int(target_offer_id),
#                 "geos": geos,
#                 "ad_outputs": ad_outputs,
#                 "creatives": creatives_payload
#             }
            
#             response = requests.post(
#                 f"{MINTEGRAL_BASE_URL}/creative_set",
#                 headers=headers,
#                 json=payload,
#                 timeout=30
#             )
            
#             response.raise_for_status()
#             data = response.json()
            
#             logger.info(f"📥 API Response for Offer {target_offer_id} (attempt {attempt + 1}):")
#             logger.info(f"   - Response: {response.text}")
            
#             if data.get("code") == 200:
#                 success_count += 1
#                 logger.info(f"✅ Copied '{final_name}' to Offer {target_offer_id}")
#                 break  # Success, exit retry loop
            
#             elif data.get("code") == 40002:
#                 # Duplicate name, try with suffix
#                 attempt += 1
#                 final_name = f"{base_name}_copy{attempt}"
#                 logger.info(f"⚠️ Name conflict, retrying as '{final_name}'")
#                 continue  # Retry with new name
            
#             else:
#                 # Other error, don't retry
#                 failed_count += 1
#                 error_msg = data.get("msg") or data.get("message") or "Unknown error"
#                 errors.append(f"Offer {target_offer_id}: {error_msg}")
#                 logger.error(f"❌ Failed to copy to Offer {target_offer_id}: {error_msg}")
#                 break
                
#         except Exception as e:
#             failed_count += 1
#             errors.append(f"Offer {target_offer_id}: {str(e)}")
#             logger.error(f"❌ Exception copying to Offer {target_offer_id}: {e}")
#             break
    
    if success_count > 0:
        return {
            "success": True,
            "message": f"{success_count}/{total_copies} Creative Set(s) 복사 완료",
            "total": total_copies,
            "success_count": success_count,
            "failed_count": failed_count,
            "errors": errors
        }
    else:
        return {
            "success": False,
            "error": f"모든 복사 실패 ({failed_count}/{total_copies})",
            "errors": errors
        }