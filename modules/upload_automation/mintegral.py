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
        st.session_state[f"mintegral_video_pages_{idx}"] = 20  # 기본 10페이지
    if f"mintegral_image_pages_{idx}" not in st.session_state:
        st.session_state[f"mintegral_image_pages_{idx}"] = 20  
    if f"mintegral_playable_pages_{idx}" not in st.session_state:
        st.session_state[f"mintegral_playable_pages_{idx}"] = 5  # 기본 5페이지
    
    # Initialize selected lists
    selected_image_md5s = []
    selected_video_md5s = []
    selected_playable_md5s = []

    # Image creatives (추가)
    with st.expander("📷 Image Creatives", expanded=False):
        image_pages = st.session_state[f"mintegral_image_pages_{idx}"]
        
        with st.spinner(f"Loading images... ({image_pages}페이지)"):
            images = get_creatives(
                creative_type="IMAGE", 
                game_filter=game_short, 
                max_pages=image_pages
            )
        
        if images:
            st.caption(f"📊 총 {len(images)}개 표시 (최대 {image_pages * 200}개 중 필터링)")
            
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
                    st.session_state[f"mintegral_image_pages_{idx}"] += 10
                    st.cache_data.clear()
                    st.rerun()
            with col2:
                st.caption(f"💡 원하는 image가 없으면 '더 보기' 클릭")
        else:
            st.info(f"'{game_short}' 필터링된 Image가 없습니다")
            if st.button("🔍 더 많은 페이지 검색 (20페이지)", key=f"search_more_images_{idx}"):
                st.session_state[f"mintegral_image_pages_{idx}"] += 20
                st.cache_data.clear()
                st.rerun()
        
    # Video creatives
    with st.expander("🎥 Video Creatives", expanded=False):
        video_pages = st.session_state[f"mintegral_video_pages_{idx}"]
        
        with st.spinner(f"Loading videos... ({video_pages}페이지)"):
            videos = get_creatives(
                creative_type="VIDEO", 
                game_filter=game_short, 
                max_pages=video_pages
            )
        
        if videos:
            st.caption(f"📊 총 {len(videos)}개 표시 (최대 {video_pages * 200}개 중 필터링)")
            
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
                    st.session_state[f"mintegral_video_pages_{idx}"] += 10
                    st.cache_data.clear()
                    st.rerun()
            with col2:
                st.caption(f"💡 원하는 video가 없으면 '더 보기' 클릭")
        else:
            st.info(f"'{game_short}' 필터링된 Video가 없습니다")
            if st.button("🔍 더 많은 페이지 검색 (20페이지)", key=f"search_more_videos_{idx}"):
                st.session_state[f"mintegral_video_pages_{idx}"] += 20
                st.cache_data.clear()
                st.rerun()
    
    # Playable creatives
    with st.expander("🎮 Playable Creatives", expanded=False):
        playable_pages = st.session_state[f"mintegral_playable_pages_{idx}"]
        
        with st.spinner(f"Loading playables... ({playable_pages}페이지)"):
            playables = get_creatives(
                creative_type="PLAYABLE", 
                game_filter=game_short, 
                max_pages=playable_pages
            )
        
        if playables:
            st.caption(f"📊 총 {len(playables)}개 표시 (최대 {playable_pages * 200}개 중 필터링)")
            
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
                if st.button("➕ 더 보기(5페이지)", key=f"load_more_playables_{idx}"):
                    st.session_state[f"mintegral_playable_pages_{idx}"] += 5
                    st.cache_data.clear()
                    st.rerun()
            with col2:
                st.caption(f"💡 원하는 playable이 없으면 '더 보기' 클릭")
        else:
            st.info(f"'{game_short}' 필터링된 Playable이 없습니다")
            if st.button("🔍 더 많은 페이지 검색 (10페이지)", key=f"search_more_playables_{idx}"):
                st.session_state[f"mintegral_playable_pages_{idx}"] += 10
                st.cache_data.clear()
                st.rerun()
    
    st.markdown("---")
    
    # Apply in Offer dropdown
    st.markdown("**Apply in Offer**")
    with st.spinner("Loading offers..."):
        offers = get_offers(game_filter=game_short, max_pages=5)

    selected_offer_id = None
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
    else:
        st.warning(f"'{game_short}' 필터링된 Offer가 없습니다")
    
    # Save settings
    st.session_state.mintegral_settings[game] = {
        "mode": "upload",
        "creative_set_name": creative_set_name,
        "selected_images": selected_image_md5s,
        "selected_videos": selected_video_md5s,
        "selected_playables": selected_playable_md5s,
        "selected_offer_id": selected_offer_id,
    }

def _render_copy_creative_set(game: str, idx: int, cur: Dict) -> None:
    """Render Copy Creative Set UI."""
    st.info("🚧 Copy Creative Set 기능은 구현 예정입니다")
    
    # Placeholder settings
    st.session_state.mintegral_settings[game] = {
        "mode": "copy",
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
        return {
            "success": False,
            "error": "Copy Creative Set 기능은 아직 구현되지 않았습니다.",
            "errors": ["Copy 기능 구현 예정"]
        }
    
    return {
        "success": False,
        "error": "알 수 없는 모드입니다.",
        "errors": [f"Unknown mode: {mode}"]
    }

def _upload_creative_set(game: str, videos: List[Dict], settings: Dict) -> Dict:
    """Upload creative set to Mintegral."""
    
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
    
    # Collect all selected creatives
    all_creatives = []
    all_creatives.extend(settings.get("selected_images", []))
    all_creatives.extend(settings.get("selected_videos", []))
    all_creatives.extend(settings.get("selected_playables", []))
    
    if not all_creatives:
        return {
            "success": False,
            "error": "선택된 Creative가 없습니다.",
            "errors": ["최소 1개 이상의 Creative를 선택해주세요."]
        }
    
    # TODO: Implement actual API call to create creative set
    logger.warning(f"Mintegral upload not yet fully implemented. Would create set '{creative_set_name}' with {len(all_creatives)} creatives for offer {offer_id}")
    
    return {
        "success": False,
        "error": "Mintegral Creative Set 생성 API는 아직 구현 중입니다.",
        "errors": ["API 통합 작업 진행 중"],
        "message": f"Creative Set '{creative_set_name}'을(를) {len(all_creatives)}개 creative로 생성할 준비가 되었습니다."
    }