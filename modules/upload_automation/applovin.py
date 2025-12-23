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

def _extract_number_from_asset(asset_id: str, asset_list: List[Dict], include_subname: bool = False) -> str:
    """
    Extract number (and optionally subname) from asset name.
    
    Examples:
    - "video123_pizzaidle_en.mp4" -> "123"
    - "playable035_pizzaidle_applovin.html" -> "035"
    - "playable035skipintro_pizzaidle_applovin.html" -> "035skipintro" (if include_subname=True)
    
    Args:
        asset_id: Asset ID to look up
        asset_list: List of assets to search in
        include_subname: If True, include subname part (e.g., "skipintro")
    """
    import re
    
    # asset_id로 asset 찾기
    asset = next((a for a in asset_list if a['id'] == asset_id), None)
    if not asset:
        return asset_id  # fallback
    
    name = asset.get('name', '')
    
    if include_subname:
        # playable035skipintro 같은 패턴 추출 (subname 포함)
        # playable + 숫자 + (선택적 알파벳) 형태
        match = re.search(r'(playable\d+[a-zA-Z]*)', name, re.IGNORECASE)
        if match:
            return match.group(1).replace('playable', '')  # "035skipintro"
        
        # video는 subname 없음
        match = re.search(r'video(\d+)', name, re.IGNORECASE)
        if match:
            return match.group(1)
    else:
        # 숫자만 추출 (기존 로직)
        match = re.search(r'(?:video|playable)(\d+)', name, re.IGNORECASE)
        if match:
            return match.group(1)
    
    # 일반적인 숫자 패턴 (fallback)
    match = re.search(r'(\d+)', name)
    if match:
        return match.group(1)
    
    return asset_id  # fallback


def _generate_creative_name(video_ids: List[str], playable_ids: List[str], assets: Dict) -> str:
    """
    Generate creative set name based on selected videos and playables.
    
    Rules:
    - 1 video + 1 playable: video123_playable456 or video123_playable456skipintro
    - Multiple videos + 1 playable: video100-109_playable456
    - 1 video + Multiple playables: video123_playabletop{count}
    - Multiple videos + Multiple playables: video100-109_playabletop{count}
    """
    import re
    
    if not video_ids and not playable_ids:
        return ""
    
    parts = []
    
    # Video 부분
    if video_ids:
        if len(video_ids) == 1:
            video_num = _extract_number_from_asset(video_ids[0], assets['videos'])
            parts.append(f"video{video_num}")
        else:
            # 여러 개: 숫자만 추출해서 최소-최대 계산
            video_nums = []
            for vid in video_ids:
                num_str = _extract_number_from_asset(vid, assets['videos'])
                # 숫자만 추출 (문자 제거)
                match = re.search(r'(\d+)', num_str)
                if match:
                    video_nums.append(int(match.group(1)))
            
            if video_nums:
                min_num = min(video_nums)
                max_num = max(video_nums)
                parts.append(f"video{min_num}-{max_num}")
            else:
                parts.append(f"video{len(video_ids)}items")
    
    # Playable 부분
    if playable_ids:
        if len(playable_ids) == 1:
            # 단일 playable: subname 포함
            playable_num = _extract_number_from_asset(
                playable_ids[0], 
                assets['playables'], 
                include_subname=True
            )
            parts.append(f"playable{playable_num}")
        else:
            # 여러 개: playabletop{count}
            parts.append(f"playabletop{len(playable_ids)}")
    
    return "_".join(parts)

def _upload_creative_set(game: str, idx: int, status: str = "PAUSED"):
    """
    Upload creative set to Applovin campaign.
    
    Args:
        game: Game name
        idx: Tab index for unique keys
        status: "PAUSED" or "LIVE"
    """
    settings = get_applovin_settings(game)
    
    if not settings:
        st.error("⚠️ Applovin 설정이 없습니다.")
        return
    
    campaign_id = settings.get("campaign_id")
    creative_action = settings.get("creative_action")
    
    if not campaign_id:
        st.error("⚠️ Campaign을 선택해주세요.")
        return
    
    if creative_action == "Create":
        video_ids = settings.get("video_ids", [])
        playable_ids = settings.get("playable_ids", [])
        creative_name = settings.get("generated_name", "")
        
        if not video_ids and not playable_ids:
            st.error("⚠️ Video 또는 Playable을 선택해주세요.")
            return
        
        if not creative_name:
            st.error("⚠️ Creative Set 이름이 필요합니다.")
            return
        
        with st.spinner(f"Uploading creative set as {status}..."):
            try:
                # API 호출
                result = _create_creative_set_api(
                    campaign_id=campaign_id,
                    name=creative_name,
                    video_ids=video_ids,
                    playable_ids=playable_ids,
                    status=status
                )
                
                if result.get("success"):
                    st.success(f"✅ Creative set '{creative_name}' uploaded as {status}!")
                    st.info(f"Creative Set ID: {result.get('id')}")
                else:
                    st.error(f"❌ Upload failed: {result.get('error')}")
            except Exception as e:
                logger.error(f"Failed to upload creative set: {e}", exc_info=True)
                st.error(f"❌ Upload error: {e}")
    
    elif creative_action == "Import":
        st.warning("⚠️ Import 기능은 아직 구현되지 않았습니다.")


def _create_creative_set_api(
    campaign_id: str,
    name: str,
    video_ids: List[str],
    playable_ids: List[str],
    status: str = "PAUSED"
) -> Dict:
    """
    Call Applovin API to create creative set.
    
    Returns:
        Dict with success, id, error
    """
    try:
        config = _get_api_config()
        headers = {
            "Authorization": config["api_key"],
            "Content-Type": "application/json"
        }
        
        # Creative set payload
        payload = {
            "campaign_id": campaign_id,
            "type": "APP",
            "name": name,
            "status": status,
            "assets": [],
            "languages": ["ENGLISH"],  # TODO: 설정 가능하게
            "countries": []  # 빈 배열 = 모든 국가
        }
        
        # Add video assets
        for vid in video_ids:
            payload["assets"].append({"id": vid})
        
        # Add playable assets
        for pid in playable_ids:
            payload["assets"].append({"id": pid})
        
        logger.info(f"Creating creative set: {name} with {len(video_ids)} videos, {len(playable_ids)} playables")
        
        response = requests.post(
            f"{APPLOVIN_BASE_URL}/creative_set/create",
            headers=headers,
            params={"account_id": config["account_id"]},
            json=payload,
            timeout=30
        )
        
        response.raise_for_status()
        result = response.json()
        
        logger.info(f"Creative set created: {result}")
        
        return {
            "success": True,
            "id": result.get("id"),
            "version": result.get("version")
        }
        
    except requests.exceptions.HTTPError as e:
        error_msg = f"API error: {e.response.status_code}"
        if e.response.text:
            error_msg += f" - {e.response.text}"
        logger.error(error_msg)
        return {"success": False, "error": error_msg}
    except Exception as e:
        logger.error(f"Failed to create creative set: {e}", exc_info=True)
        return {"success": False, "error": str(e)}

def _upload_assets_to_media_library(files: List[Dict], max_workers: int = 3) -> Dict:
    """
    Upload video/playable files to Applovin Media Library.
    
    Args:
        files: List of dicts with 'name' and 'path' keys
        max_workers: Parallel upload workers
        
    Returns:
        Dict with uploaded_ids, failed, errors
    """
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    config = _get_api_config()
    headers = {"Authorization": config["api_key"]}
    account_id = config["account_id"]
    
    uploaded_ids = []
    failed = 0
    errors = []
    
    def upload_single_file(file_info):
        try:
            file_path = file_info.get("path")
            file_name = file_info.get("name")
            
            # Determine content type
            if file_name.lower().endswith(('.mp4', '.mov')):
                content_type = 'video/mp4'
            elif file_name.lower().endswith('.html'):
                content_type = 'text/html'
            else:
                return {"success": False, "error": f"Unsupported file type: {file_name}"}
            
            # Read file
            with open(file_path, 'rb') as f:
                files_payload = {
                    'files': (file_name, f, content_type)
                }
                
                response = requests.post(
                    f"{APPLOVIN_BASE_URL}/asset/upload",
                    headers=headers,
                    params={"account_id": account_id},
                    files=files_payload,
                    timeout=120  # 2분 타임아웃 (큰 파일 대비)
                )
                
                response.raise_for_status()
                result = response.json()
                upload_id = result.get("upload_id")
                
                if not upload_id:
                    return {"success": False, "error": f"No upload_id returned for {file_name}"}
                
                # Poll upload status
                max_attempts = 30  # 최대 30번 체크 (30초)
                for attempt in range(max_attempts):
                    time.sleep(1)
                    
                    status_response = requests.get(
                        f"{APPLOVIN_BASE_URL}/asset/upload_result",
                        headers=headers,
                        params={
                            "account_id": account_id,
                            "upload_id": upload_id
                        },
                        timeout=30
                    )
                    status_response.raise_for_status()
                    status_data = status_response.json()
                    
                    upload_status = status_data.get("upload_status")
                    
                    if upload_status == "FINISHED":
                        details = status_data.get("details", [])
                        if details and details[0].get("file_status") == "SUCCESS":
                            asset_id = details[0].get("id")
                            return {
                                "success": True,
                                "asset_id": asset_id,
                                "name": file_name
                            }
                        else:
                            error_msg = details[0].get("error_message", "Unknown error")
                            return {"success": False, "error": f"{file_name}: {error_msg}"}
                    
                    elif upload_status == "PENDING":
                        continue  # Keep polling
                    else:
                        return {"success": False, "error": f"{file_name}: Unknown status {upload_status}"}
                
                return {"success": False, "error": f"{file_name}: Upload timeout"}
                
        except Exception as e:
            logger.error(f"Failed to upload {file_info.get('name')}: {e}", exc_info=True)
            return {"success": False, "error": f"{file_info.get('name')}: {str(e)}"}
    
    # Parallel upload
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(upload_single_file, f): f for f in files}
        
        for future in as_completed(futures):
            result = future.result()
            if result.get("success"):
                uploaded_ids.append({
                    "id": result["asset_id"],
                    "name": result["name"]
                })
            else:
                failed += 1
                errors.append(result.get("error", "Unknown error"))
    
    return {
        "uploaded_ids": uploaded_ids,
        "total": len(uploaded_ids),
        "failed": failed,
        "errors": errors
    }

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
    """Render Applovin settings panel with lazy loading."""
    _ensure_applovin_settings_state()
    cur = get_applovin_settings(game) or {}
    
    with container:
        st.markdown(f"#### {game} Applovin Settings")
        
        # Lazy loading: 버튼으로 명시적 로드
        campaigns_key = f"applovin_campaigns_{game}"
        assets_key = f"applovin_assets_{game}"
        
        # 데이터가 이미 로드되었는지 확인
        is_loaded = campaigns_key in st.session_state
        
        if not is_loaded:
            if st.button(f"📥 Load Applovin Data", key=f"applovin_load_{idx}"):
                with st.spinner("Loading campaigns and assets..."):
                    # Fetch campaigns
                    campaigns = get_campaigns(game=game)
                    st.session_state[campaigns_key] = campaigns
                    
                    if campaigns:
                        st.success(f"✅ Loaded {len(campaigns)} campaigns")
                    else:
                        st.warning("⚠️ No campaigns found")
                        return
                    
                    # Fetch assets (Create 모드에서 필요)
                    assets = get_assets(game=game)
                    st.session_state[assets_key] = assets
                    st.success(f"✅ Loaded {len(assets['videos'])} videos, {len(assets['playables'])} playables")
                    
                    # 강제 리렌더링
                    st.rerun()
            else:
                st.info("👆 Click to load Applovin data")
                return
        
        # 로드된 데이터 가져오기
        campaigns = st.session_state.get(campaigns_key, [])
        
        if not campaigns:
            st.warning("⚠️ No campaigns available")
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
        
        # Create 선택 시 Videos와 Playables 멀티 선택
        selected_video_ids = []
        selected_playable_ids = []
        
        if creative_action == "Create":
            assets = st.session_state.get(assets_key, {"videos": [], "playables": []})
            
            # 현재 선택된 항목 (session_state에서 가져오기)
            current_videos = cur.get("video_ids", [])
            current_playables = cur.get("playable_ids", [])
            
            # Videos 섹션
            st.markdown("##### 📹 Videos (최대 10개)")
            
            if assets["videos"]:
                video_options = {
                    f"{v['name']} (ID: {v['id']})": v['id']
                    for v in assets["videos"]
                }
                
                default_video_labels = [
                    label for label, vid in video_options.items() 
                    if vid in current_videos
                ]
                
                selected_video_labels = st.multiselect(
                    "Video 선택 (최대 10개)",
                    options=list(video_options.keys()),
                    default=default_video_labels,
                    max_selections=10,
                    key=f"applovin_videos_{idx}",
                )
                
                selected_video_ids = [video_options[label] for label in selected_video_labels]
                
                if selected_video_ids:
                    st.write(f"**선택됨: {len(selected_video_ids)}개**")
                    cols = st.columns(5)
                    for i, vid in enumerate(selected_video_ids):
                        with cols[i % 5]:
                            video_name = next(
                                (v['name'] for v in assets['videos'] if v['id'] == vid),
                                vid
                            )
                            display_name = video_name[:20] + "..." if len(video_name) > 20 else video_name
                            st.caption(f"🎬 {display_name}")
            else:
                st.warning(f"⚠️ {game}에 해당하는 Video asset이 없습니다.")
            
            st.markdown("---")
            
            # Playables 섹션 (Videos 다음에!)
            st.markdown("##### 🎮 Playables (최대 10개)")
            
            if assets["playables"]:
                playable_options = {
                    f"{p['name']} (ID: {p['id']})": p['id']
                    for p in assets["playables"]
                }
                
                default_playable_labels = [
                    label for label, pid in playable_options.items() 
                    if pid in current_playables
                ]
                
                selected_playable_labels = st.multiselect(
                    "Playable 선택 (최대 10개)",
                    options=list(playable_options.keys()),
                    default=default_playable_labels,
                    max_selections=10,
                    key=f"applovin_playables_{idx}",
                )
                
                selected_playable_ids = [playable_options[label] for label in selected_playable_labels]
                
                if selected_playable_ids:
                    st.write(f"**선택됨: {len(selected_playable_ids)}개**")
                    cols = st.columns(5)
                    for i, pid in enumerate(selected_playable_ids):
                        with cols[i % 5]:
                            playable_name = next(
                                (p['name'] for p in assets['playables'] if p['id'] == pid),
                                pid
                            )
                            display_name = playable_name[:20] + "..." if len(playable_name) > 20 else playable_name
                            st.caption(f"🎮 {display_name}")
            else:
                st.warning(f"⚠️ {game}에 해당하는 Playable asset이 없습니다.")
            
            st.markdown("---")
            
            # Creative Name 설정
            st.markdown("##### 📝 Creative Set Name")
            
            # 자동 생성된 이름 먼저 계산
            auto_generated_name = _generate_creative_name(
                selected_video_ids, 
                selected_playable_ids,
                assets
            )
            
            # 텍스트 입력 (placeholder에 자동 생성 이름 표시)
            custom_name = st.text_input(
                "Creative Set Name (비워두면 자동 생성)",
                value=cur.get("custom_name", ""),
                placeholder=auto_generated_name if auto_generated_name else "예: video123_playable456",
                key=f"applovin_custom_name_{idx}",
                help="입력하지 않으면 자동으로 이름이 생성됩니다"
            )
            
            # 최종 이름 결정
            if custom_name.strip():
                creative_name = custom_name.strip()
                st.success(f"✅ 사용할 이름: `{creative_name}`")
            else:
                creative_name = auto_generated_name
                if creative_name:
                    st.info(f"ℹ️ 자동 생성 이름: `{creative_name}`")
                else:
                    creative_name = ""
        
        # Save settings
        # Save settings
        st.session_state.applovin_settings[game] = {
            "campaign_id": str(campaign_id),
            "creative_action": creative_action,
            "video_ids": selected_video_ids if creative_action == "Create" else [],
            "playable_ids": selected_playable_ids if creative_action == "Create" else [],
            "custom_name": custom_name.strip() if creative_action == "Create" else "",
            "generated_name": creative_name if creative_action == "Create" else "",
        }
        
        st.markdown("---")
        
        # Upload buttons (양옆 배치)
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button(
                "⏸️ Save as Paused",
                key=f"applovin_upload_paused_{idx}",
                use_container_width=True,
                type="secondary"
            ):
                _upload_creative_set(game, idx, status="PAUSED")
        
        with col2:
            if st.button(
                "▶️ Save as Live",
                key=f"applovin_upload_live_{idx}",
                use_container_width=True,
                type="primary"
            ):
                _upload_creative_set(game, idx, status="LIVE")
