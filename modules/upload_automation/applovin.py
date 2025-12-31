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

from datetime import datetime, timedelta, timezone
logger = logging.getLogger(__name__)


APPLOVIN_BASE_URL = "https://api.ads.axon.ai/manage/v1"
  
def _get_api_config():
    """Get Applovin API configuration from secrets."""
    return {
        "api_key": st.secrets["applovin"]["campaign_management_api_key"],
        "reporting_api_key": st.secrets["applovin"].get("reporting_api_key", ""),
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

    if creative_action == "Import":
        source_campaign_id = settings.get("source_campaign_id")
        creative_set_ids = settings.get("selected_creative_set_ids", [])
        
        if not creative_set_ids:
            st.error("⚠️ Import할 Creative Set을 선택해주세요.")
            return
        
        with st.spinner(f"Importing {len(creative_set_ids)} creative set(s) as {status}..."):
            try:
                result = _clone_creative_sets_api(
                    source_campaign_id=source_campaign_id,
                    target_campaign_id=campaign_id,
                    creative_set_ids=creative_set_ids,
                    status=status
                )
                
                if result.get("success"):
                    st.success(f"✅ {result['total']} creative set(s) imported as {status}!")
                    
                    with st.expander("📋 Imported Creative Sets", expanded=False):
                        for item in result.get("cloned_ids", []):
                            st.write(f"✅ Original ID: {item['original_id']} → New ID: {item['new_id']}")
                    
                    if result.get("errors"):
                        st.warning(f"⚠️ {result['failed']} creative set(s) failed")
                        with st.expander("⚠️ Errors", expanded=False):
                            for err in result["errors"]:
                                st.write(f"- {err}")
                else:
                    st.error(f"❌ Import failed: {result.get('error')}")
                    
            except Exception as e:
                logger.error(f"Failed to import creative sets: {e}", exc_info=True)
                st.error(f"❌ Import error: {e}")
    
    elif creative_action == "Create":
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
def _clone_creative_sets_api(
    source_campaign_id: str,
    target_campaign_id: str,
    creative_set_ids: List[str],
    status: str = "PAUSED"
) -> Dict:
    """
    Clone multiple creative sets to target campaign.
    
    Args:
        source_campaign_id: Source campaign ID (for reference)
        target_campaign_id: Target campaign ID
        creative_set_ids: List of creative set IDs to clone
        status: PAUSED or LIVE
        
    Returns:
        Dict with success, cloned_ids, errors
    """
    try:
        config = _get_api_config()
        headers = {
            "Authorization": config["api_key"],
            "Content-Type": "application/json"
        }
        
        cloned_ids = []
        errors = []
        
        for cs_id in creative_set_ids:
            try:
                payload = {
                    "campaign_id": target_campaign_id,
                    "creative_set_id": cs_id,
                    "status": status
                }
                
                response = requests.post(
                    f"{APPLOVIN_BASE_URL}/creative_set/clone",
                    headers=headers,
                    params={"account_id": config["account_id"]},
                    json=payload,
                    timeout=30
                )
                
                response.raise_for_status()
                result = response.json()
                
                cloned_ids.append({
                    "original_id": cs_id,
                    "new_id": result.get("id"),
                    "version": result.get("version")
                })
                
                logger.info(f"Cloned creative set {cs_id} → {result.get('id')}")
                
            except Exception as e:
                error_msg = f"Creative Set {cs_id}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
        
        return {
            "success": len(cloned_ids) > 0,
            "cloned_ids": cloned_ids,
            "total": len(cloned_ids),
            "failed": len(errors),
            "errors": errors
        }
        
    except Exception as e:
        logger.error(f"Failed to clone creative sets: {e}", exc_info=True)
        return {"success": False, "error": str(e), "cloned_ids": [], "errors": [str(e)]}
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
            with ThreadPoolExecutor(max_workers=10) as executor:
                page = 2
                while page <= 60:  # 최대 6000개
                    # 5페이지씩 묶어서 병렬 요청
                    batch_pages = range(page, min(page + 10, 61))
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
        
        
        # 전체 Playables 먼저 저장 (게임 필터 전)
        all_playables = [a for a in all_assets if a.get("resource_type") == "HTML"]
        logger.info(f"Total playables (before filter): {len(all_playables)}")
        
        # 게임별 필터링 (Video만)
        if game and "game_mapping" in config:
            package_keyword = config["game_mapping"].get(game, "").lower()
            if package_keyword:
                # Video만 name으로 필터링
                filtered_videos = [
                    a for a in all_assets
                    if a.get("resource_type") == "VIDEO" and package_keyword in a.get("name", "").lower()
                ]
                
                logger.info(f"Filtered to {len(filtered_videos)} videos for {game}")
                
                return {
                    "videos": filtered_videos,
                    "playables": all_playables  # 전체 playable (Campaign에서 필터링)
                }
        
        # 게임 필터가 없는 경우
        videos = [a for a in all_assets if a.get("resource_type") == "VIDEO"]
        
        logger.info(f"Split: {len(videos)} videos, {len(all_playables)} playables")
        
        return {
            "videos": videos,
            "playables": all_playables
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch Applovin assets: {e}", exc_info=True)
        st.error(f"Applovin asset 목록을 가져오는데 실패했습니다: {e}")
        return {"videos": [], "playables": []}

@st.cache_data(ttl=300)  # 5분 캐시
def get_creative_sets_by_campaign(campaign_id: str) -> List[Dict]:
    """
    Fetch all creative sets for a specific campaign (with pagination).
    
    Args:
        campaign_id: Campaign ID
        
    Returns:
        List of creative set dicts
    """
    try:
        config = _get_api_config()
        headers = {"Authorization": config["api_key"]}
        account_id = config["account_id"]
        
        all_creative_sets = []
        page = 1
        
        while True:
            params = {
                "account_id": account_id,
                "ids": campaign_id,
                "page": page,
                "size": 100
            }
            
            response = requests.get(
                f"{APPLOVIN_BASE_URL}/creative_set/list_by_campaign_id",
                headers=headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            # Extract creative sets for this campaign
            campaigns_data = data.get("campaigns", {})
            creative_sets = campaigns_data.get(str(campaign_id), [])
            
            if not creative_sets:
                break
            
            all_creative_sets.extend(creative_sets)
            
            # 100개 미만이면 마지막 페이지
            if len(creative_sets) < 100:
                break
            
            page += 1
            
            # 안전 장치: 최대 50페이지 (5000개)
            if page > 50:
                logger.warning(f"Reached max pages for campaign {campaign_id}")
                break
        
        logger.info(f"Found {len(all_creative_sets)} creative sets for campaign {campaign_id}")
        return all_creative_sets
        
    except Exception as e:
        logger.error(f"Failed to fetch creative sets: {e}", exc_info=True)
        return []

def get_playables_used_in_campaign(campaign_id: str) -> set:
    """
    Get all playable asset IDs used in a campaign's creative sets.
    
    Args:
        campaign_id: Campaign ID
        
    Returns:
        Set of playable asset IDs
    """
    creative_sets = get_creative_sets_by_campaign(campaign_id)
    
    playable_ids = set()
    for cs in creative_sets:
        assets = cs.get("assets", [])
        for asset in assets:
            # HOSTED_HTML 타입이 playable
            if asset.get("type") == "HOSTED_HTML":
                playable_ids.add(asset.get("id"))
    
    logger.info(f"Found {len(playable_ids)} unique playables used in campaign {campaign_id}")
    return playable_ids

@st.cache_data(ttl=300)
def get_playable_performance(campaign_id: str, campaign_name: str = "") -> Dict[str, float]:
    """
    Fetch playable spend data from Asset Reporting API.
    """
    try:
        config = _get_api_config()
        reporting_key = config.get("reporting_api_key")
        
        if not reporting_key:
            logger.warning("Reporting API key not found")
            return {}
        
        # 캠페인 이름이 없으면 조회
        if not campaign_name:
            campaigns = get_campaigns()
            for c in campaigns:
                if str(c.get("id")) == str(campaign_id):
                    campaign_name = c.get("name", "")
                    break
        
        if not campaign_name:
            logger.warning(f"Campaign {campaign_id} not found")
            return {}
        
        logger.info(f"Fetching spend for campaign: {campaign_name}")
        
        # Asset Reporting API 호출
        params = {
            "api_key": reporting_key,
            "range": "last_7d",
            "columns": "asset_id,cost",
            "filter_campaign": campaign_name,
            "format": "json"
        }
        
        response = requests.get(
            "https://r.applovin.com/assetReport",
            params=params,
            timeout=120
        )
        
        if response.status_code != 200:
            logger.error(f"Asset Reporting API error: {response.status_code}")
            return {}
        
        data = response.json()
        results = data.get("results", [])
        logger.info(f"Returned {len(results)} rows")
        
        # Asset별 spend 집계
        asset_spend = {}
        for row in results:
            asset_id = str(row.get("asset_id", ""))
            spend = float(row.get("cost", 0) or 0)
            
            if asset_id and spend > 0:
                asset_spend[asset_id] = asset_spend.get(asset_id, 0) + spend
        
        return asset_spend
        
    except Exception as e:
        logger.error(f"Failed to fetch asset reporting data: {e}")
        return {}

# =========================================================
# UI Renderer
# =========================================================

def render_applovin_settings_panel(container, game: str, idx: int, is_marketer: bool = True) -> None:
    """Render Applovin settings panel with lazy loading."""
    _ensure_applovin_settings_state()
    cur = get_applovin_settings(game) or {}
    
    with container:
        
        # 제목과 Reload 버튼을 같은 줄에 배치
        title_col, reload_col = st.columns([3, 1])
        with title_col:
            st.markdown(f"#### {game} Applovin Settings")
        with reload_col:
            if st.button("🔄 Reload", key=f"applovin_reload_{idx}", use_container_width=True):
                with st.spinner("Reloading campaigns and assets..."):
                    # Lazy loading: 버튼으로 명시적 로드
                    campaigns_key = f"applovin_campaigns_{game}"
                    assets_key = f"applovin_assets_{game}"
                    
                    # Fetch campaigns
                    campaigns = get_campaigns(game=game)
                    st.session_state[campaigns_key] = campaigns
                    
                    if campaigns:
                        st.success(f"✅ Reloaded {len(campaigns)} campaigns")
                    else:
                        st.warning("⚠️ No campaigns found")
                    
                    # Fetch assets (Create 모드에서 필요)
                    assets = get_assets(game=game)
                    st.session_state[assets_key] = assets
                    st.success(f"✅ Reloaded {len(assets['videos'])} videos, {len(assets['playables'])} playables")
                    
                    # 강제 리렌더링
                    st.rerun()
        
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

        selected_cs_ids = []
        source_campaign_id = ""
        
        if creative_action == "Import":
            st.markdown("##### 📥 Import Creative Sets")
            
            # Source Campaign 선택
            st.markdown("**Source Campaign (Import from)**")
            
            source_campaign_options = {
                f"{c.get('name', 'Unnamed')} (ID: {c.get('id', 'N/A')})": c.get('id')
                for c in campaigns
            }
            
            current_source_id = cur.get("source_campaign_id", "")
            default_source_idx = 0
            if current_source_id:
                for i, cid in enumerate(source_campaign_options.values()):
                    if str(cid) == str(current_source_id):
                        default_source_idx = i
                        break
            
            selected_source_campaign = st.selectbox(
                "Source Campaign 선택",
                options=list(source_campaign_options.keys()),
                index=default_source_idx,
                key=f"applovin_source_campaign_{idx}",
                help="어느 캠페인에서 Creative Set을 가져올지 선택"
            )
            
            source_campaign_id = source_campaign_options[selected_source_campaign]
            
            # Load creative sets 버튼
            if st.button(f"🔍 Load Creative Sets", key=f"applovin_load_creativesets_{idx}"):
                with st.spinner("Loading creative sets..."):
                    creative_sets = get_creative_sets_by_campaign(source_campaign_id)
                    st.session_state[f"applovin_creative_sets_{game}_{source_campaign_id}"] = creative_sets
                    
                    if creative_sets:
                        st.success(f"✅ Loaded {len(creative_sets)} creative sets")
                    else:
                        st.warning("⚠️ No creative sets found")
                    st.rerun()
            
            # Creative Sets 다중 선택
            creative_sets = st.session_state.get(f"applovin_creative_sets_{game}_{source_campaign_id}", [])
            
            if creative_sets:
                st.markdown("**Select Creative Sets (다중 선택)**")
                
                creative_set_options = {
                    f"{cs.get('name', 'Unnamed')} (ID: {cs.get('id', 'N/A')})": cs.get('id')
                    for cs in creative_sets
                }
                
                current_cs_ids = cur.get("selected_creative_set_ids", [])
                default_cs_labels = [
                    label for label, cs_id in creative_set_options.items()
                    if cs_id in current_cs_ids
                ]
                
                selected_cs_labels = st.multiselect(
                    "Creative Sets 선택",
                    options=list(creative_set_options.keys()),
                    default=default_cs_labels,
                    key=f"applovin_creative_sets_select_{idx}",
                    help="Import할 Creative Set들을 선택하세요"
                )
                
                selected_cs_ids = [creative_set_options[label] for label in selected_cs_labels]
                
                if selected_cs_ids:
                    st.write(f"**선택됨: {len(selected_cs_ids)}개**")
                    for cs_id in selected_cs_ids:
                        cs_name = next(
                            (cs['name'] for cs in creative_sets if cs['id'] == cs_id),
                            cs_id
                        )
                        st.caption(f"📦 {cs_name}")
            else:
                st.info("👆 'Load Creative Sets' 버튼을 클릭하여 Creative Set을 불러오세요")
        
        # --- Create 모드 ---
        selected_video_ids = []
        selected_playable_ids = []
        creative_name = ""
        custom_name = ""
        
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
                # Campaign에서 실제 사용된 playable ID 가져오기
                used_playable_ids = get_playables_used_in_campaign(campaign_id)
                
                # 사용된 playable만 필터링
                campaign_playables = [
                    p for p in assets["playables"]
                    if p.get("id") in used_playable_ids
                ]
                
                # 캠페인 이름 가져오기 (이미 campaigns 리스트 있음)
                campaign_name = next(
                    (c.get("name", "") for c in campaigns if str(c.get("id")) == str(campaign_id)),
                    ""
                )
                
                # Playable spend 데이터 가져오기 (캠페인 이름 직접 전달)
                playable_spend = get_playable_performance(campaign_id, campaign_name)
                
                # Spend 기준 내림차순 정렬
                sorted_playables = sorted(
                    campaign_playables,
                    key=lambda p: playable_spend.get(p['id'], 0),
                    reverse=True
                )
                
                st.caption(f"📊 이 캠페인에서 사용된 Playable: {len(sorted_playables)}개")
                
                playable_options = {
                    f"{p['name']} (ID: {p['id']}) [${playable_spend.get(p['id'], 0):.2f}]": p['id']
                    for p in sorted_playables
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
        if creative_action == "Import":
            st.session_state.applovin_settings[game] = {
                "campaign_id": str(campaign_id),
                "creative_action": "Import",
                "source_campaign_id": source_campaign_id if 'source_campaign_id' in locals() else "",
                "selected_creative_set_ids": selected_cs_ids if 'selected_cs_ids' in locals() else [],
            }
        else:  # Create
            st.session_state.applovin_settings[game] = {
                "campaign_id": str(campaign_id),
                "creative_action": "Create",
                "video_ids": selected_video_ids,
                "playable_ids": selected_playable_ids,
                "custom_name": custom_name.strip() if custom_name else "",
                "generated_name": creative_name,
            }
    