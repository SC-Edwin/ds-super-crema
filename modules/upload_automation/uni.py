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
    get_unity_app_id,
    get_unity_campaign_set_id,
    _unity_get,
    _unity_list_playable_creatives,
    _unity_list_assigned_creative_packs,
    _unity_assign_creative_pack,
    _unity_create_playable_creative,
    _unity_create_creative_pack,
    _check_existing_creative,
    _check_existing_pack,
    _clean_playable_name_for_pack,  # 추가
    get_unity_settings as _get_unity_settings,
    _ensure_unity_settings_state,
    preview_unity_upload as _preview_unity_upload,
    apply_unity_creative_packs_to_campaign as _apply_unity_creative_packs_to_campaign,
    upload_unity_creatives_to_campaign as _upload_unity_creatives_to_campaign,
)

from concurrent.futures import ThreadPoolExecutor, as_completed
import time
# Re-export for compatibility
def get_unity_settings(game: str, **kwargs) -> Dict:
    """Re-export from unity_ads for compatibility."""
    return _get_unity_settings(game)

def preview_unity_upload(*, game: str, videos: List[Dict], settings: Dict, is_marketer: bool = True) -> Dict:
    """Re-export from unity_ads for compatibility. Default is_marketer=True for marketer mode."""
    return _preview_unity_upload(game=game, videos=videos, settings=settings, is_marketer=is_marketer)

from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def apply_unity_creative_packs_to_campaign(*, game: str, creative_pack_ids: List[str], settings: Dict, is_marketer: bool = True) -> Dict:
    """
    Marketer mode: 여러 플랫폼 + 여러 캠페인에 creative packs assign.
    - 병렬 처리 (max 2개 동시)
    - Resume 지원 (이미 assign된 pack은 skip)
    """
    platforms = settings.get("platforms", [])
    
    # 하위 호환: 기존 단일 플랫폼 모드
    if not platforms:
        return _apply_unity_creative_packs_to_campaign(
            game=game, creative_pack_ids=creative_pack_ids, settings=settings, is_marketer=is_marketer
        )
    
    all_results = {
        "game": game,
        "platforms": platforms,
        "results_per_campaign": {},
        "errors": [],
        "skipped_campaigns": [],
    }
    
    # 캠페인별 pack 선택 정보
    packs_per_campaign = settings.get("packs_per_campaign", {})
    
    # 하위 호환: 기존 방식 (플랫폼별 pack IDs)
    pack_ids_by_platform = creative_pack_ids if isinstance(creative_pack_ids, dict) else {}
    
    # 작업 목록 생성
    tasks = []
    for plat in platforms:
        plat_settings = settings.get(plat, {})
        campaign_ids = plat_settings.get("campaign_ids", [])
        
        if not campaign_ids:
            continue
        
        for cid in campaign_ids:
            # 1순위: 캠페인별 선택된 pack
            campaign_key = f"{plat}_{cid}"
            if campaign_key in packs_per_campaign:
                pack_ids = packs_per_campaign[campaign_key].get("pack_ids", [])
            else:
                # 2순위: 플랫폼별 pack (하위 호환)
                pack_ids = pack_ids_by_platform.get(plat, creative_pack_ids if isinstance(creative_pack_ids, list) else [])
            
            if not pack_ids:
                st.warning(f"[{plat.upper()}] 캠페인 `{cid}`: 선택된 pack이 없습니다. Skip.")
                continue
            
            tasks.append({
                "plat": plat,
                "cid": cid,
                "pack_ids": pack_ids,
                "org_id": settings.get("org_id", UNITY_ORG_ID_DEFAULT),
                "title_id": plat_settings.get("campaign_set_id"),
            })
    
    if not tasks:
        return all_results
    
    # 진행 상황 표시
    progress_bar = st.progress(0, text=f"0/{len(tasks)} 캠페인 처리 중...")
    status_container = st.empty()
    completed = [0]
    
    def _assign_one(task):
        """
        단일 캠페인에 assign (resume 지원)
        - 이미 assign된 pack은 skip
        - rate limit 방지를 위해 딜레이 추가
        """
        plat = task["plat"]
        cid = task["cid"]
        org_id = task["org_id"]
        title_id = task["title_id"]
        pack_ids = task["pack_ids"]
        
        result = {
            "key": f"{plat}_{cid}",
            "plat": plat,
            "cid": cid,
            "assigned_packs": [],
            "skipped_packs": [],
            "errors": [],
        }
        
        try:
            # 1. 현재 캠페인에 이미 할당된 pack 목록 조회
            assigned = _unity_list_assigned_creative_packs(
                org_id=org_id,
                title_id=title_id,
                campaign_id=cid
            )
            assigned_ids = set()
            for p in assigned:
                # assigned-creative-packs API는 creativePackId를 반환
                pack_id = str(p.get("creativePackId") or p.get("id") or "")
                if pack_id:
                    assigned_ids.add(pack_id)
            
            # 2. 이미 할당된 pack 제외
            packs_to_assign = [p for p in pack_ids if str(p) not in assigned_ids]
            skipped = [p for p in pack_ids if str(p) in assigned_ids]
            
            result["skipped_packs"] = skipped
            
            if not packs_to_assign:
                # 모든 pack이 이미 assign됨
                return result
            
            # 3. 새 pack들만 assign
            for pack_id in packs_to_assign:
                try:
                    time.sleep(0.5)  # rate limit 방지
                    _unity_assign_creative_pack(
                        org_id=org_id,
                        title_id=title_id,
                        campaign_id=cid,
                        creative_pack_id=str(pack_id)
                    )
                    result["assigned_packs"].append(pack_id)
                except Exception as e:
                    error_str = str(e).lower()
                    if any(keyword in error_str for keyword in ["limit", "maximum", "exceeded", "full", "capacity", "quota"]):
                        result["errors"].append("Creative pack 개수가 최대입니다.")
                        break  # 더 이상 시도하지 않음
                    else:
                        result["errors"].append(f"Pack {pack_id}: {str(e)}")
            
        except Exception as e:
            result["errors"].append(str(e))
        
        return result
    
    # 병렬 처리 (max 2개 동시)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(_assign_one, task): task for task in tasks}
        
        for future in as_completed(futures):
            task = futures[future]
            completed[0] += 1
            
            progress_bar.progress(
                completed[0] / len(tasks),
                text=f"{completed[0]}/{len(tasks)} 캠페인 처리 중..."
            )
            
            try:
                res = future.result()
                all_results["results_per_campaign"][res["key"]] = res
                
                # 상태 표시
                assigned_count = len(res["assigned_packs"])
                skipped_count = len(res["skipped_packs"])
                
                if skipped_count > 0 and assigned_count == 0:
                    status_container.info(f"⏭️ [{res['plat'].upper()}] 캠페인 `{res['cid']}`: 모든 pack 이미 assign됨 (skip)")
                    all_results["skipped_campaigns"].append(res["key"])
                elif skipped_count > 0:
                    status_container.success(f"✅ [{res['plat'].upper()}] 캠페인 `{res['cid']}`: {assigned_count}개 assign, {skipped_count}개 skip")
                elif assigned_count > 0:
                    status_container.success(f"✅ [{res['plat'].upper()}] 캠페인 `{res['cid']}`: {assigned_count}개 assign")
                
                if res["errors"]:
                    all_results["errors"].extend(
                        [f"[{res['plat'].upper()}/{res['cid']}] {e}" for e in res["errors"]]
                    )
                    
            except Exception as e:
                all_results["errors"].append(f"[{task['plat'].upper()}/{task['cid']}] {str(e)}")
    
    progress_bar.empty()
    status_container.empty()
    
    # 최종 요약
    total_assigned = sum(len(r["assigned_packs"]) for r in all_results["results_per_campaign"].values())
    total_skipped = sum(len(r["skipped_packs"]) for r in all_results["results_per_campaign"].values())
    
    if total_skipped > 0:
        st.info(f"ℹ️ Resume: {total_skipped}개 pack은 이미 assign되어 있어 skip됨")
    
    return all_results

def upload_unity_creatives_to_campaign(*, game: str, videos: List[Dict], settings: Dict) -> Dict:
    """
    Marketer mode: 여러 플랫폼에 각각 creative packs 생성 (병렬 처리).
    - 자동 감지: 비디오가 있으면 video_playable, Playable만 있으면 playable_only
    """
    platforms = settings.get("platforms", [])
    
    # 자동 감지: 비디오 파일 vs Playable 파일
    video_files = [
        v for v in videos 
        if not ("playable" in (v.get("name") or "").lower() or (v.get("name") or "").lower().endswith(".html"))
        and (v.get("name") or "").lower().endswith(".mp4")
    ]
    playable_files = [
        v for v in videos 
        if "playable" in (v.get("name") or "").lower() or (v.get("name") or "").lower().endswith(".html")
    ]
    
    # 자동 모드 결정
    if video_files:
        pack_mode = "video_playable"
    elif playable_files:
        pack_mode = "playable_only"
    else:
        pack_mode = "video_playable"  # 기본값
    
    if pack_mode == "playable_only":
        st.info(f"🎮 **Playable만 모드** 자동 감지: {len(playable_files)}개 Playable → {len(playable_files)}개 Pack 생성")
    else:
        st.info(f"📹 **비디오 + Playable 모드** 자동 감지: {len(video_files)}개 비디오 파일")
    
    # 하위 호환
    if not platforms:
        return _upload_unity_creatives_to_campaign(game=game, videos=videos, settings=settings)
    
    all_results = {
        "game": game,
        "platforms": platforms,
        "pack_mode": pack_mode,
        "results_per_platform": {},
        "errors": [],
    }
    
    # Playable만 모드
    if pack_mode == "playable_only":
        return _upload_playable_only_packs(game=game, videos=videos, settings=settings, all_results=all_results)
    
    # 기존 비디오 + Playable 모드
    tasks = []
    for plat in platforms:
        plat_settings = settings.get(plat, {})
        if not plat_settings.get("campaign_ids"):
            continue
        
        single_settings = {
            "platform": plat,
            "org_id": settings.get("org_id", UNITY_ORG_ID_DEFAULT),
            "title_id": plat_settings.get("campaign_set_id"),
            "campaign_id": plat_settings["campaign_ids"][0],
            "existing_playable_id": plat_settings.get("existing_playable_id", ""),
            "existing_playable_label": plat_settings.get("existing_playable_label", ""),
            "language": settings.get("language", "en"),
        }
        
        tasks.append({
            "plat": plat,
            "settings": single_settings,
        })
    
    if not tasks:
        return all_results
    
    def _upload_one(task):
        plat = task["plat"]
        try:
            result = _upload_unity_creatives_to_campaign(
                game=game, 
                videos=videos, 
                settings=task["settings"]
            )
            return {
                "plat": plat,
                "result": result,
                "error": None,
            }
        except Exception as e:
            return {
                "plat": plat,
                "result": None,
                "error": str(e),
            }
    
    # st.status() 사용 (스레드 안전)
    with st.status(f"🚀 Unity 크리에이티브 팩 생성 중... ({len(tasks)}개 플랫폼)", expanded=True) as status:
        for plat in [t["plat"] for t in tasks]:
            status.write(f"⏳ **{plat.upper()}** 대기 중...")
        
        # 병렬 처리
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(_upload_one, task): task for task in tasks}
            
            for future in as_completed(futures):
                res = future.result()
                plat = res["plat"]
                
                if res["error"]:
                    status.write(f"❌ **{plat.upper()}** 실패: {res['error']}")
                    all_results["errors"].append(f"[{plat.upper()}] {res['error']}")
                elif res["result"]:
                    result = res["result"]
                    all_results["results_per_platform"][plat] = result
                    
                    pack_count = len(result.get("creative_ids", []))
                    if pack_count > 0:
                        status.write(f"✅ **{plat.upper()}** 완료: {pack_count}개 pack 생성됨")
                    else:
                        status.write(f"⚠️ **{plat.upper()}** 완료: 생성된 pack 없음")
                    
                    if result.get("errors"):
                        all_results["errors"].extend([f"[{plat.upper()}] {e}" for e in result["errors"]])
        
        status.update(label="✅ Unity 업로드 완료!", state="complete", expanded=False)
    
    return all_results
def _upload_playable_only_packs(*, game: str, videos: List[Dict], settings: Dict, all_results: Dict) -> Dict:
    """
    Playable만으로 Creative Pack 생성.
    각 playable 파일 → 각각 1개 Pack
    """
    
    platforms = settings.get("platforms", [])
    
    # Playable 파일 필터링
    playable_files = [
        v for v in videos 
        if "playable" in (v.get("name") or "").lower() or (v.get("name") or "").lower().endswith(".html")
    ]
    
    if not playable_files:
        all_results["errors"].append("Playable 파일이 없습니다.")
        return all_results
    
    st.info(f"🎮 Playable만 모드: {len(playable_files)}개 파일 → {len(playable_files)}개 Pack 생성 예정")
    
    # 플랫폼별 작업
    tasks = []
    for plat in platforms:
        plat_settings = settings.get(plat, {})
        if not plat_settings.get("campaign_ids"):
            continue
        
        tasks.append({
            "plat": plat,
            "org_id": settings.get("org_id", UNITY_ORG_ID_DEFAULT),
            "title_id": plat_settings.get("campaign_set_id"),
            "playable_files": playable_files,
            "language": settings.get("language", "en"),
        })
    
    if not tasks:
        return all_results
    
    def _upload_playables_for_platform(task):
        plat = task["plat"]
        org_id = task["org_id"]
        title_id = task["title_id"]
        playables = task["playable_files"]
        lang = task.get("language", "en")
        
        result = {
            "plat": plat,
            "creative_ids": [],
            "errors": [],
        }
        
        for pf in playables:
            pf_name = pf.get("name", "")
            pf_path = pf.get("path", "")
            
            # Pack 이름 생성 (파일명에서 확장자 제거)
            pack_name = _clean_playable_name_for_pack(pf_name)
            
            try:
                # 1. Playable creative 생성 (또는 기존 것 사용)
                creative_id = _check_existing_creative(org_id, title_id, pf_name)
                
                if not creative_id:
                    creative_id = _unity_create_playable_creative(
                        org_id=org_id,
                        title_id=title_id,
                        playable_path=pf_path,
                        name=pf_name,
                        language=lang
                    )
                    time.sleep(0.5)
                
                # 2. Pack 생성 (또는 기존 것 사용)
                pack_id = _check_existing_pack(org_id, title_id, pack_name)
                
                if not pack_id:
                    pack_id = _unity_create_creative_pack(
                        org_id=org_id,
                        title_id=title_id,
                        pack_name=pack_name,
                        creative_ids=[creative_id],
                        pack_type="playable"
                    )
                    time.sleep(0.5)
                
                result["creative_ids"].append(pack_id)
                
            except Exception as e:
                result["errors"].append(f"{pf_name}: {str(e)}")
        
        return result
    
    # 진행 표시
    progress_bar = st.progress(0, text="Playable Pack 생성 중...")
    status_container = st.empty()
    completed = [0]
    total_tasks = len(tasks)
    
    # 병렬 처리
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(_upload_playables_for_platform, task): task for task in tasks}
        
        for future in as_completed(futures):
            task = futures[future]
            plat = task["plat"]
            completed[0] += 1
            
            progress_bar.progress(
                completed[0] / total_tasks,
                text=f"{completed[0]}/{total_tasks} 플랫폼 처리 중..."
            )
            
            try:
                res = future.result()
                all_results["results_per_platform"][plat] = {
                    "creative_ids": res["creative_ids"],
                    "errors": res["errors"],
                }
                
                pack_count = len(res["creative_ids"])
                if pack_count > 0:
                    status_container.success(f"✅ **{plat.upper()}** 완료: {pack_count}개 Playable Pack 생성됨")
                
                if res["errors"]:
                    all_results["errors"].extend([f"[{plat.upper()}] {e}" for e in res["errors"]])
                    
            except Exception as e:
                all_results["errors"].append(f"[{plat.upper()}] {str(e)}")
    
    progress_bar.empty()
    status_container.empty()
    
    return all_results
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
# 3. Fetch existing creative packs per campaign set
# -------------------------------------------------------------------------
@st.cache_data(ttl=60, show_spinner=False)
def fetch_creative_packs_for_campaign_set(game: str, platform: str = "aos") -> List[Dict]:
    """
    Campaign Set(App)에 존재하는 모든 Creative Pack 조회
    """
    try:
        org_id = (UNITY_ORG_ID_DEFAULT or "").strip()
        app_id = get_unity_campaign_set_id(game, platform)
        
        if not org_id or not app_id:
            raise RuntimeError("❌ Missing org_id or app_id")
        
        path = f"organizations/{org_id}/apps/{app_id}/creative-packs"
        
        all_packs = []
        offset = 0
        limit = 100
        
        while True:
            meta = _unity_get(path, params={"limit": limit, "offset": offset})
            
            items = []
            if isinstance(meta, list):
                items = meta
            elif isinstance(meta, dict):
                items = meta.get("items") or meta.get("data") or meta.get("results") or []
            
            if not items:
                break
            
            for pack in items:
                if isinstance(pack, dict):
                    pack_id = str(pack.get("id", ""))
                    pack_name = pack.get("name", "(no name)")
                    if pack_id:
                        all_packs.append({
                            "id": pack_id,
                            "name": pack_name,
                        })
            
            if len(items) < limit:
                break
            
            offset += limit
        
        return all_packs
        
    except Exception as e:
        logger.exception(f"Unity API error: fetch_creative_packs_for_campaign_set({game}, {platform})")
        raise RuntimeError(f"Creative Pack 조회 실패: {str(e)}")
# -------------------------------------------------------------------------
# 2. List playables per game + platform (for info)
# -------------------------------------------------------------------------
@st.cache_data(ttl=0, show_spinner=False)
def fetch_playables_for_game(game: str, platform: str = "aos") -> List[Dict]:
    try:
        org_id = (UNITY_ORG_ID_DEFAULT or "").strip()
        app_id = get_unity_campaign_set_id(game, platform)
        
        path = f"organizations/{org_id}/apps/{app_id}/creatives"
        

        
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
def render_unity_settings_panel(container, game: str, idx: int, is_marketer: bool = True, **kwargs) -> None:
    """
    Marketer-mode Unity panel
    - 플랫폼별 캠페인 선택
    - 플랫폼별 Playable 선택
    - 캠페인별 Creative Pack 선택 (assign용)
    """
    _ensure_unity_settings_state()
    cur = get_unity_settings(game) or {}

    with container:
        st.markdown(f"#### {game} Unity Settings (Marketer)")

        # 1) 플랫폼 멀티셀렉트 (기본: 둘 다 선택)
        prev_platforms = cur.get("platforms", ["aos", "ios"])
        selected_platforms = st.multiselect(
            "플랫폼 선택 (복수 선택 가능)",
            options=["aos", "ios"],
            default=prev_platforms,
            key=f"unity_mkt_platform_{idx}",
        )
        
        if not selected_platforms:
            st.warning("최소 1개 플랫폼을 선택해주세요.")
            return

        # 2) 플랫폼별 캠페인 & Playable 선택
        platform_settings = {}
        
        for plat in selected_platforms:
            plat_upper = plat.upper()
            st.markdown(f"---")
            st.markdown(f"##### 📱 {plat_upper} 설정")
            
            prev_plat_settings = cur.get(plat, {})
            
            # 캠페인 목록 가져오기
            try:
                campaigns = fetch_unity_campaigns(game, plat)
            except Exception as e:
                st.error(f"{plat_upper} 캠페인 목록 오류: {e}")
                campaigns = []
            
            selected_campaign_ids = []
            campaign_id_to_name = {}
            
            if campaigns:
                labels = [f"{c['name']} ({c['id']})" for c in campaigns]
                ids = [c["id"] for c in campaigns]
                label_to_id = dict(zip(labels, ids))
                campaign_id_to_name = {c["id"]: c["name"] for c in campaigns}
                
                prev_campaign_ids = prev_plat_settings.get("campaign_ids", [])
                default_labels = [l for l, cid in label_to_id.items() if cid in prev_campaign_ids]
                
                sel_labels = st.multiselect(
                    f"{plat_upper} 캠페인 선택",
                    options=labels,
                    default=default_labels,
                    key=f"unity_mkt_campaign_{idx}_{plat}",
                )
                selected_campaign_ids = [label_to_id[l] for l in sel_labels]
                
                if selected_campaign_ids:
                    st.caption(f"선택: {len(selected_campaign_ids)}개 캠페인")
            else:
                st.info(f"{plat_upper} 캠페인을 찾을 수 없습니다.")
            
            # Playable 선택
            playable_options = ["(선택 안 함)"]
            playable_id_map = {}
            prev_playable_id = prev_plat_settings.get("existing_playable_id", "")
            
            try:
                playables = fetch_playables_for_game(game, platform=plat)
                if playables:
                    for p in playables:
                        p_id = str(p.get("id", ""))
                        p_name = p.get("name", "(no name)")
                        p_type = p.get("type", "")
                        if p_id:
                            label = f"{p_name} ({p_type}) [{p_id[:8]}...]"
                            playable_options.append(label)
                            playable_id_map[label] = p_id
            except Exception as e:
                st.error(f"{plat_upper} Playable 조회 실패: {e}")
            
            default_playable_idx = 0
            if prev_playable_id:
                for i, (lbl, pid) in enumerate(playable_id_map.items(), start=1):
                    if pid == prev_playable_id:
                        default_playable_idx = i
                        break
            
            selected_playable_label = st.selectbox(
                f"{plat_upper} Playable 선택",
                options=playable_options,
                index=default_playable_idx,
                key=f"unity_mkt_playable_{idx}_{plat}",
            )
            selected_playable_id = playable_id_map.get(selected_playable_label, "")
            
            # 플랫폼별 설정 저장
            platform_settings[plat] = {
                "campaign_set_id": get_unity_campaign_set_id(game, plat),
                "campaign_ids": selected_campaign_ids,
                "campaign_id_to_name": campaign_id_to_name,
                "existing_playable_id": selected_playable_id,
                "existing_playable_label": selected_playable_label,
            }

        # 3) 캠페인별 Creative Pack 선택 (Assign용)
        st.markdown("---")
        st.markdown("#### 📦 캠페인별 Creative Pack 선택")
        st.caption("각 캠페인에 assign할 Creative Pack을 선택하세요.")
        
        # Refresh 버튼
        if st.button("🔄 Creative Pack 목록 새로고침", key=f"refresh_packs_{idx}"):
            for plat in selected_platforms:
                st.cache_data.clear()
            st.rerun()
        
        packs_per_campaign = {}
        
        for plat in selected_platforms:
            plat_upper = plat.upper()
            plat_settings = platform_settings.get(plat, {})
            campaign_ids = plat_settings.get("campaign_ids", [])
            campaign_id_to_name = plat_settings.get("campaign_id_to_name", {})
            
            if not campaign_ids:
                continue
            
            # 해당 플랫폼의 모든 creative packs 가져오기
            try:
                all_packs = fetch_creative_packs_for_campaign_set(game, plat)
            except Exception as e:
                st.error(f"{plat_upper} Creative Pack 조회 실패: {e}")
                all_packs = []
            
            if not all_packs:
                st.info(f"{plat_upper}: 사용 가능한 Creative Pack이 없습니다. 먼저 '크리에이티브/팩 생성'을 실행하세요.")
                continue
            
            # 비디오 번호 기준 내림차순 정렬
            import re
            def extract_video_num(pack):
                name = pack.get("name", "")
                match = re.search(r'video(\d+)', name.lower())
                return int(match.group(1)) if match else 0
            
            all_packs_sorted = sorted(all_packs, key=extract_video_num, reverse=True)
            
            st.markdown(f"**{plat_upper}** ({len(all_packs_sorted)}개 pack 사용 가능)")
            
            # 각 캠페인별로 pack 선택 (expander 없이 바로 표시)
            for cid in campaign_ids:
                campaign_name = campaign_id_to_name.get(cid, cid)
                
                pack_labels = [f"{p['name']} ({p['id'][:8]}...)" for p in all_packs_sorted]
                pack_ids = [p["id"] for p in all_packs_sorted]
                label_to_pack_id = dict(zip(pack_labels, pack_ids))
                
                # 이전 선택값 복원
                prev_selected = cur.get(f"{plat}_{cid}_packs", [])
                default_labels = [l for l, pid in label_to_pack_id.items() if pid in prev_selected]
                
                # Expander 없이 바로 표시
                selected_labels = st.multiselect(
                    f"📁 {campaign_name}",
                    options=pack_labels,
                    default=default_labels,
                    key=f"unity_packs_{idx}_{plat}_{cid}",
                )
                selected_pack_ids = [label_to_pack_id[l] for l in selected_labels]
                
                packs_per_campaign[f"{plat}_{cid}"] = {
                    "plat": plat,
                    "cid": cid,
                    "pack_ids": selected_pack_ids,
                }
        # 4) 상태 저장
        cur.update({
            "platforms": selected_platforms,
            "org_id": UNITY_ORG_ID_DEFAULT,
            "packs_per_campaign": packs_per_campaign,
            **platform_settings,
        })
        
        # 캠페인별 선택 저장
        for key, val in packs_per_campaign.items():
            plat, cid = key.split("_", 1)
            cur[f"{plat}_{cid}_packs"] = val["pack_ids"]
        
        st.session_state.unity_settings[game] = cur
        
        # 요약 표시
        st.markdown("---")
        total_campaigns = sum(len(platform_settings[p]["campaign_ids"]) for p in selected_platforms)
        total_packs_selected = sum(len(v["pack_ids"]) for v in packs_per_campaign.values())
        
        if total_campaigns > 0:
            st.success(f"✅ {len(selected_platforms)}개 플랫폼, {total_campaigns}개 캠페인, {total_packs_selected}개 pack 선택됨")