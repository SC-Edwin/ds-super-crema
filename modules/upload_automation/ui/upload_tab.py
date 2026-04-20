"""Creative Upload 탭 UI — Streamlit 위젯·레이아웃·session_state.

유스케이스·규칙은 `application/`으로, 세션 키 문자열은 `session/keys.py`로 점진 이관합니다.
"""
from __future__ import annotations

import logging
import os
import pathlib
import sys
from typing import Dict, List

# ---- `modules/upload_automation` 패키지 루트 (이 파일은 ui/ 하위)
current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
root_dir = os.path.dirname(os.path.dirname(current_dir))

if root_dir not in sys.path:
    sys.path.append(root_dir)
if current_dir not in sys.path:
    sys.path.append(current_dir)

import streamlit as st
from streamlit.components.v1 import html as components_html

from modules.upload_automation.session.keys import (
    PAGE,
    PAGE_MARKETER_TITLE,
    PAGE_OPS_TITLE,
    namespaced_key,
)
from modules.upload_automation.application.upload_validation import validate_count
from modules.upload_automation.utils import devtools
from modules.upload_automation.utils.upload_logger import log_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)
 
# =========================================================
# 3. 디버깅 및 모듈 임포트 (수정된 부분)
# =========================================================

# (1) utils/drive_import.py 파일이 있는지 확인
target_file = os.path.join(current_dir, "utils", "drive_import.py")
if not os.path.exists(target_file):
    st.error(f"🚨 [CRITICAL] 'utils/drive_import.py' 파일을 찾을 수 없습니다!")
    st.code(f"찾는 위치: {target_file}")
    
    # 현재 폴더에 무슨 파일이 있는지 보여줌
    try:
        files_in_current = os.listdir(current_dir)  # ← 수정: root_dir → current_dir
        st.warning(f"📂 현재 폴더({current_dir})에 있는 파일 목록:\n" + ", ".join(files_in_current))
    except Exception as e:
        st.error(f"폴더 목록 읽기 실패: {e}")
    st.stop()

# (2) 파일은 있는데 불러오다가 에러가 나는 경우 체크
try:
    from modules.upload_automation.utils.drive_import import import_drive_folder_videos_parallel as import_drive_folder_videos  # ← 수정
    _DRIVE_IMPORT_SUPPORTS_PROGRESS = True
except ImportError as e:
    try:
        from modules.upload_automation.utils.drive_import import import_drive_folder_videos  # ← 수정
        _DRIVE_IMPORT_SUPPORTS_PROGRESS = False
    except ImportError as e2:
        st.error("🚨 모듈을 불러오는 중 에러가 발생했습니다.")
        st.error(f"1차 시도 에러: {e}")
        st.error(f"2차 시도 에러: {e2}")
        st.info("💡 팁: requirements.txt에 필요한 라이브러리(google-api-python-client 등)가 빠져있지 않은지 확인하세요.")
        st.stop()

# 1. Game Manager (BigQuery Integration)
from modules.upload_automation.config import game_manager  # ← 수정

# 2. Operations Modules (Admin/Full Access)
# 2. Operations Modules (Admin/Full Access)
from modules.upload_automation.platforms.meta import facebook_ads as fb_ops
from modules.upload_automation.platforms.unity import unity_ads as uni_ops

# 3. Marketer Modules (Simplified/Restricted)
try:
    from modules.upload_automation.platforms.meta import fb as fb_marketer
    from modules.upload_automation.platforms.unity import uni as uni_marketer
except ImportError as e:
    st.error(
        f"Module Import Error: {e}. "
        f"Ensure platforms/meta/fb.py and platforms/unity/uni.py exist under {current_dir}"
    )
    st.stop()

# 4. Applovin Module (Both Test & Marketer modes)
try:
    from modules.upload_automation.platforms.applovin import applovin as applovin_module
except ImportError as e:
    st.error(f"Applovin Module Import Error: {e}")
    st.stop()

# 5. Google Ads Module (Marketer mode only)
try:
    from modules.upload_automation.platforms.google_ads import ga as google_marketer
    _GOOGLE_ADS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Google Ads module not available: {e}")
    _GOOGLE_ADS_AVAILABLE = False

# # Optional: safe debug helper (won't crash app even if IDs are missing)
# try:
#     st.write("unity_cfg", uni_ops.unity_cfg)
#     # This prints game_ids / campaign_sets blocks and then calls
#     # get_unity_app_id / get_unity_campaign_set_id **inside try/except**
#     # so any error is shown as text, not an exception.
#     uni_ops.debug_unity_ids("XP HERO")
# except Exception as e:
#     st.warning(f"Unity debug helper failed: {e}")
#     st.error(f"Unity debug failed: {e}")


# ----- CONFIG & STATE --------------------------------------------------
try:
    MAX_UPLOAD_MB = int(st.get_option("server.maxUploadSize"))
except Exception:
    MAX_UPLOAD_MB = 200

def _key(prefix: str, name: str) -> str:
    """Return a namespaced session state key (delegates to session.keys)."""
    return namespaced_key(prefix, name)

def init_state(prefix: str = ""):
    """Set up st.session_state containers."""
    _uploads = _key(prefix, "uploads")
    _settings = _key(prefix, "settings")
    if _uploads not in st.session_state:
        st.session_state[_uploads] = {}
    if _settings not in st.session_state:
        st.session_state[_settings] = {}

def init_remote_state(prefix: str = ""):
    """Set up st.session_state container for Drive-imported videos."""
    _rv = _key(prefix, "remote_videos")
    if _rv not in st.session_state:
        st.session_state[_rv] = {}

def _run_drive_import(folder_url_or_id: str, max_workers: int, on_progress=None):
    """Wrapper for Drive import."""
    if _DRIVE_IMPORT_SUPPORTS_PROGRESS:
        return import_drive_folder_videos(folder_url_or_id, max_workers=max_workers, on_progress=on_progress)
    files = import_drive_folder_videos(folder_url_or_id)
    total = len(files)
    if on_progress:
        done = 0
        for f in files:
            done += 1
            on_progress(done, total, f.get("name", ""), None)
    return files


# ----- STREAMLIT SETUP ------------------------------------------------
# Note: set_page_config is usually called in the main entry point (app.py).
# If this file is imported as a module, calling it again might cause warnings,
# but usually it's ignored if already set.
try:
    st.set_page_config(
        page_title="Creative 자동 업로드",
        page_icon="🎮",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
except Exception:
    pass # Ignore if page config was already set by parent app

# Hide sidebar completely
st.markdown("""
<style>
    section[data-testid="stSidebar"] {
        display: none !important;
    }
    .stApp > div:first-child {
        padding-right: 1rem !important;
    }
</style>
""", unsafe_allow_html=True)

init_state()
init_remote_state()
fb_ops.init_fb_game_defaults()



# ======================================================================
# MAIN RENDERER (Shared Logic)
# ======================================================================
def render_main_app(title: str, fb_module, unity_module, is_marketer: bool = False, prefix: str = "") -> None:
    # Dev-only log panel (enable via ?dev=1 or secrets developer_mode=true)
    devtools.render_dev_panel()
    """
    Renders the main UI.
    Dynamically loads games from BigQuery via game_manager.
    prefix: namespace for session state keys (e.g. "vn" for Vietnam tab).
    """
    # Namespaced session state key aliases
    _rv = _key(prefix, "remote_videos")
    _up = _key(prefix, "uploads")
    _st = _key(prefix, "settings")
    _us = _key(prefix, "unity_settings")
    _ucp = _key(prefix, "unity_created_packs")
    _tab = _key(prefix, "tab")
    kp = f"{prefix}_" if prefix else ""  # widget key prefix
    mode_str = "Marketer" if is_marketer else "Test"

    st.title(title)
    
    # --- LOAD GAMES FROM DB ---
    GAMES = game_manager.get_all_game_names(include_custom=is_marketer)

    if not GAMES:
        st.error("No games found. Please check BigQuery connection or Add a New Game.")
        return

    # Use query params to preserve tab selection after rerun
    query_params = st.query_params
    selected_tab = query_params.get(_tab, [None])[0] if query_params.get(_tab) else None
    
    _tabs = st.tabs(GAMES)
 
    
    # If a tab was selected via query params, try to find its index
    if selected_tab and selected_tab in GAMES:
        tab_index = GAMES.index(selected_tab)
        st.query_params[_key(prefix, "tab_index")] = tab_index
        # Note: Streamlit tabs don't support programmatic selection, but this helps with state tracking

    for i, game in enumerate(GAMES):
        with _tabs[i]:
            left_col, right_col = st.columns([2, 1], gap="large")

            # =========================
            # LEFT COLUMN: Inputs
            # =========================
            with left_col:
                with st.container(border=True):
                    st.subheader(game)

                    # --- Platform Radio ---
                    # Test mode: Facebook, Unity Ads
                    # Marketer mode: Facebook, Unity Ads, Mintegral, Applovin
                    if is_marketer:
                        platform_options = ["Facebook", "Unity Ads", "Mintegral", "Applovin"]
                        if _GOOGLE_ADS_AVAILABLE:
                            platform_options.append("Google Ads")
                    else:
                        platform_options = ["Facebook", "Unity Ads"]
                    
                    platform = st.radio(
                        "플랫폼 선택",
                        platform_options,
                        index=0,
                        horizontal=True,
                        key=f"{kp}platform_{game}",
                    )
                    # st.tabs는 rerun 시 선택 탭이 첫 탭으로 돌아갈 수 있어,
                    # 플랫폼 변경이 감지되면 현재 게임 탭을 query params에 다시 고정한다.
                    _plat_prev_key = f"{kp}platform_prev_{game}"
                    _prev_platform = st.session_state.get(_plat_prev_key)
                    if _prev_platform is None:
                        st.session_state[_plat_prev_key] = platform
                    elif _prev_platform != platform:
                        st.session_state[_plat_prev_key] = platform
                        st.query_params[_tab] = game

                    if platform == "Facebook":
                        st.markdown("### Facebook")
                    elif platform == "Unity Ads":
                        st.markdown("### Unity Ads")
                    elif platform == "Mintegral":
                        st.markdown("### Mintegral")
                    elif platform == "Applovin":
                        st.markdown("### Applovin")
                    elif platform == "Google Ads":
                        st.markdown("### Google Ads")


                    # --- Drive Import Section ---
                    st.markdown("**Creative Videos 가져오기**")
                    
                    # 탭으로 Drive / Local 선택
                    import_method = st.radio(
                        "가져오기 방법",
                        ["Google Drive", "로컬 파일"],
                        index=0,
                        horizontal=True,
                        key=f"{kp}import_method_{game}",
                    )
                    
                    if import_method == "Google Drive":
                        st.markdown("**구글 드라이브에서 Creative Videos를 가져옵니다**")
                        drv_input = st.text_input(
                            "Drive folder URL or ID",
                            key=f"{kp}drive_folder_{game}",
                            placeholder="https://drive.google.com/drive/folders/..."
                        )

                        with st.expander("Advanced import options", expanded=False):
                            workers = st.number_input(
                                "Parallel workers", min_value=1, max_value=16, value=8, key=f"{kp}drive_workers_{game}"
                            )

                        # [수정 1] 드라이브 가져오기 버튼: 너비 꽉 채우기
                        if st.button("드라이브에서 Creative 가져오기", key=f"{kp}drive_import_{game}", width="stretch"):
                            try:
                                overall = st.progress(0, text="Waiting...")
                                log_box = st.empty()
                                lines = []
                                import time
                                last_flush = [0.0]

                                def _on_progress(done, total, name, err):
                                    pct = int((done / max(total, 1)) * 100)
                                    label = f"{done}/{total} • {name}" if name else f"{done}/{total}"
                                    if err: lines.append(f"❌ {name} — {err}")
                                    else: lines.append(f"✅ {name}")
                                    
                                    now = time.time()
                                    if (now - last_flush[0]) > 0.3 or done == total:
                                        overall.progress(pct, text=label)
                                        log_box.write("\n".join(lines[-200:]))
                                        last_flush[0] = now

                                with st.status("Importing videos...", expanded=True) as status:
                                    imported = _run_drive_import(drv_input, int(workers), _on_progress)
                                    lst = st.session_state[_rv].get(game, [])
                                    # Combine existing and newly imported files
                                    combined = lst + imported
                                    # Remove duplicates by filename (case-insensitive)
                                    deduplicated = fb_ops._dedupe_by_name(combined)
                                    st.session_state[_rv][game] = deduplicated
                                    new_count = len(imported)
                                    duplicate_count = len(combined) - len(deduplicated)
                                    status.update(label=f"Done: {new_count} files imported", state="complete")
                                    if isinstance(imported, dict) and imported.get("errors"):
                                        st.warning("\n".join(imported["errors"]))
                                if duplicate_count > 0:
                                    st.success(f"Imported {new_count} videos. ({duplicate_count} duplicates removed)")
                                else:
                                    st.success(f"Imported {new_count} videos.")
                                log_event("drive_import", mode=mode_str, game=game, platform=platform,
                                          upload_method="google_drive", file_count=new_count)
                            except Exception as e:
                                st.error(f"Import failed: {e}")
                                log_event("drive_import", mode=mode_str, game=game, platform=platform,
                                          upload_method="google_drive", error_message=str(e))
                    
                    else:  # 로컬 파일
                        st.markdown("**로컬 컴퓨터에서 Creative Videos를 업로드합니다**")
                        uploaded_files = st.file_uploader(
                            "파일 선택 (Video 또는 Playable)",
                            type=["mp4", "mov", "png", "jpg", "jpeg", "zip", "html"],
                            accept_multiple_files=True,
                            key=f"{kp}local_upload_{game}",
                            help="여러 파일을 선택할 수 있습니다. (.mp4, .png, .html 형식 지원)"
                        )
                        
                        if uploaded_files:
                            # 파일 제한 체크
                            MAX_FILES = 12
                            MAX_SIZE_MB = 100
                            
                            over_limit = len(uploaded_files) > MAX_FILES
                            large_files = [f for f in uploaded_files if f.size > MAX_SIZE_MB * 1024 * 1024]
                            
                            if over_limit:
                                st.error(f"⚠️ 한 번에 {MAX_FILES}개까지만 업로드 가능합니다. 현재: {len(uploaded_files)}개")
                            if large_files:
                                st.warning(f"⚠️ {MAX_SIZE_MB}MB 초과 파일 {len(large_files)}개는 Google Drive 사용을 권장합니다.")
                            
                            if st.button("로컬 파일 추가하기", key=f"{kp}local_add_{game}", width="stretch", disabled=over_limit):
                                try:
                                    import tempfile
                                    import pathlib
                                    import gc
                                    
                                    imported = []
                                    progress = st.progress(0, text="업로드 준비 중...")
                                    
                                    for idx, uploaded_file in enumerate(uploaded_files):
                                        try:
                                            suffix = pathlib.Path(uploaded_file.name).suffix or ".mp4"
                                            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                                                # 청크 단위로 쓰기 (메모리 절약)
                                                CHUNK_SIZE = 1024 * 1024  # 1MB
                                                while True:
                                                    chunk = uploaded_file.read(CHUNK_SIZE)
                                                    if not chunk:
                                                        break
                                                    tmp.write(chunk)
                                                tmp_path = tmp.name
                                            
                                            imported.append({"name": uploaded_file.name, "path": tmp_path})
                                            progress.progress((idx + 1) / len(uploaded_files), text=f"{uploaded_file.name} ({idx+1}/{len(uploaded_files)})")
                                        except Exception as e:
                                            st.warning(f"⚠️ {uploaded_file.name} 실패: {e}")
                                            continue
                                        finally:
                                            uploaded_file.close()
                                            gc.collect()
                                    
                                    progress.empty()
                                    
                                    # 기존 파일과 병합 및 중복 제거
                                    lst = st.session_state[_rv].get(game, [])
                                    combined = lst + imported
                                    deduplicated = fb_ops._dedupe_by_name(combined)
                                    st.session_state[_rv][game] = deduplicated
                                    
                                    new_count = len(imported)
                                    duplicate_count = len(combined) - len(deduplicated)
                                    
                                    if duplicate_count > 0:
                                        st.success(f"✅ {new_count}개 파일 추가됨 ({duplicate_count}개 중복 제거됨)")
                                    else:
                                        st.success(f"✅ {new_count}개 파일 추가됨")
                                    log_event("local_upload", mode=mode_str, game=game, platform=platform,
                                              upload_method="local", file_count=new_count)

                                    # 파일 업로더 초기화 (선택사항)
                                    st.rerun()

                                except Exception as e:
                                    st.error(f"파일 추가 실패: {e}")
                                    devtools.record_exception("Local file upload failed", e)
                                    log_event("local_upload", mode=mode_str, game=game, platform=platform,
                                              upload_method="local", error_message=str(e))
                        
                        # ✅ 선택된 비디오 초기화 버튼 (file_uploader만 초기화)
                        if uploaded_files or st.session_state.get(f"{kp}local_upload_{game}"):
                            if st.button("선택된 비디오 초기화", key=f"{kp}clear_selected_{game}", width="stretch"):
                                # file_uploader의 선택만 초기화
                                if f"{kp}local_upload_{game}" in st.session_state:
                                    del st.session_state[f"{kp}local_upload_{game}"]
                                st.session_state.current_tab_index = i  # Preserve current tab
                                st.rerun()

                    # --- Display List ---
                    remote_list = st.session_state[_rv].get(game, [])
                    st.caption("다운로드된 Creatives:")
                    if remote_list:
                        for it in remote_list[:20]: st.write("•", it["name"])
                        if len(remote_list) > 20: st.write(f"... and {len(remote_list)-20} more")
                    else:
                        st.write("- (None)")
                    
                    
                    # ✅ 다운로드된 Creatives 초기화 버튼 (remote_videos만 초기화)
                    if st.button("다운로드된 Creatives 초기화", key=f"{kp}clearurl_{game}", width="stretch"):
                        st.session_state[_rv][game] = []
                        st.session_state.current_tab_index = i  # Preserve current tab
                        st.rerun()
                    
                    # ✅ Applovin Media Library 업로드 (Marketer 모드 + Applovin 플랫폼)
                    if is_marketer and platform == "Applovin":
                        if st.button(
                            "📤 Media Library에 업로드",
                            key=f"{kp}applovin_media_upload_{game}",
                            width="stretch",
                            help="Drive/로컬에서 가져온 파일을 Applovin Media Library에 업로드합니다"
                        ):
                            remote_list = st.session_state[_rv].get(game, [])
                            if not remote_list:
                                st.warning("⚠️ 업로드할 파일이 없습니다. 먼저 파일을 가져오세요.")
                            else:
                                try:
                                    with st.status("📤 Uploading to Applovin Media Library...", expanded=True) as status:
                                        result = applovin_module._upload_assets_to_media_library(
                                            files=remote_list,
                                            max_workers=3
                                        )
                                        
                                        uploaded_count = result["total"]
                                        failed_count = result["failed"]
                                        
                                        if uploaded_count > 0:
                                            status.update(
                                                label=f"✅ Uploaded {uploaded_count} asset(s)",
                                                state="complete"
                                            )
                                            st.success(
                                                f"✅ Media Library 업로드 완료!\n\n"
                                                f"- 성공: {uploaded_count}개\n"
                                                f"- 실패: {failed_count}개"
                                            )
                                            
                                            # 업로드된 asset 목록 표시
                                            with st.expander("📋 업로드된 Asset 목록", expanded=False):
                                                for asset in result["uploaded_ids"]:
                                                    st.write(f"✅ {asset['name']} (ID: {asset['id']})")
                                            
                                            # Asset 캐시 무효화
                                            assets_key = f"{kp}applovin_assets_{game}"
                                            if assets_key in st.session_state:
                                                del st.session_state[assets_key]
                                            
                                            st.info("💡 'Load Applovin Data' 버튼을 다시 클릭하여 새 asset을 확인하세요.")
                                        else:
                                            status.update(label="❌ No assets uploaded", state="error")
                                            st.error("업로드 실패")
                                        
                                        if result["errors"]:
                                            with st.expander("⚠️ Upload Errors", expanded=False):
                                                for err in result["errors"]:
                                                    st.write(f"- {err}")
                                        log_event("applovin_media_library", mode=mode_str, game=game, platform="Applovin",
                                                  file_count=len(remote_list), success_count=uploaded_count,
                                                  error_count=failed_count,
                                                  error_message="; ".join(result["errors"]) if result["errors"] else None)
                                except Exception as e:
                                    st.error(f"❌ Media Library 업로드 실패: {e}")
                                    devtools.record_exception("Applovin media library upload failed", e)
                                    log_event("applovin_media_library", mode=mode_str, game=game, platform="Applovin",
                                              file_count=len(remote_list), error_message=str(e))

                    # --- Action Buttons ---
                    if platform == "Facebook":
                        ok_msg_placeholder = st.empty()
                        btn_label = "Creative 업로드하기" if is_marketer else "Creative Test 업로드하기"
                        
                        # [수정 3] 업로드 및 전체 초기화 버튼: 너비 꽉 채우기
                        # 간격을 두어 시각적으로 분리
                        st.write("") 
                        if is_marketer:
                            media_library_btn = st.button(
                                "📤 Media Library에 업로드 (모든 비디오)", 
                                key=f"{kp}media_library_{game}", 
                                width="stretch",
                                help="Drive에서 가져온 모든 비디오를 Account Media Library에 원본 파일명으로 저장합니다."
                            )
                            st.write("")
            
                        btn_label = "Creative 업로드하기" if is_marketer else "Creative Test 업로드하기"
                        cont = st.button(btn_label, key=f"{kp}continue_{game}", width="stretch")
                        # Store current tab in query params when button is clicked
                        if cont:
                            st.query_params[_tab] = game
                        clr = st.button("전체 초기화", key=f"{kp}clear_{game}", width="stretch")
                    elif platform == "Unity Ads":
                        unity_ok_placeholder = st.empty()
                        st.write("")
                        _unity_us_est = unity_module.get_unity_settings(game, prefix=prefix)
                        _unity_remote_est = st.session_state.get(_rv, {}).get(game, [])
                        _unity_pack_pages = st.number_input(
                            "Unity 추정용: 팩 목록 조회 페이지 수 (GET, 100개/페이지)",
                            min_value=1,
                            max_value=500,
                            value=1,
                            key=f"{kp}unity_est_pack_pages_{game}",
                            help="앱에 Creative Pack이 많을수록 목록 API가 여러 번 호출됩니다. 상한 추정에만 쓰입니다.",
                        )
                        _est_create = uni_ops.estimate_unity_create_api_calls(
                            _unity_remote_est,
                            settings=_unity_us_est,
                            pack_list_pages_guess=_unity_pack_pages,
                            is_marketer=is_marketer,
                        )
                        _created_est = st.session_state.get(_ucp, {}).get(game, [])
                        _est_apply = uni_ops.estimate_unity_apply_api_calls(
                            _unity_us_est,
                            _created_est,
                            is_marketer=is_marketer,
                        )
                        with st.expander("Unity 서버 요청 수 (추정 상한)", expanded=False):
                            st.caption(
                                "실제는 재개·이미 존재하는 에셋 스킵, 429 재시도 등으로 더 적을 수 있습니다."
                            )
                            if _est_create.get("pack_mode") == "none":
                                st.write("**팩 생성**: 파일이 없어 추정 불가")
                            else:
                                st.write(
                                    f"**팩 생성** (`{_est_create.get('pack_mode')}`, "
                                    f"플랫폼 실행 {_est_create.get('platform_runs', 0)}회 기준)\n"
                                    f"- GET 상한: **{_est_create.get('get_upper', 0)}**\n"
                                    f"- POST 상한: **{_est_create.get('post_upper', 0)}**\n"
                                    f"- 합계 상한: **{_est_create.get('total_upper', 0)}**"
                                )
                            for _w in _est_create.get("warnings") or []:
                                st.warning(_w)
                            st.write(
                                f"**캠페인 적용**\n"
                                f"- GET 상한: **{_est_apply.get('get_upper', 0)}**\n"
                                f"- POST 상한: **{_est_apply.get('post_upper', 0)}**\n"
                                f"- 합계 상한: **{_est_apply.get('total_upper', 0)}**"
                            )
                            for _w in _est_apply.get("warnings") or []:
                                st.caption(_w)
                        cont_unity_create = st.button("크리에이티브/팩 생성", key=f"{kp}unity_create_{game}", width="stretch")
                        # TODO(dry-run): Unity "캠페인에 적용" — assign 전용 미리보기(POST 생략, GET/diff만).
                        # preview_unity_upload 는 업로드·팩 생성 경로만 커버. Handover_upload_automation.md Unity 모듈(uni.py) TODO 참고.
                        cont_unity_apply = st.button("캠페인에 적용", key=f"{kp}unity_apply_{game}", width="stretch")
                        # Store current tab in query params when Unity buttons are clicked
                        if cont_unity_create or cont_unity_apply:
                            st.query_params[_tab] = game
                        clr_unity = st.button("전체 초기화 (Unity)", key=f"{kp}unity_clear_{game}", width="stretch")
                    elif platform == "Mintegral":
                        mintegral_ok_placeholder = st.empty()
                        st.write("")
                        # Expander 없이 바로 버튼
                        if st.button("📤 라이브러리에 업로드하기", key=f"{kp}mintegral_lib_upload_{game}", width="stretch"):
                            remote_list = st.session_state[_rv].get(game, [])
                            
                            if not remote_list:
                                st.warning("⚠️ 먼저 위에서 파일을 가져오세요 (Google Drive 또는 로컬 파일)")
                            else:
                                try:
                                    from modules.upload_automation.platforms.mintegral import mintegral as mintegral_module
                                    
                                    # Progress UI
                                    progress_bar = st.progress(0)
                                    status_text = st.empty()
                                    result_container = st.empty()
                                    
                                    completed = [0]
                                    total = len(remote_list)

                                    def on_progress(filename, success, error):
                                        completed[0] += 1
                                        progress_bar.progress(completed[0] / total)
                                        if success:
                                            status_text.success(f"✅ {filename} ({completed[0]}/{total})")
                                        else:
                                            status_text.error(f"❌ {filename} ({completed[0]}/{total})")

                                    result = mintegral_module.batch_upload_to_library(
                                        files=remote_list,
                                        max_workers=3,
                                        on_progress=on_progress  # ← callback 전달
                                    )
                                    
                                    # Clear progress UI
                                    progress_bar.empty()
                                    status_text.empty()
                                    
                                    # Show final result
                                    success_count = result["success"]
                                    failed_count = result["failed"]
                                    errors = result["errors"]
                                    
                                    if success_count > 0:
                                        result_container.success(f"✅ Upload Complete! Success: {success_count}, Failed: {failed_count}")
                                    else:
                                        result_container.error("❌ Upload failed")
                                    
                                    if errors:
                                        with st.expander("⚠️ Errors"):
                                            for err in errors:
                                                st.error(err)

                                    log_event("mintegral_library", mode=mode_str, game=game, platform="Mintegral",
                                              file_count=len(remote_list), success_count=success_count,
                                              error_count=failed_count,
                                              error_message="; ".join(errors) if errors else None)

                                    st.cache_data.clear()

                                except Exception as e:
                                    st.error(f"Upload failed: {e}")
                                    devtools.record_exception("Mintegral library upload failed", e)
                                    log_event("mintegral_library", mode=mode_str, game=game, platform="Mintegral",
                                              file_count=len(remote_list), error_message=str(e))

                        st.write("")  # Spacing
                        cont_mintegral = st.button("Mintegral Creative Set 업로드하기", key=f"{kp}mintegral_upload_{game}", width="stretch")
                        if cont_mintegral:
                            st.query_params[_tab] = game
                        clr_mintegral = st.button("전체 초기화 (Mintegral)", key=f"{kp}mintegral_clear_{game}", width="stretch")
                    elif platform == "Applovin":
                        applovin_ok_placeholder = st.empty()
                        st.write("")
                        
                        # Applovin 업로드 (2개 버튼)
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            cont_applovin_paused = st.button(
                                "⏸️ Applovin (Paused)",
                                key=f"{kp}applovin_upload_paused_{game}",
                                width="stretch",
                                type="secondary"
                            )
                        
                        with col2:
                            cont_applovin_live = st.button(
                                "▶️ Applovin (Live)",
                                key=f"{kp}applovin_upload_live_{game}",
                                width="stretch",
                                type="primary"
                            )
                        
                        if cont_applovin_paused or cont_applovin_live:
                            st.query_params[_tab] = game
                        
                        clr_applovin = st.button("전체 초기화 (Applovin)", key=f"{kp}applovin_clear_{game}", width="stretch")
                    elif platform == "Google Ads":
                        google_ok_placeholder = st.empty()
                        st.write("")

                        cont_google_asset_upload = st.button(
                            "📤 에셋 업로드 (라이브러리)",
                            key=f"{kp}google_asset_upload_{game}",
                            width="stretch",
                            type="secondary",
                            help="Drive/로컬에서 가져온 파일을 Google Ads 에셋 라이브러리에 업로드합니다",
                        )
                        cont_google_preview = st.button(
                            "📋 Preview Distribution Plan",
                            key=f"{kp}google_preview_{game}",
                            width="stretch",
                            type="secondary",
                        )
                        cont_google_distribute = st.button(
                            "📤 Google Ads 배치",
                            key=f"{kp}google_distribute_{game}",
                            width="stretch",
                            type="primary",
                            help="업로드된 에셋을 카테고리별 광고그룹에 배치합니다",
                        )
                        if cont_google_asset_upload or cont_google_preview or cont_google_distribute:
                            st.query_params[_tab] = game

                        clr_google = st.button("전체 초기화 (Google Ads)", key=f"{kp}google_clear_{game}", width="stretch")

            # =========================
            # RIGHT COLUMN: Settings
            # =========================
            # ━━━ 수정 후 (XP HERO만 Marketer UI) ━━━
            # RIGHT COLUMN: Settings
            if platform == "Facebook":
                with right_col:
                    fb_card = st.container(border=True)
                    fb_module.render_facebook_settings_panel(fb_card, game, i, prefix=prefix)

            elif platform == "Unity Ads":
                with right_col:
                    unity_card = st.container(border=True)
                    
                    try:
                        # Marketer Mode: All games support campaign selection and creative upload
                        if is_marketer:
                            unity_module.render_unity_settings_panel(unity_card, game, i, is_marketer=True, prefix=prefix)
                        else:
                            # Operation Mode: Use existing settings panel
                            uni_ops.render_unity_settings_panel(unity_card, game, i, is_marketer=False, prefix=prefix)
                    except Exception as e:
                        st.error(str(e) if str(e) else "Unity 설정 패널 로드 실패")
                        devtools.record_exception("Unity settings panel load failed", e)
            
            elif platform == "Mintegral":
                with right_col:
                    mintegral_card = st.container(border=True)
                    try:
                        from modules.upload_automation.platforms.mintegral import mintegral as mintegral_module
                        mintegral_module.render_mintegral_settings_panel(mintegral_card, game, i, is_marketer=is_marketer)
                    except Exception as e:
                        st.error(str(e) if str(e) else "Mintegral 설정 패널 로드 실패")
                        devtools.record_exception("Mintegral settings panel load failed", e)
            
            elif platform == "Applovin":
                with right_col:
                    applovin_card = st.container(border=True)
                    try:
                        applovin_module.render_applovin_settings_panel(applovin_card, game, i, is_marketer=is_marketer)
                    except Exception as e:
                        st.error(str(e) if str(e) else "Applovin 설정 패널 로드 실패")
                        devtools.record_exception("Applovin settings panel load failed", e)

            elif platform == "Google Ads":
                with right_col:
                    google_card = st.container(border=True)
                    try:
                        google_marketer.render_google_settings_panel(
                            google_card, game, i, is_marketer=True, prefix=prefix
                        )
                    except Exception as e:
                        st.error(str(e) if str(e) else "Google Ads 설정 패널 로드 실패")
                        devtools.record_exception("Google Ads settings panel load failed", e)

            # =========================
            # EXECUTION LOGIC
            # =========================
            if platform == "Facebook" and is_marketer and "media_library_btn" in locals() and media_library_btn:
                remote_list = st.session_state[_rv].get(game, [])
                ok, msg = validate_count(remote_list)
                if not ok:
                    ok_msg_placeholder.error(msg)
                else:
                    try:
                        # Get account
                        cfg = fb_ops.FB_GAME_MAPPING.get(game)
                        if not cfg:
                            raise ValueError(f"No FB mapping for {game}")
                        
                        account = fb_ops.init_fb_from_secrets(cfg["account_id"])
                        
                        # Upload all videos to media library
                        with st.status("📤 Uploading to Media Library...", expanded=True) as status:
                            result = fb_marketer.upload_all_videos_to_media_library(
                                account=account,
                                uploaded_files=remote_list,
                                max_workers=6
                            )
                            
                            uploaded_count = result["total"]
                            failed_count = result["failed"]
                            
                            if uploaded_count > 0:
                                status.update(
                                    label=f"✅ Uploaded {uploaded_count} video(s) to Media Library", 
                                    state="complete"
                                )
                                ok_msg_placeholder.success(
                                    f"✅ Media Library 업로드 완료!\n\n"
                                    f"- 성공: {uploaded_count}개\n"
                                    f"- 실패: {failed_count}개"
                                )
                            else:
                                status.update(label="❌ No videos uploaded", state="error")
                                ok_msg_placeholder.error("업로드 실패")
                            
                            # Show errors if any
                            if result["errors"]:
                                with st.expander("⚠️ Upload Errors", expanded=False):
                                    for err in result["errors"]:
                                        st.write(f"- {err}")
                            log_event("fb_media_library", mode=mode_str, game=game, platform="Facebook",
                                      file_count=len(remote_list), success_count=uploaded_count,
                                      error_count=failed_count,
                                      error_message="; ".join(result["errors"]) if result["errors"] else None)
                    except Exception as e:
                        # 유저에게는 핵심 메시지만 보여주고, traceback은 UI에 노출하지 않음
                        st.error(str(e) if str(e) else "❌ Media Library Upload Error")
                        log_event("fb_media_library", mode=mode_str, game=game, platform="Facebook",
                                  file_count=len(remote_list), error_message=str(e))

            # ✅ FACEBOOK DRY RUN 섹션 전체 제거 (449-540줄 정도)
            # --- FACEBOOK DRY RUN ---
            # if platform == "Facebook" and is_marketer and "dry_run_fb" in locals() and dry_run_fb:
            #     remote_list = st.session_state[_rv].get(game, [])
            #     ok, msg = validate_count(remote_list)
            #     if not ok:
            #         ok_msg_placeholder.error(msg)
            #     else:
            #         try:
            #             settings = st.session_state[_st].get(game, {})
            #             preview = fb_module.preview_facebook_upload(game, remote_list, settings)
                        
            #             with st.expander("📋 Facebook Upload Preview", expanded=True):
            #                 # Show error if present
            #                 if preview.get('error'):
            #                     st.error(f"❌ **Validation Error:**\n{preview['error']}")
            #                     st.markdown("---")
                            
            #                 st.markdown("### Campaign & Ad Set")
            #                 st.write(f"**Campaign ID:** {preview['campaign_id']}")
            #                 st.write(f"**Ad Set ID:** {preview['adset_id']}")
            #                 st.write(f"**Current Active Ads:** {preview['current_ad_count']}")
            #                 st.write(f"**New Videos to Upload:** {preview['n_videos']}")
            #                 st.write(f"**Creative Type:** {preview['creative_type']}")
                            
            #                 # Capacity Information
            #                 capacity = preview.get('capacity_info', {})
            #                 st.markdown("### Ad Set Capacity")
            #                 st.write(f"**Current Creatives:** {capacity.get('current_count', 0)}")
            #                 st.write(f"**Creative Limit:** {capacity.get('limit', 50)}")
            #                 st.write(f"**Available Slots:** {capacity.get('available_slots', 0)}")
            #                 st.write(f"**New Creatives to Upload:** {capacity.get('new_creatives_count', 0)}")
                            
            #                 if capacity.get('will_exceed', False):
            #                     st.warning(f"⚠️ 업로드 후 제한을 초과합니다! ({capacity.get('current_count', 0)} + {capacity.get('new_creatives_count', 0)} > {capacity.get('limit', 50)})")
                                
            #                     ads_to_delete = capacity.get('ads_to_delete', [])
            #                     if ads_to_delete:
            #                         st.markdown("#### ��️ 삭제될 Creative 목록")
            #                         st.write(f"**삭제 예정 Creative 수:** {len(ads_to_delete)}")
                                    
            #                         for idx, ad_info in enumerate(ads_to_delete, 1):
            #                             st.markdown(f"**{idx}. {ad_info.get('name', 'N/A')}** (ID: `{ad_info.get('id', 'N/A')}`)")
            #                             st.write(f"   - 14일 누적 Spend: ${ad_info.get('spend_14d', 0):.2f}")
            #                             st.write(f"   - 7일 누적 Spend: ${ad_info.get('spend_7d', 0):.2f}")
            #                             if ad_info.get('spend_14d', 0) < 1.0:
            #                                 st.write(f"   - 삭제 이유: 14일 누적 Spend < $1")
            #                             elif ad_info.get('spend_7d', 0) < 1.0:
            #                                 st.write(f"   - 삭제 이유: 7일 누적 Spend < $1")
            #                 else:
            #                     remaining = capacity.get('available_slots', 0) - capacity.get('new_creatives_count', 0)
            #                     if remaining >= 0:
            #                         st.success(f"✅ 충분한 공간이 있습니다. 업로드 후 남은 슬롯: {remaining}")
            #                     else:
            #                         st.warning(f"⚠️ 공간이 부족합니다. 추가로 {abs(remaining)}개의 슬롯이 필요합니다.")
                            
            #                 st.divider()
                            
            #                 st.markdown("### Template Settings (from existing ads)")
            #                 template = preview['template_source']
            #                 st.write(f"**Headlines Found:** {template['headlines_found']}")
            #                 if template['headline_example']:
            #                     st.write(f"**Example Headline:** `{template['headline_example']}`")
            #                 st.write(f"**Messages Found:** {template['messages_found']}")
            #                 if template['message_example']:
            #                     st.write(f"**Example Message:** `{template['message_example']}`")
            #                 if template['cta']:
            #                     # Handle both dict and string CTA formats
            #                     if isinstance(template['cta'], dict):
            #                         cta_type = template['cta'].get('type', 'N/A')
            #                         st.write(f"**CTA:** `{cta_type}`")
            #                     else:
            #                         st.write(f"**CTA:** `{template['cta']}`")
            #                 if preview['store_url']:
            #                     st.write(f"**Store URL:** {preview['store_url']}")
                            
            #                 st.markdown("### Creatives That Would Be Created")
            #                 for idx, creative in enumerate(preview['preview_creatives'], 1):
            #                     st.markdown(f"#### Creative {idx}: {creative['name']}")
            #                     st.write(f"**Type:** {creative['type']}")
                                
            #                     # For single video mode, show detailed video size and placement info
            #                     if creative.get('type') == 'Single Video (3 sizes)' and creative.get('videos'):
            #                         videos = creative['videos']
            #                         placements = creative.get('placements', {})
            #                         placements_kr = creative.get('placements_kr', {})
                                    
            #                         for size in ['1080x1080', '1920x1080', '1080x1920']:
            #                             if size in videos:
            #                                 st.markdown(f"**{size}:**")
            #                                 st.write(f"  - Video: `{videos[size]}`")
            #                                 if size in placements:
            #                                     st.write(f"  - Placements: {', '.join(placements[size])}")
            #                                 if size in placements_kr:
            #                                     st.write(f"  - Placements (KR): {', '.join(placements_kr[size])}")
            #                     elif creative.get('videos'):
            #                         # For dynamic mode or other types
            #                         if isinstance(creative['videos'], list):
            #                             st.write(f"**Videos:** {', '.join(creative['videos'])}")
            #                         elif isinstance(creative['videos'], dict):
            #                             st.write(f"**Videos:** {', '.join(creative['videos'].values())}")
                                
            #                     # Show ALL Headlines
            #                     if creative.get('headline'):
            #                         headlines = creative['headline'] if isinstance(creative['headline'], list) else [creative['headline']]
            #                         st.markdown(f"**Headlines ({len(headlines)} total):**")
            #                         for idx, h in enumerate(headlines, 1):
            #                             st.write(f"  {idx}. `{h}`")
                                
            #                     # Show ALL Messages (Primary Text)
            #                     if creative.get('message'):
            #                         messages = creative['message'] if isinstance(creative['message'], list) else [creative['message']]
            #                         st.markdown(f"**Primary Text / Messages ({len(messages)} total):**")
            #                         for idx, m in enumerate(messages, 1):
            #                             st.write(f"  {idx}. `{m}`")
                                
            #                     # Show CTA details
            #                     if creative.get('cta'):
            #                         cta = creative['cta']
            #                         st.markdown("**Call-to-Action (CTA):**")
            #                         if isinstance(cta, dict):
            #                             cta_type = cta.get('type', 'N/A')
            #                             st.write(f"  - Type: `{cta_type}`")
            #                             if 'value' in cta:
            #                                 value = cta['value']
            #                                 if isinstance(value, dict):
            #                                     if 'link' in value:
            #                                         st.write(f"  - Link: `{value['link']}`")
            #                                     for k, v in value.items():
            #                                         if k != 'link':
            #                                             st.write(f"  - {k}: `{v}`")
            #                                 else:
            #                                     st.write(f"  - Value: `{value}`")
            #                             # Show all CTA fields
            #                             for k, v in cta.items():
            #                                 if k not in ['type', 'value']:
            #                                     st.write(f"  - {k}: `{v}`")
            #                         else:
            #                             st.write(f"  - `{cta}`")
                                
            #                     # Show Store URL if available
            #                     if preview.get('store_url'):
            #                         st.write(f"**Store URL:** `{preview['store_url']}`")
                                
            #                     # Show Aspect Ratio for dynamic creatives
            #                     if creative.get('aspect_ratio'):
            #                         st.write(f"**Aspect Ratio:** {creative['aspect_ratio']}")
            #                     if creative.get('aspect_ratio'):
            #                         st.write(f"**Aspect Ratio:** {creative['aspect_ratio']}")
            #                     st.divider()
                            
            #                 st.info("�� This is a preview. No actual uploads or changes have been made.")
            # except Exception as e:
            #     import traceback
            #     st.error(f"Preview failed: {e}")
            #     st.code(traceback.format_exc())


            # �� EXECUTION LOGIC 섹션에 추가

            
            if platform == "Facebook" and cont:
                # Preserve current tab
                st.query_params[_tab] = game
                
                remote_list = st.session_state[_rv].get(game, [])
                ok, msg = validate_count(remote_list)
                if not ok:
                    ok_msg_placeholder.error(msg)
                else:
                    try:
                        st.session_state[_up][game] = remote_list
                        settings = st.session_state[_st].get(game, {})
                        settings["_prefix"] = prefix
        
                        # ✅ 디버깅 메시지
                        if devtools.dev_enabled():
                            st.info(f"🔍 Mode: {'Marketer' if is_marketer else 'Test'}")
                            st.info(f"🔍 Using module: {fb_module.__name__}")
                            if "creative_type" in settings:
                                st.info(f"🔍 Creative Type: {settings['creative_type']}")

                            # ✅ Marketer Mode인 경우 adset_id 확인
                            if is_marketer:
                                adset_id = settings.get("adset_id")
                                st.info(f"🔍 Selected AdSet ID: {adset_id if adset_id else '❌ 없음'}")
                        
                        plan = fb_module.upload_to_facebook(game, remote_list, settings)
                        
                        if isinstance(plan, dict) and plan.get("adset_id"):
                            # Marketer fb.py returns ads_created/errors; ops facebook_ads.py returns only adset_id.
                            ads_created = plan.get("ads_created", None)
                            errors = plan.get("errors") or []

                            if ads_created is None:
                                ok_msg_placeholder.success("✅ Uploaded successfully! Ad Set created.")
                            elif int(ads_created) > 0:
                                ok_msg_placeholder.success(f"✅ Uploaded successfully! Ads created: {int(ads_created)}")
                            else:
                                # Prefer a concise first error if available
                                ok_msg_placeholder.error(errors[0] if errors else "❌ Upload failed.")
                        else:
                            ok_msg_placeholder.error("❌ Upload failed or no Ad Set ID returned.")

                        log_event("fb_upload", mode=mode_str, game=game, platform="Facebook",
                                  file_count=len(remote_list),
                                  success_count=plan.get("ads_created") if isinstance(plan, dict) else None,
                                  error_count=len(plan.get("errors", [])) if isinstance(plan, dict) else None,
                                  error_message="; ".join(plan.get("errors", [])) if isinstance(plan, dict) and plan.get("errors") else None,
                                  settings=st.session_state[_st].get(game, {}),
                                  result={"adset_id": plan.get("adset_id"), "ads_created": plan.get("ads_created")} if isinstance(plan, dict) else None)
                    except Exception as e:
                        # 유저에게는 핵심 메시지만 보여주고, traceback은 UI에 노출하지 않음
                        st.error(str(e) if str(e) else "❌ Upload Error")
                        log_event("fb_upload", mode=mode_str, game=game, platform="Facebook",
                                  file_count=len(remote_list), error_message=str(e))
                    finally:
                        # Ensure tab is preserved even after upload
                        st.query_params[_tab] = game
            if platform == "Facebook" and clr:
                st.session_state[_up].pop(game, None)
                st.session_state[_rv].pop(game, None)
                st.session_state[_st].pop(game, None)
                st.query_params[_tab] = game  # Preserve current tab
                st.rerun()

            # ✅ UNITY DRY RUN 섹션 전체 제거
            # --- UNITY DRY RUN ---
            # if platform == "Unity Ads" and is_marketer and "dry_run_unity" in locals() and dry_run_unity:
            #     remote_list = st.session_state[_rv].get(game, [])
            #     ok, msg = validate_count(remote_list)
            #     if not ok:
            #         unity_ok_placeholder.error(msg)
            #     else:
            #         try:
            #             unity_settings = unity_module.get_unity_settings(game, prefix=prefix)
            #             preview = unity_module.preview_unity_upload(
            #                 game=game,
            #                 videos=remote_list,
            #                 settings=unity_settings,
            #                 is_marketer=True  # All games in marketer mode
            #             )
                        
            #             with st.expander("📋 Unity Ads Upload Preview", expanded=True):
            #                 st.markdown("### Campaign Settings")
            #                 st.write(f"**Game:** {preview['game']}")
            #                 st.write(f"**Org ID:** {preview['org_id']}")
            #                 st.write(f"**Title ID:** {preview['title_id']}")
            #                 st.write(f"**Campaign ID:** {preview['campaign_id']}")
                            
            #                 st.markdown("### Playable Info")
            #                 playable_info = preview['playable_info']
            #                 if playable_info['selected_playable']:
            #                     st.write(f"**Selected Playable:** {playable_info['selected_playable']}")
            #                 elif playable_info['existing_playable_label']:
            #                     st.write(f"**Existing Playable:** {playable_info['existing_playable_label']}")
            #                 else:
            #                     st.warning("⚠️ No playable selected")
                            
            #                 st.markdown("### Creative Packs That Would Be Created")
            #                 st.write(f"**Total Packs:** {preview['total_packs_to_create']}")
            #                 for idx, pack in enumerate(preview['preview_packs'], 1):
            #                     st.markdown(f"#### Pack {idx}: `{pack['pack_name']}`")
            #                     st.write(f"**Portrait Video:** {pack['portrait_video']}")
            #                     st.write(f"**Landscape Video:** {pack['landscape_video']}")
            #                     st.write(f"**Playable:** {pack['playable']}")
            #                     st.divider()
                            
            #                 st.markdown("### Current Assignment Status")
            #                 current = preview['current_assigned_packs']
            #                 if current:
            #                     st.write(f"**Currently Assigned Packs:** {len(current)}")
            #                     for pack in current:
            #                         st.write(f"- `{pack['name']}` (ID: {pack['id']})")
            #                 else:
            #                     st.info("No packs currently assigned to this campaign")
                            
            #                 st.markdown("### Action Summary")
            #                 summary = preview['action_summary']
            #                 st.write(f"**Will Create:** {summary['will_create_packs']} new creative pack(s)")
                            
            #                 if summary['is_marketer_mode']:
            #                     st.write(f"**Will Assign:** {summary['will_assign_new']} new pack(s)")
            #                     st.info("ℹ️ Marketer Mode: Existing packs will remain assigned. New packs will be added.")
            #                 else:
            #                     st.write(f"**Will Unassign:** {summary['will_unassign_existing']} existing pack(s)")
            #                     st.write(f"**Will Assign:** {summary['will_assign_new']} new pack(s)")
            #                     if summary['will_unassign_existing'] > 0:
            #                         st.warning("⚠️ Test Mode: Existing creative packs will be unassigned before assigning new ones.")
                            
            #                 st.info("�� This is a preview. No actual uploads or changes have been made.")
            # except Exception as e:
            #     import traceback
            #     st.error(f"Preview failed: {e}")
            #     st.code(traceback.format_exc())
            
            # --- UNITY ACTIONS ---
            if platform == "Unity Ads":
                unity_settings = unity_module.get_unity_settings(game, prefix=prefix)
                if _ucp not in st.session_state:
                    st.session_state[_ucp] = {}

                # 1. Create Logic
                if "cont_unity_create" in locals() and cont_unity_create:
                    # Preserve current tab
                    st.query_params[_tab] = game
                    
                    remote_list = st.session_state[_rv].get(game, [])
                    ok, msg = validate_count(remote_list)
                    if not ok:
                        unity_ok_placeholder.error(msg)
                    else:
                        try:
                            summary = unity_module.upload_unity_creatives_to_campaign(
                                game=game, videos=remote_list, settings=unity_settings
                            )
                            _ctx = summary.get("upload_context") or {}
                            if _ctx:
                                st.info(
                                    "Unity 생성 컨텍스트\n"
                                    f"- org_id: `{_ctx.get('org_id', '')}`\n"
                                    f"- title_id: `{_ctx.get('title_id', '')}`\n"
                                    f"- campaign_id: `{_ctx.get('campaign_id', '')}`\n"
                                    f"- platform: `{_ctx.get('platform', '')}`\n"
                                    f"- title_id_source: `{_ctx.get('title_id_source', '')}`"
                                )
                                if _ctx.get("title_id_source") == "campaign_set":
                                    st.warning(
                                        "이 실행은 campaign set ID 기반 컨텍스트입니다. "
                                        "Unity Ads 콘솔에서 일반 앱 소재 리스트가 아닌, 동일 캠페인 컨텍스트에서 확인하세요."
                                    )
                            _pack_records = summary.get("created_pack_records") or []
                            if _pack_records:
                                with st.expander(f"생성/사용된 Pack 목록 ({len(_pack_records)}개)", expanded=False):
                                    for rec in _pack_records[:100]:
                                        st.write(f"- `{rec.get('pack_name', '')}` ({rec.get('pack_id', '')})")
                            _new_pack_count = int(summary.get("created_new_pack_count") or 0)
                            _reused_pack_count = int(summary.get("reused_existing_pack_count") or 0)
                            _new_video_count = int(summary.get("created_new_video_creative_count") or 0)
                            _reused_video_count = int(summary.get("reused_existing_video_creative_count") or 0)
                            _new_playable_count = int(summary.get("created_new_playable_creative_count") or 0)
                            _reused_playable_count = int(summary.get("reused_existing_playable_creative_count") or 0)
                            if any([
                                _new_pack_count,
                                _reused_pack_count,
                                _new_video_count,
                                _reused_video_count,
                                _new_playable_count,
                                _reused_playable_count,
                            ]):
                                st.info(
                                    "실행 결과 상세\n"
                                    f"- Pack: 신규 `{_new_pack_count}` / 재사용 `{_reused_pack_count}`\n"
                                    f"- Video Creative: 신규 `{_new_video_count}` / 재사용 `{_reused_video_count}`\n"
                                    f"- Playable Creative: 신규 `{_new_playable_count}` / 재사용 `{_reused_playable_count}`"
                                )
                            
                            # 플랫폼별 결과 처리
                            if summary.get("results_per_platform"):
                                # 새 구조: 플랫폼별 pack IDs
                                pack_ids_by_platform = {}
                                total_packs = 0
                                
                                for plat, plat_result in summary["results_per_platform"].items():
                                    plat_pack_ids = plat_result.get("creative_ids", [])
                                    pack_ids_by_platform[plat] = plat_pack_ids
                                    total_packs += len(plat_pack_ids)
                                    
                                    if plat_result.get("errors"):
                                        for err in plat_result["errors"]:
                                            st.warning(f"[{plat.upper()}] {err}")
                                
                                st.session_state[_ucp][game] = pack_ids_by_platform
                                
                                if total_packs > 0:
                                    unity_ok_placeholder.success(f"Created {total_packs} Creative Packs across {len(pack_ids_by_platform)} platform(s).")
                                else:
                                    unity_ok_placeholder.warning("No packs created.")
                            else:
                                # 하위 호환: 기존 단일 플랫폼 구조
                                pack_ids = summary.get("creative_ids", [])
                                st.session_state[_ucp][game] = pack_ids
                                
                                if pack_ids:
                                    unity_ok_placeholder.success(f"Created {len(pack_ids)} Creative Packs.")
                                else:
                                    unity_ok_placeholder.warning("No packs created.")
                            
                            if summary.get("errors"):
                                st.error("\n".join(summary["errors"]))

                            _total_packs = (sum(len(r.get("creative_ids", [])) for r in summary.get("results_per_platform", {}).values())
                                            if summary.get("results_per_platform") else len(summary.get("creative_ids", [])))
                            _all_errors = summary.get("errors", [])
                            log_event("unity_create", mode=mode_str, game=game, platform="Unity Ads",
                                      file_count=len(remote_list), success_count=_total_packs,
                                      error_count=len(_all_errors),
                                      error_message="; ".join(_all_errors) or None,
                                      settings=unity_settings)

                        except Exception as e:
                            st.error(str(e) if str(e) else "Unity upload failed")
                            devtools.record_exception("Unity upload failed", e)
                            log_event("unity_create", mode=mode_str, game=game, platform="Unity Ads",
                                      file_count=len(remote_list), error_message=str(e))
                        finally:
                            # Ensure tab is preserved even after upload
                            st.query_params[_tab] = game

                # 2. Apply Logic
                # 2. Apply Logic
                if "cont_unity_apply" in locals() and cont_unity_apply:
                    # Preserve current tab
                    st.query_params[_tab] = game
                    
                    # 오른쪽 패널에서 선택한 pack 확인
                    packs_per_campaign = unity_settings.get("packs_per_campaign", {})
                    has_selected_packs = any(v.get("pack_ids") for v in packs_per_campaign.values())
                    
                    # 방금 생성한 pack 확인
                    created_packs = st.session_state[_ucp].get(game, [])
                    
                    if not has_selected_packs and not created_packs:
                        unity_ok_placeholder.error("No packs selected. Select packs from the right panel first.")
                    else:
                        try:
                            # pack_ids는 apply 함수 내부에서 packs_per_campaign을 우선 사용함
                            res = unity_module.apply_unity_creative_packs_to_campaign(
                                game=game, creative_pack_ids=created_packs, settings=unity_settings, is_marketer=is_marketer
                            )
                            
                            # 플랫폼별 결과 처리
                            if res.get("results_per_campaign"):
                                # 새 구조: 플랫폼별 + 캠페인별 결과
                                total_assigned = 0
                                for key, campaign_res in res["results_per_campaign"].items():
                                    assigned_count = len(campaign_res.get("assigned_packs", []))
                                    total_assigned += assigned_count
                                
                                if total_assigned > 0:
                                    unity_ok_placeholder.success(f"✅ Assigned packs to {len(res['results_per_campaign'])} campaign(s).")
                                else:
                                    unity_ok_placeholder.warning("No packs assigned.")
                            else:
                                # 하위 호환
                                assigned = res.get("assigned_packs", [])
                                removed = res.get("removed_assignments", [])
                                
                                if not is_marketer and removed:
                                    unity_ok_placeholder.success(f"✅ Unassigned {len(removed)} existing pack(s).")
                                
                                if assigned:
                                    unity_ok_placeholder.success(f"✅ Assigned {len(assigned)} new pack(s).")
                                else:
                                    unity_ok_placeholder.warning("No packs assigned.")
                            
                            if res.get("errors"):
                                st.error("\n".join(res["errors"]))

                            rpc = res.get("results_per_campaign") or {}
                            _apply_success = (
                                sum(len(v.get("assigned_packs", [])) for v in rpc.values())
                                if rpc
                                else len(res.get("assigned_packs", []))
                            )
                            _apply_errors = res.get("errors", [])
                            log_event("unity_apply", mode=mode_str, game=game, platform="Unity Ads",
                                      success_count=_apply_success,
                                      error_count=len(_apply_errors),
                                      error_message="; ".join(_apply_errors) or None,
                                      settings=unity_settings)

                        except Exception as e:
                            st.error(str(e) if str(e) else "Unity apply failed")
                            devtools.record_exception("Unity apply failed", e)
                            log_event("unity_apply", mode=mode_str, game=game, platform="Unity Ads",
                                      error_message=str(e))
                        finally:
                            # Ensure tab is preserved even after apply
                            st.query_params[_tab] = game
                
                if "clr_unity" in locals() and clr_unity:
                    st.session_state[_us].pop(game, None)
                    # main uni: 마케터 설정은 항상 전역 `unity_settings`. vn 탭 등은 _us만 비우면 남을 수 있어 동기화.
                    if not uni_marketer.unity_use_namespaced_settings():
                        _ug = st.session_state.get("unity_settings")
                        if isinstance(_ug, dict):
                            _ug.pop(game, None)
                    st.session_state[_rv].pop(game, None)
                    st.query_params[_tab] = game  # Preserve current tab
                    st.rerun()
            
            # --- MINTEGRAL ACTIONS ---
            if platform == "Mintegral":
                if "cont_mintegral" in locals() and cont_mintegral:
                    st.query_params[_tab] = game
                    
                    try:
                        from modules.upload_automation.platforms.mintegral import mintegral as mintegral_module
                        mintegral_settings = mintegral_module.get_mintegral_settings(game)
                        
                        mode = mintegral_settings.get("mode", "upload")
                        
                        # Validate based on mode
                        if mode == "upload":
                            # Upload mode validation
                            if not mintegral_settings.get("selected_offer_ids"):
                                mintegral_ok_placeholder.error("❌ Offer를 선택해주세요.")
                            elif not (mintegral_settings.get("selected_images") or 
                                    mintegral_settings.get("selected_videos") or 
                                    mintegral_settings.get("selected_playables") or
                                    mintegral_settings.get("product_icon_md5")):
                                mintegral_ok_placeholder.error("❌ 최소 1개 이상의 Creative를 선택해주세요.")
                            else:
                                # ✅ 상세 에러 표시
                                with st.spinner("⏳ Uploading to Mintegral..."):
                                    result = mintegral_module.upload_to_mintegral(
                                        game=game,
                                        videos=[],
                                        settings=mintegral_settings
                                    )
                                
                                if result.get("success"):
                                    mintegral_ok_placeholder.success(f"✅ {result.get('message', 'Upload complete')}")
                                else:
                                    # ✅ 에러 메시지 상세 표시
                                    error_msg = result.get('error', 'Unknown error')
                                    mintegral_ok_placeholder.error(f"❌ {error_msg}")
                                    
                                    # ✅ errors 리스트도 표시
                                    if result.get("errors"):
                                        with st.expander("🔍 상세 에러 로그", expanded=True):
                                            for err in result["errors"]:
                                                st.error(f"• {err}")
                                    
                                    # ✅ 로그 파일 확인 안내
                                    st.info("💡 더 자세한 로그는 Streamlit Cloud → Logs 탭에서 확인하세요")

                                log_event("mintegral_upload", mode=mode_str, game=game, platform="Mintegral",
                                          success_count=1 if result.get("success") else 0,
                                          error_count=len(result.get("errors", [])),
                                          error_message=result.get("error") or ("; ".join(result.get("errors", [])) or None),
                                          settings=mintegral_settings)

                        elif mode == "copy":
                            # Copy mode validation
                            if not mintegral_settings.get("selected_creative_sets"):
                                mintegral_ok_placeholder.error("❌ 복사할 Creative Set을 선택해주세요.")
                            elif not mintegral_settings.get("target_offer_ids"):
                                mintegral_ok_placeholder.error("❌ 복사 대상 Offer를 선택해주세요.")
                            else:
                                with st.spinner("⏳ Copying Creative Sets..."):
                                    result = mintegral_module.upload_to_mintegral(
                                        game=game,
                                        videos=[],
                                        settings=mintegral_settings
                                    )
                                
                                if result.get("success"):
                                    mintegral_ok_placeholder.success(f"✅ {result.get('message', 'Copy complete')}")
                                else:
                                    error_msg = result.get('error', 'Unknown error')
                                    mintegral_ok_placeholder.error(f"❌ {error_msg}")
                                    
                                    if result.get("errors"):
                                        with st.expander("🔍 상세 에러 로그", expanded=True):
                                            for err in result["errors"]:
                                                st.error(f"• {err}")
                                    
                                    st.info("💡 더 자세한 로그는 Streamlit Cloud → Logs 탭에서 확인하세요")

                                log_event("mintegral_upload", mode=mode_str, game=game, platform="Mintegral",
                                          success_count=1 if result.get("success") else 0,
                                          error_count=len(result.get("errors", [])),
                                          error_message=result.get("error") or ("; ".join(result.get("errors", [])) or None),
                                          settings=mintegral_settings)

                        elif mode == "delete":
                            # Delete mode validation
                            if not mintegral_settings.get("selected_creative_sets"):
                                mintegral_ok_placeholder.error("❌ 삭제할 Creative Set을 선택해주세요.")
                            elif not mintegral_settings.get("delete_confirmed"):
                                mintegral_ok_placeholder.error("❌ 삭제 확인 체크박스를 선택해주세요.")
                            else:
                                with st.spinner("⏳ Deleting Creative Sets..."):
                                    result = mintegral_module.upload_to_mintegral(
                                        game=game,
                                        videos=[],
                                        settings=mintegral_settings
                                    )

                                if result.get("success"):
                                    mintegral_ok_placeholder.success(f"✅ {result.get('message', 'Delete complete')}")
                                    # Clear cached creative sets so list refreshes on next load
                                    delete_cache_key = f"mintegral_delete_creative_sets_data_{i}"
                                    st.session_state.pop(delete_cache_key, None)
                                else:
                                    error_msg = result.get('error', 'Unknown error')
                                    mintegral_ok_placeholder.error(f"❌ {error_msg}")

                                    if result.get("errors"):
                                        with st.expander("🔍 상세 에러 로그", expanded=True):
                                            for err in result["errors"]:
                                                st.error(f"• {err}")

                                    st.info("💡 더 자세한 로그는 Streamlit Cloud → Logs 탭에서 확인하세요")

                                log_event("mintegral_upload", mode=mode_str, game=game, platform="Mintegral",
                                          success_count=1 if result.get("success") else 0,
                                          error_count=len(result.get("errors", [])),
                                          error_message=result.get("error") or ("; ".join(result.get("errors", [])) or None),
                                          settings=mintegral_settings)

                    except Exception as e:
                        st.error(str(e) if str(e) else "Mintegral upload failed")
                        devtools.record_exception("Mintegral upload failed", e)
                        log_event("mintegral_upload", mode=mode_str, game=game, platform="Mintegral",
                                  error_message=str(e))
                    finally:
                        st.query_params[_tab] = game
                
                if "clr_mintegral" in locals() and clr_mintegral:
                    if _key(prefix, "mintegral_settings") in st.session_state:
                        st.session_state[_key(prefix, "mintegral_settings")].pop(game, None)
                    st.session_state[_rv].pop(game, None)
                    st.query_params[_tab] = game
                    st.rerun()
            
            # --- APPLOVIN ACTIONS ---
            if platform == "Applovin":
                # Paused 버튼 클릭 시
                if "cont_applovin_paused" in locals() and cont_applovin_paused:
                    st.query_params[_tab] = game
                    
                    applovin_settings = applovin_module.get_applovin_settings(game)
                    
                    if applovin_settings:
                        applovin_module._upload_creative_set(game, i, status="PAUSED")
                        log_event("applovin_upload", mode=mode_str, game=game, platform="Applovin",
                                  settings=applovin_settings, result={"status": "PAUSED"})
                    else:
                        applovin_ok_placeholder.warning(f"⚠️ {game}의 Applovin 설정을 먼저 완료해주세요.")

                # Live 버튼 클릭 시
                if "cont_applovin_live" in locals() and cont_applovin_live:
                    st.query_params[_tab] = game

                    applovin_settings = applovin_module.get_applovin_settings(game)

                    if applovin_settings:
                        applovin_module._upload_creative_set(game, i, status="LIVE")
                        log_event("applovin_upload", mode=mode_str, game=game, platform="Applovin",
                                  settings=applovin_settings, result={"status": "LIVE"})
                    else:
                        applovin_ok_placeholder.warning(f"⚠️ {game}의 Applovin 설정을 먼저 완료해주세요.")
                
                if "clr_applovin" in locals() and clr_applovin:
                    if _key(prefix, "applovin_settings") in st.session_state:
                        st.session_state[_key(prefix, "applovin_settings")].pop(game, None)
                    st.session_state[_rv].pop(game, None)
                    st.query_params[_tab] = game
                    st.rerun()

            # --- GOOGLE ADS ACTIONS ---
            if platform == "Google Ads":
                # Step 1: Asset Upload to Library
                if "cont_google_asset_upload" in locals() and cont_google_asset_upload:
                    st.query_params[_tab] = game
                    remote_list = st.session_state[_rv].get(game, [])
                    if not remote_list:
                        google_ok_placeholder.warning("업로드할 파일이 없습니다. 먼저 파일을 가져오세요.")
                    else:
                        try:
                            progress_bar = st.progress(0, text="에셋 업로드 준비 중...")

                            def _on_asset_progress(done, total, name, err):
                                pct = int((done / max(total, 1)) * 100)
                                if err:
                                    progress_bar.progress(pct, text=f"❌ {name}: {err}")
                                else:
                                    progress_bar.progress(pct, text=f"✅ {name} ({done}/{total})")

                            result = google_marketer.upload_assets_to_library(
                                game=game,
                                uploaded_files=remote_list,
                                prefix=prefix,
                                on_progress=_on_asset_progress,
                            )
                            progress_bar.empty()

                            if result["success"] > 0:
                                google_ok_placeholder.success(
                                    f"에셋 업로드 완료! 성공: {result['success']}개, 실패: {result['failed']}개"
                                )
                            else:
                                google_ok_placeholder.error("에셋 업로드 실패")

                            if result["errors"]:
                                with st.expander("에셋 업로드 에러", expanded=False):
                                    for err in result["errors"]:
                                        st.error(f"• {err}")
                            log_event("google_asset_upload", mode=mode_str, game=game, platform="Google Ads",
                                      file_count=len(remote_list), success_count=result["success"],
                                      error_count=result["failed"],
                                      error_message="; ".join(result["errors"]) if result["errors"] else None)
                        except Exception as e:
                            google_ok_placeholder.error(f"에셋 업로드 실패: {e}")
                            devtools.record_exception("Google Ads asset upload failed", e)
                            log_event("google_asset_upload", mode=mode_str, game=game, platform="Google Ads",
                                      file_count=len(remote_list), error_message=str(e))

                # Preview Distribution Plan
                if "cont_google_preview" in locals() and cont_google_preview:
                    st.query_params[_tab] = game
                    try:
                        plan = google_marketer.preview_google_upload(game, prefix=prefix)
                        if plan.get("error"):
                            google_ok_placeholder.error(plan["error"])
                        elif plan.get("type") == "category_based":
                            st.markdown("#### Distribution Preview")
                            st.markdown(f"**캠페인:** {plan.get('campaign_name', '')}")
                            categories = plan.get("categories", {})
                            for cat_id, cat_info in categories.items():
                                label = cat_info["label"]
                                videos = cat_info.get("videos", [])
                                playables = cat_info.get("playables", [])
                                ag_count = cat_info.get("ad_group_count", 0)

                                st.markdown(f"---\n**[{label}]** → {ag_count}개 광고그룹")
                                if videos:
                                    st.markdown(f"  영상 {len(videos)}개:")
                                    for vi, v in enumerate(videos):
                                        st.text(f"    {vi+1}. {v}")
                                if playables:
                                    st.markdown(f"  플레이어블 {len(playables)}개:")
                                    for p in playables:
                                        st.text(f"    → {p}")
                        else:
                            google_ok_placeholder.info("배치 계획이 없습니다.")
                    except Exception as e:
                        google_ok_placeholder.error(f"Preview 실패: {e}")
                        devtools.record_exception("Google Ads preview failed", e)

                # Step 2: Execute Distribution
                if "cont_google_distribute" in locals() and cont_google_distribute:
                    st.query_params[_tab] = game
                    try:
                        with st.spinner("Google Ads 배치 중..."):
                            result = google_marketer.distribute_by_category(
                                game=game,
                                prefix=prefix,
                            )
                        if result.get("success"):
                            msg = f"Google Ads 배치 완료! (성공: {result.get('total_success', 0)})"
                            details = result.get("details", [])
                            if details:
                                detail_lines = []
                                for d in details:
                                    cat = d.get("category", "")
                                    if d["type"] == "video":
                                        detail_lines.append(
                                            f"[{cat}] 영상 {d.get('placed', 0)}개 배치, "
                                            f"{d.get('ad_groups_modified', 0)}개 광고그룹 수정"
                                        )
                                    elif d["type"] == "playable":
                                        detail_lines.append(
                                            f"[{cat}] 플레이어블 '{d.get('name', '')}' → "
                                            f"{d.get('ad_groups_success', 0)}개 광고그룹"
                                        )
                                    elif d["type"] == "clone":
                                        detail_lines.append(
                                            f"[복제] '{d.get('name', '')}' 생성 — "
                                            f"미배치 영상 {d.get('video_count', 0)}개 (원본: {d.get('source', '')})"
                                        )
                                msg += "\n\n" + "\n".join(detail_lines)

                            # Unplaced info
                            unplaced_rns = result.get("unplaced_video_rns", [])
                            if unplaced_rns and not result.get("clone_result"):
                                msg += f"\n\n⚠️ 미배치 영상 {len(unplaced_rns)}개"
                            google_ok_placeholder.success(msg)
                        else:
                            google_ok_placeholder.error(
                                f"배치 실패: {result.get('error', 'Unknown error')}"
                            )
                        if result.get("errors"):
                            with st.expander("상세 에러 로그", expanded=True):
                                for err in result["errors"]:
                                    st.error(f"• {err}")
                        log_event("google_distribute", mode=mode_str, game=game, platform="Google Ads",
                                  success_count=result.get("total_success", 0),
                                  error_count=len(result.get("errors", [])),
                                  error_message="; ".join(result.get("errors", [])) or None,
                                  result={"details": result.get("details"), "unplaced": len(result.get("unplaced_video_rns", []))})
                    except Exception as e:
                        google_ok_placeholder.error(f"배치 실패: {e}")
                        devtools.record_exception("Google Ads distribute failed", e)
                        log_event("google_distribute", mode=mode_str, game=game, platform="Google Ads",
                                  error_message=str(e))

                if "clr_google" in locals() and clr_google:
                    gsk = _key(prefix, "google_settings")
                    if gsk in st.session_state:
                        st.session_state[gsk].pop(game, None)
                    # Also clear uploaded assets
                    assets_key = f"{kp}gads_uploaded_assets_{game}"
                    if assets_key in st.session_state:
                        del st.session_state[assets_key]
                    st.session_state[_rv].pop(game, None)
                    st.query_params[_tab] = game
                    st.rerun()

    # Summary
    st.subheader("Upload Summary")
    if st.session_state[_up]:
        data = [{"Game": k, "Files": len(v)} for k, v in st.session_state[_up].items()]
        st.dataframe(data)


# ======================================================================
# PAGE ROUTING
# ======================================================================



# def run():
#     """
#     Main entry point called by the parent app.
#     """
#     # ------------------------------------------------------------
#     # [수정됨] 사이드바 대신 메인 화면 상단에 모드 선택 버튼 배치
#     # ------------------------------------------------------------
    
#     # 페이지 상태 초기화
#     if "page" not in st.session_state:
#         st.session_state["page"] = "Creative 자동 업로드"

#     # 상단에 모드 전환 버튼 배치 (Tab 내부 상단에 위치하게 됨)
#     st.markdown("#### 🛠️ 모드 선택")
#     st.markdown("""
#     <style>
#     div[data-testid="stButton"] button,
#     .stButton > button {
#         width: 100% !important;
#         max-width: 400px !important;
#         height: auto !important;
#         min-height: 50px !important;
#         border-radius: 12px !important;
#         padding: 14px 24px !important;
        
#         background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%) !important;
#         border: 2px solid #ff006e !important;
        
#         box-shadow: 
#             0 4px 15px rgba(0, 0, 0, 0.8),
#             0 0 20px rgba(255, 0, 110, 0.4),
#             inset 0 2px 8px rgba(255, 255, 255, 0.1) !important;
        
#         transition: all 0.3s ease !important;
#     }

#     div[data-testid="stButton"] button p,
#     .stButton > button p {
#         font-size: 15px !important;
#         font-weight: 700 !important;
#         line-height: 1.4 !important;
#         letter-spacing: 0.5px !important;
#         white-space: nowrap !important;
#         color: #ff006e !important;
#         text-align: center !important;
#         text-shadow: 
#             0 0 10px rgba(255, 0, 110, 0.6),
#             0 0 20px rgba(255, 0, 110, 0.3) !important;
#         margin: 0 !important;
#         padding: 0 !important;
#     }

#     div[data-testid="stButton"] button:hover,
#     .stButton > button:hover {
#         transform: translateY(-3px) scale(1.02) !important;
#         background: linear-gradient(135deg, #2a1a3e 0%, #261e4e 50%, #1f4470 100%) !important;
#         border-color: #ff4d8f !important;
#         box-shadow: 
#             0 8px 25px rgba(0, 0, 0, 0.9),
#             0 0 35px rgba(255, 0, 110, 0.7),
#             inset 0 3px 10px rgba(255, 0, 110, 0.2) !important;
#     }

#     div[data-testid="stButton"] button:hover p,
#     .stButton > button:hover p {
#         color: #ff77a0 !important;
#         text-shadow: 
#             0 0 15px rgba(255, 0, 110, 0.8),
#             0 0 25px rgba(255, 0, 110, 0.4) !important;
#     }
#     </style>
#     """, unsafe_allow_html=True)
#     # 컬럼을 사용하여 버튼을 가로로 배치
#     col_mode1, col_mode2, _ = st.columns([1, 1, 4])
    
#     with col_mode1:
#         if st.button("Test", width="stretch", key="btn_mode_ops"):
#             st.session_state["page"] = "Creative 자동 업로드"
#             st.rerun()
            
#     with col_mode2:
#         if st.button("Marketer", width="stretch", key="btn_mode_mkt"):
#             st.session_state["page"] = "Creative 자동 업로드 - 마케터"
#             st.rerun()

#     # 현재 모드 확인
#     current_page = st.session_state.get("page", "Creative 자동 업로드")
    
#     # 시각적 구분선
#     st.divider()

#     # 모드에 따른 렌더링
#     if current_page == "Creative 자동 업로드":
#         # OPS MODE
#         render_main_app("Test Mode", fb_ops, uni_ops, is_marketer=False)
#     else:
#         # MARKETER MODE
#         render_main_app("Marketer Mode", fb_marketer, uni_marketer, is_marketer=True)



# # Allow standalone execution
# if __name__ == "__main__":
#     run()

# ======================================================================
# PAGE ROUTING
# ======================================================================

def run():
    """
    Main entry point called by the parent app.
    """

    # ========================================================
    # [중요] 필수 초기화 함수들 (이게 없으면 에러 납니다!)
    # ========================================================
    init_state()                    # uploads, settings 초기화
    init_remote_state()             # remote_videos 초기화 (에러 해결!)
    fb_ops.init_fb_game_defaults()  # Facebook URL/AppID 기본값 채우기 (빈칸 해결!)

    # ------------------------------------------------------------
    # [UI] 모드 선택 버튼 및 스타일 설정
    # ------------------------------------------------------------
    
    # 페이지 상태 초기화
    if PAGE not in st.session_state:
        st.session_state[PAGE] = PAGE_OPS_TITLE

    # 상단에 모드 전환 버튼 배치
    st.markdown("#### 모드 선택")
    
    
    # 컬럼을 사용하여 버튼을 가로로 배치
    col_mode1, col_mode2, _ = st.columns([1, 1, 4])
    
    with col_mode1:
        if st.button("Test", width="stretch", key="btn_mode_ops"):
            st.session_state[PAGE] = PAGE_OPS_TITLE
            log_event("mode_select", mode="Test")
            st.rerun()

    with col_mode2:
        if st.button("Marketer", width="stretch", key="btn_mode_mkt"):
            st.session_state[PAGE] = PAGE_MARKETER_TITLE
            log_event("mode_select", mode="Marketer")
            st.rerun()

    # 현재 모드 확인
    current_page = st.session_state.get(PAGE, PAGE_OPS_TITLE)
    
    # 시각적 구분선
    st.divider()

    # 모드에 따른 렌더링
    if current_page == PAGE_OPS_TITLE:
        # OPS MODE
        render_main_app("Test Mode", fb_ops, uni_ops, is_marketer=False)
    else:
        # MARKETER MODE
        render_main_app("Marketer Mode", fb_marketer, uni_marketer, is_marketer=True)


# Allow standalone execution
if __name__ == "__main__":
    run()
