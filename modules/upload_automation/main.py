"""Streamlit app: bulk upload per-game videos from Drive and create Meta creative tests."""
from __future__ import annotations

import os
import sys
import pathlib
import logging
from typing import Dict, List
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st
from streamlit.components.v1 import html as components_html 

# --- FIX: ADD CURRENT DIRECTORY TO PATH ---
# This allows importing sibling files (drive_import, facebook_ads) 
# when running from a different root directory (e.g. via app.py)
import os
import sys

# 1. 현재 파일이 있는 폴더 (modules/upload_automation)
current_dir = os.path.dirname(os.path.abspath(__file__))

# 2. 프로젝트 최상위 루트 폴더 (ds-super-crema) - 두 단계 위로 올라감
root_dir = os.path.dirname(os.path.dirname(current_dir))

# 경로 추가 (중복 방지)
if current_dir not in sys.path:
    sys.path.append(current_dir)
if root_dir not in sys.path:
    sys.path.append(root_dir)

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

# --- IMPORTS ---
try:
    from drive_import import import_drive_folder_videos_parallel as import_drive_folder_videos
    _DRIVE_IMPORT_SUPPORTS_PROGRESS = True
except ImportError:
    # If parallel import isn't available or fails, fall back
    try:
        from drive_import import import_drive_folder_videos
        _DRIVE_IMPORT_SUPPORTS_PROGRESS = False
    except ImportError:
        # Stop execution if drive_import is completely missing
        st.error(f"Critical Error: Could not find 'drive_import.py' in {current_dir}")
        st.stop()

# 1. Game Manager (BigQuery Integration)
import game_manager

# 2. Operations Modules (Admin/Full Access)
import facebook_ads as fb_ops
import unity_ads as uni_ops

# 3. Marketer Modules (Simplified/Restricted)
try:
    import fb as fb_marketer 
    import uni as uni_marketer 
except ImportError as e:
    st.error(f"Module Import Error: {e}. Please ensure fb.py and unity_marketer.py are in {current_dir}")
    st.stop()


# ----- CONFIG & STATE --------------------------------------------------
try:
    MAX_UPLOAD_MB = int(st.get_option("server.maxUploadSize"))
except Exception:
    MAX_UPLOAD_MB = 200

def init_state():
    """Set up st.session_state containers."""
    if "uploads" not in st.session_state:
        st.session_state.uploads = {}
    if "settings" not in st.session_state:
        st.session_state.settings = {}

def init_remote_state():
    """Set up st.session_state container for Drive-imported videos."""
    if "remote_videos" not in st.session_state:
        st.session_state.remote_videos = {}

def validate_count(files: List) -> tuple[bool, str]:
    """Check there is at least one .mp4/.mpeg4 file."""
    if not files:
        return False, "Please upload at least one video (.mp4 or .mpeg4)."
    allowed = {".mp4", ".mpeg4"}
    bad = []
    for u in files:
        name = getattr(u, "name", None) or (u.get("name") if isinstance(u, dict) else None)
        if not name: continue
        if pathlib.Path(name).suffix.lower() not in allowed:
            bad.append(name)
    if bad:
        return False, f"Remove non-video files: {', '.join(bad[:5])}..."
    return True, f"{len(files)} video(s) ready."

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
        initial_sidebar_state="expanded",
    )
except Exception:
    pass # Ignore if page config was already set by parent app

init_state()
init_remote_state()
fb_ops.init_fb_game_defaults()



# ======================================================================
# MAIN RENDERER (Shared Logic)
# ======================================================================
def render_main_app(title: str, fb_module, unity_module, is_marketer: bool = False) -> None:

    """
    Renders the main UI. 
    Dynamically loads games from BigQuery via game_manager.
    """
    st.title(title)
    
    # ============ 여기에 추가! ============
    # 버튼 스타일 통일
    st.markdown("""
    <style>
    div[data-testid="stButton"] button,
    .stButton > button {
        width: 100% !important;
        max-width: 400px !important;
        height: auto !important;
        min-height: 50px !important;
        border-radius: 12px !important;
        padding: 14px 24px !important;
        
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%) !important;
        border: 2px solid #ff006e !important;
        
        box-shadow: 
            0 4px 15px rgba(0, 0, 0, 0.8),
            0 0 20px rgba(255, 0, 110, 0.4),
            inset 0 2px 8px rgba(255, 255, 255, 0.1) !important;
        
        transition: all 0.3s ease !important;
    }

    div[data-testid="stButton"] button p,
    .stButton > button p {
        font-size: 15px !important;
        font-weight: 700 !important;
        line-height: 1.4 !important;
        letter-spacing: 0.5px !important;
        white-space: nowrap !important;
        color: #ff006e !important;
        text-align: center !important;
        text-shadow: 
            0 0 10px rgba(255, 0, 110, 0.6),
            0 0 20px rgba(255, 0, 110, 0.3) !important;
        margin: 0 !important;
        padding: 0 !important;
    }

    div[data-testid="stButton"] button:hover,
    .stButton > button:hover {
        transform: translateY(-3px) scale(1.02) !important;
        background: linear-gradient(135deg, #2a1a3e 0%, #261e4e 50%, #1f4470 100%) !important;
        border-color: #ff4d8f !important;
        box-shadow: 
            0 8px 25px rgba(0, 0, 0, 0.9),
            0 0 35px rgba(255, 0, 110, 0.7),
            inset 0 3px 10px rgba(255, 0, 110, 0.2) !important;
    }

    div[data-testid="stButton"] button:hover p,
    .stButton > button:hover p {
        color: #ff77a0 !important;
        text-shadow: 
            0 0 15px rgba(255, 0, 110, 0.8),
            0 0 25px rgba(255, 0, 110, 0.4) !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # --- [MARKETER ONLY] Add New Game Sidebar Form ---
    if is_marketer:
        with st.sidebar:
            st.divider()
            with st.expander("➕ Add New Game", expanded=False):
                with st.form("add_game_form"):
                    st.caption("Add a new game configuration to BigQuery.")
                    new_game_name = st.text_input("Game Name (e.g. My New RPG)")
                    st.markdown("**Facebook Details**")
                    new_fb_act = st.text_input("Ad Account ID", placeholder="act_12345678")
                    new_fb_page = st.text_input("Page ID", placeholder="1234567890")
                    st.markdown("**Unity Details**")
                    new_unity_id = st.text_input("Unity Game ID (Optional)")
                    
                    if st.form_submit_button("Save Game"):
                        if not new_game_name or not new_fb_act:
                            st.error("Name and Ad Account are required.")
                        else:
                            try:
                                # Validation (Simple Auth Check)
                                fb_ops.init_fb_from_secrets()
                                from facebook_business.adobjects.adaccount import AdAccount
                                AdAccount(new_fb_act.strip()).api_get(fields=["name"])
                                
                                # Save to BigQuery
                                game_manager.save_new_game(
                                    new_game_name, new_fb_act, new_fb_page, new_unity_id
                                )
                                st.success(f"Saved **{new_game_name}**!")
                                import time
                                time.sleep(1)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Validation/Save Failed: {e}")

    # --- LOAD GAMES FROM DB ---
    GAMES = game_manager.get_all_game_names(include_custom=is_marketer)

    if not GAMES:
        st.error("No games found. Please check BigQuery connection or Add a New Game.")
        return

    _tabs = st.tabs(GAMES)

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
                    platform = st.radio(
                        "플랫폼 선택",
                        ["Facebook", "Unity Ads"],
                        index=0,
                        horizontal=True,
                        key=f"platform_{i}",
                    )

                    if platform == "Facebook":
                        st.markdown("### Facebook")
                    else:
                        st.markdown("### Unity Ads")

                    # --- Drive Import Section ---
                    st.markdown("**구글 드라이브에서 Creative Videos를 가져옵니다**")
                    drv_input = st.text_input(
                        "Drive folder URL or ID",
                        key=f"drive_folder_{i}",
                        placeholder="https://drive.google.com/drive/folders/..."
                    )

                    with st.expander("Advanced import options", expanded=False):
                        workers = st.number_input(
                            "Parallel workers", min_value=1, max_value=16, value=8, key=f"drive_workers_{i}"
                        )

                    # [수정 1] 드라이브 가져오기 버튼: 너비 꽉 채우기
                    if st.button("드라이브에서 Creative 가져오기", key=f"drive_import_{i}", use_container_width=True):
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
                                lst = st.session_state.remote_videos.get(game, [])
                                lst.extend(imported)
                                st.session_state.remote_videos[game] = lst
                                status.update(label=f"Done: {len(imported)} files", state="complete")
                                if isinstance(imported, dict) and imported.get("errors"):
                                    st.warning("\n".join(imported["errors"]))
                            st.success(f"Imported {len(imported)} videos.")
                        except Exception as e:
                            st.error(f"Import failed: {e}")

                    # --- Display List ---
                    remote_list = st.session_state.remote_videos.get(game, [])
                    st.caption("다운로드된 Creatives:")
                    if remote_list:
                        for it in remote_list[:20]: st.write("•", it["name"])
                        if len(remote_list) > 20: st.write(f"... and {len(remote_list)-20} more")
                    else:
                        st.write("- (None)")

                    # [수정 2] 초기화 버튼: 너비 꽉 채우기
                    if st.button("초기화 (Clear Videos)", key=f"clearurl_{i}", use_container_width=True):
                        st.session_state.remote_videos[game] = []
                        st.rerun()

                    # --- Action Buttons ---
                    if platform == "Facebook":
                        ok_msg_placeholder = st.empty()
                        btn_label = "Creative 업로드하기" if is_marketer else "Creative Test 업로드하기"
                        
                        # [수정 3] 업로드 및 전체 초기화 버튼: 너비 꽉 채우기
                        # 간격을 두어 시각적으로 분리
                        st.write("") 
                        cont = st.button(btn_label, key=f"continue_{i}", use_container_width=True)
                        clr = st.button("전체 초기화", key=f"clear_{i}", use_container_width=True)
                    else:
                        unity_ok_placeholder = st.empty()
                        # Unity 버튼들도 동일하게 적용
                        st.write("")
                        cont_unity_create = st.button("크리에이티브/팩 생성", key=f"unity_create_{i}", use_container_width=True)
                        cont_unity_apply = st.button("캠페인에 적용", key=f"unity_apply_{i}", use_container_width=True)
                        clr_unity = st.button("전체 초기화 (Unity)", key=f"unity_clear_{i}", use_container_width=True)

            # =========================
            # RIGHT COLUMN: Settings
            # =========================
            if platform == "Facebook":
                with right_col:
                    # FIX: Create the container object specifically
                    fb_card = st.container(border=True)
                    # Pass the CONTAINER object (fb_card), NOT the module (st)
                    fb_module.render_facebook_settings_panel(fb_card, game, i)

            elif platform == "Unity Ads":
                with right_col:
                    # FIX: Create the container object specifically
                    unity_card = st.container(border=True)
                    # Pass the CONTAINER object (unity_card), NOT the module (st)
                    unity_module.render_unity_settings_panel(unity_card, game, i)

            # =========================
            # EXECUTION LOGIC
            # =========================
            
            # --- FACEBOOK ACTIONS ---
            if platform == "Facebook" and cont:
                remote_list = st.session_state.remote_videos.get(game, [])
                ok, msg = validate_count(remote_list)
                if not ok:
                    ok_msg_placeholder.error(msg)
                else:
                    try:
                        st.session_state.uploads[game] = remote_list
                        settings = st.session_state.settings.get(game, {})
                        
                        plan = fb_module.upload_to_facebook(game, remote_list, settings)
                        
                        if isinstance(plan, dict) and plan.get("adset_id"):
                            ok_msg_placeholder.success("Uploaded successfully! Ad Set created.")
                        else:
                            ok_msg_placeholder.error("Upload failed or no Ad Set ID returned.")
                    except Exception as e:
                        import traceback
                        st.error("Upload Error")
                        st.code(traceback.format_exc())

            if platform == "Facebook" and clr:
                st.session_state.uploads.pop(game, None)
                st.session_state.remote_videos.pop(game, None)
                st.session_state.settings.pop(game, None)
                st.rerun()

            # --- UNITY ACTIONS ---
            if platform == "Unity Ads":
                unity_settings = unity_module.get_unity_settings(game)
                if "unity_created_packs" not in st.session_state:
                    st.session_state.unity_created_packs = {}

                # 1. Create Logic
                if "cont_unity_create" in locals() and cont_unity_create:
                    remote_list = st.session_state.remote_videos.get(game, [])
                    ok, msg = validate_count(remote_list)
                    if not ok:
                        unity_ok_placeholder.error(msg)
                    else:
                        try:
                            summary = unity_module.upload_unity_creatives_to_campaign(
                                game=game, videos=remote_list, settings=unity_settings
                            )
                            pack_ids = summary.get("creative_ids", [])
                            st.session_state.unity_created_packs[game] = pack_ids
                            
                            if pack_ids:
                                unity_ok_placeholder.success(f"Created {len(pack_ids)} Creative Packs.")
                            else:
                                unity_ok_placeholder.warning("No packs created.")
                            
                            if summary.get("errors"):
                                st.error("\n".join(summary["errors"]))
                        except Exception as e:
                            import traceback
                            st.code(traceback.format_exc())

                # 2. Apply Logic
                if "cont_unity_apply" in locals() and cont_unity_apply:
                    pack_ids = st.session_state.unity_created_packs.get(game, [])
                    if not pack_ids:
                        unity_ok_placeholder.error("No packs found. Create them first.")
                    else:
                        try:
                            res = unity_module.apply_unity_creative_packs_to_campaign(
                                game=game, creative_pack_ids=pack_ids, settings=unity_settings
                            )
                            assigned = res.get("assigned_packs", [])
                            if assigned:
                                unity_ok_placeholder.success(f"Assigned {len(assigned)} packs.")
                            else:
                                unity_ok_placeholder.warning("No packs assigned.")
                        except Exception as e:
                            import traceback
                            st.code(traceback.format_exc())
                
                if "clr_unity" in locals() and clr_unity:
                    st.session_state.unity_settings.pop(game, None)
                    st.session_state.remote_videos.pop(game, None)
                    st.rerun()

    # Summary
    st.subheader("Upload Summary")
    if st.session_state.uploads:
        data = [{"Game": k, "Files": len(v)} for k, v in st.session_state.uploads.items()]
        st.dataframe(data)


# ======================================================================
# PAGE ROUTING
# ======================================================================



def run():
    """
    Main entry point called by the parent app.
    """
    # ------------------------------------------------------------
    # [수정됨] 사이드바 대신 메인 화면 상단에 모드 선택 버튼 배치
    # ------------------------------------------------------------
    
    # 페이지 상태 초기화
    if "page" not in st.session_state:
        st.session_state["page"] = "Creative 자동 업로드"

    # 상단에 모드 전환 버튼 배치 (Tab 내부 상단에 위치하게 됨)
    st.markdown("#### 🛠️ 모드 선택")
    st.markdown("""
    <style>
    div[data-testid="stButton"] button,
    .stButton > button {
        width: 100% !important;
        max-width: 400px !important;
        height: auto !important;
        min-height: 50px !important;
        border-radius: 12px !important;
        padding: 14px 24px !important;
        
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%) !important;
        border: 2px solid #ff006e !important;
        
        box-shadow: 
            0 4px 15px rgba(0, 0, 0, 0.8),
            0 0 20px rgba(255, 0, 110, 0.4),
            inset 0 2px 8px rgba(255, 255, 255, 0.1) !important;
        
        transition: all 0.3s ease !important;
    }

    div[data-testid="stButton"] button p,
    .stButton > button p {
        font-size: 15px !important;
        font-weight: 700 !important;
        line-height: 1.4 !important;
        letter-spacing: 0.5px !important;
        white-space: nowrap !important;
        color: #ff006e !important;
        text-align: center !important;
        text-shadow: 
            0 0 10px rgba(255, 0, 110, 0.6),
            0 0 20px rgba(255, 0, 110, 0.3) !important;
        margin: 0 !important;
        padding: 0 !important;
    }

    div[data-testid="stButton"] button:hover,
    .stButton > button:hover {
        transform: translateY(-3px) scale(1.02) !important;
        background: linear-gradient(135deg, #2a1a3e 0%, #261e4e 50%, #1f4470 100%) !important;
        border-color: #ff4d8f !important;
        box-shadow: 
            0 8px 25px rgba(0, 0, 0, 0.9),
            0 0 35px rgba(255, 0, 110, 0.7),
            inset 0 3px 10px rgba(255, 0, 110, 0.2) !important;
    }

    div[data-testid="stButton"] button:hover p,
    .stButton > button:hover p {
        color: #ff77a0 !important;
        text-shadow: 
            0 0 15px rgba(255, 0, 110, 0.8),
            0 0 25px rgba(255, 0, 110, 0.4) !important;
    }
    </style>
    """, unsafe_allow_html=True)
    # 컬럼을 사용하여 버튼을 가로로 배치
    col_mode1, col_mode2, _ = st.columns([1, 1, 4])
    
    with col_mode1:
        if st.button("Test", use_container_width=True, key="btn_mode_ops"):
            st.session_state["page"] = "Creative 자동 업로드"
            st.rerun()
            
    with col_mode2:
        if st.button("Marketer", use_container_width=True, key="btn_mode_mkt"):
            st.session_state["page"] = "Creative 자동 업로드 - 마케터"
            st.rerun()

    # 현재 모드 확인
    current_page = st.session_state.get("page", "Creative 자동 업로드")
    
    # 시각적 구분선
    st.divider()

    # 모드에 따른 렌더링
    if current_page == "Creative 자동 업로드":
        # OPS MODE
        render_main_app("Test Mode", fb_ops, uni_ops, is_marketer=False)
    else:
        # MARKETER MODE
        render_main_app("Marketer Mode", fb_marketer, uni_marketer, is_marketer=True)


# Allow standalone execution
if __name__ == "__main__":
    run()