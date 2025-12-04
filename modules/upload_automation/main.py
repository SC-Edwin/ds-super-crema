"""
Creative Upload Module for Super Crema
Integrates the existing Creative 자동 업로드 functionality
"""
import streamlit as st
import sys
import os
from pathlib import Path

def run():
    """Main entry point for creative upload module"""
    
    # Import all the necessary modules from the same package
    try:
        # Import from current package (upload_automation)
        from . import facebook_ads as fb_ops
        from . import unity_ads as uni_ops
        from . import fb as fb_marketer
        from . import uni as uni_marketer
        from .drive_import import import_drive_folder_videos_parallel as import_drive_folder_videos
        
    except ImportError as e:
        st.error(f"Failed to import required modules: {e}")
        st.info("Please ensure all required files are in the upload_automation folder.")
        import traceback
        st.code(traceback.format_exc())
        return
    
    # Initialize states
    init_state()
    init_remote_state()
    fb_ops.init_fb_game_defaults()
    
    # Render the main UI
    st.markdown("### 🎮 Creative 자동 업로드")
    st.caption("게임별 크리에이티브를 다운받고, 설정에 따라 자동으로 업로드합니다.")
    
    # Page selector in sidebar (replaces original sidebar)
    with st.expander("📋 페이지 선택", expanded=False):
        if "page" not in st.session_state:
            st.session_state["page"] = "Creative 자동 업로드"

        col1, col2 = st.columns(2)
        with col1:
            main_clicked = st.button("테스트", key="page_main_btn", use_container_width=True)
        with col2:
            marketer_clicked = st.button("마케터", key="page_marketer_btn", use_container_width=True)

        if main_clicked:
            st.session_state["page"] = "Creative 자동 업로드"
        if marketer_clicked:
            st.session_state["page"] = "Creative 자동 업로드 - 마케터"

        page = st.session_state["page"]
        st.caption(f"현재 페이지: **{page}**")
    
    # Unity Diagnostics - Call the function from unity_ads module
    with st.expander("🔧 Unity Diagnostics", expanded=False):
        if st.button("🔍 Check All Campaigns Auto-Start Status"):
            # Call the function from the imported unity_ads module
            uni_ops.check_all_games_auto_start()
    
    # Render main content based on selected page
    if page == "Creative 자동 업로드":
        render_main_app("🎮 Creative 자동 업로드", fb_ops, uni_ops, is_marketer=False)
    else:
        render_main_app("🎮 Creative 자동 업로드 - 마케터", fb_marketer, uni_marketer, is_marketer=True)
    
    # Footer
    st.markdown("---")
    st.caption("Creative 자동 업로드 모듈 v2.0")


# ===== Helper Functions (from original streamlit_app.py) =====

def init_state():
    """Set up st.session_state containers for uploads and settings if missing."""
    if "uploads" not in st.session_state:
        st.session_state.uploads = {}
    if "settings" not in st.session_state:
        st.session_state.settings = {}


def init_remote_state():
    """Set up st.session_state container for Drive-imported videos per game if missing."""
    if "remote_videos" not in st.session_state:
        st.session_state.remote_videos = {}


def game_tabs(n: int):
    """Return the fixed list of 10 game names (tabs)."""
    return [
        "XP HERO", "Dino Universe", "Snake Clash", "Pizza Ready", "Cafe Life",
        "Suzy's Restaurant", "Office Life", "Lumber Chopper", "Burger Please", "Prison Life",
    ]


def validate_count(files):
    """Check there is at least one .mp4/.mpeg4 file and no invalid types."""
    import pathlib
    
    if not files:
        return False, "Please upload at least one video (.mp4 or .mpeg4)."

    allowed = {".mp4", ".mpeg4"}
    bad = []
    for u in files:
        name = getattr(u, "name", None) or (u.get("name") if isinstance(u, dict) else None)
        if not name:
            continue
        if pathlib.Path(name).suffix.lower() not in allowed:
            bad.append(name)

    if bad:
        return (
            False,
            f"Only video files are allowed (.mp4/.mpeg4). "
            f"Remove non-video files: {', '.join(bad[:5])}{'…' if len(bad) > 5 else ''}",
        )
    return True, f"{len(files)} video(s) ready."


def _fname_any(u):
    """Return a filename for either a Streamlit UploadedFile or a {'name','path'} dict."""
    return getattr(u, "name", None) or (u.get("name") if isinstance(u, dict) else "")


def _dedupe_by_name(files):
    """Keep first occurrence of each filename (case-insensitive)."""
    seen = set()
    out = []
    for u in files or []:
        n = (_fname_any(u) or "").strip().lower()
        if n and n not in seen:
            seen.add(n)
            out.append(u)
    return out


def _run_drive_import(folder_url_or_id: str, max_workers: int, on_progress=None):
    """Wrapper for Drive import"""
    from modules.upload_automation.drive_import import import_drive_folder_videos_parallel as import_drive_folder_videos
    return import_drive_folder_videos(folder_url_or_id, max_workers=max_workers, on_progress=on_progress)


def render_main_app(title: str, fb_module, unity_module, is_marketer: bool = False):
    """Render the full Creative 자동 업로드 UI"""
    import time
    import pathlib
    
    st.markdown(f"#### {title}")
    
    NUM_GAMES = 10
    GAMES = game_tabs(NUM_GAMES)
    accepted_types = ["mp4", "mpeg4"]

    _tabs = st.tabs(GAMES)

    for i, game in enumerate(GAMES):
        with _tabs[i]:
            left_col, right_col = st.columns([2, 1], gap="large")

            # LEFT COLUMN
            with left_col:
                left_card = st.container(border=True)
                with left_card:
                    st.subheader(game)

                    # Platform selector
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

                    # Google Drive import section
                    st.markdown("**구글 드라이브에서 Creative Videos를 가져옵니다**")
                    drv_input = st.text_input(
                        "Drive folder URL or ID",
                        key=f"drive_folder_{i}",
                        placeholder="https://drive.google.com/drive/folders/<FOLDER_ID>",
                    )

                    with st.expander("Advanced import options", expanded=False):
                        workers = st.number_input(
                            "Parallel workers",
                            min_value=1,
                            max_value=16,
                            value=8,
                            key=f"drive_workers_{i}",
                        )

                    if st.button("드라이브에서 Creative 가져오기", key=f"drive_import_{i}"):
                        try:
                            overall = st.progress(0, text="0/0 • waiting…")
                            log_box = st.empty()
                            lines = []
                            last_flush = [0.0]

                            def _on_progress(done: int, total: int, name: str, err: str | None):
                                pct = int((done / max(total, 1)) * 100)
                                label = f"{done}/{total}"
                                if name:
                                    label += f" • {name}"
                                if err:
                                    lines.append(f"❌ {name}  —  {err}")
                                else:
                                    lines.append(f"✅ {name}")

                                now = time.time()
                                if (now - last_flush[0]) > 0.3 or done == total:
                                    overall.progress(pct, text=label)
                                    log_box.write("\n".join(lines[-200:]))
                                    last_flush[0] = now

                            with st.status("Importing videos from Drive folder...", expanded=True) as status:
                                imported = _run_drive_import(
                                    drv_input,
                                    max_workers=int(workers),
                                    on_progress=_on_progress,
                                )
                                lst = st.session_state.remote_videos.get(game, [])
                                lst.extend(imported)
                                st.session_state.remote_videos[game] = lst

                                status.update(
                                    label=f"Drive import complete: {len(imported)} file(s)",
                                    state="complete",
                                )

                            st.success(f"Imported {len(imported)} video(s) from the folder.")
                        except Exception as e:
                            st.exception(e)
                            st.error("Could not import from this folder.")

                    # Show downloaded videos
                    remote_list = st.session_state.remote_videos.get(game, [])
                    st.caption("다운로드된 Creatives:")
                    if remote_list:
                        for it in remote_list[:50]:
                            st.write("•", it["name"])
                        if len(remote_list) > 50:
                            st.write(f"... 외 {len(remote_list) - 50}개")
                    else:
                        st.write("- (현재 저장된 URL/Drive 영상 없음)")

                    if st.button("URL/Drive 영상만 초기화", key=f"clearurl_{i}"):
                        if remote_list:
                            st.session_state.remote_videos[game] = []
                            st.info("Cleared URL/Drive videos for this game.")
                            st.rerun()

                    # Platform-specific buttons
                    if platform == "Facebook":
                        ok_msg_placeholder = st.empty()
                        btn_label = "Creative 업로드하기" if is_marketer else "Creative Test 업로드하기"
                        cont = st.button(btn_label, key=f"continue_{i}")
                        clr = st.button("전체 초기화", key=f"clear_{i}")
                    else:
                        unity_ok_placeholder = st.empty()
                        cont_unity_create = st.button("크리에이티브/팩 생성", key=f"unity_create_{i}")
                        cont_unity_apply = st.button("캠페인에 적용", key=f"unity_apply_{i}")
                        clr_unity = st.button("전체 초기화 (Unity용)", key=f"unity_clear_{i}")

            # RIGHT COLUMN: Settings
            if platform == "Facebook":
                with right_col:
                    fb_card = st.container(border=True)
                    fb_module.render_facebook_settings_panel(fb_card, game, i)
            elif platform == "Unity Ads":
                with right_col:
                    unity_card = st.container(border=True)
                    unity_module.render_unity_settings_panel(unity_card, game, i)

            # Handle button actions (Facebook)
            if platform == "Facebook":
                if cont:
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
                                ok_msg_placeholder.success(f"{msg} Uploaded to Meta successfully.")
                            else:
                                ok_msg_placeholder.error("Meta upload did not return an ad set ID.")
                        except Exception as e:
                            st.exception(e)

                if clr:
                    st.session_state.uploads.pop(game, None)
                    st.session_state.remote_videos.pop(game, None)
                    st.session_state.settings.pop(game, None)
                    ok_msg_placeholder.info("Cleared saved uploads and settings.")
                    st.rerun()

            # Handle button actions (Unity)
            if platform == "Unity Ads":
                unity_settings = unity_module.get_unity_settings(game)
                
                if "unity_created_packs" not in st.session_state:
                    st.session_state.unity_created_packs = {}

                if cont_unity_create:
                    remote_list = st.session_state.remote_videos.get(game, [])
                    ok, msg = validate_count(remote_list)
                    if not ok:
                        unity_ok_placeholder.error(msg)
                    else:
                        # === NEW: Check auto-start before proceeding ===
                        org_id = unity_settings.get("org_id") or unity_module.UNITY_ORG_ID_DEFAULT
                        title_id = unity_settings.get("title_id") or ""
                        campaign_id = unity_settings.get("campaign_id") or ""
                        
                        proceed = True
                        if all([org_id, title_id, campaign_id]):
                            auto_start_status = unity_module.check_campaign_auto_start(
                                org_id=org_id,
                                title_id=title_id,
                                campaign_id=campaign_id
                            )
                            
                            if not auto_start_status.get("auto_start_enabled"):
                                st.warning(
                                    "⚠️ **Auto-Start is DISABLED** for this campaign!\n\n"
                                    "Creative packs will be uploaded but will NOT automatically start delivery. "
                                    "You'll need to manually enable them in the Unity dashboard."
                                )
                                proceed = st.checkbox(
                                    "I understand and want to proceed", 
                                    key=f"autostart_confirm_{i}"
                                )
                        
                        # Fixed: This should be at the same level as the auto-start check
                        if proceed:
                            try:
                                summary = unity_module.upload_unity_creatives_to_campaign(
                                    game=game, 
                                    videos=remote_list, 
                                    settings=unity_settings
                                )
                                pack_ids = summary.get("creative_ids") or []
                                st.session_state.unity_created_packs[game] = list(pack_ids)
                                
                                if pack_ids:
                                    unity_ok_placeholder.success(f"Created {len(pack_ids)} Unity creative packs.")
                            except Exception as e:
                                st.exception(e)

                            if cont_unity_apply:
                                pack_ids = st.session_state.unity_created_packs.get(game) or []
                                if not pack_ids:
                                    unity_ok_placeholder.error("No creative packs to apply.")
                                else:
                                    try:
                                        result = unity_module.apply_unity_creative_packs_to_campaign(
                                            game=game, creative_pack_ids=pack_ids, settings=unity_settings
                                        )
                                        assigned = result.get("assigned_packs") or []
                                        if assigned:
                                            unity_ok_placeholder.success(f"Assigned {len(assigned)} packs to campaign.")
                                    except Exception as e:
                                        st.exception(e)

                if clr_unity:
                    st.session_state.uploads.pop(game, None)
                    st.session_state.remote_videos.pop(game, None)
                    st.session_state.settings.pop(game, None)
                    unity_module.get_unity_settings(game)
                    if "unity_created_packs" in st.session_state:
                        st.session_state.unity_created_packs.pop(game, None)
                    unity_ok_placeholder.info("Cleared all Unity settings.")
                    st.rerun()

    # Summary table
    st.subheader("업로드 완료된 게임")
    if st.session_state.uploads:
        data = {"게임": [], "업로드 파일": []}
        for g, files in st.session_state.uploads.items():
            data["게임"].append(g)
            data["업로드 파일"].append(len(files))
        st.dataframe(data, hide_index=True)
    else:
        st.info("No uploads saved yet.")