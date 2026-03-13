"""
Marketer-side Google Ads helpers for Creative Auto-Upload.

Features:
1. Campaign Selection
2. Ad Group listing (sorted by 7-day spend)
3. Category tabs (일반/로컬라이징/AI/인플루언서)
4. Per-category video selection + priority ordering
5. Preview & execute distribution plan
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List

import streamlit as st

from modules.upload_automation import google_ads as gads
from modules.upload_automation import devtools

logger = logging.getLogger(__name__)

# Category definitions
CATEGORIES = [
    ("normal", "일반"),
    ("localized", "로컬라이징"),
    ("AI", "AI"),
    ("influencer", "인플루언서"),
]

# ── Session state helpers ────────────────────────────────────────────

def _key(prefix: str, name: str) -> str:
    return f"{prefix}_{name}" if prefix else name


def _settings_key(prefix: str) -> str:
    return _key(prefix, "google_settings")


def get_google_settings(game: str, prefix: str = "") -> dict:
    sk = _settings_key(prefix)
    if sk not in st.session_state:
        st.session_state[sk] = {}
    return st.session_state[sk].get(game, {})


def _get_game_codename(game: str) -> str:
    """Get the game codename from secrets mapping."""
    mapping = st.secrets.get("google_ads", {}).get("game_mapping", {})
    return mapping.get(game, "").lower()


_RES_PATTERN = re.compile(r"(\d{3,4})x(\d{3,4})")
# Strip trailing YouTube ID suffix like " (abc123)" from display labels
_YT_SUFFIX = re.compile(r"\s*\([^)]+\)\s*$")


def _strip_yt_suffix(label: str) -> str:
    """Remove trailing ' (yt_id)' from a display label to get the raw filename."""
    return _YT_SUFFIX.sub("", label)


def _orientation_sort_key(label: str) -> int:
    """Sort key for orientation: 정방(square)=0, 가로(landscape)=1, 세로(portrait)=2."""
    m = _RES_PATTERN.search(label)
    if not m:
        return 3
    w, h = int(m.group(1)), int(m.group(2))
    if w == h:
        return 0  # 정방
    elif w > h:
        return 1  # 가로
    else:
        return 2  # 세로


def _find_orientation_variants(selected_label: str, all_options: List[str]) -> List[str]:
    """Given a selected video label, find other orientation variants from all_options.

    Compares only the filename part (stripping YouTube ID suffix) so that
    different uploads of the same creative match correctly.
    Returns list sorted by: 정방 → 가로 → 세로, NOT including the original.
    """
    name = _strip_yt_suffix(selected_label)
    m = _RES_PATTERN.search(name)
    if not m:
        return []
    base = name[:m.start()] + "{RES}" + name[m.end():]
    variants = []
    for opt in all_options:
        if opt == selected_label:
            continue
        opt_name = _strip_yt_suffix(opt)
        m2 = _RES_PATTERN.search(opt_name)
        if not m2:
            continue
        opt_base = opt_name[:m2.start()] + "{RES}" + opt_name[m2.end():]
        if opt_base == base:
            variants.append(opt)
    variants.sort(key=_orientation_sort_key)
    return variants


# ── Settings Panel ───────────────────────────────────────────────────

def render_google_settings_panel(
    container,
    game: str,
    idx: int,
    is_marketer: bool = True,
    prefix: str = "",
    uploaded_files: list = None,
) -> None:
    """
    Render Google Ads settings panel for marketer mode.
    Shows campaign selection, ad group preview, and category tabs.
    """
    kp = f"{prefix}_" if prefix else ""
    sk = _settings_key(prefix)
    if sk not in st.session_state:
        st.session_state[sk] = {}

    with container:
        st.markdown("### Google Ads Settings")

        # ── 1. Campaign Selection ────────────────────────────
        st.markdown("**캠페인 선택**")

        cache_key = f"{kp}gads_campaigns_{game}"
        if cache_key not in st.session_state:
            try:
                st.session_state[cache_key] = gads.list_campaigns(game=game)
            except Exception as e:
                logger.error(f"Google Ads 캠페인 로드 실패: {gads._extract_google_ads_error(e)}")
                st.error(f"Google Ads 연결 실패: {str(e)[:200]}")
                st.session_state[cache_key] = []

        campaigns = st.session_state[cache_key]
        if not campaigns:
            st.warning("사용 가능한 캠페인이 없습니다.")
            return

        # Search / paste filter
        search_key = f"{kp}gads_campaign_search_{game}_{idx}"
        search_query = st.text_input(
            "캠페인 검색 (이름 붙여넣기 또는 키워드 입력)",
            value="",
            key=search_key,
            placeholder="캠페인 이름 검색...",
        )

        filtered_campaigns = campaigns
        if search_query.strip():
            q = search_query.strip().lower()
            filtered_campaigns = [c for c in campaigns if q in c["name"].lower()]

        if not filtered_campaigns:
            st.warning(f"'{search_query}'에 해당하는 캠페인이 없습니다.")
            return

        campaign_labels = [f"{c['name']} ({c['status']})" for c in filtered_campaigns]
        prev_idx = 0
        prev_settings = st.session_state[sk].get(game, {})
        if prev_settings.get("campaign_id"):
            for ci, c in enumerate(filtered_campaigns):
                if c["id"] == prev_settings["campaign_id"]:
                    prev_idx = ci
                    break

        sel_campaign_idx = st.selectbox(
            "Campaign",
            range(len(campaign_labels)),
            format_func=lambda i: campaign_labels[i],
            index=prev_idx,
            key=f"{kp}gads_campaign_{game}_{idx}",
        )
        selected_campaign = filtered_campaigns[sel_campaign_idx]

        # ── 2. Ad Groups (sorted by spend) ───────────────────
        st.markdown("**광고그룹 (7일 비용 소진 순)**")

        ag_cache_key = f"{kp}gads_adgroups_{selected_campaign['id']}"
        load_ag = st.button(
            "광고그룹 불러오기",
            key=f"{kp}gads_load_ag_{game}_{idx}",
        )
        if load_ag:
            try:
                with st.spinner("광고그룹 로딩 중..."):
                    ad_groups = gads.list_ad_groups_with_spend(selected_campaign["id"])
                    st.session_state[ag_cache_key] = ad_groups
            except Exception as e:
                st.error(f"광고그룹 로딩 실패: {e}")

        ad_groups = st.session_state.get(ag_cache_key, [])
        if ad_groups:
            _render_ad_groups_table(ad_groups)

            # ── Clone option ──
            from datetime import datetime as _dt
            top_ag = ad_groups[0]
            clone_cb_key = f"{kp}gads_clone_enabled_{game}_{idx}"
            clone_enabled = st.checkbox(
                "영상 남을시 애드그룹 생성하기",
                value=st.session_state.get(clone_cb_key, False),
                key=clone_cb_key,
            )
            if clone_enabled:
                default_name = f"{top_ag['name'].rsplit('_', 1)[0]}_{_dt.now().strftime('%y%m%d')}"
                clone_name_key = f"{kp}gads_clone_name_{game}_{idx}"
                clone_name = st.text_input(
                    "생성될 광고그룹 이름",
                    value=st.session_state.get(clone_name_key, default_name),
                    key=clone_name_key,
                )
                st.caption(
                    f"원본: **{top_ag['name']}** (7일 비용 1위) → "
                    f"텍스트/이미지/플레이어블 복사, 영상만 미배치분으로 교체"
                )
        else:
            st.info("'광고그룹 불러오기' 버튼을 클릭하세요.")

        # ── 3. Category Tabs ──────────────────────────────────
        st.markdown("---")
        _render_category_tabs(game, idx, kp, sk, selected_campaign, ad_groups, prefix)

        st.markdown("---")


def _render_ad_groups_table(ad_groups: List[Dict]) -> None:
    """Render the ad groups HTML table (compact display)."""
    rows_html = ""
    for ag in ad_groups:
        low = ag.get("low_count", 0)
        low_badge = f'<span style="color:#ff4d4d;font-weight:bold">{low}</span>' if low > 0 else "0"
        rows_html += f"""<tr>
            <td style="padding:4px 8px;font-size:0.8rem">{ag['name']}</td>
            <td style="padding:4px 8px;text-align:right">${ag['spend']:.0f}</td>
            <td style="padding:4px 8px;text-align:center">{low_badge}</td>
        </tr>"""
    st.markdown(f"""
    <div style="max-height:300px;overflow-y:auto;border:1px solid rgba(255,255,255,0.1);border-radius:8px">
    <table style="width:100%;border-collapse:collapse;font-size:0.85rem">
        <thead><tr style="border-bottom:1px solid rgba(255,255,255,0.2)">
            <th style="padding:6px 8px;text-align:left">광고그룹</th>
            <th style="padding:6px 8px;text-align:right">비용(7일)</th>
            <th style="padding:6px 8px;text-align:center">저실적</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
    </table></div>
    """, unsafe_allow_html=True)


def _render_category_tabs(
    game: str, idx: int, kp: str, sk: str,
    campaign: Dict, ad_groups: List[Dict], prefix: str,
) -> None:
    """Render the 4 category tabs with per-category video/playable selection."""
    st.markdown("**카테고리별 소재 배치**")

    # ── Fetch GA library assets (cached per game) ──
    lib_video_key = f"{kp}gads_lib_videos_{game}"
    lib_playable_key = f"{kp}gads_lib_playables_{game}"

    load_lib = st.button(
        "GA 라이브러리 불러오기",
        key=f"{kp}gads_load_lib_{game}_{idx}",
    )
    if load_lib:
        try:
            with st.spinner("GA 에셋 라이브러리 로딩 중..."):
                codename = _get_game_codename(game)
                campaign_id = campaign.get("id", "")

                # Fetch playables from asset library (works fine)
                all_playables = gads.list_playable_assets(game_codename=None)
                if codename:
                    lib_playables = [a for a in all_playables if codename in a["name"].lower()]
                else:
                    lib_playables = all_playables

                # Fetch videos: try standalone asset library first
                lib_videos = gads.list_video_assets(game_codename=codename)

                # If standalone query returns 0, try campaign-linked assets
                if len(lib_videos) == 0 and campaign_id:
                    lib_videos = gads.list_campaign_video_assets(
                        campaign_id=str(campaign_id)
                    )

                st.session_state[lib_video_key] = lib_videos
                st.session_state[lib_playable_key] = lib_playables
                st.success(
                    f"영상 {len(lib_videos)}개, 플레이어블 {len(lib_playables)}개 로드 완료"
                )
        except Exception as e:
            st.error(f"GA 라이브러리 로딩 실패: {e}")

    lib_videos: List[Dict] = st.session_state.get(lib_video_key, [])
    lib_playables: List[Dict] = st.session_state.get(lib_playable_key, [])

    # Also get locally uploaded files (from Drive/local import)
    _rv_key = _key(prefix, "remote_videos")
    remote_videos = st.session_state.get(_rv_key, {}).get(game, [])
    local_file_names = []
    for v in remote_videos:
        name = v["name"] if isinstance(v, dict) else getattr(v, "name", str(v))
        local_file_names.append(name)

    # Get Step 1 uploaded asset resource names
    assets_key = f"{kp}gads_uploaded_assets_{game}"
    uploaded_assets = st.session_state.get(assets_key, {})

    # Summary counts
    total_lib = len(lib_videos) + len(lib_playables)
    total_local = len(local_file_names)
    if total_lib > 0 or total_local > 0:
        parts = []
        if total_lib > 0:
            parts.append(f"라이브러리: 영상 {len(lib_videos)}개, 플레이어블 {len(lib_playables)}개")
        if total_local > 0:
            parts.append(f"신규 업로드: {total_local}개")
        st.caption(" | ".join(parts))
    else:
        st.info("'GA 라이브러리 불러오기'를 클릭하거나, 왼쪽에서 파일을 가져오세요.")

    # Full video name display in multiselect
    st.markdown("""<style>
    [data-baseweb="tag"] span { max-width: none !important; }
    </style>""", unsafe_allow_html=True)

    # ── Categorize GA library assets ──
    # AI 영상은 일반 탭에도 표시
    cat_lib_videos: Dict[str, List[Dict]] = {cat_id: [] for cat_id, _ in CATEGORIES}
    for asset in lib_videos:
        cat = asset.get("category", gads._auto_detect_category(asset["name"]))
        cat_lib_videos[cat].append(asset)
        if cat == "AI":
            cat_lib_videos["normal"].append(asset)

    cat_lib_playables: Dict[str, List[Dict]] = {cat_id: [] for cat_id, _ in CATEGORIES}
    for asset in lib_playables:
        cat = asset.get("category", gads._auto_detect_category(asset["name"]))
        cat_lib_playables[cat].append(asset)

    # ── Categorize local files ──
    video_exts = {".mp4", ".mov", ".mpeg4"}
    playable_exts = {".html", ".zip"}

    def _get_ext(name: str) -> str:
        return "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""

    cat_local_videos: Dict[str, List[str]] = {cat_id: [] for cat_id, _ in CATEGORIES}
    cat_local_playables: Dict[str, List[str]] = {cat_id: [] for cat_id, _ in CATEGORIES}
    for fname in local_file_names:
        cat = gads._auto_detect_category(fname)
        ext = _get_ext(fname)
        if ext in video_exts:
            cat_local_videos[cat].append(fname)
            if cat == "AI":
                cat_local_videos["normal"].append(fname)
        elif ext in playable_exts:
            cat_local_playables[cat].append(fname)

    # ── Create tabs ──
    tab_labels = [label for _, label in CATEGORIES]
    tabs = st.tabs(tab_labels)
    category_selections: Dict[str, Dict] = {}

    for tab_idx, (cat_id, cat_label) in enumerate(CATEGORIES):
        with tabs[tab_idx]:
            # Show filtered ad groups for this category
            if ad_groups:
                filtered_ags = gads.filter_ad_groups_by_category(ad_groups, cat_id)
                if filtered_ags:
                    st.caption(f"대상 광고그룹: {len(filtered_ags)}개")
                    ag_names = [ag["name"] for ag in filtered_ags[:5]]
                    st.markdown(
                        " / ".join(f"`{n}`" for n in ag_names)
                        + (f" ... +{len(filtered_ags) - 5}" if len(filtered_ags) > 5 else "")
                    )
                else:
                    st.caption("해당 카테고리에 맞는 광고그룹이 없습니다.")

            # ── GA Library videos ──
            lib_vids = cat_lib_videos.get(cat_id, [])

            # Build unique display labels for library videos
            # 우선순위: youtube_video_title (원본 파일명) > name > yt_id > resource_name
            lib_vid_options = []  # display labels
            lib_label_to_rn = {}  # display label → resource_name
            for v in lib_vids:
                name = v.get("name", "") or ""
                yt_title = v.get("youtube_video_title", "") or ""
                yt_id = v.get("youtube_video_id", "") or ""
                rn = v["resource_name"]
                # youtube_video_title에 원본 파일명이 들어있음
                display = yt_title or name
                if display and yt_id:
                    label = f"{display} ({yt_id})"
                elif display:
                    label = display
                elif yt_id:
                    label = yt_id
                else:
                    label = rn.split("/")[-1]  # last part of resource_name
                # Ensure uniqueness
                if label in lib_label_to_rn:
                    label = f"{label} [{rn.split('/')[-1]}]"
                lib_vid_options.append(label)
                lib_label_to_rn[label] = rn

            # ── Local (new) videos ──
            local_vids = cat_local_videos.get(cat_id, [])

            # Combined: library labels + local new files
            all_vid_options = lib_vid_options + [
                f"[신규] {n}" for n in local_vids if n not in lib_label_to_rn
            ]

            selected_videos = []
            selected_video_rns = {}  # display_label → resource_name

            if all_vid_options:
                st.markdown(f"**영상 ({len(all_vid_options)}개)**")
                ms_key = f"{kp}gads_cat_videos_{cat_id}_{game}_{idx}"
                auto_pending_key = f"{ms_key}_auto_orient_pending"

                # Apply pending auto-select BEFORE widget renders
                if st.session_state.pop(auto_pending_key, False):
                    current = st.session_state.get(ms_key, [])
                    new_selections = list(current)
                    for label in current:
                        for variant in _find_orientation_variants(label, all_vid_options):
                            if variant not in new_selections:
                                new_selections.append(variant)
                    new_selections.sort(key=_orientation_sort_key)
                    st.session_state[ms_key] = new_selections

                selected_raw = st.multiselect(
                    "배치할 영상 선택 (선택 순서 = 우선순위)",
                    all_vid_options,
                    key=ms_key,
                    label_visibility="collapsed",
                )

                # Auto-select orientation variants button
                auto_key = f"{kp}gads_auto_orient_{cat_id}_{game}_{idx}"
                if selected_raw and st.button(
                    "방향별 자동선택 (정방→가로→세로)",
                    key=auto_key,
                ):
                    st.session_state[auto_pending_key] = True
                    st.rerun()

                for sr in selected_raw:
                    selected_videos.append(sr)
                    if sr in lib_label_to_rn:
                        selected_video_rns[sr] = lib_label_to_rn[sr]
            else:
                st.caption("해당 카테고리의 영상이 없습니다.")

            # ── GA Library playables ──
            lib_plays = cat_lib_playables.get(cat_id, [])
            lib_play_names = [p["name"] for p in lib_plays]
            lib_play_rn_map = {p["name"]: p["resource_name"] for p in lib_plays}

            # ── Local (new) playables ──
            local_plays = cat_local_playables.get(cat_id, [])

            all_play_names = lib_play_names + [n for n in local_plays if n not in lib_play_names]

            selected_playables = []
            selected_playable_rns = {}

            if all_play_names:
                play_options = []
                for pn in all_play_names:
                    if pn in lib_play_rn_map:
                        play_options.append(pn)
                    else:
                        play_options.append(f"[신규] {pn}")

                st.markdown(f"**플레이어블 ({len(all_play_names)}개)**")
                selected_play_raw = st.multiselect(
                    "배치할 플레이어블 선택",
                    play_options,
                    default=[],
                    key=f"{kp}gads_cat_playables_{cat_id}_{game}_{idx}",
                    label_visibility="collapsed",
                )
                for sr in selected_play_raw:
                    actual_name = sr.replace("[신규] ", "")
                    selected_playables.append(actual_name)
                    if actual_name in lib_play_rn_map:
                        selected_playable_rns[actual_name] = lib_play_rn_map[actual_name]

            # Show status for new files
            new_in_selection = [v for v in selected_videos if v not in lib_label_to_rn]
            if new_in_selection:
                uploaded_count = sum(1 for v in new_in_selection if v.replace("[신규] ", "") in uploaded_assets)
                not_uploaded_count = len(new_in_selection) - uploaded_count
                if not_uploaded_count > 0:
                    st.warning(f"신규 영상 {not_uploaded_count}개 — 먼저 '에셋 업로드' 실행 필요")

            category_selections[cat_id] = {
                "selected_videos": selected_videos,
                "selected_playables": selected_playables,
                "video_rns": {
                    **selected_video_rns,
                    **{n: uploaded_assets.get(n.replace("[신규] ", ""), "") for n in selected_videos if n.replace("[신규] ", "") in uploaded_assets},
                },
                "playable_rns": {**selected_playable_rns, **{n: uploaded_assets[n] for n in selected_playables if n in uploaded_assets}},
            }

    # Clone option state
    clone_cb_key = f"{kp}gads_clone_enabled_{game}_{idx}"
    clone_name_key = f"{kp}gads_clone_name_{game}_{idx}"
    clone_settings = {}
    if st.session_state.get(clone_cb_key, False) and ad_groups:
        clone_settings = {
            "enabled": True,
            "source_ad_group": ad_groups[0],
            "new_name": st.session_state.get(clone_name_key, ""),
        }

    # Save all settings to session state
    st.session_state[sk][game] = {
        "campaign_id": campaign["id"],
        "campaign_name": campaign["name"],
        "category_selections": category_selections,
        "clone": clone_settings,
    }


# ── Asset Upload (Step 1) ────────────────────────────────────────────

def upload_assets_to_library(
    game: str,
    uploaded_files: list,
    prefix: str = "",
    on_progress=None,
) -> Dict:
    """
    Upload all files to Google Ads asset library (Step 1).
    Stores asset resource names in session state for later distribution.
    Returns {success: int, failed: int, errors: [], uploaded_assets: {name: rn}}.
    """
    kp = f"{prefix}_" if prefix else ""

    results = {"success": 0, "failed": 0, "errors": [], "uploaded_assets": {}}

    video_exts = {".mp4", ".mov", ".mpeg4"}
    playable_exts = {".html", ".zip"}

    total = len(uploaded_files)
    for fi, f in enumerate(uploaded_files):
        name = f["name"] if isinstance(f, dict) else getattr(f, "name", "")
        if not name:
            continue

        ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""

        try:
            # Read file data
            if isinstance(f, dict) and "data" in f:
                data = f["data"]
            elif isinstance(f, dict) and "path" in f:
                with open(f["path"], "rb") as fh:
                    data = fh.read()
            else:
                data = f.read()

            if ext in video_exts:
                asset_rn = gads.upload_video_asset(data, name)
            elif ext in playable_exts:
                asset_rn = gads.upload_html5_asset(data, name)
            else:
                logger.warning(f"Unsupported file type: {name}")
                results["errors"].append(f"{name}: 지원하지 않는 파일 형식")
                results["failed"] += 1
                continue

            results["uploaded_assets"][name] = asset_rn
            results["success"] += 1

            if on_progress:
                on_progress(fi + 1, total, name, None)

        except Exception as e:
            logger.error(f"Failed to upload asset {name}: {e}")
            results["errors"].append(f"{name}: {e}")
            results["failed"] += 1
            if on_progress:
                on_progress(fi + 1, total, name, str(e))

    # Store in session state for distribution step
    assets_key = f"{kp}gads_uploaded_assets_{game}"
    existing = st.session_state.get(assets_key, {})
    existing.update(results["uploaded_assets"])
    st.session_state[assets_key] = existing

    return results


# ── Preview & Distribution (Step 2) ─────────────────────────────────

def preview_google_upload(game: str, prefix: str = "") -> Dict:
    """
    Generate a preview of the category-based distribution plan.
    Returns per-category breakdown.
    """
    settings = get_google_settings(game, prefix)
    if not settings or not settings.get("campaign_id"):
        return {"error": "Google Ads 설정을 먼저 완료해주세요."}

    campaign_id = settings["campaign_id"]
    category_selections = settings.get("category_selections", {})

    if not category_selections:
        return {"error": "카테고리별 소재를 선택해주세요."}

    # Check if any videos/playables are selected
    has_any = False
    for cat_id, sel in category_selections.items():
        if sel.get("selected_videos") or sel.get("selected_playables"):
            has_any = True
            break

    if not has_any:
        return {"error": "배치할 소재가 없습니다."}

    # Build preview per category
    kp = f"{prefix}_" if prefix else ""
    ag_cache_key = f"{kp}gads_adgroups_{campaign_id}"
    ad_groups = st.session_state.get(ag_cache_key, [])

    preview_categories = {}
    for cat_id, cat_label in CATEGORIES:
        sel = category_selections.get(cat_id, {})
        videos = sel.get("selected_videos", [])
        playables = sel.get("selected_playables", [])

        if not videos and not playables:
            continue

        filtered_ags = gads.filter_ad_groups_by_category(ad_groups, cat_id) if ad_groups else []

        preview_categories[cat_id] = {
            "label": cat_label,
            "videos": videos,
            "playables": playables,
            "ad_group_count": len(filtered_ags),
            "ad_group_names": [ag["name"] for ag in filtered_ags],
        }

    return {
        "type": "category_based",
        "campaign_id": campaign_id,
        "campaign_name": settings.get("campaign_name", ""),
        "categories": preview_categories,
    }


def distribute_by_category(
    game: str,
    prefix: str = "",
    on_progress=None,
) -> Dict:
    """
    Execute category-based distribution (Step 2).
    Uses already-uploaded asset resource names from session state.
    Returns {success: int, failed: int, errors: [], details: [...]}.
    """
    settings = get_google_settings(game, prefix)
    if not settings or not settings.get("campaign_id"):
        return {"success": False, "error": "Google Ads 설정을 먼저 완료해주세요."}

    campaign_id = settings["campaign_id"]
    category_selections = settings.get("category_selections", {})
    kp = f"{prefix}_" if prefix else ""

    # Get ad groups
    ag_cache_key = f"{kp}gads_adgroups_{campaign_id}"
    ad_groups = st.session_state.get(ag_cache_key, [])
    if not ad_groups:
        return {"success": False, "error": "먼저 광고그룹을 불러와주세요."}

    # Check if any selections exist
    has_any = False
    for sel in category_selections.values():
        if sel.get("selected_videos") or sel.get("selected_playables"):
            has_any = True
            break
    if not has_any:
        return {"success": False, "error": "배치할 소재를 선택해주세요."}

    total_success = 0
    total_failed = 0
    all_errors = []
    details = []
    all_unplaced_rns = []  # collect unplaced video resource names across categories

    # Debug: log category_selections state
    logger.info(f"[distribute] category_selections keys: {list(category_selections.keys())}")
    for _dbg_k, _dbg_v in category_selections.items():
        logger.info(
            f"[distribute] cat={_dbg_k}: videos={_dbg_v.get('selected_videos', [])}, "
            f"playables={_dbg_v.get('selected_playables', [])}, "
            f"video_rns_keys={list(_dbg_v.get('video_rns', {}).keys())}"
        )
    logger.info(f"[distribute] ad_groups count={len(ad_groups)}, names={[ag['name'] for ag in ad_groups[:5]]}")

    for cat_id, cat_label in CATEGORIES:
        sel = category_selections.get(cat_id, {})
        selected_videos = sel.get("selected_videos", [])
        selected_playables = sel.get("selected_playables", [])
        video_rns = sel.get("video_rns", {})      # name → resource_name
        playable_rns = sel.get("playable_rns", {})  # name → resource_name

        logger.info(
            f"[distribute] Processing cat={cat_id}: "
            f"videos={len(selected_videos)}, playables={len(selected_playables)}"
        )

        if not selected_videos and not selected_playables:
            continue

        filtered_ags = gads.filter_ad_groups_by_category(ad_groups, cat_id)
        logger.info(f"[distribute] cat={cat_id}: filtered_ags={len(filtered_ags)}")
        if not filtered_ags:
            all_errors.append(f"[{cat_label}] 해당 카테고리에 맞는 광고그룹이 없습니다.")
            continue

        # ── Video distribution for this category ──
        if selected_videos:
            # Get asset resource names in selection order (= priority)
            ordered_asset_rns = []
            for vname in selected_videos:
                rn = video_rns.get(vname)
                if rn:
                    ordered_asset_rns.append(rn)
                else:
                    all_errors.append(f"[{cat_label}] {vname}: resource name을 찾을 수 없습니다.")

            if ordered_asset_rns:
                # Distribute: replace LOW-performing videos
                plan = gads.distribute_videos(
                    campaign_id,
                    ordered_asset_rns,
                    exception_map={},  # no exception routing — already categorized
                    ad_groups=filtered_ags,
                )
                result = gads.execute_distribution(campaign_id, plan)
                total_success += result["success"]
                total_failed += result["failed"]
                if result["errors"]:
                    all_errors.extend(f"[{cat_label}] {e}" for e in result["errors"])

                unplaced_rns = plan.get("unplaced", [])
                all_unplaced_rns.extend(unplaced_rns)
                details.append({
                    "category": cat_label,
                    "type": "video",
                    "placed": len(ordered_asset_rns) - len(unplaced_rns),
                    "unplaced": len(unplaced_rns),
                    "ad_groups_modified": result["success"],
                })

        # ── Playable distribution for this category ──
        if selected_playables:
            for pname in selected_playables:
                rn = playable_rns.get(pname)
                if not rn:
                    all_errors.append(f"[{cat_label}] {pname}: resource name을 찾을 수 없습니다.")
                    continue

                p_success = 0
                p_failed = 0
                for ag in filtered_ags:
                    try:
                        app_ad = gads.get_app_ad_resource(campaign_id, ag["id"])
                        if not app_ad:
                            all_errors.append(f"[{cat_label}] {ag['name']}: AppAd 없음")
                            p_failed += 1
                            continue
                        gads.add_playable_to_app_ad(
                            app_ad["ad_resource_name"],
                            app_ad["html5_assets"],
                            rn,
                        )
                        p_success += 1
                    except Exception as e:
                        all_errors.append(f"[{cat_label}] {ag['name']}: {e}")
                        p_failed += 1

                total_success += p_success
                total_failed += p_failed
                details.append({
                    "category": cat_label,
                    "type": "playable",
                    "name": pname,
                    "ad_groups_success": p_success,
                    "ad_groups_failed": p_failed,
                })

    if total_success == 0 and total_failed == 0 and not all_errors:
        error_msg = "배치 대상이 없습니다. 광고그룹에 매칭되는 카테고리가 없거나 선택한 소재의 resource name을 확인해주세요."
    elif all_errors and total_success == 0:
        error_msg = "; ".join(all_errors[:3])
    else:
        error_msg = None

    # Auto-clone: if enabled and there are unplaced videos
    clone_settings = settings.get("clone", {})
    clone_result = None
    if all_unplaced_rns and clone_settings.get("enabled"):
        source_ag = clone_settings.get("source_ad_group")
        clone_name = clone_settings.get("new_name", "")
        if source_ag and clone_name:
            try:
                clone_result = gads.clone_ad_group(
                    campaign_id=campaign_id,
                    source_ad_group_id=source_ag["id"],
                    new_name=clone_name,
                    new_video_assets=all_unplaced_rns,
                    copy_playables=True,
                )
                if clone_result.get("success"):
                    details.append({
                        "type": "clone",
                        "name": clone_name,
                        "video_count": len(all_unplaced_rns),
                        "source": source_ag["name"],
                    })
                else:
                    all_errors.append(f"[복제] {clone_result.get('error', '알 수 없는 오류')}")
            except Exception as e:
                all_errors.append(f"[복제] {e}")

    return {
        "success": total_failed == 0 and total_success > 0,
        "total_success": total_success,
        "total_failed": total_failed,
        "errors": all_errors,
        "details": details,
        "error": error_msg,
        "unplaced_video_rns": all_unplaced_rns,
        "clone_result": clone_result,
    }
