"""Unity Ads helpers for Creative 자동 업로드 Streamlit app."""

from __future__ import annotations

from typing import Dict, List, Any, Callable
from collections import deque
from datetime import datetime, timedelta, timezone
import functools
import logging
import pathlib
import re
import os
import json
import hashlib
import threading

import time
import requests
import streamlit as st
from modules.upload_automation.utils import devtools
from modules.upload_automation.network.dto import RequestExecutionContextDTO, RetryPolicyDTO
from modules.upload_automation.network.http_client import execute_request, HttpRequestError
from modules.upload_automation.network.retry_policies import (
    build_default_api_policy,
    build_upload_multipart_policy,
)
from modules.upload_automation.service.unity import (
    UNITY_ADVERTISE_API_BASE,
    build_unity_request,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Unity HTTP 호출 추적 (프로세스 단위, 스레드 안전)
# 공식 문서: https://services.docs.unity.com/docs/headers
#  - RateLimit-Policy 예: "20;w=1, 4000;w=1800" → w는 초 단위 창(1초, 1800초=30분), 앞 숫자는 해당 창당 허용 요청 수.
#  - 실제 한도는 엔드포인트·계약마다 다를 수 있어 응답 헤더를 봐야 함(고정 상수 없음).
# --------------------------------------------------------------------
_UNITY_HTTP_EVENTS: deque[tuple[float, str, str, int]] = deque(maxlen=12000)
_UNITY_HTTP_LOCK = threading.Lock()
_LAST_RATELIMIT_POLICY: str | None = None
_LAST_UNITY_RATELIMIT: str | None = None
_UNITY_GATE_TIMESTAMPS: deque[float] = deque(maxlen=20000)
_UNITY_GATE_LOCK = threading.Lock()
_UNITY_PROGRESS_HOOK: Callable[[str], None] | None = None
_UNITY_PROGRESS_HOOK_LOCK = threading.Lock()


def _record_unity_http_call(method: str, path: str, resp: requests.Response) -> None:
    """완료된 HTTP 응답 1건을 기록한다(429 포함). 네트워크 예외로 resp 없으면 호출하지 않는다."""
    global _LAST_RATELIMIT_POLICY, _LAST_UNITY_RATELIMIT
    now = time.time()
    short_path = path if len(path) <= 160 else path[:157] + "..."
    status = int(resp.status_code)
    with _UNITY_HTTP_LOCK:
        _UNITY_HTTP_EVENTS.append((now, method, short_path, status))
        pol = resp.headers.get("RateLimit-Policy") or resp.headers.get("rate-limit-policy")
        pol_s = pol.strip() if pol else ""
        if pol_s:
            if pol_s != _LAST_RATELIMIT_POLICY:
                _LAST_RATELIMIT_POLICY = pol_s
                logger.info(
                    "[Unity] RateLimit-Policy (서버): %s — w=초 단위 창, 예시 해석 20;w=1 → 1초당 20회, 4000;w=1800 → 30분당 4000회",
                    _LAST_RATELIMIT_POLICY,
                )
        ul = resp.headers.get("Unity-RateLimit") or resp.headers.get("unity-ratelimit")
        if ul and ul.strip():
            _LAST_UNITY_RATELIMIT = ul.strip()
        ev_snapshot = list(_UNITY_HTTP_EVENTS)
    n10 = sum(1 for t, _, _, _ in ev_snapshot if now - t <= 600)
    n30 = sum(1 for t, _, _, _ in ev_snapshot if now - t <= 1800)
    logger.debug(
        "[Unity HTTP] %s %s status=%s | 본 프로세스 최근 10분=%d회 30분=%d회",
        method,
        short_path,
        status,
        n10,
        n30,
    )


def unity_http_window_stats() -> Dict[str, Any]:
    """이 Streamlit 프로세스에서 기록된 Unity API 호출 수(슬라이딩 윈도)."""
    now = time.time()
    with _UNITY_HTTP_LOCK:
        ev = list(_UNITY_HTTP_EVENTS)
    n10 = sum(1 for t, _, _, _ in ev if now - t <= 600)
    n30 = sum(1 for t, _, _, _ in ev if now - t <= 1800)
    return {
        "requests_last_10m": n10,
        "requests_last_30m": n30,
        "events_in_buffer": len(ev),
    }


def unity_http_call_count_since(since_ts: float) -> int:
    """since_ts 이후(동일 시각 포함) 기록된 HTTP 응답 수. 단일 작업 구간 추정용."""
    with _UNITY_HTTP_LOCK:
        return sum(1 for t, _, _, _ in _UNITY_HTTP_EVENTS if t >= since_ts)


def unity_http_last_ratelimit_headers() -> Dict[str, str | None]:
    return {
        "RateLimit-Policy": _LAST_RATELIMIT_POLICY,
        "Unity-RateLimit": _LAST_UNITY_RATELIMIT,
    }


def _unity_http_op_summary_log(op_name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """upload/apply 등 한 번의 작업이 끝날 때 이 구간 동안의 HTTP 횟수와 프로세스 윈도 합계를 로그."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            t0 = time.time()
            try:
                return fn(*args, **kwargs)
            finally:
                game = kwargs.get("game", "")
                n = unity_http_call_count_since(t0)
                w = unity_http_window_stats()
                hdrs = unity_http_last_ratelimit_headers()
                logger.info(
                    "[Unity HTTP summary] op=%s game=%s http_in_this_call=%d "
                    "process_window_10m=%d process_window_30m=%d "
                    "RateLimit-Policy=%r Unity-RateLimit=%r",
                    op_name,
                    game,
                    n,
                    w["requests_last_10m"],
                    w["requests_last_30m"],
                    hdrs.get("RateLimit-Policy"),
                    hdrs.get("Unity-RateLimit"),
                )
                _set_unity_progress_hook(None)

        return wrapper

    return decorator


# --------------------------------------------------------------------
# Unity config from secrets.toml
# --------------------------------------------------------------------
# --------------------------------------------------------------------
# Unity config from secrets.toml
# --------------------------------------------------------------------
# --------------------------------------------------------------------
# Unity config from secrets.toml
# --------------------------------------------------------------------
unity_cfg = st.secrets.get("unity", {}) or {}

UNITY_ORG_ID_DEFAULT = unity_cfg.get("organization_id", "")
UNITY_CLIENT_ID_DEFAULT = unity_cfg.get("client_id", "")
UNITY_CLIENT_SECRET_DEFAULT = unity_cfg.get("client_secret", "")
UNITY_AUTH_HEADER_DEFAULT = unity_cfg.get("authorization_header", "")

# --- API Key Failover (quota 소진 시 다음 키로 전환) ---
_UNITY_AUTH_HEADERS: list[str] = [UNITY_AUTH_HEADER_DEFAULT]
_auth_header_2 = unity_cfg.get("authorization_header_2", "")
if _auth_header_2:
    _UNITY_AUTH_HEADERS.append(_auth_header_2)
    logger.info(f"Unity API key failover enabled: {len(_UNITY_AUTH_HEADERS)} keys loaded")

_unity_current_key_idx = 0  # 현재 사용 중인 키 인덱스

def _get_unity_auth_header() -> str:
    """현재 활성 API key 반환."""
    return _UNITY_AUTH_HEADERS[_unity_current_key_idx]

def _switch_to_next_key() -> bool:
    """Quota 소진 시 다음 키로 전환. 성공하면 True, 더 이상 키가 없으면 False."""
    global _unity_current_key_idx
    next_idx = _unity_current_key_idx + 1
    if next_idx < len(_UNITY_AUTH_HEADERS):
        _unity_current_key_idx = next_idx
        logger.warning(f"Unity quota exceeded → switched to key #{next_idx + 1}/{len(_UNITY_AUTH_HEADERS)}")
        return True
    return False

# Raw sections from secrets.toml
_raw_game_ids      = unity_cfg.get("game_ids", {}) or {}       # per-game app ids + maybe campaign-sets (XP HERO)
_raw_campaign_sets = unity_cfg.get("campaign_sets", {}) or {}  # per-game campaign-set IDs (Dino, Snake, Pizza…)
_raw_campaign_ids  = unity_cfg.get("campaign_ids", {}) or {}   # per-game campaign IDs

# --------------------------------------------------------------------
# App (title) IDs & Campaign-set IDs per game (multi-platform)
# --------------------------------------------------------------------
# We want:
#   UNITY_APP_IDS_ALL["XP HERO"]           = {"aos": "500230240", "ios": "500236189"}
#   UNITY_CAMPAIGN_SET_IDS_ALL["XP HERO"]  = {"aos": "67d0...",    "ios": "683d..."}
#
UNITY_APP_IDS_ALL: Dict[str, Dict[str, str]] = {}
UNITY_CAMPAIGN_SET_IDS_ALL: Dict[str, Dict[str, str]] = {}

# Defaults for operator mode:
# - UNITY_GAME_IDS[game]: default app (title) ID (AOS if present)
# - UNITY_CAMPAIGN_SET_IDS_DEFAULT[game]: default campaign-set ID (AOS if present)
UNITY_GAME_IDS: Dict[str, str] = {}
UNITY_CAMPAIGN_SET_IDS_DEFAULT: Dict[str, str] = {}

# Campaign IDs per game (operator mode default uses AOS entry)
UNITY_CAMPAIGN_IDS_ALL: Dict[str, Dict[str, List[str]]] = {}
UNITY_CAMPAIGN_IDS: Dict[str, List[str]] = {}

UNITY_BASE_URL = UNITY_ADVERTISE_API_BASE

# --------------------------------------------------------------------
# Global request gate constants (Redis 없이, 프로세스 전역 공용)
# - 같은 Streamlit 프로세스 내 모든 세션/사용자 요청을 완만하게 직렬화
# - Streamlit Cloud의 다중 인스턴스(프로세스 분산)까지는 조율하지 못함
# - 운영값은 secrets가 아니라 코드 상수로 관리
# --------------------------------------------------------------------
UNITY_GATE_ENABLED = True
UNITY_GATE_WINDOW_SECONDS = 1800
# Unity header sample: RateLimit-Policy=20;w=1,4000;w=1800
# Keep margin under 4000/1800 to reduce unexpected 429 from concurrent sessions.
UNITY_GATE_MAX_CALLS_PER_WINDOW = 3500
UNITY_GATE_MIN_INTERVAL_SECONDS = 0.3


def _unity_wait_for_global_slot(method: str, path: str) -> None:
    """요청 직전에 슬롯을 확보해 프로세스 전역 호출 밀도를 낮춘다."""
    if not UNITY_GATE_ENABLED:
        return

    short_path = path if len(path) <= 120 else path[:117] + "..."
    while True:
        now = time.time()
        wait_sec = 0.0
        with _UNITY_GATE_LOCK:
            cutoff = now - UNITY_GATE_WINDOW_SECONDS
            while _UNITY_GATE_TIMESTAMPS and _UNITY_GATE_TIMESTAMPS[0] < cutoff:
                _UNITY_GATE_TIMESTAMPS.popleft()

            if _UNITY_GATE_TIMESTAMPS:
                next_interval_at = _UNITY_GATE_TIMESTAMPS[-1] + UNITY_GATE_MIN_INTERVAL_SECONDS
                wait_sec = max(wait_sec, next_interval_at - now)

            if len(_UNITY_GATE_TIMESTAMPS) >= UNITY_GATE_MAX_CALLS_PER_WINDOW:
                next_window_at = _UNITY_GATE_TIMESTAMPS[0] + UNITY_GATE_WINDOW_SECONDS
                wait_sec = max(wait_sec, next_window_at - now)

            if wait_sec <= 0:
                _UNITY_GATE_TIMESTAMPS.append(now)
                return

        logger.info(
            "[Unity gate] wait %.1fs before %s %s (window=%ss max=%s min_interval=%.1fs)",
            wait_sec,
            method,
            short_path,
            UNITY_GATE_WINDOW_SECONDS,
            UNITY_GATE_MAX_CALLS_PER_WINDOW,
            UNITY_GATE_MIN_INTERVAL_SECONDS,
        )
        _emit_unity_progress_text(
            f"⏳ Unity API 대기 중: {method} {short_path} (약 {int(wait_sec + 0.99)}초 남음)"
        )
        time.sleep(min(wait_sec, 5.0))


def _set_unity_progress_hook(hook: Callable[[str], None] | None) -> None:
    with _UNITY_PROGRESS_HOOK_LOCK:
        global _UNITY_PROGRESS_HOOK
        _UNITY_PROGRESS_HOOK = hook


def _emit_unity_progress_text(msg: str) -> None:
    with _UNITY_PROGRESS_HOOK_LOCK:
        hook = _UNITY_PROGRESS_HOOK
    if hook is None:
        return
    try:
        hook(msg)
    except Exception:
        # UI 훅 실패가 업로드 본 동작을 막지 않도록 무시
        pass


def _extract_unity_retry_after_seconds(resp: requests.Response | None) -> float | None:
    """429 응답 헤더에서 재시도까지 남은 초를 추정한다."""
    if resp is None:
        return None
    headers = resp.headers or {}

    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    if retry_after:
        s = str(retry_after).strip()
        if re.fullmatch(r"\d+(\.\d+)?", s):
            try:
                return max(0.0, float(s))
            except Exception:
                pass
        try:
            dt = datetime.strptime(s, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
            return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
        except Exception:
            pass

    for k, v in headers.items():
        lk = str(k).lower()
        if "reset" not in lk and "ratelimit" not in lk:
            continue
        m = re.search(r"(?:reset\s*[:=]\s*)?(\d+(?:\.\d+)?)", str(v or ""), re.IGNORECASE)
        if not m:
            continue
        try:
            return max(0.0, float(m.group(1)))
        except Exception:
            continue
    return None


def _extract_retry_after_from_error_text(msg: str) -> int | None:
    """에러 문자열 내 retry_after_s=NN 값을 찾아 초 단위로 반환."""
    m = re.search(r"retry_after_s\s*=\s*(\d+)", str(msg or ""), re.IGNORECASE)
    if not m:
        return None
    try:
        return max(0, int(m.group(1)))
    except Exception:
        return None

# --------------------------------------------------------------------
# Build derived maps from unity_cfg (for defaults)
# --------------------------------------------------------------------

# 1) App (title) IDs & campaign-set IDs per game (multi-platform)
for game, val in _raw_game_ids.items():
    gname = str(game)
    app_ids: Dict[str, str] = {}
    camp_sets: Dict[str, str] = {}

    if hasattr(val, 'items'):  # AttrDict 호환
        for k, v in val.items():
            key = str(k)
            sval = str(v)

            # App (title) IDs
            if key in ("aos_app_id", "android_app_id"):
                app_ids["aos"] = sval
            elif key in ("ios_app_id", "ios_appid"):
                app_ids["ios"] = sval

            # Campaign-set IDs in game_ids (XP HERO style)
            elif key in ("aos", "aos_campaign_set"):
                camp_sets["aos"] = sval
            elif key in ("ios", "ios_campaign_set"):
                camp_sets["ios"] = sval

    elif isinstance(val, str):
        # Old-style single ID: treat as AOS app ID
        app_ids["aos"] = str(val)

    if app_ids:
        UNITY_APP_IDS_ALL[gname] = app_ids
        # default app ID for operator mode (prefer AOS)
        UNITY_GAME_IDS[gname] = app_ids.get("aos") or next(iter(app_ids.values()))

    if camp_sets:
        existing = UNITY_CAMPAIGN_SET_IDS_ALL.get(gname, {})
        existing.update(camp_sets)
        UNITY_CAMPAIGN_SET_IDS_ALL[gname] = existing
        if gname not in UNITY_CAMPAIGN_SET_IDS_DEFAULT:
            UNITY_CAMPAIGN_SET_IDS_DEFAULT[gname] = camp_sets.get("aos") or next(
                iter(camp_sets.values())
            )

# 2) Campaign-set IDs from unity.campaign_sets (Dino, Snake, Pizza, ...)
for game, val in _raw_campaign_sets.items():
    gname = str(game)
    if not isinstance(val, dict):
        continue

    camp_sets: Dict[str, str] = {}
    for k, v in val.items():
        key = str(k)
        sval = str(v)
        if key in ("aos", "aos_campaign_set"):
            camp_sets["aos"] = sval
        elif key in ("ios", "ios_campaign_set"):
            camp_sets["ios"] = sval

    if not camp_sets:
        continue

    existing = UNITY_CAMPAIGN_SET_IDS_ALL.get(gname, {})
    existing.update(camp_sets)
    UNITY_CAMPAIGN_SET_IDS_ALL[gname] = existing

    if gname not in UNITY_CAMPAIGN_SET_IDS_DEFAULT:
        UNITY_CAMPAIGN_SET_IDS_DEFAULT[gname] = camp_sets.get("aos") or next(
            iter(camp_sets.values())
        )

# 3) Campaign IDs per game (operator-mode default uses AOS entry)
for game, val in _raw_campaign_ids.items():
    gname = str(game)
    if hasattr(val, 'items'):  # AttrDict 호환
        plat_map: Dict[str, List[str]] = {}
        for plat, v in val.items():
            if isinstance(v, (list, tuple)):
                plat_map[str(plat)] = [str(x) for x in v]
            elif isinstance(v, str):
                plat_map[str(plat)] = [v]
            elif hasattr(v, '__iter__'):  # AttrDict list 호환
                plat_map[str(plat)] = [str(x) for x in v]
        if plat_map:
            UNITY_CAMPAIGN_IDS_ALL[gname] = plat_map
            # Default: prefer "aos", else first platform
            UNITY_CAMPAIGN_IDS[gname] = plat_map.get("aos") or next(iter(plat_map.values()))
    elif isinstance(val, (list, tuple)):
        lst = [str(x) for x in val]
        UNITY_CAMPAIGN_IDS_ALL[gname] = {"default": lst}
        UNITY_CAMPAIGN_IDS[gname] = lst
    elif isinstance(val, str):
        UNITY_CAMPAIGN_IDS_ALL[gname] = {"default": [val]}
        UNITY_CAMPAIGN_IDS[gname] = [val]

# 4) Vietnam-specific campaign IDs — read at runtime from st.secrets
def _load_vn_campaign_ids() -> Dict[str, Dict[str, List[str]]]:
    """Read unity.vn_campaign_ids from st.secrets at runtime (no restart needed)."""
    try:
        raw = st.secrets["unity"]["vn_campaign_ids"]
    except (KeyError, TypeError):
        return {}
    result: Dict[str, Dict[str, List[str]]] = {}
    for game in raw:
        val = raw[game]
        gname = str(game)
        plat_map: Dict[str, List[str]] = {}
        if hasattr(val, 'items'):
            for plat, v in val.items():
                ids = [str(x) for x in v] if hasattr(v, '__iter__') and not isinstance(v, str) else [str(v)]
                # filter out empty strings
                ids = [x for x in ids if x]
                plat_map[str(plat)] = ids
        if plat_map:
            result[gname] = plat_map
    return result

# --------------------------------------------------------------------
# Helper: select campaign IDs by prefix
# --------------------------------------------------------------------
def _get_campaign_ids_all_for_prefix(prefix: str = "") -> Dict[str, Dict[str, List[str]]]:
    if prefix == "vn":
        return _load_vn_campaign_ids()
    return UNITY_CAMPAIGN_IDS_ALL

def _get_campaign_ids_for_prefix(prefix: str = "") -> Dict[str, List[str]]:
    if prefix == "vn":
        vn = _load_vn_campaign_ids()
        return {g: v.get("aos") or next(iter(v.values())) for g, v in vn.items()}
    return UNITY_CAMPAIGN_IDS

# --------------------------------------------------------------------
# Internal helpers to build & use maps
# --------------------------------------------------------------------


def _normalize_game_name(name: str) -> str:
    """Normalize game name for tolerant matching (remove spaces, lowercase)."""
    return "".join(str(name).split()).lower()


def _unity_lookup_platform_slug(platform: str) -> str:
    """
    UI/설정의 platform 문자열을 game_ids 블록의 aos|ios 키로 매핑한다.

    campaign_ids가 플랫폼 없이 리스트일 때 UNITY_CAMPAIGN_IDS_ALL에 {"default": [...]}만
    생기고 selectbox 값이 "default"가 되는데, 이전에는 platform != "aos"면 무조건 ios로
    조회해서 잘못된 캠페인 세트/앱 ID가 선택될 수 있었다.
    """
    p = (platform or "aos").strip().lower()
    if p == "ios":
        return "ios"
    # aos, default, android, 기타 → operator 기본은 AOS와 동일하게 aos
    return "aos"


def get_unity_app_id(game: str, platform: str = "aos") -> str:
    """
    Return Unity app (title) ID for a given game + platform.
    """
    game_ids_section = unity_cfg.get("game_ids")
    
    # ❌ 삭제: isinstance(game_ids_section, dict) 체크
    if not game_ids_section:
        raise RuntimeError(f"❌ unity.game_ids is missing")
    
    plat_slug = _unity_lookup_platform_slug(platform)

    # Exact key match
    if game in game_ids_section:
        block = game_ids_section[game]
        # ❌ 삭제: isinstance(block, dict) 체크
        
        key = "aos_app_id" if plat_slug == "aos" else "ios_app_id"
        val = block.get(key)
        
        if val is not None:
            result = str(val).strip()
            if result:
                return result
    
    # Normalized key match
    target = _normalize_game_name(game)
    for k in game_ids_section:
        if _normalize_game_name(k) == target:
            v = game_ids_section[k]
            key = "aos_app_id" if plat_slug == "aos" else "ios_app_id"
            val = v.get(key) if hasattr(v, 'get') else None
            
            if val is not None:
                result = str(val).strip()
                if result:
                    return result
    
    # Fallback
    legacy = UNITY_GAME_IDS.get(game)
    if legacy:
        return str(legacy).strip()
    
    raise RuntimeError(
        f"❌ No app_id for '{game}' [{platform}]\n"
        f"Available: {list(game_ids_section.keys()) if hasattr(game_ids_section, 'keys') else 'N/A'}"
    )

# ━━━ unity_ads.py에서 이 함수를 완전히 교체하세요 ━━━

def get_unity_campaign_set_id(game: str, platform: str = "aos") -> str:
    """
    Return Unity campaign-set ID for a given game + platform.
    """
    plat = _unity_lookup_platform_slug(platform)
    
    # 1) Check unity.game_ids (XP HERO)
    game_ids_section = unity_cfg.get("game_ids")
    
    if game_ids_section:
        # Exact key
        if game in game_ids_section:
            block = game_ids_section[game]
            
            # Try direct key: 'aos' or 'ios'
            val = block.get(plat) if hasattr(block, 'get') else None
            if val is not None:
                result = str(val).strip()
                if result:
                    return result
            
            # Try alternative: 'aos_campaign_set' or 'ios_campaign_set'
            val = block.get(f"{plat}_campaign_set") if hasattr(block, 'get') else None
            if val is not None:
                result = str(val).strip()
                if result:
                    return result
        
        # Normalized key
        target = _normalize_game_name(game)
        for k in game_ids_section:
            if _normalize_game_name(k) == target:
                v = game_ids_section[k]
                if hasattr(v, 'get'):
                    val = v.get(plat) or v.get(f"{plat}_campaign_set")
                    if val is not None:
                        result = str(val).strip()
                        if result:
                            return result
    
    # 2) Check unity.campaign_sets (other games)
    cs_section = unity_cfg.get("campaign_sets")
    
    if cs_section:
        # Exact key
        if game in cs_section:
            block = cs_section[game]
            
            if hasattr(block, 'get'):
                val = block.get(plat) or block.get(f"{plat}_campaign_set")
                if val is not None:
                    result = str(val).strip()
                    if result:
                        return result
        
        # Normalized key
        target = _normalize_game_name(game)
        for k in cs_section:
            if _normalize_game_name(k) == target:
                v = cs_section[k]
                if hasattr(v, 'get'):
                    val = v.get(plat) or v.get(f"{plat}_campaign_set")
                    if val is not None:
                        result = str(val).strip()
                        if result:
                            return result
    
    raise RuntimeError(
        f"❌ No campaign-set ID for '{game}' [{platform}]"
    )

def debug_unity_ids(game: str = "XP HERO") -> None:
    """
    Streamlit debug helper: shows exactly what unity_cfg contains
    and what IDs we resolve for a given game.
    """
    st.write("unity_cfg.game_ids keys:", list((unity_cfg.get("game_ids") or {}).keys()))
    st.write("unity_cfg.campaign_sets keys:", list((unity_cfg.get("campaign_sets") or {}).keys()))

    game_ids_section = unity_cfg.get("game_ids") or {}
    cs_section = unity_cfg.get("campaign_sets") or {}

    block_g = game_ids_section.get(game)
    block_cs = cs_section.get(game)

    st.write("game_ids block (raw, exact):", block_g)
    st.write("campaign_sets block (raw, exact):", block_cs)

    try:
        aos_app = get_unity_app_id(game, "aos")
    except Exception as e:
        aos_app = f"ERROR: {e}"

    try:
        aos_cs = get_unity_campaign_set_id(game, "aos")
    except Exception as e:
        aos_cs = f"ERROR: {e}"

    st.write("get_unity_app_id(game, 'aos'):", aos_app)
    st.write("get_unity_campaign_set_id(game, 'aos'):", aos_cs)



def _uni_key(prefix: str, name: str) -> str:
    """Return a namespaced session state key."""
    return f"{prefix}_{name}" if prefix else name

def _ensure_unity_settings_state(prefix: str = "") -> None:
    _k = _uni_key(prefix, "unity_settings")
    if _k not in st.session_state:
        st.session_state[_k] = {}

def get_unity_settings(game: str, prefix: str = "") -> Dict:
    _ensure_unity_settings_state(prefix)
    return st.session_state[_uni_key(prefix, "unity_settings")].get(game, {})

# --------------------------------------------------------------------
# Unity settings UI
# --------------------------------------------------------------------
def render_unity_settings_panel(right_col, game: str, idx: int, is_marketer: bool = False, prefix: str = "") -> None:
    _ensure_unity_settings_state(prefix)
    _us = _uni_key(prefix, "unity_settings")
    kp = f"{prefix}_" if prefix else ""

    with right_col:
        st.markdown(f"#### {game} Unity Settings")
        cur = st.session_state[_us].get(game, {})

        # Test Mode: campaign_set_id(플랫폼별)를 title_id로 사용
        # Marketer Mode: 플랫폼에 따라 app_id를 title_id로 사용
        if is_marketer:
            # Marketer Mode: 플랫폼 선택에 따라 app_id 사용
            # settings에서 platform 정보를 가져오거나 기본값 "aos" 사용
            platform = cur.get("platform", "aos")
            try:
                secret_title_id = get_unity_app_id(game, platform)
            except Exception as e:
                logger.warning(f"Failed to get app ID for {game} ({platform}): {e}")
                # Fallback: UNITY_GAME_IDS 사용
                secret_title_id = str(UNITY_GAME_IDS.get(game, ""))
        else:
            available_platforms = []
            # 플랫폼 목록은 항상 기본 campaign IDs 기준 (app_id/campaign_set 공유)
            if game in UNITY_CAMPAIGN_IDS_ALL:
                available_platforms = list(UNITY_CAMPAIGN_IDS_ALL[game].keys())
            if not available_platforms:
                available_platforms = ["aos"]
            
            # 플랫폼 선택 UI
            prev_platform = cur.get("platform", "aos")
            if prev_platform not in available_platforms:
                prev_platform = available_platforms[0]
            
            platform = st.selectbox(
                "플랫폼 선택",
                options=available_platforms,
                index=available_platforms.index(prev_platform) if prev_platform in available_platforms else 0,
                key=f"{kp}unity_platform_{idx}",
            )
            
            # 선택한 플랫폼으로 campaign_set_id 가져오기
            try:
                secret_title_id = get_unity_campaign_set_id(game, platform)
            except Exception as e:
                logger.warning(f"Failed to get campaign set ID for {game} ({platform}): {e}")
                secret_title_id = ""
        
        # 플랫폼별 campaign_ids 가져오기 (prefix에 따라 VN/기본 선택)
        _cids_all = _get_campaign_ids_all_for_prefix(prefix)
        _cids = _get_campaign_ids_for_prefix(prefix)
        if is_marketer:
            secret_campaign_ids = _cids.get(game, []) or []
        else:
            # Test Mode: 선택한 플랫폼의 campaign_ids 사용
            if game in _cids_all:
                platform_campaign_ids = _cids_all[game].get(platform, [])
                if platform_campaign_ids:
                    secret_campaign_ids = platform_campaign_ids
                else:
                    secret_campaign_ids = _cids.get(game, []) or []
            else:
                secret_campaign_ids = _cids.get(game, []) or []

        # VN prefix: 캠페인 ID가 없으면 에러 표시
        if prefix == "vn" and not secret_campaign_ids:
            _plat_label = platform if not is_marketer else ""
            st.error(f"No Vietnam creative campaign live for **{game}**" + (f" ({_plat_label})" if _plat_label else ""))

        default_campaign_id_val = secret_campaign_ids[0] if secret_campaign_ids else ""

        title_key = f"{kp}unity_title_{idx}"
        campaign_key = f"{kp}unity_campaign_{idx}"

        if st.session_state.get(title_key) == "" and secret_title_id:
            st.session_state[title_key] = secret_title_id
        if secret_campaign_ids and not st.session_state.get(campaign_key) and default_campaign_id_val:
            st.session_state[campaign_key] = default_campaign_id_val

        if not is_marketer:
            # Test Mode: 플랫폼 변경 시 캐시된 값 무시
            prev_saved_platform = cur.get("platform", "aos")
            if prev_saved_platform != platform:
                current_title_id = secret_title_id
                current_campaign_id = default_campaign_id_val
                # 캐시도 갱신
                st.session_state[title_key] = secret_title_id
                st.session_state[campaign_key] = default_campaign_id_val
            else:
                current_title_id = cur.get("title_id") or st.session_state.get(title_key) or secret_title_id
                current_campaign_id = cur.get("campaign_id") or st.session_state.get(campaign_key) or default_campaign_id_val
        else:
            current_title_id = cur.get("title_id") or st.session_state.get(title_key) or secret_title_id
            current_campaign_id = cur.get("campaign_id") or st.session_state.get(campaign_key) or default_campaign_id_val

        default_org_id = cur.get("org_id") or UNITY_ORG_ID_DEFAULT
        default_client_id = cur.get("client_id") or UNITY_CLIENT_ID_DEFAULT
        default_client_secret = cur.get("client_secret") or UNITY_CLIENT_SECRET_DEFAULT

        unity_title_id = current_title_id
        st.session_state[title_key] = unity_title_id
        unity_campaign_id = current_campaign_id
        st.session_state[campaign_key] = unity_campaign_id
        unity_org_id = default_org_id
        st.session_state[f"{kp}unity_org_{idx}"] = unity_org_id
        unity_client_id = default_client_id
        st.session_state[f"{kp}unity_client_id_{idx}"] = unity_client_id
        unity_client_secret = default_client_secret
        st.session_state[f"{kp}unity_client_secret_{idx}"] = unity_client_secret

        unity_daily_budget = 0

        # Language 선택 (Creative 생성 시 사용)
        LANGUAGE_OPTIONS = {
            "English": "en",
            "Korean (한국어)": "ko",
            "Japanese (日本語)": "ja",
            "Chinese Simplified (简体中文)": "zh-CN",
            "Chinese Traditional (繁體中文)": "zh-TW",
            "French (Français)": "fr",
            "German (Deutsch)": "de",
            "Spanish (Español)": "es",
            "Portuguese (Português)": "pt",
            "Russian (Русский)": "ru",
            "Arabic (العربية)": "ar",
            "Italian (Italiano)": "it",
            "Turkish (Türkçe)": "tr",
            "Thai (ไทย)": "th",
            "Vietnamese (Tiếng Việt)": "vi",
            "Indonesian (Bahasa Indonesia)": "id",
            "Hindi (हिन्दी)": "hi",
            "Hebrew (עברית)": "he",
        }
        lang_labels = list(LANGUAGE_OPTIONS.keys())
        prev_lang = cur.get("language", "en")
        prev_lang_label = next((k for k, v in LANGUAGE_OPTIONS.items() if v == prev_lang), "English")
        try:
            lang_default_idx = lang_labels.index(prev_lang_label)
        except ValueError:
            lang_default_idx = 0

        selected_lang_label = st.selectbox(
            "Creative 언어",
            options=lang_labels,
            index=lang_default_idx,
            key=f"{kp}unity_language_{idx}",
        )
        unity_language = LANGUAGE_OPTIONS[selected_lang_label]

        st.markdown("#### Playable 선택")
        drive_playables = [
            v for v in (st.session_state[_uni_key(prefix, "remote_videos")].get(game, []) if _uni_key(prefix, "remote_videos") in st.session_state else [])
            if "playable" in (v.get("name") or "").lower()
        ]
        drive_options = [p["name"] for p in drive_playables]
        prev_drive_playable = cur.get("selected_playable", "")

        selected_drive_playable = st.selectbox(
            "Drive에서 가져온 플레이어블",
            options=["(선택 안 함)"] + drive_options,
            index=(drive_options.index(prev_drive_playable) + 1) if prev_drive_playable in drive_options else 0,
            key=f"{kp}unity_playable_{idx}",
        )
        chosen_drive_playable = selected_drive_playable if selected_drive_playable != "(선택 안 함)" else ""

        existing_labels: List[str] = ["(선택 안 함)"]
        existing_id_by_label: Dict[str, str] = {}
        prev_existing_label = cur.get("existing_playable_label", "")

        try:
            org_for_list = (unity_org_id or UNITY_ORG_ID_DEFAULT).strip()
            title_for_list = (unity_title_id or secret_title_id).strip()
            campaign_for_list = (unity_campaign_id or "").strip()

            if org_for_list:
                # Marketer: App 레벨 전체 조회
                # Operator (Test Mode): Campaign Set ID를 title_id로 사용하여 App 레벨에서 playable 조회
                if is_marketer:
                    if not title_for_list:
                        st.warning("⚠️ Title ID가 설정되지 않았습니다.")
                        playable_creatives = []
                    else:
                        playable_creatives = _unity_list_playable_creatives(
                            org_id=org_for_list, 
                            title_id=title_for_list
                        )
                else:
                    # Test Mode: 선택한 플랫폼의 campaign set ID를 title_id로 사용
                    try:
                        campaign_set_id = get_unity_campaign_set_id(game, platform)
                        logger.info(f"Test Mode: Using campaign set ID as title_id: {campaign_set_id} for game: {game}, platform: {platform}")
                        with st.expander("🔍 Debug: Unity Playable 조회 정보", expanded=False):
                            st.write(f"**Game:** {game}")
                            st.write(f"**Platform:** {platform}")
                            st.write(f"**Org ID:** {org_for_list}")
                            st.write(f"**Campaign Set ID (title_id로 사용):** {campaign_set_id}")
                        playable_creatives = _unity_list_playable_creatives(
                            org_id=org_for_list,
                            title_id=campaign_set_id
                        )
                        logger.info(f"Found {len(playable_creatives)} playables using campaign set ID")
                        if len(playable_creatives) == 0:
                            st.info(f"ℹ️ Campaign Set ID `{campaign_set_id}`에서 playable을 찾지 못했습니다. Unity에 playable이 등록되어 있는지 확인하세요.")
                    except Exception as e:
                        logger.warning(f"Failed to get campaign set ID for {game} ({platform}), error: {e}")
                        devtools.record_exception("Unity Campaign Set ID lookup failed", e)
                        st.error("❌ Campaign Set ID 조회 실패")
                        # Fallback: 기존 title_id 사용 (있는 경우)
                        if title_for_list:
                            logger.info(f"Fallback: Using title_id: {title_for_list}")
                            st.info(f"⚠️ Fallback: Title ID `{title_for_list}`를 사용합니다.")
                            try:
                                playable_creatives = _unity_list_playable_creatives(
                                    org_id=org_for_list,
                                    title_id=title_for_list
                                )
                            except Exception as e2:
                                logger.warning(f"Fallback also failed: {e2}")
                                playable_creatives = []
                                st.error(f"❌ Fallback도 실패: {e2}")
                        else:
                            playable_creatives = []
                            st.warning("⚠️ Title ID도 설정되지 않아 playable을 조회할 수 없습니다.")
                
                if playable_creatives:
                    for cr in playable_creatives:
                        cr_id = str(cr.get("id") or "")
                        cr_name = cr.get("name") or "(no name)"
                        cr_type = cr.get("type", "")
                        if not cr_id: continue
                        label = f"{cr_name} ({cr_type}) [{cr_id}]"
                        existing_labels.append(label)
                        existing_id_by_label[label] = cr_id
                else:
                    logger.info(f"No playables found for game: {game}, org: {org_for_list}")
        except Exception as e:
            error_msg = f"Unity playable 목록을 불러오지 못했습니다: {e}"
            logger.exception(error_msg)
            devtools.record_exception("Unity playable list load failed", e)
            st.error("❌ Unity playable 목록을 불러오지 못했습니다.")

        try:
            existing_default_idx = existing_labels.index(prev_existing_label)
        except ValueError:
            existing_default_idx = 0

        selected_existing_label = st.selectbox(
            "Unity에 이미 있는 playable",
            options=existing_labels,
            index=existing_default_idx,
            key=f"{kp}unity_existing_playable_{idx}",
        )

        existing_playable_id = ""
        if selected_existing_label != "(선택 안 함)":
            existing_playable_id = existing_id_by_label.get(selected_existing_label, "")

        st.warning(
            "Unity creative pack은 **9:16 영상 1개 + 16:9 영상 1개 + 1개의 playable** 조합을 기준으로 생성됩니다."
        )

        # platform 정보도 settings에 저장 (Test Mode와 Marketer Mode 모두)
        settings_dict = {
            "title_id": (unity_title_id or "").strip(),
            "campaign_id": (unity_campaign_id or "").strip(),
            "org_id": (unity_org_id or "").strip(),
            "daily_budget_usd": int(unity_daily_budget),
            "language": unity_language,
            "selected_playable": chosen_drive_playable,
            "existing_playable_id": existing_playable_id,
            "existing_playable_label": selected_existing_label,
            "title_id_source": "app" if is_marketer else "campaign_set",
        }
        
        # 플랫폼 정보 추가
        if not is_marketer:
            settings_dict["platform"] = platform

        # prefix 저장 (VN 등 별도 campaign_ids 선택용)
        if prefix:
            settings_dict["prefix"] = prefix

        st.session_state[_us][game] = settings_dict

# --------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------

def _get_upload_state_key(game: str, campaign_id: str) -> str:
    """Generate a unique key for tracking upload state per game/campaign."""
    return f"unity_upload_state_{game}_{campaign_id}"

def _init_upload_state(game: str, campaign_id: str, videos: List[Dict]) -> Dict:
    """
    Initialize or load upload state for resumability.
    
    State structure:
    {
        "video_creatives": {
            "video_filename.mp4": "creative_id_12345" or None
        },
        "playable_creative": "creative_id_67890" or None,
        "creative_packs": {
            "pack_name": "pack_id_abc" or None
        },
        "completed_packs": ["pack_id_1", "pack_id_2"],
        "total_expected": 10
    }
    """
    key = _get_upload_state_key(game, campaign_id)
    
    if key not in st.session_state:
        # Initialize new state
        state = {
            "video_creatives": {},
            "playable_creative": None,
            "creative_packs": {},
            "completed_packs": [],
            "total_expected": 0
        }
        
        # Pre-populate video names
        for v in videos or []:
            name = v.get("name", "")
            if "playable" not in name.lower():
                state["video_creatives"][name] = None
        
        st.session_state[key] = state
    
    return st.session_state[key]


ASIA_SEOUL = timezone(timedelta(hours=9))

def next_sat_0000_kst(today: datetime | None = None) -> str:
    now = (today or datetime.now(ASIA_SEOUL)).astimezone(ASIA_SEOUL)
    base = now.replace(hour=0, minute=0, second=0, microsecond=0)
    days_until_sat = (5 - base.weekday()) % 7 or 7
    start_dt = (base + timedelta(days=days_until_sat)).replace(hour=9, minute=0)
    return start_dt.isoformat()

def unity_creative_name_from_filename(filename: str) -> str:
    stem = pathlib.Path(filename).stem
    # Match 3-5 digit codes (e.g., 001, 1234, 12345)
    m = re.search(r"(\d{3,5})(?!.*\d)", stem)
    code = m.group(1) if m else "000"
    return f"video{code}"

def _extract_video_part_from_base(base: str) -> str:
    """
    Extract video part from base name (e.g., 'video001' from 'video001_1080x1920').
    Returns the part that starts with 'video' followed by digits.
    """
    # Try to find 'video' followed by digits
    m = re.search(r"(video\d+)", base, re.IGNORECASE)
    if m:
        return m.group(1).lower()
    # Fallback: use unity_creative_name_from_filename logic
    m = re.search(r"(\d{3,5})(?!.*\d)", base)
    code = m.group(1) if m else "000"
    return f"video{code}"

def _clean_playable_name_for_pack(playable_name_or_label: str) -> str:
    """
    Clean playable name for creative pack naming.
    
    Rules:
    1. If label format "name (type) [id]", extract just the name part
    2. Remove .html extension
    3. Keep only the part before the first underscore
    
    Example:
    - "playable001vari_hi_unityads.html" -> "playable001vari"
    """
    if not playable_name_or_label:
        return ""
    
    # Step 1: Extract name from label format "name (type) [id]"
    name = playable_name_or_label.split(" (")[0].strip()
    
    # Step 2: Remove .html extension
    name = re.sub(r"\.html$", "", name, flags=re.IGNORECASE)
    
    # Step 3: Keep only the part before the first underscore
    first_underscore_idx = name.find("_")
    if first_underscore_idx >= 0:
        name = name[:first_underscore_idx]
    
    return name

# --------------------------------------------------------------------
# API Helpers
# --------------------------------------------------------------------
def _unity_headers() -> dict:
    if not UNITY_AUTH_HEADER_DEFAULT:
        raise RuntimeError("unity.authorization_header is missing in secrets.toml")
    return {"Authorization": _get_unity_auth_header(), "Content-Type": "application/json"}

def _unity_post(path: str, json_body: dict) -> dict:
    last_429_detail: str = ""
    last_429_wait: float | None = None
    exhausted_quota = False

    def _on_retry(attempt: int, resp: requests.Response | None, err: Exception | None) -> None:
        nonlocal last_429_detail, exhausted_quota
        if err is not None:
            logger.warning("Request failed: %s. Retrying...", err)
            return
        if resp is None:
            return
        if resp.status_code == 429:
            detail = (resp.text or "")[:800]
            last_429_detail = detail
            last_429_wait = _extract_unity_retry_after_seconds(resp)
            rate_headers = {k: v for k, v in resp.headers.items() if "rate" in k.lower() or "retry" in k.lower() or "limit" in k.lower()}
            logger.error(
                "[Unity 429] POST %s | attempt=%s/8 | response_body=%s | rate_headers=%s",
                path,
                attempt + 1,
                detail,
                rate_headers,
            )
            if "quota" in detail.lower():
                if _switch_to_next_key():
                    logger.warning("Unity Quota Exceeded on key -> switching to next key and retrying")
                    _emit_unity_progress_text("⚠️ Unity quota 소진 감지: 보조 API 키로 전환해 재시도합니다.")
                    return
                exhausted_quota = True
                _emit_unity_progress_text("❌ Unity quota가 모두 소진되었습니다. 쿼터 리셋 후 다시 시도해주세요.")
                return
            sleep_sec = 2 ** (attempt + 1)
            hdr_wait = last_429_wait
            if hdr_wait is not None:
                sleep_sec = max(sleep_sec, hdr_wait)
            logger.warning("Unity 429 Rate Limit (attempt %s/8). Sleeping %.1fs...", attempt + 1, sleep_sec)
            _emit_unity_progress_text(
                f"⏳ Unity 429 (POST) 재시도 대기: 약 {int(sleep_sec + 0.99)}초 남음 "
                f"(attempt {attempt + 1}/8)"
            )

    def _should_retry(resp: requests.Response) -> bool:
        if resp.status_code != 429:
            return False
        if "quota" in (resp.text or "").lower():
            return not exhausted_quota
        return True

    try:
        _unity_wait_for_global_slot("POST", path)
        request_dto = build_unity_request(
            "POST",
            path,
            headers=_unity_headers(),
            json=json_body,
            timeout=60,
        )
        retry_dto = RetryPolicyDTO(
            max_retries=7,
            backoff_strategy=lambda attempt: float(2 ** (attempt + 1)),
            should_retry=_should_retry,
        )
        context = RequestExecutionContextDTO(
            on_response=lambda r: _record_unity_http_call("POST", path, r),
            on_retry=_on_retry,
        )
        resp = execute_request(request_dto, retry_dto, context=context)
    except HttpRequestError:
        if exhausted_quota and last_429_detail:
            suffix = (
                f" | retry_after_s={int(last_429_wait + 0.99)}"
                if last_429_wait is not None
                else ""
            )
            raise RuntimeError(f"Unity Quota Exceeded (all keys exhausted){suffix}: {last_429_detail}")
        if last_429_detail:
            raise RuntimeError(
                f"Unity 429 Rate Limit - POST {path} failed after 8 retries. "
                f"Last API response: {last_429_detail[:400]}"
            )
        raise RuntimeError(f"Unity POST {path} failed after 8 retries.")

    if not resp.ok:
        error_body = resp.text[:800] if resp.text else ""
        logger.error(
            f"[Unity Error] POST {path} | status={resp.status_code} | "
            f"response_body={error_body}"
        )
        raise RuntimeError(f"Unity POST {path} failed ({resp.status_code}): {error_body}")
    return resp.json()

def _unity_put(path: str, json_body: dict) -> dict:
    _unity_wait_for_global_slot("PUT", path)
    request_dto = build_unity_request(
        "PUT",
        path,
        headers=_unity_headers(),
        json=json_body,
        timeout=60,
    )
    context = RequestExecutionContextDTO(
        on_response=lambda r: _record_unity_http_call("PUT", path, r),
        on_retry=lambda attempt, resp, err: logger.warning("Unity PUT retry attempt=%s path=%s", attempt + 1, path),
    )
    resp = execute_request(request_dto, build_default_api_policy(max_retries=2), context=context)
    if not resp.ok:
        raise RuntimeError(f"Unity PUT {path} failed ({resp.status_code}): {resp.text[:400]}")
    return resp.json()

def _unity_get(path: str, params: dict | None = None) -> dict:
    last_429_detail: str = ""
    last_429_wait: float | None = None
    exhausted_quota = False

    def _on_retry(attempt: int, resp: requests.Response | None, err: Exception | None) -> None:
        nonlocal last_429_detail, exhausted_quota
        if err is not None:
            logger.warning("Unity GET request failed: %s", err)
            return
        if resp is None or resp.status_code != 429:
            return
        detail = (resp.text or "")[:800]
        last_429_detail = detail
        last_429_wait = _extract_unity_retry_after_seconds(resp)
        rate_headers = {k: v for k, v in resp.headers.items() if "rate" in k.lower() or "retry" in k.lower() or "limit" in k.lower()}
        logger.error(
            "[Unity 429] GET %s | attempt=%s/5 | response_body=%s | rate_headers=%s",
            path,
            attempt + 1,
            detail,
            rate_headers,
        )
        if "quota" in detail.lower():
            if _switch_to_next_key():
                logger.warning("Unity Quota Exceeded on GET -> switching to next key")
                _emit_unity_progress_text("⚠️ Unity quota 소진 감지(GET): 보조 API 키로 전환해 재시도합니다.")
            else:
                exhausted_quota = True
                _emit_unity_progress_text("❌ Unity quota가 모두 소진되었습니다. 쿼터 리셋 후 다시 시도해주세요.")
            return
        hdr_wait = _extract_unity_retry_after_seconds(resp)
        if hdr_wait is not None:
            _emit_unity_progress_text(
                f"⏳ Unity 429 (GET) 재시도 대기: 약 {int(hdr_wait + 0.99)}초 남음 "
                f"(attempt {attempt + 1}/5)"
            )

    def _should_retry(resp: requests.Response) -> bool:
        if resp.status_code != 429:
            return False
        if "quota" in (resp.text or "").lower():
            return not exhausted_quota
        return True

    try:
        request_dto = build_unity_request(
            "GET",
            path,
            headers=_unity_headers(),
            params=params or {},
            timeout=60,
        )
        retry_dto = RetryPolicyDTO(
            max_retries=4,
            backoff_strategy=lambda attempt: float(2 ** (attempt + 1)),
            should_retry=_should_retry,
        )
        context = RequestExecutionContextDTO(
            on_response=lambda r: _record_unity_http_call("GET", path, r),
            on_retry=_on_retry,
        )
        resp = execute_request(request_dto, retry_dto, context=context)
    except HttpRequestError:
        suffix = (
            f" | retry_after_s={int(last_429_wait + 0.99)}"
            if last_429_wait is not None
            else ""
        )
        raise RuntimeError(
            f"Unity 429 Rate Limit - GET {path} failed after 5 retries{suffix}. "
            f"Last API response: {last_429_detail[:400]}"
        )

    if not resp.ok:
        error_body = resp.text[:800] if resp.text else ""
        logger.error(
            f"[Unity Error] GET {path} | status={resp.status_code} | "
            f"params={params} | response_body={error_body}"
        )
        raise RuntimeError(f"Unity GET {path} failed ({resp.status_code}): {error_body}")
    return resp.json()

def _unity_delete(path: str) -> None:
    _unity_wait_for_global_slot("DELETE", path)
    request_dto = build_unity_request(
        "DELETE",
        path,
        headers=_unity_headers(),
        timeout=60,
    )
    context = RequestExecutionContextDTO(
        on_response=lambda r: _record_unity_http_call("DELETE", path, r),
        on_retry=lambda attempt, resp, err: logger.warning("Unity DELETE retry attempt=%s path=%s", attempt + 1, path),
    )
    resp = execute_request(request_dto, build_default_api_policy(max_retries=2), context=context)
    if not resp.ok:
        error_body = resp.text[:800] if resp.text else ""
        logger.error(
            f"[Unity Error] DELETE {path} | status={resp.status_code} | "
            f"response_body={error_body}"
        )
        raise RuntimeError(f"Unity DELETE {path} failed ({resp.status_code}): {error_body}")

# --------------------------------------------------------------------
# Creative Helpers
# --------------------------------------------------------------------
def _unity_list_assigned_creative_packs(*, org_id: str, title_id: str, campaign_id: str) -> List[dict]:
    path = f"organizations/{org_id}/apps/{title_id}/campaigns/{campaign_id}/assigned-creative-packs"
    
    # 1. No limit param (as it caused 400 error)
    meta = _unity_get(path)

    # 2. Return the correct list structure
    if isinstance(meta, list): return meta
    if isinstance(meta, dict):
        # FIX: Check 'results' which is standard for this API
        if isinstance(meta.get("results"), list): return meta["results"]
        if isinstance(meta.get("items"), list): return meta["items"]
        if isinstance(meta.get("data"), list): return meta["data"]
        
        for v in meta.values():
            if isinstance(v, list): return v

    return []

def _unity_assign_creative_pack(*, org_id: str, title_id: str, campaign_id: str, creative_pack_id: str) -> None:
    path = f"organizations/{org_id}/apps/{title_id}/campaigns/{campaign_id}/assigned-creative-packs"
    try:
        _unity_post(path, {"id": creative_pack_id})
    except Exception as e:
        error_str = str(e).lower()
        # Check if error is related to capacity/limit
        if any(keyword in error_str for keyword in ["limit", "maximum", "exceeded", "full", "capacity", "quota"]):
            raise RuntimeError("Creative pack 개수가 최대입니다. 사용하지 않는 creative을 제거해주세요.")
        raise  # Re-raise original error if not capacity-related

def _unity_unassign_creative_pack(*, org_id: str, title_id: str, campaign_id: str, assigned_creative_pack_id: str) -> None:
    path = f"organizations/{org_id}/apps/{title_id}/campaigns/{campaign_id}/assigned-creative-packs/{assigned_creative_pack_id}"
    _unity_delete(path)

def _unity_unassign_with_retry(*, org_id: str, title_id: str, campaign_id: str, assigned_creative_pack_id: str, max_retries: int = 3) -> None:
    """Unassign with exponential backoff on rate limit."""
    for attempt in range(max_retries):
        try:
            _unity_unassign_creative_pack(
                org_id=org_id,
                title_id=title_id,
                campaign_id=campaign_id,
                assigned_creative_pack_id=assigned_creative_pack_id
            )
            return  # Success
        except RuntimeError as e:
            if "429" in str(e) and attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)  # 2, 4, 8 seconds
                logger.warning(f"Rate limit hit, waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                raise  # Give up after retries

def _unity_get_creative(*, org_id: str, title_id: str, creative_id: str) -> dict:
    path = f"organizations/{org_id}/apps/{title_id}/creatives/{creative_id}"
    return _unity_get(path)

def _unity_create_video_creative(*, org_id: str, title_id: str, video_path: str, name: str, language: str = "en") -> str:
    if not os.path.isfile(video_path):
        raise RuntimeError(f"Video path does not exist: {video_path!r}")

    # FIX: Use original name for fileName, not temp file path
    display_filename = name
    if not display_filename.lower().endswith(".mp4"):
        display_filename += ".mp4"

    creative_info = {
        "name": name,
        "language": language,
        "video": {"fileName": display_filename}
    }

    path = f"organizations/{org_id}/apps/{title_id}/creatives"
    last_429_detail: str = ""

    for attempt in range(8):
        try:
            headers = {"Authorization": _get_unity_auth_header()}
            with open(video_path, "rb") as f:
                files = {
                    "creativeInfo": (None, json.dumps(creative_info), "application/json"),
                    "videoFile": (display_filename, f, "video/mp4"),
                }
                request_dto = build_unity_request(
                    "POST",
                    path,
                    headers=headers,
                    files=files,
                    timeout=300,
                )
                _unity_wait_for_global_slot("POST", f"{path}#multipart_video")
                context = RequestExecutionContextDTO(
                    on_response=lambda r: _record_unity_http_call("POST", f"{path}#multipart_video", r),
                    on_retry=lambda a, _r, err: logger.warning(
                        "[Unity HTTP retry] CREATE CREATIVE transport retry=%s/2 name=%s err=%s",
                        a + 1,
                        name,
                        str(err)[:200] if err else "",
                    ),
                )
                # TLS EOF / transient network 오류 완화를 위해 multipart 단계에도 짧은 전송 재시도 적용
                resp = execute_request(
                    request_dto,
                    build_upload_multipart_policy(max_retries=2),
                    context=context,
                )

            if resp.status_code == 429:
                detail = (resp.text or "")[:800]
                last_429_detail = detail
                rate_headers = {k: v for k, v in resp.headers.items() if "rate" in k.lower() or "retry" in k.lower() or "limit" in k.lower()}
                logger.error(
                    f"[Unity 429] CREATE CREATIVE | name={name} | attempt={attempt+1}/8 | "
                    f"response_body={detail} | rate_headers={rate_headers}"
                )
                if "quota" in detail.lower():
                    if _switch_to_next_key():
                        logger.warning(f"Unity Quota Exceeded on CREATE CREATIVE → switching to next key")
                        _emit_unity_progress_text("⚠️ Unity quota 소진 감지: 보조 API 키로 전환해 재시도합니다.")
                        continue
                    _emit_unity_progress_text("❌ Unity quota가 모두 소진되었습니다. 쿼터 리셋 후 다시 시도해주세요.")
                    hdr_wait = _extract_unity_retry_after_seconds(resp)
                    suffix = (
                        f" | retry_after_s={int(hdr_wait + 0.99)}"
                        if hdr_wait is not None
                        else ""
                    )
                    raise RuntimeError(f"Unity Quota Exceeded (all keys exhausted){suffix}: {detail}")
                sleep_sec = 5 * (attempt + 1)
                hdr_wait = _extract_unity_retry_after_seconds(resp)
                if hdr_wait is not None:
                    sleep_sec = max(sleep_sec, hdr_wait)
                _emit_unity_progress_text(
                    f"⏳ Unity 429 (CREATE CREATIVE) 재시도 대기: 약 {int(sleep_sec + 0.99)}초 남음 "
                    f"(attempt {attempt + 1}/8)"
                )
                time.sleep(sleep_sec)
                continue

            if not resp.ok:
                error_text = resp.text[:800] if resp.text else ""
                logger.error(
                    f"[Unity Error] CREATE CREATIVE | name={name} | status={resp.status_code} | "
                    f"response_body={error_text}"
                )
                # Check if error is related to capacity/limit
                error_lower = error_text.lower()
                if any(keyword in error_lower for keyword in ["limit", "maximum", "exceeded", "full", "capacity", "quota"]):
                    raise RuntimeError(f"Creative 개수가 최대입니다. 사용하지 않는 creative을 제거해주세요. (API: {error_text[:200]})")
                raise RuntimeError(f"Unity create creative failed ({resp.status_code}): {error_text}")

            body = resp.json()
            return str(body.get("id") or body.get("creativeId"))
        except Exception as e:
            if "Quota Exceeded" in str(e) or "최대" in str(e):
                raise e
            logger.warning(f"[Unity Retry] CREATE CREATIVE | name={name} | attempt={attempt+1}/8 | error={e}")
            time.sleep(5)

    # 429가 반복되어 여기에 도달한 경우, 실제 API 응답을 포함
    if last_429_detail:
        raise RuntimeError(
            f"Unity 429 Rate Limit — create creative '{name}' failed after 8 retries. "
            f"Last API response: {last_429_detail[:400]}"
        )
    raise RuntimeError(f"Unity create creative failed after 8 retries. name={name}")

def _unity_create_playable_creative(*, org_id: str, title_id: str, playable_path: str, name: str, language: str = "en") -> str:
    if not os.path.isfile(playable_path):
        raise RuntimeError(f"Playable path does not exist: {playable_path!r}")

    file_name = os.path.basename(playable_path)
    creative_info = {
        "name": name, 
        "language": language, 
        "playable": {
            "fileName": file_name,
            "orientation": "both"  # landscape, portrait, both 중 하나
        }
    }

    logger.info(f"Unity playable creativeInfo: {json.dumps(creative_info)}")
    path = f"organizations/{org_id}/apps/{title_id}/creatives"

    # MIME type 결정 (zip vs html)
    if file_name.lower().endswith(".zip"):
        mime_type = "application/zip"
    else:
        mime_type = "text/html"

    last_429_detail: str = ""

    for attempt in range(8):
        try:
            headers = {"Authorization": _get_unity_auth_header()}
            with open(playable_path, "rb") as f:
                files = {
                    "creativeInfo": (None, json.dumps(creative_info), "application/json"),
                    "playableFile": (file_name, f, mime_type),
                }
                request_dto = build_unity_request(
                    "POST",
                    path,
                    headers=headers,
                    files=files,
                    timeout=300,
                )
                _unity_wait_for_global_slot("POST", f"{path}#multipart_playable")
                context = RequestExecutionContextDTO(
                    on_response=lambda r: _record_unity_http_call("POST", f"{path}#multipart_playable", r),
                    on_retry=lambda a, _r, err: logger.warning(
                        "[Unity HTTP retry] CREATE PLAYABLE transport retry=%s/2 file=%s err=%s",
                        a + 1,
                        file_name,
                        str(err)[:200] if err else "",
                    ),
                )
                # TLS EOF / transient network 오류 완화를 위해 multipart 단계에도 짧은 전송 재시도 적용
                resp = execute_request(
                    request_dto,
                    build_upload_multipart_policy(max_retries=2),
                    context=context,
                )

            if resp.status_code == 429:
                detail = (resp.text or "")[:800]
                last_429_detail = detail
                rate_headers = {k: v for k, v in resp.headers.items() if "rate" in k.lower() or "retry" in k.lower() or "limit" in k.lower()}
                logger.error(
                    f"[Unity 429] CREATE PLAYABLE | file={file_name} | attempt={attempt+1}/8 | "
                    f"response_body={detail} | rate_headers={rate_headers}"
                )
                if "quota" in detail.lower() and _switch_to_next_key():
                    logger.warning(f"Unity Quota Exceeded on CREATE PLAYABLE → switching to next key")
                    _emit_unity_progress_text("⚠️ Unity quota 소진 감지: 보조 API 키로 전환해 재시도합니다.")
                    continue
                if "quota" in detail.lower():
                    _emit_unity_progress_text("❌ Unity quota가 모두 소진되었습니다. 쿼터 리셋 후 다시 시도해주세요.")
                    hdr_wait = _extract_unity_retry_after_seconds(resp)
                    suffix = (
                        f" | retry_after_s={int(hdr_wait + 0.99)}"
                        if hdr_wait is not None
                        else ""
                    )
                    raise RuntimeError(f"Unity Quota Exceeded (all keys exhausted){suffix}: {detail}")
                sleep_sec = 3 * (attempt + 1)
                hdr_wait = _extract_unity_retry_after_seconds(resp)
                if hdr_wait is not None:
                    sleep_sec = max(sleep_sec, hdr_wait)
                _emit_unity_progress_text(
                    f"⏳ Unity 429 (CREATE PLAYABLE) 재시도 대기: 약 {int(sleep_sec + 0.99)}초 남음 "
                    f"(attempt {attempt + 1}/8)"
                )
                time.sleep(sleep_sec)
                continue

            if not resp.ok:
                error_text = resp.text[:800] if resp.text else ""
                logger.error(
                    f"[Unity Error] CREATE PLAYABLE | file={file_name} | status={resp.status_code} | "
                    f"response_body={error_text}"
                )
                # Check if error is related to capacity/limit
                error_lower = error_text.lower()
                if any(keyword in error_lower for keyword in ["limit", "maximum", "exceeded", "full", "capacity", "quota"]):
                    raise RuntimeError(f"Creative 개수가 최대입니다. 사용하지 않는 creative을 제거해주세요. (API: {error_text[:200]})")
                raise RuntimeError(f"Unity create playable failed ({resp.status_code}): {error_text}")

            body = resp.json()
            return str(body.get("id") or body.get("creativeId"))
        except Exception as e:
            if "최대" in str(e):
                raise e
            logger.error(f"[Unity Retry] CREATE PLAYABLE | file={file_name} | attempt={attempt+1}/8 | error={e}")
            time.sleep(3)

    if last_429_detail:
        raise RuntimeError(
            f"Unity 429 Rate Limit — create playable '{file_name}' failed after 8 retries. "
            f"Last API response: {last_429_detail[:400]}"
        )
    raise RuntimeError(f"Unity create playable creative failed after 8 retries. File: {file_name}")

def _unity_create_creative_pack(*, org_id: str, title_id: str, pack_name: str, creative_ids: List[str], pack_type: str = "video") -> str:
    clean_ids = [str(x) for x in creative_ids if x]
    
    # Playable만 생성 시 1개 허용, 그 외에는 2개 이상 필요
    if pack_type == "playable":
        if len(clean_ids) < 1:
            raise RuntimeError(f"Not enough creative IDs to create a playable pack: {clean_ids}")
    else:
        if len(clean_ids) < 2:
            raise RuntimeError(f"Not enough creative IDs to create a pack: {clean_ids}")

    payload = {
        "name": pack_name,
        "creativeIds": clean_ids,
        "type": pack_type,
    }

    path = f"organizations/{org_id}/apps/{title_id}/creative-packs"
    logger.info(f"[Unity] CREATE PACK | name={pack_name} | type={pack_type} | creative_ids={clean_ids}")
    try:
        meta = _unity_post(path, payload)
    except Exception as e:
        error_str = str(e).lower()
        logger.error(f"[Unity Error] CREATE PACK FAILED | name={pack_name} | error={e}")
        # Check if error is related to capacity/limit
        if any(keyword in error_str for keyword in ["limit", "maximum", "exceeded", "full", "capacity", "quota"]):
            raise RuntimeError(f"Creative pack 개수가 최대입니다. 사용하지 않는 creative을 제거해주세요. (API: {str(e)[:200]})")
        raise  # Re-raise original error if not capacity-related
    
    creative_pack_id = meta.get("id") or meta.get("creativePackId")
    if not creative_pack_id:
        raise RuntimeError(f"Unity creative pack response missing id: {meta}")

    return str(creative_pack_id)

def _unity_list_playable_creatives(*, org_id: str, title_id: str) -> List[dict]:
    path = f"organizations/{org_id}/apps/{title_id}/creatives"
    meta = _unity_get(path)

    items: List[dict] = []
    if isinstance(meta, list): items = meta
    elif isinstance(meta, dict):
        if isinstance(meta.get("items"), list): items = meta["items"]
        elif isinstance(meta.get("data"), list): items = meta["data"]
        else:
            for v in meta.values():
                if isinstance(v, list): items.extend(v)

    playables: List[dict] = []
    for cr in items:
        if not isinstance(cr, dict): continue
        t = (cr.get("type") or "").lower()
        if "playable" in t or "cpe" in t:
             playables.append(cr)

    return playables

def _unity_list_campaign_playables(*, org_id: str, title_id: str, campaign_id: str) -> List[dict]:
    """특정 Campaign의 할당된 Playable 크리에이티브 조회 (Operator용)"""
    try:
        # 1. 캠페인에 할당된 모든 Creative Pack 가져오기
        assigned_packs = _unity_list_assigned_creative_packs(
            org_id=org_id, 
            title_id=title_id, 
            campaign_id=campaign_id
        )
        
        playable_ids = set()
        
        # 2. 각 Pack의 creativeIds에서 Playable 추출
        for pack in assigned_packs:
            pack_id = pack.get("id")
            if not pack_id:
                continue
            
            # Pack 상세정보 조회
            pack_path = f"organizations/{org_id}/apps/{title_id}/creative-packs/{pack_id}"
            pack_detail = _unity_get(pack_path)
            
            creative_ids = pack_detail.get("creativeIds", [])
            for cid in creative_ids:
                playable_ids.add(str(cid))
        
        # 3. 각 Creative ID의 타입 확인 후 Playable만 필터링
        playables = []
        for cid in playable_ids:
            try:
                creative = _unity_get_creative(org_id=org_id, title_id=title_id, creative_id=cid)
                c_type = (creative.get("type") or "").lower()
                
                if "playable" in c_type or "cpe" in c_type:
                    playables.append(creative)
                    
            except Exception as e:
                logger.warning(f"Failed to fetch creative {cid}: {e}")
                continue
        
        return playables
        
    except Exception as e:
        logger.warning(f"Failed to list campaign playables: {e}")
        return []
        
def _save_upload_state(game: str, campaign_id: str, state: Dict):
    """Save upload state to session."""
    key = _get_upload_state_key(game, campaign_id)
    st.session_state[key] = state


def _clear_upload_state(game: str, campaign_id: str):
    """Clear upload state (call when upload completes successfully)."""
    key = _get_upload_state_key(game, campaign_id)
    if key in st.session_state:
        del st.session_state[key]


def _check_existing_creative(org_id: str, title_id: str, name: str) -> str | None:
    """
    Check if a creative with this name already exists in Unity.
    Returns creative_id if found, None otherwise.
    """
    try:
        path = f"organizations/{org_id}/apps/{title_id}/creatives"
        meta = _unity_get(path, params={"limit": 100})

        items = []
        if isinstance(meta, list):
            items = meta
        elif isinstance(meta, dict):
            items = meta.get("items") or meta.get("data") or []

        for creative in items:
            if creative.get("name") == name:
                return str(creative.get("id", ""))

        return None
    except Exception as e:
        logger.warning(f"Could not check existing creative: {e}")
        return None


def _check_existing_pack(org_id: str, title_id: str, pack_name: str) -> str | None:
    """
    Check if a creative pack with this name already exists.
    Returns pack_id if found, None otherwise.
    """
    try:
        path = f"organizations/{org_id}/apps/{title_id}/creative-packs"

        all_items = []
        offset = 0
        limit = 100

        while True:
            meta = _unity_get(path, params={"limit": limit, "offset": offset})

            items = []
            if isinstance(meta, list):
                items = meta
            elif isinstance(meta, dict):
                items = meta.get("items") or meta.get("data") or []

            if not items:
                break

            all_items.extend(items)

            if len(items) < limit:
                break

            offset += limit

        pack_name_normalized = pack_name.strip().lower()

        for pack in all_items:
            existing_name = pack.get("name", "").strip().lower()
            if existing_name == pack_name_normalized:
                return str(pack.get("id", ""))

        return None
    except Exception as e:
        logger.warning(f"Could not check existing pack: {e}")
        return None

def _check_existing_pack_by_creatives(org_id: str, title_id: str, creative_ids: List[str]) -> tuple[str | None, str | None]:
    """
    Check if a creative pack already exists with the same creative IDs (video + playable combination).
    Returns (pack_id, pack_name) if found, (None, None) otherwise.
    """
    try:
        path = f"organizations/{org_id}/apps/{title_id}/creative-packs"
        meta = _unity_get(path, params={"limit": 100})

        items = []
        if isinstance(meta, list):
            items = meta
        elif isinstance(meta, dict):
            items = meta.get("items") or meta.get("data") or []

        target_creative_set = set(str(cid) for cid in creative_ids if cid)

        for pack in items:
            pack_creative_ids = pack.get("creativeIds") or pack.get("creative_ids") or []
            pack_creative_set = set(str(cid) for cid in pack_creative_ids if cid)

            if pack_creative_set == target_creative_set:
                pack_id = str(pack.get("id", ""))
                pack_name = pack.get("name", "")
                return (pack_id, pack_name)

        return (None, None)
    except Exception as e:
        logger.warning(f"Could not check existing pack by creatives: {e}")
        return (None, None)

# --------------------------------------------------------------------
# Cached lookups — fetch once, reuse in loop
# --------------------------------------------------------------------
def _fetch_all_creatives_map(org_id: str, title_id: str) -> Dict[str, str]:
    """
    Fetch ALL creatives for this app once and return {name: id} dict.
    Replaces per-video _check_existing_creative calls.
    """
    try:
        path = f"organizations/{org_id}/apps/{title_id}/creatives"
        meta = _unity_get(path, params={"limit": 100})

        items: list = []
        if isinstance(meta, list):
            items = meta
        elif isinstance(meta, dict):
            items = meta.get("items") or meta.get("data") or []

        result: Dict[str, str] = {}
        for cr in items:
            name = cr.get("name", "")
            cid = cr.get("id", "")
            if name and cid:
                result[name] = str(cid)
        logger.info(f"[Unity Cache] Fetched {len(result)} existing creatives for app {title_id}")
        return result
    except Exception as e:
        logger.warning(f"Could not fetch creatives map: {e}")
        return {}


def _fetch_all_packs_map(org_id: str, title_id: str) -> tuple[Dict[str, str], Dict[str, tuple[str, str]]]:
    """
    Fetch ALL creative packs for this app once and return:
      - name_map: {pack_name_lower: pack_id}
      - creatives_map: {frozenset_of_creative_ids_str: (pack_id, pack_name)}
    Replaces per-pack _check_existing_pack + _check_existing_pack_by_creatives calls.
    """
    try:
        path = f"organizations/{org_id}/apps/{title_id}/creative-packs"
        all_items: list = []
        offset = 0
        limit = 100

        while True:
            meta = _unity_get(path, params={"limit": limit, "offset": offset})
            items: list = []
            if isinstance(meta, list):
                items = meta
            elif isinstance(meta, dict):
                items = meta.get("items") or meta.get("data") or []
            if not items:
                break
            all_items.extend(items)
            if len(items) < limit:
                break
            offset += limit

        name_map: Dict[str, str] = {}
        creatives_map: Dict[str, tuple[str, str]] = {}

        for pack in all_items:
            pid = str(pack.get("id", ""))
            pname = pack.get("name", "")
            if pname and pid:
                name_map[pname.strip().lower()] = pid

            cids = pack.get("creativeIds") or pack.get("creative_ids") or []
            key = ",".join(sorted(str(c) for c in cids if c))
            if key and pid:
                creatives_map[key] = (pid, pname)

        logger.info(f"[Unity Cache] Fetched {len(name_map)} existing packs for app {title_id}")
        return name_map, creatives_map
    except Exception as e:
        logger.warning(f"Could not fetch packs map: {e}")
        return {}, {}
# --------------------------------------------------------------------
# Dry Run / Preview Functions
# --------------------------------------------------------------------
def preview_unity_upload(
    *,
    game: str,
    videos: List[Dict[str, Any]],
    settings: Dict[str, Any],
    is_marketer: bool = False
) -> Dict[str, Any]:
    """
    Preview what would happen if Unity upload is executed.
    Returns a dict with preview information without actually uploading.
    """
    # Get title_id: Test Mode는 campaign_set_id(플랫폼별), Marketer Mode는 플랫폼별 app_id 사용
    title_id = (settings.get("title_id") or "").strip()
    if not title_id:
        if is_marketer:
            # Marketer Mode: 플랫폼에 따라 app_id 사용
            platform = settings.get("platform", "aos")
            try:
                title_id = get_unity_app_id(game, platform)
            except Exception as e:
                logger.warning(f"Failed to get app ID for {game} ({platform}): {e}")
                # Fallback: UNITY_GAME_IDS 사용
                title_id = str(UNITY_GAME_IDS.get(game, ""))
        else:
            # Test Mode: 선택한 플랫폼의 campaign_set_id 사용
            platform = settings.get("platform", "aos")
            try:
                title_id = get_unity_campaign_set_id(game, platform)
            except Exception as e:
                logger.warning(f"Failed to get campaign set ID for {game} ({platform}): {e}")
                raise RuntimeError(f"Missing title_id for {game} ({platform}). Please set it in Unity settings.")
    
    campaign_id = (settings.get("campaign_id") or "").strip()
    if not campaign_id:
        # Test Mode: 플랫폼별 campaign_ids 사용 (prefix에 따라 VN/기본 선택)
        _pfx = settings.get("prefix", "")
        _cids_all = _get_campaign_ids_all_for_prefix(_pfx)
        _cids = _get_campaign_ids_for_prefix(_pfx)
        if not is_marketer:
            platform = settings.get("platform", "aos")
            if game in _cids_all:
                platform_campaign_ids = _cids_all[game].get(platform, [])
                if platform_campaign_ids:
                    campaign_id = str(platform_campaign_ids[0])

        # Fallback: 기본 campaign_ids
        if not campaign_id:
            ids_for_game = _cids.get(game) or []
            if ids_for_game:
                campaign_id = str(ids_for_game[0])

        # VN: 캠페인 ID 없으면 에러
        if not campaign_id and _pfx == "vn":
            raise RuntimeError(f"No Vietnam creative campaign live for {game}")

    org_id = (settings.get("org_id") or "").strip() or UNITY_ORG_ID_DEFAULT

    if not all([title_id, campaign_id, org_id]):
        missing = []
        if not title_id:
            missing.append("title_id")
        if not campaign_id:
            missing.append("campaign_id")
        if not org_id:
            missing.append("org_id")
        raise RuntimeError(f"Unity Settings Missing for preview. Missing: {', '.join(missing)}")
    
    # Get playable info
    playable_name = settings.get("selected_playable") or ""
    existing_playable_id = settings.get("existing_playable_id") or ""
    existing_playable_label = settings.get("existing_playable_label", "")
    
    # Group videos by base name
    subjects: dict[str, list[dict]] = {}
    for v in videos or []:
        n = v.get("name") or ""
        if "playable" in n.lower():
            continue
        base = n.split("_")[0]
        subjects.setdefault(base, []).append(v)
    
    # Generate preview pack names
    preview_packs = []
    for base, items in subjects.items():
        portrait = next((x for x in items if "1080x1920" in (x.get("name") or "")), None)
        landscape = next((x for x in items if "1920x1080" in (x.get("name") or "")), None)
        
        if not portrait or not landscape:
            continue
        
        # Generate pack name
        video_part = _extract_video_part_from_base(base)
        raw_p_name = playable_name if playable_name else existing_playable_label
        playable_part = _clean_playable_name_for_pack(raw_p_name)
        
        if playable_part:
            final_pack_name = f"{video_part}_{playable_part}"
        else:
            final_pack_name = f"{video_part}_playable"
        
        preview_packs.append({
            "pack_name": final_pack_name,
            "portrait_video": portrait.get("name"),
            "landscape_video": landscape.get("name"),
            "playable": playable_name or existing_playable_label or "(No playable selected)",
        })
    
    # Check currently assigned creative packs
    try:
        assigned_packs = _unity_list_assigned_creative_packs(
            org_id=org_id,
            title_id=title_id,
            campaign_id=campaign_id
        )
        current_assigned = [
            {
                "id": pack.get("id") or pack.get("assignedCreativePackId"),
                "name": pack.get("name", "Unknown"),
            }
            for pack in assigned_packs
        ]
    except Exception as e:
        logger.warning(f"Could not fetch assigned packs: {e}")
        current_assigned = []
    
    return {
        "game": game,
        "org_id": org_id,
        "title_id": title_id,
        "campaign_id": campaign_id,
        "total_packs_to_create": len(preview_packs),
        "preview_packs": preview_packs,
        "current_assigned_packs": current_assigned,
        "playable_info": {
            "selected_playable": playable_name,
            "existing_playable_id": existing_playable_id,
            "existing_playable_label": existing_playable_label,
        },
        "action_summary": {
            "will_create_packs": len(preview_packs),
            "will_unassign_existing": 0 if is_marketer else len(current_assigned),
            "will_assign_new": len(preview_packs),
            "is_marketer_mode": is_marketer,
        }
    }


def _unity_filter_video_files_for_pack(videos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Marketer/uni.py와 동일: mp4 비디오만 (playable/html 제외)."""
    return [
        v
        for v in (videos or [])
        if not (
            "playable" in (v.get("name") or "").lower()
            or (v.get("name") or "").lower().endswith(".html")
        )
        and (v.get("name") or "").lower().endswith(".mp4")
    ]


def _unity_filter_playable_files_for_pack(videos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        v
        for v in (videos or [])
        if "playable" in (v.get("name") or "").lower()
        or (v.get("name") or "").lower().endswith(".html")
    ]


def _unity_count_valid_video_pairs(videos: List[Dict[str, Any]]) -> int:
    """세로+가로 쌍이 맞는 주제 수 (upload_unity_creatives_to_campaign과 동일 규칙)."""
    video_files = _unity_filter_video_files_for_pack(videos)
    subjects: dict[str, list[dict]] = {}
    for v in video_files:
        base = (v.get("name") or "").split("_")[0]
        subjects.setdefault(base, []).append(v)
    n = 0
    for items in subjects.values():
        portrait = next((x for x in items if "1080x1920" in (x.get("name") or "")), None)
        landscape = next((x for x in items if "1920x1080" in (x.get("name") or "")), None)
        if portrait and landscape:
            n += 1
    return n


def _unity_apply_pack_counts_per_task(
    settings: Dict[str, Any], creative_pack_ids: Any
) -> List[int]:
    """마케터 멀티: (플랫폼, 캠페인) 태스크별 assign할 팩 개수."""
    platforms = settings.get("platforms") or []
    packs_per_campaign = settings.get("packs_per_campaign") or {}
    pack_ids_by_platform = creative_pack_ids if isinstance(creative_pack_ids, dict) else {}
    flat = creative_pack_ids if isinstance(creative_pack_ids, list) else []
    counts: List[int] = []
    for plat in platforms:
        plat_settings = settings.get(plat) or {}
        for cid in plat_settings.get("campaign_ids") or []:
            key = f"{plat}_{cid}"
            if key in packs_per_campaign:
                n = len(packs_per_campaign[key].get("pack_ids") or [])
            else:
                n = len(pack_ids_by_platform.get(plat) or flat)
            if n > 0:
                counts.append(n)
    return counts


def estimate_unity_create_api_calls(
    videos: List[Dict[str, Any]],
    *,
    settings: Dict[str, Any],
    pack_list_pages_guess: int = 1,
    is_marketer: bool = False,
) -> Dict[str, Any]:
    """
    '크리에이티브/팩 생성' 1회 클릭 시 Unity Advertise API로 나갈 수 있는
    HTTP 요청 수의 보수적 상한(대략치)을 추정한다.

    - GET/POST를 모두 1회 요청으로 친다.
    - 재개(resume)·이미 존재하는 크리에이티브/팩 스킵, 429 재시도는 반영하지 않는다.
    - 팩 목록은 offset 페이지마다 GET이므로 pack_list_pages_guess로 상한을 키운다.
    """
    warnings: List[str] = []
    P = max(1, int(pack_list_pages_guess))

    video_files = _unity_filter_video_files_for_pack(videos)
    playable_files = _unity_filter_playable_files_for_pack(videos)

    if video_files:
        pack_mode = "video_playable"
    elif playable_files:
        pack_mode = "playable_only"
    else:
        return {
            "pack_mode": "none",
            "platform_runs": 0,
            "get_upper": 0,
            "post_upper": 0,
            "total_upper": 0,
            "warnings": ["업로드할 비디오/Playable 파일이 없습니다."],
        }

    platforms = list(settings.get("platforms") or [])
    if platforms:
        run_plats = [p for p in platforms if settings.get(p, {}).get("campaign_ids")]
        if not run_plats:
            warnings.append("선택된 캠페인이 없으면 플랫폼별 업로드가 실행되지 않을 수 있습니다.")
    else:
        run_plats = ["__single__"]

    get_upper = 0
    post_upper = 0

    if pack_mode == "video_playable":
        n_pairs = _unity_count_valid_video_pairs(video_files)
        if n_pairs == 0:
            warnings.append("세로(1080x1920)+가로(1920x1080) 쌍이 없어 비디오 팩이 생성되지 않을 수 있습니다.")

        def video_playable_upper_for_merged(merged: Dict[str, Any]) -> tuple[int, int]:
            g = 1 + P  # creatives map 1회 + creative-packs 목록 페이지
            p = 3 * n_pairs  # 비디오 2 + 팩 1 (전부 신규 가정)
            ex = (merged.get("existing_playable_id") or "").strip()
            sel = (merged.get("selected_playable") or "").strip()
            if ex:
                g += 1  # _unity_get_creative 검증
            elif sel and any((v.get("name") or "") == sel for v in (videos or [])):
                g += 2  # _check_existing_creative + 검증 GET
                p += 1  # 신규 playable POST
            else:
                g += 1
                warnings.append("Playable이 선택되지 않으면 업로드가 중단될 수 있습니다.")
            return g, p

        if platforms:
            for plat in run_plats:
                ps = settings.get(plat) or {}
                merged = {**settings, **ps, "platform": plat}
                gi, pi = video_playable_upper_for_merged(merged)
                get_upper += gi
                post_upper += pi
        else:
            merged = {**settings, "is_marketer_mode": is_marketer}
            get_upper, post_upper = video_playable_upper_for_merged(merged)

    else:
        n_play = len(playable_files)
        # playable_only: 파일당 기존 조회(크리에이티브 1 + 팩 목록 P) + 생성 시 POST 2
        per_plat_get = n_play * (1 + P)
        per_plat_post = n_play * 2
        if platforms:
            for _plat in run_plats:
                get_upper += per_plat_get
                post_upper += per_plat_post
        else:
            get_upper = per_plat_get
            post_upper = per_plat_post

    platform_runs = len(run_plats) if platforms else 1
    total_upper = get_upper + post_upper

    return {
        "pack_mode": pack_mode,
        "platform_runs": platform_runs,
        "get_upper": get_upper,
        "post_upper": post_upper,
        "total_upper": total_upper,
        "warnings": warnings,
    }


def estimate_unity_apply_api_calls(
    settings: Dict[str, Any],
    creative_pack_ids: Any,
    *,
    is_marketer: bool,
    test_unassign_wag: int = 35,
) -> Dict[str, Any]:
    """
    '캠페인에 적용' 1회 클릭 시 나갈 수 있는 요청 수 상한(대략치).

    마케터 멀티: 캠페인마다 할당 목록 1회 GET + 신규 assign마다 1회 POST(상한).
    테스트 단일: 기존 언어사인 루프는 캠페인 상태에 따라 달라 test_unassign_wag로 잡는다.
    """
    warnings: List[str] = []
    platforms = settings.get("platforms") or []

    if platforms:
        counts = _unity_apply_pack_counts_per_task(settings, creative_pack_ids)
        if not counts:
            return {
                "get_upper": 0,
                "post_upper": 0,
                "total_upper": 0,
                "warnings": ["적용할 팩이 선택되지 않았을 수 있습니다."],
            }
        get_u = len(counts)
        post_u = sum(counts)
        return {
            "get_upper": get_u,
            "post_upper": post_u,
            "total_upper": get_u + post_u,
            "warnings": warnings,
        }

    if isinstance(creative_pack_ids, dict):
        flat: List[str] = []
        for v in creative_pack_ids.values():
            if isinstance(v, list):
                flat.extend(str(x) for x in v if x)
            elif v:
                flat.append(str(v))
    else:
        flat = [str(x) for x in (creative_pack_ids or []) if x] if isinstance(creative_pack_ids, list) else []
    n_new = len(flat)
    if is_marketer:
        return {
            "get_upper": 1,
            "post_upper": n_new,
            "total_upper": 1 + n_new,
            "warnings": warnings,
        }

    # Test mode: 목록 조회 + (언어사인 다회) + 신규 assign
    warnings.append(
        f"테스트 모드는 기존 할당 해제 횟수에 따라 추가 요청이 클 수 있어, "
        f"해제 쪽을 대략 +{test_unassign_wag}회로 잡았습니다."
    )
    return {
        "get_upper": 1 + test_unassign_wag,
        "post_upper": n_new,
        "total_upper": 1 + test_unassign_wag + n_new,
        "warnings": warnings,
    }

# --------------------------------------------------------------------
# Main Helpers
# --------------------------------------------------------------------

@_unity_http_op_summary_log("upload_unity_creatives_to_campaign")
def upload_unity_creatives_to_campaign(
    *, 
    game: str, 
    videos: List[Dict[str, Any]], 
    settings: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Upload Unity creatives and packs with resume support.
    
    If upload fails midway, calling again will:
    - Skip already uploaded creatives
    - Only upload missing items
    - Resume from where it left off
    """
    # Get title_id: Test Mode는 campaign_set_id(플랫폼별), Marketer Mode는 플랫폼별 app_id 사용
    title_id = (settings.get("title_id") or "").strip()
    if not title_id:
        # settings에서 platform 정보 가져오기
        platform = settings.get("platform", "aos")
        # is_marketer_mode가 명시적으로 설정되어 있으면 그에 따라 처리
        is_marketer = settings.get("is_marketer_mode", False)
        
        if is_marketer:
            # Marketer Mode: 플랫폼에 따라 app_id 사용
            try:
                title_id = get_unity_app_id(game, platform)
            except Exception as e:
                logger.warning(f"Failed to get app ID for {game} ({platform}): {e}")
                # Fallback: UNITY_GAME_IDS 사용
                title_id = str(UNITY_GAME_IDS.get(game, ""))
        else:
            # Test Mode: 선택한 플랫폼의 campaign_set_id 사용
            try:
                title_id = get_unity_campaign_set_id(game, platform)
            except Exception as e:
                logger.warning(f"Failed to get campaign set ID for {game} ({platform}): {e}")
                raise RuntimeError(f"Missing title_id for {game} ({platform}). Please set it in Unity settings.")
    
    campaign_id = (settings.get("campaign_id") or "").strip()
    if not campaign_id:
        # Test Mode: 플랫폼별 campaign_ids 사용 (prefix에 따라 VN/기본 선택)
        platform = settings.get("platform", "aos")
        is_marketer = settings.get("is_marketer_mode", False)
        _pfx = settings.get("prefix", "")
        _cids_all = _get_campaign_ids_all_for_prefix(_pfx)
        _cids = _get_campaign_ids_for_prefix(_pfx)

        if not is_marketer and game in _cids_all:
            platform_campaign_ids = _cids_all[game].get(platform, [])
            if platform_campaign_ids:
                campaign_id = str(platform_campaign_ids[0])

        # Fallback: 기본 campaign_ids
        if not campaign_id:
            ids_for_game = _cids.get(game) or []
            if ids_for_game:
                campaign_id = str(ids_for_game[0])

        # VN: 캠페인 ID 없으면 에러
        if not campaign_id and _pfx == "vn":
            raise RuntimeError(f"No Vietnam creative campaign live for {game}")

    org_id = (settings.get("org_id") or "").strip() or UNITY_ORG_ID_DEFAULT
    
    if not all([title_id, campaign_id, org_id]):
        missing = []
        if not title_id:
            missing.append("title_id (app ID)")
        if not campaign_id:
            missing.append("campaign_id")
        if not org_id:
            missing.append("org_id")
        raise RuntimeError(f"Unity Settings Missing for upload. Missing: {', '.join(missing)}")
    
    language = (settings.get("language") or "en").strip()
    upload_context = {
        "org_id": org_id,
        "title_id": title_id,
        "campaign_id": campaign_id,
        "language": language,
        "platform": settings.get("platform", "aos"),
        "title_id_source": settings.get("title_id_source", ""),
    }

    logger.info(f"Unity upload - org_id={org_id}, title_id={title_id}, campaign_id={campaign_id}, language={language}")
    st.caption(
        f"Unity 대상 컨텍스트: org={org_id}, title_id={title_id}, campaign_id={campaign_id}, platform={upload_context['platform']}"
    )
    if upload_context.get("title_id_source") == "campaign_set":
        st.warning(
            "현재 Test Mode로 동작 중이며 `title_id`에 campaign set ID를 사용합니다. "
            "Unity 콘솔의 일반 앱 소재 리스트에서 바로 보이지 않을 수 있으니, "
            "동일 캠페인 컨텍스트에서 확인하세요."
        )
        logger.info(
            "[Unity context] title_id_source=campaign_set org_id=%s title_id=%s campaign_id=%s platform=%s",
            org_id,
            title_id,
            campaign_id,
            upload_context["platform"],
        )

    # Unity Ads creative limit per app (typically very high, but check for safety)
    # Unity Ads creative pack limit per campaign (typically 50)
    UNITY_CREATIVE_LIMIT = 1000  # Creative limit is usually very high
    UNITY_CREATIVE_PACK_LIMIT = 50  # Creative pack limit per campaign
    
    # Check creative capacity (optional, as limit is usually very high)
    # This check is mainly for API error handling
    try:
        # Estimate how many creatives will be created
        # Each video pair (portrait + landscape) = 2 creatives, plus 1 playable per pack
        # For simplicity, we'll check during creation and handle API errors
        pass  # Creative limit check skipped as Unity Ads creative limit is usually very high
    except Exception as e:
        logger.warning(f"Could not check creative capacity: {e}")

    start_iso = next_sat_0000_kst()
    errors: List[str] = []
    created_pack_records: List[Dict[str, str]] = []
    created_new_pack_count = 0
    reused_existing_pack_count = 0
    created_new_video_creative_count = 0
    reused_existing_video_creative_count = 0
    created_new_playable_creative_count = 0
    reused_existing_playable_creative_count = 0

    # Pre-fetch existing creatives & packs (1-2 API calls instead of N per pair)
    st.info("📋 기존 creative/pack 목록 조회 중...")
    _creative_cache = _fetch_all_creatives_map(org_id, title_id)
    _pack_name_cache, _pack_creatives_cache = _fetch_all_packs_map(org_id, title_id)

    # Initialize upload state (loads existing if resuming)
    upload_state = _init_upload_state(game, campaign_id, videos)
    
    # Show resume info if applicable
    existing_packs = len([p for p in upload_state["completed_packs"] if p])
    if existing_packs > 0:
        st.info(
            f"📦 **Resuming Upload**\n\n"
            f"Found {existing_packs} previously created pack(s).\n"
            f"Will skip already uploaded items and continue from where we left off."
        )

    # ========================================
    # 1. PLAYABLE HANDLING
    # ========================================
    playable_name = settings.get("selected_playable") or ""
    existing_playable_id = settings.get("existing_playable_id") or ""
    playable_creative_id: str | None = upload_state.get("playable_creative")

    if not playable_creative_id:
        if playable_name:
            playable_item = next((v for v in (videos or []) if v.get("name") == playable_name), None)
            if playable_item:
                try:
                    # Check if already uploaded
                    playable_creative_id = _check_existing_creative(org_id, title_id, playable_name)
                    
                    if playable_creative_id:
                        reused_existing_playable_creative_count += 1
                        st.info(f"✅ Found existing playable: {playable_name}")
                    else:
                        st.info(f"⬆️ Uploading playable: {playable_name}")
                        playable_creative_id = _unity_create_playable_creative(
                            org_id=org_id,
                            title_id=title_id,
                            playable_path=playable_item["path"],
                            name=playable_name,
                            language=language
                        )
                        created_new_playable_creative_count += 1
                    
                    upload_state["playable_creative"] = playable_creative_id
                    _save_upload_state(game, campaign_id, upload_state)
                    
                except Exception as e:
                    errors.append(f"Playable creation failed: {e}")
                    playable_creative_id = None

        if not playable_creative_id and existing_playable_id:
            playable_creative_id = str(existing_playable_id)
            upload_state["playable_creative"] = playable_creative_id
            _save_upload_state(game, campaign_id, upload_state)

    # Validate Playable
    if playable_creative_id:
        try:
            p_details = _unity_get_creative(org_id=org_id, title_id=title_id, creative_id=playable_creative_id)
            p_type = (p_details.get("type") or "").lower()
            
            if "playable" not in p_type and "cpe" not in p_type:
                error_msg = f"CRITICAL: Playable ID ({playable_creative_id}) is type '{p_type}'. Must be 'playable'."
                errors.append(error_msg)
                return {
                    "game": game,
                    "campaign_id": campaign_id,
                    "errors": errors,
                    "creative_ids": upload_state["completed_packs"],
                    "upload_context": upload_context,
                    "created_pack_records": created_pack_records,
                }
        except Exception as e:
            errors.append(f"Could not validate Playable ID: {e}")
            return {
                "game": game,
                "campaign_id": campaign_id,
                "errors": errors,
                "creative_ids": upload_state["completed_packs"],
                "upload_context": upload_context,
                "created_pack_records": created_pack_records,
            }
    else:
        errors.append("No Playable End Card selected.")
        return {
            "game": game,
            "campaign_id": campaign_id,
            "errors": errors,
            "creative_ids": upload_state["completed_packs"],
            "upload_context": upload_context,
            "created_pack_records": created_pack_records,
        }

    # ========================================
    # 2. VIDEO PAIRING
    # ========================================
    subjects: dict[str, list[dict]] = {}
    for v in videos or []:
        n = v.get("name") or ""
        if "playable" in n.lower():
            continue
        base = n.split("_")[0]
        subjects.setdefault(base, []).append(v)

    total_pairs = len(subjects)
    upload_state["total_expected"] = total_pairs
    _save_upload_state(game, campaign_id, upload_state)
    
    if total_pairs == 0:
        st.warning("No video pairs found to upload.")
        return {
            "game": game,
            "campaign_id": campaign_id,
            "errors": errors,
            "creative_ids": upload_state["completed_packs"],
            "upload_context": upload_context,
            "created_pack_records": created_pack_records,
        }

    processed_count = 0
    progress_bar = st.progress(0, text=f"Starting upload... (0/{total_pairs})")
    # Batch upload controls (기본값: 8개씩 처리)
    batch_size = max(1, int(settings.get("upload_batch_size", 8)))
    batch_cooldown_seconds = max(0, int(settings.get("upload_batch_cooldown_seconds", 1)))
    
    # Status container for real-time updates
    status_container = st.empty()
    _set_unity_progress_hook(lambda m: status_container.info(m))

    # ========================================
    # 3. PROCESSING LOOP (BATCHED)
    # ========================================
    subject_items = list(subjects.items())
    batches = [
        subject_items[i : i + batch_size]
        for i in range(0, len(subject_items), batch_size)
    ]
    total_batches = len(batches)
    logger.info(
        "[Unity batch] start game=%s total_pairs=%s batch_size=%s total_batches=%s cooldown=%ss",
        game,
        total_pairs,
        batch_size,
        total_batches,
        batch_cooldown_seconds,
    )

    should_stop = False
    for batch_idx, batch_items in enumerate(batches, start=1):
        st.info(
            f"📦 Unity 배치 {batch_idx}/{total_batches} 시작 "
            f"({len(batch_items)} pairs, created {len(upload_state['completed_packs'])}/{total_pairs})"
        )

        for base, items in batch_items:
            portrait = next((x for x in items if "1080x1920" in (x.get("name") or "")), None)
            landscape = next((x for x in items if "1920x1080" in (x.get("name") or "")), None)

            if not portrait or not landscape:
                errors.append(f"{base}: Missing Portrait or Landscape video.")
                processed_count += 1
                progress_bar.progress(
                    int(processed_count / total_pairs * 100),
                    text=f"❌ Skipped {base} (Missing videos) - {processed_count}/{total_pairs}"
                )
                continue
        
            # Generate pack name
            # Extract video part (e.g., "video001")
            video_part = _extract_video_part_from_base(base)
        
            # Get playable name or label
            raw_p_name = playable_name if playable_name else settings.get("existing_playable_label", "")
        
            # Clean playable name according to rules
            playable_part = _clean_playable_name_for_pack(raw_p_name)
        
            # Final pack name: videoxxx_playable003escalater감옥 (underscore between video and playable)
            if playable_part:
                final_pack_name = f"{video_part}_{playable_part}"
            else:
                # Fallback if no playable name
                final_pack_name = f"{video_part}_playable"
        
            # Check if pack already exists
            if final_pack_name in upload_state["creative_packs"] and upload_state["creative_packs"][final_pack_name]:
                pack_id = upload_state["creative_packs"][final_pack_name]
                if pack_id not in upload_state["completed_packs"]:
                    upload_state["completed_packs"].append(pack_id)
                    _save_upload_state(game, campaign_id, upload_state)
                
                processed_count += 1
                progress_bar.progress(
                    int(processed_count / total_pairs * 100),
                    text=f"✅ Already uploaded: {base} - {processed_count}/{total_pairs}"
                )
                status_container.success(f"✅ Skipped (already exists): {final_pack_name}")
                continue

            try:
                progress_bar.progress(
                    int(processed_count / total_pairs * 100),
                    text=f"⬆️ Uploading {base} ({processed_count + 1}/{total_pairs})..."
                )
                
                # Upload portrait video (check cache first, then upload)
                p_id = upload_state["video_creatives"].get(portrait["name"])
                if not p_id:
                    p_id = _creative_cache.get(portrait["name"])

                if not p_id:
                    status_container.info(f"⬆️ Uploading portrait: {portrait['name']}")
                    p_id = _unity_create_video_creative(
                        org_id=org_id,
                        title_id=title_id,
                        video_path=portrait["path"],
                        name=portrait["name"],
                        language=language
                    )
                    created_new_video_creative_count += 1
                    _creative_cache[portrait["name"]] = p_id  # update cache
                    upload_state["video_creatives"][portrait["name"]] = p_id
                    _save_upload_state(game, campaign_id, upload_state)
                    time.sleep(2)
                else:
                    reused_existing_video_creative_count += 1
                    status_container.success(f"✅ Found existing: {portrait['name']}")

                # Upload landscape video (check cache first, then upload)
                l_id = upload_state["video_creatives"].get(landscape["name"])
                if not l_id:
                    l_id = _creative_cache.get(landscape["name"])

                if not l_id:
                    status_container.info(f"⬆️ Uploading landscape: {landscape['name']}")
                    l_id = _unity_create_video_creative(
                        org_id=org_id,
                        title_id=title_id,
                        video_path=landscape["path"],
                        name=landscape["name"],
                        language=language
                    )
                    created_new_video_creative_count += 1
                    _creative_cache[landscape["name"]] = l_id  # update cache
                    upload_state["video_creatives"][landscape["name"]] = l_id
                    _save_upload_state(game, campaign_id, upload_state)
                    time.sleep(2)
                else:
                    reused_existing_video_creative_count += 1
                    status_container.success(f"✅ Found existing: {landscape['name']}")

                pack_creatives = [p_id, l_id, playable_creative_id]

                # ✅ Check cache instead of API calls
                pack_id = _pack_name_cache.get(final_pack_name.strip().lower())
                existing_pack_name = final_pack_name

                if pack_id:
                    reused_existing_pack_count += 1
                    status_container.warning(
                        f"⚠️ **Creative pack already exists with same name:**\n\n"
                        f"   - Pack Name: `{final_pack_name}`\n"
                        f"   - Pack ID: `{pack_id}`\n"
                        f"   - Skipping creation...\n"
                    )
                    logger.info(f"Skipping pack creation for {final_pack_name} - already exists ({pack_id})")
                else:
                    # Check by creative IDs combination (from cache)
                    cids_key = ",".join(sorted(str(c) for c in pack_creatives if c))
                    cached_match = _pack_creatives_cache.get(cids_key)
                    if cached_match:
                        reused_existing_pack_count += 1
                        pack_id, existing_pack_name = cached_match
                        status_container.warning(
                            f"⚠️ **Creative pack already exists** with same video + playable combination:\n\n"
                            f"   - Existing Pack Name: `{existing_pack_name}`\n"
                            f"   - Existing Pack ID: `{pack_id}`\n"
                            f"   - Skipping upload for: `{final_pack_name}`\n\n"
                            f"   Continuing with remaining uploads..."
                        )
                        logger.info(f"Skipping pack creation for {final_pack_name} - already exists as {existing_pack_name} ({pack_id})")
                
                # ✅ pack_id가 있으면 생성하지 않고 기존 팩 사용
                if not pack_id:
                    status_container.info(f"📦 Creating pack: {final_pack_name}")
                    logger.info(f"Creating pack with org_id={org_id}, title_id={title_id}, pack_name={final_pack_name}, creative_ids={pack_creatives}")
                    pack_id = _unity_create_creative_pack(
                        org_id=org_id,
                        title_id=title_id,
                        pack_name=final_pack_name,
                        creative_ids=pack_creatives,
                        pack_type="video+playable"
                    )
                    logger.info(f"✅ Created pack with ID: {pack_id}")
                    created_new_pack_count += 1
                    # Update caches with newly created pack
                    _pack_name_cache[final_pack_name.strip().lower()] = pack_id
                    cids_key = ",".join(sorted(str(c) for c in pack_creatives if c))
                    _pack_creatives_cache[cids_key] = (pack_id, final_pack_name)
                    logger.info(
                        "[Unity pack created] org_id=%s title_id=%s campaign_id=%s pack_id=%s pack_name=%s",
                        org_id,
                        title_id,
                        campaign_id,
                        pack_id,
                        final_pack_name,
                    )
                    time.sleep(2)
                else:
                    # ✅ 기존 팩이 있으면 명확하게 표시
                    if existing_pack_name != final_pack_name:
                        status_container.success(f"✅ Found existing pack with same video + playable: `{existing_pack_name}`")
                    else:
                        status_container.success(f"✅ Found existing pack with same name: `{final_pack_name}`")
                
                upload_state["creative_packs"][final_pack_name] = pack_id
                upload_state["completed_packs"].append(pack_id)
                created_pack_records.append(
                    {
                        "pack_id": str(pack_id),
                        "pack_name": final_pack_name,
                    }
                )
                _save_upload_state(game, campaign_id, upload_state)
                
                status_container.success(f"✅ Completed: {final_pack_name}")
                time.sleep(0.5)

            except Exception as e:
                msg = str(e)
                msg_lower = msg.lower()
                logger.error(f"[Unity Error] PACK LOOP FAILED | base={base} | error={msg}")

                # 1) Capacity/quota limit (not rate limit — won't resolve with retry)
                is_capacity = any(kw in msg_lower for kw in ["최대", "capacity", "full", "maximum"])
                # 2) Rate limit (429 — may resolve with time)
                is_rate_limit = "429" in msg or "Quota Exceeded" in msg

                if is_capacity:
                    errors.append(f"🚫 Creative/Pack 용량 초과 at {base}: {msg[:200]}")
                    status_container.error(
                        f"🚫 **Creative/Pack 용량 초과**\n\n"
                        f"Unity API 응답: `{msg[:300]}`\n\n"
                        f"사용하지 않는 creative/pack을 삭제한 후 다시 시도해주세요.\n"
                        f"Progress saved: {len(upload_state['completed_packs'])}/{total_pairs} packs."
                    )
                    should_stop = True
                    break
                elif is_rate_limit:
                    errors.append(f"⚠️ Rate limit at {base}: {msg[:200]}")
                    retry_after_s = _extract_retry_after_from_error_text(msg)
                    retry_hint = ""
                    if retry_after_s is not None:
                        kst = timezone(timedelta(hours=9))
                        retry_at_kst = datetime.now(kst) + timedelta(seconds=retry_after_s)
                        retry_hint = (
                            f"\n예상 재시도 가능 시간: 약 {retry_after_s}초 후 "
                            f"({retry_at_kst.strftime('%Y-%m-%d %H:%M:%S')} KST)"
                        )
                    status_container.error(
                        f"⚠️ **API Rate Limit**\n\n"
                        f"Unity API 응답: `{msg[:300]}`\n\n"
                        f"Progress saved: {len(upload_state['completed_packs'])}/{total_pairs} packs.\n"
                        f"{retry_hint}\n"
                        f"Click '크리에이티브/팩 생성' again to resume."
                    )
                    should_stop = True
                    break
                else:
                    logger.exception(f"Unity pack creation failed for {base}")
                    errors.append(f"{base}: {msg}")
                    status_container.error(f"❌ Failed: {base} - {msg[:300]}")

            finally:
                processed_count += 1
                pct = int(processed_count / total_pairs * 100)
                completed = len(upload_state["completed_packs"])
                progress_bar.progress(
                    pct,
                    text=f"Batch {batch_idx}/{total_batches} | Processing: {processed_count}/{total_pairs} pairs | Created: {completed}/{total_pairs} packs",
                )

        if should_stop:
            break

        # 배치 사이 쿨다운 (마지막 배치는 제외)
        if batch_idx < total_batches and batch_cooldown_seconds > 0:
            logger.info(
                "[Unity batch] completed %s/%s, cooldown %ss before next batch (created=%s/%s)",
                batch_idx,
                total_batches,
                batch_cooldown_seconds,
                len(upload_state["completed_packs"]),
                total_pairs,
            )
            status_container.info(
                f"⏸️ Batch {batch_idx}/{total_batches} 완료. "
                f"{batch_cooldown_seconds}초 대기 후 다음 배치를 시작합니다."
            )
            time.sleep(batch_cooldown_seconds)

    progress_bar.empty()
    status_container.empty()
    _set_unity_progress_hook(None)
    
    # Final summary
    total_created = len(upload_state["completed_packs"])
    
    if total_created == total_pairs:
        st.success(
            f"🎉 **Upload Complete!**\n\n"
            f"Successfully resolved **{total_created}/{total_pairs}** creative packs.\n"
            f"(신규 생성: {created_new_pack_count}, 기존 재사용: {reused_existing_pack_count})"
        )
        # Clear state on successful completion
        _clear_upload_state(game, campaign_id)
    elif total_created > 0:
        st.warning(
            f"⚠️ **Partial Upload**\n\n"
            f"Resolved **{total_created}/{total_pairs}** creative packs.\n"
            f"(신규 생성: {created_new_pack_count}, 기존 재사용: {reused_existing_pack_count})\n"
            f"Click '크리에이티브/팩 생성' again to continue uploading remaining packs."
        )
    if total_created > 0 and created_new_pack_count == 0 and reused_existing_pack_count > 0:
        st.info(
            "이번 실행에서는 새 팩 생성 없이 기존 팩만 재사용되었습니다. "
            "동일한 video/playable 이름 조합이 이미 존재할 수 있습니다."
        )
    
    return {
        "game": game,
        "campaign_id": campaign_id,
        "start_iso": start_iso,
        "creative_ids": upload_state["completed_packs"],
        "errors": errors,
        "removed_ids": [],
        "total_created": total_created,
        "total_expected": total_pairs,
        "created_new_pack_count": created_new_pack_count,
        "reused_existing_pack_count": reused_existing_pack_count,
        "created_new_video_creative_count": created_new_video_creative_count,
        "reused_existing_video_creative_count": reused_existing_video_creative_count,
        "created_new_playable_creative_count": created_new_playable_creative_count,
        "reused_existing_playable_creative_count": reused_existing_playable_creative_count,
        "upload_context": upload_context,
        "created_pack_records": created_pack_records,
    }

@_unity_http_op_summary_log("apply_unity_creative_packs_to_campaign")
def apply_unity_creative_packs_to_campaign(*, game: str, creative_pack_ids: List[str], settings: Dict[str, Any], is_marketer: bool = False) -> Dict[str, Any]:
    # TODO(dry-run): 여기서 POST assign / Test Mode unassign 루프를 건너뛰고
    # GET assigned-creative-packs 기반 diff 또는 호출 계획만 반환. uni.apply 와 동일 플래그 공유.
    if not creative_pack_ids:
        raise RuntimeError("No creative pack IDs to apply.")

    title_id = (settings.get("title_id") or "").strip() or str(UNITY_GAME_IDS.get(game, ""))
    campaign_id = (settings.get("campaign_id") or "").strip()
    org_id = (settings.get("org_id") or "").strip() or UNITY_ORG_ID_DEFAULT

    if not all([title_id, campaign_id, org_id]):
         raise RuntimeError("Unity settings missing for apply step.")

    removed_ids: List[str] = []
    assigned_packs: List[str] = []
    errors: List[str] = []

    # --- PROGRESS BAR FOR APPLY STEP ---
    progress_bar = st.progress(0, text="Fetching existing assignments...")

    # 1. Unassign existing (Only for Test Mode, not Marketer Mode)
    if not is_marketer:
        # Test Mode: Unassign existing packs first
        max_loops = 20 # Safety limit
        loop_count = 0
        
        while loop_count < max_loops:
            try:
                assigned = _unity_list_assigned_creative_packs(org_id=org_id, title_id=title_id, campaign_id=campaign_id)
                if not assigned:
                    break
                    
                total_unassign = len(assigned)
                loop_count += 1
                
                for idx, item in enumerate(assigned):
                    assigned_id = item.get("id") or item.get("assignedCreativePackId")
                    if assigned_id:
                        # Update progress bar
                        progress_bar.progress(
                            int((idx + 1) / max(total_unassign, 1) * 50), 
                            text=f"Unassigning batch {loop_count}: {idx + 1}/{total_unassign}..."
                        )
                        
                        try:
                            _unity_unassign_with_retry(  # 새 함수 사용
                                org_id=org_id,
                                title_id=title_id,
                                campaign_id=campaign_id,
                                assigned_creative_pack_id=str(assigned_id)
                            )
                            removed_ids.append(str(assigned_id))
                            time.sleep(1.0)  # 0.2 → 1.0으로 증가
                        except Exception as e:
                            errors.append(f"Unassign error {assigned_id}: {e}")
                
                # Short sleep between pages
                time.sleep(1)
                
            except Exception as e:
                errors.append(f"List assigned error: {e}")
                break
    else:
        # Marketer Mode: Skip unassign, just show current assignments
        try:
            assigned = _unity_list_assigned_creative_packs(org_id=org_id, title_id=title_id, campaign_id=campaign_id)
            if assigned:
                st.info(f"ℹ️ Marketer Mode: {len(assigned)} existing pack(s) will remain assigned. New packs will be added.")
        except Exception as e:
            logger.warning(f"Could not fetch existing assignments: {e}")

    # 2. Assign new
    total_assign = len(creative_pack_ids)
    count_a = 0
    start_pct = 0 if is_marketer else 50  # Marketer mode starts at 0% since no unassign
    
    for pack_id in creative_pack_ids:
        count_a += 1
        pct = start_pct + int(count_a / max(total_assign, 1) * (100 - start_pct))
        progress_bar.progress(pct, text=f"Assigning new packs {count_a}/{total_assign}...")
        
        try:
            _unity_assign_creative_pack(org_id=org_id, title_id=title_id, campaign_id=campaign_id, creative_pack_id=str(pack_id))
            assigned_packs.append(str(pack_id))
            time.sleep(0.5) 
        except Exception as e:
            error_str = str(e).lower()
            # Check if error is related to capacity/limit
            if any(keyword in error_str for keyword in ["limit", "maximum", "exceeded", "full", "capacity", "quota"]):
                errors.append(f"Creative pack 개수가 최대입니다. 사용하지 않는 creative을 제거해주세요.")
            else:
                errors.append(f"Assign error {pack_id}: {e}")

    progress_bar.empty()

    return {
        "game": game,
        "campaign_id": campaign_id,
        "assigned_packs": assigned_packs,
        "removed_assignments": removed_ids,
        "errors": errors,
    }

