"""
Vietnam Creative Upload Tab
베트남팀 전용 Creative Upload 모듈.
현재는 Test Mode와 동일하지만, 게임 목록/API 설정/UI를
독립적으로 커스터마이징할 수 있도록 분리해둠.
"""
from __future__ import annotations

from modules.upload_automation.main import (
    init_state,
    init_remote_state,
    render_main_app,
)
from modules.upload_automation import facebook_ads as fb_ops
from modules.upload_automation import unity_ads as uni_ops

# ── Vietnam-specific config ──────────────────────────────────────────
PREFIX = "vn"

# 게임 목록 (None = 기본 DEFAULT_GAME_NAMES 사용, 리스트 지정 시 해당 게임만 표시)
# 예: GAMES = ["XP HERO", "Dino Universe"]
GAMES = None

# Facebook / Unity 모듈 (다른 API 키가 필요하면 별도 모듈로 교체 가능)
FB_MODULE = fb_ops
UNITY_MODULE = uni_ops


# ── Entry point ──────────────────────────────────────────────────────
def run():
    """Vietnam tab entry point — called from app.py."""
    init_state(prefix=PREFIX)
    init_remote_state(prefix=PREFIX)
    FB_MODULE.init_fb_game_defaults(prefix=PREFIX)

    render_main_app(
        "Creative Upload - Vietnam",
        FB_MODULE,
        UNITY_MODULE,
        is_marketer=False,
        prefix=PREFIX,
    )
