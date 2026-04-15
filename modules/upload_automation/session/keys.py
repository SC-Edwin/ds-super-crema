"""Creative Upload 탭에서 쓰는 `st.session_state` 키·상수.

위젯과 상태가 붙어 있어도 키 문자열은 여기서만 정의해 두면
UI(`ui/`)와 나중에 뺄 유스케이스(`application/`)가 동일 계약을 참조할 수 있습니다.
"""

# ---- 상단 모드 (Test / Marketer) ----
PAGE = "page"

PAGE_OPS_TITLE = "Creative 자동 업로드"
PAGE_MARKETER_TITLE = "Creative 자동 업로드 - 마케터"


def namespaced_key(prefix: str, name: str) -> str:
    """prefix가 있으면 `{prefix}_{name}`, 없으면 `name` (기존 `_key`와 동일 규칙)."""
    return f"{prefix}_{name}" if prefix else name
