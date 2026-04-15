from __future__ import annotations

import pathlib
from typing import Any, List


def validate_count(files: List[Any]) -> tuple[bool, str]:
    """Check there is at least one .mp4/.mpeg4/.html/.zip file (Streamlit 없음)."""
    if not files:
        return False, "Please upload at least one file (.mp4, .mpeg4, or .html)."
    allowed = {".mp4", ".mpeg4", ".html", ".zip"}
    bad = []
    for u in files:
        name = getattr(u, "name", None) or (u.get("name") if isinstance(u, dict) else None)
        if not name:
            continue
        if pathlib.Path(name).suffix.lower() not in allowed:
            bad.append(name)
    if bad:
        return False, f"Remove unsupported files: {', '.join(bad[:5])}..."
    return True, f"{len(files)} file(s) ready."
