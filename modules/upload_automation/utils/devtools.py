from __future__ import annotations

import logging
import traceback
from collections import deque
from typing import Deque

import streamlit as st

_LOG_KEY = "_dev_log_buffer"
_TB_KEY = "_dev_tracebacks"
_INIT_KEY = "_dev_log_init"


def dev_enabled() -> bool:
    """Developer mode toggle: ?dev=1 or secrets developer_mode=true."""
    try:
        qp = st.query_params.get("dev")
        if isinstance(qp, list):
            qp = qp[0] if qp else ""
        qp_flag = str(qp).strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        qp_flag = False

    secrets_flag = bool(st.secrets.get("developer_mode", False))
    return bool(qp_flag or secrets_flag)


class _StreamlitRingBufferHandler(logging.Handler):
    """Logs into st.session_state ring buffer (only when dev_enabled)."""

    def __init__(self, maxlen: int = 800) -> None:
        super().__init__()
        self.maxlen = maxlen

    def emit(self, record: logging.LogRecord) -> None:
        if not dev_enabled():
            return
        buf: Deque[str] = st.session_state.setdefault(_LOG_KEY, deque(maxlen=self.maxlen))
        buf.append(self.format(record))


def init_dev_logging() -> None:
    """Idempotently install a log handler that captures logs into session_state in dev mode."""
    if st.session_state.get(_INIT_KEY):
        return
    st.session_state[_INIT_KEY] = True

    handler = _StreamlitRingBufferHandler(maxlen=800)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))

    root = logging.getLogger()
    root.addHandler(handler)


def record_exception(context: str, exc: BaseException) -> None:
    """Always log to server; in dev mode also store traceback into session_state."""
    logging.getLogger(__name__).exception("%s: %s", context, exc)
    if not dev_enabled():
        return
    tbs: Deque[str] = st.session_state.setdefault(_TB_KEY, deque(maxlen=30))
    tbs.append(f"{context}\n{traceback.format_exc()}")


def render_dev_panel() -> None:
    """Render a developer-only expander showing recent exceptions + recent logs."""
    if not dev_enabled():
        return

    init_dev_logging()

    with st.expander("Developer Logs", expanded=False):
        tbs = list(st.session_state.get(_TB_KEY, []))
        logs = list(st.session_state.get(_LOG_KEY, []))

        if tbs:
            st.markdown("**Recent Exceptions**")
            st.code("\n\n".join(tbs[-5:]))
        else:
            st.caption("No exceptions captured yet.")

        st.markdown("**Recent Logs**")
        if logs:
            st.code("\n".join(logs[-200:]))
        else:
            st.caption("No logs captured yet.")

