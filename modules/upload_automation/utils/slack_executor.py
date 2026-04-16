"""ThreadPoolExecutor that posts uncaught worker exceptions to Slack (Bot API).

- `submit` 시점에 Streamlit 세션·쿼리 파라미터·(선택) ContextVar 메타를 스냅샷해 워커에 전달합니다.
- 워커 스레드에서 `st.session_state`를 읽지 않아도 동일 사용자/플랫폼 맥락을 알림에 넣을 수 있습니다.

secrets.toml (둘 다 있을 때만 전송):

    [slack]
    bot_token = "xoxb-..."
    channel_id = "C0123ABCD"

선택 — 코드에서 임의 필드 덮어쓰기(예: 광고 계정, 네트워크 라벨):

    from modules.upload_automation.utils.slack_executor import slack_alarm_extras

    with slack_alarm_extras(ad_network="Meta", ad_account_id="act_123", customer_id="123-456-7890"):
        ...  # 이 구간에서 submit되는 작업에 메타가 합쳐짐
"""
from __future__ import annotations

import contextlib
import contextvars
import functools
import json
import traceback
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Iterator, Tuple

_SLACK_POST_MESSAGE = "https://slack.com/api/chat.postMessage"

# submit 시점에 merge되어 스냅샷에 포함됨 (중첩 with 지원)
_slack_alarm_extras: contextvars.ContextVar[Dict[str, Any] | None] = contextvars.ContextVar(
    "slack_alarm_extras", default=None
)


@contextlib.contextmanager
def slack_alarm_extras(**kwargs: Any) -> Iterator[None]:
    """Slack 알림에 포함할 추가 컨텍스트(네트워크·계정 ID 등). 중첩 시 shallow merge."""
    prev = _slack_alarm_extras.get()
    merged: Dict[str, Any] = {**(prev or {}), **kwargs}
    token = _slack_alarm_extras.set(merged)
    try:
        yield
    finally:
        _slack_alarm_extras.reset(token)


def _slack_bot_credentials() -> Tuple[str | None, str | None]:
    try:
        import streamlit as st

        sec = getattr(st, "secrets", None)
        if sec is None:
            return None, None
        slack = sec.get("slack")
        if not isinstance(slack, dict):
            return None, None
        token = slack.get("bot_token") or slack.get("bot_user_oauth_token")
        channel = slack.get("channel_id") or slack.get("channel")
        token_s = str(token).strip() if token else None
        ch_s = str(channel).strip() if channel else None
        return (token_s or None, ch_s or None)
    except Exception:
        return None, None


def _read_user_session_fields() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    try:
        import streamlit as st

        ss = getattr(st, "session_state", None)
        if ss is None:
            return out
        for key in ("user_email", "user_name", "user_role", "login_method"):
            if key in ss and ss[key] is not None:
                out[key] = ss[key]
    except Exception:
        pass
    return out


def _suffix_after_marker(key: str, marker: str) -> str | None:
    i = key.find(marker)
    if i == -1:
        return None
    return key[i + len(marker) :]


def _read_game_scoped_widgets(marker: str, skip_substr: str | None) -> Dict[str, Any]:
    """`{prefix}platform_{game}` 형태 키에서 game → 값 추출."""
    out: Dict[str, Any] = {}
    try:
        import streamlit as st

        for k in list(st.session_state.keys()):
            if not isinstance(k, str):
                continue
            if marker not in k:
                continue
            if skip_substr and skip_substr in k:
                continue
            game = _suffix_after_marker(k, marker)
            if not game:
                continue
            try:
                out[game] = st.session_state[k]
            except Exception:
                continue
    except Exception:
        pass
    return out


def _safe_query_params(max_keys: int = 24, max_val_len: int = 160) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        import streamlit as st

        qp = st.query_params
        for i, k in enumerate(qp.keys()):
            if i >= max_keys:
                break
            v = qp.get(k)
            if isinstance(v, list):
                v = v[0] if v else ""
            out[str(k)[:80]] = str(v)[:max_val_len]
    except Exception:
        pass
    return out


def _capture_alarm_snapshot() -> Dict[str, Any]:
    """메인(Streamlit) 스레드에서 `submit` 직전에 호출."""
    snap: Dict[str, Any] = {}
    snap.update(_read_user_session_fields())

    platforms = _read_game_scoped_widgets("platform_", "platform_prev")
    if platforms:
        snap["game_platforms"] = platforms

    imports = _read_game_scoped_widgets("import_method_", None)
    if imports:
        snap["game_import_methods"] = imports

    qp = _safe_query_params()
    if qp:
        snap["query_params"] = qp

    extras = _slack_alarm_extras.get()
    if extras:
        snap.update(extras)
    return snap


def _format_context_block(ctx: Dict[str, Any] | None) -> str:
    if not ctx:
        return ""
    lines: list[str] = []
    if ctx.get("user_email") or ctx.get("user_name"):
        ue = ctx.get("user_email", "")
        un = ctx.get("user_name", "")
        lines.append(f"• *사용자:* `{un}` ({ue})".strip())
    if ctx.get("user_role"):
        lines.append(f"• *역할:* `{ctx['user_role']}`")
    if ctx.get("login_method"):
        lines.append(f"• *로그인 방식:* `{ctx['login_method']}`")

    gp = ctx.get("game_platforms")
    if isinstance(gp, dict) and gp:
        parts = [f"{g} → `{v}`" for g, v in sorted(gp.items(), key=lambda x: x[0])[:20]]
        lines.append("• *게임별 플랫폼(광고 네트워크 UI):* " + "; ".join(parts))
        if len(gp) > 20:
            lines.append(f"  _(외 {len(gp) - 20}개 게임 생략)_")

    gi = ctx.get("game_import_methods")
    if isinstance(gi, dict) and gi:
        parts = [f"{g} → `{v}`" for g, v in sorted(gi.items(), key=lambda x: x[0])[:20]]
        lines.append("• *가져오기 방법:* " + "; ".join(parts))

    reserved = {
        "user_email",
        "user_name",
        "user_role",
        "login_method",
        "game_platforms",
        "game_import_methods",
        "query_params",
    }
    for k, v in sorted(ctx.items()):
        if k in reserved:
            continue
        if v is None or v == "":
            continue
        if isinstance(v, (dict, list)):
            try:
                v = json.dumps(v, ensure_ascii=False)[:300]
            except Exception:
                v = str(v)[:300]
        else:
            v = str(v)[:500]
        lines.append(f"• *{k}:* `{v}`")

    qp = ctx.get("query_params")
    if isinstance(qp, dict) and qp:
        qstr = ", ".join(f"{k}={v}" for k, v in sorted(qp.items())[:12])
        lines.append(f"• *URL 쿼리:* {qstr}")

    if not lines:
        return ""
    return "\n*컨텍스트*\n" + "\n".join(lines) + "\n"


def _post_slack_text(text: str, timeout_sec: float = 10.0) -> None:
    token, channel_id = _slack_bot_credentials()
    if not token or not channel_id:
        return
    if len(text) > 39000:
        text = text[:38950] + "\n…(truncated)"
    payload = {"channel": channel_id, "text": text, "mrkdwn": True}
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        _SLACK_POST_MESSAGE,
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
        if not data.get("ok"):
            return
    except (urllib.error.URLError, OSError, TimeoutError, ValueError, TypeError):
        return


def notify_worker_exception(
    module: str,
    qualname: str,
    exc: BaseException,
    context: Dict[str, Any] | None = None,
) -> None:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    if len(tb) > 2800:
        tb = tb[:2790] + "\n…(truncated)"
    ctx_block = _format_context_block(context)
    text = (
        f":rotating_light: *Super Crema* worker 예외\n"
        f"{ctx_block}"
        f"*모듈:* `{module}`\n"
        f"*함수:* `{qualname}`\n"
        f"*타입:* `{type(exc).__name__}`\n"
        f"*메시지:* {exc!s}\n"
        f"```{tb}```"
    )
    _post_slack_text(text)


def _wrap_worker(fn: Callable[..., Any], context_snapshot: Dict[str, Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            mod = getattr(fn, "__module__", "?")
            qn = getattr(fn, "__qualname__", getattr(fn, "__name__", repr(fn)))
            notify_worker_exception(mod, qn, e, context_snapshot)
            raise

    return wrapped


class SlackNotifyThreadPoolExecutor(ThreadPoolExecutor):
    """concurrent.futures.ThreadPoolExecutor와 동일하되, submit 시 스냅샷을 붙여 worker 예외 시 Slack 전송."""

    def submit(self, fn, /, *args, **kwargs):  # type: ignore[override]
        snap = _capture_alarm_snapshot()
        return super().submit(_wrap_worker(fn, snap), *args, **kwargs)
