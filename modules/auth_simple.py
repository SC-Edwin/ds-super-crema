"""
통합 인증 모듈 (Google + ID/PW)
"""

import hashlib
import os
import uuid
from datetime import datetime

import streamlit as st
from google.cloud import bigquery


def debug_log(event, **kwargs):
    ts = datetime.utcnow().isoformat()
    run_id = st.session_state.setdefault("_run_id", str(uuid.uuid4())[:8])

    payload = {
        "ts": ts,
        "run_id": run_id,
        "event": event,
        **kwargs,
    }

    print(f"[SC-AUTH] {payload}")

    st.session_state.setdefault("_debug_logs", []).append(payload)
    if len(st.session_state["_debug_logs"]) > 300:
        st.session_state["_debug_logs"] = st.session_state["_debug_logs"][-300:]


# ========== Google OAuth 헬퍼 함수 ==========
def get_google_oauth_flow():
    from google_auth_oauthlib.flow import Flow

    redirect_uri = st.secrets["google_oauth"]["redirect_uri"]

    flow = Flow.from_client_config(
        {
            "web": {
                "client_id": st.secrets["google_oauth"]["client_id"],
                "client_secret": st.secrets["google_oauth"]["client_secret"],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [redirect_uri],
            }
        },
        scopes=[
            "openid",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
        ],
        redirect_uri=redirect_uri,
    )
    return flow


def get_google_login_url():
    import urllib.parse

    redirect_uri = st.secrets["google_oauth"]["redirect_uri"]

    debug_log(
        "oauth_login_url_build",
        streamlit_env=os.getenv("STREAMLIT_ENV"),
        redirect_uri=redirect_uri,
    )

    params = {
        "response_type": "code",
        "client_id": st.secrets["google_oauth"]["client_id"],
        "redirect_uri": redirect_uri,
        "scope": "openid https://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/userinfo.profile",
        "access_type": "offline",
        "prompt": "select_account",
    }
    return f"https://accounts.google.com/o/oauth2/auth?{urllib.parse.urlencode(params)}"


def handle_google_callback():
    import requests as req

    query_params = st.query_params

    if "code" not in query_params:
        debug_log("oauth_callback_no_code")
        return None

    code = query_params.get("code")
    if isinstance(code, list):
        code = code[0]

    if st.session_state.get("oauth_code_used") == code:
        debug_log("oauth_callback_code_already_used")
        return None

    try:
        debug_log("oauth_token_exchange_start")

        token_resp = req.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": st.secrets["google_oauth"]["client_id"],
                "client_secret": st.secrets["google_oauth"]["client_secret"],
                "redirect_uri": st.secrets["google_oauth"]["redirect_uri"],
                "grant_type": "authorization_code",
            },
            timeout=20,
        )
        token_data = token_resp.json()

        debug_log("oauth_token_exchange_response", token_keys=list(token_data.keys()))

        if "error" in token_data:
            raise Exception(f"{token_data['error']}: {token_data.get('error_description', '')}")

        st.session_state["oauth_code_used"] = code

        from google.oauth2 import id_token
        from google.auth.transport import requests

        idinfo = id_token.verify_oauth2_token(
            token_data["id_token"],
            requests.Request(),
            st.secrets["google_oauth"]["client_id"],
        )
        email = idinfo.get("email")
        debug_log("oauth_callback_verified", email=email)
        return email

    except Exception as e:
        debug_log("oauth_callback_error", error=repr(e))
        if "invalid_grant" in str(e):
            st.query_params.clear()
            st.rerun()
        st.error(f"OAuth 처리 실패: {repr(e)}")
        st.query_params.clear()
        return None


@st.cache_data(ttl=300)
def load_users():
    def h(pw: str) -> str:
        return hashlib.sha256(pw.encode()).hexdigest()

    return {
        "edwin": {"password_hash": h("edwin123"), "name": "Edwin", "role": "admin"},
        "jaepark": {"password_hash": h("jaepark17"), "name": "Jaepark", "role": "admin"},
        "eader": {"password_hash": h("eader29"), "name": "Eader", "role": "admin"},
        "sam": {"password_hash": h("sam83"), "name": "Sam", "role": "user"},
        "sonak": {"password_hash": h("sonak61"), "name": "Sonak", "role": "user"},
        "sonic": {"password_hash": h("sonic74"), "name": "Sonic", "role": "user"},
        "seyoung": {"password_hash": h("seyoung58"), "name": "Seyoung", "role": "user"},
        "eli": {"password_hash": h("eli91"), "name": "Eli", "role": "user"},
        "jonghoon": {"password_hash": h("jonghoon36"), "name": "Jonghoon", "role": "user"},
        "kyle": {"password_hash": h("kyle64"), "name": "Kyle", "role": "user"},
        "tory": {"password_hash": h("tory27"), "name": "Tory", "role": "user"},
        "hini": {"password_hash": h("hini55"), "name": "Hini", "role": "user"},
        "nova": {"password_hash": h("nova48"), "name": "Nova", "role": "user"},
        "dawoony": {"password_hash": h("dawoony72"), "name": "Dawoony", "role": "user"},
        "luca": {"password_hash": h("luca19"), "name": "Luca", "role": "user"},
        "zino": {"password_hash": h("zino46"), "name": "Zino", "role": "user"},
        "crissy": {"password_hash": h("crissy31"), "name": "Crissy", "role": "user"},
        "kira": {"password_hash": h("kira69"), "name": "Kira", "role": "user"},
        "heny": {"password_hash": h("heny88"), "name": "Heny", "role": "user"},
    }


def log_action(user_email, login_method, action):
    print(f"[LOG] {user_email} - {login_method} - {action}")
    return

    try:
        client = bigquery.Client()

        rows = [{
            "log_id": f"{user_email}_{datetime.now().isoformat()}",
            "user_email": user_email,
            "login_method": login_method,
            "action": action,
            "timestamp": datetime.now().isoformat(),
            "user_agent": st.context.headers.get("User-Agent", "Unknown"),
        }]

        table_id = "roas-test-456808.marketing_datascience.super_crema_access_logs"
        client.insert_rows_json(table_id, rows)

    except Exception as e:
        print(f"Log error: {e}")


COOKIE_KEYS = ["sc_email", "sc_name", "sc_role", "sc_method"]


def _is_cookie_ready():
    return st.session_state.get("_cookie_ready", False)


def _queue_cookie_op(op, *args):
    st.session_state.setdefault("_pending_cookie_ops", []).append((op, args))
    debug_log(
        "queue_cookie_op",
        op=op,
        args_repr=repr(args),
        queue_size=len(st.session_state["_pending_cookie_ops"]),
    )


def _flush_cookie_ops():
    if not _is_cookie_ready():
        debug_log("flush_cookie_ops_skipped_not_ready")
        return

    controller = st.session_state.get("_cookie_ctrl")
    if controller is None:
        debug_log("flush_cookie_ops_skipped_no_controller")
        return

    pending = st.session_state.get("_pending_cookie_ops", [])
    debug_log("flush_cookie_ops_start", pending_count=len(pending))

    if not pending:
        return

    remain = []
    for op, args in pending:
        try:
            getattr(controller, op)(*args)
            debug_log("flush_cookie_op_success", op=op, args_repr=repr(args))
        except Exception as e:
            remain.append((op, args))
            debug_log("flush_cookie_op_error", op=op, args_repr=repr(args), error=repr(e))

    st.session_state["_pending_cookie_ops"] = remain
    debug_log("flush_cookie_ops_done", remain_count=len(remain))


def _save_session_cookie(user_email, user_name, user_role, login_method):
    controller = st.session_state.get("_cookie_ctrl")
    if controller is None:
        debug_log("save_cookie_no_controller")
        return False

    debug_log(
        "save_cookie_start",
        cookie_ready=_is_cookie_ready(),
        user_email=user_email,
        user_name=user_name,
        user_role=user_role,
        login_method=login_method,
    )

    if not _is_cookie_ready():
        _queue_cookie_op("set", "sc_email", user_email)
        _queue_cookie_op("set", "sc_name", user_name)
        _queue_cookie_op("set", "sc_role", user_role)
        _queue_cookie_op("set", "sc_method", login_method)
        debug_log("save_cookie_queued_not_ready")
        return False

    try:
        controller.set("sc_email", user_email)
        controller.set("sc_name", user_name)
        controller.set("sc_role", user_role)
        controller.set("sc_method", login_method)
        debug_log("save_cookie_success")
        return True
    except Exception as e:
        _queue_cookie_op("set", "sc_email", user_email)
        _queue_cookie_op("set", "sc_name", user_name)
        _queue_cookie_op("set", "sc_role", user_role)
        _queue_cookie_op("set", "sc_method", login_method)
        debug_log("save_cookie_error_queued", error=repr(e))
        return False


def _clear_session_cookie():
    controller = st.session_state.get("_cookie_ctrl")
    if controller is None:
        debug_log("clear_cookie_no_controller")
        return False

    if not _is_cookie_ready():
        for key in COOKIE_KEYS:
            _queue_cookie_op("remove", key)
        debug_log("clear_cookie_queued_not_ready")
        return False

    ok = True
    for key in COOKIE_KEYS:
        try:
            controller.remove(key)
            debug_log("clear_cookie_removed", key=key)
        except Exception as e:
            _queue_cookie_op("remove", key)
            debug_log("clear_cookie_remove_error", key=key, error=repr(e))
            ok = False
    return ok


def check_authentication():
    debug_log(
        "check_auth_start",
        authenticated=st.session_state.get("authenticated"),
        cookie_ready=_is_cookie_ready(),
    )

    if st.session_state.get("authenticated", False):
        debug_log("check_auth_session_already_authenticated")
        _flush_cookie_ops()
        return True

    controller = st.session_state.get("_cookie_ctrl")
    if controller is None:
        debug_log("check_auth_no_controller")
        return False

    if not _is_cookie_ready():
        debug_log("check_auth_cookie_not_ready")
        return False

    _flush_cookie_ops()

    try:
        email = controller.get("sc_email")
        name = controller.get("sc_name")
        role = controller.get("sc_role")
        login_method = controller.get("sc_method")

        debug_log(
            "check_auth_cookie_values",
            email=repr(email),
            name=repr(name),
            role=repr(role),
            login_method=repr(login_method),
        )

        if not email:
            debug_log("check_auth_no_email_in_cookie")
            return False

        st.session_state["authenticated"] = True
        st.session_state["user_email"] = email
        st.session_state["user_name"] = name or ""
        st.session_state["user_role"] = role or "user"
        st.session_state["login_method"] = login_method or "password"

        debug_log("check_auth_restored_from_cookie", user_email=email)
        return True

    except Exception as e:
        debug_log("check_auth_exception", error=repr(e))
        return False


def login_with_password(username, password):
    users = load_users()

    debug_log("login_with_password_attempt", username=username)

    if username not in users:
        debug_log("login_with_password_user_not_found", username=username)
        return False, "사용자를 찾을 수 없습니다"

    user = users[username]

    if user["password_hash"] is None:
        debug_log("login_with_password_google_only", username=username)
        return False, "이 계정은 Google 로그인만 가능합니다"

    password_hash = hashlib.sha256(password.encode()).hexdigest()

    if password_hash != user["password_hash"]:
        debug_log("login_with_password_wrong_password", username=username)
        return False, "비밀번호가 일치하지 않습니다"

    st.session_state["authenticated"] = True
    st.session_state["user_email"] = username
    st.session_state["user_name"] = user["name"]
    st.session_state["user_role"] = user["role"]
    st.session_state["login_method"] = "password"

    _save_session_cookie(username, user["name"], user["role"], "password")
    log_action(username, "password", "login")
    debug_log("login_with_password_success", username=username)

    return True, "로그인 성공"


def login_with_google(email):
    allowed_domains = ["@supercent.io"]
    allowed_emails = ["rumble@supercent.vn"]

    debug_log("login_with_google_attempt", email=email)

    if not any(email.endswith(domain) for domain in allowed_domains) and email not in allowed_emails:
        debug_log("login_with_google_rejected", email=email)
        return False, "🚫 Supercent 계정만 사용 가능합니다"

    name = email.split("@")[0].capitalize()
    admins = ["edwin@supercent.io"]
    role = "admin" if email in admins else "user"

    st.session_state["authenticated"] = True
    st.session_state["user_email"] = email
    st.session_state["user_name"] = name
    st.session_state["user_role"] = role
    st.session_state["login_method"] = "google"

    _save_session_cookie(email, name, role, "google")
    log_action(email, "google", "login")
    debug_log("login_with_google_success", email=email, role=role)

    return True, f"✅ 환영합니다, {name}님!"


def logout():
    debug_log(
        "logout_start",
        user_email=st.session_state.get("user_email"),
        login_method=st.session_state.get("login_method"),
    )

    _clear_session_cookie()

    for key in [
        "authenticated",
        "user_email",
        "user_name",
        "user_role",
        "login_method",
    ]:
        if key in st.session_state:
            del st.session_state[key]

    debug_log("logout_done")


def show_login_page():
    st.markdown("""
    <div class="super-crema-header">
        <h1 class="super-crema-title">🎬 SUPER CREMA</h1>
        <p class="super-crema-subtitle">Creative Intelligence Automation Platform</p>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([2.5, 1.5, 2.5])

    with col2:
        with st.container(border=False):
            st.markdown("### 🔐 로그인")

            st.markdown("##### 🌐 Supercent 계정 로그인 (권장)")
            st.info("🏢  @supercent.io 계정만 사용 가능합니다.")

            email = handle_google_callback()
            if email:
                success, message = login_with_google(email)
                if success:
                    st.success(message)
                    st.query_params.clear()
                    st.rerun()
                else:
                    st.error(message)
                    st.query_params.clear()

            auth_url = get_google_login_url()
            st.markdown("""
            <style>
            a[data-testid="stBaseLinkButton-secondary"] {
                background: white !important;
                color: #444 !important;
                border: 1px solid #ddd !important;
                border-radius: 8px !important;
                font-weight: 600 !important;
                margin-bottom: 25px !important;
            }
            a[data-testid="stBaseLinkButton-secondary"] p {
                color: #444 !important;
            }
            </style>
            """, unsafe_allow_html=True)
            st.link_button("🌐 Sign in with Google", auth_url, use_container_width=True)

            st.markdown("---")

            st.markdown("##### 🔑 로컬 계정 로그인")

            with st.form(key="local_login_form"):
                username = st.text_input(
                    "Username",
                    key="local_username_2",
                    placeholder="아이디 입력",
                    label_visibility="visible",
                )
                password = st.text_input(
                    "Password",
                    type="password",
                    key="local_password_2",
                    placeholder="비밀번호 입력",
                    label_visibility="visible",
                )

                submitted = st.form_submit_button("Login", use_container_width=True)



                if submitted:
                    success, message = login_with_password(username, password)
                    if success:
                        st.success(message)
                        st.session_state['_cookie_just_set'] = True  # rerun 지연 플래그
                    else:
                        st.error(message)