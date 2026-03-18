"""
통합 인증 모듈 (Google + ID/PW)
서버 세션 토큰 방식: 쿠키엔 토큰 1개, 유저 정보는 서버 메모리
"""
import hashlib
import secrets
import streamlit as st
from datetime import datetime, timedelta

# ========== 서버 메모리 세션 저장소 ==========
active_sessions = {}  # {token: {email, name, role, method, expires}}
SESSION_EXPIRE_HOURS = 24 * 7  # 7일


def _create_session(email, name, role, method):
    token = secrets.token_urlsafe(32)
    active_sessions[token] = {
        "email": email,
        "name": name,
        "role": role,
        "method": method,
        "expires": datetime.now() + timedelta(hours=SESSION_EXPIRE_HOURS),
    }
    return token


def _validate_session(token):
    if not token or token not in active_sessions:
        return None
    session = active_sessions[token]
    if datetime.now() > session["expires"]:
        del active_sessions[token]
        return None
    return session


def _delete_session(token):
    if token and token in active_sessions:
        del active_sessions[token]


# ========== 쿠키 헬퍼 ==========
def _get_ctrl():
    return st.session_state.get('_cookie_ctrl')


def _save_token_cookie(token):
    ctrl = _get_ctrl()
    if ctrl is None:
        return
    try:
        ctrl.set('sc_session', token, expires_at=datetime(2030, 1, 1))
    except Exception:
        pass


def _get_token_cookie():
    ctrl = _get_ctrl()
    if ctrl is None:
        return None
    try:
        return ctrl.get(cookie='sc_session')
    except Exception:
        return None


def _delete_token_cookie():
    ctrl = _get_ctrl()
    if ctrl is None:
        return
    try:
        ctrl.delete('sc_session')
    except Exception:
        pass


# ========== Google OAuth ==========
def get_google_login_url():
    import urllib.parse
    redirect_uri = st.secrets["google_oauth"]["redirect_uri"]
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
        return None
    code = query_params.get("code")
    if isinstance(code, list):
        code = code[0]
    if st.session_state.get("oauth_code_used") == code:
        return None
    try:
        token_resp = req.post("https://oauth2.googleapis.com/token", data={
            "code": code,
            "client_id": st.secrets["google_oauth"]["client_id"],
            "client_secret": st.secrets["google_oauth"]["client_secret"],
            "redirect_uri": st.secrets["google_oauth"]["redirect_uri"],
            "grant_type": "authorization_code",
        }, timeout=20)
        token_data = token_resp.json()
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
        return idinfo.get("email")
    except Exception as e:
        if "invalid_grant" in str(e):
            st.query_params.clear()
            st.rerun()
        st.error(f"OAuth 처리 실패: {repr(e)}")
        st.query_params.clear()
        return None


# ========== 유저 목록 ==========
@st.cache_data(ttl=300)
def load_users():
    def h(pw): return hashlib.sha256(pw.encode()).hexdigest()
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


# ========== 인증 함수 ==========
def check_authentication():
    if st.session_state.get('authenticated', False):
        return True
    token = _get_token_cookie()
    if not token:
        return False
    session = _validate_session(token)
    if not session:
        _delete_token_cookie()
        return False
    st.session_state.authenticated = True
    st.session_state.user_email = session["email"]
    st.session_state.user_name = session["name"]
    st.session_state.user_role = session["role"]
    st.session_state.login_method = session["method"]
    st.session_state._session_token = token
    return True


def _set_session(email, name, role, method):
    token = _create_session(email, name, role, method)
    st.session_state.authenticated = True
    st.session_state.user_email = email
    st.session_state.user_name = name
    st.session_state.user_role = role
    st.session_state.login_method = method
    st.session_state._session_token = token
    _save_token_cookie(token)


def login_with_password(username, password):
    users = load_users()
    if username not in users:
        return False, "사용자를 찾을 수 없습니다"
    user = users[username]
    if user["password_hash"] is None:
        return False, "이 계정은 Google 로그인만 가능합니다"
    if hashlib.sha256(password.encode()).hexdigest() != user["password_hash"]:
        return False, "비밀번호가 일치하지 않습니다"
    _set_session(username, user["name"], user["role"], "password")
    log_action(username, "password", "login")
    return True, "로그인 성공"


def login_with_google(email):
    allowed_domains = ["@supercent.io"]
    allowed_emails = ["rumble@supercent.vn"]
    if not any(email.endswith(d) for d in allowed_domains) and email not in allowed_emails:
        return False, "🚫 Supercent 계정만 사용 가능합니다"
    name = email.split("@")[0].capitalize()
    role = "admin" if email in ["edwin@supercent.io"] else "user"
    _set_session(email, name, role, "google")
    log_action(email, "google", "login")
    return True, f"✅ 환영합니다, {name}님!"


def logout():
    token = st.session_state.get('_session_token')
    _delete_session(token)
    _delete_token_cookie()
    for key in ['authenticated', 'user_email', 'user_name', 'user_role', 'login_method', '_session_token']:
        st.session_state.pop(key, None)


# ========== 로그인 UI ==========
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
            st.info("🏢 @supercent.io 계정만 사용 가능합니다.")

            email = handle_google_callback()
            if email:
                success, message = login_with_google(email)
                if success:
                    st.query_params.clear()
                    st.rerun()
                else:
                    st.error(message)
                    st.query_params.clear()

            auth_url = get_google_login_url()
            st.markdown("""
            <style>
            a[data-testid="stBaseLinkButton-secondary"] {
                background: white !important; color: #444 !important;
                border: 1px solid #ddd !important; border-radius: 8px !important;
                font-weight: 600 !important; margin-bottom: 25px !important;
            }
            a[data-testid="stBaseLinkButton-secondary"] p { color: #444 !important; }
            </style>
            """, unsafe_allow_html=True)
            st.link_button("🌐 Sign in with Google", auth_url, use_container_width=True)

            st.markdown("---")
            st.markdown("##### 🔑 로컬 계정 로그인")

            with st.form(key="local_login_form"):
                username = st.text_input("Username", key="local_username_2", placeholder="아이디 입력")
                password = st.text_input("Password", type="password", key="local_password_2", placeholder="비밀번호 입력")
                submitted = st.form_submit_button("Login", use_container_width=True)
                if submitted:
                    success, message = login_with_password(username, password)
                    if success:
                        st.rerun()
                    else:
                        st.error(message)
