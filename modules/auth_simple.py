"""
통합 인증 모듈 (streamlit-authenticator 기반)
"""
import streamlit as st
import streamlit_authenticator as stauth
import hashlib
import os


# ========== 유저 설정 ==========
def _get_config():
    def h(pw): return hashlib.sha256(pw.encode()).hexdigest()
    
    credentials = {
        "usernames": {
            "edwin": {"name": "Edwin", "password": h("edwin123"), "role": "admin"},
            "jaepark": {"name": "Jaepark", "password": h("jaepark17"), "role": "admin"},
            "eader": {"name": "Eader", "password": h("eader29"), "role": "admin"},
            "sam": {"name": "Sam", "password": h("sam83"), "role": "user"},
            "sonak": {"name": "Sonak", "password": h("sonak61"), "role": "user"},
            "sonic": {"name": "Sonic", "password": h("sonic74"), "role": "user"},
            "seyoung": {"name": "Seyoung", "password": h("seyoung58"), "role": "user"},
            "eli": {"name": "Eli", "password": h("eli91"), "role": "user"},
            "jonghoon": {"name": "Jonghoon", "password": h("jonghoon36"), "role": "user"},
            "kyle": {"name": "Kyle", "password": h("kyle64"), "role": "user"},
            "tory": {"name": "Tory", "password": h("tory27"), "role": "user"},
            "hini": {"name": "Hini", "password": h("hini55"), "role": "user"},
            "nova": {"name": "Nova", "password": h("nova48"), "role": "user"},
            "dawoony": {"name": "Dawoony", "password": h("dawoony72"), "role": "user"},
            "luca": {"name": "Luca", "password": h("luca19"), "role": "user"},
            "zino": {"name": "Zino", "password": h("zino46"), "role": "user"},
            "crissy": {"name": "Crissy", "password": h("crissy31"), "role": "user"},
            "kira": {"name": "Kira", "password": h("kira69"), "role": "user"},
            "heny": {"name": "Heny", "password": h("heny88"), "role": "user"},
        }
    }
    
    cookie_config = {
        "expiry_days": 7,
        "key": st.secrets.get("cookie_secret", "super-crema-secret-key-2024"),
        "name": "sc_auth_cookie"
    }
    
    return credentials, cookie_config


def get_authenticator():
    if '_authenticator' not in st.session_state:
        credentials, cookie_config = _get_config()
        st.session_state._authenticator = stauth.Authenticate(
            credentials=credentials,
            cookie_name=cookie_config["name"],
            cookie_key=cookie_config["key"],
            cookie_expiry_days=cookie_config["expiry_days"],
        )
    return st.session_state._authenticator


def log_action(user_email, login_method, action):
    print(f"[LOG] {user_email} - {login_method} - {action}")


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


def login_with_google(email):
    allowed_domains = ["@supercent.io"]
    allowed_emails = ["rumble@supercent.vn"]
    if not any(email.endswith(d) for d in allowed_domains) and email not in allowed_emails:
        return False, "🚫 Supercent 계정만 사용 가능합니다"
    name = email.split("@")[0].capitalize()
    role = "admin" if email in ["edwin@supercent.io"] else "user"
    st.session_state.authenticated = True
    st.session_state.user_email = email
    st.session_state.user_name = name
    st.session_state.user_role = role
    st.session_state.login_method = "google"
    log_action(email, "google", "login")
    return True, f"✅ 환영합니다, {name}님!"


# ========== 인증 함수 ==========
def check_authentication():
    # Google 로그인으로 이미 인증된 경우
    if st.session_state.get('authenticated', False):
        return True

    # streamlit-authenticator 쿠키 체크
    authenticator = get_authenticator()


    # auth_status = authenticator.login(location='unrendered')
    name, auth_status, username = authenticator.login(location='unrendered')




    if auth_status is True:
        username = st.session_state.get("username")
        credentials, _ = _get_config()
        user_info = credentials["usernames"].get(username, {})
        st.session_state.authenticated = True
        st.session_state.user_email = username
        st.session_state.user_name = user_info.get("name", username)
        st.session_state.user_role = user_info.get("role", "user")
        st.session_state.login_method = "password"
        return True

    return False


def logout():
    authenticator = get_authenticator()
    try:
        authenticator.logout(location='unrendered')
    except Exception:
        pass
    for key in ['authenticated', 'user_email', 'user_name', 'user_role', 'login_method',
                'username', 'name', 'authentication_status', '_authenticator']:
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

            # Google 로그인
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

            # 로컬 계정 로그인 (streamlit-authenticator)
            st.markdown("##### 🔑 로컬 계정 로그인")
            authenticator = get_authenticator()
            name, auth_status, username = authenticator.login(
                fields={
                    'Form name': '',
                    'Username': 'Username',
                    'Password': 'Password',
                    'Login': 'Login'
                }
            )

            if auth_status is True:
                credentials, _ = _get_config()
                user_info = credentials["usernames"].get(username, {})
                st.session_state.authenticated = True
                st.session_state.user_email = username
                st.session_state.user_name = user_info.get("name", username)
                st.session_state.user_role = user_info.get("role", "user")
                st.session_state.login_method = "password"
                log_action(username, "password", "login")
                st.rerun()
            elif auth_status is False:
                st.error("아이디 또는 비밀번호가 올바르지 않습니다")