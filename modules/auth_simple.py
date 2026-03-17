"""
통합 인증 모듈 (Google + ID/PW)
"""
import streamlit as st
import hashlib
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
import os
import json
from datetime import timedelta


# ========== Config ==========




# ========== Google OAuth 헬퍼 함수 ==========
def get_google_oauth_flow():
    """
    Google OAuth Flow 생성
    - redirect_uri는 환경별 Secrets에서만 가져온다
    - (local / dev / main 모두 동일 코드)
    """
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
    """PKCE 없이 OAuth URL 생성 (Streamlit Cloud 호환)"""
    import urllib.parse

    redirect_uri = st.secrets["google_oauth"]["redirect_uri"]
    print(f"[OAUTH] STREAMLIT_ENV={os.getenv('STREAMLIT_ENV')}")
    print(f"[OAUTH] redirect_uri={redirect_uri}")

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
    """PKCE 없이 토큰 교환 (세션 유실에 안전)"""
    import requests as req

    query_params = st.query_params

    if "code" not in query_params:
        return None

    code = query_params.get("code")
    if isinstance(code, list):
        code = code[0]

    # 🔒 이미 처리한 code면 무시
    if st.session_state.get("oauth_code_used") == code:
        return None

    try:
        # 직접 토큰 교환 (PKCE code_verifier 불필요)
        token_resp = req.post("https://oauth2.googleapis.com/token", data={
            "code": code,
            "client_id": st.secrets["google_oauth"]["client_id"],
            "client_secret": st.secrets["google_oauth"]["client_secret"],
            "redirect_uri": st.secrets["google_oauth"]["redirect_uri"],
            "grant_type": "authorization_code",
        })
        token_data = token_resp.json()

        if "error" in token_data:
            raise Exception(f"{token_data['error']}: {token_data.get('error_description', '')}")

        # 🔒 code 소비 완료 기록
        st.session_state.oauth_code_used = code

        # id_token에서 이메일 추출
        from google.oauth2 import id_token
        from google.auth.transport import requests

        idinfo = id_token.verify_oauth2_token(
            token_data["id_token"],
            requests.Request(),
            st.secrets["google_oauth"]["client_id"]
        )
        return idinfo.get("email")

    except Exception as e:
        if "invalid_grant" in str(e):
            st.query_params.clear()
            st.rerun()
        st.error(f"OAuth 처리 실패: {repr(e)}")
        st.query_params.clear()
        return None





# ========== Google Sheets 연동 ==========
@st.cache_data(ttl=300)
def load_users():
    import hashlib

    def h(pw: str) -> str:
        return hashlib.sha256(pw.encode()).hexdigest()

    return {
        # ===== Admin =====
        "edwin": {
            "password_hash": h("edwin123"),
            "name": "Edwin",
            "role": "admin",
        },
        "jaepark": {
            "password_hash": h("jaepark17"),
            "name": "Jaepark",
            "role": "admin",
        },
        "eader": {
            "password_hash": h("eader29"),
            "name": "Eader",
            "role": "admin",
        },

        # ===== Members =====
        "sam": {
            "password_hash": h("sam83"),
            "name": "Sam",
            "role": "user",
        },
        "sonak": {
            "password_hash": h("sonak61"),
            "name": "Sonak",
            "role": "user",
        },
        "sonic": {
            "password_hash": h("sonic74"),
            "name": "Sonic",
            "role": "user",
        },
        "seyoung": {
            "password_hash": h("seyoung58"),
            "name": "Seyoung",
            "role": "user",
        },
        "eli": {
            "password_hash": h("eli91"),
            "name": "Eli",
            "role": "user",
        },
        "jonghoon": {
            "password_hash": h("jonghoon36"),
            "name": "Jonghoon",
            "role": "user",
        },
        "kyle": {
            "password_hash": h("kyle64"),
            "name": "Kyle",
            "role": "user",
        },
        "tory": {
            "password_hash": h("tory27"),
            "name": "Tory",
            "role": "user",
        },
        "hini": {
            "password_hash": h("hini55"),
            "name": "Hini",
            "role": "user",
        },
        "nova": {
            "password_hash": h("nova48"),
            "name": "Nova",
            "role": "user",
        },
        "dawoony": {
            "password_hash": h("dawoony72"),
            "name": "Dawoony",
            "role": "user",
        },
        "luca": {
            "password_hash": h("luca19"),
            "name": "Luca",
            "role": "user",
        },
        "zino": {
            "password_hash": h("zino46"),
            "name": "Zino",
            "role": "user",
        },
        "crissy": {
            "password_hash": h("crissy31"),
            "name": "Crissy",
            "role": "user",
        },
        "kira": {
            "password_hash": h("kira69"),
            "name": "Kira",
            "role": "user",
        },
        "heny": {
            "password_hash": h("heny88"),
            "name": "Heny",
            "role": "user",
        },
    }

# ========== 로그 기록 ==========
def log_action(user_email, login_method, action):
    """BigQuery에 로그 저장 (임시 비활성화)"""
    print(f"[LOG] {user_email} - {login_method} - {action}")
    return  # ← 이 줄 추가하면 아래 코드 실행 안됨

    try:
        client = bigquery.Client()
        
        rows = [{
            'log_id': f"{user_email}_{datetime.now().isoformat()}",
            'user_email': user_email,
            'login_method': login_method,
            'action': action,
            'timestamp': datetime.now().isoformat(),
            'user_agent': st.context.headers.get('User-Agent', 'Unknown')
        }]
        
        table_id = 'roas-test-456808.marketing_datascience.super_crema_access_logs'
        client.insert_rows_json(table_id, rows)
        
    except Exception as e:
        # 로그 실패해도 앱은 계속 실행
        print(f"Log error: {e}")



def _save_session_cookie(user_email, user_name, user_role, login_method):
    """세션 정보 저장 (session_state only)"""
    print(f"[AUTH] Session saved for: {user_email}")

    



# ========== 인증 함수 ==========
def check_authentication():
    """현재 세션 인증 상태 확인 (session_state 기반)"""

    if st.session_state.get('authenticated', False):
        print(f"[AUTH] Session active: {st.session_state.get('user_email')}")
        return True

    print(f"[AUTH] Not authenticated")
    return False



def login_with_password(username, password):
    """ID/PW 로그인"""
    users = load_users()
    
    if username not in users:
        return False, "사용자를 찾을 수 없습니다"
    
    user = users[username]
    
    if user['password_hash'] is None:
        return False, "이 계정은 Google 로그인만 가능합니다"
    
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    if password_hash != user['password_hash']:
        return False, "비밀번호가 일치하지 않습니다"
    
    # 로그인 성공
    st.session_state.authenticated = True
    st.session_state.user_email = username
    st.session_state.user_name = user['name']
    st.session_state.user_role = user['role']
    st.session_state.login_method = 'password'
    
    _save_session_cookie(username, user['name'], user['role'], 'password')


    log_action(username, 'password', 'login')

    try:
        from modules.upload_automation.upload_logger import log_event
        log_event("login")
    except Exception as e:
        print(f"[upload_logger] {e}")

    return True, "로그인 성공"

def login_with_google(email):
    """Google 로그인 - Supercent 도메인이면 자동 허용"""
    
    # 1. 도메인 체크 ( supercent.io 둘 다 허용)
    allowed_domains = ['@supercent.io']
    allowed_emails = ['rumble@supercent.vn']

    if not any(email.endswith(domain) for domain in allowed_domains) and email not in allowed_emails:
        return False, "🚫 Supercent 계정만 사용 가능합니다"
    
    # 2. 이름 자동 생성 (이메일 앞부분 사용)
    name = email.split('@')[0].capitalize()
    
    # 3. 관리자 지정 (필요하면 이메일 추가)
    admins = [ 'edwin@supercent.io']  # 관리자 이메일 리스트
    role = 'admin' if email in admins else 'user'
    
    # 4. 세션에 저장
    st.session_state.authenticated = True
    st.session_state.user_email = email
    st.session_state.user_name = name
    st.session_state.user_role = role
    st.session_state.login_method = 'google'
    
    _save_session_cookie(email, name, role, 'google')

    # 5. 로그 기록
    log_action(email, 'google', 'login')

    try:
        from modules.upload_automation.upload_logger import log_event
        log_event("login")
    except Exception as e:
        print(f"[upload_logger] {e}")

    return True, f"✅ 환영합니다, {name}님!"




def logout():
    """로그아웃"""
    if 'user_email' in st.session_state:
        log_action(
            st.session_state.user_email,
            st.session_state.get('login_method', 'unknown'),
            'logout'
        )
        try:
            from modules.upload_automation.upload_logger import log_event
            log_event("logout")
        except Exception:
            pass

    for key in ['authenticated', 'user_email', 'user_name', 'user_role', 'login_method']:
        if key in st.session_state:
            del st.session_state[key]


# ========== 로그인 UI ==========
def show_login_page():
    """통합 로그인 페이지 (로그인 폼을 st.container로 감싸서 CSS 적용 가능하게 수정)"""
    st.markdown("""
    <div class="super-crema-header">
        <h1 class="super-crema-title">🎬 SUPER CREMA</h1>
        <p class="super-crema-subtitle">Creative Intelligence Automation Platform</p>
    </div>
    """, unsafe_allow_html=True)
    
    # 중앙 정렬을 위한 컬럼 분리
    col1, col2, col3 = st.columns([2.5, 1.5, 2.5])
    
    with col2:
        # 🚨 여기에 st.container()를 사용하여 모든 로그인 요소를 감쌉니다.
        # 이 컨테이너는 main.py의 CSS가 정확히 타겟팅할 수 있는 대상이 됩니다.
        with st.container(border=False):
            st.markdown("### 🔐 로그인")
            
            # --- 1. Google 로그인 섹션 (상단 배치) ---
            
            st.markdown("##### 🌐 Supercent 계정 로그인 (권장)")
            st.info("🏢  @supercent.io 계정만 사용 가능합니다.")
            
            # OAuth 콜백 처리 (URL에 code가 있으면)
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
            
            # Google 로그인 버튼
            auth_url = get_google_login_url()
            st.markdown("""
            <style>
            /* Google 로그인 버튼 흰색 스타일 */
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
            
            st.markdown("---") # 구분선 추가
            
            # --- 2. ID/Password 로그인 섹션 (하단 배치) ---
            
            st.markdown("##### 🔑 로컬 계정 로그인")
            
            # 폼으로 감싸서 버튼 클릭 시에만 입력 값을 처리하도록 변경
            with st.form(key="local_login_form"):
                username = st.text_input(
                    "Username", 
                    key="local_username_2",
                    placeholder="아이디 입력",
                    label_visibility="visible"
                )
                password = st.text_input(
                    "Password", 
                    type="password",
                    key="local_password_2",
                    placeholder="비밀번호 입력",
                    label_visibility="visible"
                )
                
                # 버튼
                submitted = st.form_submit_button("Login", use_container_width=True)
                
                if submitted:
                    success, message = login_with_password(username, password)
                    
                    if success:
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)












