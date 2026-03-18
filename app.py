"""
Super Crema - Creative Intelligence Platform
"""

import random
import streamlit as st
import extra_streamlit_components as stx
from modules.auth_simple import check_authentication, show_login_page, logout, log_action


if '_cookie_ctrl' not in st.session_state:
    st.session_state._cookie_ctrl = stx.CookieManager()


# 세션당 1회 생성, bootstrap rerun으로 브라우저 쿠키 로딩 보장


def get_random_animal_emoji():
    animal_emojis = [
        "🐶", "🐱", "🐭", "🐹", "🐰", "🦊", "🐻", "🐼", "🐨", "🐯",
        "🦁", "🐮", "🐷", "🐸", "🐒", "🐔", "🐧", "🦉", "🦋", "🦄",
        "🐘", "🦒", "🦓", "🦔", "🐕", "🐈", "🐇", "🐿️", "🦝", "🦛"
    ]
    return random.choice(animal_emojis)


st.set_page_config(
    page_title="Super Crema",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="collapsed"
)


def apply_theme():
    st.markdown("""
    <style>
    header[data-testid="stHeader"] { display: none; }
    .main > div, .main, .block-container { padding-top: 0rem !important; }
    .stApp { background: linear-gradient(135deg, #0a0a0a 0%, #1a1a1a 100%); }

    .super-crema-header {
        background: linear-gradient(90deg, rgba(15,15,30,0.85) 0%, rgba(26,26,46,0.9) 50%, rgba(15,15,30,0.85) 100%);
        background-size: 200% auto;
        backdrop-filter: blur(10px);
        animation: gradient 3s ease infinite;
        padding: 0.8rem 2rem;
        border-radius: 16px;
        text-align: center;
        margin-bottom: 1rem;
        margin-top: 0;
        box-shadow: 0 4px 16px rgba(255,0,110,0.25), 0 8px 25px rgba(255,0,110,0.15), inset 0 2px 10px rgba(255,255,255,0.1);
        border: 1px solid rgba(255,255,255,0.1);
        border-bottom: 2px solid rgba(0,0,0,0.2);
    }
    @keyframes gradient {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    .super-crema-title { font-size: 4rem; font-weight: 900; color: #ffffff; text-shadow: 0 0 15px rgba(255,0,110,0.5); margin: 0; letter-spacing: 1px; }
    .super-crema-subtitle { font-size: 1rem; color: #ffffff; margin-top: 0.2rem; margin-bottom: 0; opacity: 0.9; }

    .stTabs [data-baseweb="tab-list"] { gap: 8px; background: rgba(26,26,26,0.6); backdrop-filter: blur(10px); padding: 8px; border-radius: 12px; }
    .stTabs [data-baseweb="tab"] { background: linear-gradient(135deg, rgba(255,0,110,0.1) 0%, rgba(255,77,143,0.1) 100%); border: 1px solid rgba(255,0,110,0.3); border-radius: 10px; color: #ffffff; font-weight: 600; font-size: 1rem; padding: 10px 20px; }
    .stTabs [aria-selected="true"] { background: linear-gradient(135deg, #ff006e 0%, #ff4d8f 100%) !important; border-color: #ff006e !important; box-shadow: 0 4px 15px rgba(255,0,110,0.5); }

    [data-testid="stMetric"] { background: linear-gradient(135deg, rgba(255,0,110,0.1) 0%, rgba(26,26,26,0.8) 100%); backdrop-filter: blur(10px); border: 1px solid rgba(255,0,110,0.3); border-radius: 16px; padding: 1.5rem; box-shadow: 0 8px 32px rgba(0,0,0,0.4), 0 0 20px rgba(255,0,110,0.2); transform-style: preserve-3d; transition: all 0.4s cubic-bezier(0.4,0,0.2,1); }
    [data-testid="stMetric"]:hover { transform: translateY(-10px) rotateX(5deg); box-shadow: 0 20px 50px rgba(0,0,0,0.6), 0 0 40px rgba(255,0,110,0.5); border-color: rgba(255,0,110,0.6); }

    .js-plotly-plot { background: linear-gradient(135deg, rgba(255,0,110,0.05) 0%, rgba(26,26,26,0.9) 100%) !important; backdrop-filter: blur(10px); border: 1px solid rgba(255,0,110,0.2); border-radius: 16px; padding: 1rem; box-shadow: 0 8px 32px rgba(0,0,0,0.4), 0 0 20px rgba(255,0,110,0.15); transform-style: preserve-3d; transition: all 0.4s cubic-bezier(0.4,0,0.2,1); }
    .js-plotly-plot:hover { transform: translateY(-8px) scale(1.02); box-shadow: 0 20px 50px rgba(0,0,0,0.6), 0 0 40px rgba(255,0,110,0.4); border-color: rgba(255,0,110,0.5); }

    [data-testid="stDataFrame"] { background: linear-gradient(135deg, rgba(255,0,110,0.05) 0%, rgba(26,26,26,0.9) 100%); backdrop-filter: blur(10px); border: 1px solid rgba(255,0,110,0.2); border-radius: 16px; padding: 1rem; box-shadow: 0 8px 32px rgba(0,0,0,0.4), 0 0 20px rgba(255,0,110,0.15); transform-style: preserve-3d; transition: all 0.4s cubic-bezier(0.4,0,0.2,1); }
    [data-testid="stDataFrame"]:hover { transform: translateY(-5px); box-shadow: 0 15px 40px rgba(0,0,0,0.5), 0 0 30px rgba(255,0,110,0.3); }

    [data-baseweb="select"], [data-baseweb="input"] { background: rgba(26,26,26,0.8) !important; backdrop-filter: blur(10px); border: 1px solid rgba(255,0,110,0.3) !important; border-radius: 12px; transition: all 0.3s ease; }
    input[type="text"], input[type="password"] { pointer-events: auto !important; user-select: text !important; -webkit-user-select: text !important; }
    [data-baseweb="select"]:hover, [data-baseweb="input"]:hover { border-color: rgba(255,0,110,0.6) !important; box-shadow: 0 0 20px rgba(255,0,110,0.3); }

    h1, h2, h3 { color: #ffffff !important; }
    p, span, div { color: #cccccc !important; }

    div[data-baseweb="select"] { transition: none !important; }
    div[data-baseweb="select"] > div { transition: none !important; box-shadow: none !important; }
    div[data-baseweb="select"]:hover { box-shadow: none !important; }

    [data-testid="column"]:nth-child(2) [data-testid="stVerticalBlock"] {
        background: rgba(10,10,20,0.95) !important;
        backdrop-filter: blur(15px) !important;
        padding: 3rem 2.5rem !important;
        border-radius: 20px !important;
        border: 2px solid rgba(255,0,110,0.4) !important;
        box-shadow: 0 0 100px rgba(255,0,110,0.5) !important, 0 0 0 5px rgba(255,255,255,0.05) !important;
        transform: perspective(1px) translateZ(0) !important;
        transition: all 0.5s ease-out !important;
    }
    [data-testid="column"]:nth-child(2) [data-testid="stVerticalBlock"]:hover {
        box-shadow: 0 0 150px rgba(255,0,110,0.6) !important, 0 0 0 5px rgba(255,255,255,0.1) !important;
        transform: translateY(-5px) !important;
    }
    </style>
    """, unsafe_allow_html=True)


def render_header():
    st.markdown("""
    <div class="super-crema-header">
        <h1 class="super-crema-title">🎬 SUPER CREMA</h1>
        <p class="super-crema-subtitle">Creative Intelligence Automation Platform</p>
    </div>
    """, unsafe_allow_html=True)


def main():
    apply_theme()



    if not check_authentication():
        if 'logout' in st.query_params:
            st.query_params.clear()
        show_login_page()
        return

    render_header()

    if 'logout' in st.query_params:
        logout()
        st.query_params.clear()
        st.rerun()
        return

    col1, col2 = st.columns([8.5, 1.5])
    emoji = get_random_animal_emoji()
    method_emoji = "🔑" if st.session_state.get('login_method') == 'password' else "🌐"

    with col1:
        pass
    with col2:
        st.markdown(f"""
        <div style="text-align: right; line-height: 1.3; margin-top: 5px;">
            <p style="font-size: 0.8rem; margin: 0; color: #fff;">
                {emoji} {st.session_state.get('user_name', '')} ({st.session_state.get('user_role', '')})
            </p>
            <p style="font-size: 0.7rem; margin: 0; color: #ccc; opacity: 0.9;">
                {method_emoji} {st.session_state.get('login_method', '')}
                <a href="?logout=true" style="color: #ff006e; margin-left: 5px; text-decoration: none; font-weight: 600;">
                    🚪 Logout
                </a>
            </p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "📊 Performance M/L",
        "👁️ Creative Upload",
        "📊 Video M/L",
        "🌐 Localization",
        "🎬 Video Generation",
        "🇻🇳 Creative Upload - Vietnam",
        "🇻🇳 Performance M/L - Vietnam",
    ])

    with tab1:
        log_action(st.session_state.get('user_email', ''), st.session_state.get('login_method', ''), 'access_performance_ml')
        st.markdown('<div id="viz-root">', unsafe_allow_html=True)
        from modules.visualization import main as viz_main
        viz_main.run()
        st.markdown('</div>', unsafe_allow_html=True)

    with tab2:
        try:
            from modules.upload_automation import main as upload_main
            upload_main.run()
        except Exception as e:
            st.error(f"소재 업로드 모듈 로드 실패: {str(e)}")
            import traceback
            st.code(traceback.format_exc())

    with tab3:
        st.info("🚧 Comming Soon")

    with tab4:
        st.markdown("""
        <div style="text-align: center; padding: 3rem;">
            <h2 style="color: #ffffff; margin-bottom: 1rem;">🌐 Localization 리다이렉팅</h2>
            <p style="color: #cccccc; font-size: 1rem; margin-bottom: 2rem;">동영상 편집, 생성 서버와 연결하려면 아래 버튼을 클릭하세요.</p>
        </div>
        """, unsafe_allow_html=True)
        col1, col2, col3 = st.columns([2, 2, 2])
        with col2:
            st.link_button("🌐 이동", "https://creative-crema.web.app/", use_container_width=True, type="primary")

    with tab5:
        st.info("🚧 Comming Soon")

    with tab6:
        st.markdown('<div id="upload-vn-root">', unsafe_allow_html=True)
        from modules.upload_automation import vietnam as vietnam_main
        vietnam_main.run()
        st.markdown('</div>', unsafe_allow_html=True)

    with tab7:
        log_action(st.session_state.get('user_email', ''), st.session_state.get('login_method', ''), 'access_performance_ml_vn')
        st.markdown('<div id="viz-vn-root">', unsafe_allow_html=True)
        try:
            from modules.visualization import main as viz_main
            viz_main.run(test_market='VN', key_prefix='vn')
        except Exception as e:
            st.error(f"VN 탭 오류: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.caption("© 2025 Super Crema - Supercent Marketing Intelligence Team")





if __name__ == "__main__":
    main()



