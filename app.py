"""
Super Crema - Creative Intelligence Platform
"""

import streamlit as st

st.set_page_config(
    page_title="Super Crema",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="collapsed"
)

def apply_theme():
    st.markdown("""
    <style>
    /* Streamlit 상단 툴바 숨기기 */
    header[data-testid="stHeader"] {
        display: none;
    }
    
    /* 상단 여백 제거 */
    .main > div {
        padding-top: 0.5rem !important;
    }
    
    .stApp {
        background: linear-gradient(135deg, #0a0a0a 0%, #1a1a1a 100%);
    }
    
    .super-crema-header {
        background: linear-gradient(90deg, #ff006e 0%, #ff4d8f 50%, #ff006e 100%);
        background-size: 200% auto;
        animation: gradient 3s ease infinite;
        padding: 0.8rem 2rem;
        border-radius: 12px;
        text-align: center;
        margin-bottom: 1rem;
        margin-top: 0;
        box-shadow: 0 4px 20px rgba(255, 0, 110, 0.3);
    }
    
    @keyframes gradient {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    .super-crema-title {
        font-size: 4rem;
        font-weight: 900;
        color: #ffffff;
        text-shadow: 0 0 15px rgba(255, 0, 110, 0.5);
        margin: 0;
        letter-spacing: 1px;
    }
    
    .super-crema-subtitle {
        font-size: 1rem;
        color: #ffffff;
        margin-top: 0.2rem;
        margin-bottom: 0;
        opacity: 0.9;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: rgba(26, 26, 26, 0.6);
        backdrop-filter: blur(10px);
        padding: 8px;
        border-radius: 12px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: linear-gradient(135deg, rgba(255, 0, 110, 0.1) 0%, rgba(255, 77, 143, 0.1) 100%);
        border: 1px solid rgba(255, 0, 110, 0.3);
        border-radius: 10px;
        color: #ffffff;
        font-weight: 600;
        font-size: 1rem;
        padding: 10px 20px;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #ff006e 0%, #ff4d8f 100%) !important;
        border-color: #ff006e !important;
        box-shadow: 0 4px 15px rgba(255, 0, 110, 0.5);
    }
    
    h1, h2, h3 { color: #ffffff !important; }
    p, span, div { color: #cccccc !important; }
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
    render_header()
    
    tab1, tab2, tab3 = st.tabs(["📊 시각화", "🚀 소재 업로드", "🎬 동영상 제작"])
    
    with tab1:
        try:
            from modules.visualization import main as viz_main
            viz_main.run()
        except Exception as e:
            st.error(f"시각화 모듈 로드 실패: {str(e)}")
    
    with tab2:
        st.info("🚧 업로드 자동화 모듈 개발 예정")
    
    with tab3:
        st.info("🚧 동영상 자동화 모듈 개발 예정")
    
    st.markdown("---")
    st.caption("© 2025 Super Crema - Supercent Marketing Intelligence Team")

if __name__ == "__main__":
    main()







