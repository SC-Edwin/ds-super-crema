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
    
    /* ========== 배경 ========== */
    .stApp {
        background: linear-gradient(135deg, #0a0a0a 0%, #1a1a1a 100%) !important;
    }
    
    /* ========== 헤더 (최우선) ========== */
    .super-crema-header {
        background: linear-gradient(90deg, #ff006e 0%, #ff4d8f 50%, #ff006e 100%) !important;
        background-size: 200% auto !important;
        animation: gradient 3s ease infinite !important;
        padding: 0.8rem 2rem !important;
        border-radius: 12px !important;
        text-align: center !important;
        margin-bottom: 1rem !important;
        margin-top: 0 !important;
        box-shadow: 0 4px 20px rgba(255, 0, 110, 0.3) !important;
        position: relative !important;
        z-index: 100 !important;
    }
    
    @keyframes gradient {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    .super-crema-title {
        font-size: 4rem !important;
        font-weight: 900 !important;
        color: #ffffff !important;
        text-shadow: 0 0 15px rgba(255, 0, 110, 0.5) !important;
        margin: 0 !important;
        letter-spacing: 1px !important;
    }
    
    .super-crema-subtitle {
        font-size: 1rem !important;
        color: #ffffff !important;
        margin-top: 0.2rem !important;
        margin-bottom: 0 !important;
        opacity: 0.9 !important;
    }
    
    /* ========== 탭 ========== */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px !important;
        background: rgba(26, 26, 26, 0.6) !important;
        backdrop-filter: blur(10px) !important;
        padding: 8px !important;
        border-radius: 12px !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: linear-gradient(135deg, rgba(255, 0, 110, 0.1) 0%, rgba(255, 77, 143, 0.1) 100%) !important;
        border: 1px solid rgba(255, 0, 110, 0.3) !important;
        border-radius: 10px !important;
        color: #ffffff !important;
        font-weight: 600 !important;
        font-size: 1rem !important;
        padding: 10px 20px !important;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #ff006e 0%, #ff4d8f 100%) !important;
        border-color: #ff006e !important;
        box-shadow: 0 4px 15px rgba(255, 0, 110, 0.5) !important;
    }
    
    /* ========== 3D Floating Cards ========== */
    
    /* 메트릭 카드 */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(255, 0, 110, 0.1) 0%, rgba(26, 26, 26, 0.8) 100%) !important;
        backdrop-filter: blur(10px) !important;
        border: 1px solid rgba(255, 0, 110, 0.3) !important;
        border-radius: 16px !important;
        padding: 1.5rem !important;
        box-shadow: 
            0 8px 32px rgba(0, 0, 0, 0.4),
            0 0 20px rgba(255, 0, 110, 0.2) !important;
        transform-style: preserve-3d !important;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1) !important;
    }
    
    [data-testid="stMetric"]:hover {
        transform: translateY(-10px) rotateX(5deg) !important;
        box-shadow: 
            0 20px 50px rgba(0, 0, 0, 0.6),
            0 0 40px rgba(255, 0, 110, 0.5) !important;
        border-color: rgba(255, 0, 110, 0.6) !important;
    }
    
    /* Plotly 차트 */
    .js-plotly-plot {
        background: linear-gradient(135deg, rgba(255, 0, 110, 0.05) 0%, rgba(26, 26, 26, 0.9) 100%) !important;
        backdrop-filter: blur(10px) !important;
        border: 1px solid rgba(255, 0, 110, 0.2) !important;
        border-radius: 16px !important;
        padding: 1rem !important;
        box-shadow: 
            0 8px 32px rgba(0, 0, 0, 0.4),
            0 0 20px rgba(255, 0, 110, 0.15) !important;
        transform-style: preserve-3d !important;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1) !important;
    }
    
    .js-plotly-plot:hover {
        transform: translateY(-8px) scale(1.02) !important;
        box-shadow: 
            0 20px 50px rgba(0, 0, 0, 0.6),
            0 0 40px rgba(255, 0, 110, 0.4) !important;
        border-color: rgba(255, 0, 110, 0.5) !important;
    }
    
    /* DataFrame */
    [data-testid="stDataFrame"] {
        background: linear-gradient(135deg, rgba(255, 0, 110, 0.05) 0%, rgba(26, 26, 26, 0.9) 100%) !important;
        backdrop-filter: blur(10px) !important;
        border: 1px solid rgba(255, 0, 110, 0.2) !important;
        border-radius: 16px !important;
        padding: 1rem !important;
        box-shadow: 
            0 8px 32px rgba(0, 0, 0, 0.4),
            0 0 20px rgba(255, 0, 110, 0.15) !important;
        transform-style: preserve-3d !important;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1) !important;
    }
    
    [data-testid="stDataFrame"]:hover {
        transform: translateY(-5px) !important;
        box-shadow: 
            0 15px 40px rgba(0, 0, 0, 0.5),
            0 0 30px rgba(255, 0, 110, 0.3) !important;
    }
    
    /* 입력 요소 */
    [data-baseweb="select"],
    [data-baseweb="input"] {
        background: rgba(26, 26, 26, 0.8) !important;
        backdrop-filter: blur(10px) !important;
        border: 1px solid rgba(255, 0, 110, 0.3) !important;
        border-radius: 12px !important;
        transition: all 0.3s ease !important;
    }
    
    [data-baseweb="select"]:hover,
    [data-baseweb="input"]:hover {
        border-color: rgba(255, 0, 110, 0.6) !important;
        box-shadow: 0 0 20px rgba(255, 0, 110, 0.3) !important;
    }
    
    /* ========== 텍스트 색상 ========== */
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
    
    tab1, tab2, tab3, tab4 = st.tabs(["📊 모델링 시각화", "🚀 소재 업로드", "🎬 동영상 제작", "🌐 로컬라이징"])
    
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

    with tab4:
        st.info("🚧 로컬라이징 자동화 Comming Soon 12/12")        
    
    st.markdown("---")
    st.caption("© 2025 Super Crema - Supercent Marketing Intelligence Team")

if __name__ == "__main__":
    main()







