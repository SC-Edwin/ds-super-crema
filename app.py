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
        padding-top: 0rem !important;
    }

    .main {
        padding-top: 0rem !important;
    }

    .block-container {
        padding-top: 0rem !important;
    }
                                    
    
    .stApp {
        background: linear-gradient(135deg, #0a0a0a 0%, #1a1a1a 100%);
    }
    
    .super-crema-header {
        background: linear-gradient(90deg, rgba(15, 15, 30, 0.85) 0%, rgba(26, 26, 46, 0.9) 50%, rgba(15, 15, 30, 0.85) 100%);
        background-size: 200% auto;
        backdrop-filter: blur(10px);
        animation: gradient 3s ease infinite;                
        padding: 0.8rem 2rem;
        border-radius: 16px;
        text-align: center;
        margin-bottom: 1rem;
        margin-top: 0;
        
        /* 3D 입체감 */
        box-shadow: 
            0 4px 16px rgba(255, 0, 110, 0.25),
            0 8px 25px rgba(255, 0, 110, 0.15),
            inset 0 2px 10px rgba(255, 255, 255, 0.1);
        
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-bottom: 2px solid rgba(0, 0, 0, 0.2);
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
    
    /* ========== 3D Floating Cards ========== */
    
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(255, 0, 110, 0.1) 0%, rgba(26, 26, 26, 0.8) 100%);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 0, 110, 0.3);
        border-radius: 16px;
        padding: 1.5rem;
        box-shadow: 
            0 8px 32px rgba(0, 0, 0, 0.4),
            0 0 20px rgba(255, 0, 110, 0.2);
        transform-style: preserve-3d;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    }
    
    [data-testid="stMetric"]:hover {
        transform: translateY(-10px) rotateX(5deg);
        box-shadow: 
            0 20px 50px rgba(0, 0, 0, 0.6),
            0 0 40px rgba(255, 0, 110, 0.5);
        border-color: rgba(255, 0, 110, 0.6);
    }
    
    .js-plotly-plot {
        background: linear-gradient(135deg, rgba(255, 0, 110, 0.05) 0%, rgba(26, 26, 26, 0.9) 100%) !important;
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 0, 110, 0.2);
        border-radius: 16px;
        padding: 1rem;
        box-shadow: 
            0 8px 32px rgba(0, 0, 0, 0.4),
            0 0 20px rgba(255, 0, 110, 0.15);
        transform-style: preserve-3d;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    }
    
    .js-plotly-plot:hover {
        transform: translateY(-8px) scale(1.02);
        box-shadow: 
            0 20px 50px rgba(0, 0, 0, 0.6),
            0 0 40px rgba(255, 0, 110, 0.4);
        border-color: rgba(255, 0, 110, 0.5);
    }
    
    [data-testid="stDataFrame"] {
        background: linear-gradient(135deg, rgba(255, 0, 110, 0.05) 0%, rgba(26, 26, 26, 0.9) 100%);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 0, 110, 0.2);
        border-radius: 16px;
        padding: 1rem;
        box-shadow: 
            0 8px 32px rgba(0, 0, 0, 0.4),
            0 0 20px rgba(255, 0, 110, 0.15);
        transform-style: preserve-3d;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    }
    
    [data-testid="stDataFrame"]:hover {
        transform: translateY(-5px);
        box-shadow: 
            0 15px 40px rgba(0, 0, 0, 0.5),
            0 0 30px rgba(255, 0, 110, 0.3);
    }
    
    [data-baseweb="select"],
    [data-baseweb="input"] {
        background: rgba(26, 26, 26, 0.8) !important;
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 0, 110, 0.3) !important;
        border-radius: 12px;
        transition: all 0.3s ease;
    }
    
    [data-baseweb="select"]:hover,
    [data-baseweb="input"]:hover {
        border-color: rgba(255, 0, 110, 0.6) !important;
        box-shadow: 0 0 20px rgba(255, 0, 110, 0.3);
    }
    
    h1, h2, h3 { color: #ffffff !important; }
    p, span, div { color: #cccccc !important; }
    
    /* ========== Heny & Kyle 버튼 (블랙핑크 스타일) ========== */
    div[data-testid="stButton"] button,
    .stButton > button {
        width: 55px !important;
        height: 55px !important;
        min-width: 55px !important;
        min-height: 55px !important;
        border-radius: 50% !important;
        padding: 8px !important;
        
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%) !important;
        border: 2px solid #ff006e !important;
        
        box-shadow: 
            0 4px 15px rgba(0, 0, 0, 0.8),
            0 0 20px rgba(255, 0, 110, 0.4),
            inset 0 2px 8px rgba(255, 255, 255, 0.1) !important;
        
        transition: all 0.3s ease !important;
    }

    div[data-testid="stButton"] button p,
    .stButton > button p {
        font-size: 12px !important;
        font-weight: 700 !important;
        line-height: 1.1 !important;
        letter-spacing: 0.5px !important;
        white-space: pre-line !important;
        color: #ff006e !important;
        text-shadow: 
            0 0 10px rgba(255, 0, 110, 0.6),
            0 0 20px rgba(255, 0, 110, 0.3) !important;
        margin: 0 !important;
        padding: 0 !important;
    }

    div[data-testid="stButton"] button:hover,
    .stButton > button:hover {
        transform: translateY(-3px) scale(1.08) !important;
        background: linear-gradient(135deg, #2a1a3e 0%, #261e4e 50%, #1f4470 100%) !important;
        border-color: #ff4d8f !important;
        box-shadow: 
            0 8px 25px rgba(0, 0, 0, 0.9),
            0 0 35px rgba(255, 0, 110, 0.7),
            inset 0 3px 10px rgba(255, 0, 110, 0.2) !important;
    }

    div[data-testid="stButton"] button:hover p,
    .stButton > button:hover p {
        color: #ff77a0 !important;
        text-shadow: 
            0 0 15px rgba(255, 0, 110, 0.8),
            0 0 25px rgba(255, 0, 110, 0.4) !important;
    }

    div[data-testid="stButton"] button:active,
    .stButton > button:active {
        transform: translateY(-1px) scale(1.03) !important;
    }

    /* ========== Selectbox 안정화 (그림자 버그 제거) ========== */
    div[data-baseweb="select"] {
        transition: none !important;
    }

    div[data-baseweb="select"] > div {
        transition: none !important;
        box-shadow: none !important;
    }

    div[data-baseweb="select"]:hover {
        box-shadow: none !important;
    }
    /* ======================================================= */
                
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
    
    tab1, tab2, tab3, tab4 = st.tabs(["📊 모델링 시각화", "🚀 소재 업로드", "🌐 로컬라이징", "🎬 동영상 제작"])
    
    with tab1:
        try:
            from modules.visualization import main as viz_main
            viz_main.run()
        except Exception as e:
            st.error(f"시각화 모듈 로드 실패: {str(e)}")
    
    # with tab2:
    #     st.info("🚧 업로드 자동화 모듈 개발 예정")

    with tab2:
            try:
                from modules.upload_automation import main as upload_main
                upload_main.run()
            except Exception as e:
                st.error(f"소재 업로드 모듈 로드 실패: {str(e)}")
                import traceback
                st.code(traceback.format_exc())


    
    with tab3:
        st.info("🚧 동영상 자동화 모듈 개발 예정")

    with tab4:
        st.info("🚧 로컬라이징 자동화 Comming Soon 12/12")        
    
    st.markdown("---")
    st.caption("© 2025 Super Crema - Supercent Marketing Intelligence Team")

if __name__ == "__main__":
    main()












