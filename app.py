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
    /* 기존 코드 유지... */
    
    /* ========== 3D Floating Cards 효과 추가 ========== */
    
    /* 모든 stMetric (지표 카드) */
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
    
    /* Plotly 차트 컨테이너 */
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
    
    /* DataFrame 테이블 */
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
    
    /* Selectbox, Multiselect 등 입력 요소 */
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
    
    /* ================================================ */
    
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







