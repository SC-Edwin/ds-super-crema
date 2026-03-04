"""
시각화 모듈
Creative Performance Prediction 시각화 대시보드

Last updated: 2024-12-02
Author: Edwin
"""



import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from google.cloud import bigquery

from datetime import datetime, timedelta
import pandas as pd
import streamlit.components.v1 as components





def get_friday_based_week(date):
    """
    금요일 기준 주차 계산
    
    Args:
        date: datetime or string (YYYY-MM-DD)
    
    Returns:
        str: 'YYYY-Wnn' 형식 (예: '2024-W49')
    """
    if isinstance(date, str):
        date = pd.to_datetime(date)
    
    if pd.isna(date):
        return None
    
    # 해당 날짜가 속한 주의 금요일 찾기
    # weekday(): 월=0, 화=1, 수=2, 목=3, 금=4, 토=5, 일=6
    days_since_friday = (date.weekday() - 4) % 7
    week_friday = date - timedelta(days=days_since_friday)
    
    # ISO 주차 형식 (YYYY-Wnn)
    year = week_friday.year
    week_num = week_friday.isocalendar()[1]
    
    return f"{year}-W{week_num:02d}"


def get_week_label(week_str, reference_weeks):
    """
    주차 코드를 날짜+요일 레이블로 변환
    예: '2026-W05' → '2026-01-30 (금)'
    """
    if not week_str:
        return week_str
    
    try:
        year = int(week_str.split('-W')[0])
        week_num = int(week_str.split('-W')[1])
        
        # ISO 주차의 금요일 날짜 계산
        jan4 = datetime(year, 1, 4)
        week1_friday = jan4 + timedelta(days=(4 - jan4.weekday()))
        target_friday = week1_friday + timedelta(weeks=(week_num - 1))
        
        # 요일 한글
        day_names = ['월', '화', '수', '목', '금', '토', '일']
        day_name = day_names[target_friday.weekday()]
        
        date_label = f"{target_friday.strftime('%Y-%m-%d')} ({day_name})"
        
    except:
        date_label = week_str
    
    # 이번주/전주/전전주 표시
    return date_label

    






# ================================
# BigQuery 연결
# ================================
@st.cache_resource
def get_bigquery_client():
    """BigQuery 클라이언트 초기화"""
    from google.oauth2 import service_account
    
    # Streamlit Cloud
    try:
        if "gcp_service_account" in st.secrets:  # ← Secrets 있으면
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(
                credentials=credentials,
                project=st.secrets["gcp_service_account"]["project_id"]
            )
    except Exception as e:
        pass  # ← Secrets 없으면 넘어감
    
    # 로컬 (Application Default Credentials)
    return bigquery.Client(project='roas-test-456808')  # ← 로컬 인증 사용






@st.cache_data(ttl=300)
def load_prediction_data():
    """최신 예측 결과 데이터 로드"""
    client = get_bigquery_client()
    
    query = """
        WITH WeekendData AS (
        SELECT *
        FROM `roas-test-456808.marketing_datascience.creative_performance_high_performing_predicted`
        WHERE 
            prediction_timestamp = (
            SELECT MAX(prediction_timestamp)
            FROM `roas-test-456808.marketing_datascience.creative_performance_high_performing_predicted`
            )
        ),
        LatestSnapshot AS (
        SELECT *
        FROM (
            SELECT
            *,
            impressions_1 + impressions_2 + impressions_3 as sum_impressions,
            installs_1 + installs_2 + installs_3 as sum_installs,
            clicks_1 + clicks_2 + clicks_3 as sum_clicks,
            ROUND(cost_1 + cost_2 + cost_3,2) as sum_costs,
            COALESCE(ROUND(SAFE_DIVIDE((cost_1 + cost_2 + cost_3), (installs_1 + installs_2 + installs_3)),2),0) as sum_CPI,
            ROW_NUMBER() OVER (
                PARTITION BY subject, network, app, past_network, future_locality
                ORDER BY SAFE_CAST(prediction_timestamp AS TIMESTAMP) DESC
            ) AS row_num
            FROM WeekendData
        )
        WHERE row_num = 1
        )
        SELECT
        subject,
        COALESCE(
            REGEXP_EXTRACT(subject, r'(-?\\d+)'),
            subject
        ) AS subject_label,
        network,
        app,
        locality,
        future_locality,
        day_1,
        day_2,
        day_3,
        prediction_score,  
        ranking_score,
        past_network,
        sum_impressions,
        sum_installs,
        sum_clicks,
        sum_costs,
        sum_CPI,
        roas_sum_1to3,
        ROUND(SAFE_DIVIDE(sum_installs * 1000, sum_impressions), 2) as IPM,
        ROUND(SAFE_DIVIDE(sum_clicks * 100, sum_impressions), 2) as CTR,
        ROUND(SAFE_DIVIDE(sum_installs * 100, sum_clicks), 2) as CVR,
        ROUND(SAFE_DIVIDE(sum_installs * 100, sum_impressions), 2) as CVR_IMP,
        retention_rate_sum_1to3,
        engagement_quality_2,
        ROW_NUMBER() OVER (
            PARTITION BY app, past_network, network, future_locality
            ORDER BY ranking_score DESC
        ) AS rank_per_network
        FROM LatestSnapshot
        """
    
    df = client.query(query).to_dataframe()
    return df




def create_plotly_theme():
    """Plotly 차트 테마 - 블랙 + 핑크 통일"""
    return {
        'template': 'plotly_dark',
        'paper_bgcolor': 'rgba(26, 26, 26, 0.6)',
        'plot_bgcolor': 'rgba(20, 20, 20, 0.5)',
        'font': {'color': '#ffffff', 'family': 'Arial', 'size': 11},
        'colorway': ['#ff006e', '#ff4d8f', '#ff77a0', '#a855f7', '#8b00ff']
    }




def run():
    """시각화 모듈 메인"""
    
    st.markdown("""
        <style>
        /* 🔥 viz 탭 한눈에 보기 버튼 - 초강력 격리 (우선순위 9999) */
        body div[id="viz-root"] .st-key-ai_btn button,
        body .st-key-ai_btn button[data-testid="stBaseButton-secondary"],
        body .st-key-ai_btn button[kind="secondary"] {
            /* 배경 & 색상 */
            background: rgba(26, 26, 26, 0.8) !important;
            color: #ffffff !important;
            
            /* 테두리 & 그림자 */
            border: 2px solid #ff006e !important;
            border-radius: 8px !important;
            box-shadow: 
                0 0 10px rgba(255, 0, 110, 0.4),
                0 0 20px rgba(255, 0, 110, 0.2),
                inset 0 0 10px rgba(255, 0, 110, 0.1) !important;
            
            /* 크기 & 패딩 */
            width: auto !important;
            max-width: 120px !important;
            min-width: 0 !important;
            height: auto !important;
            min-height: 0 !important;
            padding: 0.4rem 1rem !important;
            
            /* 폰트 */
            font-size: 0.9rem !important;
            font-weight: 600 !important;
            
            /* 애니메이션 */
            transition: all 0.3s ease !important;
            transform: none !important;
        }

        /* 호버 */
        body div[id="viz-root"] .st-key-ai_btn button:hover,
        body .st-key-ai_btn button[data-testid="stBaseButton-secondary"]:hover,
        body .st-key-ai_btn button[kind="secondary"]:hover {
            background: rgba(26, 26, 26, 0.95) !important;
            border-color: #ff4d8f !important;
            box-shadow: 
                0 0 15px rgba(255, 0, 110, 0.6),
                0 0 30px rgba(255, 77, 143, 0.4),
                0 0 45px rgba(255, 0, 110, 0.2),
                inset 0 0 15px rgba(255, 0, 110, 0.15) !important;
            transform: translateY(-2px) !important;
        }
        
        /* 텍스트 스타일 강제 */
        body .st-key-ai_btn button p {
            font-size: 0.9rem !important;
            font-weight: 600 !important;
            color: #ffffff !important;
            text-shadow: none !important;
            letter-spacing: 0 !important;
            line-height: 1.4 !important;
            margin: 0 !important;
            padding: 0 !important;
            white-space: nowrap !important;
        }
        
        /* 여백 */
        body .st-key-ai_btn {
            margin-top: -0.5rem !important;
        }
        
        /* 🚨 업로드 탭 스타일 무효화 (최고 우선순위) */
        body #upload-root .st-key-ai_btn button {
            all: revert !important;
        }
        </style>

    """, unsafe_allow_html=True)
        
    
    st.markdown("## 🥇 Top Creatives by Network")
    
    # 데이터 로드
    with st.spinner("🔄 데이터 로딩 중..."):
        try:
            df = load_prediction_data()
            # st.success(f"✅ {len(df)}개 크리에이티브 로드 완료!")
        except Exception as e:
            st.error(f"❌ 데이터 로드 실패: {str(e)}")
            st.info("💡 GCP 인증이 필요합니다.")
            st.code("gcloud auth application-default login")
            return
        
    # ========== 주차 계산 추가 ==========
    # day_1 기준으로 업로드 주차 계산
    df['upload_week'] = df['day_1'].apply(get_friday_based_week)
    
    # ========== Locality 이모지 라벨 ==========
    df['subject_label_emoji'] = df.apply(
        lambda x: f"{x['subject_label']} 🇺🇸" if x['future_locality'] == 'US' 
                  else f"{x['subject_label']} 🌍", 
        axis=1
    )
                
    
    # 현재 기준 주차들 계산
    today = datetime.now()
    reference_weeks = {
        'this': get_friday_based_week(today),
        'last': get_friday_based_week(today - timedelta(weeks=1)),
        'two_ago': get_friday_based_week(today - timedelta(weeks=2))
    }
    # ====================================


    
    # 필터 (메인 화면 왼쪽)
    st.markdown("### 🔍 Filter")

    col1, col2, col3, col_spacer = st.columns([1.2, 1.2, 1.5, 4])



    with col1:
            all_apps = ['All'] + sorted(df['app'].unique().tolist())
            selected_app = st.selectbox("📱 App", all_apps)

                        
            clicked_hk = st.button(
                "One Click View",
                key="ai_btn",
                help="AI-powered quick insights"
            )
                        

            if clicked_hk:
                st.session_state['show_ai_recommendation'] = True
                        


    with col2:
            all_future_localities = ['All'] + sorted(df['future_locality'].dropna().unique().tolist())
            selected_future_locality = st.selectbox("🎯 투자 지역", all_future_localities)

    with col3:
        # 주차 목록 (최신순, None 제외)
        available_weeks = sorted(
            [w for w in df['upload_week'].unique() if w is not None], 
            reverse=True
        )
        
        # 레이블 → 주차코드 매핑 생성
        week_label_to_code = {'All': 'All'}
        week_options = ['All']
        
        for w in available_weeks:
            label = get_week_label(w, reference_weeks)
            week_label_to_code[label] = w
            week_options.append(label)
        
        
        selected_week_label = st.selectbox("📅 업로드 날짜", week_options)

        
        # 레이블 → 실제 주차 코드 변환
        selected_week = week_label_to_code.get(selected_week_label, 'All')


    # 필터 적용
    filtered_df = df.copy()
    if selected_app != 'All':
        filtered_df = filtered_df[filtered_df['app'] == selected_app]

    if selected_future_locality != 'All':
            filtered_df = filtered_df[filtered_df['future_locality'] == selected_future_locality]

    if selected_week != 'All':
        # 디버깅 로그
        print(f"[DEBUG] selected_week_label: {selected_week_label}")
        print(f"[DEBUG] selected_week (코드): {selected_week}")
        print(f"[DEBUG] df의 upload_week 값들: {df['upload_week'].unique().tolist()}")
        
        filtered_df = filtered_df[filtered_df['upload_week'] == selected_week]

    if len(filtered_df) == 0:
        st.warning("⚠️ 선택한 조건에 맞는 데이터가 없습니다.")
        return
        




    # ========== 테스트 소재 목록 ==========
    unique_subjects = filtered_df['subject_label'].unique()
    # 숫자 정렬 (001, 002, 003 순서로)
    unique_subjects_sorted = sorted(unique_subjects, key=lambda x: int(x) if x.isdigit() else float('inf'))
    subject_count = len(unique_subjects_sorted)


    # video001 형식으로 변환
    subject_list_display = ', '.join([f"video{str(s).zfill(3)}" for s in unique_subjects_sorted[:25]])
    if subject_count > 25:  # ← 여기도 25로
        subject_list_display += f" ... (+{subject_count - 25}개 더)"

    st.info(f"📋 **테스트 소재 ({subject_count}개):** {subject_list_display}")    



    # ========== 팝업 모달 (Dialog) ==========
    
    @st.dialog("One Click View - AI Recommendations", width="large")
    def show_ai_modal(filtered_df, selected_app, selected_locality, selected_week_label):

        
        """AI 추천 모달"""
        
        app_text = selected_app if selected_app != 'All' else '전체'
        loc_text = selected_locality if selected_locality != 'All' else '전체'
        week_text = selected_week_label if selected_week_label != 'All' else '전체 주차'
        
        st.markdown(f"**{app_text}** × **{loc_text}** × **{week_text}** - {len(filtered_df)}개 소재 분析")
        
        st.markdown("---")

        

        
        # best_per_creative['gap'] = best_per_creative.apply(get_score_gap, axis=1)

        # 모든 데이터 사용 (네트워크별 전체 소재)
        best_per_creative = filtered_df.copy()

        best_per_creative['path'] = (
            best_per_creative['past_network'] + ' → ' + 
            best_per_creative['network']
        )

        # gap 계산 (필요 없지만 컬럼 유지)
        best_per_creative['gap'] = 0.0

        # 테이블
        st.markdown("### 📊 소재별 최적 투자 경로")

        # 모든 네트워크 데이터 사용
        # 수정: 확률에 패널티 적용
        all_data = filtered_df.copy()
        all_data['path'] = all_data['past_network'] + ' → ' + all_data['network']

        # 패널티 적용된 확률 계산
        # 패널티 적용된 확률 계산
        all_data['probability_pct'] = all_data['prediction_score'] * 100
        all_data.loc[all_data['sum_installs'] == 0, 'probability_pct'] *= 0.1
        all_data.loc[all_data['sum_impressions'] == 0, 'probability_pct'] *= 0.05
        all_data['probability_pct'] = all_data['probability_pct'].round(1)


        # # 디버그: 593 소재 확인
        # debug_593 = all_data[all_data['subject_label'] == '593']

        # print("🐛 DEBUG 593:", debug_593[['subject_label', 'sum_impressions', 'sum_installs', 'prediction_score', 'probability_pct']].to_dict())


        # if len(debug_593) > 0:
        #     st.write("🐛 DEBUG 593:", debug_593[['subject_label', 'sum_impressions', 'sum_installs', 'prediction_score', 'probability_pct']].to_dict())


        # 네트워크 목록
        networks = sorted(all_data['network'].unique())

        # 병렬 배치 (최대 3개씩)
        num_networks = len(networks)
        if num_networks <= 3:
            cols = st.columns(num_networks)
            network_groups = [networks]
        else:
            # 3개씩 묶어서 행으로 나눔
            cols = st.columns(3)
            network_groups = [networks[i:i+3] for i in range(0, num_networks, 3)]

        # 첫 번째 행 (최대 3개)
        for idx, net in enumerate(networks[:min(3, num_networks)]):
            with cols[idx]:
                network_data = all_data[all_data['network'] == net].copy()
                
                network_data = network_data.sort_values('probability_pct', ascending=False)
                
                # 중복 제거: 같은 소재는 가장 높은 확률만 유지
                network_data = network_data.drop_duplicates(subset=['subject_label'], keep='first')
                
                st.markdown(f"#### 🎯 {net.upper()}")
                st.caption(f"{len(network_data)}개 소재")
                
                # past_network 추가
                display_df = network_data[[
                    'subject_label', 'past_network', 'probability_pct'
                ]]

                st.dataframe(
                    display_df,
                    column_config={
                        'subject_label': st.column_config.TextColumn('소재', width='small'),
                        'past_network': st.column_config.TextColumn('Past', width='small'),
                        'probability_pct': st.column_config.NumberColumn('확률순위', format="%.1f%%", width='small')
                    },
                    hide_index=True,
                    width="stretch",
                    height=400
                )

        # 두 번째 행 (4개 이상일 경우)
        if num_networks > 3:
            st.markdown("---")
            remaining_networks = networks[3:]
            cols2 = st.columns(min(3, len(remaining_networks)))
            
            for idx, net in enumerate(remaining_networks):
                with cols2[idx]:
                    network_data = all_data[all_data['network'] == net].copy()
            
                    network_data = network_data.sort_values('probability_pct', ascending=False)
                    
                    # 중복 제거: 같은 소재는 가장 높은 확률만 유지
                    network_data = network_data.drop_duplicates(subset=['subject_label'], keep='first')
                    
                    st.markdown(f"#### 🎯 {net.upper()}")
                    st.caption(f"{len(network_data)}개 소재")

                    
                    # past_network 추가
                    display_df = network_data[[
                         'subject_label', 'past_network', 'probability_pct'
                    ]]
                    
                    st.dataframe(
                        display_df,
                    column_config={
                        'subject_label': st.column_config.TextColumn('소재', width='small'),
                        'past_network': st.column_config.TextColumn('Past', width='small'),
                        'probability_pct': st.column_config.NumberColumn('확률순위', format="%.1f%%", width='small')
                    },
                        hide_index=True,
                        width="stretch",
                        height=400
                    )
        
        # 인사이트 시각화
        st.markdown("---")
        st.markdown("### 💡 AI 인사이트")
        
        col_viz1, col_viz2 = st.columns(2)
        
        theme = create_plotly_theme()
        
        with col_viz1:
            # 네트워크별 추천 수
            network_counts = best_per_creative['network'].value_counts()
            
            fig_pie = go.Figure(data=[go.Pie(
                labels=network_counts.index,
                values=network_counts.values,
                marker=dict(
                    colors=['#ff006e', '#ff4d8f', '#ff77a0', '#a855f7', '#8b00ff']
                ),
                textfont=dict(color='white', size=14)
            )])
            
            fig_pie.update_layout(
                **theme,
                title='최적 네트워크 분포',
                height=300,
                showlegend=True
            )
            
            st.plotly_chart(fig_pie, width="stretch", key='ai_modal_pie')
        
        with col_viz2:
            # Past 네트워크별 평균 스코어
            past_avg = best_per_creative.groupby('past_network')['ranking_score'].mean().sort_values(ascending=True)
            
            fig_bar = go.Figure(data=[go.Bar(
                x=past_avg.values,
                y=past_avg.index,
                orientation='h',
                marker=dict(
                    color=past_avg.values,
                    colorscale=[[0, '#ff77a0'], [0.5, '#ff4d8f'], [1, '#ff006e']],
                    line=dict(color='rgba(255, 255, 255, 0.3)', width=2)
                ),
                text=[f'{v:.2f}' for v in past_avg.values],
                textposition='outside',
                cliponaxis=False
            )])
            
            fig_bar.update_layout(
                **theme,
                title='Past 네트워크별 평균 Score',
                height=300,
                margin=dict(l=20, r=100, t=40, b=40),
                xaxis=dict(
                    range=[0, past_avg.values.max() * 1.12]
                ),
                xaxis_title='Average Score',
                yaxis_title='',
                showlegend=False
            )
            
            st.plotly_chart(fig_bar, width="stretch", key='ai_modal_bar')
        
        # 핵심 인사이트 요약
        st.markdown("---")
        
        col_insight1, col_insight2, col_insight3 = st.columns(3)
        
        with col_insight1:
            best_network = network_counts.index[0]
            best_count = network_counts.values[0]
            st.metric(
                "🏆 최다 추천 네트워크",
                best_network.upper(),
                f"{best_count}개 소재 ({best_count/len(best_per_creative)*100:.0f}%)"
            )
        
        with col_insight2:
            best_past = past_avg.index[-1]
            best_past_score = past_avg.values[-1]
            st.metric(
                "📈 최고 Past 네트워크",
                best_past.upper(),
                f"평균 {best_past_score:.2f}"
            )
        
        with col_insight3:
            avg_gap = best_per_creative['gap'].mean()
            st.metric(
                "🎯 평균 우위 점수",
                f"+{avg_gap:.2f}",
                "1등과 2등 차이"
            )


    # 버튼 클릭 시 팝업 호출
    if st.session_state.get('show_ai_recommendation', False):
        show_ai_modal(filtered_df, selected_app, selected_future_locality, selected_week_label)
        st.session_state['show_ai_recommendation'] = False  
    
    # ========== 새로운 탭 구조: Future Network 중심 ==========
    future_networks = sorted(filtered_df['network'].unique())

    st.markdown("---")

    # 탭 생성 (Future Network만)
    tabs = st.tabs([f"📊 {net.upper()}" for net in future_networks])

    for idx, future_net in enumerate(future_networks):
        with tabs[idx]:
            # 해당 Future Network 데이터
            future_net_df = filtered_df[filtered_df['network'] == future_net].copy()
            
            # Past Network 목록
            past_networks = sorted(future_net_df['past_network'].unique())
            
            st.markdown(f"### 🎯 {future_net.upper()} Network")
            st.markdown(f"**Past Networks:** {', '.join([p.upper() for p in past_networks])}")
            st.markdown("---")
            
            # Past Network별로 섹션 구분
            if len(past_networks) == 2:
                st.markdown("### 📊 Past Network 비교")
                
                col_left, col_divider, col_right = st.columns([10, 0.3, 10])
                
                # 구분선
                with col_divider:
                    st.markdown("""
                    <div style="
                        width: 1px;
                        height: 100%;
                        background: linear-gradient(
                            to bottom,
                            transparent 0%,
                            rgba(255, 0, 110, 0.2) 10%,
                            rgba(255, 0, 110, 0.4) 50%,
                            rgba(255, 0, 110, 0.2) 90%,
                            transparent 100%
                        );
                        margin: 0 auto;
                    "></div>
                    """, unsafe_allow_html=True)
                
                for col_idx, (col, past_net) in enumerate(zip([col_left, col_right], past_networks)):
                    with col:
                        # 해당 조합 데이터
                        combo_df = future_net_df[future_net_df['past_network'] == past_net].copy()
                        combo_df = combo_df.sort_values('ranking_score', ascending=False).reset_index(drop=True)
                        
                        # 필터링 후 rank 재계산 (1부터 시작)
                        combo_df['rank_per_network'] = range(1, len(combo_df) + 1)
                        
                        top_10_bubble = combo_df




                        all_data_df = combo_df
                        
                        if len(top_10_bubble) == 0:
                            st.warning(f"⚠️ {past_net.upper()} 데이터 없음")
                            continue
                        
                        # 섹션 헤더
                        st.markdown(f"#### 🔄 {past_net.upper()} → {future_net.upper()}")
                        st.markdown("---")
                        
                        theme = create_plotly_theme()
                        
                        # 버블 차트
                        st.markdown("##### 🎯 소재 순위")
                        
                        bubble_size = top_10_bubble['ranking_score'] * 8 + 20
                        
                        fig_bubble = go.Figure()
                        
                        # Locality별 색상
                        locality_colors = top_10_bubble['future_locality'].map({
                            'US': '#ff006e', 'GLOBAL': '#8b00ff'
                        }).fillna('#ff006e')
                        
                        fig_bubble.add_trace(go.Scatter(
                            x=top_10_bubble['rank_per_network'],
                            y=top_10_bubble['ranking_score'],
                            mode='markers+text',
                            marker=dict(
                                size=bubble_size,
                                color=locality_colors,
                                showscale=False,
                                line=dict(color='rgba(255, 255, 255, 0.5)', width=2),
                                opacity=0.9
                            ),
                           
                            text=top_10_bubble['subject_label_emoji'],
                            textposition='top center',
                            textfont=dict(color='white', size=9),
                            hovertemplate='<b>%{text}</b><br>Rank: %{x}<br>Score: %{y:.2f}<extra></extra>'
                        ))
                        
                        fig_bubble.update_layout(
                            **theme,
                            height=400,
                            margin=dict(l=20, r=20, t=20, b=40),
                            xaxis_title='순위',
                            yaxis_title='Score',
                            xaxis=dict(autorange='reversed', showgrid=False),
                            yaxis=dict(showgrid=True, gridcolor='rgba(255, 255, 255, 0.1)', gridwidth=1),
                            showlegend=False
                        )
                        
                        st.plotly_chart(fig_bubble, width="stretch", key=f'bubble_{future_net}_{past_net}_{col_idx}')
                        
                        # 6개 차트 (2x3 그리드로 축소)
                        st.markdown("##### 📊 주요 지표")
                        
                        chart_height = 180
                        
                        def bar_with_headroom(
                            df: pd.DataFrame,
                            *,
                            x: str,
                            y: str,
                            text: str,
                            theme: dict,
                            height: int,
                            color: str,
                            texttemplate: str,
                            headroom_pct: float = 0.12,
                        ):
                            fig = px.bar(df, x=x, y=y, text=text, color_discrete_sequence=[color])
                            y_max = float(df[y].max()) if len(df) else 0.0
                            headroom = y_max * headroom_pct if y_max > 0 else 1.0
                            fig.update_layout(
                                **theme,
                                height=height,
                                margin=dict(l=20, r=20, t=30, b=40),
                                showlegend=False,
                                xaxis=dict(tickangle=-45, title="", showgrid=False),
                                yaxis=dict(title="", showgrid=True, gridcolor="rgba(255,255,255,0.1)", range=[0, y_max + headroom]),
                            )
                            fig.update_traces(
                                texttemplate=texttemplate,
                                textposition="outside",
                                cliponaxis=False,
                                marker=dict(line=dict(color=color, width=2)),
                            )
                            return fig
                        
                        # Row 1
                        row1_col1, row1_col2 = st.columns(2)
                        
                        with row1_col1:
                            st.markdown("###### 👁️ Impressions")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="sum_impressions",
                                text="sum_impressions", theme=theme, height=chart_height,
                                color="#0096ff", texttemplate="%{text:,.0f}"
                            )
                            st.plotly_chart(fig, width="stretch", key=f'imp_{future_net}_{past_net}_{col_idx}')
                        
                        with row1_col2:
                            st.markdown("###### 📲 Installs")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="sum_installs",
                                text="sum_installs", theme=theme, height=chart_height,
                                color="#a855f7", texttemplate="%{text:,.0f}"
                            )
                            st.plotly_chart(fig, width="stretch", key=f'inst_{future_net}_{past_net}_{col_idx}')
                        
                        # Row 2
                        row2_col1, row2_col2 = st.columns(2)
                        
                        with row2_col1:
                            st.markdown("###### 💰 CPI")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="sum_CPI",
                                text="sum_CPI", theme=theme, height=chart_height,
                                color="#ff006e", texttemplate="$%{text:.2f}"
                            )
                            st.plotly_chart(fig, width="stretch", key=f'cpi_{future_net}_{past_net}_{col_idx}')
                        
                        with row2_col2:
                            st.markdown("###### 📈 IPM")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="IPM",
                                text="IPM", theme=theme, height=chart_height,
                                color="#ff4d8f", texttemplate="%{text:.2f}"
                            )
                            st.plotly_chart(fig, width="stretch", key=f'ipm_{future_net}_{past_net}_{col_idx}')
                        
                        # Row 3
                        row3_col1, row3_col2 = st.columns(2)
                        
                        with row3_col1:
                            st.markdown("###### 🎯 CTR")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="CTR",
                                text="CTR", theme=theme, height=chart_height,
                                color="#ff77a0", texttemplate="%{text:.2f}%"
                            )
                            st.plotly_chart(fig, width="stretch", key=f'ctr_{future_net}_{past_net}_{col_idx}')
                        
                        with row3_col2:
                            st.markdown("###### 💎 ROAS")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="roas_sum_1to3",
                                text="roas_sum_1to3", theme=theme, height=chart_height,
                                color="#8b00ff", texttemplate="%{text:.2f}"
                            )
                            st.plotly_chart(fig, width="stretch", key=f'roas_{future_net}_{past_net}_{col_idx}')
                        
                        # 테이블
                        st.markdown("---")
                        st.markdown("##### 📋 Details")
                        
                        display_table = all_data_df[[
                            'rank_per_network', 'app', 'subject_label',
                            'sum_impressions', 'sum_installs', 'sum_CPI', 'IPM', 'CTR', 'CVR', 'CVR_IMP','sum_costs','roas_sum_1to3', 'ranking_score'
                        ]].copy()
                        
                        display_table.columns = ['Rank', 'App', '소재', 'Impressions', 'Installs', 'CPI', 'IPM', 'CTR%', 'CVR%', 'CVR_IMP%','COST','ROAS', 'Score']
                        
                        st.dataframe(
                            display_table,
                            hide_index=True,
                            width="stretch",
                            height=300
                        )
                        
                        # Export
                        csv = all_data_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Export CSV",
                            data=csv,
                            file_name=f"{past_net}_to_{future_net}_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv",
                            key=f'export_{future_net}_{past_net}_{col_idx}',
                            width="stretch"
                        )
            
            else:
                # ========== 2개 아닐 때: 기존 방식 (세로 배치) ==========
                for past_idx, past_net in enumerate(past_networks):
                    # 해당 조합 데이터
                    combo_df = future_net_df[future_net_df['past_network'] == past_net].copy()
                    combo_df = combo_df.sort_values('ranking_score', ascending=False).reset_index(drop=True)
                    
                    # 필터링 후 rank 재계산 (1부터 시작)
                    combo_df['rank_per_network'] = range(1, len(combo_df) + 1)
                    
                    top_10_bubble = combo_df


                    all_data_df = combo_df
                    
                    if len(top_10_bubble) == 0:
                        continue
                    
                    # Past Network 섹션 헤더
                    st.markdown(f"#### 🔄 Past: {past_net.upper()} → Future: {future_net.upper()}")

                    
                    # Row 1: 버블 차트 + 6개 지표 차트
                    col_bubble, col_charts = st.columns([1, 3])
                    
                    theme = create_plotly_theme()
                    
                    with col_bubble:
                        st.markdown("##### 🎯 소재 순위")
                        
                        bubble_size = top_10_bubble['ranking_score'] * 8 + 20
                        
                        fig_bubble = go.Figure()
                        
                        # Locality별 색상
                        locality_colors = top_10_bubble['future_locality'].map({
                            'US': '#ff006e', 'GLOBAL': '#8b00ff'
                        }).fillna('#ff006e')
                        
                        fig_bubble.add_trace(go.Scatter(
                            x=top_10_bubble['rank_per_network'],
                            y=top_10_bubble['ranking_score'],
                            mode='markers+text',
                            marker=dict(
                                size=bubble_size,
                                color=locality_colors,
                                showscale=False,
                                line=dict(color='rgba(255, 255, 255, 0.5)', width=2),
                                opacity=0.9
                            ),
                            text=top_10_bubble['subject_label_emoji'],
                            textposition='top center',
                            textfont=dict(color='white', size=9),
                            hovertemplate='<b>%{text}</b><br>Rank: %{x}<br>Score: %{y:.2f}<extra></extra>'
                        ))
                        
                        fig_bubble.update_layout(
                            **theme,
                            height=580,
                            margin=dict(l=20, r=20, t=20, b=40),
                            xaxis_title='순위',
                            yaxis_title='Score',
                            xaxis=dict(autorange='reversed', showgrid=False),
                            yaxis=dict(showgrid=True, gridcolor='rgba(255, 255, 255, 0.1)', gridwidth=1),
                            showlegend=False
                        )
                        
                        st.plotly_chart(fig_bubble, width="stretch", key=f'bubble_{future_net}_{past_net}_{past_idx}')
                    
                    with col_charts:
                        row1_col1, row1_col2, row1_col3 = st.columns(3)
                        row2_col1, row2_col2, row2_col3 = st.columns(3)
                        
                        chart_height = 250
                        
                        def bar_with_headroom(
                            df: pd.DataFrame,
                            *,
                            x: str,
                            y: str,
                            text: str,
                            theme: dict,
                            height: int,
                            color: str,
                            texttemplate: str,
                            headroom_pct: float = 0.12,
                        ):
                            fig = px.bar(df, x=x, y=y, text=text, color_discrete_sequence=[color])
                            y_max = float(df[y].max()) if len(df) else 0.0
                            headroom = y_max * headroom_pct if y_max > 0 else 1.0
                            fig.update_layout(
                                **theme,
                                height=height,
                                margin=dict(l=20, r=20, t=40, b=60),
                                showlegend=False,
                                xaxis=dict(tickangle=-45, title="", showgrid=False),
                                yaxis=dict(title="", showgrid=True, gridcolor="rgba(255,255,255,0.1)", range=[0, y_max + headroom]),
                            )
                            fig.update_traces(
                                texttemplate=texttemplate,
                                textposition="outside",
                                cliponaxis=False,
                                marker=dict(line=dict(color=color, width=2)),
                            )
                            return fig
                        
                        with row1_col1:
                            st.markdown("##### 👁️ Impressions")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="sum_impressions",
                                text="sum_impressions", theme=theme, height=chart_height,
                                color="#0096ff", texttemplate="%{text:,.0f}"
                            )
                            st.plotly_chart(fig, width="stretch", key=f'imp_{future_net}_{past_net}_{past_idx}')
                        
                        with row1_col2:
                            st.markdown("##### 📲 Installs")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="sum_installs",
                                text="sum_installs", theme=theme, height=chart_height,
                                color="#a855f7", texttemplate="%{text:,.0f}"
                            )
                            st.plotly_chart(fig, width="stretch", key=f'inst_{future_net}_{past_net}_{past_idx}')
                        
                        with row1_col3:
                            st.markdown("##### 💰 CPI")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="sum_CPI",
                                text="sum_CPI", theme=theme, height=chart_height,
                                color="#ff006e", texttemplate="$%{text:.2f}"
                            )
                            st.plotly_chart(fig, width="stretch", key=f'cpi_{future_net}_{past_net}_{past_idx}')
                        
                        with row2_col1:
                            st.markdown("##### 📈 IPM")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="IPM",
                                text="IPM", theme=theme, height=chart_height,
                                color="#ff4d8f", texttemplate="%{text:.2f}"
                            )
                            st.plotly_chart(fig, width="stretch", key=f'ipm_{future_net}_{past_net}_{past_idx}')
                        
                        with row2_col2:
                            st.markdown("##### 🎯 CTR")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="CTR",
                                text="CTR", theme=theme, height=chart_height,
                                color="#ff77a0", texttemplate="%{text:.2f}%"
                            )
                            st.plotly_chart(fig, width="stretch", key=f'ctr_{future_net}_{past_net}_{past_idx}')
                        
                        with row2_col3:
                            st.markdown("##### 💎 ROAS")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="roas_sum_1to3",
                                text="roas_sum_1to3", theme=theme, height=chart_height,
                                color="#8b00ff", texttemplate="%{text:.2f}"
                            )
                            st.plotly_chart(fig, width="stretch", key=f'roas_{future_net}_{past_net}_{past_idx}')
                    
                    # 테이블
                    st.markdown("---")
                    st.markdown("##### 📋 Details")
                    
                    display_table = all_data_df[[
                        'rank_per_network', 'app', 'subject_label',
                        'sum_impressions', 'sum_installs', 'sum_CPI', 'IPM', 'CTR', 'CVR', 'CVR_IMP','sum_costs','roas_sum_1to3', 'ranking_score'
                    ]].copy()
                    
                    display_table.columns = ['Rank', 'App', '소재', 'Impressions', 'Installs', 'CPI', 'IPM', 'CTR%', 'CVR%', 'CVR_IMP%','COST','ROAS', 'Score']
                    
                    st.dataframe(
                        display_table,
                        hide_index=True,
                        width="stretch",
                        height=400
                    )
                    
                    # Export
                    col_export, col_space = st.columns([1, 3])
                    with col_export:
                        csv = all_data_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Export CSV",
                            data=csv,
                            file_name=f"{past_net}_to_{future_net}_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv",
                            key=f'export_{future_net}_{past_net}_{past_idx}',
                            width="stretch"
                        )
                    
                    # Past Network 구분선 (마지막 섹션 제외)
                    if past_idx < len(past_networks) - 1:
                        st.markdown("---")
                        st.markdown("<br><br>", unsafe_allow_html=True)
    
    st.markdown("---")
    st.caption(f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} KST")



if __name__ == "__main__":
    run()




















