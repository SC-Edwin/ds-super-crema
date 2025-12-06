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
    주차 코드를 사용자 친화적 레이블로 변환
    
    Args:
        week_str: 'YYYY-Wnn' 형식
        reference_weeks: dict {'this': 'YYYY-Wnn', 'last': ..., 'two_ago': ...}
    
    Returns:
        str: '이번주 (2024-W49)' 같은 형식
    """
    if week_str == reference_weeks['this']:
        return f"이번주 ({week_str})"
    elif week_str == reference_weeks['last']:
        return f"전주 ({week_str})"
    elif week_str == reference_weeks['two_ago']:
        return f"전전주 ({week_str})"
    else:
        return week_str
    


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
        AND rank != 'nan'
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
            PARTITION BY subject, network, app, past_network
            ORDER BY SAFE_CAST(prediction_timestamp AS TIMESTAMP) DESC) AS row_num
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
        PARTITION BY app, past_network, network
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



# ================================
# 메인 시각화
# ================================
def run():
    """시각화 모듈 메인"""
    
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

    with col2:
        all_localities = ['All'] + sorted(df['locality'].unique().tolist())
        selected_locality = st.selectbox("🌍 Locality", all_localities)

    with col3:  # ← 새로 추가!
        # 주차 목록 (최신순, None 제외)
        available_weeks = sorted(
            [w for w in df['upload_week'].unique() if w is not None], 
            reverse=True
        )
        
        # 사용자 친화적 레이블 생성
        week_options = ['All'] + [
            get_week_label(w, reference_weeks) for w in available_weeks
        ]
        
        selected_week_label = st.selectbox("📅 업로드 주차", week_options)
        
        # 레이블 → 실제 주차 코드 변환
        if selected_week_label == 'All':
            selected_week = 'All'
        else:
            # 괄호 안의 주차 코드 추출 (예: "이번주 (2024-W49)" → "2024-W49")
            import re
            match = re.search(r'\((.*?)\)', selected_week_label)
            selected_week = match.group(1) if match else selected_week_label
                    

    # Henry & Kyle 버튼 (필터 아래 왼쪽)
    # [수정] 버튼이 숨을 쉴 수 있게 컬럼 너비를 0.5 -> 1.5로 넓혔습니다.
    # # col_btn, col_spacer = st.columns([1.5, 7]) 
    # col_btn, col_spacer = st.columns([0.3, 9.7])


    # with col_btn:
    #     # [수정] 줄바꿈(\n) 제거 & use_container_width=True 추가
    #     if st.button("Heny & Kyle", key="ai_btn", help="Heny & Kyle AI 추천", use_container_width=True):
    #         st.session_state['show_ai_recommendation'] = True

    # Henry & Kyle 버튼 (필터와 같은 그리드에 정렬)
    btn_col1, btn_col2, btn_col3, btn_spacer = st.columns([1.2, 1.2, 1.5, 4])

    # 1열(📱 App가 시작하는 위치)에 맞게 배치
    with btn_col1:
        if st.button(
            "Heny\n&\nKyle",  # ← 3줄로 나눔!
            key="ai_btn",
            help="Heny & Kyle AI 추천",
            width="content",
        ):
            st.session_state['show_ai_recommendation'] = True



    # 필터 적용
    filtered_df = df.copy()
    if selected_app != 'All':
        filtered_df = filtered_df[filtered_df['app'] == selected_app]

    if selected_locality != 'All':
        filtered_df = filtered_df[filtered_df['locality'] == selected_locality]
    
    if selected_week != 'All':  # ← 새로 추가!
        filtered_df = filtered_df[filtered_df['upload_week'] == selected_week]

    if len(filtered_df) == 0:
        st.warning("⚠️ 선택한 조건에 맞는 데이터가 없습니다.")
        return
    

    # ========== 팝업 모달 (Dialog) ==========
    @st.dialog("🤖 Henry & Kyle AI 추천", width="large")
    def show_ai_modal(filtered_df, selected_app, selected_locality, selected_week_label):  # ← 파라미터 추가
        """AI 추천 모달"""
        
        app_text = selected_app if selected_app != 'All' else '전체'
        loc_text = selected_locality if selected_locality != 'All' else '전체'
        week_text = selected_week_label if selected_week_label != 'All' else '전체 주차'  # ← 추가
        
        st.markdown(f"**{app_text}** × **{loc_text}** × **{week_text}** - {len(filtered_df)}개 소재 분析")  # ← 수정
        
        st.markdown("---")
        
        # 소재별 최적 경로 계산
        best_per_creative = filtered_df.loc[
            filtered_df.groupby('subject_label')['ranking_score'].idxmax()
        ]
        
        best_per_creative['path'] = (
            best_per_creative['past_network'] + ' → ' + 
            best_per_creative['network']
        )
        
        # 2등과의 차이 계산
        def get_score_gap(row):
            same_creative = filtered_df[filtered_df['subject_label'] == row['subject_label']]
            sorted_scores = same_creative['ranking_score'].sort_values(ascending=False)
            if len(sorted_scores) >= 2:
                return sorted_scores.iloc[0] - sorted_scores.iloc[1]
            return 0
        
        best_per_creative['gap'] = best_per_creative.apply(get_score_gap, axis=1)
        
        # 아이콘 추가
        def add_icon(row):
            rank = row['rank_per_network']
            if rank <= 3:
                return '🏆'
            elif rank <= 10:
                return '⭐'
            return ''
        
        best_per_creative['icon'] = best_per_creative.apply(add_icon, axis=1)
        
        # 테이블
        st.markdown("### 📊 소재별 최적 투자 경로")
        
        # 확률(%) 계산
        best_per_creative['probability_pct'] = (best_per_creative['prediction_score'] * 100).round(1)

        display_df = best_per_creative[[
            'icon', 'subject_label', 'path', 'probability_pct',  # ← ranking_score 대신!
            'rank_per_network', 'sum_CPI', 'gap'
        ]].sort_values('probability_pct', ascending=False).reset_index(drop=True)  # ← 정렬 기준도 변경

        st.dataframe(
            display_df,
            column_config={
                'icon': st.column_config.TextColumn('', width='small'),
                'subject_label': st.column_config.TextColumn('소재', width='small'),
                'path': st.column_config.TextColumn('최적 경로', width='medium'),
                'probability_pct': st.column_config.NumberColumn('확률', format="%.1f%%", width='small'),  # ← 추가!
                'rank_per_network': st.column_config.TextColumn('순위', width='small'),
                'sum_CPI': st.column_config.NumberColumn('CPI', format="$%.2f", width='small'),
                'gap': st.column_config.NumberColumn('차이', format="+%.2f", width='small')
            },
            hide_index=True,
            use_container_width=True,
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
            
            st.plotly_chart(fig_pie, use_container_width=True)
        
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
            
            st.plotly_chart(fig_bar, use_container_width=True)  # ← 이게 누락됐었음!
        
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
        show_ai_modal(filtered_df, selected_app, selected_locality, selected_week_label)
        st.session_state['show_ai_recommendation'] = False  # 리셋
    
    # 네트워크 조합 (Past → Future)
    # ========== 새로운 탭 구조: Future Network 중심 ==========
    # Future Network별로 그룹화
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
                        combo_df = combo_df.sort_values(['app', 'rank_per_network']).reset_index(drop=True)
                        
                        top_10_bubble = combo_df.head(10)
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
                        
                        fig_bubble.add_trace(go.Scatter(
                            x=top_10_bubble['rank_per_network'],
                            y=top_10_bubble['ranking_score'],
                            mode='markers+text',
                            marker=dict(
                                size=bubble_size,
                                color=top_10_bubble['ranking_score'],
                                colorscale=[[0, '#ff77a0'], [0.5, '#ff4d8f'], [1, '#ff006e']],
                                showscale=False,
                                line=dict(color='rgba(255, 255, 255, 0.5)', width=2),
                                opacity=0.9
                            ),
                            text=top_10_bubble['subject_label'],
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
                        
                        st.plotly_chart(fig_bubble, use_container_width=True)
                        
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
                            st.plotly_chart(fig, use_container_width=True)
                        
                        with row1_col2:
                            st.markdown("###### 📲 Installs")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="sum_installs",
                                text="sum_installs", theme=theme, height=chart_height,
                                color="#a855f7", texttemplate="%{text:,.0f}"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        
                        # Row 2
                        row2_col1, row2_col2 = st.columns(2)
                        
                        with row2_col1:
                            st.markdown("###### 💰 CPI")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="sum_CPI",
                                text="sum_CPI", theme=theme, height=chart_height,
                                color="#ff006e", texttemplate="$%{text:.2f}"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        
                        with row2_col2:
                            st.markdown("###### 📈 IPM")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="IPM",
                                text="IPM", theme=theme, height=chart_height,
                                color="#ff4d8f", texttemplate="%{text:.2f}"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        
                        # Row 3
                        row3_col1, row3_col2 = st.columns(2)
                        
                        with row3_col1:
                            st.markdown("###### 🎯 CTR")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="CTR",
                                text="CTR", theme=theme, height=chart_height,
                                color="#ff77a0", texttemplate="%{text:.2f}%"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        
                        with row3_col2:
                            st.markdown("###### 💎 ROAS")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="roas_sum_1to3",
                                text="roas_sum_1to3", theme=theme, height=chart_height,
                                color="#8b00ff", texttemplate="%{text:.2f}"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        
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
                            use_container_width=True,
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
                            use_container_width=True
                        )
            
            else:
                # ========== 2개 아닐 때: 기존 방식 (세로 배치) ==========
                for past_idx, past_net in enumerate(past_networks):
                    # 해당 조합 데이터
                    combo_df = future_net_df[future_net_df['past_network'] == past_net].copy()
                    combo_df = combo_df.sort_values(['app', 'rank_per_network']).reset_index(drop=True)
                    
                    top_10_bubble = combo_df.head(10)
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
                        
                        fig_bubble.add_trace(go.Scatter(
                            x=top_10_bubble['rank_per_network'],
                            y=top_10_bubble['ranking_score'],
                            mode='markers+text',
                            marker=dict(
                                size=bubble_size,
                                color=top_10_bubble['ranking_score'],
                                colorscale=[[0, '#ff77a0'], [0.5, '#ff4d8f'], [1, '#ff006e']],
                                showscale=False,
                                line=dict(color='rgba(255, 255, 255, 0.5)', width=2),
                                opacity=0.9
                            ),
                            text=top_10_bubble['subject_label'],
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
                        
                        st.plotly_chart(fig_bubble, use_container_width=True)
                    
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
                            st.plotly_chart(fig, use_container_width=True)
                        
                        with row1_col2:
                            st.markdown("##### 📲 Installs")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="sum_installs",
                                text="sum_installs", theme=theme, height=chart_height,
                                color="#a855f7", texttemplate="%{text:,.0f}"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        
                        with row1_col3:
                            st.markdown("##### 💰 CPI")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="sum_CPI",
                                text="sum_CPI", theme=theme, height=chart_height,
                                color="#ff006e", texttemplate="$%{text:.2f}"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        
                        with row2_col1:
                            st.markdown("##### 📈 IPM")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="IPM",
                                text="IPM", theme=theme, height=chart_height,
                                color="#ff4d8f", texttemplate="%{text:.2f}"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        
                        with row2_col2:
                            st.markdown("##### 🎯 CTR")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="CTR",
                                text="CTR", theme=theme, height=chart_height,
                                color="#ff77a0", texttemplate="%{text:.2f}%"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        
                        with row2_col3:
                            st.markdown("##### 💎 ROAS")
                            fig = bar_with_headroom(
                                top_10_bubble, x="subject_label", y="roas_sum_1to3",
                                text="roas_sum_1to3", theme=theme, height=chart_height,
                                color="#8b00ff", texttemplate="%{text:.2f}"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                    
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
                        use_container_width=True,
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
                            use_container_width=True
                        )
                    
                    # Past Network 구분선 (마지막 섹션 제외)
                    if past_idx < len(past_networks) - 1:
                        st.markdown("---")
                        st.markdown("<br><br>", unsafe_allow_html=True)
    
    st.markdown("---")
    st.caption(f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} KST")

if __name__ == "__main__":
    run()





