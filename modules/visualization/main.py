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
    QUALIFY rank_per_network <= 10
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
    
    # 필터 (메인 화면 왼쪽)
    st.markdown("### 🔍 Filter")
    col1, col2, col_spacer = st.columns([1.2, 1.2, 5]) 

    with col1:
        all_apps = ['All'] + sorted(df['app'].unique().tolist())
        selected_app = st.selectbox("📱 App", all_apps)

    with col2:
        all_localities = ['All'] + sorted(df['locality'].unique().tolist())
        selected_locality = st.selectbox("🌍 Locality", all_localities)

    # Henry & Kyle 버튼 (필터 아래 왼쪽)
    col_btn, col_spacer = st.columns([0.5, 8])

    with col_btn:
        if st.button("Heny\n&\nKyle", key="ai_btn", help="Heny & Kyle AI 추천"):
            st.session_state['show_ai_recommendation'] = True

    # 필터 적용
    filtered_df = df.copy()
    if selected_app != 'All':
        filtered_df = filtered_df[filtered_df['app'] == selected_app]
    if selected_locality != 'All':
        filtered_df = filtered_df[filtered_df['locality'] == selected_locality]
    
    if len(filtered_df) == 0:
        st.warning("⚠️ 선택한 조건에 맞는 데이터가 없습니다.")
        return
    

    # ========== 팝업 모달 (Dialog) ==========
    @st.dialog("🤖 Henry & Kyle AI 추천", width="large")
    def show_ai_modal(filtered_df, selected_app, selected_locality):
        """AI 추천 모달"""
        
        app_text = selected_app if selected_app != 'All' else '전체'
        loc_text = selected_locality if selected_locality != 'All' else '전체'
        st.markdown(f"**{app_text}** × **{loc_text}** - {len(filtered_df)}개 소재 분석")
        
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
        show_ai_modal(filtered_df, selected_app, selected_locality)
        st.session_state['show_ai_recommendation'] = False  # 리셋
    
    # 네트워크 조합 (Past → Future)
    combinations = filtered_df.groupby(['past_network', 'network']).size().reset_index()[['past_network', 'network']]
    
    st.markdown("---")
    
    # 탭 생성
    tabs = st.tabs([f"📊 {row['past_network']} → {row['network']}" for _, row in combinations.iterrows()])
    
    for idx, (_, combo) in enumerate(combinations.iterrows()):
        with tabs[idx]:
            past_net = combo['past_network']
            future_net = combo['network']
            
            # 해당 조합 데이터
            combo_df = filtered_df[
                (filtered_df['past_network'] == past_net) & 
                (filtered_df['network'] == future_net)
            ].copy()
            
            # 랭킹은 이미 rank_per_network에 있음
            # combo_df = combo_df.sort_values('rank_per_network').reset_index(drop=True)

            combo_df = combo_df.sort_values(['app', 'rank_per_network']).reset_index(drop=True)

            
            # top_10_df = combo_df.head(10)

                        
            # 버블 차트용: Top 10만
            top_10_bubble = combo_df.head(10)

            # 테이블용: 전체
            all_data_df = combo_df

            
            if len(top_10_bubble) == 0:
                st.warning(f"⚠️ {past_net} → {future_net}에 데이터가 없습니다.")
                continue
            
            # Row 1: 버블 차트 + 6개 지표 차트
            col_bubble, col_charts = st.columns([1, 3])
            
            theme = create_plotly_theme()
            
            with col_bubble:
                st.markdown("##### 🎯 소재 순위")
                
                # 버블 크기: 적당하게 (Score 기반)
                bubble_size = top_10_bubble['ranking_score'] * 8 + 20  # 최소 20, 최대 100
                
                # 버블 차트
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
                        line=dict(
                            color='rgba(255, 255, 255, 0.5)',  # 테두리 약하게
                            width=2
                        ),
                        opacity=0.9
                    ),
                    text=top_10_bubble['subject_label'],
                    textposition='top center',
                    textfont=dict(
                        color='white',
                        size=9
                    ),
                    hovertemplate='<b>%{text}</b><br>Rank: %{x}<br>Score: %{y:.2f}<extra></extra>'
                ))
                
                fig_bubble.update_layout(
                    **theme,
                    height=580,
                    margin=dict(l=20, r=20, t=20, b=40),
                    xaxis_title='순위',
                    yaxis_title='Score',
                    xaxis=dict(
                        autorange='reversed',
                        showgrid=False
                    ),
                    yaxis=dict(
                        showgrid=True,  # 가로 그리드만
                        gridcolor='rgba(255, 255, 255, 0.1)',
                        gridwidth=1
                    ),
                    showlegend=False
                )
                
                st.plotly_chart(fig_bubble, use_container_width=True)
            
            with col_charts:
                # 6개 차트 (3x2 그리드)
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
                    """Create a Plotly bar chart with extra y-axis headroom so 'outside' text labels don't get clipped."""
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
                        cliponaxis=False,  # 핵심: 라벨이 plot 영역 밖으로 나가도 안 잘리게
                        marker=dict(line=dict(color=color, width=2)),
                    )
                    return fig
                # Row 1

                with row1_col1:
                    st.markdown("##### 👁️ Impressions")
                    fig = bar_with_headroom(
                        top_10_bubble,
                        x="subject_label",
                        y="sum_impressions",
                        text="sum_impressions",
                        theme=theme,
                        height=chart_height,
                        color="#0096ff",
                        texttemplate="%{text:,.0f}",
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with row1_col2:
                    st.markdown("##### 📲 Installs")
                    fig = bar_with_headroom(
                        top_10_bubble,
                        x="subject_label",
                        y="sum_installs",
                        text="sum_installs",
                        theme=theme,
                        height=chart_height,
                        color="#a855f7",
                        texttemplate="%{text:,.0f}",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with row1_col3:
                    st.markdown("##### 💰 CPI")
                    fig = bar_with_headroom(
                        top_10_bubble,
                        x="subject_label",
                        y="sum_CPI",
                        text="sum_CPI",
                        theme=theme,
                        height=chart_height,
                        color="#ff006e",
                        texttemplate="$%{text:.2f}",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with row2_col1:
                    st.markdown("##### 📈 IPM")
                    fig = bar_with_headroom(
                        top_10_bubble,
                        x="subject_label",
                        y="IPM",
                        text="IPM",
                        theme=theme,
                        height=chart_height,
                        color="#ff4d8f",
                        texttemplate="%{text:.2f}",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with row2_col2:
                    st.markdown("##### 🎯 CTR")
                    fig = bar_with_headroom(
                        top_10_bubble,
                        x="subject_label",
                        y="CTR",
                        text="CTR",
                        theme=theme,
                        height=chart_height,
                        color="#ff77a0",
                        texttemplate="%{text:.2f}%",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with row2_col3:
                    st.markdown("##### 💎 ROAS")
                    fig = bar_with_headroom(
                        top_10_bubble,
                        x="subject_label",
                        y="roas_sum_1to3",
                        text="roas_sum_1to3",
                        theme=theme,
                        height=chart_height,
                        color="#8b00ff",
                        texttemplate="%{text:.2f}",
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            # 테이블
            st.markdown("---")
            st.markdown("##### 📋 Top 10 Details")
            
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
            
            st.markdown("<br>", unsafe_allow_html=True)  # ← 추가!


            # Export
            col_export, col_space = st.columns([1, 3])
            with col_export:
                csv = all_data_df.to_csv(index=False)
                st.download_button(
                    label="📥 Export CSV",
                    data=csv,
                    file_name=f"{past_net}_to_{future_net}_top10_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    key=f'export_{past_net}_{future_net}',
                    use_container_width=True
                )
    
    st.markdown("---")
    st.caption(f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} KST")

if __name__ == "__main__":
    run()





















