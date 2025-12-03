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
        if "gcp_service_account" in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(
                credentials=credentials,
                project=st.secrets["gcp_service_account"]["project_id"]
            )
    except Exception as e:
        st.error(f"❌ GCP 인증 실패: {e}")
        st.stop()  # 여기서 멈춤
    
    # 로컬 (Secrets 없을 때만)
    return bigquery.Client(project='roas-test-456808')


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
    
    st.markdown("## 📊 Top 10 Creatives by Network")
    
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
    col1, col2, col_spacer = st.columns([1.5, 1.5, 5])  # 왼쪽에 붙이기
    
    with col1:
        all_apps = ['All'] + sorted(df['app'].unique().tolist())
        selected_app = st.selectbox("📱 App", all_apps)
    
    with col2:
        all_localities = ['All'] + sorted(df['locality'].unique().tolist())
        selected_locality = st.selectbox("🌍 Locality", all_localities)

    # 필터 적용
    filtered_df = df.copy()
    if selected_app != 'All':
        filtered_df = filtered_df[filtered_df['app'] == selected_app]
    if selected_locality != 'All':
        filtered_df = filtered_df[filtered_df['locality'] == selected_locality]
    
    if len(filtered_df) == 0:
        st.warning("⚠️ 선택한 조건에 맞는 데이터가 없습니다.")
        return
    
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
                    height=560,
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
                
                # Row 1
                with row1_col1:
                    st.markdown("##### 👁️ Impressions")
                    fig = px.bar(top_10_bubble, x='subject_label', y='sum_impressions', text='sum_impressions', color_discrete_sequence=['#0096ff'])
                    fig.update_layout(**theme, height=chart_height, margin=dict(l=20, r=20, t=20, b=60), showlegend=False,
                                     xaxis={'tickangle': -45, 'title': '', 'showgrid': False},
                                     yaxis={'title': '', 'showgrid': True, 'gridcolor': 'rgba(255,255,255,0.1)'})
                    fig.update_traces(texttemplate='%{text:,.0f}', textposition='inside', marker=dict(line=dict(color='#0096ff', width=2)))
                    st.plotly_chart(fig, use_container_width=True)
                
                with row1_col2:
                    st.markdown("##### 📲 Installs")
                    fig = px.bar(top_10_bubble, x='subject_label', y='sum_installs', text='sum_installs', color_discrete_sequence=['#a855f7'])
                    fig.update_layout(**theme, height=chart_height, margin=dict(l=20, r=20, t=20, b=60), showlegend=False,
                                     xaxis={'tickangle': -45, 'title': '', 'showgrid': False},
                                     yaxis={'title': '', 'showgrid': True, 'gridcolor': 'rgba(255,255,255,0.1)'})
                    fig.update_traces(texttemplate='%{text:,.0f}', textposition='inside', marker=dict(line=dict(color='#a855f7', width=2)))
                    st.plotly_chart(fig, use_container_width=True)
                
                with row1_col3:
                    st.markdown("##### 💰 CPI")
                    fig = px.bar(top_10_bubble, x='subject_label', y='sum_CPI', text='sum_CPI', color_discrete_sequence=['#ff006e'])
                    fig.update_layout(**theme, height=chart_height, margin=dict(l=20, r=20, t=20, b=60), showlegend=False,
                                     xaxis={'tickangle': -45, 'title': '', 'showgrid': False},
                                     yaxis={'title': '', 'showgrid': True, 'gridcolor': 'rgba(255,255,255,0.1)'})
                    fig.update_traces(texttemplate='$%{text:.2f}', textposition='outside', marker=dict(line=dict(color='#ff006e', width=2)))
                    st.plotly_chart(fig, use_container_width=True)
                
                # Row 2
                with row2_col1:
                    st.markdown("##### 📈 IPM")
                    fig = px.bar(top_10_bubble, x='subject_label', y='IPM', text='IPM', color_discrete_sequence=['#ff4d8f'])
                    fig.update_layout(**theme, height=chart_height, margin=dict(l=20, r=20, t=20, b=60), showlegend=False,
                                     xaxis={'tickangle': -45, 'title': '', 'showgrid': False},
                                     yaxis={'title': '', 'showgrid': True, 'gridcolor': 'rgba(255,255,255,0.1)'})
                    fig.update_traces(texttemplate='%{text:.2f}', textposition='outside', marker=dict(line=dict(color='#ff4d8f', width=2)))
                    st.plotly_chart(fig, use_container_width=True)
                
                with row2_col2:
                    st.markdown("##### 🎯 CTR")
                    fig = px.bar(top_10_bubble, x='subject_label', y='CTR', text='CTR', color_discrete_sequence=['#ff77a0'])
                    fig.update_layout(**theme, height=chart_height, margin=dict(l=20, r=20, t=20, b=60), showlegend=False,
                                     xaxis={'tickangle': -45, 'title': '', 'showgrid': False},
                                     yaxis={'title': '', 'showgrid': True, 'gridcolor': 'rgba(255,255,255,0.1)'})
                    fig.update_traces(texttemplate='%{text:.2f}%', textposition='outside', marker=dict(line=dict(color='#ff77a0', width=2)))
                    st.plotly_chart(fig, use_container_width=True)
                
                with row2_col3:
                    st.markdown("##### 💎 ROAS")
                    fig = px.bar(top_10_bubble, x='subject_label', y='roas_sum_1to3', text='roas_sum_1to3', color_discrete_sequence=['#8b00ff'])
                    fig.update_layout(**theme, height=chart_height, margin=dict(l=20, r=20, t=20, b=60), showlegend=False,
                                     xaxis={'tickangle': -45, 'title': '', 'showgrid': False},
                                     yaxis={'title': '', 'showgrid': True, 'gridcolor': 'rgba(255,255,255,0.1)'})
                    fig.update_traces(texttemplate='%{text:.2f}', textposition='outside', marker=dict(line=dict(color='#8b00ff', width=2)))
                    st.plotly_chart(fig, use_container_width=True)
            
            # 테이블
            st.markdown("---")
            st.markdown("##### 📋 Top 10 Details")
            
            display_table = all_data_df[[
                'rank_per_network', 'app', 'subject_label',
                'sum_impressions', 'sum_installs', 'sum_CPI', 'IPM', 'CTR', 'CVR', 'sum_costs','roas_sum_1to3', 'ranking_score'
            ]].copy()
            
            display_table.columns = ['Rank', 'App', '소재', 'Impressions', 'Installs', 'CPI', 'IPM', 'CTR%', 'CVR%', 'COST','ROAS', 'Score']
            
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
                    file_name=f"{past_net}_to_{future_net}_top10_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    key=f'export_{past_net}_{future_net}',
                    use_container_width=True
                )
    
    st.markdown("---")
    st.caption(f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} KST")

if __name__ == "__main__":
    run()















