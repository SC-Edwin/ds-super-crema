"""
시각화 모듈
Creative Performance Trend 시각화 대시보드
Author: Eader
"""


import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
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
        pass
    
    # 로컬 (Application Default Credentials)
    return bigquery.Client(project='roas-test-456808')


@st.cache_data(ttl=300)
def load_creative_trend_data(start_date, end_date, selected_app=None, selected_os=None, selected_locality=None):
    """
    크리에이티브 성과 트렌드 데이터 로드
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)
        selected_app: 선택된 앱 (None이면 전체)
        selected_os: 선택된 OS (None이면 전체)
        selected_locality: 선택된 국가 (None이면 전체)
    """
    client = get_bigquery_client()
    
    # 동적 WHERE 조건 생성
    where_conditions = [
        f"DATE(day) BETWEEN '{start_date}' AND '{end_date}'",
        "campaign NOT LIKE '%test%'"
    ]
    
    if selected_app and selected_app != 'All':
        where_conditions.append(f"app = '{selected_app}'")
    
    if selected_os and selected_os != 'All':
        where_conditions.append(f"os = '{selected_os}'")
    
    if selected_locality and selected_locality != 'All':
        where_conditions.append(f"locality = '{selected_locality}'")
    
    where_clause = " AND ".join(where_conditions)
    
    query = f"""
    WITH DailyPerformance AS (
      SELECT
        DATE(day) as date,
        subject,
        app,
        os,
        locality,
        network,
        SUM(impressions) as impressions,
        SUM(installs) as installs,
        SUM(clicks) as clicks,
        SUM(cost) as cost,
        SAFE_DIVIDE(SUM(cost), SUM(installs)) as CPI,
        SAFE_DIVIDE(SUM(installs) * 1000, SUM(impressions)) as IPM,
        SAFE_DIVIDE(SUM(clicks) * 100, SUM(impressions)) as CTR,
        SAFE_DIVIDE(SUM(installs) * 100, SUM(clicks)) as CVR
      FROM `roas-test-456808.marketing_datascience.creative_performance`
      WHERE {where_clause}
      GROUP BY date, subject, app, os, locality, network
    )
    SELECT *
    FROM DailyPerformance
    ORDER BY date DESC, installs DESC
    """
    
    df = client.query(query).to_dataframe()
    return df


@st.cache_data(ttl=600)
def get_filter_options():
    """필터 옵션 데이터 로드 (App, OS, locality)"""
    client = get_bigquery_client()

    
    query = """
    SELECT DISTINCT
      app,
      os,
      locality
    FROM `roas-test-456808.marketing_datascience.creative_performance`
    WHERE DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
     AND os != 'rebound'
     AND app != '0'
    ORDER BY app, os, locality
    """
    
    df = client.query(query).to_dataframe()
    
    return {
        'apps': sorted(df['app'].unique().tolist()),
        'os': sorted(df['os'].unique().tolist()),
        'countries': sorted(df['locality'].unique().tolist())
    }


def create_plotly_theme():
    """Plotly 차트 테마 - 블랙 + 핑크"""
    return {
        'template': 'plotly_dark',
        'paper_bgcolor': 'rgba(26, 26, 26, 0.6)',
        'plot_bgcolor': 'rgba(20, 20, 20, 0.5)',
        'font': {'color': '#ffffff', 'family': 'Arial', 'size': 11},
        'colorway': ['#ff006e', '#ff4d8f', '#ff77a0', '#a855f7', '#8b00ff']
    }


def run():
    """Creative Performance Trend 메인"""
    
    # 페이지 타이틀
    st.markdown("## 📈 Creative Performance Trend")
    
    # ========== 필터 영역 ==========
    st.markdown("### 🔍 Filter")
    
    # 필터 옵션 로드
    with st.spinner("🔄 필터 옵션 로딩 중..."):
        try:
            filter_options = get_filter_options()
        except Exception as e:
            st.error(f"❌ 필터 옵션 로드 실패: {str(e)}")
            return
    
    # 4개 컬럼: App, OS, locality, Date
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        apps = ['All'] + filter_options['apps']
        selected_app = st.selectbox("📱 App", apps)
    
    with col2:
        os_options = ['All'] + filter_options['os']
        selected_os = st.selectbox("💻 OS", os_options)
    
    with col3:
        countries = ['All'] + filter_options['countries']
        selected_locality = st.selectbox("🌍 locality", countries)
    
    with col4:
        # 날짜 범위 선택 (기본값: 최근 7일)
        default_end = datetime.now().date()
        default_start = default_end - timedelta(days=7)
        
        date_range = st.date_input(
            "📅 Date Range",
            value=(default_start, default_end),
            max_value=datetime.now().date(),
            help="시작일과 종료일을 선택하세요"
        )
    
    # 날짜 범위 검증
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        st.warning("⚠️ 시작일과 종료일을 모두 선택해주세요.")
        return
    
    # 날짜 차이 계산
    date_diff = (end_date - start_date).days
    if date_diff > 90:
        st.warning("⚠️ 최대 90일까지 조회 가능합니다.")
        return
    
    st.markdown("---")
    
    # ========== 데이터 로드 ==========
    with st.spinner("🔄 데이터 로딩 중..."):
        try:
            df = load_creative_trend_data(
                start_date=str(start_date),
                end_date=str(end_date),
                selected_app=selected_app,
                selected_os=selected_os,
                selected_locality=selected_locality
            )
            
            if len(df) == 0:
                st.warning("⚠️ 선택한 조건에 맞는 데이터가 없습니다.")
                return
            
            # st.success(f"✅ {len(df):,}개 레코드 로드 완료! ({start_date} ~ {end_date})")
            
        except Exception as e:
            st.error(f"❌ 데이터 로드 실패: {str(e)}")
            return
    
    # ========== 주요 지표 요약 ==========
    st.markdown("### 📊 Summary Metrics")
    
    metric_col1, metric_col2, metric_col3, metric_col4, metric_col5 = st.columns(5)
    
    total_impressions = df['impressions'].sum()
    total_installs = df['installs'].sum()
    total_clicks = df['clicks'].sum()
    total_cost = df['cost'].sum()
    avg_cpi = total_cost / total_installs if total_installs > 0 else 0
    
    with metric_col1:
        st.metric(
            "👁️ Impressions",
            f"{total_impressions:,.0f}"
        )
    
    with metric_col2:
        st.metric(
            "📲 Installs",
            f"{total_installs:,.0f}"
        )
    
    with metric_col3:
        st.metric(
            "👆 Clicks",
            f"{total_clicks:,.0f}"
        )
    
    with metric_col4:
        st.metric(
            "💰 Total Cost",
            f"${total_cost:,.2f}"
        )
    
    with metric_col5:
        st.metric(
            "📊 Avg CPI",
            f"${avg_cpi:.2f}"
        )
    
    st.markdown("---")
    
    # ========== 시각화 영역 ==========
    st.markdown("### 📈 Trend Analysis")
    
    theme = create_plotly_theme()
    
    # 일별 추세 (Installs)
    daily_trend = df.groupby('date').agg({
        'installs': 'sum',
        'impressions': 'sum',
        'cost': 'sum'
    }).reset_index()
    
    daily_trend = daily_trend.sort_values('date')
    
    fig_trend = go.Figure()
    
    fig_trend.add_trace(go.Scatter(
        x=daily_trend['date'],
        y=daily_trend['installs'],
        mode='lines+markers',
        name='Installs',
        line=dict(color='#ff006e', width=3),
        marker=dict(size=8, color='#ff006e', line=dict(color='white', width=2))
    ))
    
    fig_trend.update_layout(
        **theme,
        title='Daily Install Trend',
        height=400,
        xaxis_title='Date',
        yaxis_title='Installs',
        hovermode='x unified'
    )
    
    st.plotly_chart(fig_trend, use_container_width=True, key='daily_trend')
    
    # 2개 컬럼: Top Creatives + Network Distribution
    viz_col1, viz_col2 = st.columns(2)
    
    with viz_col1:
        st.markdown("#### 🏆 Top 10 Creatives")
        
        top_creatives = df.groupby('subject').agg({
            'installs': 'sum',
            'cost': 'sum'
        }).reset_index()
        
        top_creatives['CPI'] = top_creatives['cost'] / top_creatives['installs']
        top_creatives = top_creatives.sort_values('installs', ascending=False).head(10)
        
        fig_bar = px.bar(
            top_creatives,
            x='subject',
            y='installs',
            text='installs',
            color='CPI',
            color_continuous_scale='Sunset'
        )
        
        fig_bar.update_layout(
            **theme,
            height=400,
            xaxis_title='',
            yaxis_title='Installs',
            xaxis={'tickangle': -45}
        )
        
        fig_bar.update_traces(
            texttemplate='%{text:,.0f}',
            textposition='outside'
        )
        
        st.plotly_chart(fig_bar, use_container_width=True, key='top_creatives')
    
    with viz_col2:
        st.markdown("#### 🌐 Network Distribution")
        
        network_perf = df.groupby('network').agg({
            'installs': 'sum'
        }).reset_index()
        
        fig_pie = go.Figure(data=[go.Pie(
            labels=network_perf['network'],
            values=network_perf['installs'],
            marker=dict(colors=['#ff006e', '#ff4d8f', '#ff77a0', '#a855f7', '#8b00ff']),
            textfont=dict(color='white', size=14)
        )])
        
        fig_pie.update_layout(
            **theme,
            height=400
        )
        
        st.plotly_chart(fig_pie, use_container_width=True, key='network_dist')
    
    st.markdown("---")
    
    # ========== 상세 데이터 테이블 ==========
    st.markdown("### 📋 Detailed Data")
    
    display_df = df[[
        'date', 'subject', 'app', 'os', 'locality', 'network',
        'impressions', 'installs', 'clicks', 'cost', 'CPI', 'IPM', 'CTR', 'CVR'
    ]].copy()
    
    display_df = display_df.sort_values(['date', 'installs'], ascending=[False, False])
    
    st.dataframe(
        display_df,
        hide_index=True,
        use_container_width=True,
        height=500
    )
    
    # CSV Export
    csv = display_df.to_csv(index=False)
    st.download_button(
        label="📥 Export CSV",
        data=csv,
        file_name=f"creative_trend_{start_date}_to_{end_date}.csv",
        mime="text/csv",
        use_container_width=False
    )
    
    st.markdown("---")
    st.caption(f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} KST")


if __name__ == "__main__":
    run()