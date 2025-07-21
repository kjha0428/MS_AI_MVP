# main.py - 번호이동정산 AI 분석 시스템 메인 애플리케이션 (Azure SQL Database 연동)
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re
from datetime import datetime, timedelta
import logging

# Azure 설정 및 샘플 데이터 매니저 임포트
from azure_config import get_azure_config
from sample_data import SampleDataManager, create_sample_database
from database_manager import DatabaseManagerFactory

# 샘플 데이터 임포트
from sample_data import create_sample_database

# 페이지 설정
st.set_page_config(
    page_title="번호이동정산 AI 분석 시스템",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# CSS 스타일
st.markdown(
    """
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(31, 38, 135, 0.37);
    }
    
    .metric-card {
        background: rgba(255, 255, 255, 0.95);
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        border-left: 5px solid #667eea;
        transition: transform 0.2s ease;
    }
    
    .chat-container {
        background: linear-gradient(145deg, #f8f9fa, #e9ecef);
        border-radius: 15px;
        padding: 1.5rem;
        margin: 1rem 0;
        border: 1px solid #dee2e6;
    }
    
    .query-example {
        background: rgba(102, 126, 234, 0.1);
        border-left: 4px solid #667eea;
        padding: 1rem;
        margin: 0.5rem 0;
        border-radius: 0 8px 8px 0;
        cursor: pointer;
    }
    
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.6rem 1.2rem;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    
    .success-alert {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        color: white;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    
    .error-alert {
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a52 100%);
        color: white;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }

    .azure-status {
        background: linear-gradient(135deg, #0078d4 0%, #106ebe 100%);
        color: white;
        padding: 0.8rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        text-align: center;
        font-weight: 500;
    }
    
    .local-status {
        background: linear-gradient(135deg, #ffa500 0%, #ff8c00 100%);
        color: white;
        padding: 0.8rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        text-align: center;
        font-weight: 500;
</style>
""",
    unsafe_allow_html=True,
)

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# 데이터베이스 초기화
@st.cache_resource
def init_system():
    """시스템 초기화 - Azure 설정 및 데이터베이스 연결"""
    try:
        # Azure 설정 로드
        azure_config = get_azure_config()

        # 데이터베이스 매니저 생성
        db_manager = DatabaseManagerFactory.create_manager(azure_config)

        # 샘플 데이터 매니저 생성
        sample_manager = SampleDataManager(azure_config)

        # 데이터베이스 연결 생성
        conn = sample_manager.create_sample_database()

        return {
            "azure_config": azure_config,
            "db_manager": db_manager,
            "sample_manager": sample_manager,
            "connection": conn,
            "is_azure": sample_manager.is_using_azure(),
            "connection_info": sample_manager.get_connection_info(),
        }

    except Exception as e:
        logger.error(f"시스템 초기화 실패: {e}")
        # 폴백: 로컬 모드로 초기화
        azure_config = get_azure_config()
        sample_manager = SampleDataManager(azure_config, force_local=True)
        conn = sample_manager.create_sample_database()

        return {
            "azure_config": azure_config,
            "db_manager": None,
            "sample_manager": sample_manager,
            "connection": conn,
            "is_azure": False,
            "connection_info": sample_manager.get_connection_info(),
            "fallback": True,
        }


# 대시보드 데이터 조회 (수정된 버전)
@st.cache_data(ttl=300)
def get_dashboard_data(_conn, is_azure=False):
    """대시보드용 데이터 조회"""

    # 포트인 월별 집계 - 올바른 컬럼명 사용
    port_in_query = """
    SELECT 
        {} as month,
        COUNT(*) as count,
        SUM(SETL_AMT) as amount,
        BCHNG_COMM_CMPN_ID as operator
    FROM PY_NP_SBSC_RMNY_TXN 
    WHERE TRT_DATE >= {} 
        AND NP_STTUS_CD IN ('OK', 'WD')
    GROUP BY {}, BCHNG_COMM_CMPN_ID
    ORDER BY month
    """.format(
        "FORMAT(TRT_DATE, 'yyyy-MM')" if is_azure else "strftime('%Y-%m', TRT_DATE)",
        "DATEADD(month, -4, GETDATE())" if is_azure else "date('now', '-4 months')",
        "FORMAT(TRT_DATE, 'yyyy-MM')" if is_azure else "strftime('%Y-%m', TRT_DATE)",
    )

    # 포트아웃 월별 집계 - 올바른 컬럼명 사용
    port_out_query = """
    SELECT 
        {} as month,
        COUNT(*) as count,
        SUM(PAY_AMT) as amount,
        BCHNG_COMM_CMPN_ID as operator
    FROM PY_NP_TRMN_RMNY_TXN 
    WHERE NP_TRMN_DATE IS NOT NULL 
        AND NP_TRMN_DATE >= {}
        AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
    GROUP BY {}, BCHNG_COMM_CMPN_ID
    ORDER BY month
    """.format(
        (
            "FORMAT(NP_TRMN_DATE, 'yyyy-MM')"
            if is_azure
            else "strftime('%Y-%m', NP_TRMN_DATE)"
        ),
        "DATEADD(month, -4, GETDATE())" if is_azure else "date('now', '-4 months')",
        (
            "FORMAT(NP_TRMN_DATE, 'yyyy-MM')"
            if is_azure
            else "strftime('%Y-%m', NP_TRMN_DATE)"
        ),
    )

    try:
        port_in_df = pd.read_sql_query(port_in_query, _conn)
        port_out_df = pd.read_sql_query(port_out_query, _conn)
    except Exception as e:
        st.error(f"데이터 조회 오류: {e}")
        port_in_df = pd.DataFrame()
        port_out_df = pd.DataFrame()

    return port_in_df, port_out_df


# SQL 쿼리 생성 함수 (수정된 버전)
def generate_sql_query(user_input, is_azure=False):
    """사용자 입력을 SQL 쿼리로 변환 (Azure SQL/SQLite 호환)"""

    user_input_lower = user_input.lower()

    # 날짜 함수 매핑
    date_func = {
        "now_minus_months": lambda months: (
            f"DATEADD(month, -{months}, GETDATE())"
            if is_azure
            else f"date('now', '-{months} months')"
        ),
        "format_month": lambda col: (
            f"FORMAT({col}, 'yyyy-MM')" if is_azure else f"strftime('%Y-%m', {col})"
        ),
        "substr_phone": lambda col: (
            f"LEFT({col}, 3) + '****' + RIGHT({col}, 4)"
            if is_azure
            else f"SUBSTR({col}, 1, 3) || '****' || SUBSTR({col}, -4)"
        ),
    }

    # 1. 월별 집계 쿼리
    if "월별" in user_input_lower or "추이" in user_input_lower:
        if "포트인" in user_input_lower:
            return f"""
            SELECT 
                {date_func['format_month']('TRT_DATE')} as 번호이동월,
                BCHNG_COMM_CMPN_ID as 전사업자,
                COUNT(*) as 총건수,
                SUM(SETL_AMT) as 총금액,
                {'ROUND(AVG(SETL_AMT), 0)' if not is_azure else 'CAST(AVG(SETL_AMT) AS INT)'} as 정산금액평균
            FROM PY_NP_SBSC_RMNY_TXN 
            WHERE TRT_DATE >= {date_func['now_minus_months'](6)}
                AND NP_STTUS_CD IN ('OK', 'WD')
            GROUP BY {date_func['format_month']('TRT_DATE')}, BCHNG_COMM_CMPN_ID
            ORDER BY 번호이동월 DESC, 총금액 DESC
            """
        elif "포트아웃" in user_input_lower:
            return f"""
            SELECT 
                {date_func['format_month']('NP_TRMN_DATE')} as 번호이동월,
                BCHNG_COMM_CMPN_ID as 전사업자,
                COUNT(*) as 총건수,
                SUM(PAY_AMT) as 총금액,
                {'ROUND(AVG(PAY_AMT), 0)' if not is_azure else 'CAST(AVG(PAY_AMT) AS INT)'} as 정산금액평균
            FROM PY_NP_TRMN_RMNY_TXN 
            WHERE NP_TRMN_DATE IS NOT NULL 
                AND NP_TRMN_DATE >= {date_func['now_minus_months'](4)}
                AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
            GROUP BY {date_func['format_month']('NP_TRMN_DATE')}, BCHNG_COMM_CMPN_ID
            ORDER BY 번호이동월 DESC, 총금액 DESC
            """

    # 2. 전화번호 검색 (마스킹 적용)
    phone_match = re.search(r"010[- ]?\d{4}[- ]?\d{4}", user_input)
    if phone_match:
        phone = phone_match.group().replace("-", "").replace(" ", "")
        return f"""
        SELECT 
            'PORT_IN' as 번호이동타입,
            TRT_DATE as 번호이동일,
            {date_func['substr_phone']('TEL_NO')} as 전화번호,
            SVC_CONT_ID,
            SETL_AMT as 정산금액,
            BCHNG_COMM_CMPN_ID as 전사업자,
            ACHNG_COMM_CMPN_ID as 후사업자,
            NP_STTUS_CD as 상태
        FROM PY_NP_SBSC_RMNY_TXN 
        WHERE TEL_NO = '{phone}' AND NP_STTUS_CD IN ('OK', 'WD')
        UNION ALL
        SELECT 
            'PORT_OUT' as 번호이동타입,
            NP_TRMN_DATE as 번호이동일,
            {date_func['substr_phone']('TEL_NO')} as 전화번호,
            SVC_CONT_ID,
            PAY_AMT as 정산금액,
            BCHNG_COMM_CMPN_ID as 전사업자,
            ACHNG_COMM_CMPN_ID as 후사업자,
            NP_TRMN_DTL_STTUS_VAL as 상태
        FROM PY_NP_TRMN_RMNY_TXN 
        WHERE TEL_NO = '{phone}' AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
        ORDER BY 번호이동일 DESC
        """

    # 3. 사업자별 현황
    if any(
        keyword in user_input_lower
        for keyword in ["사업자", "회사", "통신사", "skt", "kt", "lgu+"]
    ):
        operator_filter = ""
        if "skt" in user_input_lower or "sk" in user_input_lower:
            operator_filter = (
                "AND (BCHNG_COMM_CMPN_ID = 'SKT' OR ACHNG_COMM_CMPN_ID = 'SKT')"
            )
        elif "kt" in user_input_lower:
            operator_filter = (
                "AND (BCHNG_COMM_CMPN_ID = 'KT' OR ACHNG_COMM_CMPN_ID = 'KT')"
            )
        elif "lgu" in user_input_lower:
            operator_filter = (
                "AND (BCHNG_COMM_CMPN_ID = 'LGU+' OR ACHNG_COMM_CMPN_ID = 'LGU+')"
            )

        return f"""
        SELECT 
            BCHNG_COMM_CMPN_ID as 사업자,
            'PORT_IN' as 번호이동타입,
            COUNT(*) as 번호이동건수,
            SUM(SETL_AMT) as 총정산금액,
            {'ROUND(AVG(SETL_AMT), 0)' if not is_azure else 'CAST(AVG(SETL_AMT) AS INT)'} as 정산금액평균,
            {'MIN(TRT_DATE)' if not is_azure else 'MIN(CAST(TRT_DATE AS DATE))'} as 최초일자,
            {'MAX(TRT_DATE)' if not is_azure else 'MAX(CAST(TRT_DATE AS DATE))'} as 최신일자
        FROM PY_NP_SBSC_RMNY_TXN
        WHERE TRT_DATE >= {date_func['now_minus_months'](3)}
            AND NP_STTUS_CD IN ('OK', 'WD')
            {operator_filter}
        GROUP BY BCHNG_COMM_CMPN_ID
        UNION ALL
        SELECT 
            BCHNG_COMM_CMPN_ID as 사업자,
            'PORT_OUT' as 번호이동타입,
            COUNT(*) as 번호이동건수,
            SUM(PAY_AMT) as 총정산금액,
            {'ROUND(AVG(PAY_AMT), 0)' if not is_azure else 'CAST(AVG(PAY_AMT) AS INT)'} as 정산금액평균,
            {'MIN(NP_TRMN_DATE)' if not is_azure else 'MIN(CAST(NP_TRMN_DATE AS DATE))'} as 최초일자,
            {'MAX(NP_TRMN_DATE)' if not is_azure else 'MAX(CAST(NP_TRMN_DATE AS DATE))'} as 최신일자
        FROM PY_NP_TRMN_RMNY_TXN
        WHERE NP_TRMN_DATE IS NOT NULL 
            AND NP_TRMN_DATE >= {date_func['now_minus_months'](3)}
            AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
            {operator_filter}
        GROUP BY BCHNG_COMM_CMPN_ID
        ORDER BY 사업자, 번호이동타입
        """

    # 4. 예치금 조회
    if "예치금" in user_input_lower:
        return f"""
        SELECT 
            {date_func['format_month']('RMNY_DATE')} as 수납월,
            COUNT(*) as 총건수,
            SUM(DEPAZ_AMT) as 총금액,
            {'ROUND(AVG(DEPAZ_AMT), 0)' if not is_azure else 'CAST(AVG(DEPAZ_AMT) AS INT)'} as 평균금액,
            MIN(DEPAZ_AMT) as 최소금액,
            MAX(DEPAZ_AMT) as 최대금액,
            DEPAZ_DIV_CD as 예치금구분,
            RMNY_METH_CD as 수납방법
        FROM PY_DEPAZ_BAS
        WHERE RMNY_DATE >= {date_func['now_minus_months'](3)}
            AND DEPAZ_DIV_CD = '10'
            AND RMNY_METH_CD = 'NA'
        GROUP BY {date_func['format_month']('RMNY_DATE')}, DEPAZ_DIV_CD, RMNY_METH_CD
        ORDER BY 수납월 DESC
        """

    # 5. 기본 현황 쿼리
    return f"""
    WITH summary AS (
        SELECT 
            'PORT_IN' as 번호이동타입,
            COUNT(*) as 번호이동건수,
            SUM(SETL_AMT) as 총정산금액,
            {'ROUND(AVG(SETL_AMT), 0)' if not is_azure else 'CAST(AVG(SETL_AMT) AS INT)'} as 정산금액평균,
            COUNT(DISTINCT BCHNG_COMM_CMPN_ID) as 관련사업자수
        FROM PY_NP_SBSC_RMNY_TXN
        WHERE TRT_DATE >= {date_func['now_minus_months'](1)}
            AND NP_STTUS_CD IN ('OK', 'WD')
        UNION ALL
        SELECT 
            'PORT_OUT' as 번호이동타입,
            COUNT(*) as 번호이동건수,
            SUM(PAY_AMT) as 총정산금액,
            {'ROUND(AVG(PAY_AMT), 0)' if not is_azure else 'CAST(AVG(PAY_AMT) AS INT)'} as 정산금액평균,
            COUNT(DISTINCT BCHNG_COMM_CMPN_ID) as 관련사업자수
        FROM PY_NP_TRMN_RMNY_TXN
        WHERE NP_TRMN_DATE IS NOT NULL 
            AND NP_TRMN_DATE >= {date_func['now_minus_months'](1)}
            AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
    )
    SELECT 
        번호이동타입,
        번호이동건수,
        총정산금액,
        정산금액평균,
        관련사업자수,
        {'CASE WHEN 번호이동타입 = ''PORT_IN'' THEN ''📥 '' + 번호이동타입 ELSE ''📤 '' + 번호이동타입 END' if is_azure else 'CASE WHEN 번호이동타입 = ''PORT_IN'' THEN ''📥 '' || 번호이동타입 ELSE ''📤 '' || 번호이동타입 END'} as 타입표시
    FROM summary
    ORDER BY 총정산금액 DESC
    """


# 메인 애플리케이션
def main():
    # 시스템 초기화
    system_info = init_system()

    azure_config = system_info["azure_config"]
    db_manager = system_info["db_manager"]
    sample_manager = system_info["sample_manager"]
    conn = system_info["connection"]
    is_azure = system_info["is_azure"]
    connection_info = system_info["connection_info"]

    # 헤더
    st.markdown(
        """
    <div class="main-header">
        <h1>📊 번호이동정산 AI 분석 시스템</h1>
        <p>🤖 Azure OpenAI 기반 자연어 쿼리 생성 및 실시간 대시보드</p>
        <p><small>✨ 데이터 기반 의사결정을 위한 스마트 분석 플랫폼</small></p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # 연결 상태 표시
    display_connection_status(connection_info, system_info.get("fallback", False))

    # 실시간 대시보드
    st.header("📈 번호이동 추이 분석 대시보드")

    with st.spinner("📊 최신 데이터를 분석하고 있습니다..."):
        port_in_df, port_out_df = get_dashboard_data(conn, is_azure)

    # 메트릭 카드 표시
    display_metrics(port_in_df, port_out_df)

    # 추이 차트 표시
    display_charts(port_in_df, port_out_df)

    # 구분선
    st.markdown("---")

    # AI 챗봇 섹션
    display_chatbot(conn, is_azure)

    # 사이드바
    display_sidebar(conn, system_info)


def display_connection_status(connection_info, is_fallback=False):
    """연결 상태 표시"""

    if is_fallback:
        st.markdown(
            """
        <div class="error-alert">
            ⚠️ Azure 연결 실패로 로컬 모드로 전환되었습니다
        </div>
        """,
            unsafe_allow_html=True,
        )

    connection_type = connection_info["type"]

    if connection_type == "Azure SQL Database":
        st.markdown(
            """
        <div class="azure-status">
            ☁️ Azure SQL Database 연결됨 | 실시간 데이터 사용
        </div>
        """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
        <div class="local-status">
            💻 로컬 SQLite 모드 | 샘플 데이터 사용
        </div>
        """,
            unsafe_allow_html=True,
        )


def display_metrics(port_in_df, port_out_df):
    """주요 메트릭 표시"""

    col1, col2, col3, col4 = st.columns(4)

    # 총 건수 및 금액 계산
    total_port_in = port_in_df["count"].sum() if not port_in_df.empty else 0
    total_port_out = port_out_df["count"].sum() if not port_out_df.empty else 0
    total_in_amount = port_in_df["amount"].sum() if not port_in_df.empty else 0
    total_out_amount = port_out_df["amount"].sum() if not port_out_df.empty else 0

    with col1:
        st.metric(
            label="📥 총 포트인",
            value=f"{total_port_in:,}건",
            delta=(
                f"+{total_port_in - total_port_out}건"
                if total_port_in > total_port_out
                else None
            ),
        )

    with col2:
        st.metric(
            label="📤 총 포트아웃",
            value=f"{total_port_out:,}건",
            delta=(
                f"{total_port_out - total_port_in:+,}건"
                if total_port_out != total_port_in
                else None
            ),
        )

    with col3:
        st.metric(
            label="💰 포트인 정산액",
            value=f"{total_in_amount:,.0f}원",
            delta=f"평균 {total_in_amount/max(total_port_in,1):,.0f}원/건",
        )

    with col4:
        st.metric(
            label="💸 포트아웃 정산액",
            value=f"{total_out_amount:,.0f}원",
            delta=f"평균 {total_out_amount/max(total_port_out,1):,.0f}원/건",
        )


def display_charts(port_in_df, port_out_df):
    """추이 차트 표시"""

    if not port_in_df.empty or not port_out_df.empty:
        # 월별 총 건수 집계
        port_in_monthly = (
            port_in_df.groupby("month")
            .agg({"count": "sum", "amount": "sum"})
            .reset_index()
            if not port_in_df.empty
            else pd.DataFrame()
        )
        port_out_monthly = (
            port_out_df.groupby("month")
            .agg({"count": "sum", "amount": "sum"})
            .reset_index()
            if not port_out_df.empty
            else pd.DataFrame()
        )

        # 2x2 서브플롯 생성
        fig = make_subplots(
            rows=2,
            cols=2,
            subplot_titles=(
                "📊 월별 건수 추이",
                "💰 월별 정산액 추이",
                "🏢 사업자별 포트인 현황",
                "📈 사업자별 포트아웃 현황",
            ),
            specs=[
                [{"secondary_y": False}, {"secondary_y": False}],
                [{"secondary_y": False}, {"secondary_y": False}],
            ],
        )

        # 1. 월별 건수 추이
        if not port_in_monthly.empty:
            fig.add_trace(
                go.Scatter(
                    x=port_in_monthly["month"],
                    y=port_in_monthly["count"],
                    mode="lines+markers",
                    name="포트인",
                    line=dict(color="#1f77b4"),
                ),
                row=1,
                col=1,
            )

        if not port_out_monthly.empty:
            fig.add_trace(
                go.Scatter(
                    x=port_out_monthly["month"],
                    y=port_out_monthly["count"],
                    mode="lines+markers",
                    name="포트아웃",
                    line=dict(color="#ff7f0e"),
                ),
                row=1,
                col=1,
            )

        # 2. 월별 정산액 추이
        if not port_in_monthly.empty:
            fig.add_trace(
                go.Scatter(
                    x=port_in_monthly["month"],
                    y=port_in_monthly["amount"],
                    mode="lines+markers",
                    name="포트인 금액",
                    line=dict(color="#2ca02c"),
                ),
                row=1,
                col=2,
            )

        if not port_out_monthly.empty:
            fig.add_trace(
                go.Scatter(
                    x=port_out_monthly["month"],
                    y=port_out_monthly["amount"],
                    mode="lines+markers",
                    name="포트아웃 금액",
                    line=dict(color="#d62728"),
                ),
                row=1,
                col=2,
            )

        # 3. 사업자별 포트인 현황
        if not port_in_df.empty:
            port_in_by_operator = (
                port_in_df.groupby("operator")["count"].sum().reset_index()
            )
            fig.add_trace(
                go.Bar(
                    x=port_in_by_operator["operator"],
                    y=port_in_by_operator["count"],
                    name="포트인 사업자별",
                    marker_color="#1f77b4",
                ),
                row=2,
                col=1,
            )

        # 4. 사업자별 포트아웃 현황
        if not port_out_df.empty:
            port_out_by_operator = (
                port_out_df.groupby("operator")["count"].sum().reset_index()
            )
            fig.add_trace(
                go.Bar(
                    x=port_out_by_operator["operator"],
                    y=port_out_by_operator["count"],
                    name="포트아웃 사업자별",
                    marker_color="#ff7f0e",
                ),
                row=2,
                col=2,
            )

        fig.update_layout(
            height=800, showlegend=True, title_text="📊 번호이동 종합 분석 대시보드"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("📊 표시할 데이터가 없습니다. 샘플 데이터를 생성해주세요.")


def display_chatbot(_conn, is_azure=False):
    """AI 챗봇 인터페이스"""

    st.header("🤖 자연어 기반 SQL 쿼리 생성 챗봇")

    # DB 타입 표시
    db_type_info = "☁️ Azure SQL Database" if is_azure else "💻 SQLite"
    st.info(f"현재 연결: {db_type_info}")

    # 채팅 히스토리 초기화
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # 예시 쿼리 버튼들
    st.subheader("💡 빠른 쿼리 예시")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("📊 월별 포트인 현황"):
            st.session_state.user_input = "월별 포트인 현황을 알려줘"

    with col2:
        if st.button("🔍 특정 번호 조회"):
            st.session_state.user_input = "010-1234-5678 번호의 정산 내역 확인해줘"

    with col3:
        if st.button("📈 사업자별 집계"):
            st.session_state.user_input = "사업자별 번호이동 정산 현황 보여줘"

    # 추가 예시들
    st.markdown("### 🎯 더 많은 예시")
    examples = [
        "SK텔레콤 포트아웃 현황 알려줘",
        "최근 3개월 예치금 현황 보여줘",
        "월별 번호이동 추이 분석해줘",
        "LG유플러스 관련 정산 내역 확인해줘",
    ]

    for i, example in enumerate(examples):
        if st.button(f"💬 {example}", key=f"example_{i}"):
            st.session_state.user_input = example

    # 사용자 입력
    user_input = st.text_input(
        "💬 질문을 입력하세요:",
        key="user_input",
        placeholder="예: '2024년 1월 SK텔레콤 포트인 정산 금액 알려줘'",
    )

    if st.button("🚀 쿼리 생성 및 실행") and user_input:
        with st.spinner("🤖 AI가 쿼리를 생성하고 실행 중입니다..."):
            try:
                # SQL 쿼리 생성 (Azure/SQLite 호환)
                sql_query = generate_sql_query(user_input, is_azure)

                # 쿼리 실행
                result_df = pd.read_sql_query(sql_query, _conn)

                # 결과 표시
                st.markdown(
                    """
                <div class="success-alert">
                    ✅ 쿼리가 성공적으로 실행되었습니다!
                </div>
                """,
                    unsafe_allow_html=True,
                )

                # 생성된 SQL 표시
                with st.expander("🔍 생성된 SQL 쿼리 보기"):
                    st.code(sql_query, language="sql")

                # 결과 데이터 표시
                if not result_df.empty:
                    st.subheader("📋 쿼리 실행 결과")
                    st.dataframe(result_df, use_container_width=True)

                    # 결과 시각화
                    create_result_visualization(result_df)

                    # CSV 다운로드
                    csv = result_df.to_csv(index=False, encoding="utf-8-sig")
                    st.download_button(
                        label="📥 결과 데이터 다운로드 (CSV)",
                        data=csv,
                        file_name=f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                    )
                else:
                    st.warning("⚠️ 쿼리 결과가 없습니다.")

                # 채팅 히스토리에 추가
                st.session_state.chat_history.append(
                    {
                        "user": user_input,
                        "sql": sql_query,
                        "result_count": len(result_df) if not result_df.empty else 0,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "db_type": "Azure SQL" if is_azure else "SQLite",
                    }
                )

            except Exception as e:
                st.markdown(
                    f"""
                <div class="error-alert">
                    ❌ 쿼리 실행 중 오류가 발생했습니다: {str(e)}
                </div>
                """,
                    unsafe_allow_html=True,
                )
                st.info("💡 다른 방식으로 질문해보시거나 예시 쿼리를 사용해보세요.")

    # 채팅 히스토리 표시
    if st.session_state.chat_history:
        st.subheader("📝 최근 쿼리 히스토리")
        with st.expander("히스토리 보기"):
            for chat in reversed(st.session_state.chat_history[-5:]):
                st.markdown(
                    f"""
                <div class="chat-container">
                    <strong>🗣️ 질문:</strong> {chat['user']}<br>
                    <strong>⏰ 시간:</strong> {chat['timestamp']}<br>
                    <strong>📊 결과:</strong> {chat['result_count']}건<br>
                    <strong>🗄️ DB:</strong> {chat.get('db_type', 'Unknown')}
                </div>
                """,
                    unsafe_allow_html=True,
                )

    # 채팅 히스토리 표시
    if st.session_state.chat_history:
        st.subheader("📝 최근 쿼리 히스토리")
        with st.expander("히스토리 보기"):
            for chat in reversed(st.session_state.chat_history[-5:]):
                st.markdown(
                    f"""
                <div class="chat-container">
                    <strong>🗣️ 질문:</strong> {chat['user']}<br>
                    <strong>⏰ 시간:</strong> {chat['timestamp']}<br>
                    <strong>📊 결과:</strong> {chat['result_count']}건
                </div>
                """,
                    unsafe_allow_html=True,
                )


def create_result_visualization(df):
    """결과 데이터 시각화"""

    if len(df.columns) < 2:
        return

    # 컬럼명을 기반으로 적절한 차트 생성
    columns = df.columns.tolist()

    # 총금액과 사업자가 있는 경우
    if "총금액" in columns and (
        "사업자" in columns or "전사업자" in columns or "후사업자" in columns
    ):
        operator_col = next(
            (col for col in ["사업자", "전사업자", "후사업자"] if col in columns), None
        )
        if operator_col:
            fig = px.bar(
                df,
                x=operator_col,
                y="총금액",
                color="번호이동타입" if "번호이동타입" in columns else None,
                title="💰 사업자별 정산 금액 비교",
            )
            st.plotly_chart(fig, use_container_width=True)

    # 월별 데이터가 있는 경우
    elif "번호이동월" in columns and "총금액" in columns:
        operator_col = next(
            (col for col in ["전사업자", "후사업자"] if col in columns), None
        )
        fig = px.line(
            df,
            x="번호이동월",
            y="총금액",
            color=operator_col if operator_col else None,
            title="📈 월별 정산 금액 추이",
        )
        st.plotly_chart(fig, use_container_width=True)

    # 번호이동타입별 데이터가 있는 경우
    elif "번호이동타입" in columns and "번호이동건수" in columns:
        fig = px.pie(
            df,
            values="번호이동건수",
            names="번호이동타입",
            title="📊 포트인/포트아웃 비율",
        )
        st.plotly_chart(fig, use_container_width=True)


def display_sidebar(_conn, system_info):
    """사이드바 표시"""

    with st.sidebar:
        st.header("🔧 시스템 정보")

        # 연결 정보 표시
        connection_info = system_info["connection_info"]
        is_azure = system_info["is_azure"]

        st.subheader("🔗 데이터베이스 연결")
        if is_azure:
            st.success("☁️ Azure SQL Database")
            st.info("🔵 운영 모드")
        else:
            st.warning("💻 로컬 SQLite")
            st.info("🟡 개발 모드 (샘플 데이터)")

        # 데이터베이스 현황
        st.subheader("📊 데이터 현황")
        try:
            if is_azure:
                # Azure SQL Database 쿼리
                port_in_count = pd.read_sql_query(
                    "SELECT COUNT(*) as count FROM PY_NP_SBSC_RMNY_TXN WHERE NP_STTUS_CD IN ('OK', 'WD')",
                    _conn,
                ).iloc[0]["count"]
                port_out_count = pd.read_sql_query(
                    "SELECT COUNT(*) as count FROM PY_NP_TRMN_RMNY_TXN WHERE NP_TRMN_DTL_STTUS_VAL IN ('1', '3')",
                    _conn,
                ).iloc[0]["count"]
                deposit_count = pd.read_sql_query(
                    "SELECT COUNT(*) as count FROM PY_DEPAZ_BAS WHERE DEPAZ_DIV_CD = '10'",
                    _conn,
                ).iloc[0]["count"]
            else:
                # SQLite 쿼리
                port_in_count = pd.read_sql_query(
                    "SELECT COUNT(*) as count FROM PY_NP_SBSC_RMNY_TXN", _conn
                ).iloc[0]["count"]
                port_out_count = pd.read_sql_query(
                    "SELECT COUNT(*) as count FROM PY_NP_TRMN_RMNY_TXN", _conn
                ).iloc[0]["count"]
                deposit_count = pd.read_sql_query(
                    "SELECT COUNT(*) as count FROM PY_DEPAZ_BAS", _conn
                ).iloc[0]["count"]

            st.metric("📥 포트인 데이터", f"{port_in_count:,}건")
            st.metric("📤 포트아웃 데이터", f"{port_out_count:,}건")
            st.metric("💰 예치금 데이터", f"{deposit_count:,}건")

        except Exception as e:
            st.error(f"데이터 조회 오류: {e}")

        st.markdown("---")

        # Azure 서비스 상태
        st.subheader("⚙️ Azure 서비스 상태")
        azure_config = system_info["azure_config"]

        try:
            # Azure 연결 테스트
            test_results = azure_config.test_connection()

            st.write(
                "🔐 Key Vault:",
                "✅ 연결됨" if test_results["key_vault"] else "❌ 연결 실패",
            )
            st.write(
                "🤖 OpenAI:", "✅ 연결됨" if test_results["openai"] else "❌ 연결 실패"
            )
            st.write(
                "🗄️ Database:",
                "✅ 연결됨" if test_results["database"] else "❌ 연결 실패",
            )

            production_ready = azure_config.is_production_ready()
            if production_ready:
                st.success("🟢 운영 준비 완료")
            else:
                st.warning("🟡 개발 모드")
                if test_results["errors"]:
                    with st.expander("오류 세부사항"):
                        for error in test_results["errors"]:
                            st.text(f"• {error}")

        except Exception as e:
            st.error(f"Azure 상태 확인 실패: {e}")

        st.markdown("---")

        # 데이터 관리
        st.subheader("🗂️ 데이터 관리")

        if is_azure:
            # Azure 모드에서 샘플 데이터 관리
            col1, col2 = st.columns(2)

            with col1:
                if st.button("🔄 데이터 새로고침"):
                    st.cache_data.clear()
                    st.rerun()

            with col2:
                if st.button("🧹 샘플 데이터 정리"):
                    try:
                        sample_manager = system_info["sample_manager"]
                        sample_manager.cleanup_sample_data(_conn)
                        st.success("샘플 데이터 정리 완료")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"정리 실패: {e}")

            # 강제 로컬 모드 전환
            if st.button("💻 로컬 모드로 전환"):
                st.cache_resource.clear()
                st.session_state.clear()
                st.rerun()

        else:
            # 로컬 모드에서의 데이터 관리
            if st.button("🔄 데이터 새로고침"):
                st.cache_data.clear()
                st.cache_resource.clear()
                st.rerun()

            # Azure 모드 시도
            if st.button("☁️ Azure 모드 시도"):
                st.cache_resource.clear()
                st.session_state.clear()
                st.rerun()

        st.markdown("---")

        # 사용법 안내
        st.subheader("💡 사용법 안내")
        st.markdown(
            """
        **쿼리 예시:**
        - "월별 포트인 현황"
        - "SK텔레콤 정산 내역"
        - "010-1234-5678 번호 조회"
        - "사업자별 비교"
        - "예치금 현황"
        
        **데이터베이스:**
        - ☁️ Azure: 실시간 운영 데이터
        - 💻 로컬: 샘플 테스트 데이터
        """
        )

        st.markdown("---")


# 메인 실행
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"애플리케이션 시작 실패: {e}")
        st.info("로컬 모드로 전환하여 다시 시도해보세요.")

        # 긴급 폴백 - 기본 로컬 모드
        try:
            st.header("🔧 긴급 복구 모드")
            st.warning("시스템 오류로 인해 기본 모드로 실행됩니다.")

            # 기본 샘플 데이터베이스 생성
            conn = create_sample_database()

            st.success("✅ 기본 데이터베이스 연결 성공")

            # 기본 현황 표시
            try:
                basic_query = """
                SELECT 
                    'PORT_IN' as type,
                    COUNT(*) as count,
                    SUM(SETL_AMT) as amount
                FROM PY_NP_SBSC_RMNY_TXN
                UNION ALL
                SELECT 
                    'PORT_OUT' as type,
                    COUNT(*) as count,
                    SUM(PAY_AMT) as amount
                FROM PY_NP_TRMN_RMNY_TXN
                """

                basic_df = pd.read_sql_query(basic_query, conn)
                st.dataframe(basic_df)

            except Exception as basic_error:
                st.error(f"기본 쿼리 실행 실패: {basic_error}")

        except Exception as fallback_error:
            st.error(f"긴급 복구 모드 실패: {fallback_error}")
            st.info("시스템 관리자에게 문의하세요.")
