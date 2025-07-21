# main.py - 번호이동정산 AI 분석 시스템 메인 애플리케이션
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re
from datetime import datetime, timedelta

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
</style>
""",
    unsafe_allow_html=True,
)


# 데이터베이스 초기화
@st.cache_resource
def init_database():
    """샘플 데이터베이스 초기화"""
    return create_sample_database()


# 대시보드 데이터 조회
@st.cache_data(ttl=300)  # 5분 캐시
def get_dashboard_data(_conn):
    """대시보드용 데이터 조회"""

    # 포트인 월별 집계 - 컬럼명 수정
    port_in_query = """
    SELECT 
        strftime('%Y-%m', TRT_DATE) as month,
        COUNT(*) as count,
        SUM(SETL_AMT) as amount,
        BCHNG_COMM_CMPN_ID as operator
    FROM PY_NP_SBSC_RMNY_TXN 
    WHERE TRT_DATE >= date('now', '-4 months')
    GROUP BY strftime('%Y-%m', TRT_DATE), BCHNG_COMM_CMPN_ID
    ORDER BY month
    """

    # 포트아웃 월별 집계 - 컬럼명 수정
    port_out_query = """
    SELECT 
        strftime('%Y-%m', NP_TRMN_DATE) as month,
        COUNT(*) as count,
        SUM(PAY_AMT) as amount,
        ACHNG_COMM_CMPN_ID as operator
    FROM PY_NP_TRMN_RMNY_TXN 
    WHERE NP_TRMN_DATE IS NOT NULL 
    AND NP_TRMN_DATE >= date('now', '-4 months')
    GROUP BY strftime('%Y-%m', NP_TRMN_DATE), ACHNG_COMM_CMPN_ID
    ORDER BY month
    """

    try:
        port_in_df = pd.read_sql_query(port_in_query, _conn)
        port_out_df = pd.read_sql_query(port_out_query, _conn)
    except Exception as e:
        st.error(f"데이터 조회 오류: {e}")
        port_in_df = pd.DataFrame()
        port_out_df = pd.DataFrame()

    return port_in_df, port_out_df


# SQL 쿼리 생성 함수
def generate_sql_query(user_input):
    """사용자 입력을 SQL 쿼리로 변환"""

    user_input_lower = user_input.lower()

    # 1. 월별 집계 쿼리
    if "월별" in user_input_lower or "추이" in user_input_lower:
        if "포트인" in user_input_lower:
            return """
            SELECT 
                strftime('%Y-%m', TRT_DATE) as 번호이동월,
                BCHNG_COMM_CMPN_ID as 전사업자,
                COUNT(*) as 총건수,
                SUM(SETL_AMT) as 총금액,
                ROUND(AVG(SETL_AMT), 0) as 정산금액평균
            FROM PY_NP_SBSC_RMNY_TXN 
            WHERE TRT_DATE >= date('now', '-3 months')
            GROUP BY strftime('%Y-%m', TRT_DATE), BCHNG_COMM_CMPN_ID
            ORDER BY 번호이동월 DESC, 총금액 DESC
            """
        elif "포트아웃" in user_input_lower:
            return """
            SELECT 
                strftime('%Y-%m', NP_TRMN_DATE) as 번호이동월,
                ACHNG_COMM_CMPN_ID as 후사업자,
                COUNT(*) as 총건수,
                SUM(PAY_AMT) as 총금액,
                ROUND(AVG(PAY_AMT), 0) as 정산금액평균
            FROM PY_NP_TRMN_RMNY_TXN 
            WHERE NP_TRMN_DATE IS NOT NULL 
            AND NP_TRMN_DATE >= date('now', '-3 months')
            GROUP BY strftime('%Y-%m', NP_TRMN_DATE), ACHNG_COMM_CMPN_ID
            ORDER BY 번호이동월 DESC, 총금액 DESC
            """

    # 2. 전화번호 검색
    phone_match = re.search(r"010[- ]?\d{4}[- ]?\d{4}", user_input)
    if phone_match:
        phone = phone_match.group().replace("-", "").replace(" ", "")
        return f"""
        SELECT 
            'PORT_IN' as 번호이동타입,
            TRT_DATE as 번호이동일,
            SUBSTR(TEL_NO, 1, 3) || '****' || SUBSTR(TEL_NO, -4) as 전화번호,
            SVC_CONT_ID,
            SETL_AMT as 정산금액,
            BCHNG_COMM_CMPN_ID as 사업자
        FROM PY_NP_SBSC_RMNY_TXN 
        WHERE TEL_NO = '{phone}'
        UNION ALL
        SELECT 
            'PORT_OUT' as 번호이동타입,
            NP_TRMN_DATE as 번호이동일,
            SUBSTR(TEL_NO, 1, 3) || '****' || SUBSTR(TEL_NO, -4) as 전화번호,
            SVC_CONT_ID,
            PAY_AMT as 정산금액,
            ACHNG_COMM_CMPN_ID as 사업자
        FROM PY_NP_TRMN_RMNY_TXN 
        WHERE TEL_NO = '{phone}'
        ORDER BY 번호이동일 DESC
        """

    # 3. 사업자별 현황
    if any(
        keyword in user_input_lower
        for keyword in ["사업자", "회사", "통신사", "skt", "kt", "lgu+"]
    ):
        return """
        SELECT 
            BCHNG_COMM_CMPN_ID as 사업자,
            'PORT_IN' as 번호이동타입,
            COUNT(*) as 번호이동건수,
            SUM(SETL_AMT) as 총정산금액,
            ROUND(AVG(SETL_AMT), 0) as 정산금액평균
        FROM PY_NP_SBSC_RMNY_TXN
        WHERE TRT_DATE >= date('now', '-3 months')
        GROUP BY BCHNG_COMM_CMPN_ID
        UNION ALL
        SELECT 
            ACHNG_COMM_CMPN_ID as 사업자,
            'PORT_OUT' as 번호이동타입,
            COUNT(*) as 번호이동건수,
            SUM(PAY_AMT) as 총정산금액,
            ROUND(AVG(PAY_AMT), 0) as 정산금액평균
        FROM PY_NP_TRMN_RMNY_TXN
        WHERE NP_TRMN_DATE IS NOT NULL 
        AND NP_TRMN_DATE >= date('now', '-3 months')
        GROUP BY ACHNG_COMM_CMPN_ID
        ORDER BY 사업자, 번호이동타입
        """

    # 4. 예치금 조회
    if "예치금" in user_input_lower:
        return """
        SELECT 
            COUNT(*) as 총건수,
            SUM(DEPAZ_AMT) as 총금액,
            ROUND(AVG(DEPAZ_AMT), 0) as 평균금액,
            MIN(DEPAZ_AMT) as 최소금액,
            MAX(DEPAZ_AMT) as 최대금액
        FROM PY_DEPAZ_BAS
        WHERE RMNY_DATE >= date('now', '-3 months')
        """

    # 5. 기본 현황 쿼리
    return """
    SELECT 
        'PORT_IN' as 번호이동타입,
        COUNT(*) as 번호이동건수,
        SUM(SETL_AMT) as 총정산금액,
        ROUND(AVG(SETL_AMT), 0) as 정산금액평균
    FROM PY_NP_SBSC_RMNY_TXN
    WHERE TRT_DATE >= date('now', '-1 months')
    UNION ALL
    SELECT 
        'PORT_OUT' as 번호이동타입,
        COUNT(*) as 번호이동건수,
        SUM(PAY_AMT) as 총정산금액,
        ROUND(AVG(PAY_AMT), 0) as 정산금액평균
    FROM PY_NP_TRMN_RMNY_TXN
    WHERE NP_TRMN_DATE IS NOT NULL 
    AND NP_TRMN_DATE >= date('now', '-1 months')
    """


# 메인 애플리케이션
def main():
    # 데이터베이스 초기화
    conn = init_database()

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

    # 실시간 대시보드
    st.header("📈 번호이동 추이 분석 대시보드")

    with st.spinner("📊 최신 데이터를 분석하고 있습니다..."):
        port_in_df, port_out_df = get_dashboard_data(conn)

    # 메트릭 카드 표시
    display_metrics(port_in_df, port_out_df)

    # 추이 차트 표시
    display_charts(port_in_df, port_out_df)

    # 구분선
    st.markdown("---")

    # AI 챗봇 섹션
    display_chatbot(conn)

    # 사이드바
    display_sidebar(conn)


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


def display_chatbot(_conn):
    """AI 챗봇 인터페이스"""

    st.header("🤖 자연어 기반 SQL 쿼리 생성 챗봇")

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
                # SQL 쿼리 생성
                sql_query = generate_sql_query(user_input)

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


def display_sidebar(_conn):
    """사이드바 표시"""

    with st.sidebar:
        st.header("🔧 시스템 정보")

        # 데이터베이스 현황
        try:
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
            st.error(f"데이터베이스 연결 오류: {e}")

        st.markdown("---")

        # 시스템 상태
        st.subheader("⚙️ 시스템 상태")
        st.success("🟢 샘플 데이터베이스 연결됨")
        st.info("🔵 개발 모드 (샘플 데이터)")
        st.success("🟢 Streamlit 서버 실행중")

        # 사용법 안내
        st.markdown("---")
        st.subheader("💡 사용법 안내")
        st.markdown(
            """
        **쿼리 예시:**
        - "월별 포트인 현황"
        - "SK텔레콤 정산 내역"
        - "010-1234-5678 번호 조회"
        - "사업자별 비교"
        - "예치금 현황"
        """
        )

        # 새로고침 버튼
        if st.button("🔄 데이터 새로고침"):
            st.cache_data.clear()
            st.rerun()


# 메인 실행
if __name__ == "__main__":
    main()
