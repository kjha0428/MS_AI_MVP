import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import openai
from datetime import datetime, timedelta
import re
import json

# 페이지 설정
st.set_page_config(
    page_title="번호이동정산 AI 분석 시스템",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# CSS 스타일링
st.markdown(
    """
<style>
    .main-header {
        text-align: center;
        color: #1f77b4;
        margin-bottom: 30px;
    }
    .chart-container {
        background-color: #f8f9fa;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 30px;
    }
    .chat-container {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
    }
    .user-message {
        background-color: #e3f2fd;
        padding: 10px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .ai-message {
        background-color: #f5f5f5;
        padding: 10px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .query-box {
        background-color: #fff3e0;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #ff9800;
        margin: 10px 0;
    }
</style>
""",
    unsafe_allow_html=True,
)


# 데이터베이스 설정 및 샘플 데이터 생성
@st.cache_resource
def init_database():
    conn = sqlite3.connect(":memory:", check_same_thread=False)

    # 테이블 생성
    conn.execute(
        """
        CREATE TABLE number_port_history (
            transaction_id TEXT PRIMARY KEY,
            phone_number TEXT,
            service_contract_id TEXT,
            port_type TEXT,
            operator_name TEXT,
            transaction_date DATE
        )
    """
    )

    conn.execute(
        """
        CREATE TABLE provisional_receipt (
            transaction_id TEXT,
            settlement_amount INTEGER,
            transaction_date DATE,
            operator_name TEXT,
            port_type TEXT,
            FOREIGN KEY(transaction_id) REFERENCES number_port_history(transaction_id)
        )
    """
    )

    conn.execute(
        """
        CREATE TABLE deposit_account (
            service_contract_id TEXT,
            deposit_amount INTEGER,
            deposit_date DATE
        )
    """
    )

    # 샘플 데이터 삽입
    import random
    from datetime import datetime, timedelta

    operators = ["KT", "SKT", "LGU+", "KT알뜰폰", "SKT알뜰폰"]
    port_types = ["PORT_IN", "PORT_OUT"]

    # 지난 4개월 데이터 생성
    base_date = datetime.now() - timedelta(days=120)

    for i in range(1000):
        transaction_id = f"T{i+1:06d}"
        phone_number = f"010-{random.randint(1000,9999)}-{random.randint(1000,9999)}"
        service_contract_id = f"SC{i+1:06d}"
        port_type = random.choice(port_types)
        operator_name = random.choice(operators)
        transaction_date = (
            base_date + timedelta(days=random.randint(0, 120))
        ).strftime("%Y-%m-%d")
        settlement_amount = random.randint(1000, 50000)
        deposit_amount = random.randint(500, settlement_amount)

        conn.execute(
            """
            INSERT INTO number_port_history 
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                transaction_id,
                phone_number,
                service_contract_id,
                port_type,
                operator_name,
                transaction_date,
            ),
        )

        conn.execute(
            """
            INSERT INTO provisional_receipt 
            VALUES (?, ?, ?, ?, ?)
        """,
            (
                transaction_id,
                settlement_amount,
                transaction_date,
                operator_name,
                port_type,
            ),
        )

        conn.execute(
            """
            INSERT INTO deposit_account 
            VALUES (?, ?, ?)
        """,
            (service_contract_id, deposit_amount, transaction_date),
        )

    conn.commit()
    return conn


# OpenAI API 설정 (실제 환경에서는 환경변수나 Azure Key Vault 사용)
# openai.api_key = "your-openai-api-key"


# 모의 OpenAI 응답 함수 (실제 환경에서는 OpenAI API 호출)
def generate_sql_query(user_input, table_schema):
    """
    실제 환경에서는 Azure OpenAI API를 호출하여 SQL 쿼리를 생성
    현재는 패턴 매칭으로 모의 구현
    """
    user_input_lower = user_input.lower()

    # 패턴 1: 사업자별 집계 조회
    if any(
        keyword in user_input_lower
        for keyword in ["사업자", "통신사", "집계", "합계", "총액"]
    ):
        # 월, 사업자명, 포트타입 추출
        month_match = re.search(r"(\d{1,2})월|(\d{4})년\s*(\d{1,2})월", user_input)
        operator_match = re.search(r"(KT|SKT|LGU\+|알뜰폰)", user_input)
        port_match = re.search(r"(포트인|포트아웃|PORT_IN|PORT_OUT)", user_input)

        query = """
SELECT 
    operator_name,
    port_type,
    COUNT(*) as transaction_count,
    SUM(settlement_amount) as total_amount
FROM provisional_receipt 
WHERE 1=1"""

        if month_match:
            if month_match.group(2) and month_match.group(3):  # 년월 둘다
                query += f"\n  AND strftime('%Y-%m', transaction_date) = '{month_match.group(2)}-{month_match.group(3):02d}'"
            elif month_match.group(1):  # 월만
                query += f"\n  AND strftime('%m', transaction_date) = '{int(month_match.group(1)):02d}'"

        if operator_match:
            query += f"\n  AND operator_name LIKE '%{operator_match.group(1)}%'"

        if port_match:
            port_type = (
                "PORT_IN"
                if "포트인" in user_input_lower or "port_in" in user_input_lower
                else "PORT_OUT"
            )
            query += f"\n  AND port_type = '{port_type}'"

        query += "\nGROUP BY operator_name, port_type\nORDER BY total_amount DESC"

        return query

    # 패턴 2: 개별 번호 조회
    elif any(
        keyword in user_input_lower for keyword in ["010-", "번호", "서비스계약", "sc"]
    ) or re.search(r"010-\d{4}-\d{4}", user_input):
        phone_match = re.search(r"(010-\d{4}-\d{4})", user_input)
        contract_match = re.search(r"(SC\d{6})", user_input)

        query = """
SELECT 
    h.phone_number,
    h.service_contract_id,
    h.port_type,
    h.operator_name,
    h.transaction_date,
    p.settlement_amount,
    d.deposit_amount,
    (p.settlement_amount - d.deposit_amount) as net_settlement
FROM number_port_history h
LEFT JOIN provisional_receipt p ON h.transaction_id = p.transaction_id
LEFT JOIN deposit_account d ON h.service_contract_id = d.service_contract_id
WHERE """

        if phone_match:
            query += f"h.phone_number = '{phone_match.group(1)}'"
        elif contract_match:
            query += f"h.service_contract_id = '{contract_match.group(1)}'"
        else:
            query += "h.phone_number LIKE '%010-%'"

        return query

    # 패턴 3: 이상 징후 분석
    elif any(
        keyword in user_input_lower
        for keyword in ["증가", "급증", "이상", "분석", "비교"]
    ):
        query = """
WITH monthly_summary AS (
    SELECT 
        operator_name,
        strftime('%Y-%m', transaction_date) as month,
        SUM(settlement_amount) as monthly_amount
    FROM provisional_receipt 
    WHERE transaction_date >= date('now', '-3 months')
    GROUP BY operator_name, strftime('%Y-%m', transaction_date)
),
growth_analysis AS (
    SELECT 
        operator_name,
        month,
        monthly_amount,
        LAG(monthly_amount) OVER (PARTITION BY operator_name ORDER BY month) as prev_amount
    FROM monthly_summary
)
SELECT 
    operator_name,
    month,
    monthly_amount,
    prev_amount,
    CASE 
        WHEN prev_amount > 0 THEN 
            ROUND((monthly_amount - prev_amount) * 100.0 / prev_amount, 2)
        ELSE NULL 
    END as growth_rate
FROM growth_analysis 
WHERE prev_amount IS NOT NULL
ORDER BY growth_rate DESC"""

        return query

    # 기본 쿼리
    else:
        return """
SELECT 
    operator_name,
    port_type,
    COUNT(*) as transaction_count,
    SUM(settlement_amount) as total_amount
FROM provisional_receipt 
GROUP BY operator_name, port_type
ORDER BY total_amount DESC
LIMIT 10"""


# 데이터 조회 함수
@st.cache_data
def get_trend_data(_conn):
    """최근 3개월 번호이동 추이 데이터 조회"""
    query = """
    SELECT 
        strftime('%Y-%m', transaction_date) as month,
        port_type,
        COUNT(*) as count,
        SUM(settlement_amount) as amount
    FROM provisional_receipt 
    WHERE transaction_date >= date('now', '-3 months')
    GROUP BY strftime('%Y-%m', transaction_date), port_type
    ORDER BY month, port_type
    """
    return pd.read_sql_query(query, _conn)


def execute_user_query(_conn, query):
    """사용자가 생성한 쿼리 실행"""
    try:
        return pd.read_sql_query(query, _conn)
    except Exception as e:
        return f"쿼리 실행 오류: {str(e)}"


# 메인 애플리케이션
def main():
    # 데이터베이스 초기화
    conn = init_database()

    # 헤더
    st.markdown(
        "<h1 class='main-header'>📊 번호이동정산 AI 분석 시스템</h1>",
        unsafe_allow_html=True,
    )

    # 상단 대시보드 (항상 표시)
    st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
    st.markdown("### 📈 최근 3개월 번호이동 추이")

    # 추이 데이터 조회
    trend_data = get_trend_data(conn)

    if not trend_data.empty:
        # 건수와 금액 차트를 나란히 배치
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 건수 추이")
            fig_count = px.line(
                trend_data,
                x="month",
                y="count",
                color="port_type",
                title="월별 번호이동 건수",
                labels={"count": "건수", "month": "월", "port_type": "포트타입"},
            )
            fig_count.update_layout(height=400)
            st.plotly_chart(fig_count, use_container_width=True)

        with col2:
            st.markdown("#### 금액 추이")
            fig_amount = px.line(
                trend_data,
                x="month",
                y="amount",
                color="port_type",
                title="월별 정산 금액",
                labels={"amount": "금액(원)", "month": "월", "port_type": "포트타입"},
            )
            fig_amount.update_layout(height=400)
            st.plotly_chart(fig_amount, use_container_width=True)

        # 요약 통계
        st.markdown("#### 📊 요약 통계")
        summary_cols = st.columns(4)

        port_in_data = trend_data[trend_data["port_type"] == "PORT_IN"]
        port_out_data = trend_data[trend_data["port_type"] == "PORT_OUT"]

        with summary_cols[0]:
            st.metric("총 포트인 건수", f"{port_in_data['count'].sum():,}")
        with summary_cols[1]:
            st.metric("총 포트아웃 건수", f"{port_out_data['count'].sum():,}")
        with summary_cols[2]:
            st.metric("총 포트인 금액", f"{port_in_data['amount'].sum():,}원")
        with summary_cols[3]:
            st.metric("총 포트아웃 금액", f"{port_out_data['amount'].sum():,}원")

    st.markdown("</div>", unsafe_allow_html=True)

    # 하단 AI 챗봇 섹션
    st.markdown("<div class='chat-container'>", unsafe_allow_html=True)
    st.markdown("### 🤖 AI 정산 데이터 조회 챗봇")
    st.markdown("자연어로 질문하시면 SQL 쿼리를 생성하고 결과를 보여드립니다.")

    # 세션 상태 초기화
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # 예시 질문 버튼들
    st.markdown("#### 💡 예시 질문")
    example_cols = st.columns(3)

    with example_cols[0]:
        if st.button("📊 KT 포트인 정산 현황"):
            st.session_state.current_input = "KT 포트인 정산 금액 알려줘"

    with example_cols[1]:
        if st.button("🔍 개별 번호 조회"):
            st.session_state.current_input = "010-1234-5678 번호 정산 내역 확인해줘"

    with example_cols[2]:
        if st.button("📈 이상 징후 분석"):
            st.session_state.current_input = "정산 금액이 급증한 사업자 찾아줘"

    # 사용자 입력
    user_input = st.text_input(
        "질문을 입력하세요:",
        value=st.session_state.get("current_input", ""),
        placeholder="예: 2024년 1월 SKT 포트인 정산 금액 알려줘",
    )

    if st.button("🚀 쿼리 생성 및 실행") and user_input:
        # 채팅 기록에 사용자 입력 추가
        st.session_state.chat_history.append({"role": "user", "content": user_input})

        # 테이블 스키마 정보
        table_schema = {
            "number_port_history": [
                "transaction_id",
                "phone_number",
                "service_contract_id",
                "port_type",
                "operator_name",
                "transaction_date",
            ],
            "provisional_receipt": [
                "transaction_id",
                "settlement_amount",
                "transaction_date",
                "operator_name",
                "port_type",
            ],
            "deposit_account": [
                "service_contract_id",
                "deposit_amount",
                "deposit_date",
            ],
        }

        # SQL 쿼리 생성
        generated_query = generate_sql_query(user_input, table_schema)

        # 쿼리 실행
        result = execute_user_query(conn, generated_query)

        # AI 응답 생성
        ai_response = {
            "role": "assistant",
            "content": f"다음 쿼리를 생성하고 실행했습니다:",
            "query": generated_query,
            "result": result,
        }

        st.session_state.chat_history.append(ai_response)

        # current_input 초기화
        if "current_input" in st.session_state:
            del st.session_state.current_input

    # 채팅 기록 표시
    if st.session_state.chat_history:
        st.markdown("#### 💬 대화 기록")

        for i, message in enumerate(
            reversed(st.session_state.chat_history[-6:])
        ):  # 최근 6개만 표시
            if message["role"] == "user":
                st.markdown(
                    f"<div class='user-message'><strong>👤 사용자:</strong> {message['content']}</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f"<div class='ai-message'><strong>🤖 AI:</strong> {message['content']}</div>",
                    unsafe_allow_html=True,
                )

                # 생성된 쿼리 표시
                if "query" in message:
                    st.markdown("<div class='query-box'>", unsafe_allow_html=True)
                    st.markdown("**생성된 SQL 쿼리:**")
                    st.code(message["query"], language="sql")
                    st.markdown("</div>", unsafe_allow_html=True)

                # 실행 결과 표시
                if "result" in message:
                    if isinstance(message["result"], pd.DataFrame):
                        if not message["result"].empty:
                            st.markdown("**실행 결과:**")
                            st.dataframe(message["result"], use_container_width=True)

                            # 결과가 숫자 데이터인 경우 간단한 차트 생성
                            numeric_cols = (
                                message["result"]
                                .select_dtypes(include=["number"])
                                .columns
                            )
                            if len(numeric_cols) > 0 and len(message["result"]) > 1:
                                if (
                                    len(message["result"]) <= 10
                                ):  # 너무 많은 데이터는 차트로 표시하지 않음
                                    chart_fig = px.bar(
                                        message["result"],
                                        x=message["result"].columns[0],
                                        y=numeric_cols[0],
                                        title=f"{numeric_cols[0]} 차트",
                                    )
                                    st.plotly_chart(chart_fig, use_container_width=True)
                        else:
                            st.info("조회 결과가 없습니다.")
                    else:
                        st.error(message["result"])

    # 채팅 기록 초기화 버튼
    if st.session_state.chat_history:
        if st.button("🗑️ 대화 기록 초기화"):
            st.session_state.chat_history = []
            st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)

    # 사이드바에 도움말
    with st.sidebar:
        st.markdown("### 📋 사용 가이드")
        st.markdown(
            """
        **대시보드 기능:**
        - 최근 3개월 번호이동 추이 자동 표시
        - 포트인/포트아웃 분리 차트
        - 건수/금액 기준 분석
        
        **AI 챗봇 기능:**
        - 자연어로 질문 입력
        - SQL 쿼리 자동 생성
        - 실행 결과 즉시 표시
        
        **질문 예시:**
        - "KT 포트인 정산 금액 알려줘"
        - "010-1234-5678 번호 확인해줘"
        - "급증한 사업자 찾아줘"
        """
        )

        st.markdown("### ⚙️ 시스템 정보")
        st.info(
            f"""
        **데이터베이스:** SQLite (In-Memory)
        **AI 모델:** GPT-4 (모의)
        **마지막 업데이트:** {datetime.now().strftime('%Y-%m-%d %H:%M')}
        """
        )


if __name__ == "__main__":
    main()
