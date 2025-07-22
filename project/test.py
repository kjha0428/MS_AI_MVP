# minimal_main.py - 즉시 실행 가능한 최소 버전

import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
from datetime import datetime, timedelta
import random

st.set_page_config(
    page_title="번호이동정산 AI 분석 시스템", page_icon="📊", layout="wide"
)

# 헤더
st.title("📊 번호이동정산 AI 분석 시스템")
st.subheader("🚀 빠른 실행 모드")


# 즉시 실행되는 샘플 데이터 생성
@st.cache_resource
def create_instant_data():
    """즉시 생성되는 샘플 데이터"""

    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # 테이블 생성
    cursor.execute(
        """
        CREATE TABLE port_data (
            id INTEGER PRIMARY KEY,
            date DATE,
            amount INTEGER,
            operator TEXT,
            type TEXT
        )
    """
    )

    # 샘플 데이터 생성
    operators = ["SKT", "KT", "LGU+"]
    types = ["PORT_IN", "PORT_OUT"]

    for i in range(30):
        date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        operator = operators[i % 3]
        port_type = types[i % 2]
        amount = random.randint(15000, 50000)

        cursor.execute(
            """
            INSERT INTO port_data (date, amount, operator, type)
            VALUES (?, ?, ?, ?)
        """,
            (date, amount, operator, port_type),
        )

    conn.commit()
    return conn


# 데이터 로드
st.write("📊 샘플 데이터 생성 중...")
conn = create_instant_data()
st.success("✅ 데이터 준비 완료!")


# 데이터 조회
@st.cache_data
def get_data(_conn):
    query = """
    SELECT 
        strftime('%Y-%m', date) as month,
        operator,
        type,
        COUNT(*) as count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM port_data 
    GROUP BY strftime('%Y-%m', date), operator, type
    ORDER BY month DESC
    """
    return pd.read_sql_query(query, _conn)


df = get_data(conn)

# 메트릭 표시
col1, col2, col3, col4 = st.columns(4)

total_count = df["count"].sum()
total_amount = df["total_amount"].sum()
port_in_count = df[df["type"] == "PORT_IN"]["count"].sum()
port_out_count = df[df["type"] == "PORT_OUT"]["count"].sum()

with col1:
    st.metric("총 거래 건수", f"{total_count:,}건")
with col2:
    st.metric("총 정산 금액", f"{total_amount:,.0f}원")
with col3:
    st.metric("포트인", f"{port_in_count:,}건")
with col4:
    st.metric("포트아웃", f"{port_out_count:,}건")

# 차트 표시
st.subheader("📈 월별 거래 현황")

col1, col2 = st.columns(2)

with col1:
    # 월별 거래량
    monthly_data = df.groupby(["month", "type"])["count"].sum().reset_index()
    fig1 = px.bar(
        monthly_data,
        x="month",
        y="count",
        color="type",
        title="월별 거래 건수",
        barmode="group",
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    # 사업자별 현황
    operator_data = df.groupby(["operator", "type"])["total_amount"].sum().reset_index()
    fig2 = px.bar(
        operator_data,
        x="operator",
        y="total_amount",
        color="type",
        title="사업자별 정산 금액",
        barmode="group",
    )
    st.plotly_chart(fig2, use_container_width=True)

# 데이터 테이블
st.subheader("📋 상세 데이터")
st.dataframe(df, use_container_width=True)

# 간단한 쿼리 실행기
st.subheader("🔍 간단한 데이터 조회")

query_options = {
    "전체 현황": "SELECT type, COUNT(*) as count, SUM(amount) as total FROM port_data GROUP BY type",
    "월별 집계": "SELECT strftime('%Y-%m', date) as month, COUNT(*) as count, SUM(amount) as total FROM port_data GROUP BY month ORDER BY month DESC",
    "사업자별 집계": "SELECT operator, COUNT(*) as count, AVG(amount) as avg_amount FROM port_data GROUP BY operator",
}

selected_query = st.selectbox("쿼리 선택:", list(query_options.keys()))

if st.button("쿼리 실행"):
    query = query_options[selected_query]
    result = pd.read_sql_query(query, conn)
    st.dataframe(result, use_container_width=True)

# 시스템 정보
with st.sidebar:
    st.header("🔧 시스템 정보")
    st.success("✅ SQLite 메모리 DB")
    st.success("✅ 샘플 데이터 30건")
    st.info("ℹ️ 빠른 실행 모드")

    st.markdown("---")
    st.markdown("### 💡 이 버전의 특징")
    st.markdown(
        """
    - ⚡ 즉시 실행
    - 📊 기본 차트 제공
    - 🔍 간단한 쿼리 실행
    - 💾 메모리 기반 데이터
    """
    )

st.markdown("---")
st.caption("📱 최소 실행 버전 - 빠른 테스트용")
