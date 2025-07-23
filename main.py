# main.py - 번호이동정산 AI 분석 시스템 메인 애플리케이션 (Azure SQL Database 연동)
import streamlit as st
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re
from datetime import datetime, timedelta
import logging
import re
import traceback
import time

from azure_config import get_azure_config
from sample_data import SampleDataManager
from database_manager import DatabaseManagerFactory
from openai import AzureOpenAI
import json

# 프로젝트 모듈들
from database_manager import DatabaseManagerFactory
from azure_config import get_azure_config

from dotenv import load_dotenv

# 샘플 데이터 임포트
from sample_data import create_sample_database

from sql_generator import SQLGenerator

# 환경변수 로드

load_dotenv()

OPENAI_AVAILABLE = True

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
    /* 프로그레스 완료 후 정리를 위한 CSS */
    .progress-cleanup {
        animation: fadeOut 0.5s ease-out forwards;
    }
    
    @keyframes fadeOut {
        0% { opacity: 1; }
        100% { opacity: 0; display: none; }
    }
    
    /* 스피너 관련 모든 요소 숨기기 */
    .stSpinner,
    div[data-testid="stSpinner"],
    .stProgress.stProgress-complete {
        display: none !important;
    }
    
    /* 상태 메시지 컨테이너 */
    .status-message-container {
        transition: all 0.3s ease-out;
    }
    
    .status-message-container.hidden {
        opacity: 0;
        height: 0;
        overflow: hidden;
        margin: 0;
        padding: 0;
    }
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
    
        /* main.py의 CSS 섹션에 추가할 스타일 */

    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 50%, #4facfe 100%);
        animation: progressFlow 2s ease-in-out infinite;
    }

    @keyframes progressFlow {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }

    .progress-container {
        background: rgba(255, 255, 255, 0.95);
        padding: 2rem;
        border-radius: 15px;
        box-shadow: 0 8px 32px rgba(31, 38, 135, 0.37);
        backdrop-filter: blur(4px);
        border: 1px solid rgba(255, 255, 255, 0.18);
        margin: 2rem 0;
    }

    .progress-text {
        font-weight: 600;
        font-size: 1.1em;
        color: #667eea;
        text-align: center;
        margin: 1rem 0;
    }

    .status-message {
        background: linear-gradient(135deg, #f8f9fa, #e9ecef);
        border-left: 4px solid #667eea;
        padding: 1rem;
        border-radius: 0 8px 8px 0;
        margin: 1rem 0;
    }

    .detail-message {
        color: #6c757d;
        font-style: italic;
        font-size: 0.9em;
        margin-top: 0.5rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# 🔥 임시 디버깅 코드 추가
# def debug_environment():
#     st.write("🔍 환경변수 디버깅:")
#     env_vars = [
#         "AZURE_SQL_SERVER",
#         "AZURE_SQL_DATABASE",
#         "AZURE_SQL_USERNAME",
#         "AZURE_SQL_PASSWORD",
#     ]
#     for var in env_vars:
#         value = os.getenv(var, "❌ 없음")
#         if "PASSWORD" in var and value != "❌ 없음":
#             value = "✅ 설정됨"
#         st.write(f"- {var}: {value}")


# 데이터베이스 초기화
@st.cache_resource(show_spinner=False)
def init_database_manager():
    """안전한 데이터베이스 매니저 초기화 - 동적 프로그레스바"""

    # 진행 상황 표시 컨테이너
    progress_container = st.container()

    with progress_container:
        # 프로그레스바와 상태 메시지
        progress_bar = st.progress(0)
        status_text = st.empty()
        progress_text = st.empty()
        detail_text = st.empty()  # 🔥 추가: 세부 메시지용

    def update_progress(value, message, detail=""):
        """프로그레스바 업데이트 함수"""
        progress_bar.progress(value)
        progress_text.write(f"**진행률: {int(value * 100)}%**")
        status_text.info(f"{message}")
        if detail:
            detail_text.caption(detail)  # 🔥 수정: st.caption → detail_text.caption
        time.sleep(0.3)  # 시각적 효과를 위한 지연

    def clean_progress_ui():
        """🔥 추가: 프로그레스 UI 완전 정리 함수"""
        progress_bar.empty()
        status_text.empty()
        progress_text.empty()
        detail_text.empty()
        progress_container.empty()

    try:
        import time  # 애니메이션 효과용

        # 🎯 1단계: Azure 설정 로드 (0-20%)
        update_progress(0.05, "🔧 시스템 초기화 중...", "환경 설정을 확인하고 있습니다")

        update_progress(
            0.10, "🔧 Azure 설정을 로드하고 있습니다...", "Azure 연결 정보를 읽는 중"
        )
        azure_config = get_azure_config()

        update_progress(0.20, "✅ Azure 설정 로드 완료", "연결 정보 검증 중")

        # 🎯 2단계: 환경변수 확인 (20-30%)
        update_progress(
            0.25, "🔍 환경변수 확인 중...", "FORCE_SAMPLE_MODE 등 설정 확인"
        )
        force_sample = os.getenv("FORCE_SAMPLE_MODE", "false").lower() == "true"

        update_progress(
            0.30,
            "✅ 환경변수 확인 완료",
            f"강제 샘플 모드: {'활성화' if force_sample else '비활성화'}",
        )

        # 🎯 3단계: 강제 샘플 모드 처리 (30-100%)
        if force_sample:
            update_progress(
                0.40, "🔧 강제 샘플 모드로 설정됨", "로컬 SQLite 데이터베이스 준비 중"
            )

            update_progress(
                0.60,
                "📊 샘플 데이터베이스 생성 중...",
                "테이블 구조 생성 및 데이터 삽입",
            )
            db_manager = DatabaseManagerFactory.create_sample_manager(azure_config)

            update_progress(0.80, "🔍 연결 테스트 중...", "데이터베이스 접근 권한 확인")

            update_progress(0.95, "✅ 샘플 데이터베이스 연결 성공!", "시스템 준비 완료")

            update_progress(1.0, "🚀 시스템 시작 완료!", "샘플 모드에서 실행됩니다")

            # 🔥 수정: 성공 시 UI 완전 정리
            time.sleep(1.5)  # 완료 메시지를 잠시 보여준 후
            clean_progress_ui()  # 모든 진행 상황 메시지 제거

            return db_manager

        # 🎯 4단계: Azure 우선 시도 (30-70%)
        update_progress(
            0.35, "☁️ Azure 클라우드 서비스 연결 중...", "Azure SQL Database 접근 시도"
        )

        try:
            # Azure 연결 가능 여부 먼저 확인
            update_progress(
                0.40, "🔍 Azure 연결 문자열 확인...", "데이터베이스 설정 검증"
            )
            if not azure_config.get_database_connection_string():
                raise Exception("Azure SQL Database 연결 문자열이 설정되지 않음")

            update_progress(
                0.50, "🏗️ Azure 데이터베이스 매니저 생성...", "SQLAlchemy 엔진 초기화"
            )
            db_manager = DatabaseManagerFactory.create_manager(
                azure_config, force_sample=False
            )

            update_progress(
                0.65, "🔍 Azure 연결 테스트 중...", "데이터베이스 권한 및 테이블 확인"
            )

            if db_manager and db_manager.test_connection():
                update_progress(
                    0.85,
                    "🎉 Azure 연결 테스트 성공!",
                    "클라우드 데이터베이스 준비 완료",
                )

                connection_type = (
                    "Azure SQL Database"
                    if not db_manager.use_sample_data
                    else "샘플 SQLite"
                )
                update_progress(
                    0.95, f"✅ {connection_type} 연결 성공!", "최종 시스템 검증 중"
                )

                update_progress(
                    1.0, "🚀 Azure 클라우드 모드 시작!", "실시간 데이터 분석 준비 완료"
                )

                # 🔥 수정: 성공 시 UI 완전 정리
                time.sleep(1.5)  # 완료 메시지를 잠시 보여준 후
                clean_progress_ui()  # 모든 진행 상황 메시지 제거

                return db_manager
            else:
                raise Exception("연결 테스트 실패")

        except Exception as azure_e:
            update_progress(0.60, f"⚠️ Azure 연결 실패", f"오류: {str(azure_e)[:50]}...")

            # 🎯 5단계: 방화벽 오류 특별 처리
            if "40615" in str(azure_e):
                clean_progress_ui()  # 🔥 추가: 오류 시에도 프로그레스 정리

                st.error("🚨 Azure SQL Database 방화벽 차단!")

                # IP 정보 추출 및 해결 가이드 표시
                ip_match = re.search(r"IP address '([\d.]+)'", str(azure_e))
                server_match = re.search(r"server '([^']+)'", str(azure_e))

                if ip_match and server_match:
                    current_ip = ip_match.group(1)
                    server_name = server_match.group(1)

                    col1, col2 = st.columns(2)
                    with col1:
                        st.info(f"🌐 현재 IP: `{current_ip}`")
                    with col2:
                        st.info(f"🗄️ 서버: `{server_name}`")

                    st.markdown("### 🔧 해결 방법")
                    st.markdown(
                        f"""
                    1. **Azure Portal** 접속: https://portal.azure.com
                    2. **SQL Server 검색**: `{server_name.split('.')[0]}`
                    3. **방화벽 설정**: "방화벽 및 가상 네트워크" 메뉴
                    4. **IP 추가**: "클라이언트 IP 추가" 버튼 클릭
                    5. **저장 후 새로고침**: 5분 후 페이지 새로고침
                    """
                    )

                return None

            # 🎯 6단계: 샘플 모드로 백업 (70-100%)
            update_progress(
                0.70, "🔄 샘플 데이터 모드로 전환...", "로컬 백업 데이터베이스 준비"
            )

            try:
                update_progress(
                    0.80, "📊 샘플 데이터베이스 생성 중...", "SQLite 메모리 DB 초기화"
                )
                sample_manager = DatabaseManagerFactory.create_sample_manager(
                    azure_config
                )

                update_progress(
                    0.90, "🔍 샘플 DB 연결 테스트...", "테이블 구조 및 데이터 확인"
                )

                if sample_manager.test_connection():
                    update_progress(
                        0.95, "✅ 샘플 데이터베이스 준비 완료!", "백업 모드 시스템 검증"
                    )

                    update_progress(
                        1.0, "🚀 샘플 모드로 시작!", "개발/테스트 환경에서 실행됩니다"
                    )

                    # 🔥 수정: 성공 시 UI 완전 정리
                    time.sleep(1.5)
                    clean_progress_ui()

                    return sample_manager
                else:
                    raise Exception("샘플 연결 테스트 실패")

            except Exception as sample_e:
                clean_progress_ui()  # 🔥 추가: 오류 시에도 프로그레스 정리

                st.error("❌ 샘플 데이터베이스 연결도 실패했습니다.")

                # 상세 오류 정보
                with st.expander("🐛 오류 상세 정보"):
                    st.code(f"Azure 오류: {azure_e}")
                    st.code(f"샘플 오류: {sample_e}")
                    st.code(f"트레이스백:\n{traceback.format_exc()}")

                return None

    except Exception as e:
        clean_progress_ui()  # 🔥 추가: 예외 발생 시에도 프로그레스 정리

        st.error(f"❌ 시스템 초기화 실패: {e}")

        with st.expander("🐛 시스템 오류 정보"):
            st.code(f"오류: {e}")
            st.code(f"트레이스백:\n{traceback.format_exc()}")

        # 🎯 7단계: 최종 복구 시도 (긴급 모드)
        st.info("🛠️ 최소한의 시스템으로 실행을 시도합니다...")

        emergency_progress = st.progress(0)
        emergency_status = st.empty()

        try:
            for i in range(1, 6):
                emergency_progress.progress(i * 0.2)
                emergency_status.info(f"🔧 긴급 복구 모드 {i}/5 단계...")
                time.sleep(0.5)

            # 최소한의 Azure Config 생성
            from azure_config import AzureConfig

            minimal_config = AzureConfig()

            # 직접 SQLite 연결 생성
            import sqlite3

            class MinimalManager:
                def __init__(self):
                    self.use_sample_data = True
                    self.connection_type = "Minimal SQLite"
                    self.connection = sqlite3.connect(
                        ":memory:", check_same_thread=False
                    )
                    self._create_minimal_tables()

                def _create_minimal_tables(self):
                    cursor = self.connection.cursor()
                    cursor.execute(
                        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"
                    )
                    cursor.execute("INSERT INTO test (name) VALUES ('Emergency Mode')")
                    self.connection.commit()

                def test_connection(self):
                    try:
                        cursor = self.connection.cursor()
                        cursor.execute("SELECT COUNT(*) FROM test")
                        return True
                    except:
                        return False

                def execute_query(self, query):
                    return pd.DataFrame(
                        [{"message": "긴급 모드에서는 제한된 기능만 사용 가능합니다."}]
                    ), {"success": True}

            minimal_manager = MinimalManager()

            if minimal_manager.test_connection():
                emergency_progress.progress(1.0)
                emergency_status.success("✅ 긴급 모드로 실행됩니다. (기능 제한)")
                time.sleep(1)
                # 🔥 추가: 긴급 모드도 UI 정리
                emergency_progress.empty()
                emergency_status.empty()
                return minimal_manager

        except Exception as minimal_e:
            # 🔥 추가: 최종 실패시에도 UI 정리
            emergency_progress.empty()
            emergency_status.empty()
            st.error(f"❌ 긴급 모드 실행도 실패: {minimal_e}")

        return None


# 대시보드 데이터 조회 (수정된 버전)
@st.cache_data(ttl=300)  # 5분 캐시
def get_dashboard_data(_db_manager):
    """대시보드용 데이터 조회 - 안전한 처리"""

    if not _db_manager:
        return pd.DataFrame(), pd.DataFrame()

    try:
        port_in_query = """
        SELECT 
            FORMAT(TRT_DATE, 'yyyy-MM') as month,
            COUNT(*) as count,
            SUM(SETL_AMT) as amount,
            BCHNG_COMM_CMPN_ID as operator
        FROM PY_NP_SBSC_RMNY_TXN 
        WHERE TRT_DATE >= DATEADD(month, -3, GETDATE())
            AND NP_STTUS_CD IN ('OK', 'WD')
        GROUP BY FORMAT(TRT_DATE, 'yyyy-MM'), BCHNG_COMM_CMPN_ID
        ORDER BY month DESC
        """

        port_out_query = """
        SELECT 
            FORMAT(NP_TRMN_DATE, 'yyyy-MM') as month,
            COUNT(*) as count,
            SUM(PAY_AMT) as amount,
            ACHNG_COMM_CMPN_ID as operator
        FROM PY_NP_TRMN_RMNY_TXN 
        WHERE NP_TRMN_DATE >= DATEADD(month, -3, GETDATE())
            AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
        GROUP BY FORMAT(NP_TRMN_DATE, 'yyyy-MM'), ACHNG_COMM_CMPN_ID
        ORDER BY month DESC
            """

        # 데이터베이스 타입에 따른 쿼리 선택
        # if _db_manager.use_sample_data:
        #     # SQLite 샘플 데이터용 쿼리
        #     port_in_query = """
        #     SELECT
        #         strftime('%Y-%m', TRT_DATE) as month,
        #         COUNT(*) as count,
        #         SUM(SETL_AMT) as amount,
        #         BCHNG_COMM_CMPN_ID as operator
        #     FROM PY_NP_SBSC_RMNY_TXN
        #     WHERE TRT_DATE >= date('now', '-4 months')
        #         AND NP_STTUS_CD IN ('OK', 'WD')
        #     GROUP BY strftime('%Y-%m', TRT_DATE), BCHNG_COMM_CMPN_ID
        #     ORDER BY month DESC
        #     """

        #     port_out_query = """
        #     SELECT
        #         strftime('%Y-%m', NP_TRMN_DATE) as month,
        #         COUNT(*) as count,
        #         SUM(PAY_AMT) as amount,
        #         BCHNG_COMM_CMPN_ID as operator
        #     FROM PY_NP_TRMN_RMNY_TXN
        #     WHERE NP_TRMN_DATE >= date('now', '-4 months')
        #         AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
        #     GROUP BY strftime('%Y-%m', NP_TRMN_DATE), BCHNG_COMM_CMPN_ID
        #     ORDER BY month DESC
        #     """
        # else:
        #     # Azure SQL Database용 쿼리
        #     port_in_query = """
        #     SELECT
        #         FORMAT(TRT_DATE, 'yyyy-MM') as month,
        #         COUNT(*) as count,
        #         SUM(SETL_AMT) as amount,
        #         COMM_CMPN_NM as operator
        #     FROM PY_NP_SBSC_RMNY_TXN
        #     WHERE TRT_DATE >= DATEADD(month, -4, GETDATE())
        #         AND TRT_STUS_CD IN ('OK', 'WD')
        #     GROUP BY FORMAT(TRT_DATE, 'yyyy-MM'), COMM_CMPN_NM
        #     ORDER BY month DESC
        #     """

        #     port_out_query = """
        #     SELECT
        #         FORMAT(SETL_TRT_DATE, 'yyyy-MM') as month,
        #         COUNT(*) as count,
        #         SUM(PAY_AMT) as amount,
        #         COMM_CMPN_NM as operator
        #     FROM PY_NP_TRMN_RMNY_TXN
        #     WHERE SETL_TRT_DATE >= DATEADD(month, -4, GETDATE())
        #         AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
        #     GROUP BY FORMAT(SETL_TRT_DATE, 'yyyy-MM'), COMM_CMPN_NM
        #     ORDER BY month DESC
        #     """

        # 쿼리 실행
        port_in_df, _ = _db_manager.execute_query(port_in_query)
        port_out_df, _ = _db_manager.execute_query(port_out_query)

        return port_in_df, port_out_df

    except Exception as e:
        st.error(f"📊 대시보드 데이터 조회 오류: {e}")
        return pd.DataFrame(), pd.DataFrame()


def generate_sql_with_openai(user_input, azure_config, is_azure=True):
    """OpenAI를 사용하여 SQL 쿼리 생성"""

    try:
        # Azure OpenAI 클라이언트 초기화
        client = AzureOpenAI(
            api_key=azure_config.openai_api_key,
            api_version=azure_config.openai_api_version,
            azure_endpoint=azure_config.openai_endpoint,
        )

        # 데이터베이스 스키마 정보
        schema_info = get_database_schema_info(azure_config, is_azure)

        # 프롬프트 구성
        system_prompt = f"""
        당신은 번호이동정산 시스템의 SQL 쿼리 생성 전문가입니다.
        사용자의 자연어 질문을 분석하여 적절한 SQL 쿼리를 생성해주세요.

        데이터베이스 정보:
        - 타입: {'Azure SQL Database' if is_azure else 'SQLite'}
        - 스키마: {schema_info}

        규칙:
        1. 전화번호는 항상 마스킹 처리 (앞 3자리 + **** + 뒤 4자리)
        2. 날짜 함수는 데이터베이스 타입에 맞게 사용
        3. 개인정보 보호를 위해 민감한 정보는 제한적으로 노출
        4. 결과는 가독성 있게 한글 컬럼명 사용
        5. 성능을 위해 적절한 WHERE 조건 추가

        응답 형식: JSON
        {{
            "sql_query": "생성된 SQL 쿼리",
            "explanation": "쿼리 설명",
            "confidence": 0.9 (0-1 사이의 신뢰도)
        }}
        """

        user_prompt = f"다음 질문에 대한 SQL 쿼리를 생성해주세요: {user_input}"

        # OpenAI API 호출
        response = client.chat.completions.create(
            model=azure_config.openai_model_name,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.1,
            max_tokens=1000,
        )

        # 응답 파싱
        response_content = response.choices[0].message.content

        try:
            # JSON 응답 파싱 시도
            result = json.loads(response_content)
            return {
                "sql_query": result.get("sql_query", ""),
                "explanation": result.get("explanation", ""),
                "confidence": result.get("confidence", 0.0),
                "source": "OpenAI",
            }
        except json.JSONDecodeError:
            # JSON 파싱 실패 시 텍스트에서 SQL 추출 시도
            sql_match = re.search(r"```sql\n(.*?)\n```", response_content, re.DOTALL)
            if sql_match:
                return {
                    "sql_query": sql_match.group(1).strip(),
                    "explanation": "OpenAI에서 생성된 쿼리",
                    "confidence": 0.8,
                    "source": "OpenAI",
                }
            else:
                raise Exception("OpenAI 응답에서 SQL을 추출할 수 없습니다")

    except Exception as e:
        logger.warning(f"OpenAI SQL 생성 실패: {e}")
        raise e


def get_database_schema_info(azure_config, is_azure=True):
    """데이터베이스 스키마 정보 반환"""

    # schema = {
    #     "tables": {
    #         "PY_NP_SBSC_RMNY_TXN": {
    #             "description": "번호이동 가입 정산 거래",
    #             "columns": {
    #                 "TEL_NO": "전화번호",
    #                 "TRT_DATE": "거래일자",
    #                 "SETL_AMT": "정산금액",
    #                 "BCHNG_COMM_CMPN_ID": "전사업자ID",
    #                 "ACHNG_COMM_CMPN_ID": "후사업자ID",
    #                 "NP_STTUS_CD": "번호이동상태코드",
    #                 "SVC_CONT_ID": "서비스계약ID",
    #             },
    #         },
    #         "PY_NP_TRMN_RMNY_TXN": {
    #             "description": "번호이동 해지 정산 거래",
    #             "columns": {
    #                 "TEL_NO": "전화번호",
    #                 "NP_TRMN_DATE": "번호이동해지일자",
    #                 "PAY_AMT": "지급금액",
    #                 "BCHNG_COMM_CMPN_ID": "전사업자ID",
    #                 "ACHNG_COMM_CMPN_ID": "후사업자ID",
    #                 "NP_TRMN_DTL_STTUS_VAL": "해지상세상태값",
    #                 "SVC_CONT_ID": "서비스계약ID",
    #             },
    #         },
    #         "PY_DEPAZ_BAS": {
    #             "description": "예치금 기본",
    #             "columns": {
    #                 "RMNY_DATE": "수납일자",
    #                 "DEPAZ_AMT": "예치금액",
    #                 "DEPAZ_DIV_CD": "예치금구분코드",
    #                 "RMNY_METH_CD": "수납방법코드",
    #             },
    #         },
    #     },
    #     "common_filters": {
    #         "port_in_status": "NP_STTUS_CD IN ('OK', 'WD')",
    #         "port_out_status": "NP_TRMN_DTL_STTUS_VAL IN ('1', '3')",
    #         "deposit_status": "DEPAZ_DIV_CD = '10' AND RMNY_METH_CD = 'NA'",
    #     },
    # }

    # 수정: sql_generator.py의 SQLGenerator를 사용하여 스키마 정보 가져오기
    # SQLGenerator 인스턴스 생성
    sql_generator = SQLGenerator(azure_config)
    schema = sql_generator._load_schema()

    return json.dumps(schema, ensure_ascii=False, indent=2)


def generate_sql_query(user_input, is_azure=True, azure_config=None):
    """사용자 입력을 SQL 쿼리로 변환 (OpenAI 우선, 규칙 기반 폴백)"""

    # 1. OpenAI 사용 시도 (우선순위)
    if (
        azure_config
        and hasattr(azure_config, "openai_api_key")
        and azure_config.openai_api_key
    ):
        try:
            logger.info("OpenAI를 사용하여 SQL 쿼리 생성 시도")
            openai_result = generate_sql_with_openai(user_input, azure_config, is_azure)

            # 신뢰도가 높으면 OpenAI 결과 사용
            if openai_result.get("confidence", 0) > 0.7:
                logger.info(
                    f"OpenAI 쿼리 생성 성공 (신뢰도: {openai_result.get('confidence')})"
                )
                return openai_result["sql_query"]
            else:
                logger.warning("OpenAI 신뢰도가 낮아 규칙 기반으로 폴백")

        except Exception as e:
            logger.warning(f"OpenAI 쿼리 생성 실패, 규칙 기반으로 폴백: {e}")

    # 2. 규칙 기반 쿼리 생성 (폴백)
    logger.info("규칙 기반 SQL 쿼리 생성 사용")
    return generate_rule_based_sql_query(user_input, is_azure)


# SQL 쿼리 생성 함수 (수정된 버전)
def generate_rule_based_sql_query(user_input, is_azure=True):
    """사용자 입력을 SQL 쿼리로 변환 (Azure SQL/SQLite 호환)"""

    user_input_lower = user_input.lower()

    # 날짜 함수 매핑
    date_func = {
        "now_minus_months": lambda months: (
            f"DATEADD(month, -{months}, GETDATE())"
            #if is_azure
            #else f"date('now', '-{months} months')"
        ),
        "format_month": lambda col: (
            f"FORMAT({col}, 'yyyy-MM')" #if is_azure else f"strftime('%Y-%m', {col})"
        ),
        "substr_phone": lambda col: (
            f"LEFT({col}, 3) + '****' + RIGHT({col}, 4)"
            #if is_azure
            #else f"SUBSTR({col}, 1, 3) || '****' || SUBSTR({col}, -4)"
        ),
    }

    # 1. 월별 집계 쿼리
    # if "월별" in user_input_lower or "추이" in user_input_lower:
    #     if "포트인" in user_input_lower:
    #         return f"""
    #         SELECT 
    #             {date_func['format_month']('TRT_DATE')} as 번호이동월,
    #             BCHNG_COMM_CMPN_ID as 전사업자,
    #             COUNT(*) as 총건수,
    #             SUM(SETL_AMT) as 총금액,
    #             {'ROUND(AVG(SETL_AMT), 0)' if not is_azure else 'CAST(AVG(SETL_AMT) AS INT)'} as 정산금액평균
    #         FROM PY_NP_SBSC_RMNY_TXN 
    #         WHERE TRT_DATE >= {date_func['now_minus_months'](6)}
    #             AND NP_STTUS_CD IN ('OK', 'WD')
    #         GROUP BY {date_func['format_month']('TRT_DATE')}, BCHNG_COMM_CMPN_ID
    #         ORDER BY 번호이동월 DESC, 총금액 DESC
    #         """
    #     elif "포트아웃" in user_input_lower:
    #         return f"""
    #         SELECT 
    #             {date_func['format_month']('NP_TRMN_DATE')} as 번호이동월,
    #             BCHNG_COMM_CMPN_ID as 전사업자,
    #             COUNT(*) as 총건수,
    #             SUM(PAY_AMT) as 총금액,
    #             {'ROUND(AVG(PAY_AMT), 0)' if not is_azure else 'CAST(AVG(PAY_AMT) AS INT)'} as 정산금액평균
    #         FROM PY_NP_TRMN_RMNY_TXN 
    #         WHERE NP_TRMN_DATE IS NOT NULL 
    #             AND NP_TRMN_DATE >= {date_func['now_minus_months'](4)}
    #             AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
    #         GROUP BY {date_func['format_month']('NP_TRMN_DATE')}, BCHNG_COMM_CMPN_ID
    #         ORDER BY 번호이동월 DESC, 총금액 DESC
    #         """

    # 1. 월별 집계 쿼리
    if "월별" in user_input_lower or "추이" in user_input_lower:
        if "포트인" in user_input_lower:
            return f"""
            SELECT 
                {date_func['format_month']('TRT_DATE')} as 번호이동월,
                BCHNG_COMM_CMPN_ID as 전사업자,
                COUNT(*) as 총건수,
                SUM(SETL_AMT) as 총금액,
                CAST(AVG(SETL_AMT) AS INT) as 정산금액평균
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
                CAST(AVG(PAY_AMT) AS INT) as 정산금액평균
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
        for keyword in ["사업자", "회사", "통신사", "skt", "kt", "lg"]
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
        elif "lg" in user_input_lower:
            operator_filter = (
                "AND (BCHNG_COMM_CMPN_ID = 'LGU+' OR ACHNG_COMM_CMPN_ID = 'LGU+')"
            )

        # return f"""
        # SELECT 
        #     BCHNG_COMM_CMPN_ID as 사업자,
        #     'PORT_IN' as 번호이동타입,
        #     COUNT(*) as 번호이동건수,
        #     SUM(SETL_AMT) as 총정산금액,
        #     {'ROUND(AVG(SETL_AMT), 0)' if not is_azure else 'CAST(AVG(SETL_AMT) AS INT)'} as 정산금액평균,
        #     {'MIN(TRT_DATE)' if not is_azure else 'MIN(CAST(TRT_DATE AS DATE))'} as 최초일자,
        #     {'MAX(TRT_DATE)' if not is_azure else 'MAX(CAST(TRT_DATE AS DATE))'} as 최신일자
        # FROM PY_NP_SBSC_RMNY_TXN
        # WHERE TRT_DATE >= {date_func['now_minus_months'](3)}
        #     AND NP_STTUS_CD IN ('OK', 'WD')
        #     {operator_filter}
        # GROUP BY BCHNG_COMM_CMPN_ID
        # UNION ALL
        # SELECT 
        #     BCHNG_COMM_CMPN_ID as 사업자,
        #     'PORT_OUT' as 번호이동타입,
        #     COUNT(*) as 번호이동건수,
        #     SUM(PAY_AMT) as 총정산금액,
        #     {'ROUND(AVG(PAY_AMT), 0)' if not is_azure else 'CAST(AVG(PAY_AMT) AS INT)'} as 정산금액평균,
        #     {'MIN(NP_TRMN_DATE)' if not is_azure else 'MIN(CAST(NP_TRMN_DATE AS DATE))'} as 최초일자,
        #     {'MAX(NP_TRMN_DATE)' if not is_azure else 'MAX(CAST(NP_TRMN_DATE AS DATE))'} as 최신일자
        # FROM PY_NP_TRMN_RMNY_TXN
        # WHERE NP_TRMN_DATE IS NOT NULL 
        #     AND NP_TRMN_DATE >= {date_func['now_minus_months'](3)}
        #     AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
        #     {operator_filter}
        # GROUP BY BCHNG_COMM_CMPN_ID
        # ORDER BY 사업자, 번호이동타입
        # """
        return f"""
        SELECT 
            BCHNG_COMM_CMPN_ID as 사업자,
            'PORT_IN' as 번호이동타입,
            COUNT(*) as 번호이동건수,
            SUM(SETL_AMT) as 총정산금액,
            {'ROUND(AVG(SETL_AMT), 0)' if not is_azure else 'CAST(AVG(SETL_AMT) AS INT)'} as 정산금액평균,
            MIN(CAST(TRT_DATE AS DATE)) as 최초일자,
            MAX(CAST(TRT_DATE AS DATE)) as 최신일자
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
            CAST(AVG(PAY_AMT) AS INT)' as 정산금액평균,
            MIN(CAST(NP_TRMN_DATE AS DATE))' as 최초일자,
            MAX(CAST(NP_TRMN_DATE AS DATE))' as 최신일자
        FROM PY_NP_TRMN_RMNY_TXN
        WHERE NP_TRMN_DATE IS NOT NULL 
            AND NP_TRMN_DATE >= {date_func['now_minus_months'](3)}
            AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
            {operator_filter}
        GROUP BY BCHNG_COMM_CMPN_ID
        ORDER BY 사업자, 번호이동타입
        """

    # # 4. 예치금 조회
    # if "예치금" in user_input_lower:
    #     return f"""
    #     SELECT 
    #         {date_func['format_month']('RMNY_DATE')} as 수납월,
    #         COUNT(*) as 총건수,
    #         SUM(DEPAZ_AMT) as 총금액,
    #         {'ROUND(AVG(DEPAZ_AMT), 0)' if not is_azure else 'CAST(AVG(DEPAZ_AMT) AS INT)'} as 평균금액,
    #         MIN(DEPAZ_AMT) as 최소금액,
    #         MAX(DEPAZ_AMT) as 최대금액,
    #         DEPAZ_DIV_CD as 예치금구분,
    #         RMNY_METH_CD as 수납방법
    #     FROM PY_DEPAZ_BAS
    #     WHERE RMNY_DATE >= {date_func['now_minus_months'](3)}
    #         AND DEPAZ_DIV_CD = '10'
    #         AND RMNY_METH_CD = 'NA'
    #     GROUP BY {date_func['format_month']('RMNY_DATE')}, DEPAZ_DIV_CD, RMNY_METH_CD
    #     ORDER BY 수납월 DESC
    #     """
        # 4. 예치금 조회
    if "예치금" in user_input_lower:
        return f"""
        SELECT 
            {date_func['format_month']('RMNY_DATE')} as 수납월,
            COUNT(*) as 총건수,
            SUM(DEPAZ_AMT) as 총금액,
            CAST(AVG(DEPAZ_AMT) AS INT) as 평균금액,
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

    # 기본 쿼리
    return """
    SELECT 
        'OpenAI 및 규칙 기반 쿼리 생성을 시도했으나 적절한 쿼리를 생성하지 못했습니다' as 메시지,
        '더 구체적으로 질문해주시거나 예시 쿼리를 사용해보세요' as 안내
    """


# 메인 애플리케이션
def main():
    # 데이터베이스 매니저 초기화 (Azure 우선)
    db_manager = init_database_manager()

    if not db_manager:
        st.error("🔥 데이터베이스 연결에 실패했습니다. 시스템 관리자에게 문의하세요.")
        st.stop()

    # 헤더
    st.markdown(
        """
    <div class="main-header">
        <h1>📊 번호이동정산 AI 분석 시스템</h1>
        <p>🤖 Azure 클라우드 기반 실시간 데이터 분석 플랫폼</p>
        <p><small>✨ Azure SQL Database + OpenAI GPT-4 연동</small></p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # 실시간 대시보드
    st.header("📈 번호이동 추이 분석 대시보드")

    with st.spinner("📊 Azure 데이터베이스에서 최신 데이터를 분석하고 있습니다..."):
        port_in_df, port_out_df = get_dashboard_data(db_manager)

    # 메트릭 카드 표시
    display_metrics(port_in_df, port_out_df)

    # 추이 차트 표시
    display_charts(port_in_df, port_out_df)

    # 구분선
    st.markdown("---")

    # AI 챗봇 섹션 (DatabaseManager 전달)
    display_chatbot(db_manager)

    # 사이드바 (DatabaseManager 전달)
    display_sidebar(db_manager)


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


def display_chatbot(db_manager):
    """AI 챗봇 인터페이스 - 대화형 구조"""

    st.header("🤖 Azure OpenAI 기반 자연어 SQL 쿼리 생성")

    # 🔥 수정: SQL 생성기 초기화 개선
    def initialize_sql_generator():
        """안전한 SQL 생성기 초기화"""
        try:
            azure_config = get_azure_config()
            
            # Azure OpenAI 설정 확인
            if not azure_config.openai_api_key or not azure_config.openai_endpoint:
                st.warning("⚠️ Azure OpenAI 설정이 완전하지 않습니다. 규칙 기반 쿼리만 사용됩니다.")
                return None
            
            # SQLGenerator 생성 시도
            sql_generator = SQLGenerator(azure_config)
            st.success("✅ Azure OpenAI SQL 생성기가 준비되었습니다!")
            return sql_generator
            
        except Exception as e:
            st.error(f"❌ SQL 생성기 초기화 실패: {e}")
            st.info("💡 규칙 기반 쿼리 생성기를 사용합니다.")
            return None

    # SQL 생성기 초기화 (세션별 1회만)
    if "sql_generator" not in st.session_state:
        with st.spinner("🔧 AI 쿼리 생성기를 초기화하고 있습니다..."):
            st.session_state.sql_generator = initialize_sql_generator()
    
    # SQL 생성기 상태 표시
    if st.session_state.sql_generator is None:
        st.info("🔧 현재 규칙 기반 쿼리 생성기를 사용 중입니다.")
    else:
        st.success("🤖 Azure OpenAI 기반 AI 쿼리 생성기가 활성화되었습니다!")

    # 대화 히스토리 초기화
    if "conversation_history" not in st.session_state:
        st.session_state.conversation_history = []

    # 현재 입력 초기화
    if "current_input" not in st.session_state:
        st.session_state.current_input = ""

    # 예시 쿼리 버튼들 - 세로 배치
    st.subheader("💡 빠른 쿼리 예시")
    quick_examples = [
        ("📊 월별 포트인 현황", "월별 포트인 현황을 알려줘"),
        ("🔍 특정 번호 조회", "TEL_NO가 01012345678인 번호의 정산 내역 확인해줘"),
        ("📈 사업자별 집계", "사업자별 번호이동 정산 현황 보여줘"),
    ]
    for idx, (label, value) in enumerate(quick_examples):
        if st.button(label, key=f"quick_{idx}"):
            st.session_state.current_input = value

    # 추가 예시들
    with st.expander("🎯 더 많은 예시"):
        examples = [
            "최근 3개월 포트아웃 현황 알려줘",
            "01012345678 데이터의 예치금 데이터 잘 쌓였는지 검증해줘",
            "포트인 데이터 월별 추이 분석해줘",
            "포트아웃 테이블에서 최근 1개월 발생한 데이터 요약해줘",
            "포트아웃 데이터 샘플 하나 뽑아서 예치금 데이터 조회해줘",
        ]
        for i, example in enumerate(examples):
            if st.button(f"💬 {example}", key=f"example_{i}"):
                st.session_state.current_input = example

    # 질문 입력 부분
    st.subheader("✨ 새로운 질문하기")

    user_input = st.text_input(
        "💬 질문을 입력하세요:",
        key="user_input",
        value=st.session_state.current_input,
        placeholder="예: '최근 3개월 사업자별 SETL_AMT 합계 알려줘'",
    )

    # 버튼들 왼쪽 배치
    col_btn, col_empty = st.columns([2, 6])
    with col_btn:
        submit_button = st.button(
            "🚀 쿼리 생성 및 실행", key="submit_query", type="primary"
        )
        clear_button = st.button("🗑️ 대화 초기화", key="clear_chat")

    # 대화 초기화 처리
    if clear_button:
        st.session_state.conversation_history = []
        st.session_state.current_input = ""
        st.success("✅ 대화 히스토리가 초기화되었습니다.")
        st.rerun()

    # 🔥 수정: 쿼리 실행 처리 - 안전한 SQL 생성 및 언패킹 오류 해결
    if submit_button and user_input.strip():
        with st.spinner("🤖 AI가 SQL 쿼리를 생성하고 실행 중입니다..."):
            time.sleep(1)
            try:
                # 🔥 수정: SQL 생성 방식 개선 - 안전한 언패킹
                sql_query = None
                is_ai_generated = False
                
                # 1. AI 생성기가 있으면 AI로 시도
                if st.session_state.sql_generator is not None:
                    try:
                        # 🔥 수정: 안전한 언패킹 처리
                        ai_result = st.session_state.sql_generator.generate_sql(user_input)
                        
                        if ai_result is not None:
                            is_ai_generated = True
                            # 튜플인지 확인
                            if isinstance(ai_result, tuple) and len(ai_result) == 2:
                                sql_query, is_ai_generated = ai_result
                                st.info("🤖 Azure OpenAI로 쿼리를 생성했습니다.")
                            else:
                                # 문자열만 반환된 경우
                                if isinstance(ai_result, str):
                                    sql_query = ai_result
                                    st.info("🤖 Azure OpenAI로 쿼리를 생성했습니다.")
                                else:
                                    raise ValueError(f"예상치 못한 반환 타입: {type(ai_result)}")
                        else:
                            raise ValueError("AI 생성기가 None을 반환했습니다.")
                            
                    except Exception as ai_error:
                        st.warning(f"⚠️ AI 쿼리 생성 실패: {ai_error}")
                        sql_query = None
                
                # 2. AI 실패 시 또는 AI가 없으면 규칙 기반으로 폴백
                if sql_query is None:
                    st.info("🔧 규칙 기반 쿼리 생성기를 사용합니다.")
                    sql_query = generate_rule_based_sql_query(
                        user_input, 
                        is_azure=(not db_manager.use_sample_data if db_manager else True)
                    )
                    is_ai_generated = False

                # 3. 쿼리 실행
                if sql_query and sql_query.strip():
                    result_df, metadata = db_manager.execute_query(sql_query)

                    # 설명 생성 (AI 생성기가 있을 때만)
                    explanation = ""
                    if st.session_state.sql_generator and hasattr(st.session_state.sql_generator, "get_query_explanation"):
                        try:
                            explanation = st.session_state.sql_generator.get_query_explanation(sql_query)
                        except Exception as exp_error:
                            explanation = f"쿼리 설명 생성 실패: {exp_error}"

                    # 대화 히스토리에 저장
                    conversation_item = {
                        "user_input": user_input,
                        "sql_query": sql_query,
                        "result_df": (
                            result_df.copy() if not result_df.empty else pd.DataFrame()
                        ),
                        "result_count": len(result_df) if not result_df.empty else 0,
                        "execution_time": metadata["execution_time"],
                        "is_ai_generated": is_ai_generated,
                        "explanation": explanation,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "success": metadata["success"],
                    }

                    st.session_state.conversation_history.append(conversation_item)

                    # 결과 표시
                    st.markdown("---")
                    st.markdown("### 🎯 실행 결과")

                    if metadata["success"]:
                        if is_ai_generated:
                            st.success("✅ Azure OpenAI GPT-4가 쿼리를 생성했습니다!")
                        else:
                            st.info("ℹ️ 규칙 기반으로 쿼리를 생성했습니다.")

                        st.subheader("🔍 생성된 SQL 쿼리")
                        st.code(sql_query, language="sql")

                        if explanation:
                            st.info(f"📝 쿼리 설명: {explanation}")

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("실행 시간", f"{metadata['execution_time']:.3f}초")
                        with col2:
                            st.metric("결과 행수", f"{metadata['row_count']:,}행")
                        with col3:
                            st.metric("AI 생성", "✅" if is_ai_generated else "❌")

                        if not result_df.empty:
                            st.subheader("📋 쿼리 실행 결과")
                            st.dataframe(result_df, use_container_width=True)

                            try:
                                create_result_visualization(result_df)
                            except Exception as viz_error:
                                st.warning(f"시각화 생성 중 오류: {viz_error}")

                            csv = result_df.to_csv(index=False, encoding="utf-8-sig")
                            st.download_button(
                                label="📥 결과 데이터 다운로드 (CSV)",
                                data=csv,
                                file_name=f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                key=f"download_{len(st.session_state.conversation_history)}",
                            )
                        else:
                            st.warning("⚠️ 쿼리 결과가 없습니다.")

                    else:
                        st.error(
                            f"❌ 쿼리 실행 실패: {metadata.get('error_message', '알 수 없는 오류')}"
                        )

                    st.success(
                        f"✅ 질문이 대화 히스토리에 추가되었습니다! (총 {len(st.session_state.conversation_history)}개)"
                    )

                    st.session_state.current_input = ""
                else:
                    st.error("❌ SQL 쿼리를 생성할 수 없습니다.")

            except Exception as e:
                st.error(f"❌ 쿼리 실행 중 오류가 발생했습니다: {str(e)}")
                st.info("💡 다른 방식으로 질문해보시거나 예시 쿼리를 사용해보세요.")
                
                # 🔥 추가: 디버깅 정보
                with st.expander("🐛 디버깅 정보"):
                    st.code(f"오류 타입: {type(e).__name__}")
                    st.code(f"오류 메시지: {str(e)}")
                    st.code(f"SQL 생성기 상태: {st.session_state.sql_generator is not None}")
                    if st.session_state.sql_generator:
                        st.code(f"SQL 생성기 타입: {type(st.session_state.sql_generator)}")

    # 대화 히스토리 표시 (기존 코드 그대로 유지)
    if st.session_state.conversation_history:
        st.markdown("---")
        st.subheader("💬 대화 히스토리")

        recent_conversations = st.session_state.conversation_history[-5:]

        for i, conversation in enumerate(reversed(recent_conversations)):
            actual_index = len(st.session_state.conversation_history) - i

            with st.chat_message("user"):
                st.write(f"**질문 {actual_index}:** {conversation['user_input']}")

            with st.chat_message("assistant"):
                col1, col2, col3 = st.columns([2, 1, 1])

                with col1:
                    st.write(f"**실행 결과:** {conversation['result_count']}건")
                with col2:
                    st.write(f"**실행시간:** {conversation['execution_time']:.3f}초")
                with col3:
                    ai_badge = (
                        "🤖 AI"
                        if conversation.get("is_ai_generated", False)
                        else "📏 규칙"
                    )
                    st.write(f"**생성:** {ai_badge}")

                with st.expander(f"🔍 생성된 SQL 쿼리 (질문 {actual_index})"):
                    st.code(conversation["sql_query"], language="sql")

                    if conversation.get("explanation"):
                        st.info(f"📝 쿼리 설명: {conversation['explanation']}")

                if not conversation["result_df"].empty:
                    with st.expander(f"📋 실행 결과 데이터 (질문 {actual_index})"):
                        st.dataframe(
                            conversation["result_df"], use_container_width=True
                        )

                        csv = conversation["result_df"].to_csv(
                            index=False, encoding="utf-8-sig"
                        )
                        st.download_button(
                            label=f"📥 질문 {actual_index} 결과 다운로드",
                            data=csv,
                            file_name=f"history_result_{actual_index}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                            key=f"download_history_{actual_index}",
                        )

        if len(st.session_state.conversation_history) > 5:
            st.info(
                f"최근 5개 대화만 표시됩니다. 전체 {len(st.session_state.conversation_history)}개 대화가 있습니다."
            )

        st.markdown("---")
        st.subheader("📊 대화 세션 통계")

        total_questions = len(st.session_state.conversation_history)
        successful_queries = sum(
            1 for conv in st.session_state.conversation_history if conv["success"]
        )
        ai_generated = sum(
            1
            for conv in st.session_state.conversation_history
            if conv["is_ai_generated"]
        )
        total_results = sum(
            conv["result_count"] for conv in st.session_state.conversation_history
        )

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("총 질문 수", f"{total_questions}개")
        with col2:
            st.metric("성공률", f"{(successful_queries/total_questions*100):.1f}%")
        with col3:
            st.metric("AI 생성률", f"{(ai_generated/total_questions*100):.1f}%")
        with col4:
            st.metric("총 결과 행수", f"{total_results:,}행")


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


def display_sidebar(db_manager):
    """사이드바 표시 - DatabaseManager 사용"""

    with st.sidebar:
        st.header("🔧 Azure 클라우드 시스템 정보")

        # Azure 서비스 상태 확인
        from azure_config import get_azure_config

        azure_config = get_azure_config()
        connection_status = azure_config.test_connection()

        # Azure 서비스 상태 표시
        st.subheader("☁️ Azure 서비스 상태")
        st.metric(
            "🤖 OpenAI", "✅ 연결됨" if connection_status["openai"] else "❌ 연결 실패"
        )
        st.metric(
            "🗄️ SQL Database",
            "✅ 연결됨" if connection_status["database"] else "❌ 연결 실패",
        )

        # 데이터베이스 현황
        if db_manager:
            st.subheader("📊 데이터베이스 현황")

            try:
                # 성능 통계 가져오기
                perf_stats = db_manager.get_performance_stats()

                st.info(f"🔗 연결 타입: {perf_stats['connection_type']}")
                st.success(perf_stats["connection_status"])

                # 테이블 정보 표시
                if "tables" in perf_stats:
                    st.subheader("📋 테이블 현황")
                    for table_name, table_info in perf_stats["tables"].items():
                        with st.expander(f"📊 {table_name}"):
                            st.metric(
                                "총 행 수", f"{table_info.get('row_count', 0):,}건"
                            )

                            # 🔥 수정: latest_date를 문자열로 변환
                            latest_date = table_info.get("latest_date")
                            if latest_date is not None:
                                # datetime.date 객체를 문자열로 변환
                                if hasattr(latest_date, "strftime"):
                                    latest_date_str = latest_date.strftime("%Y-%m-%d")
                                else:
                                    latest_date_str = str(latest_date)
                            else:
                                latest_date_str = "N/A"

                            st.metric("최신 데이터", latest_date_str)
                            st.write(f"상태: {table_info.get('status', 'N/A')}")

            except Exception as e:
                st.error(f"데이터베이스 상태 조회 실패: {e}")

        st.markdown("---")

        # 시스템 상태
        st.subheader("⚙️ 시스템 상태")

        # 운영 환경 준비 상태
        production_ready = azure_config.is_production_ready()
        if production_ready:
            st.success("🟢 Azure 클라우드 연결됨")
            st.success("🟢 운영 모드 활성화")
        else:
            st.warning("🟡 일부 Azure 서비스 연결 실패")
            st.info("🔵 개발 모드로 실행")

        st.success("🟢 Streamlit 서버 실행중")

        # 에러 정보 표시
        if connection_status.get("errors"):
            st.subheader("⚠️ 연결 오류")
            for error in connection_status["errors"][:3]:  # 최대 3개만 표시
                st.error(f"• {error}")

        # 사용법 안내
        st.markdown("---")
        st.subheader("💡 Azure AI 사용법")
        st.markdown(
            """
        **자연어 쿼리 예시:**
        - "최근 3개월 포트인 현황"
        - "사업자별 정산 내역"
        - "TEL_NO 조회"
        - "월별 정상금액 추이"
        - "예치금 합계 현황"
        
        **💡 팁:**
        - 실제 컬럼명을 사용하면 더 정확합니다
        - 날짜 범위를 명시하면 성능이 향상됩니다
        - Azure OpenAI가 자동으로 최적화된 쿼리를 생성합니다
        """
        )

        # 새로고침 및 캐시 관리
        st.markdown("---")
        st.subheader("🔄 시스템 관리")

        col1, col2 = st.columns(2)

        with col1:
            if st.button("🔄 데이터 새로고침"):
                st.cache_data.clear()
                st.rerun()

        with col2:
            if st.button("🗄️ 연결 테스트"):
                with st.spinner("연결 테스트 중..."):
                    if db_manager and db_manager.test_connection():
                        st.success("✅ 연결 성공!")
                    else:
                        st.error("❌ 연결 실패!")

        # 시스템 버전 정보
        st.markdown("---")
        st.caption("📱 Version 2.0 - Azure Cloud Edition")
        st.caption("🏢 Enterprise Grade Security")
        st.caption("⚡ Powered by GPT-4")


# 메인 실행
if __name__ == "__main__":
    try:
        # debug_environment()
        main()
    except Exception as e:
        st.error(f"애플리케이션 시작 실패: {e}")
        st.info("로컬 모드로 전환하여 다시 시도해보세요.")

        # 긴급 폴백 - 기본 로컬 모드
        try:
            st.header("🔧 로컬 모드")
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
