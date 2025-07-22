# openai_sql_generator.py - OpenAI 기반 SQL 쿼리 생성기
import re
import json
import logging
from typing import Tuple, Optional, Dict, Any
from azure_config import AzureConfig


class OpenAISQLGenerator:
    """OpenAI를 사용한 자연어-SQL 변환기"""

    def __init__(self, azure_config: AzureConfig):
        self.azure_config = azure_config
        self.openai_client = azure_config.get_openai_client()
        self.logger = logging.getLogger(__name__)

        # 데이터베이스 스키마 정보
        self.db_schema = self._get_schema_info()

        # 예시 쿼리들
        self.example_queries = self._get_example_queries()

    def _get_schema_info(self) -> str:
        """데이터베이스 스키마 정보 반환"""
        return """
                데이터베이스 스키마:

                1. PY_NP_SBSC_RMNY_TXN (포트인 - 가입번호이동 정산 테이블)
                - TRT_DATE: 처리일자 (DATE)
                - BCHNG_COMM_CMPN_ID: 변경전 통신사 (VARCHAR) - KT, SKT, LGU+ 등
                - ACHNG_COMM_CMPN_ID: 변경후 통신사 (VARCHAR)
                - TEL_NO: 전화번호 (VARCHAR) - 개인정보이므로 마스킹 필요
                - SETL_AMT: 정산금액 (DECIMAL)
                - NP_STTUS_CD: 상태코드 (VARCHAR) - 'OK', 'CN', 'WD'
                - SVC_CONT_ID: 서비스계약ID (VARCHAR)

                2. PY_NP_TRMN_RMNY_TXN (포트아웃 - 해지번호이동 정산 테이블)
                - NP_TRMN_DATE: 번호이동해지일자 (DATE)
                - BCHNG_COMM_CMPN_ID: 변경전 통신사 (VARCHAR)
                - ACHNG_COMM_CMPN_ID: 변경후 통신사 (VARCHAR)
                - TEL_NO: 전화번호 (VARCHAR) - 개인정보이므로 마스킹 필요
                - PAY_AMT: 지급금액 (DECIMAL)
                - NP_TRMN_DTL_STTUS_VAL: 상태값 (VARCHAR) - '1', '2', '3'
                - SVC_CONT_ID: 서비스계약ID (VARCHAR)

                3. PY_DEPAZ_BAS (예치금 기본 테이블)
                - RMNY_DATE: 수납일자 (DATE)
                - DEPAZ_AMT: 예치금액 (DECIMAL)
                - DEPAZ_DIV_CD: 예치금구분 (VARCHAR) - '10': 입금, '90': 차감
                - RMNY_METH_CD: 수납방법 (VARCHAR) - 'NA': 계좌이체, 'CA': 현금
                - SVC_CONT_ID: 서비스계약ID (VARCHAR)

                PY_NP_TRMN_RMNY_TXN.SVC_CONT_ID=PY_DEPAZ_BAS AND PY_NP_TRMN_RMNY_TXN.NP_TRMN_DATE=PY_DEPAZ_BAS.RMNY_DATE인 대상은 PY_NP_TRMN_RMNY_TXN.PAY_AMT=PY_DEPAZ_BAS.DEPAZ_AMT로 정산됨.

                통신사 코드: KT, SKT, LGU+, KT MVNO, SKT MVNO, LGU+ MVNO
                """

    def _get_example_queries(self) -> str:
        """예시 쿼리들 반환"""
        return """
                예시 쿼리:

                1. 월별 포트인 현황:
                SELECT 
                    strftime('%Y-%m', TRT_DATE) as 월,
                    COUNT(*) as 건수,
                    SUM(SETL_AMT) as 총금액
                FROM PY_NP_SBSC_RMNY_TXN 
                WHERE NP_STTUS_CD IN ('OK', 'WD')
                GROUP BY strftime('%Y-%m', TRT_DATE)
                ORDER BY 월 DESC;

                2. 사업자별 정산 현황:
                SELECT 
                    BCHNG_COMM_CMPN_ID as 사업자,
                    COUNT(*) as 번호이동건수,
                    SUM(SETL_AMT) as 총정산금액
                FROM PY_NP_SBSC_RMNY_TXN
                WHERE NP_STTUS_CD IN ('OK', 'WD')
                GROUP BY BCHNG_COMM_CMPN_ID
                ORDER BY 총정산금액 DESC;

                3. 전화번호 검색하여 정산 데이터 검증 (개인정보 마스킹):
                SELECT 
                    'PORT_OUT' as 번호이동타입,
                    SUBSTR(TEL_NO, 1, 3) || '****' || SUBSTR(TEL_NO, -4) as 전화번호,
                    BCHNG_COMM_CMPN_ID as 변경전통신사,
                    ACHNG_COMM_CMPN_ID as 변경후통신사,
                    A.NP_TRMN_DATE as 번호이동일자,
                    B.RMNY_DATE as 수납일자,
                    A.PAY_AMT as 정산금액,
                    B.DEPAZ_AMT as 예치금액,
                FROM PY_NP_TRMN_RMNY_TXN A, PY_DEPZ_BAS B
                WHERE AND A.NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
                    AND B.DEPAZ_DIV_CD = '10'
                    A.SVC_CONT_ID = B.SVC_CONT_ID
                    AND A.NP_TRMN_DATE = B.RMNY_DATE
                """

    def _create_system_prompt(self, is_azure: bool = False) -> str:
        """시스템 프롬프트 생성"""
        db_type = "Azure SQL Database" if is_azure else "SQLite"
        date_functions = (
            """
            Azure SQL: DATEADD(month, -N, GETDATE()), FORMAT(date, 'yyyy-MM')
            SQLite: date('now', '-N months'), strftime('%Y-%m', date)
            """
            if is_azure
            else """
                SQLite: date('now', '-N months'), strftime('%Y-%m', date)
                """
        )

        return f"""
                당신은 번호이동정산 데이터를 분석하는 SQL 쿼리 생성 전문가입니다.

                {self.db_schema}

                {self.example_queries}

                데이터베이스 타입: {db_type}
                날짜 함수: {date_functions}

                중요한 규칙:
                1. 개인정보 보호: 전화번호는 반드시 마스킹 (SUBSTR(TEL_NO, 1, 3) || '****' || SUBSTR(TEL_NO, -4))
                2. 유효한 데이터만: 포트인은 NP_STTUS_CD IN ('OK', 'WD'), 포트아웃은 NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
                3. 최근 데이터 우선: 기본적으로 최근 3개월 데이터 조회
                4. 안전한 쿼리만: SELECT 문만 허용, DDL/DML 금지
                5. 한국어 컬럼 별칭 사용

                응답 형식: 유효한 SQL 쿼리만 반환하세요. 설명이나 다른 텍스트는 포함하지 마세요.
                """

    def generate_sql_with_openai(
        self, user_input: str, is_azure: bool = False
    ) -> Tuple[str, bool, Optional[str]]:
        """
        OpenAI를 사용하여 SQL 쿼리 생성

        Returns:
            Tuple[str, bool, str]: (SQL 쿼리, 성공 여부, 오류 메시지)
        """
        if not self.openai_client:
            return "", False, "OpenAI 클라이언트가 설정되지 않았습니다"

        try:
            # 시스템 프롬프트 생성
            system_prompt = self._create_system_prompt(is_azure)

            # OpenAI API 호출
            messages = [
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": f"다음 요청을 SQL 쿼리로 변환해주세요: {user_input}",
                },
            ]

            # 새로운 OpenAI API 방식
            if hasattr(self.openai_client, "chat") and hasattr(
                self.openai_client.chat, "completions"
            ):
                response = self.openai_client.chat.completions.create(
                    model=self.azure_config.openai_model_name,
                    messages=messages,
                    max_tokens=1000,
                    temperature=0.1,
                    top_p=0.9,
                )
                sql_query = response.choices[0].message.content.strip()

            # 구버전 OpenAI API 방식
            elif hasattr(self.openai_client, "ChatCompletion"):
                response = self.openai_client.ChatCompletion.create(
                    engine=self.azure_config.openai_model_name,
                    messages=messages,
                    max_tokens=1000,
                    temperature=0.1,
                    top_p=0.9,
                )
                sql_query = response.choices[0].message.content.strip()

            else:
                return "", False, "지원되지 않는 OpenAI 클라이언트 버전입니다"

            # SQL 블록에서 쿼리 추출
            sql_query = self._extract_sql_from_response(sql_query)

            # 쿼리 검증
            if self._validate_generated_sql(sql_query):
                self.logger.info("OpenAI SQL 쿼리 생성 성공")
                return sql_query, True, None
            else:
                return "", False, "생성된 쿼리가 안전하지 않습니다"

        except Exception as e:
            error_msg = f"OpenAI SQL 생성 실패: {str(e)}"
            self.logger.error(error_msg)
            return "", False, error_msg

    def _extract_sql_from_response(self, response: str) -> str:
        """응답에서 SQL 쿼리 추출"""
        # ```sql ... ``` 블록에서 추출
        sql_match = re.search(
            r"```sql\s*(.*?)\s*```", response, re.DOTALL | re.IGNORECASE
        )
        if sql_match:
            return sql_match.group(1).strip()

        # ``` ... ``` 블록에서 추출
        code_match = re.search(r"```\s*(.*?)\s*```", response, re.DOTALL)
        if code_match:
            return code_match.group(1).strip()

        # 블록이 없으면 전체 응답 반환
        return response.strip()

    def _validate_generated_sql(self, sql_query: str) -> bool:
        """생성된 SQL 쿼리 검증"""
        if not sql_query:
            return False

        sql_upper = sql_query.upper().strip()

        # 1. SELECT 문인지 확인
        if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
            return False

        # 2. 위험한 키워드 확인
        dangerous_keywords = [
            "DROP",
            "DELETE",
            "INSERT",
            "UPDATE",
            "ALTER",
            "CREATE",
            "TRUNCATE",
        ]
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                return False

        # 3. 허용된 테이블만 사용
        allowed_tables = ["PY_NP_SBSC_RMNY_TXN", "PY_NP_TRMN_RMNY_TXN", "PY_DEPAZ_BAS"]
        has_allowed_table = any(table in sql_query for table in allowed_tables)
        if not has_allowed_table:
            return False

        return True


# main.py에 통합할 함수들
def generate_sql_with_ai(
    user_input: str, azure_config, is_azure: bool = False
) -> Tuple[str, bool, str]:
    """
    AI를 사용하여 SQL 쿼리 생성 (main.py에서 사용)

    Returns:
        Tuple[str, bool, str]: (SQL 쿼리, AI 사용 여부, 생성 방법)
    """
    # 1. OpenAI 사용 시도
    if azure_config and azure_config.openai_api_key:
        try:
            sql_generator = OpenAISQLGenerator(azure_config)
            sql_query, success, error = sql_generator.generate_sql_with_openai(
                user_input, is_azure
            )

            if success:
                return sql_query, True, "🤖 AI 생성"
            else:
                # OpenAI 실패시 규칙 기반으로 폴백
                pass
        except Exception as e:
            logging.getLogger(__name__).warning(
                f"OpenAI 쿼리 생성 실패, 규칙 기반으로 전환: {e}"
            )

    # 2. 규칙 기반 쿼리 생성 (폴백)
    from main import generate_sql_query  # 기존 함수 임포트

    try:
        sql_query = generate_sql_query(user_input, is_azure)
        return sql_query, False, "📋 규칙 기반"
    except Exception as e:
        # 최종 폴백 - 기본 쿼리
        basic_query = f"""
        SELECT 
            'PORT_IN' as 번호이동타입,
            COUNT(*) as 번호이동건수,
            SUM(SETL_AMT) as 총정산금액
        FROM PY_NP_SBSC_RMNY_TXN
        WHERE TRT_DATE >= {'DATEADD(month, -1, GETDATE())' if is_azure else "date('now', '-1 months')"}
            AND NP_STTUS_CD IN ('OK', 'WD')
        """
        return basic_query, False, "🔧 기본 쿼리"


# 테스트 함수
def test_openai_sql_generator():
    """OpenAI SQL 생성기 테스트"""
    print("🧪 OpenAI SQL 생성기 테스트를 시작합니다...")

    try:
        from azure_config import get_azure_config

        azure_config = get_azure_config()

        if not azure_config.openai_api_key:
            print("❌ OpenAI API 키가 설정되지 않았습니다")
            return

        sql_generator = OpenAISQLGenerator(azure_config)

        test_queries = [
            "월별 포트인 현황을 알려줘",
            "SK텔레콤 포트아웃 정산 내역 보여줘",
            "최근 3개월 예치금 현황",
            "사업자별 번호이동 건수 비교",
            "전화번호 010-1234-5678의 포트아웃 정산 데이터 검증해줘",
        ]

        print("\n📋 테스트 결과:")
        for i, query in enumerate(test_queries, 1):
            print(f"\n{i}. 입력: '{query}'")

            sql, success, error = sql_generator.generate_sql_with_openai(
                query, is_azure=False
            )

            if success:
                print(f"   ✅ 성공")
                print(f"   📄 생성된 SQL:")
                print(f"   {sql[:200]}...")
            else:
                print(f"   ❌ 실패: {error}")

        print("\n✅ 테스트 완료!")

    except Exception as e:
        print(f"❌ 테스트 실패: {e}")


if __name__ == "__main__":
    test_openai_sql_generator()
