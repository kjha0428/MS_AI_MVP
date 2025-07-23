# sql_generator.py - AI 기반 SQL 쿼리 생성기
import json
import re
import logging
from typing import Dict, List, Optional, Tuple
from azure_config import AzureConfig
from azure_config import get_azure_config


class SQLGenerator:
    """자연어를 SQL로 변환하는 AI 기반 쿼리 생성기"""

    def __init__(self, azure_config: AzureConfig):
        """SQL 생성기 초기화"""
        self.azure_config = azure_config
        self.openai_client = azure_config.get_openai_client()
        self.logger = logging.getLogger(__name__)

        # 데이터베이스 스키마 정보
        self.db_schema = self._load_schema()

        # 통신사 매핑 (sample_data.py의 operators와 일치)
        self.operator_mapping = {
            "KT": "KT",
            "SKT": "SKT",
            "SK텔레콤": "SKT",
            "SK": "SKT",
            "LGU+": "LGU+",
            "LG유플러스": "LGU+",
            "LG": "LGU+",
            "유플러스": "LGU+",
            "KT MVNO": "KT MVNO",
            "SKT MVNO": "SKT MVNO",
            "LGU+ MVNO": "LGU+ MVNO",
        }

    def _load_schema(self) -> Dict:
        """데이터베이스 스키마 정보 로드"""
        return {
            "PY_NP_TRMN_RMNY_TXN": {
                "description": "해지번호이동 정산 테이블 (포트아웃)",
                "alias": "trmn",
                "columns": {
                    "NP_DIV_CD": "번호이동구분코드 (VARCHAR(3)) - 'OUT'",
                    "TRMN_NP_ADM_NO": "해지번호이동관리번호 (VARCHAR(11), PK)",
                    "NP_TRMN_DATE": "번호이동해지일자 (DATE)",
                    "CNCL_WTHD_DATE": "취소철회일자 (DATE)",
                    "BCHNG_COMM_CMPN_ID": "변경전통신회사아이디 (VARCHAR(11))",
                    "ACHNG_COMM_CMPN_ID": "변경후통신회사아이디 (VARCHAR(11))",
                    "SVC_CONT_ID": "서비스계약아이디 (VARCHAR(20))",
                    "BILL_ACC_ID": "청구계정아이디 (VARCHAR(11))",
                    "TEL_NO": "전화번호 (VARCHAR(20))",
                    "NP_TRMN_DTL_STTUS_VAL": "번호이동해지상세상태값 (VARCHAR(3)) - '1':해지완료, '2':취소, '3':철회",
                    "PAY_AMT": "지급금액 (NUMBER(18,3))",
                },
                "common_filters": [
                    "NP_TRMN_DATE >= DATEADD(month, -3, GETDATE())",
                    "NP_TRMN_DTL_STTUS_VAL IN ('1', '3')",
                ],
            },
            "PY_NP_SBSC_RMNY_TXN": {
                "description": "가입번호이동 정산 테이블 (포트인)",
                "alias": "sbsc",
                "columns": {
                    "NP_DIV_CD": "번호이동구분코드 (VARCHAR(3)) - 'IN'",
                    "NP_SBSC_RMNY_SEQ": "번호이동가입수납일련번호 (INTEGER, PK)",
                    "TRT_DATE": "처리일자 (DATE)",
                    "CNCL_DATE": "취소일자 (DATE)",
                    "BCHNG_COMM_CMPN_ID": "변경전통신회사아이디 (VARCHAR(11))",
                    "ACHNG_COMM_CMPN_ID": "변경후통신회사아이디 (VARCHAR(11))",
                    "SVC_CONT_ID": "서비스계약아이디 (VARCHAR(20))",
                    "BILL_ACC_ID": "청구계정아이디 (VARCHAR(11))",
                    "TEL_NO": "전화번호 (VARCHAR(20))",
                    "NP_STTUS_CD": "번호이동상태코드 (VARCHAR(3)) - 'OK':완료, 'CN':취소, 'WD':철회",
                    "SETL_AMT": "정산금액 (DECIMAL(15,2))",
                },
                "common_filters": [
                    "TRT_DATE >= DATEADD(month, -3, GETDATE())",
                    "NP_STTUS_CD IN ('OK', 'WD')",
                ],
            },
            "PY_DEPAZ_BAS": {
                "description": "예치금 기본 테이블",
                "alias": "depaz",
                "columns": {
                    "DEPAZ_SEQ": "예치금일련번호 (INTEGER, PK)",
                    "SVC_CONT_ID": "서비스계약아이디 (VARCHAR(20))",
                    "BILL_ACC_ID": "청구계정아이디 (VARCHAR(11))",
                    "DEPAZ_DIV_CD": "예치금구분코드 (VARCHAR(3)) - '10':입금, '90':취소",
                    "RMNY_DATE": "수납일자 (DATE)",
                    "RMNY_METH_CD": "수납방법코드 (VARCHAR(5)) - 'NA':번호이동미청구금, 'CA':현금",
                    "DEPAZ_AMT": "예치금액 (DECIMAL(15,2))",
                },
                "common_filters": [
                    "RMNY_DATE >= DATEADD(month, -3, GETDATE())",
                    "RMNY_METH_CD = 'NA'",
                    "DEPAZ_DIV_CD = '10'",
                ],
            },
        }

    def _create_system_prompt(self) -> str:
        """AI용 시스템 프롬프트 생성"""
        schema_text = json.dumps(self.db_schema, ensure_ascii=False, indent=2)

        return f"""
        당신은 번호이동정산 데이터베이스를 위한 SQL 쿼리 생성 전문가입니다.

        ## 데이터베이스 스키마:
        {schema_text}

        ## 중요한 규칙:
        1. PY_NP_TRMN_RMNY_TXN은 포트아웃(해지) 데이터 - 일자: NP_TRMN_DATE
        2. PY_NP_SBSC_RMNY_TXN은 포트인(가입) 데이터 - 일자: TRT_DATE  
        3. PY_DEPAZ_BAS는 예치금 데이터 - 일자: RMNY_DATE
        4. 전화번호는 PY_NP_SBSC_RMNY_TXN.TEL_NO 또는 PY_NP_TRMN_RMNY_TXN.TEL_NO에서 조회
        5. PY_DEPAZ_BAS 테이블에는 TEL_NO가 없어서 PY_NP_TRMN_RMNY_TXN 테이블에서 SVC_CONT_ID, RMNY_DATE와 조인
        6. 개인정보 보호: 휴대전화번호는 LEFT(TEL_NO, 3) || '****' || RIGHT(TEL_NO, 4) 형태로 마스킹
        7. 날짜 필터링 시 최근 3개월을 기본으로 설정
        8. 집계 쿼리 시 적절한 GROUP BY와 ORDER BY 사용
        9. 금액은 SUM, AVG 등 집계함수 사용 시 ROUND 적용

        ## 쿼리 패턴:
        - 월별 집계: FORMAT(date_column, 'yyyy-MM')
        - 최근 N개월: DATEADD(month, -N, GETDATE())
        - 금액 집계: SUM(PAY_AMT) 또는 SUM(SETL_AMT) 또는 SUM(DEPAZ_AMT)

        ## 응답 형식:
        유효한 SQL 쿼리만 반환하세요. 설명이나 다른 텍스트는 포함하지 마세요.
        """

    def generate_sql(self, user_input: str) -> Tuple[str, bool]:
        """
        자연어 입력을 SQL 쿼리로 변환

        Returns:
            Tuple[str, bool]: (SQL 쿼리, AI 사용 여부)
        """
        try:
            # 1. AI 기반 쿼리 생성 시도
            if self.openai_client:
                try:
                    ai_sql = self._generate_ai_sql(user_input)
                    if ai_sql and self._validate_sql(ai_sql):
                        self.logger.info("AI 기반 SQL 쿼리 생성 성공")
                        return ai_sql, True
                    else:
                        self.logger.warning("AI 생성 쿼리 검증 실패, 규칙 기반으로 전환")
                except Exception as ai_error:
                    self.logger.error(f"AI SQL 생성 중 오류: {ai_error}")

            # 2. 규칙 기반 쿼리 생성 (백업)
            try:
                rule_sql = self._generate_rule_based_sql(user_input)
                if rule_sql and self._validate_sql(rule_sql):
                    self.logger.info("규칙 기반 SQL 쿼리 생성 성공")
                    return rule_sql, False
                else:
                    self.logger.warning("규칙 기반 쿼리 검증 실패, 기본 쿼리 사용")
            except Exception as rule_error:
                self.logger.error(f"규칙 기반 SQL 생성 중 오류: {rule_error}")

            # 3. 최종 백업: 기본 쿼리
            default_query = self._get_default_query()
            return default_query, False

        except Exception as e:
            self.logger.error(f"전체 SQL 생성 실패: {e}")
            # 🔥 수정: 예외 발생 시에도 항상 튜플 반환
            error_query = """
            SELECT 
                'SQL 쿼리 생성 중 오류가 발생했습니다' as 오류메시지,
                '다시 시도해주세요' as 안내
            """
            return error_query, False

    def _generate_ai_sql(self, user_input: str) -> Optional[str]:
        """AI를 사용한 SQL 쿼리 생성"""
        try:
            messages = [
                {"role": "system", "content": self._create_system_prompt()},
                {
                    "role": "user",
                    "content": f"다음 요청을 SQL 쿼리로 변환해주세요: {user_input}",
                },
            ]

            # 🔥 수정: 모델명을 azure_config에서 가져오되, 기본값 설정
            model_name = self.azure_config.openai_model_name or "gpt-4o"

            # 🔥 수정: 배포된 모델명 확인 및 예외 처리
            try:
                response = self.openai_client.chat.completions.create(
                    model=model_name,  # 🔥 변경: 하드코딩된 "gpt-4" 대신 설정값 사용
                    messages=messages,
                    max_tokens=1000,
                    temperature=0.1,
                    top_p=0.9,
                )
            except Exception as api_error:
                # 🔥 추가: 404 오류 특별 처리
                if "DeploymentNotFound" in str(api_error) or "404" in str(api_error):
                    self.logger.warning(
                        f"배포된 모델 '{model_name}'을 찾을 수 없습니다. 대체 모델을 시도합니다."
                    )
                else:
                    # 다른 API 오류는 그대로 전파
                    raise api_error

            sql_query = response.choices[0].message.content.strip()

            # SQL 블록에서 쿼리 추출 (```sql ... ``` 형태)
            sql_query = self._extract_sql_from_response(sql_query)

            return sql_query

        except Exception as e:
            self.logger.error(f"AI SQL 생성 실패: {e}")
            return None

    def _extract_operator_filter(self, user_input: str) -> str:
        """통신사 필터 추출"""
        for key, value in self.operator_mapping.items():
            if key.lower() in user_input.lower():
                return f"AND (BCHNG_COMM_CMPN_ID = '{value}' OR ACHNG_COMM_CMPN_ID = '{value}')"
        return ""

    def _generate_rule_based_sql(self, user_input: str) -> str:
        """규칙 기반 SQL 쿼리 생성"""
        user_input_lower = user_input.lower()

        # 기간 필터 추출
        date_filter = self._extract_date_filter(user_input_lower)

        # 1. 월별 집계 쿼리
        if "월별" in user_input_lower or "추이" in user_input_lower:
            if "포트인" in user_input_lower:
                return f"""
                SELECT 
                    FORMAT(TRT_DATE, 'yyyy-MM') as 월,
                    BCHNG_COMM_CMPN_ID as 전사업자,
                    COUNT(*) as 총건수,
                    SUM(SETL_AMT) as 총금액,
                    ROUND(AVG(CAST(SETL_AMT AS FLOAT)), 0) as 평균금액
                FROM PY_NP_SBSC_RMNY_TXN 
                WHERE TRT_DATE >= {date_filter}
                    AND NP_STTUS_CD IN ('OK', 'WD')
                GROUP BY FORMAT(TRT_DATE, 'yyyy-MM'), BCHNG_COMM_CMPN_ID
                ORDER BY 월 DESC, 총금액 DESC
                """
            elif "포트아웃" in user_input_lower:
                return f"""
                SELECT 
                    FORMAT(NP_TRMN_DATE, 'yyyy-MM') as 월,
                    ACHNG_COMM_CMPN_ID as 전사업자,
                    COUNT(*) as 총건수,
                    SUM(PAY_AMT) as 총금액,
                    ROUND(AVG(CAST(PAY_AMT AS FLOAT)), 0) as 평균금액
                FROM PY_NP_TRMN_RMNY_TXN 
                WHERE NP_TRMN_DATE >= {date_filter}
                    AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
                GROUP BY FORMAT(NP_TRMN_DATE, 'yyyy-MM'), ACHNG_COMM_CMPN_ID
                ORDER BY 월 DESC, 총금액 DESC
                """

        # 2. 전화번호 검색
        phone_match = re.search(r"010[- ]?\d{4}[- ]?\d{4}", user_input)
        if phone_match:
            phone = phone_match.group().replace("-", "").replace(" ", "")
            return f"""
            SELECT 
                'PORT_IN' as 번호이동타입,
                TRT_DATE as 번호이동일,
                LEFT(TEL_NO, 3) + '****' + RIGHT(TEL_NO, 4) as 전화번호,
                SETL_AMT as 정산금액,
                BCHNG_COMM_CMPN_ID as 사업자,
                NP_STTUS_CD as 상태
            FROM PY_NP_SBSC_RMNY_TXN 
            WHERE TEL_NO = '{phone}' AND NP_STTUS_CD IN ('OK', 'WD')
            UNION ALL
            SELECT 
                'PORT_OUT' as 번호이동타입,
                NP_TRMN_DATE as 번호이동일,
                LEFT(TEL_NO, 3) + '****' + RIGHT(TEL_NO, 4) as 전화번호,
                PAY_AMT as 정산금액,
                ACHNG_COMM_CMPN_ID as 사업자,
                NP_TRMN_DTL_STTUS_VAL as 상태
            FROM PY_NP_TRMN_RMNY_TXN 
            WHERE TEL_NO = '{phone}' AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
            ORDER BY 번호이동일 DESC
            """

        # 3. 사업자별 현황
        if any(keyword in user_input_lower for keyword in ["사업자", "회사", "통신사", "현황"]):
            return f"""
            SELECT 
                BCHNG_COMM_CMPN_ID as 사업자,
                'PORT_IN' as 타입,
                COUNT(*) as 건수,
                SUM(SETL_AMT) as 총금액,
                ROUND(AVG(CAST(SETL_AMT AS FLOAT)), 0) as 평균금액
            FROM PY_NP_SBSC_RMNY_TXN
            WHERE TRT_DATE >= {date_filter}
                AND NP_STTUS_CD IN ('OK', 'WD')
            GROUP BY BCHNG_COMM_CMPN_ID
            UNION ALL
            SELECT 
                ACHNG_COMM_CMPN_ID as 사업자,
                'PORT_OUT' as 타입,
                COUNT(*) as 건수,
                SUM(PAY_AMT) as 총금액,
                ROUND(AVG(CAST(PAY_AMT AS FLOAT)), 0) as 평균금액
            FROM PY_NP_TRMN_RMNY_TXN
            WHERE NP_TRMN_DATE >= {date_filter}
                AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
            GROUP BY ACHNG_COMM_CMPN_ID
            ORDER BY 사업자, 타입
            """

        # 4. 기본 쿼리 반환
        return self._get_default_query()

    def perator_filter(self, user_input: str) -> str:
        """통신사 필터 추출"""
        for key, value in self.operator_mapping.items():
            if key in user_input and "포트인" in user_input:
                return f"AND BCHNG_COMM_CMPN_ID = (SELECT BCHNG_COMM_CMPN_ID FROM PY_NP_SBSC_RMNY_TXN WHERE BCHNG_COMM_CMPN_ID IN ('KT', 'SKT', 'LGU+', 'KT MVNO', 'SKT MVNO', 'LGU+ MVNO') LIMIT 1)"
            elif key in user_input and "포트아웃" in user_input:
                return f"AND ACHNG_COMM_CMPN_ID = (SELECT ACHNG_COMM_CMPN_ID FROM PY_NP_TRMN_RMNY_TXN WHERE ACHNG_COMM_CMPN_ID IN ('KT', 'SKT', 'LGU+', 'KT MVNO', 'SKT MVNO', 'LGU+ MVNO') LIMIT 1)"
        return ""

    def _extract_date_filter(self, user_input: str) -> str:
        """기간 필터 추출"""
        import re

        if "최근 1개월" in user_input or "최근 한달" in user_input:
            # return "date('now', '-1 month')"
            return "DATEADD(month, -1, GETDATE())"
        elif "최근 3개월" in user_input:
            # return "date('now', '-3 months')"
            return "DATEADD(month, -3, GETDATE())"
        elif "최근 6개월" in user_input:
            # return "date('now', '-6 months')"
            return "DATEADD(month, -6, GETDATE())"
        elif "최근 1년" in user_input:
            # return "date('now', '-1 year')"
            return "DATEADD(year, -1, GETDATE())"

        # 숫자 + 개월 패턴 검색
        month_match = re.search(r"(\d+)개?월", user_input)
        if month_match:
            months = int(month_match.group(1))
            # return f"date('now', '-{months} months')"
            return f"DATEADD(year, -{months}, GETDATE())"

        # 기본값: 최근 3개월
        # return "date('now', '-3 months')"
        return "DATEADD(month, -3, GETDATE())"

    def _is_monthly_trend_query(self, user_input: str) -> bool:
        """월별 추이 쿼리 여부 판단"""
        keywords = ["월별", "추이", "트렌드", "변화", "패턴", "증감"]
        return any(keyword in user_input for keyword in keywords)

    def _is_phone_search_query(self, user_input: str) -> bool:
        """전화번호 검색 쿼리 여부 판단"""
        phone_pattern = r"010[- ]?\d{4}[- ]?\d{4}"
        return bool(re.search(phone_pattern, user_input))

    def _is_operator_comparison_query(self, user_input: str) -> bool:
        """사업자 비교 쿼리 여부 판단"""
        keywords = ["사업자", "회사", "통신사", "비교", "현황", "순위"]
        return any(keyword in user_input for keyword in keywords)

    def _is_deposit_query(self, user_input: str) -> bool:
        """예치금 쿼리 여부 판단"""
        keywords = ["예치금", "보증금", "입금"]
        return any(keyword in user_input for keyword in keywords)

    def _is_anomaly_detection_query(self, user_input: str) -> bool:
        """이상 징후 탐지 쿼리 여부 판단"""
        keywords = ["이상", "급증", "급감", "변화", "증가", "감소", "이상치"]
        return any(keyword in user_input for keyword in keywords)

    def _generate_monthly_trend_query(
        self, user_input: str, operator_filter: str, date_filter: str
    ) -> str:
        """월별 추이 분석 쿼리 생성 - Azure SQL 문법"""

        # date_filter를 Azure SQL 형식으로 변환
        azure_date_filter = self._convert_to_azure_date_filter(date_filter)

        if "포트인" in user_input or "가입" in user_input:
            return f"""
            SELECT 
                FORMAT(TRT_DATE, 'yyyy-MM') as month,
                BCHNG_COMM_CMPN_ID as operator_name,
                COUNT(*) as transaction_count,
                SUM(SETL_AMT) as total_amount,
                ROUND(AVG(CAST(SETL_AMT AS FLOAT)), 0) as avg_amount,
                MIN(SETL_AMT) as min_amount,
                MAX(SETL_AMT) as max_amount
            FROM PY_NP_SBSC_RMNY_TXN 
            WHERE TRT_DATE >= {azure_date_filter}
                AND TRT_STUS_CD IN ('OK', 'WD')
                {operator_filter}
            GROUP BY FORMAT(TRT_DATE, 'yyyy-MM'), BCHNG_COMM_CMPN_ID
            ORDER BY month DESC, total_amount DESC
            """

        elif "포트아웃" in user_input or "해지" in user_input:
            return f"""
            SELECT 
                FORMAT(SETL_TRT_DATE, 'yyyy-MM') as month,
                ACHNG_COMM_CMPN_ID as operator_name,
                COUNT(*) as transaction_count,
                SUM(PAY_AMT) as total_amount,
                ROUND(AVG(CAST(PAY_AMT AS FLOAT)), 0) as avg_amount,
                MIN(PAY_AMT) as min_amount,
                MAX(PAY_AMT) as max_amount
            FROM PY_NP_TRMN_RMNY_TXN 
            WHERE SETL_TRT_DATE >= {azure_date_filter}
                AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
                {operator_filter}
            GROUP BY FORMAT(SETL_TRT_DATE, 'yyyy-MM'), ACHNG_COMM_CMPN_ID
            ORDER BY month DESC, total_amount DESC
            """

        else:
            # 포트인/포트아웃 통합 월별 분석
            return f"""
            WITH monthly_data AS (
                SELECT 
                    FORMAT(TRT_DATE, 'yyyy-MM') as month,
                    'PORT_IN' as port_type,
                    BCHNG_COMM_CMPN_ID as operator_name,
                    COUNT(*) as transaction_count,
                    SUM(SETL_AMT) as total_amount,
                    ROUND(AVG(CAST(SETL_AMT AS FLOAT)), 0) as avg_amount
                FROM PY_NP_SBSC_RMNY_TXN 
                WHERE TRT_DATE >= {azure_date_filter}
                    AND TRT_STUS_CD IN ('OK', 'WD')
                    {operator_filter}
                GROUP BY FORMAT(TRT_DATE, 'yyyy-MM'), BCHNG_COMM_CMPN_ID
                UNION ALL
                SELECT 
                    FORMAT(SETL_TRT_DATE, 'yyyy-MM') as month,
                    'PORT_OUT' as port_type,
                    ACHNG_COMM_CMPN_ID as operator_name,
                    COUNT(*) as transaction_count,
                    SUM(PAY_AMT) as total_amount,
                    ROUND(AVG(CAST(PAY_AMT AS FLOAT)), 0) as avg_amount
                FROM PY_NP_TRMN_RMNY_TXN 
                WHERE SETL_TRT_DATE >= {azure_date_filter}
                    AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
                    {operator_filter}
                GROUP BY FORMAT(SETL_TRT_DATE, 'yyyy-MM'), ACHNG_COMM_CMPN_ID
            )
            SELECT 
                month,
                port_type,
                operator_name,
                transaction_count,
                total_amount,
                avg_amount
            FROM monthly_data
            ORDER BY month DESC, operator_name, port_type
            """

    def _convert_to_azure_date_filter(self, date_filter: str) -> str:
        """날짜 필터를 Azure SQL 형식으로 변환"""
        date_mapping = {
            "date('now')": "CAST(GETDATE() AS DATE)",
            "date('now', '-1 day')": "DATEADD(day, -1, GETDATE())",
            "date('now', '-7 days')": "DATEADD(day, -7, GETDATE())",
            "date('now', '-1 month')": "DATEADD(month, -1, GETDATE())",
            "date('now', '-3 months')": "DATEADD(month, -3, GETDATE())",
            "date('now', '-6 months')": "DATEADD(month, -6, GETDATE())",
            "date('now', '-1 year')": "DATEADD(year, -1, GETDATE())",
        }

        return date_mapping.get(date_filter, "DATEADD(month, -3, GETDATE())")

    def _generate_phone_search_query(self, user_input: str) -> str:
        """전화번호 검색 쿼리 생성 - Azure SQL 문법"""
        phone_match = re.search(r"010[- ]?\d{4}[- ]?\d{4}", user_input)
        if phone_match:
            phone = phone_match.group().replace("-", "").replace(" ", "")
            return f"""
            WITH phone_history AS (
                SELECT 
                    'PORT_IN' as port_type,
                    TRT_DATE as transaction_date,
                    SUBSTRING(TEL_NO, 1, 3) + '****' + RIGHT(TEL_NO, 4) as masked_phone,
                    SVC_CONT_ID,
                    SETL_AMT as settlement_amount,
                    BCHNG_COMM_CMPN_ID as operator_name,
                    TRT_STUS_CD as status,
                    '포트인: ' + BCHNG_COMM_CMPN_ID + '로 이동' as description
                FROM PY_NP_SBSC_RMNY_TXN 
                WHERE TEL_NO = '{phone}' AND TRT_STUS_CD IN ('OK', 'WD')
                
                UNION ALL
                
                SELECT 
                    'PORT_OUT' as port_type,
                    SETL_TRT_DATE as transaction_date,
                    SUBSTRING(TEL_NO, 1, 3) + '****' + RIGHT(TEL_NO, 4) as masked_phone,
                    SVC_CONT_ID,
                    PAY_AMT as settlement_amount,
                    ACHNG_COMM_CMPN_ID as operator_name,
                    NP_TRMN_DTL_STTUS_VAL as status,
                    '포트아웃: ' + ACHNG_COMM_CMPN_ID + '에서 이동' as description
                FROM PY_NP_TRMN_RMNY_TXN 
                WHERE TEL_NO = '{phone}' AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
            )
            SELECT 
                port_type,
                transaction_date,
                masked_phone,
                SVC_CONT_ID as service_contract_id,
                settlement_amount,
                operator_name,
                status,
                description
            FROM phone_history
            ORDER BY transaction_date DESC
            """
        return self._get_default_query()

    def _generate_operator_comparison_query(
        self, operator_filter: str, date_filter: str
    ) -> str:
        """사업자별 현황 비교 쿼리 생성 - Azure SQL 문법"""
        azure_date_filter = self._convert_to_azure_date_filter(date_filter)

        return f"""
        WITH operator_summary AS (
            SELECT 
                'PORT_IN' as port_type,
                BCHNG_COMM_CMPN_ID as operator_name
                COUNT(*) as transaction_count,
                SUM(SETL_AMT) as total_amount,
                ROUND(AVG(CAST(SETL_AMT AS FLOAT)), 0) as avg_amount,
                MIN(SETL_AMT) as min_amount,
                MAX(SETL_AMT) as max_amount
            FROM PY_NP_SBSC_RMNY_TXN
            WHERE TRT_DATE >= {azure_date_filter}
                AND TRT_STUS_CD IN ('OK', 'WD')
                {operator_filter}
            GROUP BY BCHNG_COMM_CMPN_ID
            UNION ALL
            SELECT 
                'PORT_OUT' as port_type,
                ACHNG_COMM_CMPN_ID as operator_name
                COUNT(*) as transaction_count,
                SUM(PAY_AMT) as total_amount,
                ROUND(AVG(CAST(PAY_AMT AS FLOAT)), 0) as avg_amount,
                MIN(PAY_AMT) as min_amount,
                MAX(PAY_AMT) as max_amount
            FROM PY_NP_TRMN_RMNY_TXN
            WHERE NP_TRMN_DATE >= {azure_date_filter}
                AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
                {operator_filter}
            GROUP BY ACHNG_COMM_CMPN_ID
        ),
        ranked_operators AS (
            SELECT 
                *,
                RANK() OVER (PARTITION BY port_type ORDER BY total_amount DESC) as amount_rank,
                RANK() OVER (PARTITION BY port_type ORDER BY transaction_count DESC) as count_rank
            FROM operator_summary
        )
        SELECT 
            operator_name,
            port_type,
            transaction_count,
            total_amount,
            avg_amount,
            min_amount,
            max_amount,
            amount_rank,
            count_rank,
            CASE 
                WHEN amount_rank = 1 THEN '🥇 1위'
                WHEN amount_rank = 2 THEN '🥈 2위' 
                WHEN amount_rank = 3 THEN '🥉 3위'
                ELSE CAST(amount_rank AS NVARCHAR(10)) + '위'
            END as ranking_display
        FROM ranked_operators
        ORDER BY port_type, amount_rank
        """

    def _generate_deposit_query(self, operator_filter: str, date_filter: str) -> str:
        """예치금 현황 쿼리 생성 - Azure SQL 문법"""
        azure_date_filter = self._convert_to_azure_date_filter(date_filter)

        return f"""
        SELECT 
            BILL_ACC_ID,
            COUNT(*) as deposit_count,
            SUM(DEPAZ_AMT) as total_deposit,
            ROUND(AVG(CAST(DEPAZ_AMT AS FLOAT)), 0) as avg_deposit,
            MIN(DEPAZ_AMT) as min_deposit,
            MAX(DEPAZ_AMT) as max_deposit,
            FORMAT(RMNY_DATE, 'yyyy-MM') as deposit_month,
            DEPAZ_DIV_CD as deposit_type,
            RMNY_METH_CD as payment_method
        FROM PY_DEPAZ_BAS
        WHERE RMNY_DATE >= {azure_date_filter}
            AND RMNY_METH_CD = 'NA'
            AND DEPAZ_DIV_CD = '10'
        GROUP BY BILL_ACC_ID, FORMAT(RMNY_DATE, 'yyyy-MM'), DEPAZ_DIV_CD, RMNY_METH_CD
        ORDER BY deposit_month DESC, total_deposit DESC
        """

    def _generate_anomaly_detection_query(self, date_filter: str) -> str:
        """이상 징후 탐지 쿼리 생성"""
        return f"""
        WITH monthly_stats AS (
            SELECT 
                'PORT_IN' as port_type,
                FORMAT(TRT_DATE, 'yyyy-MM') as month,
                BCHNG_COMM_CMPN_ID as operator_code,
                COUNT(*) as monthly_count,
                SUM(SETL_AMT) as monthly_amount
            FROM PY_NP_SBSC_RMNY_TXN 
            WHERE TRT_DATE >= {date_filter} AND NP_STTUS_CD IN ('OK', 'WD')
            GROUP BY strftime('%Y-%m', TRT_DATE), BCHNG_COMM_CMPN_ID
            UNION ALL
            SELECT 
                'PORT_OUT' as port_type,
                FORMAT(NP_TRMN_DATE, 'yyyy-MM') as month,
                ACHNG_COMM_CMPN_ID as operator_code,
                COUNT(*) as monthly_count,
                SUM(PAY_AMT) as monthly_amount
            FROM PY_NP_TRMN_RMNY_TXN 
            WHERE NP_TRMN_DATE >= {date_filter} AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
            GROUP BY strftime('%Y-%m', NP_TRMN_DATE), ACHNG_COMM_CMPN_ID
        ),
        growth_analysis AS (
            SELECT 
                port_type,
                month,
                operator_code,
                monthly_amount,
                LAG(monthly_amount) OVER (
                    PARTITION BY operator_code, port_type 
                    ORDER BY month
                ) as prev_month_amount,
                CASE 
                    WHEN LAG(monthly_amount) OVER (
                        PARTITION BY operator_code, port_type 
                        ORDER BY month
                    ) > 0 THEN
                        ROUND(
                            (monthly_amount - LAG(monthly_amount) OVER (
                                PARTITION BY operator_code, port_type 
                                ORDER BY month
                            )) * 100.0 / LAG(monthly_amount) OVER (
                                PARTITION BY operator_code, port_type 
                                ORDER BY month
                            ), 2
                        )
                    ELSE NULL
                END as growth_rate
            FROM monthly_stats
        )
        SELECT 
            month,
            operator_code,
            port_type,
            monthly_amount,
            prev_month_amount,
            growth_rate,
            CASE 
                WHEN ABS(growth_rate) >= 50 THEN '⚠️ 급격한 변화'
                WHEN ABS(growth_rate) >= 30 THEN '📈 큰 변화'
                WHEN ABS(growth_rate) >= 20 THEN '📊 변화 감지'
                ELSE '➡️ 정상 범위'
            END as alert_level
        FROM growth_analysis
        WHERE growth_rate IS NOT NULL
        ORDER BY ABS(growth_rate) DESC, month DESC
        """

    def _generate_summary_query(self, date_filter: str) -> str:
        """요약 현황 쿼리 생성"""
        return f"""
        WITH summary_stats AS (
            SELECT 
                'PORT_IN' as port_type,
                COUNT(*) as transaction_count,
                SUM(SETL_AMT) as total_amount,
                ROUND(AVG(SETL_AMT), 0) as avg_amount,
                MIN(SETL_AMT) as min_amount,
                MAX(SETL_AMT) as max_amount
            FROM PY_NP_SBSC_RMNY_TXN
            WHERE TRT_DATE >= {date_filter} AND NP_STTUS_CD IN ('OK', 'WD')
            UNION ALL
            SELECT 
                'PORT_OUT' as port_type,
                COUNT(*) as transaction_count,
                SUM(PAY_AMT) as total_amount,
                ROUND(AVG(PAY_AMT), 0) as avg_amount,
                MIN(PAY_AMT) as min_amount,
                MAX(PAY_AMT) as max_amount
            FROM PY_NP_TRMN_RMNY_TXN
            WHERE NP_TRMN_DATE >= {date_filter} AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
        )
        SELECT 
            port_type,
            transaction_count,
            total_amount,
            avg_amount,
            min_amount,
            max_amount,
            CASE 
                WHEN port_type = 'PORT_IN' THEN '📥 포트인 (가입)'
                ELSE '📤 포트아웃 (해지)'
            END as type_display
        FROM summary_stats
        ORDER BY total_amount DESC
        """

    def _get_default_query(self) -> str:
        """기본 쿼리 반환"""
        return """
        SELECT 
            'PORT_IN' as 번호이동타입,
            COUNT(*) as 거래건수,
            SUM(SETL_AMT) as 총금액,
            ROUND(AVG(SETL_AMT), 0) as 평균금액
        FROM PY_NP_SBSC_RMNY_TXN
        WHERE TRT_DATE >= DATEADD(month, -1, GETDATE())
            AND NP_STTUS_CD IN ('OK', 'WD')
        UNION ALL
        SELECT 
            'PORT_OUT' as 번호이동타입,
            COUNT(*) as 거래건수,
            SUM(PAY_AMT) as 총금액,
            ROUND(AVG(PAY_AMT), 0) as 평균금액
        FROM PY_NP_TRMN_RMNY_TXN
        WHERE NP_TRMN_DATE >= DATEADD(month, -1, GETDATE())
            AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
        """

    def _extract_sql_from_response(self, response: str) -> str:
        """응답에서 SQL 쿼리 추출"""
        import re

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

    def _validate_sql(self, sql_query: str) -> bool:
        """생성된 SQL 쿼리 검증"""
        try:
            if not sql_query or not sql_query.strip():
                self.logger.warning("빈 SQL 쿼리")
                return False

            sql_upper = sql_query.upper().strip()

            # 1. SELECT 문인지 확인
            if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
                self.logger.warning("SELECT 또는 WITH로 시작하지 않는 쿼리")
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
                "EXEC",
                "EXECUTE",
            ]
            for keyword in dangerous_keywords:
                if keyword in sql_upper:
                    self.logger.warning(f"위험한 키워드 발견: {keyword}")
                    return False

            # 3. 유효한 테이블명 확인
            valid_tables = list(self.db_schema.keys())
            has_valid_table = any(table in sql_query for table in valid_tables)
            if not has_valid_table:
                self.logger.warning("유효한 테이블명이 없음")
                return False

            # 4. 기본적인 SQL 구조 확인
            required_keywords = ["SELECT", "FROM"]
            for keyword in required_keywords:
                if keyword not in sql_upper:
                    self.logger.warning(f"필수 키워드 누락: {keyword}")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"SQL 검증 중 오류: {e}")
            return False

    def get_query_explanation(self, sql_query: str) -> str:
        """생성된 SQL 쿼리에 대한 설명 생성"""
        explanations = []

        sql_upper = sql_query.upper()

        # 테이블 분석
        if "PY_NP_SBSC_RMNY_TXN" in sql_query:
            explanations.append("📥 포트인(가입) 데이터를 조회합니다")
        if "PY_NP_TRMN_RMNY_TXN" in sql_query:
            explanations.append("📤 포트아웃(해지) 데이터를 조회합니다")
        if "PY_DEPAZ_BAS" in sql_query:
            explanations.append("💰 예치금 데이터를 조회합니다")

        # 집계 함수 분석
        if "SUM(" in sql_upper:
            explanations.append("💰 금액 합계를 계산합니다")
        if "COUNT(" in sql_upper:
            explanations.append("📊 거래 건수를 계산합니다")
        if "AVG(" in sql_upper:
            explanations.append("📈 평균값을 계산합니다")

        # 그룹화 분석
        if "GROUP BY" in sql_upper:
            if "STRFTIME" in sql_upper:
                explanations.append("📅 월별로 그룹화하여 분석합니다")
            if "ACHNG_COMM_CMPN_ID" in sql_query or "BHNG_COMM_CMPN_ID" in sql_query:
                explanations.append("🏢 통신사별로 그룹화하여 분석합니다")

        # 정렬 분석
        if "ORDER BY" in sql_upper:
            explanations.append("📋 결과를 정렬하여 표시합니다")

        # 필터링 분석
        if "WHERE" in sql_upper:
            explanations.append("🔍 조건에 맞는 데이터만 필터링합니다")

        return (
            " | ".join(explanations) if explanations else "기본 데이터 조회 쿼리입니다"
        )


# 테스트 함수
def test_sql_generator():
    """SQL 생성기 테스트"""

    print("🧪 SQL 생성기 테스트를 시작합니다...")

    azure_config = get_azure_config()
    sql_generator = SQLGenerator(azure_config)

    test_queries = [
        "월별 포트인 현황을 알려줘",
        "SK텔레콤 포트아웃 정산 내역",
        "010-1234-5678 번호 조회",
        "사업자별 비교 현황",
        "최근 3개월 예치금 현황",
        "이상 징후 탐지",
    ]

    print("\n📋 테스트 결과:")
    for i, query in enumerate(test_queries, 1):
        print(f"\n{i}. 입력: {query}")
        try:
            sql, is_ai = sql_generator.generate_sql(query)
            validation = sql_generator._validate_sql(sql)
            explanation = sql_generator.get_query_explanation(sql)

            print(f"   AI 사용: {'✅' if is_ai else '❌'}")
            print(f"   검증 결과: {'✅ 통과' if validation else '❌ 실패'}")
            print(f"   설명: {explanation}")
            print(f"   SQL 미리보기: {sql[:100]}...")

        except Exception as e:
            print(f"   ❌ 오류: {e}")


if __name__ == "__main__":
    test_sql_generator()
