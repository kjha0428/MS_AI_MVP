# sql_generator.py - AI 기반 SQL 쿼리 생성기
import json
import re
import logging
from typing import Dict, List, Optional, Tuple
from azure_config import AzureConfig


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
                    "NP_TRMN_DATE >= date('now', '-3 months')",
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
                    "TRT_DATE >= date('now', '-3 months')",
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
                    "RMNY_DATE >= date('now', '-3 months')",
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
5. 개인정보 보호: 휴대전화번호는 SUBSTR(TEL_NO, 1, 3) || '****' || SUBSTR(TEL_NO, -4) 형태로 마스킹
6. 날짜 필터링 시 최근 3개월을 기본으로 설정
7. 집계 쿼리 시 적절한 GROUP BY와 ORDER BY 사용
8. 금액은 SUM, AVG 등 집계함수 사용 시 ROUND 적용
9. SQLite 문법 사용 (strftime, date 함수 등)

## 쿼리 패턴:
- 월별 집계: strftime('%Y-%m', date_column)
- 최근 N개월: date('now', '-N months')
- 금액 집계: SUM(SETL_AMT) 또는 SUM(DEPAZ_AMT)

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
                ai_sql = self._generate_ai_sql(user_input)
                if ai_sql and self._validate_sql(ai_sql):
                    self.logger.info("AI 기반 SQL 쿼리 생성 성공")
                    return ai_sql, True
                else:
                    self.logger.warning("AI 생성 쿼리 검증 실패, 규칙 기반으로 전환")

            # 2. 규칙 기반 쿼리 생성 (백업)
            rule_sql = self._generate_rule_based_sql(user_input)
            return rule_sql, False

        except Exception as e:
            self.logger.error(f"SQL 생성 실패: {e}")
            return self._get_default_query(), False

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

            response = self.openai_client.chat.completions.create(
                model="gpt-4",  # 실제 배포된 모델명으로 변경
                messages=messages,
                max_tokens=1000,
                temperature=0.1,
                top_p=0.9,
            )

            sql_query = response.choices[0].message.content.strip()

            # SQL 블록에서 쿼리 추출 (```sql ... ``` 형태)
            sql_match = re.search(r"```sql\s*(.*?)\s*```", sql_query, re.DOTALL)
            if sql_match:
                sql_query = sql_match.group(1).strip()

            return sql_query

        except Exception as e:
            self.logger.error(f"AI SQL 생성 실패: {e}")
            return None

    def _generate_rule_based_sql(self, user_input: str) -> str:
        """규칙 기반 SQL 쿼리 생성 (고도화 버전)"""
        user_input_lower = user_input.lower()

        # 통신사 필터 추출
        operator_filter = self._extract_operator_filter(user_input_lower)

        # 기간 필터 추출
        date_filter = self._extract_date_filter(user_input_lower)

        # 쿼리 타입 결정 및 생성
        if self._is_monthly_trend_query(user_input_lower):
            return self._generate_monthly_trend_query(
                user_input_lower, operator_filter, date_filter
            )

        elif self._is_phone_search_query(user_input):
            return self._generate_phone_search_query(user_input)

        elif self._is_operator_comparison_query(user_input_lower):
            return self._generate_operator_comparison_query(
                operator_filter, date_filter
            )

        elif self._is_deposit_query(user_input_lower):
            return self._generate_deposit_query(operator_filter, date_filter)

        elif self._is_anomaly_detection_query(user_input_lower):
            return self._generate_anomaly_detection_query(date_filter)

        else:
            return self._generate_summary_query(date_filter)

    def _extract_operator_filter(self, user_input: str) -> str:
        """통신사 필터 추출"""
        for key, value in self.operator_mapping.items():
            if key in user_input and "포트인" in user_input:
                return f"AND BCHNG_COMM_CMPN_ID = (SELECT BCHNG_COMM_CMPN_ID FROM PY_NP_SBSC_RMNY_TXN WHERE BCHNG_COMM_CMPN_ID IN ('KT', 'SKT', 'LGU+', 'KT MVNO', 'SKT MVNO', 'LGU+ MVNO') LIMIT 1)"
            elif key in user_input and "포트아웃" in user_input:
                return f"AND ACHNG_COMM_CMPN_ID = (SELECT ACHNG_COMM_CMPN_ID FROM PY_NP_TRMN_RMNY_TXN WHERE ACHNG_COMM_CMPN_ID IN ('KT', 'SKT', 'LGU+', 'KT MVNO', 'SKT MVNO', 'LGU+ MVNO') LIMIT 1)"
        return ""

    def _extract_date_filter(self, user_input: str) -> str:
        """기간 필터 추출"""
        date_patterns = {
            "오늘": "date('now')",
            "어제": "date('now', '-1 day')",
            "이번주": "date('now', 'weekday 0', '-6 days')",
            "지난주": "date('now', 'weekday 0', '-13 days')",
            "이번달": "date('now', 'start of month')",
            "지난달": "date('now', 'start of month', '-1 month')",
            "최근 1주일": "date('now', '-7 days')",
            "최근 7일": "date('now', '-7 days')",
            "최근 1개월": "date('now', '-1 month')",
            "최근 30일": "date('now', '-30 days')",
            "최근 3개월": "date('now', '-3 months')",
            "최근 6개월": "date('now', '-6 months')",
            "최근 1년": "date('now', '-1 year')",
        }

        for period, sql_date in date_patterns.items():
            if period in user_input:
                return sql_date

        # 숫자 + 개월/월 패턴 검색
        month_match = re.search(r"(\d+)개?월", user_input)
        if month_match:
            months = int(month_match.group(1))
            return f"date('now', '-{months} months')"

        return "date('now', '-3 months')"  # 기본값

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
        """월별 추이 분석 쿼리 생성"""

        if "포트인" in user_input or "가입" in user_input:
            return f"""
            SELECT 
                strftime('%Y-%m', TRT_DATE) as month,
                BCHNG_COMM_CMPN_ID as operator_code,
                COUNT(*) as transaction_count,
                SUM(SETL_AMT) as total_amount,
                ROUND(AVG(SETL_AMT), 0) as avg_amount,
                MIN(SETL_AMT) as min_amount,
                MAX(SETL_AMT) as max_amount
            FROM PY_NP_SBSC_RMNY_TXN 
            WHERE TRT_DATE >= {date_filter} 
                AND NP_STTUS_CD IN ('OK', 'WD')
                {operator_filter}
            GROUP BY strftime('%Y-%m', TRT_DATE), BCHNG_COMM_CMPN_ID
            ORDER BY month DESC, total_amount DESC
            """

        elif "포트아웃" in user_input or "해지" in user_input:
            return f"""
            SELECT 
                strftime('%Y-%m', NP_TRMN_DATE) as month,
                BCHNG_COMM_CMPN_ID as operator_code,
                COUNT(*) as transaction_count,
                SUM(PAY_AMT) as total_amount,
                ROUND(AVG(PAY_AMT), 0) as avg_amount,
                MIN(PAY_AMT) as min_amount,
                MAX(PAY_AMT) as max_amount
            FROM PY_NP_TRMN_RMNY_TXN 
            WHERE NP_TRMN_DATE >= {date_filter} 
                AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
                {operator_filter}
            GROUP BY strftime('%Y-%m', NP_TRMN_DATE), BCHNG_COMM_CMPN_ID
            ORDER BY month DESC, total_amount DESC
            """

        else:
            # 포트인/포트아웃 통합 월별 분석
            return f"""
            WITH monthly_data AS (
                SELECT 
                    strftime('%Y-%m', TRT_DATE) as month,
                    'PORT_IN' as port_type,
                    BCHNG_COMM_CMPN_ID as operator_code,
                    COUNT(*) as transaction_count,
                    SUM(SETL_AMT) as total_amount,
                    ROUND(AVG(SETL_AMT), 0) as avg_amount
                FROM PY_NP_SBSC_RMNY_TXN 
                WHERE TRT_DATE >= {date_filter} 
                    AND NP_STTUS_CD IN ('OK', 'WD')
                    {operator_filter}
                GROUP BY strftime('%Y-%m', TRT_DATE), BCHNG_COMM_CMPN_ID
                
                UNION ALL
                
                SELECT 
                    strftime('%Y-%m', NP_TRMN_DATE) as month,
                    'PORT_OUT' as port_type,
                    BCHNG_COMM_CMPN_ID as operator_code,
                    COUNT(*) as transaction_count,
                    SUM(PAY_AMT) as total_amount,
                    ROUND(AVG(PAY_AMT), 0) as avg_amount
                FROM PY_NP_TRMN_RMNY_TXN 
                WHERE NP_TRMN_DATE >= {date_filter} 
                    AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
                    {operator_filter}
                GROUP BY strftime('%Y-%m', NP_TRMN_DATE), BCHNG_COMM_CMPN_ID
            )
            SELECT 
                month,
                port_type,
                operator_code,
                transaction_count,
                total_amount,
                avg_amount
            FROM monthly_data
            ORDER BY month DESC, operator_code, port_type
            """

    def _generate_phone_search_query(self, user_input: str) -> str:
        """전화번호 검색 쿼리 생성"""
        phone_match = re.search(r"010[- ]?\d{4}[- ]?\d{4}", user_input)
        if phone_match:
            phone = phone_match.group().replace("-", "").replace(" ", "")
            return f"""
            WITH phone_history AS (
                SELECT 
                    'PORT_IN' as port_type,
                    TRT_DATE as transaction_date,
                    SUBSTR(TEL_NO, 1, 3) || '****' || SUBSTR(TEL_NO, -4) as masked_phone,
                    SVC_CONT_ID,
                    SETL_AMT as settlement_amount,
                    BCHNG_COMM_CMPN_ID as operator_code,
                    NP_STTUS_CD as status,
                    '포트인: ' || BCHNG_COMM_CMPN_ID || '에서 ' || ACHNG_COMM_CMPN_ID || '로 이동' as description
                FROM PY_NP_SBSC_RMNY_TXN 
                WHERE TEL_NO = '{phone}' AND NP_STTUS_CD IN ('OK', 'WD')
                
                UNION ALL
                
                SELECT 
                    'PORT_OUT' as port_type,
                    NP_TRMN_DATE as transaction_date,
                    SUBSTR(TEL_NO, 1, 3) || '****' || SUBSTR(TEL_NO, -4) as masked_phone,
                    SVC_CONT_ID,
                    PAY_AMT as settlement_amount,
                    BCHNG_COMM_CMPN_ID as operator_code,
                    NP_TRMN_DTL_STTUS_VAL as status,
                    '포트아웃: ' || BCHNG_COMM_CMPN_ID || '에서 ' || ACHNG_COMM_CMPN_ID || '로 이동' as description
                FROM PY_NP_TRMN_RMNY_TXN 
                WHERE TEL_NO = '{phone}' AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
            )
            SELECT 
                port_type,
                transaction_date,
                masked_phone,
                SVC_CONT_ID as service_contract_id,
                settlement_amount,
                operator_code,
                status,
                description
            FROM phone_history
            ORDER BY transaction_date DESC
            """
        return self._get_default_query()

    def _generate_operator_comparison_query(
        self, operator_filter: str, date_filter: str
    ) -> str:
        """사업자별 현황 비교 쿼리 생성"""
        return f"""
        WITH operator_summary AS (
            SELECT 
                BCHNG_COMM_CMPN_ID as operator_code,
                'PORT_IN' as port_type,
                COUNT(*) as transaction_count,
                SUM(SETL_AMT) as total_amount,
                ROUND(AVG(SETL_AMT), 0) as avg_amount,
                MIN(SETL_AMT) as min_amount,
                MAX(SETL_AMT) as max_amount
            FROM PY_NP_SBSC_RMNY_TXN
            WHERE TRT_DATE >= {date_filter} 
                AND NP_STTUS_CD IN ('OK', 'WD')
                {operator_filter}
            GROUP BY BCHNG_COMM_CMPN_ID
            
            UNION ALL
            
            SELECT 
                BCHNG_COMM_CMPN_ID as operator_code,
                'PORT_OUT' as port_type,
                COUNT(*) as transaction_count,
                SUM(PAY_AMT) as total_amount,
                ROUND(AVG(PAY_AMT), 0) as avg_amount,
                MIN(PAY_AMT) as min_amount,
                MAX(PAY_AMT) as max_amount
            FROM PY_NP_TRMN_RMNY_TXN
            WHERE NP_TRMN_DATE >= {date_filter} 
                AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
                {operator_filter}
            GROUP BY BCHNG_COMM_CMPN_ID
        ),
        ranked_operators AS (
            SELECT 
                *,
                RANK() OVER (PARTITION BY port_type ORDER BY total_amount DESC) as amount_rank,
                RANK() OVER (PARTITION BY port_type ORDER BY transaction_count DESC) as count_rank
            FROM operator_summary
        )
        SELECT 
            operator_code,
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
                ELSE CAST(amount_rank AS TEXT) || '위'
            END as ranking_display
        FROM ranked_operators
        ORDER BY port_type, amount_rank
        """

    def _generate_deposit_query(self, operator_filter: str, date_filter: str) -> str:
        """예치금 현황 쿼리 생성"""
        return f"""
        SELECT 
            BILL_ACC_ID as account_id,
            COUNT(*) as deposit_count,
            SUM(DEPAZ_AMT) as total_deposit,
            ROUND(AVG(DEPAZ_AMT), 0) as avg_deposit,
            MIN(DEPAZ_AMT) as min_deposit,
            MAX(DEPAZ_AMT) as max_deposit,
            strftime('%Y-%m', RMNY_DATE) as deposit_month,
            DEPAZ_DIV_CD as deposit_type,
            RMNY_METH_CD as payment_method
        FROM PY_DEPAZ_BAS
        WHERE RMNY_DATE >= {date_filter}
            AND RMNY_METH_CD = 'NA'
            AND DEPAZ_DIV_CD = '10'
        GROUP BY BILL_ACC_ID, strftime('%Y-%m', RMNY_DATE), DEPAZ_DIV_CD, RMNY_METH_CD
        ORDER BY deposit_month DESC, total_deposit DESC
        """

    def _generate_anomaly_detection_query(self, date_filter: str) -> str:
        """이상 징후 탐지 쿼리 생성"""
        return f"""
        WITH monthly_stats AS (
            SELECT 
                strftime('%Y-%m', TRT_DATE) as month,
                BCHNG_COMM_CMPN_ID as operator_code,
                'PORT_IN' as port_type,
                COUNT(*) as monthly_count,
                SUM(SETL_AMT) as monthly_amount
            FROM PY_NP_SBSC_RMNY_TXN 
            WHERE TRT_DATE >= {date_filter} AND NP_STTUS_CD IN ('OK', 'WD')
            GROUP BY strftime('%Y-%m', TRT_DATE), BCHNG_COMM_CMPN_ID
            
            UNION ALL
            
            SELECT 
                strftime('%Y-%m', NP_TRMN_DATE) as month,
                BCHNG_COMM_CMPN_ID as operator_code,
                'PORT_OUT' as port_type,
                COUNT(*) as monthly_count,
                SUM(PAY_AMT) as monthly_amount
            FROM PY_NP_TRMN_RMNY_TXN 
            WHERE NP_TRMN_DATE >= {date_filter} AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
            GROUP BY strftime('%Y-%m', NP_TRMN_DATE), BCHNG_COMM_CMPN_ID
        ),
        growth_analysis AS (
            SELECT 
                month,
                operator_code,
                port_type,
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
            'PORT_IN' as port_type,
            COUNT(*) as transaction_count,
            SUM(SETL_AMT) as total_amount,
            ROUND(AVG(SETL_AMT), 0) as avg_amount
        FROM PY_NP_SBSC_RMNY_TXN
        WHERE TRT_DATE >= date('now', '-1 months') AND NP_STTUS_CD IN ('OK', 'WD')
        UNION ALL
        SELECT 
            'PORT_OUT' as port_type,
            COUNT(*) as transaction_count,
            SUM(PAY_AMT) as total_amount,
            ROUND(AVG(PAY_AMT), 0) as avg_amount
        FROM PY_NP_TRMN_RMNY_TXN
        WHERE NP_TRMN_DATE >= date('now', '-1 months') AND NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
        """

    def _validate_sql(self, sql_query: str) -> bool:
        """생성된 SQL 쿼리 검증"""
        try:
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
            if "COMM_CMPN_NM" in sql_query:
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
    from azure_config import get_azure_config

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
