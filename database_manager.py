# database_manager.py - 데이터베이스 연결 및 쿼리 실행 관리
import pymssql
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd
import sqlite3
import logging
import time
from typing import Optional, Dict, Any, Tuple
from contextlib import contextmanager
from azure_config import AzureConfig
from sample_data import SampleDataManager
from sqlalchemy import text
from sample_data import create_sample_database


class DatabaseManager:
    """데이터베이스 연결 및 쿼리 실행 관리 클래스"""

    def __init__(self, azure_config: AzureConfig, use_sample_data: bool = False):
        """
        데이터베이스 매니저 초기화
        """
        self.azure_config = azure_config
        self.use_sample_data = use_sample_data
        self.logger = logging.getLogger(__name__)

        # 연결 설정
        self.connection_string = None
        self.sample_connection = None
        self.sqlalchemy_engine = None

        # 🔥 제거: sample_manager 속성 제거 (필요시 임시로만 생성)

        # 성능 설정
        self.max_execution_time = 30
        self.max_result_rows = 10000

        # 연결 타입 정보
        self.connection_type = (
            "Sample SQLite" if use_sample_data else "Azure SQL Database"
        )

        # 연결 초기화
        self._initialize_connection()

    def _initialize_connection(self):
        """데이터베이스 연결 초기화 - 타입에 따라 분기"""
        if self.use_sample_data:
            self.logger.info("🔧 샘플 데이터베이스 연결 초기화...")
            self._initialize_sample_connection()
        else:
            self.logger.info("☁️ Azure SQL Database 연결 초기화...")
            self._initialize_azure_connection()

    def _initialize_sample_connection(self):
        """샘플 데이터베이스 연결 초기화"""
        try:
            # sample_data 모듈 동적 임포트
            try:
                self.sample_connection = create_sample_database(
                    self.azure_config, force_local=False
                )
                self.logger.info("✅ 샘플 데이터베이스 연결 성공")
            except ImportError as e:
                self.logger.error(f"sample_data 모듈 임포트 실패: {e}")
                # 간단한 메모리 SQLite 생성
                self.sample_connection = sqlite3.connect(
                    ":memory:", check_same_thread=False
                )
                self._create_basic_sample_tables()
                self.logger.info("✅ 기본 SQLite 메모리 DB 생성")

        except Exception as e:
            self.logger.error(f"❌ 샘플 데이터베이스 연결 실패: {e}")
            raise e

    def _create_basic_sample_tables(self):
        """기본 샘플 테이블 생성 (sample_data 모듈 없을 때)"""
        try:
            cursor = self.sample_connection.cursor()

            # 기본 테이블 생성
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS PY_NP_SBSC_RMNY_TXN (
                    NP_SBSC_RMNY_SEQ INTEGER PRIMARY KEY,
                    TRT_DATE DATE,
                    SETL_AMT DECIMAL(15,2),
                    BCHNG_COMM_CMPN_ID VARCHAR(11),
                    ACHNG_COMM_CMPN_ID VARCHAR(11),
                    TEL_NO VARCHAR(20),
                    NP_STTUS_CD VARCHAR(3)
                )
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS PY_NP_TRMN_RMNY_TXN (
                    TRMN_NP_ADM_NO VARCHAR(11) PRIMARY KEY,
                    NP_TRMN_DATE DATE,
                    PAY_AMT DECIMAL(15,2),
                    BCHNG_COMM_CMPN_ID VARCHAR(11),
                    ACHNG_COMM_CMPN_ID VARCHAR(11),
                    TEL_NO VARCHAR(20),
                    NP_TRMN_DTL_STTUS_VAL VARCHAR(3)
                )
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS PY_DEPAZ_BAS (
                    DEPAZ_SEQ INTEGER PRIMARY KEY,
                    RMNY_DATE DATE,
                    DEPAZ_AMT DECIMAL(15,2),
                    DEPAZ_DIV_CD VARCHAR(3),
                    RMNY_METH_CD VARCHAR(5)
                )
            """
            )

            # 기본 샘플 데이터 삽입
            sample_data = [
                ("2024-01-15", 15000, "SKT", "KT", "01012345678", "OK"),
                ("2024-02-20", 25000, "KT", "LGU+", "01087654321", "OK"),
                ("2024-03-10", 18000, "LGU+", "SKT", "01055667788", "WD"),
            ]

            for data in sample_data:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO PY_NP_SBSC_RMNY_TXN 
                    (TRT_DATE, SETL_AMT, BCHNG_COMM_CMPN_ID, ACHNG_COMM_CMPN_ID, TEL_NO, NP_STTUS_CD)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    data,
                )

            self.sample_connection.commit()
            self.logger.info("📊 기본 샘플 데이터 생성 완료")

        except Exception as e:
            self.logger.error(f"기본 샘플 테이블 생성 실패: {e}")
            raise e

    def _initialize_azure_connection(self):
        """Azure SQL Database 연결 초기화"""
        try:
            # Azure 연결 문자열 가져오기
            self.connection_string = self.azure_config.get_database_connection_string()
            if not self.connection_string:
                raise ValueError("데이터베이스 연결 문자열을 가져올 수 없습니다")

            # SQLAlchemy 엔진 생성
            self._create_sqlalchemy_engine()

            # 연결 테스트
            if self.test_connection():
                self.logger.info("✅ Azure SQL Database 연결 성공")

                # 🔥 수정: SampleDataManager의 올바른 메서드 사용
                try:
                    from sample_data import SampleDataManager

                    self.logger.info("Azure 테이블 및 샘플 데이터 설정 중...")

                    # SampleDataManager 인스턴스 생성
                    sample_manager = SampleDataManager(
                        self.azure_config, force_local=False
                    )

                    # 🔥 수정: ensure_tables_exist 메서드 사용
                    sample_manager.ensure_tables_exist()

                    self.logger.info("✅ Azure 테이블 설정 완료")

                except Exception as table_error:
                    self.logger.warning(
                        f"테이블 생성 중 오류 (무시하고 계속): {table_error}"
                    )

            else:
                raise Exception("Azure 연결 테스트 실패")

        except Exception as e:
            self.logger.error(f"❌ Azure SQL Database 연결 실패: {e}")
            raise e

    def check_azure_permissions(self):
        """Azure SQL Database 권한 확인"""
        try:
            from sqlalchemy import text

            with self.sqlalchemy_engine.connect() as conn:
                # 현재 사용자 확인
                result = conn.execute(text("SELECT CURRENT_USER as current_user"))
                current_user = result.fetchone()[0]
                self.logger.info(f"현재 사용자: {current_user}")

                # 데이터베이스 확인
                result = conn.execute(text("SELECT DB_NAME() as database_name"))
                database_name = result.fetchone()[0]
                self.logger.info(f"현재 데이터베이스: {database_name}")

        except Exception as e:
            self.logger.warning(f"권한 확인 실패: {e}")

    def _ensure_azure_tables_with_sample_manager(self):
        """기존 create_sample_database 함수를 사용하여 Azure 테이블 설정"""
        try:
            self.logger.info("샘플 데이터 함수를 사용하여 Azure 테이블 설정 중...")

            # 🔥 수정: create_sample_database 함수 직접 사용 (Azure 설정과 force_local=False)
            from sample_data import create_sample_database

            azure_conn = create_sample_database(self.azure_config, force_local=False)

            if azure_conn:
                # 연결 테스트
                if hasattr(azure_conn, "cursor"):
                    # pymssql 연결인 경우
                    cursor = azure_conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM PY_NP_SBSC_RMNY_TXN")
                    count = cursor.fetchone()[0]
                    azure_conn.close()  # 연결 정리
                else:
                    # SQLAlchemy 연결인 경우
                    from sqlalchemy import text

                    with azure_conn.connect() as conn:
                        result = conn.execute(
                            text("SELECT COUNT(*) FROM PY_NP_SBSC_RMNY_TXN")
                        )
                        count = result.fetchone()[0]

                self.logger.info(
                    f"✅ Azure 테이블 설정 완료 - 포트인 데이터: {count}건"
                )
            else:
                raise Exception("Azure 데이터베이스 생성 실패")

        except Exception as e:
            self.logger.error(f"샘플 데이터 함수를 통한 테이블 설정 실패: {e}")
            raise e

    def _create_sqlalchemy_engine(self):
        """SQLAlchemy 엔진 생성 - connection_string 직접 사용"""
        try:
            if not self.connection_string:
                raise ValueError("연결 문자열이 없습니다")

            # 🔥 수정: 이미 완성된 connection_string을 직접 사용
            self.sqlalchemy_engine = create_engine(
                self.connection_string, pool_timeout=20, pool_recycle=3600, echo=False
            )

            self.logger.info("SQLAlchemy 엔진 생성 성공")

        except Exception as e:
            self.logger.error(f"SQLAlchemy 엔진 생성 실패: {e}")
            raise e

    @contextmanager
    def get_connection(self):
        """데이터베이스 연결 컨텍스트 매니저"""
        if self.use_sample_data:
            # SQLite 샘플 연결
            if not self.sample_connection:
                raise Exception("샘플 데이터베이스가 초기화되지 않았습니다")
            yield self.sample_connection
        else:
            # 🔥 수정: SQLAlchemy 엔진 사용
            if not self.sqlalchemy_engine:
                raise Exception("SQLAlchemy 엔진이 없습니다")

            conn = None
            try:
                conn = self.sqlalchemy_engine.connect()
                yield conn
            except Exception as e:
                self.logger.error(f"Azure 연결 오류: {e}")
                raise e
            finally:
                if conn:
                    conn.close()

    def execute_query(
        self, sql_query: str, params: Optional[Dict] = None
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """SQL 쿼리 실행"""
        start_time = time.time()
        metadata = {
            "execution_time": 0,
            "row_count": 0,
            "column_count": 0,
            "query_hash": hash(sql_query),
            "success": False,
            "error_message": None,
            "database_type": "Azure SQL" if not self.use_sample_data else "SQLite",
            "query_preview": (
                sql_query[:100] + "..." if len(sql_query) > 100 else sql_query
            ),
        }

        try:
            # 쿼리 검증
            if not self._validate_query_safety(sql_query):
                raise ValueError("안전하지 않은 쿼리입니다")

            # 쿼리 실행
            if self.use_sample_data:
                # SQLite용 쿼리
                with self.get_connection() as conn:
                    df = pd.read_sql_query(sql_query, conn, params=params)
            else:
                # 🔥 수정: SQLAlchemy 엔진 존재 확인
                if not self.sqlalchemy_engine:
                    raise Exception("SQLAlchemy 엔진이 초기화되지 않았습니다")

                df = pd.read_sql_query(sql_query, self.sqlalchemy_engine, params=params)

            # 결과 크기 제한
            if len(df) > self.max_result_rows:
                self.logger.warning(
                    f"결과가 최대 행 수({self.max_result_rows})를 초과하여 잘렸습니다"
                )
                df = df.head(self.max_result_rows)
                metadata["truncated"] = True

            # 메타데이터 업데이트
            execution_time = time.time() - start_time
            metadata.update(
                {
                    "execution_time": round(execution_time, 3),
                    "row_count": len(df),
                    "column_count": len(df.columns),
                    "success": True,
                    "data_size_mb": round(
                        df.memory_usage(deep=True).sum() / 1024 / 1024, 2
                    ),
                }
            )

            self.logger.info(
                f"쿼리 실행 성공: {metadata['row_count']}행, {execution_time:.3f}초, DB: {metadata['database_type']}"
            )

            return df, metadata

        except Exception as e:
            execution_time = time.time() - start_time
            error_message = str(e)

            metadata.update(
                {
                    "execution_time": round(execution_time, 3),
                    "success": False,
                    "error_message": error_message,
                    "error_type": type(e).__name__,
                }
            )

            self.logger.error(f"쿼리 실행 실패: {error_message}")
            return pd.DataFrame(), metadata

    def _validate_query_safety(self, sql_query: str) -> bool:
        """쿼리 안전성 검증"""
        try:
            sql_upper = sql_query.upper().strip()

            # 1. 허용된 구문만 실행
            allowed_start_keywords = ["SELECT", "WITH"]
            if not any(
                sql_upper.startswith(keyword) for keyword in allowed_start_keywords
            ):
                self.logger.warning("허용되지 않은 SQL 구문")
                return False

            # 2. 위험한 키워드 차단
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
                "MERGE",
                "BULK",
            ]
            for keyword in dangerous_keywords:
                if keyword in sql_upper:
                    self.logger.warning(f"위험한 키워드 발견: {keyword}")
                    return False

            # 3. 허용된 테이블만 접근
            allowed_tables = [
                "PY_NP_TRMN_RMNY_TXN",
                "PY_NP_SBSC_RMNY_TXN",
                "PY_DEPAZ_BAS",
            ]
            has_allowed_table = any(table in sql_query for table in allowed_tables)
            if not has_allowed_table:
                self.logger.warning("허용된 테이블이 없음")
                return False

            # 4. 쿼리 길이 제한
            if len(sql_query) > 5000:
                self.logger.warning("쿼리가 너무 깁니다")
                return False

            return True

        except Exception as e:
            self.logger.error(f"쿼리 검증 중 오류: {e}")
            return False

    def test_connection(self) -> bool:
        """데이터베이스 연결 테스트"""
        try:
            # 🔥 수정: test_query를 함수 시작 부분에 정의
            test_query = "SELECT 1 as test_value"

            if self.use_sample_data:
                # SQLite 테스트
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(test_query)
                    result = cursor.fetchone()
                    return result is not None
            else:
                # 🔥 수정: SQLAlchemy 엔진으로 테스트
                if not self.sqlalchemy_engine:
                    self.logger.error("SQLAlchemy 엔진이 초기화되지 않았습니다")
                    return False

                # 🔥 수정: SQLAlchemy text() 함수 사용
                from sqlalchemy import text

                with self.sqlalchemy_engine.connect() as conn:
                    result = conn.execute(text(test_query))
                    row = result.fetchone()
                    return row is not None

        except Exception as e:
            self.logger.error(f"연결 테스트 실패: {e}")
            return False

    def cleanup_connections(self):
        """연결 정리"""
        try:
            if self.use_sample_data and self.sample_connection:
                self.sample_connection.close()
                self.logger.info("샘플 데이터베이스 연결 종료")

            if self.sqlalchemy_engine:
                self.sqlalchemy_engine.dispose()
                self.logger.info("SQLAlchemy 엔진 정리 완료")

        except Exception as e:
            self.logger.error(f"연결 정리 중 오류: {e}")

    def get_table_info(self) -> Dict[str, Dict]:
        """테이블 정보 조회 - 속성명 수정"""
        table_info = {}

        try:
            with self.get_connection() as conn:
                # 샘플 데이터와 Azure 데이터베이스에서 다른 테이블명 사용
                tables = [
                    "PY_NP_TRMN_RMNY_TXN",
                    "PY_NP_SBSC_RMNY_TXN",
                    "PY_DEPAZ_BAS",
                ]
                date_columns = {
                    "PY_NP_TRMN_RMNY_TXN": "NP_TRMN_DATE",
                    "PY_NP_SBSC_RMNY_TXN": "TRT_DATE",
                    "PY_DEPAZ_BAS": "RMNY_DATE",
                }

                for table in tables:
                    try:
                        # 테이블 존재 여부 확인
                        if self.use_sample_data:
                            # SQLite용 테이블 존재 확인
                            check_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
                        else:
                            # Azure SQL용 테이블 존재 확인
                            check_query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}'"

                        check_result = pd.read_sql_query(check_query, conn)
                        if check_result.empty:
                            table_info[table] = {
                                "row_count": 0,
                                "latest_date": "N/A",  # 🔥 수정: 문자열로 설정
                                "status": "❌ 테이블 없음",
                            }
                            continue

                        # 테이블 행 수 조회
                        count_query = f"SELECT COUNT(*) as row_count FROM {table}"
                        count_result = pd.read_sql_query(count_query, conn)
                        row_count = count_result.iloc[0]["row_count"]

                        # 최근 데이터 날짜 조회
                        date_column = date_columns.get(table)
                        if date_column:
                            if self.use_sample_data:
                                date_query = f"SELECT MAX({date_column}) as latest_date FROM {table}"
                            else:
                                date_query = f"SELECT MAX({date_column}) as latest_date FROM {table}"

                            date_result = pd.read_sql_query(date_query, conn)
                            latest_date_raw = date_result.iloc[0]["latest_date"]

                            # 🔥 수정: 날짜를 문자열로 변환
                            if latest_date_raw is not None:
                                if hasattr(latest_date_raw, "strftime"):
                                    latest_date = latest_date_raw.strftime("%Y-%m-%d")
                                else:
                                    latest_date = str(latest_date_raw)
                            else:
                                latest_date = "N/A"
                        else:
                            latest_date = "N/A"

                        table_info[table] = {
                            "row_count": row_count,
                            "latest_date": latest_date,  # 🔥 수정: 이미 문자열로 변환됨
                            "status": "✅ 활성",
                        }

                    except Exception as e:
                        table_info[table] = {
                            "row_count": 0,
                            "latest_date": "N/A",  # 🔥 수정: 문자열로 설정
                            "status": f"❌ 오류: {str(e)[:50]}...",
                        }

        except Exception as e:
            self.logger.error(f"테이블 정보 조회 실패: {e}")

        return table_info

    def get_performance_stats(self) -> Dict[str, Any]:
        """데이터베이스 성능 통계 - Azure 정보 추가"""
        stats = {
            "connection_type": self.connection_type,
            "max_execution_time": self.max_execution_time,
            "max_result_rows": self.max_result_rows,
            "connection_status": (
                "✅ 연결됨" if self.test_connection() else "❌ 연결 실패"
            ),
            "azure_services": (
                self.azure_config.test_connection()
                if not self.use_sample_data
                else None
            ),
        }

        # 테이블 정보 추가
        try:
            table_info = self.get_table_info()
            stats["tables"] = table_info
        except Exception as e:
            self.logger.error(f"테이블 정보 조회 실패: {e}")
            stats["tables"] = {}

        return stats

    def get_sample_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """테이블 샘플 데이터 조회 - 속성명 및 컬럼명 수정"""
        try:
            if table_name not in [
                "PY_NP_TRMN_RMNY_TXN",
                "PY_NP_SBSC_RMNY_TXN",
                "PY_DEPAZ_BAS",
            ]:
                raise ValueError(f"허용되지 않은 테이블: {table_name}")

            # 데이터베이스 타입에 따른 쿼리 생성
            if self.use_sample_data:  # SQLite 샘플 데이터
                # if table_name in ["PY_NP_TRMN_RMNY_TXN"]:
                #     sample_query = f"""
                #     SELECT
                #         SUBSTR(TEL_NO, 1, 3) || '****' || SUBSTR(TEL_NO, -4) as masked_phone,
                #         SVC_CONT_ID,
                #         PAY_AMT,
                #         BCHNG_COMM_CMPN_ID as operator,
                #         NP_TRMN_DATE as transaction_date
                #     FROM {table_name}
                #     ORDER BY NP_TRMN_DATE DESC
                #     LIMIT {limit}
                #     """
                # elif table_name == "PY_NP_SBSC_RMNY_TXN":
                #     sample_query = f"""
                #     SELECT
                #         SUBSTR(TEL_NO, 1, 3) || '****' || SUBSTR(TEL_NO, -4) as masked_phone,
                #         SVC_CONT_ID,
                #         SETL_AMT,
                #         BCHNG_COMM_CMPN_ID as operator,
                #         TRT_DATE as transaction_date
                #     FROM {table_name}
                #     ORDER BY TRT_DATE DESC
                #     LIMIT {limit}
                #     """
                # else:  # PY_DEPAZ_BAS
                #     sample_query = f"""
                #     SELECT
                #         SVC_CONT_ID,
                #         DEPAZ_AMT,
                #         DEPAZ_DIV_CD,
                #         RMNY_DATE as deposit_date
                #     FROM {table_name}
                #     ORDER BY RMNY_DATE DESC
                #     LIMIT {limit}
                #     """
                # else:  # Azure SQL Database
                if table_name == "PY_NP_TRMN_RMNY_TXN":
                    sample_query = f"""
                    SELECT TOP {limit}
                        SUBSTRING(TEL_NO, 1, 3) + '****' + RIGHT(TEL_NO, 4) as masked_phone,
                        SVC_CONT_ID,
                        PAY_AMT,
                        ACHNG_COMM_CMPN_ID as operator,
                        NP_TRMN_DATE as transaction_date
                    FROM {table_name}
                    ORDER BY NP_TRMN_DATE DESC
                    """
                elif table_name == "PY_NP_SBSC_RMNY_TXN":
                    sample_query = f"""
                    SELECT TOP {limit}
                        SUBSTRING(TEL_NO, 1, 3) + '****' + RIGHT(TEL_NO, 4) as masked_phone,
                        SVC_CONT_ID,
                        SETL_AMT,
                        BCHNG_COMM_CMPN_ID as operator,
                        TRT_DATE as transaction_date
                    FROM {table_name}
                    ORDER BY TRT_DATE DESC
                    """
                else:  # PY_DEPAZ_BAS
                    sample_query = f"""
                    SELECT TOP {limit}
                        SVC_CONT_ID,
                        BILL_ACC_ID,
                        DEPAZ_AMT,
                        RMNY_DATE as deposit_date
                    FROM {table_name}
                    ORDER BY RMNY_DATE DESC
                    """

            df, _ = self.execute_query(sample_query)
            return df

        except Exception as e:
            self.logger.error(f"샘플 데이터 조회 실패: {e}")
            return pd.DataFrame()

    def get_database_type(self) -> str:
        """현재 사용 중인 데이터베이스 타입 반환"""
        return "Azure SQL Database" if self.use_sample_data else "SQLite"

    def is_azure_mode(self) -> bool:
        """Azure 모드 사용 여부 반환"""
        return self.use_sample_data


def get_connection_info(self) -> Dict[str, Any]:
    """연결 정보 반환"""
    return {
        "type": self.get_database_type(),
        "azure_ready": (
            self.azure_config.is_production_ready() if self.azure_config else False
        ),
        "use_sample_data": self.use_sample_data,
        "connection_string_available": bool(self.connection_string),
        "sqlalchemy_engine_available": bool(self.sqlalchemy_engine),  # 🔥 수정
    }


# 데이터베이스 매니저 팩토리
class DatabaseManagerFactory:
    """데이터베이스 매니저 팩토리 클래스"""

    @staticmethod
    def create_manager(
        azure_config: AzureConfig, force_sample: bool = False
    ) -> DatabaseManager:
        """
        환경에 따라 적절한 데이터베이스 매니저 생성

        Args:
            azure_config: Azure 설정
            force_sample: 강제로 샘플 데이터 사용

        Returns:
            DatabaseManager 인스턴스
        """
        logger = logging.getLogger(__name__)

        # 1. 강제 샘플 모드
        if force_sample:
            logger.info("🔧 강제 샘플 모드로 실행")
            try:
                return DatabaseManager(azure_config, use_sample_data=True)
            except Exception as e:
                logger.error(f"샘플 모드 생성 실패: {e}")
                raise Exception(f"샘플 데이터베이스 생성 실패: {e}")

        # 2. Azure 우선 시도
        logger.info("☁️ Azure 클라우드 연결 시도...")
        try:
            # 🔥 수정: Azure 서비스 상태 먼저 확인
            connection_status = azure_config.test_connection()

            # 🔥 수정: connection_status가 dict인지 확인하고 안전하게 접근
            if not isinstance(connection_status, dict):
                logger.warning("Azure 연결 상태 확인 실패 - 샘플 모드로 전환")
                return DatabaseManager(azure_config, use_sample_data=True)

            if not connection_status.get("database", False):
                logger.warning("Azure 데이터베이스 설정 불완전 - 샘플 모드로 전환")
                return DatabaseManager(azure_config, use_sample_data=True)

            # Azure 매니저 생성 시도
            azure_manager = DatabaseManager(azure_config, use_sample_data=False)

            if azure_manager.test_connection():
                logger.info("✅ Azure 데이터베이스 연결 성공")
                return azure_manager
            else:
                logger.warning("Azure 연결 테스트 실패 - 샘플 모드로 전환")
                azure_manager.cleanup_connections()
                return DatabaseManager(azure_config, use_sample_data=True)

        except Exception as e:
            logger.error(f"Azure 연결 실패: {e}")
            logger.info("🔄 샘플 모드로 백업...")

            try:
                return DatabaseManager(azure_config, use_sample_data=True)
            except Exception as sample_e:
                logger.error(f"샘플 모드도 실패: {sample_e}")
                raise Exception(
                    f"모든 데이터베이스 연결 실패. Azure: {e}, Sample: {sample_e}"
                )

    @staticmethod
    def create_azure_manager(azure_config) -> DatabaseManager:
        """Azure SQL Database 전용 매니저 생성"""
        if not azure_config or not azure_config.is_production_ready():
            raise ValueError("Azure 환경이 준비되지 않았습니다")

        return DatabaseManager(azure_config, use_sample_data=False)

    @staticmethod
    def create_sample_manager(azure_config: AzureConfig) -> DatabaseManager:
        """샘플 데이터 전용 매니저 생성"""
        return DatabaseManager(azure_config, use_sample_data=True)


# 테스트 함수
def test_database_manager():
    """데이터베이스 매니저 테스트"""
    print("🧪 데이터베이스 매니저 테스트를 시작합니다...")

    try:
        # Azure 설정 로드 시도
        try:
            from azure_config import get_azure_config

            azure_config = get_azure_config()
            print("Azure 설정 로드 성공")
        except Exception as e:
            print(f"Azure 설정 로드 실패: {e}")
            azure_config = None

        # 1. Azure 모드 테스트 (가능한 경우)
        if azure_config and azure_config.is_production_ready():
            print("\n☁️ Azure SQL Database 테스트:")
            try:
                azure_manager = DatabaseManagerFactory.create_azure_manager(
                    azure_config
                )

                # 연결 테스트
                connection_ok = azure_manager.test_connection()
                print(f"   연결 테스트: {'✅ 성공' if connection_ok else '❌ 실패'}")

                # 성능 통계
                stats = azure_manager.get_performance_stats()
                print(f"   연결 타입: {stats['connection_type']}")
                print(f"   연결 상태: {stats['connection_status']}")

                # 테이블 정보
                print("\n📋 Azure 테이블 정보:")
                for table_name, info in stats["tables"].items():
                    print(
                        f"   {table_name}: {info['row_count']:,}행, 최신: {info['latest_date']}"
                    )

                # 쿼리 실행 테스트
                print("\n🔍 Azure 쿼리 실행 테스트:")
                # if azure_manager.use_sample_data:
                test_query = """
                SELECT TOP 1
                    COUNT(*) as total_count,
                    SUM(SETL_AMT) as total_amount
                FROM PY_NP_SBSC_RMNY_TXN
                WHERE TRT_DATE >= DATEADD(month, -1, GETDATE())
                    AND NP_STTUS_CD IN ('OK', 'WD')
                """
                # else:
                #     test_query = """
                #     SELECT 
                #         COUNT(*) as total_count,
                #         SUM(SETL_AMT) as total_amount
                #     FROM PY_NP_SBSC_RMNY_TXN
                #     WHERE TRT_DATE >= date('now', '-1 months')
                #         AND NP_STTUS_CD IN ('OK', 'WD')
                #     """

                df, metadata = azure_manager.execute_query(test_query)
                print(f"   실행 시간: {metadata['execution_time']}초")
                print(f"   결과 행수: {metadata['row_count']}")
                print(f"   성공 여부: {'✅' if metadata['success'] else '❌'}")
                print(f"   DB 타입: {metadata['database_type']}")

                if not df.empty and metadata["success"]:
                    print(f"   총 건수: {df.iloc[0]['total_count']:,}")
                    print(f"   총 금액: {df.iloc[0]['total_amount']:,.0f}원")

                print("   ✅ Azure 모드 테스트 성공")
            except Exception as e:
                print(f"   ❌ Azure 모드 테스트 실패: {e}")

        # 2. 샘플 모드 테스트
        print("\n💻 샘플 데이터베이스 테스트:")
        sample_manager = DatabaseManagerFactory.create_sample_manager(azure_config)

        # 연결 테스트
        connection_ok = sample_manager.test_connection()
        print(f"   연결 테스트: {'✅ 성공' if connection_ok else '❌ 실패'}")

        # 성능 통계
        stats = sample_manager.get_performance_stats()
        print(f"   연결 타입: {stats['connection_type']}")
        print(f"   연결 상태: {stats['connection_status']}")

        # 테이블 정보
        print("\n📋 샘플 테이블 정보:")
        for table_name, info in stats["tables"].items():
            print(
                f"   {table_name}: {info['row_count']:,}행, 최신: {info['latest_date']}"
            )

        # 쿼리 실행 테스트
        print("\n🔍 샘플 쿼리 실행 테스트:")
        test_query = """
        SELECT 
            COUNT(*) as total_count,
            SUM(SETL_AMT) as total_amount
        FROM PY_NP_SBSC_RMNY_TXN
        WHERE TRT_DATE >= DATEADD((month, -1, GETDATE())
            AND NP_STTUS_CD IN ('OK', 'WD')
        """

        df, metadata = sample_manager.execute_query(test_query)
        print(f"   실행 시간: {metadata['execution_time']}초")
        print(f"   결과 행수: {metadata['row_count']}")
        print(f"   성공 여부: {'✅' if metadata['success'] else '❌'}")
        print(f"   DB 타입: {metadata['database_type']}")

        if not df.empty and metadata["success"]:
            print(f"   총 건수: {df.iloc[0]['total_count']:,}")
            print(f"   총 금액: {df.iloc[0]['total_amount']:,.0f}원")

        # 샘플 데이터 조회
        print("\n📄 샘플 데이터:")
        sample_df = sample_manager.get_sample_data("PY_NP_SBSC_RMNY_TXN", 3)
        if not sample_df.empty:
            print(sample_df.to_string(index=False))

        print("\n✅ 모든 테스트 완료!")

    except Exception as e:
        print(f"\n❌ 테스트 실패: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_database_manager()
