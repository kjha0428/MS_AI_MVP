# database_manager.py - 데이터베이스 연결 및 쿼리 실행 관리
import pyodbc
import pandas as pd
import sqlite3
import logging
import time
from typing import Optional, Dict, Any, Tuple
from contextlib import contextmanager
from azure_config import AzureConfig
from sample_data import SampleDataManager


class DatabaseManager:
    """데이터베이스 연결 및 쿼리 실행 관리 클래스"""

    def __init__(self, azure_config: AzureConfig, use_sample_data: bool = False):
        """
        데이터베이스 매니저 초기화

        Args:
            azure_config: Azure 설정 객체
            use_sample_data: 샘플 데이터 사용 여부 (개발용)
        """
        self.azure_config = azure_config
        self.use_sample_data = use_sample_data
        self.logger = logging.getLogger(__name__)

        # 연결 설정
        self.connection_string = None
        self.sample_manager = None
        self.sample_connection = None

        # Azure SQL Database 사용 여부 결정
        self.use_azure = (
            not use_sample_data
            and azure_config
            and azure_config.is_production_ready()
            and azure_config.sql_connection_string
        )

        # 성능 설정
        self.max_execution_time = 30  # 최대 쿼리 실행 시간 (초)
        self.max_result_rows = 2000  # 최대 결과 행 수

        self._initialize_connection()

    def _initialize_connection(self):
        """데이터베이스 연결 초기화"""
        if self.use_azure:
            self._initialize_azure_connection()
        else:
            self._initialize_sample_connection()

    def _initialize_sample_connection(self):
        """샘플 데이터베이스 연결 초기화"""
        try:
            self.sample_manager = SampleDataManager(self.azure_config, force_local=True)
            self.sample_connection = self.sample_manager.create_sample_database()
            self.logger.info("샘플 데이터베이스 연결 성공")
        except Exception as e:
            self.logger.error(f"샘플 데이터베이스 연결 실패: {e}")
            raise e

    def _initialize_azure_connection(self):
        """Azure SQL Database 연결 초기화"""
        try:
            self.connection_string = self.azure_config.get_database_connection_string()
            if not self.connection_string:
                raise ValueError("데이터베이스 연결 문자열을 가져올 수 없습니다")

            # Azure용 샘플 데이터 매니저 생성
            self.sample_manager = SampleDataManager(
                self.azure_config, force_local=False
            )

            # 연결 테스트
            if self.test_connection():
                self.logger.info("Azure SQL Database 연결 성공")
            else:
                raise Exception("연결 테스트 실패")

        except Exception as e:
            self.logger.error(f"Azure SQL Database 연결 실패: {e}")
            self.logger.info("샘플 데이터베이스로 폴백")
            self.use_azure = False
            self._initialize_sample_connection()
            raise e

    @contextmanager
    def get_connection(self):
        """데이터베이스 연결 컨텍스트 매니저"""
        if self.use_azure:
            conn = None
            try:
                import pyodbc

                conn = pyodbc.connect(
                    self.connection_string,
                    timeout=self.max_execution_time,
                    autocommit=True,
                )
                yield conn
            except Exception as e:
                self.logger.error(f"Azure SQL Database 연결 오류: {e}")
                raise e
            finally:
                if conn:
                    conn.close()
        else:
            yield self.sample_connection

    def execute_query(
        self, sql_query: str, params: Optional[Dict] = None
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        SQL 쿼리 실행

        Args:
            sql_query: 실행할 SQL 쿼리
            params: 쿼리 파라미터 (선택사항)

        Returns:
            Tuple[pd.DataFrame, Dict]: (결과 데이터프레임, 실행 메타데이터)
        """
        start_time = time.time()
        metadata = {
            "execution_time": 0,
            "row_count": 0,
            "column_count": 0,
            "query_hash": hash(sql_query),
            "success": False,
            "error_message": None,
            "database_type": "Azure SQL" if self.use_azure else "SQLite",
            "query_preview": (
                sql_query[:100] + "..." if len(sql_query) > 100 else sql_query
            ),
        }

        try:
            # 쿼리 검증
            if not self._validate_query_safety(sql_query):
                raise ValueError("안전하지 않은 쿼리입니다")

            # 쿼리 실행
            with self.get_connection() as conn:
                df = pd.read_sql_query(sql_query, conn, params=params)

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
            test_query = "SELECT 1 as test_value"

            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(test_query)
                result = cursor.fetchone()
                return result is not None

        except Exception as e:
            self.logger.error(f"연결 테스트 실패: {e}")
            return False

    def get_table_info(self) -> Dict[str, Dict]:
        """테이블 정보 조회"""
        table_info = {}

        try:
            with self.get_connection() as conn:
                tables = ["PY_NP_TRMN_RMNY_TXN", "PY_NP_SBSC_RMNY_TXN", "PY_DEPAZ_BAS"]

                for table in tables:
                    try:
                        # 테이블 행 수 조회
                        count_query = f"SELECT COUNT(*) as row_count FROM {table}"
                        count_result = pd.read_sql_query(count_query, conn)
                        row_count = count_result.iloc[0]["row_count"]

                        # 최근 데이터 날짜 조회
                        if table == "PY_NP_TRMN_RMNY_TXN":
                            date_query = (
                                f"SELECT MAX(NP_TRMN_DATE) as latest_date FROM {table}"
                            )
                        elif table == "PY_NP_SBSC_RMNY_TXN":
                            date_query = (
                                f"SELECT MAX(TRT_DATE) as latest_date FROM {table}"
                            )
                        else:  # PY_DEPAZ_BAS
                            date_query = (
                                f"SELECT MAX(RMNY_DATE) as latest_date FROM {table}"
                            )

                        date_result = pd.read_sql_query(date_query, conn)
                        latest_date = date_result.iloc[0]["latest_date"]

                        table_info[table] = {
                            "row_count": row_count,
                            "latest_date": latest_date,
                            "status": "✅ 활성",
                        }

                    except Exception as e:
                        table_info[table] = {
                            "row_count": 0,
                            "latest_date": None,
                            "status": f"❌ 오류: {str(e)}",
                        }

        except Exception as e:
            self.logger.error(f"테이블 정보 조회 실패: {e}")

        return table_info

    def get_performance_stats(self) -> Dict[str, Any]:
        """데이터베이스 성능 통계"""
        stats = {
            "connection_type": (
                "Sample SQLite" if self.use_sample_data else "Azure SQL Database"
            ),
            "max_execution_time": self.max_execution_time,
            "max_result_rows": self.max_result_rows,
            "connection_status": (
                "✅ 연결됨" if self.test_connection() else "❌ 연결 실패"
            ),
        }

        # 테이블 정보 추가
        table_info = self.get_table_info()
        stats["tables"] = table_info

        return stats

    def get_sample_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """테이블 샘플 데이터 조회"""
        try:
            if table_name not in [
                "PY_NP_TRMN_RMNY_TXN",
                "PY_NP_SBSC_RMNY_TXN",
                "PY_DEPAZ_BAS",
            ]:
                raise ValueError(f"허용되지 않은 테이블: {table_name}")

            # 개인정보 마스킹을 위한 컬럼 선택
            if table_name in ["PY_NP_TRMN_RMNY_TXN", "PY_NP_SBSC_RMNY_TXN"]:
                sample_query = f"""
                SELECT 
                    SUBSTR(TEL_NO, 1, 3) || '****' || SUBSTR(TEL_NO, -4) as masked_phone,
                    SVC_CONT_ID,
                    {'PAY_AMT' if table_name == 'PY_NP_TRMN_RMNY_TXN' else 'SETL_AMT'} as SETL_AMT,
                    {'ACHNG_COMM_CMPN_ID' if table_name == 'PY_NP_TRMN_RMNY_TXN' else 'BCHNG_COMM_CMPN_ID'} as COMM_CMPN_NM,
                    {'ACHNG_COMM_CMPN_ID' if table_name == 'PY_NP_TRMN_RMNY_TXN' else 'BCHNG_COMM_CMPN_ID'} as transaction_date
                FROM {table_name}
                ORDER BY {'NP_TRMN_DATE' if table_name == 'PY_NP_TRMN_RMNY_TXN' else 'TRT_DATE'} DESC
                LIMIT {limit}
                """
            else:  # PY_DEPAZ_BAS
                sample_query = f"""
                SELECT 
                    SVC_CONT_ID,
                    DEPAZ_AMT,
                    RMNY_DATE as deposit_date
                FROM {table_name}
                ORDER BY RMNY_DATE DESC
                LIMIT {limit}
                """

            df, _ = self.execute_query(sample_query)
            return df

        except Exception as e:
            self.logger.error(f"샘플 데이터 조회 실패: {e}")
            return pd.DataFrame()

    def get_database_type(self) -> str:
        """현재 사용 중인 데이터베이스 타입 반환"""
        return "Azure SQL Database" if self.use_azure else "SQLite"

    def is_azure_mode(self) -> bool:
        """Azure 모드 사용 여부 반환"""
        return self.use_azure

    def get_connection_info(self) -> Dict[str, Any]:
        """연결 정보 반환"""
        return {
            "type": self.get_database_type(),
            "azure_ready": (
                self.azure_config.is_production_ready() if self.azure_config else False
            ),
            "use_azure": self.use_azure,
            "use_sample_data": self.use_sample_data,
            "connection_string_available": bool(self.connection_string),
            "sample_manager_available": bool(self.sample_manager),
        }


# 데이터베이스 매니저 팩토리
class DatabaseManagerFactory:
    """데이터베이스 매니저 팩토리 클래스"""

    @staticmethod
    def create_manager(
        azure_config=None, force_sample: bool = False
    ) -> DatabaseManager:
        """
        환경에 따라 적절한 데이터베이스 매니저 생성

        Args:
            azure_config: Azure 설정
            force_sample: 강제로 샘플 데이터 사용

        Returns:
            DatabaseManager 인스턴스
        """
        # 개발 환경이거나 Azure 연결이 불가능한 경우 샘플 데이터 사용
        use_sample = force_sample or not (
            azure_config and azure_config.is_production_ready()
        )

        return DatabaseManager(azure_config, use_sample_data=use_sample)

    @staticmethod
    def create_azure_manager(azure_config) -> DatabaseManager:
        """Azure SQL Database 전용 매니저 생성"""
        if not azure_config or not azure_config.is_production_ready():
            raise ValueError("Azure 환경이 준비되지 않았습니다")

        return DatabaseManager(azure_config, use_sample_data=False)

    @staticmethod
    def create_sample_manager(azure_config=None) -> DatabaseManager:
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
                if azure_manager.use_azure:
                    test_query = """
                    SELECT TOP 1
                        COUNT(*) as total_count,
                        SUM(SETL_AMT) as total_amount
                    FROM PY_NP_SBSC_RMNY_TXN
                    WHERE TRT_DATE >= DATEADD(month, -1, GETDATE())
                        AND NP_STTUS_CD IN ('OK', 'WD')
                    """
                else:
                    test_query = """
                    SELECT 
                        COUNT(*) as total_count,
                        SUM(SETL_AMT) as total_amount
                    FROM PY_NP_SBSC_RMNY_TXN
                    WHERE TRT_DATE >= date('now', '-1 months')
                        AND NP_STTUS_CD IN ('OK', 'WD')
                    """

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
        WHERE TRT_DATE >= date('now', '-1 months')
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
