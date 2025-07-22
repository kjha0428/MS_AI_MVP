# sample_data.py - 간단한 샘플 데이터 관리 (Key Vault 없음)
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import logging
from typing import Optional, Dict, Any


class SampleDataManager:
    """간단한 샘플 데이터 관리 클래스"""

    def __init__(self, azure_config=None, force_local: bool = False):
        """
        샘플 데이터 매니저 초기화

        Args:
            azure_config: Azure 설정 객체 (선택사항)
            force_local: 강제로 로컬 SQLite 사용
        """
        self.azure_config = azure_config
        self.force_local = force_local
        self.logger = logging.getLogger(__name__)

        # Azure 사용 가능 여부 확인 (연결 문자열이 실제로 있는지 확인)
        self.use_azure = (
            not force_local
            and azure_config
            and azure_config.is_production_ready()
            and hasattr(azure_config, "sql_connection_string")
            and azure_config.sql_connection_string
            and azure_config.sql_connection_string.strip()  # 빈 문자열 체크
        )

        if self.use_azure:
            self.logger.info("Azure SQL Database 모드로 초기화")
        else:
            self.logger.info("로컬 SQLite 모드로 초기화")

    def create_sample_database(self):
        """샘플 데이터베이스 생성"""
        try:
            if self.use_azure:
                return self._create_azure_database()
            else:
                return self._create_local_database()

        except Exception as e:
            self.logger.error(f"샘플 데이터베이스 생성 실패: {e}")
            # Azure 실패시 로컬로 폴백
            if self.use_azure:
                self.logger.warning("Azure 연결 실패, 로컬 SQLite로 전환")
                self.use_azure = False
                return self._create_local_database()
            raise e

    def _create_azure_database(self):
        """Azure SQL Database 샘플 데이터 생성"""
        try:
            import pyodbc
        except ImportError:
            raise ImportError("pyodbc가 필요합니다: pip install pyodbc")

        conn_string = self.azure_config.get_database_connection_string()
        if not conn_string or not conn_string.strip():
            raise ValueError("Azure SQL Database 연결 문자열이 설정되지 않았습니다")

        try:
            conn = pyodbc.connect(conn_string, timeout=30)
        except Exception as e:
            raise Exception(f"Azure SQL Database 연결 실패: {e}")

        try:
            # 테이블 존재 여부 확인
            if not self._azure_tables_exist(conn):
                self.logger.info("Azure SQL Database에 테이블 생성 중...")
                self._create_azure_tables(conn)

            # 기존 데이터 확인
            data_count = self._check_azure_data(conn)

            # 충분한 데이터가 있으면 그대로 사용
            if data_count["total"] > 1000:
                self.logger.info(
                    f"충분한 샘플 데이터가 존재합니다 ({data_count['total']:,}건)"
                )
                return conn

            # 데이터 추가 생성
            self.logger.info("샘플 데이터 생성 중...")
            self._generate_azure_data(conn)

            return conn

        except Exception as e:
            conn.close()
            raise e

    def _create_local_database(self):
        """로컬 SQLite 샘플 데이터 생성"""
        conn = sqlite3.connect(":memory:", check_same_thread=False)

        # 테이블 생성
        self._create_sqlite_tables(conn)

        # 샘플 데이터 생성
        self._generate_sqlite_data(conn)

        self.logger.info("✅ 로컬 샘플 데이터베이스 생성 완료")
        return conn

    def _azure_tables_exist(self, conn) -> bool:
        """Azure SQL Database 테이블 존재 여부 확인"""
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME IN ('PY_NP_TRMN_RMNY_TXN', 'PY_NP_SBSC_RMNY_TXN', 'PY_DEPAZ_BAS')
            """
            )
            result = cursor.fetchone()
            return result[0] == 3
        except:
            return False

    def _check_azure_data(self, conn) -> Dict[str, int]:
        """Azure SQL Database 데이터 현황 확인"""
        cursor = conn.cursor()
        tables = ["PY_NP_TRMN_RMNY_TXN", "PY_NP_SBSC_RMNY_TXN", "PY_DEPAZ_BAS"]
        counts = {}
        total = 0

        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                counts[table] = count
                total += count
            except:
                counts[table] = 0

        counts["total"] = total
        return counts

    def _create_azure_tables(self, conn):
        """Azure SQL Database 테이블 생성"""
        cursor = conn.cursor()

        # 포트아웃 테이블
        cursor.execute(
            """
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PY_NP_TRMN_RMNY_TXN')
            CREATE TABLE PY_NP_TRMN_RMNY_TXN (
                NP_DIV_CD NVARCHAR(3),
                TRMN_NP_ADM_NO NVARCHAR(11) PRIMARY KEY,
                NP_TRMN_DATE DATE NOT NULL,
                CNCL_WTHD_DATE DATE,
                BCHNG_COMM_CMPN_ID NVARCHAR(50),
                ACHNG_COMM_CMPN_ID NVARCHAR(50),
                SVC_CONT_ID NVARCHAR(20),
                BILL_ACC_ID NVARCHAR(11),
                TEL_NO NVARCHAR(20),
                NP_TRMN_DTL_STTUS_VAL NVARCHAR(3),
                PAY_AMT DECIMAL(18,3),
                CREATED_AT DATETIME2 DEFAULT GETDATE()
            )
        """
        )

        # 포트인 테이블
        cursor.execute(
            """
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PY_NP_SBSC_RMNY_TXN')
            CREATE TABLE PY_NP_SBSC_RMNY_TXN (
                NP_DIV_CD NVARCHAR(3),
                NP_SBSC_RMNY_SEQ INT IDENTITY(1,1) PRIMARY KEY,
                TRT_DATE DATE NOT NULL,
                CNCL_DATE DATE,
                BCHNG_COMM_CMPN_ID NVARCHAR(50),
                ACHNG_COMM_CMPN_ID NVARCHAR(50),
                SVC_CONT_ID NVARCHAR(20),
                BILL_ACC_ID NVARCHAR(11),
                TEL_NO NVARCHAR(20),
                NP_STTUS_CD NVARCHAR(3),
                SETL_AMT DECIMAL(15,2),
                CREATED_AT DATETIME2 DEFAULT GETDATE()
            )
        """
        )

        # 예치금 테이블
        cursor.execute(
            """
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PY_DEPAZ_BAS')
            CREATE TABLE PY_DEPAZ_BAS (
                DEPAZ_SEQ INT IDENTITY(1,1) PRIMARY KEY,
                SVC_CONT_ID NVARCHAR(20),
                BILL_ACC_ID NVARCHAR(11),
                DEPAZ_DIV_CD NVARCHAR(3),
                RMNY_DATE DATE,
                RMNY_METH_CD NVARCHAR(5),
                DEPAZ_AMT DECIMAL(15,2),
                CREATED_AT DATETIME2 DEFAULT GETDATE()
            )
        """
        )

        conn.commit()
        self.logger.info("Azure SQL Database 테이블 생성 완료")

    def _create_sqlite_tables(self, conn):
        """SQLite 테이블 생성"""
        cursor = conn.cursor()

        # 포트아웃 테이블
        cursor.execute(
            """
            CREATE TABLE PY_NP_TRMN_RMNY_TXN (
                NP_DIV_CD VARCHAR(3),
                TRMN_NP_ADM_NO VARCHAR(11) PRIMARY KEY,
                NP_TRMN_DATE DATE NOT NULL,
                CNCL_WTHD_DATE DATE,
                BCHNG_COMM_CMPN_ID VARCHAR(50),
                ACHNG_COMM_CMPN_ID VARCHAR(50),
                SVC_CONT_ID VARCHAR(20),
                BILL_ACC_ID VARCHAR(11),
                TEL_NO VARCHAR(20),
                NP_TRMN_DTL_STTUS_VAL VARCHAR(3),
                PAY_AMT DECIMAL(18,3)
            )
        """
        )

        # 포트인 테이블
        cursor.execute(
            """
            CREATE TABLE PY_NP_SBSC_RMNY_TXN (
                NP_DIV_CD VARCHAR(3),
                NP_SBSC_RMNY_SEQ INTEGER PRIMARY KEY,
                TRT_DATE DATE NOT NULL,
                CNCL_DATE DATE,
                BCHNG_COMM_CMPN_ID VARCHAR(50),
                ACHNG_COMM_CMPN_ID VARCHAR(50),
                SVC_CONT_ID VARCHAR(20),
                BILL_ACC_ID VARCHAR(11),
                TEL_NO VARCHAR(20),
                NP_STTUS_CD VARCHAR(3),
                SETL_AMT DECIMAL(15,2)
            )
        """
        )

        # 예치금 테이블
        cursor.execute(
            """
            CREATE TABLE PY_DEPAZ_BAS (
                DEPAZ_SEQ INTEGER PRIMARY KEY,
                SVC_CONT_ID VARCHAR(20),
                BILL_ACC_ID VARCHAR(11),
                DEPAZ_DIV_CD VARCHAR(3),
                RMNY_DATE DATE,
                RMNY_METH_CD VARCHAR(5),
                DEPAZ_AMT DECIMAL(15,2)
            )
        """
        )

        conn.commit()
        self.logger.info("SQLite 테이블 생성 완료")

    def _generate_azure_data(self, conn):
        """Azure SQL Database 샘플 데이터 생성"""
        cursor = conn.cursor()
        operators = ["KT", "SKT", "LGU+", "KT MVNO", "SKT MVNO", "LGU+ MVNO"]

        # 최근 4개월 기간
        end_date = datetime.now()
        start_date = end_date - timedelta(days=120)

        # 포트아웃 데이터 생성
        for i in range(800):
            random_days = random.randint(0, 120)
            transaction_date = start_date + timedelta(days=random_days)

            from_operator = random.choice(operators)
            to_operator = random.choice([op for op in operators if op != from_operator])
            status = random.choice(["1", "2", "3"])

            cursor.execute(
                """
                INSERT INTO PY_NP_TRMN_RMNY_TXN 
                (NP_DIV_CD, TRMN_NP_ADM_NO, NP_TRMN_DATE, BCHNG_COMM_CMPN_ID, 
                 ACHNG_COMM_CMPN_ID, SVC_CONT_ID, BILL_ACC_ID, TEL_NO, 
                 NP_TRMN_DTL_STTUS_VAL, PAY_AMT)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    "OUT",
                    f"AT{i:07d}",
                    transaction_date.strftime("%Y-%m-%d"),
                    from_operator,
                    to_operator,
                    f"{i+1:020d}",
                    f"{i+1:011d}",
                    f"010{random.randint(1000,9999)}{random.randint(1000,9999)}",
                    status,
                    random.randint(1000, 500000),
                ),
            )

        # 포트인 데이터 생성
        for i in range(1000):
            random_days = random.randint(0, 120)
            transaction_date = start_date + timedelta(days=random_days)

            from_operator = random.choice(operators)
            to_operator = random.choice([op for op in operators if op != from_operator])
            status = random.choice(["OK", "CN", "WD"])

            cursor.execute(
                """
                INSERT INTO PY_NP_SBSC_RMNY_TXN 
                (NP_DIV_CD, TRT_DATE, BCHNG_COMM_CMPN_ID, ACHNG_COMM_CMPN_ID, 
                 SVC_CONT_ID, BILL_ACC_ID, TEL_NO, NP_STTUS_CD, SETL_AMT)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    "IN",
                    transaction_date.strftime("%Y-%m-%d"),
                    from_operator,
                    to_operator,
                    f"{i+1:020d}",
                    f"{i+1:011d}",
                    f"010{random.randint(1000,9999)}{random.randint(1000,9999)}",
                    status,
                    random.randint(1000, 500000),
                ),
            )

        # 예치금 데이터 생성
        for i in range(600):
            random_days = random.randint(0, 120)
            transaction_date = start_date + timedelta(days=random_days)

            cursor.execute(
                """
                INSERT INTO PY_DEPAZ_BAS 
                (SVC_CONT_ID, BILL_ACC_ID, DEPAZ_DIV_CD, RMNY_DATE, RMNY_METH_CD, DEPAZ_AMT)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    f"{i+1:020d}",
                    f"{i+1:011d}",
                    random.choice(["10", "90"]),
                    transaction_date.strftime("%Y-%m-%d"),
                    random.choice(["NA", "CA"]),
                    random.randint(1000, 500000),
                ),
            )

        conn.commit()
        self.logger.info("Azure SQL Database 샘플 데이터 생성 완료")

    def _generate_sqlite_data(self, conn):
        """SQLite 샘플 데이터 생성"""
        operators = ["KT", "SKT", "LGU+", "KT MVNO", "SKT MVNO", "LGU+ MVNO"]

        # 최근 4개월 기간
        end_date = datetime.now()
        start_date = end_date - timedelta(days=120)

        # 포트아웃 데이터
        port_out_data = []
        for i in range(800):
            random_days = random.randint(0, 120)
            transaction_date = start_date + timedelta(days=random_days)

            from_operator = random.choice(operators)
            to_operator = random.choice([op for op in operators if op != from_operator])
            status = random.choice(["1", "2", "3"])

            cncl_date = None
            if status == "2":
                cncl_date = transaction_date.strftime("%Y-%m-%d")
            elif status == "3":
                cncl_date = (
                    transaction_date + timedelta(days=random.randint(1, 15))
                ).strftime("%Y-%m-%d")

            port_out_data.append(
                (
                    "OUT",
                    f"T{i+1:07d}",
                    transaction_date.strftime("%Y-%m-%d"),
                    cncl_date,
                    from_operator,
                    to_operator,
                    f"{i+1:020d}",
                    f"{i+1:011d}",
                    f"010{random.randint(1000,9999)}{random.randint(1000,9999)}",
                    status,
                    random.randint(1000, 500000),
                )
            )

        conn.executemany(
            """
            INSERT INTO PY_NP_TRMN_RMNY_TXN 
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
            port_out_data,
        )

        # 포트인 데이터
        port_in_data = []
        for i in range(1000):
            random_days = random.randint(0, 120)
            transaction_date = start_date + timedelta(days=random_days)

            from_operator = random.choice(operators)
            to_operator = random.choice([op for op in operators if op != from_operator])
            status = random.choice(["OK", "CN", "WD"])

            cncl_date = None
            if status == "CN":
                cncl_date = transaction_date.strftime("%Y-%m-%d")
            elif status == "WD":
                cncl_date = (
                    transaction_date + timedelta(days=random.randint(1, 15))
                ).strftime("%Y-%m-%d")

            port_in_data.append(
                (
                    "IN",
                    i + 1,
                    transaction_date.strftime("%Y-%m-%d"),
                    cncl_date,
                    from_operator,
                    to_operator,
                    f"{i+1:020d}",
                    f"{i+1:011d}",
                    f"010{random.randint(1000,9999)}{random.randint(1000,9999)}",
                    status,
                    random.randint(1000, 500000),
                )
            )

        conn.executemany(
            """
            INSERT INTO PY_NP_SBSC_RMNY_TXN 
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
            port_in_data,
        )

        # 예치금 데이터
        deposit_data = []
        for i in range(600):
            random_days = random.randint(0, 120)
            transaction_date = start_date + timedelta(days=random_days)

            deposit_data.append(
                (
                    i + 1,
                    f"{i+1:020d}",
                    f"{i+1:011d}",
                    random.choice(["10", "90"]),
                    transaction_date.strftime("%Y-%m-%d"),
                    random.choice(["NA", "CA"]),
                    random.randint(1000, 500000),
                )
            )

        conn.executemany(
            """
            INSERT INTO PY_DEPAZ_BAS 
            VALUES (?,?,?,?,?,?,?)
        """,
            deposit_data,
        )

        conn.commit()
        self.logger.info(
            f"SQLite 샘플 데이터 생성 완료: 포트아웃 {len(port_out_data)}건, 포트인 {len(port_in_data)}건, 예치금 {len(deposit_data)}건"
        )

    def is_using_azure(self) -> bool:
        """Azure 사용 여부 반환"""
        return self.use_azure

    def get_connection_info(self) -> Dict[str, Any]:
        """연결 정보 반환"""
        return {
            "type": "Azure SQL Database" if self.use_azure else "SQLite",
            "azure_ready": (
                self.azure_config.is_production_ready() if self.azure_config else False
            ),
            "force_local": self.force_local,
        }

    def get_sample_statistics(self, conn):
        """샘플 데이터 통계 조회"""
        stats = {}

        try:
            # 포트아웃 통계
            port_out_query = """
                SELECT 
                    COUNT(*) as total_count,
                    SUM(PAY_AMT) as total_amount,
                    AVG(PAY_AMT) as avg_amount
                FROM PY_NP_TRMN_RMNY_TXN
                WHERE NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
            """
            port_out_df = pd.read_sql_query(port_out_query, conn)
            stats["port_out"] = port_out_df.iloc[0].to_dict()

            # 포트인 통계
            port_in_query = """
                SELECT 
                    COUNT(*) as total_count,
                    SUM(SETL_AMT) as total_amount,
                    AVG(SETL_AMT) as avg_amount
                FROM PY_NP_SBSC_RMNY_TXN
                WHERE NP_STTUS_CD IN ('OK', 'WD')
            """
            port_in_df = pd.read_sql_query(port_in_query, conn)
            stats["port_in"] = port_in_df.iloc[0].to_dict()

            # 예치금 통계
            deposit_query = """
                SELECT 
                    COUNT(*) as total_count,
                    SUM(DEPAZ_AMT) as total_amount,
                    AVG(DEPAZ_AMT) as avg_amount
                FROM PY_DEPAZ_BAS
                WHERE DEPAZ_DIV_CD = '10'
            """
            deposit_df = pd.read_sql_query(deposit_query, conn)
            stats["deposit"] = deposit_df.iloc[0].to_dict()

            return stats

        except Exception as e:
            self.logger.error(f"통계 조회 실패: {e}")
            return {}

    def cleanup_sample_data(self, conn):
        """샘플 데이터 정리 (Azure만 해당)"""
        if not self.use_azure:
            self.logger.info("SQLite는 메모리 기반이므로 정리가 불필요합니다")
            return

        try:
            cursor = conn.cursor()

            # Azure SQL에서 샘플 데이터 삭제
            cursor.execute(
                "DELETE FROM PY_NP_TRMN_RMNY_TXN WHERE CREATED_AT >= DATEADD(day, -1, GETDATE())"
            )
            cursor.execute(
                "DELETE FROM PY_NP_SBSC_RMNY_TXN WHERE CREATED_AT >= DATEADD(day, -1, GETDATE())"
            )
            cursor.execute(
                "DELETE FROM PY_DEPAZ_BAS WHERE CREATED_AT >= DATEADD(day, -1, GETDATE())"
            )

            conn.commit()
            self.logger.info("Azure SQL Database 샘플 데이터 정리 완료")

        except Exception as e:
            self.logger.error(f"샘플 데이터 정리 실패: {e}")


# 기존 함수와의 호환성을 위한 래퍼 함수
def create_sample_database(azure_config=None, force_local: bool = False):
    """
    샘플 데이터베이스 생성 (기존 함수와 호환)

    Args:
        azure_config: Azure 설정 (None이면 로컬 모드)
        force_local: 강제로 로컬 SQLite 사용

    Returns:
        데이터베이스 연결 객체
    """
    manager = SampleDataManager(azure_config, force_local)
    return manager.create_sample_database()


def get_sample_statistics(conn):
    """샘플 데이터 통계 조회 (기존 함수와 호환)"""
    try:
        print("\n📊 샘플 데이터 통계:")
        print("=" * 50)

        # 포트아웃 통계
        port_out_query = """
            SELECT 
                COUNT(*) as total_count,
                SUM(PAY_AMT) as total_amount,
                AVG(PAY_AMT) as avg_amount
            FROM PY_NP_TRMN_RMNY_TXN
            WHERE NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
        """
        port_out_df = pd.read_sql_query(port_out_query, conn)

        print("📤 포트아웃 현황:")
        print(f"   총 건수: {port_out_df.iloc[0]['total_count']:,}건")
        print(f"   총 정산액: {port_out_df.iloc[0]['total_amount']:,.0f}원")
        print(f"   평균 정산액: {port_out_df.iloc[0]['avg_amount']:,.0f}원")

        # 포트인 통계
        port_in_query = """
            SELECT 
                COUNT(*) as total_count,
                SUM(SETL_AMT) as total_amount,
                AVG(SETL_AMT) as avg_amount
            FROM PY_NP_SBSC_RMNY_TXN
            WHERE NP_STTUS_CD IN ('OK', 'WD')
        """
        port_in_df = pd.read_sql_query(port_in_query, conn)

        print("\n📥 포트인 현황:")
        print(f"   총 건수: {port_in_df.iloc[0]['total_count']:,}건")
        print(f"   총 정산액: {port_in_df.iloc[0]['total_amount']:,.0f}원")
        print(f"   평균 정산액: {port_in_df.iloc[0]['avg_amount']:,.0f}원")

        # 예치금 통계
        deposit_query = """
            SELECT 
                COUNT(*) as total_count,
                SUM(DEPAZ_AMT) as total_amount,
                AVG(DEPAZ_AMT) as avg_amount
            FROM PY_DEPAZ_BAS
            WHERE DEPAZ_DIV_CD = '10'
        """
        deposit_df = pd.read_sql_query(deposit_query, conn)

        print("\n💰 예치금 현황:")
        print(f"   총 건수: {deposit_df.iloc[0]['total_count']:,}건")
        print(f"   총 예치금: {deposit_df.iloc[0]['total_amount']:,.0f}원")
        print(f"   평균 예치금: {deposit_df.iloc[0]['avg_amount']:,.0f}원")

        print("=" * 50)

    except Exception as e:
        print(f"통계 조회 실패: {e}")


# 테스트 함수
def test_sample_data_manager():
    """샘플 데이터 매니저 테스트"""
    print("🧪 샘플 데이터 매니저 테스트를 시작합니다...")

    try:
        # Azure 설정 로드 시도
        try:
            from azure_config import get_azure_config

            azure_config = get_azure_config()
            print(f"Azure 설정 로드 성공")
        except Exception as e:
            print(f"Azure 설정 로드 실패: {e}")
            azure_config = None

        # 1. Azure 모드 테스트 (가능한 경우)
        if azure_config and azure_config.is_production_ready():
            print("\n☁️ Azure SQL Database 모드 테스트:")
            try:
                azure_manager = SampleDataManager(azure_config, force_local=False)
                azure_conn = azure_manager.create_sample_database()

                connection_info = azure_manager.get_connection_info()
                print(f"   연결 타입: {connection_info['type']}")

                # 통계 확인
                stats = azure_manager.get_sample_statistics(azure_conn)
                if stats:
                    print("   📊 Azure 데이터 통계:")
                    for data_type, stat in stats.items():
                        count = stat.get("total_count", 0)
                        amount = stat.get("total_amount", 0)
                        print(f"     {data_type}: {count:,}건, {amount:,.0f}원")

                azure_conn.close()
                print("   ✅ Azure 모드 테스트 성공")
            except Exception as e:
                print(f"   ❌ Azure 모드 테스트 실패: {e}")

        # 2. 로컬 모드 테스트
        print("\n💻 로컬 SQLite 모드 테스트:")
        local_manager = SampleDataManager(azure_config, force_local=True)
        local_conn = local_manager.create_sample_database()

        connection_info = local_manager.get_connection_info()
        print(f"   연결 타입: {connection_info['type']}")

        # 통계 확인
        stats = local_manager.get_sample_statistics(local_conn)
        if stats:
            print("   📊 로컬 데이터 통계:")
            for data_type, stat in stats.items():
                count = stat.get("total_count", 0)
                amount = stat.get("total_amount", 0)
                print(f"     {data_type}: {count:,}건, {amount:,.0f}원")

        # 3. 호환성 테스트
        print("\n🔄 기존 함수 호환성 테스트:")
        compat_conn = create_sample_database(azure_config, force_local=True)
        get_sample_statistics(compat_conn)

        print("\n✅ 모든 테스트 완료!")

    except Exception as e:
        print(f"\n❌ 테스트 실패: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_sample_data_manager()
