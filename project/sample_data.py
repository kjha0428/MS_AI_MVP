# sample_data.py - Azure SQL Database 연동 샘플 데이터 관리
import sqlite3
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime, timedelta
import random
import logging
from typing import Optional, Dict, Any, Tuple
from azure_config import AzureConfig


class SampleDataManager:
    """샘플 데이터 관리 클래스 - SQLite와 Azure SQL Database 지원"""

    def __init__(self, azure_config: AzureConfig, force_local: bool = False):
        """
        샘플 데이터 매니저 초기화

        Args:
            azure_config: Azure 설정 객체
            force_local: 강제로 로컬 SQLite 사용
        """
        self.azure_config = azure_config
        self.force_local = force_local
        self.logger = logging.getLogger(__name__)

        # Azure 연결 가능 여부 확인
        self.use_azure = not force_local and azure_config.is_production_ready()

        # 연결 정보
        self.connection_string = None
        self.sqlite_conn = None

        if self.use_azure:
            self.connection_string = azure_config.get_database_connection_string()
            self.logger.info("Azure SQL Database 모드로 초기화")
        else:
            self.logger.info("로컬 SQLite 모드로 초기화")

    def get_connection(self):
        """데이터베이스 연결 반환"""
        if self.use_azure:
            return pyodbc.connect(self.connection_string, timeout=30)
        else:
            if self.sqlite_conn is None:
                self.sqlite_conn = sqlite3.connect(":memory:", check_same_thread=False)
            return self.sqlite_conn

    def create_sample_database(self):
        """
        샘플 데이터베이스 생성 및 데이터 삽입
        Azure SQL Database 연결시에는 기존 데이터 확인 후 생성
        """
        try:
            if self.use_azure:
                return self._handle_azure_sample_data()
            else:
                return self._create_local_sample_data()

        except Exception as e:
            self.logger.error(f"샘플 데이터베이스 생성 실패: {e}")
            # Azure 실패시 로컬로 폴백
            if self.use_azure:
                self.logger.warning("Azure 연결 실패, 로컬 SQLite로 전환")
                self.use_azure = False
                return self._create_local_sample_data()
            raise e

    def _handle_azure_sample_data(self):
        """Azure SQL Database 샘플 데이터 처리"""
        conn = self.get_connection()

        try:
            # 1. 테이블 존재 여부 확인
            if not self._check_tables_exist(conn):
                self.logger.info("Azure SQL Database에 테이블 생성 중...")
                self._create_azure_tables(conn)

            # 2. 기존 데이터 확인
            data_stats = self._check_existing_data(conn)

            # 3. 데이터가 충분히 있으면 그대로 사용
            if self._is_sufficient_data(data_stats):
                self.logger.info("충분한 샘플 데이터가 이미 존재합니다")
                self._log_data_statistics(data_stats)
                return conn

            # 4. 데이터가 부족하면 추가 생성
            self.logger.info("샘플 데이터 추가 생성 중...")
            self._generate_azure_sample_data(conn, data_stats)

            # 5. 최종 통계 출력
            final_stats = self._check_existing_data(conn)
            self._log_data_statistics(final_stats)

            return conn

        except Exception as e:
            self.logger.error(f"Azure 샘플 데이터 처리 실패: {e}")
            conn.close()
            raise e

    def _create_local_sample_data(self):
        """로컬 SQLite 샘플 데이터 생성"""
        conn = self.get_connection()

        # 테이블 생성
        self._create_sqlite_tables(conn)

        # 샘플 데이터 생성
        self._generate_sample_data(conn)

        self.logger.info("✅ 로컬 샘플 데이터베이스가 성공적으로 생성되었습니다!")
        return conn

    def _check_tables_exist(self, conn) -> bool:
        """Azure SQL Database에서 테이블 존재 여부 확인"""
        try:
            cursor = conn.cursor()

            # 시스템 테이블에서 확인
            check_query = """
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME IN (
                'PY_NP_TRMN_RMNY_TXN', 
                'PY_NP_SBSC_RMNY_TXN', 
                'PY_DEPAZ_BAS'
            )
            """

            cursor.execute(check_query)
            result = cursor.fetchone()
            table_count = result[0] if result else 0

            self.logger.info(f"Azure SQL Database에서 {table_count}/3개 테이블 발견")
            return table_count == 3

        except Exception as e:
            self.logger.error(f"테이블 존재 확인 실패: {e}")
            return False

    def _check_existing_data(self, conn) -> Dict[str, int]:
        """기존 데이터 현황 확인"""
        data_stats = {
            "PY_NP_TRMN_RMNY_TXN": 0,
            "PY_NP_SBSC_RMNY_TXN": 0,
            "PY_DEPAZ_BAS": 0,
        }

        try:
            cursor = conn.cursor()

            for table_name in data_stats.keys():
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    result = cursor.fetchone()
                    data_stats[table_name] = result[0] if result else 0
                except Exception as e:
                    self.logger.warning(f"테이블 {table_name} 데이터 확인 실패: {e}")
                    data_stats[table_name] = 0

            return data_stats

        except Exception as e:
            self.logger.error(f"기존 데이터 확인 실패: {e}")
            return data_stats

    def _is_sufficient_data(self, data_stats: Dict[str, int]) -> bool:
        """충분한 데이터가 있는지 확인"""
        min_requirements = {
            "PY_NP_TRMN_RMNY_TXN": 500,  # 최소 500건
            "PY_NP_SBSC_RMNY_TXN": 600,  # 최소 600건
            "PY_DEPAZ_BAS": 500,  # 최소 500건
        }

        for table_name, min_count in min_requirements.items():
            if data_stats.get(table_name, 0) < min_count:
                self.logger.info(
                    f"{table_name}: {data_stats.get(table_name, 0)}건 (최소 {min_count}건 필요)"
                )
                return False

        return True

    def _create_azure_tables(self, conn):
        """Azure SQL Database 테이블 생성"""
        cursor = conn.cursor()

        # 1. 해지번호이동 정산 테이블 (포트아웃)
        cursor.execute(
            """
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PY_NP_TRMN_RMNY_TXN')
            CREATE TABLE PY_NP_TRMN_RMNY_TXN (
                NP_DIV_CD NVARCHAR(3),
                TRMN_NP_ADM_NO NVARCHAR(11) PRIMARY KEY,
                NP_TRMN_DATE DATE NOT NULL,
                CNCL_WTHD_DATE DATE,
                BCHNG_COMM_CMPN_ID NVARCHAR(11),
                ACHNG_COMM_CMPN_ID NVARCHAR(11),
                SVC_CONT_ID NVARCHAR(20),
                BILL_ACC_ID NVARCHAR(11),
                TEL_NO NVARCHAR(20),
                NP_TRMN_DTL_STTUS_VAL NVARCHAR(3),
                PAY_AMT DECIMAL(18,3),
                CREATED_AT DATETIME2 DEFAULT GETDATE(),
                IS_SAMPLE_DATA BIT DEFAULT 1
            )
        """
        )

        # 2. 가입번호이동 정산 테이블 (포트인)
        cursor.execute(
            """
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PY_NP_SBSC_RMNY_TXN')
            CREATE TABLE PY_NP_SBSC_RMNY_TXN (
                NP_DIV_CD NVARCHAR(3),
                NP_SBSC_RMNY_SEQ INT IDENTITY(1,1) PRIMARY KEY,
                TRT_DATE DATE NOT NULL,
                CNCL_DATE DATE,
                BCHNG_COMM_CMPN_ID NVARCHAR(11),
                ACHNG_COMM_CMPN_ID NVARCHAR(11),
                SVC_CONT_ID NVARCHAR(20),
                BILL_ACC_ID NVARCHAR(11),
                TEL_NO NVARCHAR(20),
                NP_STTUS_CD NVARCHAR(3),
                SETL_AMT DECIMAL(15,2),
                CREATED_AT DATETIME2 DEFAULT GETDATE(),
                IS_SAMPLE_DATA BIT DEFAULT 1
            )
        """
        )

        # 3. 예치금 기본 테이블
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
                CREATED_AT DATETIME2 DEFAULT GETDATE(),
                IS_SAMPLE_DATA BIT DEFAULT 1
            )
        """
        )

        # 인덱스 생성
        self._create_azure_indexes(cursor)

        conn.commit()
        self.logger.info("Azure SQL Database 테이블 생성 완료")

    def _create_azure_indexes(self, cursor):
        """Azure SQL Database 인덱스 생성"""
        indexes = [
            "CREATE NONCLUSTERED INDEX IX_PY_NP_TRMN_RMNY_TXN_DATE ON PY_NP_TRMN_RMNY_TXN(NP_TRMN_DATE)",
            "CREATE NONCLUSTERED INDEX IX_PY_NP_TRMN_RMNY_TXN_OPERATOR ON PY_NP_TRMN_RMNY_TXN(BCHNG_COMM_CMPN_ID)",
            "CREATE NONCLUSTERED INDEX IX_PY_NP_SBSC_RMNY_TXN_DATE ON PY_NP_SBSC_RMNY_TXN(TRT_DATE)",
            "CREATE NONCLUSTERED INDEX IX_PY_NP_SBSC_RMNY_TXN_OPERATOR ON PY_NP_SBSC_RMNY_TXN(BCHNG_COMM_CMPN_ID)",
            "CREATE NONCLUSTERED INDEX IX_PY_DEPAZ_BAS_DATE ON PY_DEPAZ_BAS(RMNY_DATE)",
        ]

        for index_sql in indexes:
            try:
                cursor.execute(
                    f"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '{index_sql.split()[-1].split('(')[0]}') {index_sql}"
                )
            except Exception as e:
                self.logger.warning(f"인덱스 생성 실패: {e}")

    def _create_sqlite_tables(self, conn):
        """SQLite 테이블 생성"""
        # 1. 해지번호이동 정산 테이블 (포트아웃)
        conn.execute(
            """
            CREATE TABLE PY_NP_TRMN_RMNY_TXN (
                NP_DIV_CD VARCHAR(3),
                TRMN_NP_ADM_NO VARCHAR(11) PRIMARY KEY,
                NP_TRMN_DATE DATE NOT NULL,
                CNCL_WTHD_DATE DATE,
                BCHNG_COMM_CMPN_ID VARCHAR(11),
                ACHNG_COMM_CMPN_ID VARCHAR(11),
                SVC_CONT_ID VARCHAR(20),
                BILL_ACC_ID VARCHAR(11),
                TEL_NO VARCHAR(20),
                NP_TRMN_DTL_STTUS_VAL VARCHAR(3),
                PAY_AMT DECIMAL(18,3)
            )
        """
        )

        # 2. 가입번호이동 정산 테이블 (포트인)
        conn.execute(
            """
            CREATE TABLE PY_NP_SBSC_RMNY_TXN (
                NP_DIV_CD VARCHAR(3),
                NP_SBSC_RMNY_SEQ INTEGER PRIMARY KEY,
                TRT_DATE DATE NOT NULL,
                CNCL_DATE DATE,
                BCHNG_COMM_CMPN_ID VARCHAR(11),
                ACHNG_COMM_CMPN_ID VARCHAR(11),
                SVC_CONT_ID VARCHAR(20),
                BILL_ACC_ID VARCHAR(11),
                TEL_NO VARCHAR(20),
                NP_STTUS_CD VARCHAR(3),
                SETL_AMT DECIMAL(15,2)
            )
        """
        )

        # 3. 예치금 기본 테이블
        conn.execute(
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

    def _generate_azure_sample_data(self, conn, existing_stats: Dict[str, int]):
        """Azure SQL Database용 샘플 데이터 생성"""
        cursor = conn.cursor()

        # 통신사 정보
        operators = {
            "C001": "KT",
            "C002": "SKT",
            "C003": "LGU+",
            "C004": "KT MVNO",
            "C005": "SKT MVNO",
            "C006": "LGU+ MVNO",
        }

        # 최근 4개월 기간 설정
        end_date = datetime.now()
        start_date = end_date - timedelta(days=120)

        # 필요한 데이터 양 계산
        target_counts = {
            "PY_NP_TRMN_RMNY_TXN": 1000,
            "PY_NP_SBSC_RMNY_TXN": 1200,
            "PY_DEPAZ_BAS": 1000,
        }

        # 1. 포트아웃 데이터 생성
        port_out_needed = max(
            0,
            target_counts["PY_NP_TRMN_RMNY_TXN"]
            - existing_stats["PY_NP_TRMN_RMNY_TXN"],
        )
        if port_out_needed > 0:
            self._generate_azure_port_out_data(
                cursor, operators, start_date, end_date, port_out_needed
            )

        # 2. 포트인 데이터 생성
        port_in_needed = max(
            0,
            target_counts["PY_NP_SBSC_RMNY_TXN"]
            - existing_stats["PY_NP_SBSC_RMNY_TXN"],
        )
        if port_in_needed > 0:
            self._generate_azure_port_in_data(
                cursor, operators, start_date, end_date, port_in_needed
            )

        # 3. 예치금 데이터 생성
        deposit_needed = max(
            0, target_counts["PY_DEPAZ_BAS"] - existing_stats["PY_DEPAZ_BAS"]
        )
        if deposit_needed > 0:
            self._generate_azure_deposit_data(
                cursor, start_date, end_date, deposit_needed
            )

        conn.commit()
        self.logger.info("Azure SQL Database 샘플 데이터 생성 완료")

    def _generate_azure_port_out_data(
        self, cursor, operators, start_date, end_date, count
    ):
        """Azure SQL Database 포트아웃 데이터 생성"""
        batch_size = 100
        total_batches = (count + batch_size - 1) // batch_size

        for batch in range(total_batches):
            batch_data = []
            current_batch_size = min(batch_size, count - batch * batch_size)

            for i in range(current_batch_size):
                # 랜덤 날짜 생성
                random_days = random.randint(0, (end_date - start_date).days)
                transaction_date = start_date + timedelta(days=random_days)

                # 통신사 선택
                from_operator = random.choice(list(operators.values()))
                to_operator = random.choice(
                    [k for k in operators.values() if k != from_operator]
                )

                # 상태 코드
                status_val = random.choice(["1", "2", "3"])
                trmn_date = transaction_date.strftime("%Y-%m-%d")

                if status_val == "1":
                    cncl_date = None
                elif status_val == "2":
                    cncl_date = trmn_date
                else:
                    random_days = random.randint(1, 15)
                    cncl_date = (
                        transaction_date + timedelta(days=random_days)
                    ).strftime("%Y-%m-%d")

                pay_amount = random.randint(10, 1000000)

                batch_data.append(
                    (
                        "OUT",
                        f"AT{batch:04d}{i:04d}",  # 고유 ID
                        trmn_date,
                        cncl_date,
                        from_operator,
                        to_operator,
                        f"{(batch * batch_size + i + 1):020d}",
                        f"{(batch * batch_size + i + 1):011d}",
                        f"010{random.randint(1000,9999)}{random.randint(1000,9999)}",
                        status_val,
                        pay_amount,
                    )
                )

            # 배치 삽입
            cursor.executemany(
                """
                INSERT INTO PY_NP_TRMN_RMNY_TXN 
                (NP_DIV_CD, TRMN_NP_ADM_NO, NP_TRMN_DATE, CNCL_WTHD_DATE, 
                 BCHNG_COMM_CMPN_ID, ACHNG_COMM_CMPN_ID, SVC_CONT_ID, BILL_ACC_ID, 
                 TEL_NO, NP_TRMN_DTL_STTUS_VAL, PAY_AMT)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
                batch_data,
            )

        self.logger.info(f"포트아웃 데이터 {count}건 생성 완료")

    def _generate_azure_port_in_data(
        self, cursor, operators, start_date, end_date, count
    ):
        """Azure SQL Database 포트인 데이터 생성"""
        batch_size = 100
        total_batches = (count + batch_size - 1) // batch_size

        for batch in range(total_batches):
            batch_data = []
            current_batch_size = min(batch_size, count - batch * batch_size)

            for i in range(current_batch_size):
                # 랜덤 날짜 생성
                random_days = random.randint(0, (end_date - start_date).days)
                transaction_date = start_date + timedelta(days=random_days)

                # 통신사 선택
                from_operator = random.choice(list(operators.values()))
                to_operator = random.choice(
                    [k for k in operators.values() if k != from_operator]
                )

                # 상태 코드
                status_cd = random.choice(["OK", "CN", "WD"])
                trt_date = transaction_date.strftime("%Y-%m-%d")

                if status_cd == "OK":
                    cncl_date = None
                elif status_cd == "CN":
                    cncl_date = trt_date
                else:
                    random_days = random.randint(1, 15)
                    cncl_date = (
                        transaction_date + timedelta(days=random_days)
                    ).strftime("%Y-%m-%d")

                settlement_amount = random.randint(10, 1000000)

                batch_data.append(
                    (
                        "IN",
                        trt_date,
                        cncl_date,
                        from_operator,
                        to_operator,
                        f"{(batch * batch_size + i + 1):020d}",
                        f"{(batch * batch_size + i + 1):011d}",
                        f"010{random.randint(1000,9999)}{random.randint(1000,9999)}",
                        status_cd,
                        settlement_amount,
                    )
                )

            # 배치 삽입
            cursor.executemany(
                """
                INSERT INTO PY_NP_SBSC_RMNY_TXN 
                (NP_DIV_CD, TRT_DATE, CNCL_DATE, BCHNG_COMM_CMPN_ID, ACHNG_COMM_CMPN_ID, 
                 SVC_CONT_ID, BILL_ACC_ID, TEL_NO, NP_STTUS_CD, SETL_AMT)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
                batch_data,
            )

        self.logger.info(f"포트인 데이터 {count}건 생성 완료")

    def _generate_azure_deposit_data(self, cursor, start_date, end_date, count):
        """Azure SQL Database 예치금 데이터 생성"""
        batch_size = 100
        total_batches = (count + batch_size - 1) // batch_size

        for batch in range(total_batches):
            batch_data = []
            current_batch_size = min(batch_size, count - batch * batch_size)

            for i in range(current_batch_size):
                random_days = random.randint(0, (end_date - start_date).days)
                transaction_date = start_date + timedelta(days=random_days)

                batch_data.append(
                    (
                        f"{(batch * batch_size + i + 1):020d}",
                        f"{(batch * batch_size + i + 1):011d}",
                        random.choice(["10", "90"]),
                        transaction_date.strftime("%Y-%m-%d"),
                        random.choice(["NA", "CA"]),
                        random.randint(10, 1000000),
                    )
                )

            # 배치 삽입
            cursor.executemany(
                """
                INSERT INTO PY_DEPAZ_BAS 
                (SVC_CONT_ID, BILL_ACC_ID, DEPAZ_DIV_CD, RMNY_DATE, RMNY_METH_CD, DEPAZ_AMT)
                VALUES (?,?,?,?,?,?)
            """,
                batch_data,
            )

        self.logger.info(f"예치금 데이터 {count}건 생성 완료")

    def _generate_sample_data(self, conn):
        """SQLite용 샘플 데이터 생성 (기존 로직)"""
        # 통신사 정보
        operators = {
            "C001": "KT",
            "C002": "SKT",
            "C003": "LGU+",
            "C004": "KT MVNO",
            "C005": "SKT MVNO",
            "C006": "LGU+ MVNO",
        }

        # 최근 4개월 기간 설정
        end_date = datetime.now()
        start_date = end_date - timedelta(days=120)

        self.logger.info("📊 SQLite 샘플 데이터를 생성 중입니다...")

        # 1. 해지번호이동 데이터 (포트아웃) 생성
        self._generate_sqlite_port_out_data(conn, operators, start_date, end_date)

        # 2. 가입번호이동 데이터 (포트인) 생성
        self._generate_sqlite_port_in_data(conn, operators, start_date, end_date)

        self.logger.info("✨ SQLite 샘플 데이터 생성 완료!")

    def _generate_sqlite_port_out_data(self, conn, operators, start_date, end_date):
        """SQLite 포트아웃 데이터 생성"""
        port_out_data = []
        deposit_data = []

        # 1000건의 포트아웃 데이터 생성
        for i in range(1000):
            # 랜덤 날짜 생성
            random_days = random.randint(0, (end_date - start_date).days)
            transaction_date = start_date + timedelta(days=random_days)

            # 통신사 선택 (변경전/변경후)
            from_operator_code = random.choice(list(operators.values()))
            to_operator_code = random.choice(
                [k for k in operators.values() if k != from_operator_code]
            )

            # 번호이동 상태 코드에 따른 cncl_wthd_date 설정
            np_trmn_dtl_sttus_val = random.choice(["1", "2", "3"])
            np_trmn_date = transaction_date.strftime("%Y-%m-%d")

            if np_trmn_dtl_sttus_val == "1":
                cncl_wthd_date = None  # NULL
            elif np_trmn_dtl_sttus_val == "2":
                cncl_wthd_date = np_trmn_date  # NP_TRMN_DATE 동일
            else:  # WD
                # CNCL_WTHD_DATE 이후 1~15일 랜덤 날짜
                random_days = random.randint(1, 15)
                cncl_wthd_date = (
                    transaction_date + timedelta(days=random_days)
                ).strftime("%Y-%m-%d")

            svc_cont_id = f"{i+1:020d}"
            bill_acc_id = f"{i+1:011d}"
            tel_no = f"010{random.randint(1000,9999)}{random.randint(1000,9999)}"
            pay_amount = random.randint(10, 1000000)

            port_out_data.append(
                (
                    "OUT",  # NP_DIV_CD
                    f"T{i+1:07d}",  # TRMN_NP_ADM_NO
                    np_trmn_date,  # NP_TRMN_DATE
                    cncl_wthd_date,  # CNCL_WTHD_DATE
                    from_operator_code,  # BCHNG_COMM_CMPN_ID
                    to_operator_code,  # ACHNG_COMM_CMPN_ID
                    svc_cont_id,  # SVC_CONT_ID
                    bill_acc_id,  # BILL_ACC_ID
                    tel_no,  # TEL_NO
                    np_trmn_dtl_sttus_val,  # NP_TRMN_DTL_STTUS_VAL
                    pay_amount,  # PAY_AMT
                )
            )

            deposit_data.append(
                (
                    i + 1,  # DEPAZ_SEQ
                    svc_cont_id,  # SVC_CONT_ID
                    bill_acc_id,  # BILL_ACC_ID
                    random.choice(["10", "90"]),  # DEPAZ_DIV_CD
                    np_trmn_date,  # RMNY_DATE
                    random.choice(["NA", "CA"]),  # RMNY_METH_CD
                    pay_amount,  # DEPAZ_AMT
                )
            )

        # PY_NP_TRMN_RMNY_TXN 테이블에 포트아웃 데이터 삽입
        conn.executemany(
            """
            INSERT INTO PY_NP_TRMN_RMNY_TXN 
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
            port_out_data,
        )

        # PY_DEPAZ_BAS 테이블에 예치금 데이터 삽입
        conn.executemany(
            """
            INSERT INTO PY_DEPAZ_BAS 
            VALUES (?,?,?,?,?,?,?)
        """,
            deposit_data,
        )

        conn.commit()
        self.logger.info(f"📤 포트아웃 데이터 {len(port_out_data)}건 생성 완료")
        self.logger.info(f"💰 예치금 데이터 {len(deposit_data)}건 생성 완료")

    def _generate_sqlite_port_in_data(self, conn, operators, start_date, end_date):
        """SQLite 포트인 데이터 생성"""
        port_in_data = []

        # 1200건의 포트인 데이터 생성
        for i in range(1200):
            # 랜덤 날짜 생성 (최근으로 갈수록 증가 추세)
            random_days = random.randint(0, (end_date - start_date).days)
            transaction_date = start_date + timedelta(days=random_days)

            # 통신사 선택
            from_operator_code = random.choice(list(operators.values()))
            to_operator_code = random.choice(
                [k for k in operators.values() if k != from_operator_code]
            )

            # 번호이동 상태 코드에 따른 cncl_date 설정
            np_sttus_cd = random.choice(["OK", "CN", "WD"])
            trt_date = transaction_date.strftime("%Y-%m-%d")

            if np_sttus_cd == "OK":
                cncl_date = None  # NULL
            elif np_sttus_cd == "CN":
                cncl_date = trt_date  # TRT_DATE 동일
            else:  # WD
                # CNCL_DATE 이후 1~15일 랜덤 날짜
                random_days = random.randint(1, 15)
                cncl_date = (transaction_date + timedelta(days=random_days)).strftime(
                    "%Y-%m-%d"
                )

            settlement_amount = random.randint(10, 1000000)

            port_in_data.append(
                (
                    "IN",  # NP_DIV_CD,
                    i + 1,  # NP_SBSC_RMNY_SEQ
                    trt_date,  # TRT_DATE
                    cncl_date,  # CNCL_DATE
                    from_operator_code,  # BCHNG_COMM_CMPN_ID
                    to_operator_code,  # ACHNG_COMM_CMPN_ID
                    f"{i+1:020d}",  # SVC_CONT_ID
                    f"{i+1:011d}",  # BILL_ACC_ID
                    f"010{random.randint(1000,9999)}{random.randint(1000,9999)}",  # TEL_NO
                    np_sttus_cd,  # NP_STTUS_CD
                    settlement_amount,  # SETL_AMT
                )
            )

        # 데이터베이스에 삽입
        conn.executemany(
            """
            INSERT INTO PY_NP_SBSC_RMNY_TXN 
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
            port_in_data,
        )

        conn.commit()
        self.logger.info(f"📥 포트인 데이터 {len(port_in_data)}건 생성 완료")

    def _log_data_statistics(self, data_stats: Dict[str, int]):
        """데이터 통계 로깅"""
        self.logger.info("\n📊 샘플 데이터 통계:")
        self.logger.info("=" * 50)

        total_records = sum(data_stats.values())

        for table_name, count in data_stats.items():
            table_display = {
                "PY_NP_TRMN_RMNY_TXN": "📤 포트아웃",
                "PY_NP_SBSC_RMNY_TXN": "📥 포트인",
                "PY_DEPAZ_BAS": "💰 예치금",
            }.get(table_name, table_name)

            self.logger.info(f"   {table_display}: {count:,}건")

        self.logger.info(f"   📊 총 데이터: {total_records:,}건")
        self.logger.info("=" * 50)

    def get_sample_statistics(self, conn):
        """생성된 샘플 데이터 통계 확인"""
        try:
            if self.use_azure:
                return self._get_azure_statistics(conn)
            else:
                return self._get_sqlite_statistics(conn)
        except Exception as e:
            self.logger.error(f"통계 조회 실패: {e}")
            return {}

    def _get_azure_statistics(self, conn):
        """Azure SQL Database 통계 조회"""
        stats = {}
        cursor = conn.cursor()

        try:
            # 포트아웃 통계
            cursor.execute(
                """
                SELECT 
                    COUNT(*) as total_count,
                    SUM(PAY_AMT) as total_amount,
                    AVG(PAY_AMT) as avg_amount,
                    MIN(PAY_AMT) as min_amount,
                    MAX(PAY_AMT) as max_amount
                FROM PY_NP_TRMN_RMNY_TXN
                WHERE NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
            """
            )
            port_out_result = cursor.fetchone()

            if port_out_result:
                stats["port_out"] = {
                    "total_count": port_out_result[0],
                    "total_amount": port_out_result[1] or 0,
                    "avg_amount": port_out_result[2] or 0,
                    "min_amount": port_out_result[3] or 0,
                    "max_amount": port_out_result[4] or 0,
                }

            # 포트인 통계
            cursor.execute(
                """
                SELECT 
                    COUNT(*) as total_count,
                    SUM(SETL_AMT) as total_amount,
                    AVG(SETL_AMT) as avg_amount,
                    MIN(SETL_AMT) as min_amount,
                    MAX(SETL_AMT) as max_amount
                FROM PY_NP_SBSC_RMNY_TXN
                WHERE NP_STTUS_CD IN ('OK', 'WD')
            """
            )
            port_in_result = cursor.fetchone()

            if port_in_result:
                stats["port_in"] = {
                    "total_count": port_in_result[0],
                    "total_amount": port_in_result[1] or 0,
                    "avg_amount": port_in_result[2] or 0,
                    "min_amount": port_in_result[3] or 0,
                    "max_amount": port_in_result[4] or 0,
                }

            # 예치금 통계
            cursor.execute(
                """
                SELECT 
                    COUNT(*) as total_count,
                    SUM(DEPAZ_AMT) as total_amount,
                    AVG(DEPAZ_AMT) as avg_amount
                FROM PY_DEPAZ_BAS
                WHERE RMNY_METH_CD = 'NA' AND DEPAZ_DIV_CD = '10'
            """
            )
            deposit_result = cursor.fetchone()

            if deposit_result:
                stats["deposit"] = {
                    "total_count": deposit_result[0],
                    "total_amount": deposit_result[1] or 0,
                    "avg_amount": deposit_result[2] or 0,
                }

            return stats

        except Exception as e:
            self.logger.error(f"Azure 통계 조회 실패: {e}")
            return {}

    def _get_sqlite_statistics(self, conn):
        """SQLite 통계 조회"""
        stats = {}

        try:
            # 포트아웃 통계
            port_out_stats = pd.read_sql_query(
                """
                SELECT 
                    COUNT(*) as total_count,
                    SUM(PAY_AMT) as total_amount,
                    AVG(PAY_AMT) as avg_amount,
                    MIN(PAY_AMT) as min_amount,
                    MAX(PAY_AMT) as max_amount
                FROM PY_NP_TRMN_RMNY_TXN
                WHERE NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
            """,
                conn,
            )

            if not port_out_stats.empty:
                stats["port_out"] = port_out_stats.iloc[0].to_dict()

            # 포트인 통계
            port_in_stats = pd.read_sql_query(
                """
                SELECT 
                    COUNT(*) as total_count,
                    SUM(SETL_AMT) as total_amount,
                    AVG(SETL_AMT) as avg_amount,
                    MIN(SETL_AMT) as min_amount,
                    MAX(SETL_AMT) as max_amount
                FROM PY_NP_SBSC_RMNY_TXN
                WHERE NP_STTUS_CD IN ('OK', 'WD')
            """,
                conn,
            )

            if not port_in_stats.empty:
                stats["port_in"] = port_in_stats.iloc[0].to_dict()

            # 예치금 통계
            deposit_stats = pd.read_sql_query(
                """
                SELECT 
                    COUNT(*) as total_count,
                    SUM(DEPAZ_AMT) as total_amount,
                    AVG(DEPAZ_AMT) as avg_amount
                FROM PY_DEPAZ_BAS
                WHERE RMNY_METH_CD = 'NA' AND DEPAZ_DIV_CD = '10'
            """,
                conn,
            )

            if not deposit_stats.empty:
                stats["deposit"] = deposit_stats.iloc[0].to_dict()

            return stats

        except Exception as e:
            self.logger.error(f"SQLite 통계 조회 실패: {e}")
            return {}

    def cleanup_sample_data(self, conn):
        """샘플 데이터 정리 (Azure만 해당)"""
        if not self.use_azure:
            self.logger.info("SQLite는 메모리 기반이므로 정리가 불필요합니다")
            return

        try:
            cursor = conn.cursor()

            # 샘플 데이터만 삭제
            cursor.execute("DELETE FROM PY_NP_TRMN_RMNY_TXN WHERE IS_SAMPLE_DATA = 1")
            cursor.execute("DELETE FROM PY_NP_SBSC_RMNY_TXN WHERE IS_SAMPLE_DATA = 1")
            cursor.execute("DELETE FROM PY_DEPAZ_BAS WHERE IS_SAMPLE_DATA = 1")

            conn.commit()
            self.logger.info("Azure SQL Database 샘플 데이터 정리 완료")

        except Exception as e:
            self.logger.error(f"샘플 데이터 정리 실패: {e}")

    def is_using_azure(self) -> bool:
        """Azure SQL Database 사용 여부 반환"""
        return self.use_azure

    def get_connection_info(self) -> Dict[str, Any]:
        """연결 정보 반환"""
        return {
            "type": "Azure SQL Database" if self.use_azure else "SQLite",
            "azure_ready": self.azure_config.is_production_ready(),
            "force_local": self.force_local,
        }


# 기존 함수들과의 호환성을 위한 래퍼 함수들
def create_sample_database(
    azure_config: Optional[AzureConfig] = None, force_local: bool = False
):
    """
    샘플 데이터베이스 생성 (기존 함수와 호환)

    Args:
        azure_config: Azure 설정 (None이면 로컬 모드)
        force_local: 강제로 로컬 SQLite 사용

    Returns:
        데이터베이스 연결 객체
    """
    if azure_config is None:
        # Azure 설정이 없으면 기존 로컬 모드로 동작
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        manager = SampleDataManager(None, force_local=True)
        manager.sqlite_conn = conn
        manager._create_sqlite_tables(conn)
        manager._generate_sample_data(conn)
        print("✅ 로컬 샘플 데이터베이스가 성공적으로 생성되었습니다!")
        return conn
    else:
        # Azure 설정이 있으면 새로운 매니저 사용
        manager = SampleDataManager(azure_config, force_local)
        return manager.create_sample_database()


def get_sample_statistics(conn):
    """샘플 데이터 통계 조회 (기존 함수와 호환)"""
    # 연결 타입에 따라 적절한 쿼리 실행
    try:
        # SQLite인지 확인
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        # SQLite라면 기존 로직 사용

        print("\n📊 샘플 데이터 통계:")
        print("=" * 50)

        # 포트아웃 통계
        port_out_stats = pd.read_sql_query(
            """
            SELECT 
                COUNT(*) as total_count,
                SUM(PAY_AMT) as total_amount,
                AVG(PAY_AMT) as avg_amount,
                MIN(PAY_AMT) as min_amount,
                MAX(PAY_AMT) as max_amount
            FROM PY_NP_TRMN_RMNY_TXN
            WHERE NP_TRMN_DTL_STTUS_VAL IN ('1', '3')
        """,
            conn,
        )

        print("📤 포트아웃 현황:")
        print(f"   총 건수: {port_out_stats.iloc[0]['total_count']:,}건")
        print(f"   총 정산액: {port_out_stats.iloc[0]['total_amount']:,.0f}원")
        print(f"   평균 정산액: {port_out_stats.iloc[0]['avg_amount']:,.0f}원")

        # 포트인 통계
        port_in_stats = pd.read_sql_query(
            """
            SELECT 
                COUNT(*) as total_count,
                SUM(SETL_AMT) as total_amount,
                AVG(SETL_AMT) as avg_amount,
                MIN(SETL_AMT) as min_amount,
                MAX(SETL_AMT) as max_amount
            FROM PY_NP_SBSC_RMNY_TXN
            WHERE NP_STTUS_CD IN ('OK', 'WD')
        """,
            conn,
        )

        print("\n📥 포트인 현황:")
        print(f"   총 건수: {port_in_stats.iloc[0]['total_count']:,}건")
        print(f"   총 정산액: {port_in_stats.iloc[0]['total_amount']:,.0f}원")
        print(f"   평균 정산액: {port_in_stats.iloc[0]['avg_amount']:,.0f}원")

        # 예치금 통계
        deposit_stats = pd.read_sql_query(
            """
            SELECT 
                COUNT(*) as total_count,
                SUM(DEPAZ_AMT) as total_amount,
                AVG(DEPAZ_AMT) as avg_amount
            FROM PY_DEPAZ_BAS
            WHERE RMNY_METH_CD = 'NA' AND DEPAZ_DIV_CD = '10'
        """,
            conn,
        )

        print("\n💰 예치금 현황:")
        print(f"   총 건수: {deposit_stats.iloc[0]['total_count']:,}건")
        print(f"   총 예치금: {deposit_stats.iloc[0]['total_amount']:,.0f}원")
        print(f"   평균 예치금: {deposit_stats.iloc[0]['avg_amount']:,.0f}원")

        print("=" * 50)

    except Exception as e:
        print(f"통계 조회 실패: {e}")


# 테스트 함수
def test_sample_data_manager():
    """샘플 데이터 매니저 테스트"""
    print("🧪 샘플 데이터 매니저 테스트를 시작합니다...")

    try:
        from azure_config import get_azure_config

        azure_config = get_azure_config()

        # 1. Azure 모드 테스트 (가능한 경우)
        if azure_config.is_production_ready():
            print("\n🔵 Azure SQL Database 모드 테스트:")
            azure_manager = SampleDataManager(azure_config, force_local=False)
            azure_conn = azure_manager.create_sample_database()

            connection_info = azure_manager.get_connection_info()
            print(f"   연결 타입: {connection_info['type']}")
            print(f"   Azure 준비 상태: {connection_info['azure_ready']}")

            # 통계 확인
            stats = azure_manager.get_sample_statistics(azure_conn)
            if stats:
                print("   📊 Azure 데이터 통계:")
                for data_type, stat in stats.items():
                    print(f"     {data_type}: {stat.get('total_count', 0):,}건")

            azure_conn.close()

        # 2. 로컬 모드 테스트
        print("\n🟡 로컬 SQLite 모드 테스트:")
        local_manager = SampleDataManager(azure_config, force_local=True)
        local_conn = local_manager.create_sample_database()

        connection_info = local_manager.get_connection_info()
        print(f"   연결 타입: {connection_info['type']}")

        # 통계 확인
        stats = local_manager.get_sample_statistics(local_conn)
        if stats:
            print("   📊 로컬 데이터 통계:")
            for data_type, stat in stats.items():
                print(f"     {data_type}: {stat.get('total_count', 0):,}건")

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
