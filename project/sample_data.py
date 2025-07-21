# sample_data.py - 번호이동정산 샘플 데이터 생성
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random


def create_sample_database():
    """샘플 데이터베이스 생성 및 데이터 삽입"""

    # 메모리 내 SQLite 데이터베이스 생성
    conn = sqlite3.connect(":memory:", check_same_thread=False)

    # 테이블 생성
    create_tables(conn)

    # 샘플 데이터 생성
    generate_sample_data(conn)

    print("✅ 샘플 데이터베이스가 성공적으로 생성되었습니다!")
    return conn


def create_tables(conn):
    """실제 테이블 구조에 맞는 테이블 생성"""

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
            BILL_ACC_ID VARCHAR2(11),
            TEL_NO VARCHAR(20),
            NP_TRMN_DTL_STTUS_VAL VARCHAR2(3),
            PAY_AMT NUMBER(18,3)
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
            BILL_ACC_ID VARCHAR2(11),
            TEL_NO VARCHAR(20),
            NP_STTUS_CD VARCHAR2(3),
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
            BILL_ACC_ID VARCHAR2(11),
            DEPAZ_DIV_CD VARCHAR(3),
            RMNY_DATE DATE,
            RMNY_METH_CD VARCHAR2(5),
            DEPAZ_AMT DECIMAL(15,2)
        )
    """
    )

    conn.commit()
    print("📋 테이블이 생성되었습니다.")


def generate_sample_data(conn):
    """현실적인 샘플 데이터 생성"""

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

    print("📊 샘플 데이터를 생성 중입니다...")

    # 1. 해지번호이동 데이터 (포트아웃) 생성
    generate_port_out_data(conn, operators, start_date, end_date)

    # 2. 가입번호이동 데이터 (포트인) 생성
    generate_port_in_data(conn, operators, start_date, end_date)

    conn.execute(
        """
        INSERT INTO PY_NP_SBSC_RMNY_TXN 
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            "IN",
            9999,
            "2025-07-19",
            "",
            "SKT",
            "KT",
            "987654321",
            "10987654321",
            "01012345678",
            "1",
            3000,
        ),
    )

    print("✨ 모든 샘플 데이터 생성 완료!")


def generate_port_out_data(conn, operators, start_date, end_date):
    """포트아웃 데이터 생성"""

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
        # TRT_STUS_CD에 따라 NP_TRMN_DATE 설정
        if np_trmn_dtl_sttus_val == "1":
            cncl_wthd_date = None  # NULL
        elif np_trmn_dtl_sttus_val == "2":
            cncl_wthd_date = np_trmn_date  # NP_TRMN_DATE 동일
        else:  # WD
            # CNCL_WTHD_DATE 이후 1~15일 랜덤 날짜
            random_days = random.randint(1, 15)
            cncl_wthd_date = (transaction_date + timedelta(days=random_days)).strftime(
                "%Y-%m-%d"
            )

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
    print(f"📤 포트아웃 데이터 {len(port_out_data)}건 생성 완료")
    print(f"💰 예치금 데이터 {len(deposit_data)}건 생성 완료")


def generate_port_in_data(conn, operators, start_date, end_date):
    """포트인 데이터 생성"""

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
        # TRT_STUS_CD에 따라 NP_TRMN_DATE 설정
        if np_sttus_cd == "OK":
            cncl_date = None  # NULL
        elif np_sttus_cd == "CN":
            cncl_date = trt_date  # TRT_DATE 동일
        else:  # WD
            # CNCL_WTHD_DATE 이후 1~15일 랜덤 날짜
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
    print(f"📥 포트인 데이터 {len(port_in_data)}건 생성 완료")


def get_sample_statistics(conn):
    """생성된 샘플 데이터 통계 확인"""

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
        WHERE RMNY_METH_CD = 'NA'
        AND  DEPAZ_DIV_CD = '10'
        """,
        conn,
    )

    print("\n💰 예치금 현황:")
    print(f"   총 건수: {deposit_stats.iloc[0]['total_count']:,}건")
    print(f"   총 예치금: {deposit_stats.iloc[0]['total_amount']:,.0f}원")
    print(f"   평균 예치금: {deposit_stats.iloc[0]['avg_amount']:,.0f}원")

    print("=" * 50)


# 테스트 실행
if __name__ == "__main__":
    # 샘플 데이터베이스 생성
    conn = create_sample_database()

    # 통계 출력
    get_sample_statistics(conn)

    # 연결 종료
    conn.close()
