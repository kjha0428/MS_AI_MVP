# azure_config.py - 간단한 환경변수 기반 Azure 설정
import os
from typing import Optional
import logging

# import pyodbc
import pymssql
from urllib.parse import quote_plus


class AzureConfig:
    """환경변수 기반 Azure 설정 클래스 (Key Vault, 서비스 주체 없음)"""

    def __init__(self):
        """Azure 설정 초기화"""
        # Azure OpenAI 설정 (환경변수에서 직접 로드)
        self.openai_api_key = os.getenv("AZURE_OPENAI_API_KEY")
        self.openai_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        self.openai_api_version = os.getenv("AZURE_OPENAI_API_VERSION")
        self.openai_model_name = os.getenv("AZURE_OPENAI_MODEL_NAME")

        # Azure SQL Database 설정 (환경변수에서 직접 로드)
        self.sql_connection_string = os.getenv("AZURE_SQL_CONNECTION_STRING")

        # 로거 설정
        self.logger = logging.getLogger(__name__)

        # 설정 상태 로깅
        self._log_configuration_status()

    def _log_configuration_status(self):
        """설정 상태 로깅"""
        self.logger.info("Azure 설정 상태:")
        self.logger.info(
            f"  OpenAI API Key: {'✅ 설정됨' if self.openai_api_key else '❌ 없음'}"
        )
        self.logger.info(
            f"  OpenAI Endpoint: {'✅ 설정됨' if self.openai_endpoint else '❌ 없음'}"
        )
        self.logger.info(
            f"  SQL Connection: {'✅ 설정됨' if self.sql_connection_string else '❌ 없음'}"
        )

    def get_openai_client(self):
        """Azure OpenAI 클라이언트 생성"""
        try:
            if not self.openai_api_key or not self.openai_endpoint:
                self.logger.warning("Azure OpenAI 설정이 완전하지 않습니다")
                return None

            # openai 라이브러리 임포트 시도
            try:
                import openai
            except ImportError:
                self.logger.error(
                    "openai 라이브러리가 설치되지 않았습니다: pip install openai"
                )
                return None

            # Azure OpenAI 클라이언트 생성 (버전 호환성 고려)
            try:
                # 최신 버전 방식으로 시도
                client = openai.AzureOpenAI(
                    api_key=self.openai_api_key,
                    api_version=self.openai_api_version,
                    azure_endpoint=self.openai_endpoint,
                )

                # 간단한 연결 테스트 (실제 API 호출 없이)
                if hasattr(client, "chat"):
                    self.logger.info("Azure OpenAI 클라이언트 생성 성공")
                    return client
                else:
                    raise Exception("클라이언트 객체가 올바르지 않습니다")

            except TypeError as te:
                # 구버전 방식으로 재시도
                self.logger.warning(f"최신 방식 실패, 구버전 방식으로 재시도: {te}")
                try:
                    # 구버전 openai 라이브러리 방식
                    openai.api_type = "azure"
                    openai.api_key = self.openai_api_key
                    openai.api_base = self.openai_endpoint
                    openai.api_version = self.openai_api_version

                    self.logger.info("Azure OpenAI 클라이언트 설정 완료 (구버전 방식)")
                    return openai  # 구버전에서는 openai 모듈 자체를 반환

                except Exception as legacy_error:
                    self.logger.error(f"구버전 방식도 실패: {legacy_error}")
                    return None

        except Exception as e:
            self.logger.error(f"Azure OpenAI 클라이언트 생성 실패: {e}")
            return None

    def get_database_connection_string(self) -> Optional[str]:
        """SQLAlchemy용 연결 URL 반환 (None 체크 강화)"""
        try:
            server = os.getenv("AZURE_SQL_SERVER")
            database = os.getenv("AZURE_SQL_DATABASE")
            username = os.getenv("AZURE_SQL_USERNAME")
            password = os.getenv("AZURE_SQL_PASSWORD")

            # 🔥 수정: None 체크 강화
            if not all([server, database, username, password]):
                missing = []
                if not server:
                    missing.append("AZURE_SQL_SERVER")
                if not database:
                    missing.append("AZURE_SQL_DATABASE")
                if not username:
                    missing.append("AZURE_SQL_USERNAME")
                if not password:
                    missing.append("AZURE_SQL_PASSWORD")

                self.logger.warning(f"누락된 환경변수: {', '.join(missing)}")
                return None

            # .database.windows.net이 없으면 추가
            if not server.endswith(".database.windows.net"):
                server = f"{server}.database.windows.net"

            # SQLAlchemy 연결 URL 생성 (pymssql 드라이버 사용)
            user_encoded = quote_plus(username)
            password_encoded = quote_plus(password)

            connection_url = f"mssql+pymssql://{user_encoded}:{password_encoded}@{server}:1433/{database}?charset=utf8&timeout=30"

            self.logger.info("SQLAlchemy 연결 URL 생성 성공")
            return connection_url

        except Exception as e:
            self.logger.error(f"SQLAlchemy 연결 URL 생성 실패: {e}")
            return None

    def _get_available_sql_server_driver(self) -> Optional[str]:
        """pymssql은 드라이버 확인이 불필요"""
        try:
            # 🔥 수정: pymssql은 별도 드라이버가 필요없음
            self.logger.info("pymssql 사용 - ODBC 드라이버 불필요")
            return "pymssql"  # 또는 이 메서드 자체를 제거

        except Exception as e:
            self.logger.error(f"pymssql 확인 실패: {e}")
            return None

    def test_database_connection(self) -> bool:
        """SQLAlchemy를 사용한 Azure SQL Database 연결 테스트"""
        try:
            from sqlalchemy import create_engine, text

            connection_url = self.get_database_connection_string()
            if not connection_url:
                self.logger.warning("데이터베이스 연결 URL이 없어 테스트를 건너뜁니다")
                return False

            # SQLAlchemy 엔진으로 연결 테스트
            engine = create_engine(connection_url, pool_timeout=10)

            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                row = result.fetchone()

            if row:
                self.logger.info("SQLAlchemy Azure SQL Database 연결 테스트 성공")
                return True
            else:
                self.logger.error("SQLAlchemy Azure SQL Database 연결 테스트 실패")
                return False

        except Exception as e:
            self.logger.error(f"SQLAlchemy Azure SQL Database 연결 테스트 실패: {e}")
            return False

    def test_connection(self) -> dict:
        """Azure 서비스 연결 테스트"""
        results = {
            "openai": False,
            "database": False,
            "errors": [],
        }

        # OpenAI 부분은 동일...

        # 🔥 수정: Database 연결 테스트 부분
        connection_url = self.get_database_connection_string()
        if connection_url:
            try:
                if self.test_database_connection():
                    results["database"] = True
                else:
                    results["errors"].append("데이터베이스 연결 실패")
            except Exception as e:
                results["errors"].append(f"데이터베이스 연결 테스트 실패: {str(e)}")
        else:
            results["errors"].append("데이터베이스 연결 정보가 설정되지 않음")

        return results

    def is_production_ready(self) -> bool:
        """운영 환경 준비 상태 확인"""
        # OpenAI와 Database 설정을 개별적으로 확인
        has_openai = bool(self.openai_api_key and self.openai_endpoint)

        # Database 연결 문자열 생성 가능한지 확인
        has_database = bool(self.get_database_connection_string())

        # 최소한 하나의 서비스가 완전히 설정되어 있어야 함
        is_ready = has_openai or has_database

        if is_ready:
            self.logger.info(
                f"사용 가능한 서비스: OpenAI={has_openai}, Database={has_database}"
            )
        else:
            self.logger.warning("사용 가능한 Azure 서비스가 없습니다")

        return is_ready

    def get_configuration_summary(self) -> dict:
        """현재 설정 요약 반환"""
        return {
            "azure_openai_available": bool(
                self.openai_api_key and self.openai_endpoint
            ),
            "azure_sql_available": bool(self.get_database_connection_string()),  # 수정
            "openai_model": self.openai_model_name,
            "openai_api_version": self.openai_api_version,
            "production_ready": self.is_production_ready(),
        }


# 싱글톤 패턴으로 Azure 설정 관리
_azure_config_instance = None


def get_azure_config() -> AzureConfig:
    """Azure 설정 싱글톤 인스턴스 반환"""
    global _azure_config_instance
    if _azure_config_instance is None:
        _azure_config_instance = AzureConfig()
    return _azure_config_instance


def setup_environment_guide():
    """환경변수 설정 가이드"""
    print("🔧 Azure 환경변수 설정 가이드")
    print("=" * 60)

    print("\n📋 필요한 환경변수:")

    env_vars = {
        "AZURE_OPENAI_API_KEY": {
            "description": "Azure OpenAI API 키",
            "required": "OpenAI 사용시 필수",
            "example": "your-openai-api-key-here",
        },
        "AZURE_OPENAI_ENDPOINT": {
            "description": "Azure OpenAI 엔드포인트",
            "required": "OpenAI 사용시 필수",
            "example": "https://your-resource.openai.azure.com/",
        },
        "AZURE_OPENAI_MODEL_NAME": {
            "description": "OpenAI 모델명",
            "required": "선택사항 (기본값: gpt-4)",
            "example": "gpt-4",
        },
        "AZURE_SQL_CONNECTION_STRING": {
            "description": "Azure SQL Database 연결 문자열",
            "required": "Database 사용시 필수",
            "example": "Driver={ODBC Driver 18 for SQL Server};Server=tcp:server.database.windows.net,1433;Database=dbname;Authentication=ActiveDirectoryMsi;Encrypt=yes;",
        },
    }

    for var_name, info in env_vars.items():
        current_value = os.getenv(var_name)
        status = "✅ 설정됨" if current_value else "❌ 없음"

        print(f"\n• {var_name}")
        print(f"  설명: {info['description']}")
        print(f"  필수여부: {info['required']}")
        print(f"  현재상태: {status}")
        print(f"  예시: {info['example']}")


def test_azure_services():
    """Azure 서비스 연결 테스트"""
    print("🔧 Azure 서비스 테스트를 시작합니다...")

    azure_config = get_azure_config()

    # 현재 설정 상태 출력
    config_summary = azure_config.get_configuration_summary()
    print(f"\n📊 현재 설정 상태:")
    print(
        f"   Azure OpenAI: {'✅ 사용 가능' if config_summary['azure_openai_available'] else '❌ 설정 필요'}"
    )
    print(
        f"   Azure SQL: {'✅ 사용 가능' if config_summary['azure_sql_available'] else '❌ 설정 필요'}"
    )
    print(
        f"   운영 준비: {'✅ 완료' if config_summary['production_ready'] else '❌ 설정 필요'}"
    )

    # 연결 테스트
    test_results = azure_config.test_connection()

    print(f"\n📋 서비스 테스트 결과:")
    print(f"🤖 OpenAI: {'✅ 성공' if test_results['openai'] else '❌ 실패'}")
    print(f"🗄️ Database: {'✅ 성공' if test_results['database'] else '❌ 실패'}")

    if test_results["errors"]:
        print(f"\n⚠️ 발견된 문제:")
        for error in test_results["errors"]:
            print(f"   - {error}")

    # 운영 준비 상태
    production_ready = azure_config.is_production_ready()
    if production_ready:
        print(f"\n🚀 시스템 상태: ✅ 사용 준비 완료")
        if config_summary["azure_openai_available"]:
            print(
                f"   - Azure OpenAI 사용 가능 (모델: {config_summary['openai_model']})"
            )
        if config_summary["azure_sql_available"]:
            print(f"   - Azure SQL Database 사용 가능")
    else:
        print(f"\n🚀 시스템 상태: ❌ 추가 설정 필요")
        print(f"   최소한 OpenAI 또는 Database 중 하나는 설정해야 합니다")

    return test_results


if __name__ == "__main__":
    # 환경변수 설정 가이드 출력
    setup_environment_guide()

    print("\n" + "=" * 60)

    # 테스트 수행
    test_azure_services()
