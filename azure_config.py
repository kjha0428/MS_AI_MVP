# azure_config.py - 간단한 환경변수 기반 Azure 설정
import os
from typing import Optional
import logging

# import pyodbc
import pymssql
from urllib.parse import quote_plus
from dotenv import load_dotenv

class AzureConfig:
    """환경변수 기반 Azure 설정 클래스"""

    def __init__(self):
        """Azure 설정 초기화 - 웹앱 환경 최적화"""
        # 🔥 추가: 웹앱 환경에서 .env 파일 강제 로드
        try:
            load_dotenv(override=True)  # 기존 환경변수 덮어쓰기
            self.logger = logging.getLogger(__name__)
            self.logger.info("환경변수 파일 로드 완료")
        except Exception as e:
            self.logger = logging.getLogger(__name__)
            self.logger.warning(f".env 파일 로드 실패: {e}")

        # Azure OpenAI 설정 (환경변수에서 직접 로드)
        self.openai_api_key = os.getenv("AZURE_OPENAI_API_KEY")
        self.openai_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        self.openai_api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01")  # 기본값 설정
        self.openai_model_name = os.getenv("AZURE_OPENAI_MODEL_NAME", "gpt-4o")  # 기본값 설정

        # 🔥 추가: 엔드포인트 정규화
        if self.openai_endpoint:
            self.openai_endpoint = self.openai_endpoint.rstrip('/')
            if not self.openai_endpoint.startswith('https://'):
                self.logger.error(f"엔드포인트가 올바르지 않습니다: {self.openai_endpoint}")

        # Azure SQL Database 설정
        self.sql_connection_string = os.getenv("AZURE_SQL_CONNECTION_STRING")

        # 🔥 추가: 웹앱 환경에서 설정 검증
        self._validate_web_app_settings()

        # 설정 상태 로깅
        self._log_configuration_status()

    def _validate_web_app_settings(self):
        """웹앱 환경에서 설정 검증"""
        missing_vars = []
        
        if not self.openai_api_key:
            missing_vars.append("AZURE_OPENAI_API_KEY")
        if not self.openai_endpoint:
            missing_vars.append("AZURE_OPENAI_ENDPOINT")
        
        if missing_vars:
            self.logger.error("🔥 웹앱 환경에서 누락된 환경변수:")
            for var in missing_vars:
                self.logger.error(f"  - {var}")
            self.logger.error("Azure Web App → 구성 → 애플리케이션 설정에서 환경변수 추가 필요")
        else:
            self.logger.info("✅ 웹앱 환경 설정 검증 완료")

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
        """Azure OpenAI 클라이언트 생성 - 웹앱 환경 최적화"""
        try:
            # 🔥 추가: 웹앱 환경에서 설정 재확인
            if not self.openai_api_key or not self.openai_endpoint:
                self.logger.error("🔥 웹앱 환경에서 OpenAI 설정 누락!")
                self.logger.error("해결 방법:")
                self.logger.error("1. Azure Portal → Web App → 구성 → 애플리케이션 설정")
                self.logger.error("2. 다음 환경변수 추가:")
                self.logger.error("   - AZURE_OPENAI_API_KEY")
                self.logger.error("   - AZURE_OPENAI_ENDPOINT")
                self.logger.error("   - AZURE_OPENAI_API_VERSION")
                self.logger.error("   - AZURE_OPENAI_MODEL_NAME")
                return None

            try:
                from openai import AzureOpenAI
                
                # 🔥 수정: 웹앱 환경에서 안정적인 클라이언트 생성
                self.logger.info(f"웹앱에서 OpenAI 클라이언트 생성 시도...")
                self.logger.info(f"  - Endpoint: {self.openai_endpoint}")
                self.logger.info(f"  - API Version: {self.openai_api_version}")
                self.logger.info(f"  - Model: {self.openai_model_name}")
                
                client = AzureOpenAI(
                    api_key=self.openai_api_key,
                    azure_endpoint=self.openai_endpoint,
                    api_version=self.openai_api_version,
                    # 🔥 추가: 웹앱 환경에서 타임아웃 설정
                    timeout=30.0,
                    max_retries=3
                )
                
                # 🔥 추가: 웹앱에서 실제 연결 테스트 (중요!)
                try:
                    test_response = client.chat.completions.create(
                        model=self.openai_model_name,
                        messages=[{"role": "user", "content": "test"}],
                        max_tokens=1,
                        timeout=10
                    )
                    self.logger.info("✅ 웹앱에서 OpenAI 연결 테스트 성공!")
                    return client
                    
                except Exception as test_error:
                    error_str = str(test_error)
                    
                    if "403" in error_str:
                        self.logger.error("🔥 웹앱에서 OpenAI 방화벽 차단!")
                        self.logger.error("해결 방법:")
                        self.logger.error("1. Azure Portal → OpenAI 리소스 → 네트워킹")
                        self.logger.error("2. '모든 네트워크' 선택 또는 Azure Web App IP 추가")
                        self.logger.error("3. Web App의 아웃바운드 IP 주소 확인 필요")
                    elif "404" in error_str:
                        self.logger.error(f"🔥 웹앱에서 모델 '{self.openai_model_name}' 배포 없음!")
                    elif "401" in error_str:
                        self.logger.error("🔥 웹앱에서 API 키 인증 실패!")
                    else:
                        self.logger.error(f"🔥 웹앱에서 OpenAI 연결 실패: {error_str}")
                    
                    # 테스트 실패해도 클라이언트는 반환 (재시도 가능하도록)
                    return client
                
            except ImportError:
                self.logger.error("웹앱에서 openai 라이브러리 import 실패!")
                self.logger.error("requirements.txt에 'openai==1.34.0' 추가 확인")
                return None
            except Exception as e:
                self.logger.error(f"웹앱에서 OpenAI 클라이언트 생성 실패: {e}")
                return None
                
        except Exception as e:
            self.logger.error(f"웹앱 환경 예상치 못한 오류: {e}")
            return None

    def get_available_models(self) -> list:
        """사용 가능한 OpenAI 모델 목록 조회"""
        try:
            if not self.openai_api_key or not self.openai_endpoint:
                return []

            client = self.get_openai_client()
            if not client:
                return []

            # 🔥 추가: 일반적으로 사용되는 모델명들
            common_models = ["gpt-4o", "gpt-4", "gpt-4o-mini"]

            available_models = []
            for model in common_models:
                try:
                    # 간단한 테스트 요청으로 모델 사용 가능 여부 확인
                    test_response = client.chat.completions.create(
                        model=model,
                        messages=[{"role": "user", "content": "test"}],
                        max_tokens=1,
                    )
                    available_models.append(model)
                    self.logger.info(f"사용 가능한 모델: {model}")
                except Exception as e:
                    if "DeploymentNotFound" in str(e):
                        self.logger.debug(f"모델 '{model}' 배포되지 않음")
                    else:
                        self.logger.debug(f"모델 '{model}' 테스트 실패: {e}")

            return available_models

        except Exception as e:
            self.logger.error(f"모델 목록 조회 실패: {e}")
            return []

    def validate_openai_deployment(self) -> dict:
        """OpenAI 배포 상태 검증"""
        result = {
            "configured_model": self.openai_model_name,
            "model_available": False,
            "available_models": [],
            "recommendation": None,
        }

        try:
            available_models = self.get_available_models()
            result["available_models"] = available_models

            if self.openai_model_name in available_models:
                result["model_available"] = True
                result["recommendation"] = (
                    f"설정된 모델 '{self.openai_model_name}'이 사용 가능합니다."
                )
            else:
                if available_models:
                    result["recommendation"] = (
                        f"'{self.openai_model_name}' 대신 '{available_models[0]}'을 사용하세요."
                    )
                else:
                    result["recommendation"] = (
                        "사용 가능한 모델이 없습니다. Azure OpenAI 배포를 확인하세요."
                    )

        except Exception as e:
            result["recommendation"] = f"모델 검증 실패: {e}"

        return result

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
            # 🔥 수정: SQLAlchemy import 체크 추가
            try:
                from sqlalchemy import create_engine, text
            except ImportError:
                self.logger.error(
                    "SQLAlchemy가 설치되지 않았습니다: pip install sqlalchemy"
                )
                return False

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

        # 🔥 수정: OpenAI 연결 테스트
        try:
            if self.openai_api_key and self.openai_endpoint:
                client = self.get_openai_client()
                if client:
                    results["openai"] = True
                else:
                    results["errors"].append("OpenAI 클라이언트 생성 실패")
            else:
                results["errors"].append("OpenAI 설정이 완전하지 않음")
        except Exception as e:
            results["errors"].append(f"OpenAI 연결 테스트 실패: {str(e)}")

        # 🔥 수정: Database 연결 테스트 부분
        try:
            connection_url = self.get_database_connection_string()
            if connection_url:
                if self.test_database_connection():
                    results["database"] = True
                else:
                    results["errors"].append("데이터베이스 연결 실패")
            else:
                results["errors"].append("데이터베이스 연결 정보가 설정되지 않음")
        except Exception as e:
            results["errors"].append(f"데이터베이스 연결 테스트 실패: {str(e)}")

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
