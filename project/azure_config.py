# azure_config.py - Azure 서비스 연동 설정
import os
from typing import Optional
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.core.exceptions import ResourceNotFoundError
import openai
import logging


class AzureConfig:
    """Azure 서비스 연동 설정 클래스"""

    def __init__(self):
        """Azure 설정 초기화"""
        # self.key_vault_url = os.getenv("AZURE_KEY_VAULT_URL")
        # self.tenant_id = os.getenv("AZURE_TENANT_ID")
        # self.client_id = os.getenv("AZURE_CLIENT_ID")
        # self.client_secret = os.getenv("AZURE_CLIENT_SECRET")

        # 로거 설정
        self.logger = logging.getLogger(__name__)

        # Azure 인증 설정
        self.credential = self._get_credential()

        # Key Vault 클라이언트 초기화
        if self.key_vault_url and self.credential:
            try:
                self.secret_client = SecretClient(
                    vault_url=self.key_vault_url, credential=self.credential
                )
                self.logger.info("Azure Key Vault 클라이언트 초기화 성공")
            except Exception as e:
                self.logger.error(f"Key Vault 클라이언트 초기화 실패: {e}")
                self.secret_client = None
        else:
            self.secret_client = None
            self.logger.warning("Key Vault URL 또는 인증 정보가 없습니다")

    def _get_credential(self):
        """Azure 인증 credential 가져오기"""
        try:
            # 서비스 주체 인증 (운영 환경)
            if all([self.tenant_id, self.client_id, self.client_secret]):
                self.logger.info("서비스 주체 인증을 사용합니다")
                return ClientSecretCredential(
                    tenant_id=self.tenant_id,
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                )

            # 기본 Azure 인증 (개발 환경)
            else:
                self.logger.info("기본 Azure 인증을 사용합니다")
                return DefaultAzureCredential()

        except Exception as e:
            self.logger.error(f"Azure 인증 설정 실패: {e}")
            return None

    def get_secret(self, secret_name: str) -> Optional[str]:
        """Key Vault에서 시크릿 값을 가져옴"""
        if not self.secret_client:
            self.logger.warning("Secret client가 초기화되지 않았습니다")
            return None

        try:
            secret = self.secret_client.get_secret(secret_name)
            self.logger.info(f"시크릿 '{secret_name}' 조회 성공")
            return secret.value

        except ResourceNotFoundError:
            self.logger.error(
                f"시크릿 '{secret_name}'을 Key Vault에서 찾을 수 없습니다"
            )
            return None

        except Exception as e:
            self.logger.error(f"시크릿 '{secret_name}' 조회 실패: {e}")
            return None

    def get_openai_client(self):
        """Azure OpenAI 클라이언트 생성"""
        try:
            # Key Vault에서 OpenAI 설정 가져오기
            api_key = self.get_secret("azure-openai-api-key")
            endpoint = self.get_secret("azure-openai-endpoint")
            api_version = (
                self.get_secret("azure-openai-api-version") or "2024-02-15-preview"
            )

            # 환경변수 백업 (Key Vault 실패시)
            if not api_key:
                api_key = os.getenv("AZURE_OPENAI_API_KEY")
                self.logger.warning("환경변수에서 OpenAI API 키를 사용합니다")

            if not endpoint:
                endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
                self.logger.warning("환경변수에서 OpenAI 엔드포인트를 사용합니다")

            if not api_key or not endpoint:
                raise ValueError(
                    "Azure OpenAI API 키 또는 엔드포인트가 설정되지 않았습니다"
                )

            # Azure OpenAI 클라이언트 생성
            client = openai.AzureOpenAI(
                api_key=api_key, api_version=api_version, azure_endpoint=endpoint
            )

            self.logger.info("Azure OpenAI 클라이언트 생성 성공")
            return client

        except Exception as e:
            self.logger.error(f"Azure OpenAI 클라이언트 생성 실패: {e}")
            return None

    def get_database_connection_string(self) -> Optional[str]:
        """Azure SQL Database 연결 문자열 가져옴"""
        try:
            # Key Vault에서 연결 문자열 가져오기
            conn_string = self.get_secret("azure-sql-connection-string")

            # 환경변수 백업
            if not conn_string:
                conn_string = os.getenv("AZURE_SQL_CONNECTION_STRING")
                self.logger.warning("환경변수에서 DB 연결 문자열을 사용합니다")

            if conn_string:
                self.logger.info("데이터베이스 연결 문자열 조회 성공")
                return conn_string
            else:
                self.logger.error("데이터베이스 연결 문자열이 설정되지 않았습니다")
                return None

        except Exception as e:
            self.logger.error(f"데이터베이스 연결 문자열 가져오기 실패: {e}")
            return None

    def test_connection(self) -> dict:
        """Azure 서비스 연결 테스트"""
        results = {"key_vault": False, "openai": False, "database": False, "errors": []}

        # Key Vault 테스트
        try:
            if self.secret_client:
                # 테스트용 시크릿 조회 시도
                test_secret = self.get_secret("test-secret")
                results["key_vault"] = True
                self.logger.info("Key Vault 연결 테스트 성공")
            else:
                results["errors"].append("Key Vault 클라이언트가 초기화되지 않음")
        except Exception as e:
            results["errors"].append(f"Key Vault 연결 실패: {str(e)}")

        # OpenAI 테스트
        try:
            openai_client = self.get_openai_client()
            if openai_client:
                results["openai"] = True
                self.logger.info("OpenAI 클라이언트 테스트 성공")
            else:
                results["errors"].append("OpenAI 클라이언트 생성 실패")
        except Exception as e:
            results["errors"].append(f"OpenAI 연결 실패: {str(e)}")

        # Database 테스트
        try:
            conn_string = self.get_database_connection_string()
            if conn_string:
                results["database"] = True
                self.logger.info("데이터베이스 연결 문자열 테스트 성공")
            else:
                results["errors"].append("데이터베이스 연결 문자열 없음")
        except Exception as e:
            results["errors"].append(f"데이터베이스 연결 실패: {str(e)}")

        return results

    def is_production_ready(self) -> bool:
        """운영 환경 준비 상태 확인"""
        test_results = self.test_connection()
        return all(
            [
                test_results["key_vault"],
                test_results["openai"],
                test_results["database"],
            ]
        )


# 싱글톤 패턴으로 Azure 설정 관리
_azure_config_instance = None


def get_azure_config() -> AzureConfig:
    """Azure 설정 싱글톤 인스턴스 반환"""
    global _azure_config_instance
    if _azure_config_instance is None:
        _azure_config_instance = AzureConfig()
    return _azure_config_instance


# 테스트 함수
def test_azure_services():
    """Azure 서비스 연결 테스트"""
    print("🔧 Azure 서비스 연결 테스트를 시작합니다...")

    azure_config = get_azure_config()
    test_results = azure_config.test_connection()

    print("\n📋 테스트 결과:")
    print(f"🔐 Key Vault: {'✅ 성공' if test_results['key_vault'] else '❌ 실패'}")
    print(f"🤖 OpenAI: {'✅ 성공' if test_results['openai'] else '❌ 실패'}")
    print(f"🗄️ Database: {'✅ 성공' if test_results['database'] else '❌ 실패'}")

    if test_results["errors"]:
        print("\n⚠️ 오류 목록:")
        for error in test_results["errors"]:
            print(f"   - {error}")

    production_ready = azure_config.is_production_ready()
    print(
        f"\n🚀 운영 준비 상태: {'✅ 준비 완료' if production_ready else '❌ 설정 필요'}"
    )

    return test_results


if __name__ == "__main__":
    # 직접 실행시 테스트 수행
    test_azure_services()
