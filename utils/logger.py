# utils/logger.py - 로깅 유틸리티
import logging
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional
from logging.handlers import RotatingFileHandler
import traceback


class MNPLogger:
    """번호이동정산 시스템 전용 로거"""

    def __init__(
        self,
        name: str = "mnp_analysis",
        log_level: str = "INFO",
        log_dir: str = "logs",
        max_file_size: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
    ):
        """
        로거 초기화

        Args:
            name: 로거 이름
            log_level: 로그 레벨 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_dir: 로그 파일 디렉토리
            max_file_size: 최대 로그 파일 크기 (바이트)
            backup_count: 백업 파일 개수
        """
        self.name = name
        self.log_dir = log_dir

        # 로그 디렉토리 생성
        self._ensure_log_directory()

        # 로거 설정
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))

        # 핸들러가 이미 추가되어 있으면 제거 (중복 방지)
        if self.logger.handlers:
            self.logger.handlers.clear()

        # 파일 핸들러 추가
        self._setup_file_handler(max_file_size, backup_count)

        # 콘솔 핸들러 추가
        self._setup_console_handler()

        # 시작 로그
        self.logger.info(f"MNP Logger 초기화 완료 - Level: {log_level}")

    def _ensure_log_directory(self):
        """로그 디렉토리 생성"""
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)

    def _setup_file_handler(self, max_file_size: int, backup_count: int):
        """파일 핸들러 설정"""
        log_file = os.path.join(
            self.log_dir, f"{self.name}_{datetime.now().strftime('%Y%m')}.log"
        )

        file_handler = RotatingFileHandler(
            log_file, maxBytes=max_file_size, backupCount=backup_count, encoding="utf-8"
        )
        file_handler.setLevel(logging.INFO)

        # 파일용 포맷터 (상세 정보 포함)
        file_formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)

        self.logger.addHandler(file_handler)

    def _setup_console_handler(self):
        """콘솔 핸들러 설정"""
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.WARNING)  # 콘솔에는 경고 이상만 출력

        # 콘솔용 포맷터 (간단한 형태)
        console_formatter = logging.Formatter("%(levelname)s | %(name)s | %(message)s")
        console_handler.setFormatter(console_formatter)

        self.logger.addHandler(console_handler)

    def _create_log_entry(self, event_type: str, **kwargs) -> Dict[str, Any]:
        """로그 엔트리 생성"""
        return {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "system": "mnp_analysis",
            **kwargs,
        }

    def log_query_execution(
        self,
        user_input: str,
        sql_query: str,
        execution_time: float,
        result_count: int,
        success: bool = True,
        error_message: Optional[str] = None,
        ai_generated: bool = False,
    ):
        """쿼리 실행 로그"""

        log_entry = self._create_log_entry(
            event_type="query_execution",
            user_input=user_input,
            sql_query=(
                sql_query[:500] + "..." if len(sql_query) > 500 else sql_query
            ),  # SQL 길이 제한
            execution_time=execution_time,
            result_count=result_count,
            success=success,
            error_message=error_message,
            ai_generated=ai_generated,
            query_hash=hash(sql_query),
        )

        if success:
            self.logger.info(json.dumps(log_entry, ensure_ascii=False))
        else:
            self.logger.error(json.dumps(log_entry, ensure_ascii=False))

    def log_user_activity(
        self,
        activity: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        details: Optional[Dict] = None,
    ):
        """사용자 활동 로그"""

        log_entry = self._create_log_entry(
            event_type="user_activity",
            activity=activity,
            user_id=user_id or "anonymous",
            session_id=session_id,
            details=details or {},
        )

        self.logger.info(json.dumps(log_entry, ensure_ascii=False))

    def log_system_event(
        self,
        event: str,
        component: str,
        status: str = "success",
        details: Optional[Dict] = None,
    ):
        """시스템 이벤트 로그"""

        log_entry = self._create_log_entry(
            event_type="system_event",
            event=event,
            component=component,
            status=status,
            details=details or {},
        )

        if status == "success":
            self.logger.info(json.dumps(log_entry, ensure_ascii=False))
        elif status == "warning":
            self.logger.warning(json.dumps(log_entry, ensure_ascii=False))
        else:  # error, failed
            self.logger.error(json.dumps(log_entry, ensure_ascii=False))

    def log_error(
        self,
        error_type: str,
        error_message: str,
        component: str,
        context: Optional[Dict] = None,
        include_traceback: bool = True,
    ):
        """에러 로그"""

        log_entry = self._create_log_entry(
            event_type="error",
            error_type=error_type,
            error_message=error_message,
            component=component,
            context=context or {},
        )

        # 스택 트레이스 추가
        if include_traceback:
            log_entry["traceback"] = traceback.format_exc()

        self.logger.error(json.dumps(log_entry, ensure_ascii=False))

    def log_security_event(
        self,
        event_type: str,
        severity: str,
        description: str,
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        details: Optional[Dict] = None,
    ):
        """보안 이벤트 로그"""

        log_entry = self._create_log_entry(
            event_type="security_event",
            security_event_type=event_type,
            severity=severity,
            description=description,
            user_id=user_id or "unknown",
            ip_address=ip_address or "unknown",
            details=details or {},
        )

        # 보안 이벤트는 항상 WARNING 이상으로 로깅
        if severity in ["critical", "high"]:
            self.logger.critical(json.dumps(log_entry, ensure_ascii=False))
        elif severity == "medium":
            self.logger.error(json.dumps(log_entry, ensure_ascii=False))
        else:  # low
            self.logger.warning(json.dumps(log_entry, ensure_ascii=False))

    def log_performance_metric(
        self,
        metric_name: str,
        metric_value: float,
        unit: str,
        component: str,
        additional_data: Optional[Dict] = None,
    ):
        """성능 메트릭 로그"""

        log_entry = self._create_log_entry(
            event_type="performance_metric",
            metric_name=metric_name,
            metric_value=metric_value,
            unit=unit,
            component=component,
            additional_data=additional_data or {},
        )

        self.logger.info(json.dumps(log_entry, ensure_ascii=False))

    def log_data_access(
        self,
        table_name: str,
        operation: str,
        record_count: int,
        user_id: Optional[str] = None,
        filters_applied: Optional[Dict] = None,
    ):
        """데이터 접근 로그 (감사 목적)"""

        log_entry = self._create_log_entry(
            event_type="data_access",
            table_name=table_name,
            operation=operation,
            record_count=record_count,
            user_id=user_id or "system",
            filters_applied=filters_applied or {},
        )

        self.logger.info(json.dumps(log_entry, ensure_ascii=False))

    def get_log_statistics(self, days: int = 7) -> Dict[str, Any]:
        """로그 통계 조회"""
        try:
            stats = {
                "total_entries": 0,
                "error_count": 0,
                "query_count": 0,
                "user_activity_count": 0,
                "average_query_time": 0,
                "most_common_errors": [],
                "period_days": days,
            }

            # 실제 구현에서는 로그 파일을 파싱하여 통계 생성
            # 여기서는 기본 구조만 제공

            return stats

        except Exception as e:
            self.log_error(
                error_type="log_statistics_error",
                error_message=str(e),
                component="logger",
            )
            return {}

    def cleanup_old_logs(self, days_to_keep: int = 30):
        """오래된 로그 파일 정리"""
        try:
            current_time = datetime.now()

            for filename in os.listdir(self.log_dir):
                if filename.startswith(self.name) and filename.endswith(".log"):
                    file_path = os.path.join(self.log_dir, filename)
                    file_time = datetime.fromtimestamp(os.path.getctime(file_path))

                    if (current_time - file_time).days > days_to_keep:
                        os.remove(file_path)
                        self.logger.info(f"오래된 로그 파일 삭제: {filename}")

            self.log_system_event(
                event="log_cleanup",
                component="logger",
                status="success",
                details={"days_to_keep": days_to_keep},
            )

        except Exception as e:
            self.log_error(
                error_type="log_cleanup_error", error_message=str(e), component="logger"
            )


# 싱글톤 로거 인스턴스
_logger_instance = None


def get_logger() -> MNPLogger:
    """싱글톤 로거 인스턴스 반환"""
    global _logger_instance
    if _logger_instance is None:
        log_level = os.getenv("LOG_LEVEL", "INFO")
        _logger_instance = MNPLogger(log_level=log_level)
    return _logger_instance


# 편의 함수들
def log_query(
    user_input: str,
    sql_query: str,
    execution_time: float,
    result_count: int,
    success: bool = True,
    error: str = None,
):
    """쿼리 실행 로그 편의 함수"""
    logger = get_logger()
    logger.log_query_execution(
        user_input=user_input,
        sql_query=sql_query,
        execution_time=execution_time,
        result_count=result_count,
        success=success,
        error_message=error,
    )


def log_user(activity: str, details: Dict = None):
    """사용자 활동 로그 편의 함수"""
    logger = get_logger()
    logger.log_user_activity(activity=activity, details=details)


def log_error(error_type: str, message: str, component: str, context: Dict = None):
    """에러 로그 편의 함수"""
    logger = get_logger()
    logger.log_error(
        error_type=error_type,
        error_message=message,
        component=component,
        context=context,
    )


def log_system(event: str, component: str, status: str = "success"):
    """시스템 이벤트 로그 편의 함수"""
    logger = get_logger()
    logger.log_system_event(event=event, component=component, status=status)


# 테스트 함수
def test_logger():
    """로거 테스트"""
    print("🧪 로거 테스트를 시작합니다...")

    logger = get_logger()

    # 각종 로그 테스트
    print("\n📝 다양한 로그 타입 테스트:")

    # 1. 쿼리 실행 로그
    logger.log_query_execution(
        user_input="월별 포트인 현황 조회",
        sql_query="SELECT * FROM PY_NP_SBSC_RMNY_TXN",
        execution_time=1.234,
        result_count=150,
        success=True,
        ai_generated=True,
    )
    print("   ✅ 쿼리 실행 로그")

    # 2. 사용자 활동 로그
    logger.log_user_activity(
        activity="dashboard_access",
        details={"page": "main_dashboard", "action": "view"},
    )
    print("   ✅ 사용자 활동 로그")

    # 3. 시스템 이벤트 로그
    logger.log_system_event(
        event="application_startup", component="main_app", status="success"
    )
    print("   ✅ 시스템 이벤트 로그")

    # 4. 에러 로그
    logger.log_error(
        error_type="database_connection_error",
        error_message="연결 시간 초과",
        component="database_manager",
        context={"timeout": 30, "retry_count": 3},
    )
    print("   ✅ 에러 로그")

    # 5. 보안 이벤트 로그
    logger.log_security_event(
        event_type="suspicious_query",
        severity="medium",
        description="DROP 구문이 포함된 쿼리 시도",
        user_id="test_user",
    )
    print("   ✅ 보안 이벤트 로그")

    # 6. 성능 메트릭 로그
    logger.log_performance_metric(
        metric_name="query_execution_time",
        metric_value=2.5,
        unit="seconds",
        component="sql_generator",
    )
    print("   ✅ 성능 메트릭 로그")

    # 7. 데이터 접근 로그
    logger.log_data_access(
        table_name="PY_NP_SBSC_RMNY_TXN",
        operation="SELECT",
        record_count=1000,
        filters_applied={"date_range": "last_3_months"},
    )
    print("   ✅ 데이터 접근 로그")

    # 편의 함수 테스트
    print("\n🚀 편의 함수 테스트:")
    log_query("테스트 쿼리", "SELECT 1", 0.5, 1)
    log_user("test_activity", {"test": True})
    log_system("test_event", "test_component")
    print("   ✅ 편의 함수들")

    print("\n📂 로그 파일 확인:")
    log_dir = "logs"
    if os.path.exists(log_dir):
        log_files = [f for f in os.listdir(log_dir) if f.endswith(".log")]
        for log_file in log_files:
            file_path = os.path.join(log_dir, log_file)
            file_size = os.path.getsize(file_path)
            print(f"   📄 {log_file}: {file_size:,} bytes")
    else:
        print("   ⚠️ 로그 디렉토리가 없습니다")

    print("\n✅ 로거 테스트 완료!")


if __name__ == "__main__":
    test_logger()
