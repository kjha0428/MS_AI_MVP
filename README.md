# MS_AI_MVP
# 📘 Azure 기반 생성형 AI 프로젝트 제안서

## ✅ 프로젝트: 번호이동정산 데이터 확인 SQL 쿼리 자동 생성 에이전트

### 📌 개요 및 목적

번호이동정산 관련 데이터 확인 문의 메일이 접수되면, **메일 내용을 분석하여 적절한 JOIN 기반 SQL 쿼리를 자동으로 생성**해주는 AI Agent 시스템입니다.

**핵심 기능:**
- 고객 정보 기반 특정 금액 확인 쿼리 생성
- 월별 정산 금액 비교 및 집계 쿼리 생성
- 복잡한 JOIN 쿼리 자동 생성
- 번호이동정산 도메인 특화 쿼리 패턴 적용

**주요 처리 시나리오:**
1. **개별 고객 정산 금액 확인**: 고객 정보 → 관련 테이블 JOIN → 특정 금액 검증
2. **월별 정산 금액 변동 분석**: 전월 대비 금액 변동 → SUM 집계 → 건수 및 금액 비교

### 🔧 활용 기술 및 Azure 서비스

**AI 및 자연어 처리:**
- **Azure OpenAI Service (GPT-4)**: 메일 내용 분석, JOIN 쿼리 생성
- **Azure Cognitive Services**: 고객 정보 추출 및 의도 파악

**데이터 처리:**
- **Azure SQL Database**: 번호이동정산 관련 테이블 (고객정보, 정산내역, 요금정보 등)
- **Azure Blob Storage**: 테이블 스키마 정보, 컬럼 메타데이터 저장
- **Azure Cognitive Search**: 테이블 스키마, 관계 정보, JOIN 패턴 검색

**UI 및 워크플로우:**
- **Streamlit**: 웹 기반 챗봇 인터페이스
- **Azure Functions**: 서버리스 기반 쿼리 생성 로직
- **Azure App Service**: Streamlit 앱 호스팅

**모니터링:**
- **Azure Monitor**: 시스템 상태 및 성능 모니터링
- **Azure Application Insights**: 쿼리 생성 성공률 추적

### 🧩 아키텍처

```
[Streamlit 챗봇 UI] → [사용자 메일 내용 입력] → [Azure Functions API]
                                                          ↓
[메일 내용 분석] → [고객정보/요청유형 추출] → [Azure OpenAI]
                                                          ↓
[테이블 스키마 로드] ← [Blob Storage] ← [테이블/컬럼 정보]
                                                          ↓
[시나리오 분류] → [개별 확인 / 월별 비교] → [쿼리 패턴 선택]
                                                          ↓
[적절한 테이블 선택] → [컬럼 매핑] → [JOIN 관계 설정]
                                                          ↓
[JOIN 쿼리 생성] → [쿼리 검증] → [문법/성능 체크]
                                                          ↓
[Streamlit UI 출력] ← [생성된 쿼리 반환] ← [결과 포맷팅]
```

### 📋 테이블 스키마 정보 관리

**스키마 정보 구성:**
```json
{
  "tables": {
    "customer_info": {
      "description": "고객 기본 정보",
      "columns": {
        "customer_id": {"type": "varchar", "description": "고객ID"},
        "phone_number": {"type": "varchar", "description": "전화번호"},
        "name": {"type": "varchar", "description": "고객명"},
        "carrier": {"type": "varchar", "description": "통신사"}
      },
      "primary_key": "customer_id"
    },
    "settlement_history": {
      "description": "번호이동 정산 이력",
      "columns": {
        "settlement_id": {"type": "varchar", "description": "정산ID"},
        "customer_id": {"type": "varchar", "description": "고객ID"},
        "settlement_amount": {"type": "decimal", "description": "정산금액"},
        "settlement_date": {"type": "date", "description": "정산일자"},
        "settlement_type": {"type": "varchar", "description": "정산유형"}
      },
      "foreign_keys": {
        "customer_id": "customer_info.customer_id"
      }
    }
  }
}
```

**활용 방식:**
- 테이블 및 컬럼 정보를 JSON 형태로 저장
- AI가 스키마 정보를 참조하여 정확한 JOIN 생성
- 컬럼명과 설명을 통한 의미적 매핑
- 외래키 정보를 통한 자동 관계 설정

### 🖥️ Streamlit 챗봇 인터페이스 구성
- 채팅 형태의 대화 인터페이스
- 메일 내용 입력 텍스트 박스
- 실시간 쿼리 생성 진행 상태 표시

**주요 기능:**
```python
# Streamlit 앱 구성 예시
import streamlit as st
import requests

st.title("🔍 번호이동정산 SQL 쿼리 생성기")

# 채팅 인터페이스
if "messages" not in st.session_state:
    st.session_state.messages = []

# 메일 내용 입력
user_input = st.chat_input("메일 내용을 입력하세요...")

if user_input:
    # 사용자 메시지 표시
    with st.chat_message("user"):
        st.write(user_input)
    
    # AI 응답 (쿼리 생성)
    with st.chat_message("assistant"):
        response = generate_sql_query(user_input)
        st.code(response, language="sql")
```

**추가 기능:**
- 쿼리 복사 버튼
- 쿼리 히스토리 사이드바
- 자주 사용하는 패턴 즐겨찾기
- 쿼리 실행 시뮬레이션 (선택사항)

### 🎯 주요 쿼리 생성 패턴
```sql
-- 예시: 고객 정보 기반 번호이동 정산 금액 확인
SELECT 
    c.customer_id,
    c.phone_number,
    s.settlement_amount,
    s.settlement_date,
    f.fee_type,
    f.fee_amount
FROM customer_info c
JOIN settlement_history s ON c.customer_id = s.customer_id
JOIN fee_details f ON s.settlement_id = f.settlement_id
WHERE c.phone_number = '010-1234-5678'
  AND s.settlement_date >= '2024-01-01'
```

**2. 월별 정산 금액 변동 분석**
```sql
-- 예시: 전월 대비 정산 금액 변동 분석
SELECT 
    DATE_FORMAT(settlement_date, '%Y-%m') as settlement_month,
    COUNT(*) as total_count,
    SUM(settlement_amount) as total_amount,
    AVG(settlement_amount) as avg_amount,
    LAG(SUM(settlement_amount)) OVER (ORDER BY DATE_FORMAT(settlement_date, '%Y-%m')) as prev_month_amount
FROM settlement_history
WHERE settlement_date >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
GROUP BY DATE_FORMAT(settlement_date, '%Y-%m')
ORDER BY settlement_month
```

### 🔍 기대 효과

**업무 효율성:**
- 복잡한 JOIN 쿼리 작성 시간 85% 단축
- 고객 정보 기반 데이터 검색 자동화
- 월별 정산 분석 업무 표준화

**사용자 인터페이스:**
- 직관적인 챗봇 형태의 웹 인터페이스
- 메일 내용 복사&붙여넣기 간편 입력
- 실시간 쿼리 생성 및 결과 확인
- 쿼리 히스토리 및 재사용 기능

**시스템 접근성:**
- 웹 브라우저만으로 접근 가능
- 별도 소프트웨어 설치 불필요
- 모바일 환경에서도 사용 가능

**분석 품질:**
- 전월 대비 변동 분석 자동화
- 다양한 집계 지표 제공
- 데이터 트렌드 파악 지원

### ⚠️ 구현 시 고려사항

**데이터 보안:**
- 고객 개인정보 접근 권한 관리
- 민감한 정산 데이터 마스킹
- 쿼리 실행 권한 제한 (읽기 전용)

**쿼리 품질:**
- 복잡한 JOIN 성능 최적화
- 인덱스 활용 쿼리 패턴 적용
- 대용량 데이터 처리 시 페이징 고려

**시스템 안정성:**
- 제공된 테이블 스키마 정보 기반 검증
- 존재하지 않는 컬럼/테이블 참조 방지
- 스키마 정보 업데이트 시 자동 반영



### 🎉 결론

번호이동정산 데이터 확인 SQL 쿼리 자동 생성 프로젝트는 고객 정보 기반 개별 확인과 월별 정산 변동 분석이라는 두 가지 핵심 업무를 자동화하여 담당자의 업무 효율성을 크게 향상시킬 수 있는 실용적인 솔루션입니다. 복잡한 JOIN 쿼리와 집계 분석을 자동으로 생성하여 정확하고 신속한 데이터 확인이 가능합니다.
