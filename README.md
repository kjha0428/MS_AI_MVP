# MS_AI_MVP

# MS AI 프로젝트 제안서

# 📊 자연어 기반 AI SQL 쿼리 생성 시스템 제안서

## 📌 프로젝트 개요 및 목적

Azure OpenAI와 Streamlit을 활용하여 **번호이동정산 데이터 분석 자동화** 및 **자연어 기반 쿼리 생성**을 지원하는 AI 분석 시스템 구축 프로젝트입니다.

### 주요 목표

- 번호이동 집계 내역 데이터 시각화를 통한 추이 분석 자동화
- 자연어 기반 SQL 쿼리 생성으로 정산 데이터 조회 효율성 향상
- 정산 업무 담당자의 데이터 접근성 및 분석 속도 개선

## 🔧 활용 기술 및 Azure 서비스

### Azure 서비스

- **Azure OpenAI Service (GPT-4)**: 자연어 → SQL 쿼리 변환
- **Azure SQL Database**: 번호이동정산 데이터 저장 및 조회
- **Azure App Service**: Streamlit 웹 애플리케이션 호스팅
- **Azure Key Vault**: 데이터베이스 연결 정보 및 API 키 보안 관리

### 기술 스택

- **Frontend**: Streamlit (Python)
- **Backend**: Python, SQLAlchemy
- **Database**: Azure SQL Database
- **AI/ML**: Azure OpenAI GPT-4, Plotly 차트 생성

## 🧩 시스템 아키텍처

```
[사용자 입력] → [Streamlit UI]
                    ↓
[추이 분석 모듈] ← [Azure OpenAI] → [쿼리 생성 모듈]
        ↓                              ↓
[Plotly 차트 생성] ← [Azure SQL DB] → [정산 데이터 조회]
        ↓                              ↓
[실시간 대시보드] ← [결과 출력] → [챗봇 인터페이스]

```

## 🎯 주요 기능

### 1. 번호이동 추이 분석 대시보드

- **조회 기간**: 조회일 기준 전월 3개월간 데이터 자동 집계
- **시각화**: 포트인/포트아웃 별도 추이 그래프 생성
- **실시간 업데이트**: 화면 상단에 상시 표시

### 2. 자연어 기반 SQL 쿼리 생성 챗봇

### 기능 A: 집계 조회 쿼리 생성

- **입력**: 월, 사업자명, 포트인/포트아웃 여부
- **출력**: 해당 조건별 집계 내역 SUM 조회 쿼리

### 기능 B: 정산 검증 쿼리 생성

- **입력**: 전화번호 또는 서비스계약번호
- **출력**: 번호이동정산 관련 테이블 JOIN 쿼리
- **검증**: 정산 금액 정상 적재 확인

## 💡 주요 기술 특징

### AI 기반 쿼리 생성

- GPT-4를 활용한 자연어 처리로 복잡한 SQL 작성 불필요
- 테이블 스키마 정보 자동 참조하여 정확한 쿼리 생성
- 사용자 친화적인 챗봇 인터페이스 제공

### 실시간 데이터 시각화

- Plotly 기반 인터랙티브 차트 생성
- 포트인/포트아웃 데이터 분리 시각화
- 반응형 웹 인터페이스로 다양한 디바이스 지원

### 보안 및 권한 관리

- Azure Key Vault를 통한 민감 정보 보호
- 읽기 전용 데이터베이스 연결로 데이터 안전성 확보
- 쿼리 실행 전 검증 단계 적용

## ⚠️ 구현 시 주의사항

### 데이터 보안

- 개인정보 포함 필드 마스킹 처리 필요
- 정산 데이터 접근 권한 관리 강화
- 쿼리 실행 결과 로깅 및 감사 체계 구축

### 성능 최적화

- 대용량 데이터 조회 시 페이징 처리 적용
- 자주 사용되는 쿼리 결과 캐싱
- 데이터베이스 인덱스 최적화 권장

### AI 모델 관리

- 쿼리 생성 정확도 향상을 위한 프롬프트 엔지니어링
- 테이블 스키마 변경 시 모델 재훈련 필요
- 오류 쿼리 생성 시 사용자 피드백 수집 체계

### 사용자 경험

- 복잡한 조인 쿼리 생성 시 실행 전 확인 단계 추가
- 쿼리 실행 시간 안내 및 진행 상황 표시
- 에러 발생 시 사용자 친화적 메시지 제공

## 💼 활용 예시

### 시나리오 1: 월별 번호이동 트렌드 분석

**상황**: 정산 담당자가 최근 3개월간 번호이동 패턴을 확인하고 싶은 경우

**사용법**:

- 시스템 접속 시 화면 상단에 자동으로 표시되는 대시보드 확인
- 포트인/포트아웃 별도 그래프로 트렌드 비교 분석
- 건수/금액 기준 필터링으로 세부 분석 가능

**결과**:

- "지난 3개월간 포트인 증가율 15%, 포트아웃 감소율 8% 확인"
- 월별 변동 패턴을 통한 계절성 분석 가능

### 시나리오 2: 특정 사업자 정산 현황 조회

**상황**: "2024년 1월 A통신사 포트인 정산 금액을 확인하고 싶어요"

**사용법**:

`사용자 입력: "2024년 1월 A통신사 포트인 정산 금액 알려줘"
AI 응답: 다음 쿼리를 생성했습니다.
  SELECT SUM(settlement_amount) as total_amount
  FROM provisional_receipt 
  WHERE YEAR(transaction_date) = 2024 
    AND MONTH(transaction_date) = 1 
    AND operator_name = 'A통신사' 
    AND port_type = 'PORT_IN'`

**결과**: 자동 생성된 쿼리로 즉시 정산 금액 확인 가능

### 시나리오 3: 개별 번호 정산 검증

**상황**: "010-1234-5678 번호의 정산 내역이 정상적으로 처리되었는지 확인하고 싶어요"

**사용법**:

`사용자 입력: "010-1234-5678 번호 정산 내역 확인해줘"
AI 응답: 번호이동 정산 검증 쿼리를 생성했습니다.
  SELECT 
      h.phone_number,
      h.service_contract_id,
      h.port_type,
      h.transaction_date,
      p.settlement_amount as provisional_amount,
      d.deposit_amount,
      (p.settlement_amount - d.deposit_amount) as net_settlement
  FROM number_port_history h
  LEFT JOIN provisional_receipt p ON h.transaction_id = p.transaction_id
  LEFT JOIN deposit_account d ON h.service_contract_id = d.service_contract_id
  WHERE h.phone_number = '010-1234-5678'`

**결과**: 가수납-예치금 차액 계산으로 정산 정확성 즉시 검증

### 시나리오 4: 이상 징후 탐지

**상황**: "최근 정산 금액이 급증한 사업자가 있는지 확인하고 싶어요"

**사용법**:

`사용자 입력: "이번 달 정산 금액이 평소보다 많이 증가한 사업자 찾아줘"
AI 응답: 월별 사업자 정산 증가율 분석 쿼리를 생성했습니다.
  WITH monthly_summary AS (
      SELECT 
          operator_name,
          YEAR(transaction_date) as year,
          MONTH(transaction_date) as month,
          SUM(settlement_amount) as monthly_amount
      FROM provisional_receipt 
      WHERE transaction_date >= DATEADD(month, -3, GETDATE())
      GROUP BY operator_name, YEAR(transaction_date), MONTH(transaction_date)
  ),
  growth_analysis AS (
      SELECT 
          operator_name,
          monthly_amount,
          LAG(monthly_amount) OVER (PARTITION BY operator_name ORDER BY year, month) as prev_amount,
          ((monthly_amount - LAG(monthly_amount) OVER (PARTITION BY operator_name ORDER BY year, month)) / LAG(monthly_amount) OVER (PARTITION BY operator_name ORDER BY year, month)) * 100 as growth_rate
      FROM monthly_summary
  )
  SELECT operator_name, monthly_amount, prev_amount, growth_rate
  FROM growth_analysis 
  WHERE growth_rate > 50
  ORDER BY growth_rate DESC`

**결과**: 정산 금액 급증 사업자 리스트 및 증가율 자동 분석

## 🎯 기대 효과

### 업무 효율성

- 정산 데이터 조회 시간 **단축**
- 수동 쿼리 작성 오류 **감소**
- 번호이동 추이 분석 자동화로 **업무 시간 단축**

### 데이터 접근성

- 업무 담당자가 아니어도 복잡한 정산 데이터 쉽게 조회 가능
- 실시간 대시보드로 즉시 현황 파악
- 챗봇 인터페이스로 신속한 데이터 확인

### 의사결정 지원

- 번호이동 패턴 분석을 통한 사업 전략 수립
- 정산 이상 징후 조기 발견 및 대응
- 데이터 기반 포트인/포트아웃 최적화 방안 도출

---

**🔧 기술 지원**: Azure OpenAI, Streamlit, Python
**📊 결과물**: 번호이동정산 AI 분석 시스템 웹 애플리케이션


### 🎉 결론

번호이동정산 데이터 확인 SQL 쿼리 자동 생성 프로젝트는 고객 정보 기반 개별 확인과 월별 정산 변동 분석이라는 두 가지 핵심 업무를 자동화하여 담당자의 업무 효율성을 크게 향상시킬 수 있는 실용적인 솔루션입니다. 복잡한 JOIN 쿼리와 집계 분석을 자동으로 생성하여 정확하고 신속한 데이터 확인이 가능합니다.
