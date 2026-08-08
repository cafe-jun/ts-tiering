# Phase 1 — Parquet + S3 저장 계층 검증

## 목표

**"IoT 시계열을 Parquet으로 S3에 두고 조회하는 게 실제로 쓸 만한가"를 숫자로 답한다.**

코드를 많이 만드는 게 목표가 아니다. Phase 1의 산출물은 **측정 결과표**이고,
코드는 그 표를 만들기 위한 도구다.

## 범위 밖 (Phase 1에서 하지 않는 것)

명시적으로 적어둔다. 이 목록에 손대기 시작하면 Phase 1은 끝나지 않는다.

- Kafka 연동 — Phase 2
- 회사 Cassandra 연결 — Phase 3
- hot/cold 쿼리 라우터 — Phase 2
- compaction / reconciliation — Phase 3
- ThingsBoard 어댑터 — Phase 3

Phase 1은 **합성 데이터만** 사용하고 회사 인프라를 전혀 건드리지 않는다.

## 종료 조건 (Exit Criteria)

아래 4개가 모두 채워지면 Phase 1 완료.

1. 합성 데이터 1억 건을 Parquet으로 S3에 적재할 수 있다
2. 파티셔닝 스킴 3종에 대한 **스캔량 / 쿼리 지연 / 압축률 비교표**가 있다
3. DuckDB와 Athena 양쪽에서 같은 쿼리를 돌린 **비교 결과**가 있다
4. ADR 3건이 작성되어 있다 (Parquet 라이브러리 선택 / 값 타입 표현 / 파티션 스킴)

---

## 주차별 계획 (6~8주, 주말 기준)

### W1 — 스캐폴딩 + 도메인 모델 + 데이터 생성기

- Gradle 8.x wrapper, 멀티모듈 구성 (`core`, `storage-s3`, `bench`)
- `core`: `Datapoint`, `TimeRange`, `PartitionSpec` 인터페이스
- `bench`: 합성 데이터 생성기 (ThingsBoard `ts_kv` 형태 모사)
  - tenant / deviceProfile / deviceId / key / ts / value
  - 값 분포를 현실적으로: 온도는 완만한 sine + 노이즈, 상태값은 대부분 불변
  - **평평한 난수는 금물** — 압축률 측정이 무의미해진다
- 산출: `./gradlew :bench:generate --count=10_000_000` 이 로컬 파일로 떨어짐

**Exit:** 1천만 건이 JSON/CSV로 생성되고, 크기와 생성 시간이 기록됨

### W2 — Parquet 쓰기 (이 프로젝트의 첫 관문)

- `parquet-java` vs `Apache Arrow Java` 실제로 둘 다 붙여본다
- Hadoop 의존성이 얼마나 딸려오는지, 없이 쓸 수 있는지 확인
- Arrow는 `--add-opens java.base/java.nio=ALL-UNNAMED` JVM 옵션 필요
- **결정 사항: 값 타입 표현**
  - TB `ts_kv`는 `bool_v / str_v / long_v / dbl_v` 4컬럼으로 분리
  - Parquet에서도 4컬럼(sparse)으로 갈지, 단일 컬럼 + 타입 태그로 갈지
  - 압축률과 쿼리 편의가 갈리는 지점 → 둘 다 만들어서 비교

**Exit:** ADR-0001(라이브러리), ADR-0002(값 타입) 작성. 1천만 건 Parquet 생성 성공

### W3 — S3 적재 (LocalStack)

- `deploy/docker-compose.dev.yml`에 LocalStack
- multipart upload, 파일 크기별 업로드 시간 측정
- 1억 건으로 스케일업 (파일 수, 총 크기, 소요 시간 기록)

**Exit:** 1억 건이 S3(LocalStack)에 적재됨. 압축률 실측치 확보

### W4 — DuckDB 조회

- `httpfs` 확장으로 S3 Parquet 직접 조회
- 대표 쿼리 3종 정의 (이 쿼리로 모든 벤치마크를 돌린다)
  - Q1: 단일 디바이스 / 단일 키 / 7일 (좁은 범위)
  - Q2: 단일 디바이스 / 단일 키 / 1년 일평균 (긴 범위 + 집계)
  - Q3: 프로파일 전체 / 단일 키 / 1개월 평균 (넓은 범위)
- p50/p95, 스캔 바이트 기록

**Exit:** 3종 쿼리 baseline 수치 확보

### W5~W6 — 파티셔닝 스킴 3종 비교

같은 데이터를 3가지 경로 규칙으로 재적재하고 W4의 쿼리 3종을 각각 돌린다.

| 스킴 | 경로 | 예상 문제 |
|---|---|---|
| A | `date=/hour=` | 디바이스 하나 보려고 전체 스캔 |
| B | `tenant=/profile=/date=/hour=` | 균형점 후보 |
| C | `tenant=/deviceId=/date=` | 디렉터리 폭발 (small file) |

- 각 스킴별로 Parquet 내부 정렬 순서도 변수로 둔다 (`deviceId,ts` vs `ts`)
- row group statistics로 프루닝이 실제로 걸리는지 확인

**Exit:** 3×3 매트릭스 완성. ADR-0003 작성

### W7 — 결과 정리 + 공개

- `docs/benchmark/` 에 결과표 정리
- README에 아키텍처 다이어그램 + 핵심 수치
- **블로그 1편 발행** — "IoT 시계열 Parquet 파티셔닝 실측"

> 여기서 반드시 외부에 공개한다. 코드를 더 쓰기 전에.

### W8 — Athena 비교 (실 AWS, 선택)

- 실제 S3 + Glue Catalog + Athena
- 같은 쿼리 3종, 스캔 바이트와 비용 비교
- **블로그 2편** — "DuckDB vs Athena, 어느 규모에서 갈리는가"

**Exit:** Phase 1 완료

---

## 측정 지표 (before/after를 반드시 기록)

| 지표 | 측정 시점 | 비고 |
|---|---|---|
| 압축률 (원본 대비) | W3 | 이력서 문장의 핵심 |
| 파일 수 / 평균 파일 크기 | W3, W5 | small file 문제 정량화 |
| 쿼리 p50 / p95 | W4, W6 | 쿼리 3종 각각 |
| 쿼리당 스캔 바이트 | W4, W6 | 파티셔닝 효과의 직접 증거 |
| 적재 처리량 (건/초) | W3 | archiver 설계 입력값 |

측정할 때마다 `docs/benchmark/`에 날짜와 함께 남긴다. 나중에 재현하려면 훨씬 귀찮다.

---

## 리스크

| 리스크 | 대응 |
|---|---|
| W2 Hadoop 의존성에서 좌초 | 타임박스 2주. 안 되면 Arrow로 선회하고 그 자체를 ADR로 남김 |
| 합성 데이터가 비현실적이라 압축률이 왜곡 | W1에서 값 분포 설계에 시간 투자 |
| W5~6 벤치마크가 지루해서 이탈 | W7 블로그를 W6 직후 바로 쓴다. 완주 전에 공개 |
| 1억 건이 로컬에서 버거움 | 1천만 건으로 전 과정 검증 후 마지막에만 확대 |

## 환경

- Java 17 (Homebrew OpenJDK) — Spring Boot 3.x baseline
- Gradle 8.x (wrapper로 고정, 시스템 7.3.3 사용 안 함)
- Docker 24.0.7 — LocalStack, 이후 Redpanda

## 개발 도구 결정 (2026-08-08)

| 도구 | Phase 1 | 이후 | 근거 |
|---|---|---|---|
| Ponytail | 미적용 | 미적용 | "덜 쓰기" 축. 경계/정합성 로직에서 오히려 구멍을 만듦 |
| MoAI-ADK | **미적용** | Phase 2부터 검토 | Phase 1은 탐색·측정 단계. 버려질 실험 코드에 SPEC/TDD 강제는 순수 오버헤드 |

Phase 2 진입 시점(`archiver`, `query`가 프로덕션 코드가 되는 시점)에 MoAI-ADK를 재검토한다.
그때 확인할 것: Gradle 멀티모듈 감지 여부, 실제 토큰 소모량.
