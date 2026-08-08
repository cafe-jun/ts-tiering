# ts-tiering

IoT 시계열 데이터를 hot(Cassandra) / cold(S3 Parquet) 계층으로 나누고,
**조회하는 쪽은 그 경계를 모르게** 만드는 티어링 계층.

> 상태: Phase 1 진행 중 — [계획](docs/PHASE1.md)

## 문제

IoT 시계열은 접근 빈도가 극단적으로 비대칭이다.
최근 데이터는 대시보드가 초당 수백 번 읽고, 6개월 전 데이터는 한 달에 몇 번 읽는다.
그런데 둘 다 같은 저장소에서 같은 GB당 비용을 낸다.

## 구조

```
Kafka (telemetry)
  ├→ Cassandra          hot  : 최근 N일
  └→ archiver → S3      cold : 전체 기간 (Parquet)
                 ↑
     Query Router ──────┘
     시간 범위로 라우팅 + 경계 구간 병합/중복제거
```

## 모듈

| 모듈 | 역할 | Phase |
|---|---|---|
| `core` | 도메인 모델, 파티션 규칙 인터페이스 | 1 |
| `storage-s3` | Parquet 읽기/쓰기, S3 I/O | 1 |
| `bench` | 합성 데이터 생성 + 벤치마크 하네스 | 1 |
| `archiver` | Kafka → Parquet → S3 (스트림) | 2 |
| `query` | hot/cold 라우팅 + REST API | 2 |
| `storage-cassandra` | Cassandra 읽기 전용 | 2 |
| `backfiller` | Cassandra → S3 과거 데이터 이관 | 3 |
| `compactor` | 작은 파일 병합 | 3 |
| `reconciler` | 원본/아카이브 정합성 검증 | 3 |

## 설계 결정

[docs/adr/](docs/adr/) 참고.

## 벤치마크

[docs/benchmark/](docs/benchmark/) 참고. (Phase 1 W7에 채워짐)
