# ts-tiering

[![build](https://github.com/cafe-jun/ts-tiering/actions/workflows/build.yml/badge.svg)](https://github.com/cafe-jun/ts-tiering/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

IoT 시계열 데이터를 hot(Cassandra) / cold(S3 Parquet) 계층으로 나누고,
**조회하는 쪽은 그 경계를 모르게** 만드는 티어링 계층.

> 상태: **Phase 1 / W3 적재 완료** — [계획](docs/PHASE1.md) · [벤치마크](docs/benchmark/) · [ADR](docs/adr/)

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
| `storage-parquet` | Parquet 쓰기 (Hadoop 없이), 푸터 파싱 | 1 |
| `storage-s3` | S3 I/O | 1 |
| `bench` | 합성 데이터 생성 + 벤치마크 하네스 | 1 |
| `archiver` | Kafka → Parquet → S3 (스트림) | 2 |
| `query` | hot/cold 라우팅 + REST API | 2 |
| `storage-cassandra` | Cassandra 읽기 전용 | 2 |
| `backfiller` | Cassandra → S3 과거 데이터 이관 | 3 |
| `compactor` | 작은 파일 병합 | 3 |
| `reconciler` | 원본/아카이브 정합성 검증 | 3 |

## 설계 결정

- [ADR-0001 — Parquet 라이브러리와 Hadoop 의존성](docs/adr/0001-parquet-library.md)
- [ADR-0002 — 텔레메트리 값의 Parquet 표현](docs/adr/0002-value-layout.md)

## 벤치마크

[docs/benchmark/](docs/benchmark/) 참고.

- [W1 기준선](docs/benchmark/w1-baseline.md) — 합성 데이터 생성기와 값 분포 검증
- [W2 레이아웃 × 코덱](docs/benchmark/w2-parquet-layout.md) — PER_KEY_TYPED + ZSTD 채택 근거
- [W3 granularity 프로브](docs/benchmark/w3-partition-granularity.md) — 시 단위 파티션이 일 단위보다 **2.83배 크다**
- [W3 S3 적재](docs/benchmark/w3-s3-ingest.md) — 1년치 **134,028,000 건 → 132.0 MiB / 5,475 객체**

> 압축비 190.6배는 **NDJSON 대비**다. NDJSON 은 아무도 실제로 쓰지 않는 뚱뚱한 분모이므로
> 그대로 인용하면 안 된다. 정직한 분모(Cassandra 실제 디스크) 산출은 아직 남아 있다.

## 개발

```bash
./gradlew build                                                    # 빌드 + 테스트
./gradlew :bench:generate --args="--count=10_000_000 --out=data/raw-10m.ndjson"
./gradlew :bench:parquetBench --args="--count=10_000_000"      # 레이아웃 × 코덱 비교
```

Java 17 / Gradle 8.10.2 (wrapper 사용).

## 라이선스

[Apache License 2.0](LICENSE)
