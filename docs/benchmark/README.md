# 벤치마크

Phase 1 의 산출물은 코드가 아니라 이 디렉터리다. 측정마다 날짜·환경·재현 명령을 함께 남긴다.

## 한눈에

**같은 1년치 데이터(134,028,000건 / 디바이스 51대 / 60초 주기)를 저장했을 때**

| | 크기 | 행당 |
|---|---|---|
| NDJSON | 24.6 GiB | 196.8 B |
| Cassandra `ts_kv` (RF=1) | 약 1.47 GiB | 11.79 B |
| **Parquet + ZSTD (S3)** | **132.0 MiB** | **1.02 B** |

**인용할 숫자는 Cassandra 대비 11.5배.** NDJSON 대비 190배가 아니다 —
Cassandra 자체가 이미 NDJSON 을 16.7배 압축하고 있어서, 그 배수의 대부분은
"JSON 이 뚱뚱했다"는 뜻일 뿐이다.

**채택한 레이아웃** (ADR-0002 · [0004](../adr/0004-partition-scheme.md))

```
s3://bucket/tenant=<uuid>/date=<YYYY-MM-DD>/key=<키>/part-N.parquet
  → tenant_id, device_id, ts, value        PER_KEY_TYPED + ZSTD
  → 파일 안에서 (device_id, ts) 정렬, Parquet v2
```

## 측정 목록

| 문서 | 무엇을 답했나 |
|---|---|
| [W1 기준선](w1-baseline.md) | 합성 데이터의 값 분포가 실제 센서의 압축 특성을 갖는가. A안 데이터셋 확정 |
| [W2 레이아웃 × 코덱](w2-parquet-layout.md) | 값을 4컬럼(sparse) / 문자열 / 키별 타입 중 무엇으로 담을까 → PER_KEY_TYPED + ZSTD |
| [W3 granularity 프로브](w3-partition-granularity.md) | 시 단위 파티션이 일 단위보다 **2.83배 크다**. 파일당 고정 오버헤드 약 2.0 KB |
| [W3 S3 적재](w3-s3-ingest.md) | 1년치 → 132.0 MiB / 5,475 객체. **업로드 시간은 바이트가 아니라 객체 수가 지배** |
| [W3 Cassandra 기준선](w3-cassandra-baseline.md) | 정직한 분모. `ts_kv` 로 11.79 B/행 → **Parquet 이 11.5배 작다** |
| [W4 DuckDB 조회](w4-duckdb-baseline.md) | 프루닝은 99.4% 걸리는데 **지연은 객체 나열이 지배한다** (531ms) |
| [W5~W6 파티션 스킴](w5-partition-schemes.md) | 디바이스 파티션은 **바이트 26배 절감 / 지연 43배 악화**. 정렬 + Parquet v2 는 순수 이득 |

## 반복해서 나온 것

측정을 거듭할수록 같은 이야기가 다른 자리에서 계속 나왔다.

**1. 비용은 바이트가 아니라 객체 수에 붙는다.**
업로드에서(W3: 객체당 4.4ms × 5,475), 조회에서(W4: 나열만 531ms),
스킴 비교에서(W5: C 는 바이트 26배 적은데 지연 43배) 모두 같은 결론이었다.
이 규모에서 파일을 잘게 쪼개는 대가는 용량·적재·조회 세 곳 모두에 붙는다.

**2. 분모를 밝히지 않은 배수는 의미가 없다.**
338배 → 190배 → 11.5배. 같은 데이터, 같은 Parquet 인데 30배가 왔다 갔다 했다.

**3. 라이브러리 기본값이 결론을 가릴 수 있다.**
`parquet-java` 의 기본 라이터 버전(v1)에는 `DELTA_BINARY_PACKED` 가 없어,
정렬의 이득이 손해로 뒤집혀 보였다. v2 로 바꾸자 −12%가 됐다.

## 재현

```bash
docker compose -f deploy/docker-compose.dev.yml up -d          # MinIO
./gradlew :bench:generate --args="--days=365 --devices-per-tenant=17 --interval-seconds=60 --out=none"
./gradlew :bench:ingest   --args="--days=30 --devices-per-tenant=17 --interval-seconds=60 --scheme=tenant-date --sort=true --s3=true"
./gradlew :bench:query    --args="--iterations=20"

docker compose -f deploy/docker-compose.dev.yml up -d cassandra  # 분모 측정용
./gradlew :bench:cassandraBaseline --args="--days=30 --devices-per-tenant=17 --interval-seconds=60"
```

진단 도구: `:bench:colProbe`(열별 크기·인코딩), `:bench:schemaProbe`(스킴이 노출하는 열).

## 공통 한계

전 측정에 걸쳐 있는 조건이다. 개별 문서에도 적어뒀지만 여기 모아둔다.

- **합성 데이터다.** 값 분포는 W1 에서 실제 센서 특성에 맞춰 설계했고 `SensorsTest` 가
  회귀로 고정하지만, 실제 테넌트의 키 구성·카디널리티와는 다르다
- **로컬 MinIO 루프백이고, 실 AWS 는 검증하지 않았다.** 실 S3 에서 LIST·GET 왕복이
  더 비싸 "나열이 지배한다"는 결론이 강해질 것으로 보이나 **미검증 가정이다.**
  관리형 엔진(Athena) 비교도 하지 않았다 — AWS 가 이 프로젝트의 범위 밖이다
- **단일 프로세스, 동시성 없음.** 대시보드가 초당 수백 번 읽는 상황은 재지 않았다
- **데이터셋이 작다.** 1년치 전체가 132 MiB 라 파일이 10~500 KiB 대이고,
  이 때문에 나열 비용이 모든 것을 지배한다. 파일이 100 MB 급이면 순위가 뒤집힐 수 있다
