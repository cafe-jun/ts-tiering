# W3 — Cassandra 기준선 (정직한 분모)

측정일: 2026-08-09
환경: Cassandra 4.1.11 (Docker 단일 노드, RF=1), macOS darwin 23.4.0 / Apple Silicon
데이터: A안 구성의 30일치 = 11,016,000 행 (테넌트 3 × 디바이스 17 × 키 5, 60초 주기)

```
docker compose -f deploy/docker-compose.dev.yml up -d cassandra
./gradlew :bench:cassandraBaseline --args="--days=30 --devices-per-tenant=17 --interval-seconds=60"
docker exec ts-tiering-cassandra nodetool flush thingsboard ts_kv_cf
docker exec ts-tiering-cassandra nodetool tablestats thingsboard.ts_kv_cf
```

## 왜 쟀나

W1·W2·W3 문서가 반복해서 같은 지적을 남겼다 — **NDJSON 대비 배수는 의미가 약하다.**
필드명과 UUID(36자)를 매 행 텍스트로 반복하는, 아무도 실제로 쓰지 않는 포맷이 분모이기 때문이다.
회사를 설득할 때 필요한 문장은 "JSON 보다 190배 작다"가 아니라
**"지금 Cassandra 가 먹고 있는 디스크의 몇 분의 일인가"**다.

해석적 모델(행 오버헤드 × 행 수)을 쓰지 않았다. 셀 헤더·타임스탬프 델타·LZ4 청크 경계가
얽혀 2배쯤 틀리기 쉽고, 그러면 결론을 인용할 수 없다. 대신 ThingsBoard 스키마를 로컬에
그대로 세우고 같은 데이터를 넣어 SSTable 크기를 실측했다.

**회사 Cassandra 에는 접속하지 않았다** (PHASE1.md 범위). 로컬 재현이다.

## 스키마

ThingsBoard 의 `ts_kv_cf` 를 그대로 옮겼다. 충실도가 곧 숫자의 신뢰도다.

```cql
CREATE TABLE thingsboard.ts_kv_cf (
    entity_type text, entity_id uuid, key text, partition bigint, ts bigint,
    bool_v boolean, str_v text, long_v bigint, dbl_v double, json_v text,
    PRIMARY KEY ((entity_type, entity_id, key, partition), ts)
);
```

- **파티션 규칙**은 TB 기본값 `TS_KV_PARTITIONING=MONTHS` 를 따랐다. 파티션을 임의로 자르면
  파티션 헤더 대 행 수의 비율이 달라져 크기가 통째로 바뀐다
- **압축 설정은 건드리지 않았다.** Cassandra 기본값(LZ4)이 운영에서 쓰이는 값이고,
  여기서 손대면 분모가 우리에게 유리하게 왜곡된다
- INSERT 는 값 컬럼 하나만 채운다. TB 도 그렇게 쓰므로 나머지 컬럼은 셀 자체가 생기지 않는다
- `entity_id` 만 `timeuuid` → `uuid` 로 바꿨다. 합성 UUID 가 버전 1이 아니라서인데,
  두 타입 모두 저장은 16바이트라 크기에 영향이 없다

## 결과

| 지표 | 값 |
|---|---|
| 행 수 | 11,016,000 |
| **SSTable 크기 (`Space used (live)`)** | **129,888,500 bytes (123.9 MiB)** |
| 교차검증 (`du` on disk) | 129,906,098 bytes — 차이 17,598 B (0.014%) |
| 행당 | **11.79 bytes** |
| 파티션 수 | 255 (= 51 디바이스 × 5 키 × 월 파티션 1개) |
| 적재 | 99.4s (110,803 rows/s) |

### compaction 을 돌리지 않은 근거

SSTable 이 4개로 남아 있지만 병합하지 않았다. 부가 파일이 전부 합쳐 약 200 KB —
**전체의 0.15%** 다.

| 파일 | 합계 |
|---|---|
| `*-Data.db` (4개) | 129,555,167 B |
| `*-Index.db` | 180,015 B |
| `*-Statistics.db` | 23,348 B |
| `*-Filter.db` / `*-Summary.db` | 1,344 / 1,044 B |

각 행의 `ts` 가 겹치지 않아 SSTable 간 행 중복도 없다. 병합해도 숫자가 유의미하게 바뀌지 않는다.

## 정직한 분모로 본 압축률

**같은 30일치 데이터**를 세 방식으로 저장한 결과다. 외삽이 없다.

| 저장 방식 | 크기 | 행당 | Parquet 대비 |
|---|---|---|---|
| NDJSON | 2,167,232,293 B | 196.7 B | 192.3x |
| **Cassandra `ts_kv` (RF=1)** | **129,888,500 B** | **11.79 B** | **11.5x** |
| **Parquet + ZSTD** | **11,272,888 B** | **1.02 B** | 1.00x |

> **인용할 숫자는 11.5배다.** 190배가 아니다.

NDJSON 이 얼마나 부적절한 분모였는지도 같이 드러난다 —
**Cassandra 자체가 이미 NDJSON 을 16.7배 압축하고 있었다.**
190배 중 실제로 Parquet 이 기여한 몫은 11.5배이고 나머지는 "JSON 이 뚱뚱했다"는 뜻이었다.

### 1년 환산

| | Cassandra (RF=1) | Parquet + ZSTD |
|---|---|---|
| 134,028,000 건 / 365일 | 약 1,580,310,566 B (**1.47 GiB**) | 138,414,594 B (**132.0 MiB**) — 실측 |
| 디바이스 1대당 | 29.6 MiB | 2.59 MiB |

행당 바이트가 파티션 수에 거의 영향받지 않으므로(월 파티션이 12배로 늘어도 행 수는 그대로)
Cassandra 쪽 외삽은 안전하다. Parquet 쪽은 외삽이 아니라 [실측](w3-s3-ingest.md)이다.

## 이 숫자의 한계

**1. RF=1 이다.** 운영 Cassandra 는 보통 RF=3 이고 디스크는 그대로 3배가 된다.
그 기준이면 **약 34배**다. S3 도 내부적으로 복제하지만 그건 GB당 가격에 이미 포함돼 있으므로,
비용 비교로는 "RF=3 Cassandra 디스크 vs S3 객체 바이트"가 맞다.
다만 곱셈이 들어간 값이니 11.5배와 섞어 쓰지 말고 따로 표기한다.

**2. TTL 이 없다.** ThingsBoard 가 텔레메트리에 TTL 을 걸면 셀마다 만료 정보가 붙어
Cassandra 쪽이 더 커진다. 즉 **11.5배는 보수적인 하한**이다.

**3. Cassandra 4.1 (`big` SSTable 포맷) 기준이다.** 5.0 의 BTI 포맷은 인덱스 구조가 달라
수치가 조금 움직일 수 있다. 회사가 쓰는 버전에 맞춰 다시 재는 것이 정확하다.

**4. 합성 데이터다.** 값 분포는 W1 에서 실제 센서 특성에 맞춰 설계했지만
(`SensorsTest` 가 회귀로 고정), 실제 테넌트의 키 구성·카디널리티와는 다르다.

**5. 단일 노드다.** 실제 클러스터의 hint, repair 잔여물, 스냅샷은 포함돼 있지 않다 —
운영 디스크 사용량은 이보다 크면 컸지 작지 않다.

→ 종합하면 **11.5배는 하한**이고, 실제 운영 환경에서는 이보다 유리하게 나올 가능성이 높다.
