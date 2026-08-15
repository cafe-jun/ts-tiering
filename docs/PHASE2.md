# Phase 2 — 스트림 적재 + hot/cold 라우팅

## 목표

**"조회하는 쪽이 hot/cold 경계를 모르게" 를 실제로 동작시킨다.**

Phase 1 은 cold 계층 하나를 정지 상태로 측정했다. Phase 2 는 그걸 **흐르는 데이터**에 붙이고,
읽는 쪽에서 두 계층을 하나처럼 보이게 만든다.

Phase 1 의 산출물이 측정 결과표였다면, Phase 2 의 산출물은 **동작하는 파이프라인**이다.
다만 성격이 바뀌었다고 측정을 그만두지는 않는다 — 아래 종료 조건은 전부 숫자로 확인 가능해야 한다.

## Phase 1 이 넘겨준 것

### 정해진 것 (그대로 쓴다)

| | 결정 | 근거 |
|---|---|---|
| 객체 키 | `tenant=/date=/key=/part-N.parquet` | [ADR-0004](adr/0004-partition-scheme.md) |
| 값 표현 | PER_KEY_TYPED + ZSTD | [ADR-0002](adr/0002-value-layout.md) |
| 파일 내 정렬 | `(device_id, ts)` + Parquet v2 | ADR-0004 |
| 쿼리 엔진 | DuckDB | [ADR-0003](adr/0003-query-engine.md) |
| Parquet 라이브러리 | parquet-java, Hadoop 없이 | [ADR-0001](adr/0001-parquet-library.md) |

### 측정된 것 (설계 입력값)

| | 값 | 쓰임 |
|---|---|---|
| 적재 처리량 | 1,337,299 pt/s (단일 스레드) | archiver 파티션 수 산정 |
| 디바이스 1대 / 1년 | 2.59 MiB | 용량 계획 |
| 압축률 (Cassandra 대비) | 11.5배 | 비용 산정 |
| 업로드 | 객체당 4.4ms | 배치 크기 결정 |
| 조회 | 나열이 지연을 지배 | 카탈로그 도입 판단 |

### 제약 (어기면 터진다)

- **Java 는 Parquet 을 읽지 못한다** (ADR-0001). 쿼리 경로는 반드시 DuckDB 를 거친다.
  `reconciler` 의 건수 대조는 푸터만 읽으면 되므로 무관하다
- **파티션 열 이름은 파일 안의 열 이름과 겹치면 안 된다** (ADR-0004).
  겹치면 파티션 쪽이 조용히 죽어 프루닝이 사라진다
- **`HivePartitionSpecs` 는 스레드 안전하지 않다.** 인스턴스당 캐시를 들고 있다.
  병렬 적재를 하려면 스레드당 인스턴스를 만들어야 한다
- **정렬 모드에서는 크기 기반 롤링이 동작하지 않는다.** 파일 크기를 미리 알 수 없다

## 가장 큰 위험: Phase 1 의 라이터는 시간순 도착을 전제했다

이걸 계획의 중심에 둔다. Phase 1 의 결과 중 상당수가 이 전제 위에 서 있다.

`PartitionedParquetWriter` 는 합성 생성기가 **ts 오름차순**으로 방출한다는 성질에 기대고 있다.
그 덕분에 닫힌 날짜 파티션에 데이터가 다시 오지 않았고, 축출이 5,411회 일어나도
**재개봉이 0**이었다 ([W3](benchmark/w3-s3-ingest.md)). 파일 수가 파티션 수와 정확히 같았다.

**Kafka 는 그렇지 않다.** 네트워크 지연, 재시도, 파티션 리밸런스, 디바이스 시계 오차로
지연 도착이 생긴다. 닫힌 파티션에 늦은 데이터가 오면 새 part 파일이 열리고,
그게 곧 [W3~W6 이 내내 지적한 small file 문제](benchmark/w6-manifest-vs-glob.md)다.

즉 **Phase 1 의 "파일 수 = 파티션 수" 는 운이 아니라 전제였고, Phase 2 에서 그 전제가 깨진다.**
W1 이 이걸 예감하고 생성기 주석에 적어뒀다 — "방출 순서는 도착 순서다".

그래서 Phase 2 의 첫 관문은 Kafka 연동이 아니라 **파티션을 언제 닫을 것인가**다.

## 물려받는 코드의 알려진 결함

Phase 2 진입 전에 Phase 1 코드를 검토해서 찾은 것들이다. **Phase 1 에서는 드러나지 않는다** —
벤치가 매 실행 전에 트리를 지우고(`IngestMain` 의 `deleteRecursively`), 입력이 합성 데이터라
안전한 키만 들어오기 때문이다. Phase 2 에서는 둘 다 성립하지 않는다.

> **1번과 2번은 2026-08-15 에 고쳤다** (아래 각 항목 참고). 3번은 남아 있다.

### 1. 재시작하면 기존 part 파일을 조용히 덮어쓴다 (유실) ✅ 수정됨

`PartitionedParquetWriter` 의 part 번호는 **인메모리 맵**에서만 온다
(`nextPart`, 파일:167·256). 새 프로세스는 이 맵이 비어 있으니 모든 파티션에서 `part-0` 부터
다시 세고, `ParquetDatapointWriter` 는 파일을 열 때 무조건 지운다
(`Files.deleteIfExists`, 파일:81). 디렉터리를 스캔해 기존 part 최대값을 찾는 코드가 없다.

archiver 재시작(배포·OOM·컨테이너 재기동) 때마다 이전 part 가 삭제된다.
**로컬 디스크 단계에서 사라지므로 업로드 실패 로그조차 남지 않는다.**
Kafka 오프셋은 커밋됐고 hot 은 TTL 로 지워졌는데 cold 에는 없는 상태가 만들어진다.

**수정 (2026-08-15):** 파일명이 `part-<writerId>-<seq>.parquet` 이 됐다.
`writerId` 는 인스턴스별 UUID 앞 8자라 재시작하면 다른 값이 나온다.
`ParquetDatapointWriter` 의 `deleteIfExists` 도 제거해서, 같은 경로가 이미 있으면
`ParquetWriter` 의 기본 모드(CREATE)가 예외를 던진다 — 조용히 지우는 대신 시끄럽게 실패한다.
회귀 테스트: `PartitionedParquetWriterTest#secondWriterInstanceDoesNotOverwriteFirstOnesFiles`.

**남은 것:** 임시 경로에 쓰고 close 성공 후 atomic move 하는 패턴.
미완성 파일이 업로드 후보에 섞이는 문제는 아직 열려 있다 (W2 에서).

### 2. 디바이스가 보내는 `key` 가 검증 없이 객체 경로가 된다 ✅ 수정됨

`PartitionedParquetWriter:183` 이 `dp.key()` 를, `HivePartitionSpecs:69,87` 이
`dp.deviceProfile()` 을 그대로 경로에 붙인다. `Datapoint` 의 검증은 `requireNonNull` 뿐이다.

Phase 1 은 `Sensors.defaultProfile()` 의 안전한 5개 키만 써서 이 축이 있는 줄도 몰랐다.
Phase 2 archiver 는 Kafka 에서 **임의의 ThingsBoard 키**를 받는다.
`a/b` 는 디렉터리를 한 단계 더 파고, `../../x` 는 root 밖으로 나가며,
`=` 나 공백이 들어간 키는 Hive 파티션 파싱과 DuckDB glob 을 깨뜨린다.

**수정 (2026-08-15):** `PartitionValues` 가 화이트리스트(`[A-Za-z0-9._-]`, 128자)로 검증한다.
검증 지점은 도메인 객체가 아니라 **경로를 만드는 라이터**다 — 슬래시가 든 키가 텔레메트리로는
정당할 수 있고, 문제는 그게 객체 키가 될 때 생기기 때문이다.
값은 몇 개뿐이므로 검증 결과를 캐시해 행마다 정규식을 돌리지 않는다.

**인코딩하지 않고 거부한다.** 퍼센트 인코딩은 쓰기와 읽기가 같은 규칙을 써야 하는 양쪽 계약이고
쿼리 쪽 glob 생성까지 그 규칙을 알아야 한다. 그 복잡도를 지금 감당할 이유가 없다.
회귀 테스트: `PathEscapeTest`(라이터가 실제로 막는지), `PartitionValuesTest`(규칙 자체).

**남은 것:** 거부된 키를 격리 파티션으로 흘리는 경로. 지금은 예외를 던지고 끝난다 —
archiver 에서는 한 건 때문에 컨슈머가 멈추면 안 되므로 W2 에서 처리해야 한다.

### 3. IO 실패 시 파일 핸들이 샌다 (미수정)

`close()` 는 `closed = true` 를 루프 앞에서 세워 재호출이 무의미하고,
`evictUntilWithinLimit` 은 `closeFile` 보다 `it.remove()` 를 먼저 해서 실패한 슬롯을 잃는다.
롤오버 경로에서 `openFile` 이 실패하면 슬롯이 `writer == null` 로 남아 이후 NPE 를 낸다.

장기 실행에서 일시적 IO 오류 하나가 fd 를 영구히 새게 하고,
닫히지 않은 파일은 푸터가 없어 읽을 수도 없는데 `closedFiles` 에도 안 잡혀 업로드되지 않는다.

**W2 에서 함께 정리할 것.** 측정용 단발 실행에서는 드러나지 않던 문제다.

## 범위 밖

Phase 1 과 같은 이유로 명시한다.

- **AWS** — 이 프로젝트는 로컬에서만 진행한다. S3 는 MinIO, Kafka 는 Redpanda
- 회사 Cassandra 연결 — Phase 3
- compaction / reconciliation — Phase 3
- ThingsBoard 어댑터 — Phase 3
- 인증·인가 — 라우터는 내부용으로만 만든다

## 종료 조건

1. Kafka(Redpanda)에 흘린 데이터가 S3 에 Parquet 으로 쌓인다
2. ✅ **지연 도착이 있어도 파일 수가 통제된다** — [측정표](benchmark/p2w1-partition-closing.md)
3. hot/cold 경계를 걸치는 시간 범위 질의가 **중복 없이** 정확한 결과를 낸다
4. archiver 를 죽였다 살려도 데이터가 유실·중복되지 않는다 (건수 대조로 확인)
5. ADR 2건 추가 (파티션 닫기 전략 ✅ [ADR-0005](adr/0005-partition-closing.md) / hot-cold 경계 처리)

## 주차별 계획

### W1 — 파티션 닫기 전략 ✅

Kafka 를 붙이기 전에 이것부터 풀었다. 순서를 바꿨다면 나중에 라이터를 다시 썼을 것이다.

**Exit:** ✅ 완료 (2026-08-15) — [측정](benchmark/p2w1-partition-closing.md), [ADR-0005](adr/0005-partition-closing.md)

전제가 실제로 깨진다는 것부터 확인됐다. 지연 5% 를 주입하면 라이터 슬롯 16 에서
파일이 450 → 1,019,755개가 되고 Parquet 이 NDJSON 대비 **1.2배**로 주저앉는다.

> **계획 단계의 전략 구분이 틀렸다.** 위에 "워터마크 / 유예 시간 / 재개봉 허용" 셋을
> 나란한 후보로 적어뒀는데, 재보니 같은 축의 다른 이름이었다.
> `lru` 슬롯 256 과 `watermark-close` 10일이 **바이트 단위로 동일한 결과**를 낸다 —
> 재개봉을 가르는 것은 정책 종류가 아니라 **열어두는 창이 지연 상한을 덮는가**다.
>
> 실제 축은 둘이다: **창 크기**와 **창을 벗어난 것을 버릴지**.

채택: 이벤트 시각 워터마크로 닫고(기본 7일), 무손실로 간다.
LRU 대비 같은 결과에 동시 열린 파티션이 30% 적고, 무엇보다 필요한 슬롯 수를
지연 분포에서 계산할 수 있다 — LRU 는 창 크기가 슬롯 수의 함수라 순환 참조가 된다.

곁가지로 **유실의 대가가 잃은 행만이 아니라는 것**도 나왔다. drop 은 행이 3.46% 적은데
크기가 1.56배 커진다. 정렬된 `ts` 의 델타 인코딩이 구멍 때문에 무너지기 때문이다.

### W2 — archiver: Kafka → Parquet → S3

- Redpanda 를 `deploy/docker-compose.dev.yml` 에 추가
- 컨슈머 → `PartitionedParquetWriter` → `S3ObjectStore`
- W1 의 닫기 전략 적용
- **오프셋 커밋 시점**이 정확성을 좌우한다. S3 업로드 성공 후 커밋해야 유실이 없고,
  그러면 재시작 시 중복이 생긴다. 어느 쪽을 택할지가 이 주의 결정 사항

**Exit:** 1천만 건을 Kafka 로 흘려 S3 에 적재. 중간에 강제 종료 후 재시작해도 건수가 맞는다

### W3 — storage-cassandra (읽기 전용)

- Phase 1 의 `CassandraBaselineMain` 이 쓴 `ts_kv` 스키마를 그대로 읽는다
- 시간 범위 질의만 지원한다. 쓰기는 하지 않는다
- 로컬 Cassandra 는 Phase 1 에서 이미 띄워봤다 (compose 에 있음)

**Exit:** hot 계층에서 시간 범위로 읽어온다

### W4~W5 — query: hot/cold 라우터

이 프로젝트의 이름값을 하는 부분이다.

- 시간 범위를 받아 hot / cold / 양쪽 중 어디로 보낼지 결정
- **경계 구간 처리가 핵심이다.** `TimeRange` 가 반열린 구간 `[from, to)` 인 이유가 여기 있다 —
  W1 에서 "닫힌 구간으로 두면 경계값 하나가 두 번 조회되고 이게 중복 원인이 된다"고 적어뒀다
- cold 조회는 DuckDB 를 거친다 (ADR-0001 제약)
- 중복 제거 기준: `(tenant, device, key, ts)` 가 같으면 hot 을 신뢰한다 — 근거를 ADR 로 남길 것
- REST API 는 최소한으로

**Exit:** 경계를 걸치는 질의가 중복 없이 정확하다. 경계 케이스 테스트가 회귀로 고정됨

### W6 — 카탈로그 도입 판단

[W6 보강](benchmark/w6-manifest-vs-glob.md)이 남긴 숙제다.
파티션 통계로 프루닝하는 카탈로그가 있으면 조회가 **182배** 빨라지고 스킴 선택 자체가 바뀐다.

- Iceberg 를 붙일 때의 비용을 재본다 (매니페스트 읽기 비용 포함 — W6 보강은 그걸 뺀 낙관적 상한이었다)
- 붙이지 않는다면 `--manifest` 실험처럼 파일 목록을 자체 관리할지 판단
- 이 결정이 ADR-0004 의 스킴 선택을 뒤집을 수 있다

**Exit:** 도입 여부 결정 + ADR-0004 갱신 또는 유지 근거

## Phase 1 이 남긴 미해결 항목

[ADR-0002](adr/0002-value-layout.md) 가 "W3~W6 에서 답한다"고 적어둔 것 중 **답하지 못한 것들**이다.
Phase 2 에서 닫거나, 닫지 않을 이유를 적는다.

| 항목 | 상태 |
|---|---|
| 파일당 최소 크기 임계값 (PER_KEY / SPARSE 하이브리드 분기점) | **미해결.** 키가 5개뿐이라 하이브리드가 필요한 상황이 오지 않았다 |
| `device_id` 를 `FIXED_LEN_BYTE_ARRAY(16)` 으로? | **미해결.** 재보지 않았다 |
| row group 크기 운영 기본값 | **부분.** W5 에서 64 KiB 는 측정용 장치였고 운영값은 정하지 않았다 |

셋 다 Phase 1 의 데이터셋이 작아서 답이 나오지 않았다. Phase 2 에서 실제 키 개수와
데이터 밀도가 정해지면 그때 다시 본다.

## 리스크

| 리스크 | 대응 |
|---|---|
| **지연 도착으로 small file 폭증** | W1 에서 전략을 먼저 정한다. Kafka 연동보다 앞에 둔 이유 |
| 오프셋 커밋과 S3 업로드의 원자성 부재 | 중복 허용 + 조회 시 중복 제거로 간다. 유실보다 중복이 낫다 |
| 경계 구간 중복/누락 | `TimeRange` 반열린 구간을 끝까지 지킨다. 경계 테스트를 먼저 쓴다 |
| 라우터가 커지며 Phase 1 측정 전제가 흐려짐 | cold 조회 경로는 벤치 하네스와 같은 SQL 을 쓴다 |
| 로컬에서 Kafka + Cassandra + MinIO 동시 구동 부담 | 필요한 것만 띄운다. compose 서비스는 이미 분리돼 있다 |

## 환경

Phase 1 과 동일 + Redpanda. 전부 로컬 Docker.

- Java 17, Gradle 8.10.2 (wrapper)
- MinIO (S3), Cassandra 4.1, Redpanda (Kafka)

## 개발 도구 재검토

[PHASE1.md](PHASE1.md) 가 "Phase 2 진입 시점에 MoAI-ADK 를 재검토한다"고 예약해뒀다.
지금이 그 시점이다.

Phase 1 에서 미적용한 근거는 "버려질 실험 코드에 SPEC/TDD 강제는 순수 오버헤드"였다.
**Phase 2 는 그 전제가 바뀐다** — archiver 와 query 는 버려질 코드가 아니고,
경계·정합성 로직은 정확히 SPEC 이 값어치를 하는 영역이다.

확인하기로 했던 것: Gradle 멀티모듈 감지 여부, 실제 토큰 소모량.

> 이 판단은 도구를 써본 사람만 할 수 있다. 계획서에 자리만 남겨두고 비워둔다.
