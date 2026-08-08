# W2 — 값 레이아웃 × 코덱

측정일: 2026-08-08
환경: macOS (darwin 23.4.0, Apple Silicon), Java 17, parquet-java 1.17.1
데이터: 합성 1천만 건 (테넌트 3 × 디바이스 200 × 키 5, 10초 주기, 커버 9시간)

```
./gradlew :bench:parquetBench --args="--count=10_000_000"
```

## 결과

| 레이아웃 | 코덱 | 파일 수 | bytes | MiB | NDJSON 대비 | 쓰기 |
|---|---|---|---|---|---|---|
| NDJSON (기준선) | — | 1 | 1,966,190,528 | 1875.1 | 1.0x | 4.4s |
| SPARSE_TYPED | SNAPPY | 1 | 9,627,604 | 9.2 | 204.2x | 7.6s |
| SPARSE_TYPED | GZIP | 1 | 8,213,734 | 7.8 | 239.4x | 9.1s |
| SPARSE_TYPED | ZSTD | 1 | 8,183,398 | 7.8 | 240.3x | 6.8s |
| STRINGIFIED | SNAPPY | 1 | 14,247,114 | 13.6 | 138.0x | 7.2s |
| STRINGIFIED | GZIP | 1 | 11,141,772 | 10.6 | 176.5x | 7.7s |
| STRINGIFIED | ZSTD | 1 | 12,134,759 | 11.6 | 162.0x | 7.3s |
| PER_KEY_TYPED | SNAPPY | 5 | 7,385,800 | 7.0 | 266.2x | 5.8s |
| PER_KEY_TYPED | GZIP | 5 | 5,908,974 | 5.6 | 332.7x | 6.1s |
| **PER_KEY_TYPED** | **ZSTD** | **5** | **5,817,227** | **5.5** | **338.0x** | **5.9s** |

## ⚠️ 338배를 그대로 인용하면 안 된다

W1 문서에도 적었지만 다시 강조한다. 이 배수는 세 가지로 부풀려져 있다.

1. **커버 기간이 9시간뿐** — ts 고유값이 3,240개라 딕셔너리가 비현실적으로 잘 먹는다.
   `ts` 열이 압축 후 37K 밖에 안 되는 게 그 증거다. 1년치면 완전히 달라진다.
2. **NDJSON 이 뚱뚱한 분모** — 필드명과 UUID(36자)를 매 행 텍스트로 반복한다.
   `device_id` 열이 압축 전 12,264K → 압축 후 1,470K 인 게 이 낭비의 크기다.
3. **디바이스가 600개뿐** — 실제 테넌트는 훨씬 많고, 그만큼 딕셔너리가 커진다.

**보고용 숫자는 (a) 1년 커버 데이터셋에서, (b) Cassandra 실제 디스크 대비로 재야 한다 (W3).**

여기서 의미 있는 건 절대 배수가 아니라 **레이아웃 간 상대 비교**다. 그건 같은 데이터에
같은 조건이므로 유효하다.

## 열별 내역 (ZSTD, 1천만 건)

### SPARSE_TYPED

| 열 | 압축 전 | 압축 후 | 배율 | 인코딩 |
|---|---|---|---|---|
| tenant_id | 41K | 24K | 1.7x | PLAIN_DICTIONARY, BIT_PACKED |
| profile | 14K | 18K | 0.8x | PLAIN_DICTIONARY, BIT_PACKED |
| device_id | 12,264K | 1,470K | 8.3x | PLAIN_DICTIONARY, BIT_PACKED |
| key | 3,695K | 49K | 74.1x | PLAIN_DICTIONARY, BIT_PACKED |
| ts | 52K | 37K | 1.4x | PLAIN_DICTIONARY, BIT_PACKED |
| bool_v | 1,499K | 80K | 18.7x | PLAIN, BIT_PACKED, RLE |
| long_v | 3,259K | 1,676K | 1.9x | PLAIN_DICTIONARY, BIT_PACKED, RLE |
| dbl_v | 5,594K | 4,345K | 1.3x | PLAIN_DICTIONARY, BIT_PACKED, RLE |
| str_v | 1,467K | 71K | 20.6x | PLAIN_DICTIONARY, BIT_PACKED, RLE |

### STRINGIFIED

| 열 | 압축 전 | 압축 후 | 배율 |
|---|---|---|---|
| (앞부분 동일) | | | |
| value_type | 2,474K | 40K | 61.7x |
| **value_str** | **11,193K** | **10,026K** | **1.1x** |

### PER_KEY_TYPED — 키별 파일

| 파일 | 크기 | 행 수 |
|---|---|---|
| key=temperature | 1.81 MiB | 2,000,000 |
| key=power_wh | 1.73 MiB | 2,000,000 |
| key=humidity | 1.68 MiB | 2,000,000 |
| key=error_code | 0.16 MiB | 2,000,000 |
| key=running | 0.16 MiB | 2,000,000 |

W1 에서 설계한 센서 분포가 그대로 드러난다 — RLE 가 먹는 `running` 과
딕셔너리가 먹는 `error_code` 는 연속값 키의 1/10 이다.

## 읽어낸 것

**1. 값 컬럼이 전부다.** SPARSE_TYPED 총 7.8 MiB 중 값 컬럼이 6.0 MiB(77%).
메타데이터는 이미 딕셔너리로 접혀서 거의 공짜다. 앞으로 크기를 줄이려면 값 컬럼을 봐야 한다.

**2. null 은 비싸지 않다.** SPARSE_TYPED 는 행마다 4개 중 3개가 null 인데,
definition level 이 RLE 로 접혀 `bool_v` 18.7x / `str_v` 20.6x 가 나왔다.
"sparse 는 낭비"라는 직관은 Parquet 에서 틀린다.

**3. 문자열화의 대가가 크다.** value_str 은 1.1x 밖에 압축되지 않는다.
숫자를 텍스트로 바꾸는 순간 델타·비트팩킹이 통째로 죽는다.

**4. `profile` 열은 압축하면 오히려 커진다** (14K → 18K, 0.8x).
값이 하나뿐이라 딕셔너리 페이지 오버헤드가 데이터보다 크다.
지금은 14K 라 무시할 수준이지만, 파일을 잘게 쪼개면 파일마다 이 오버헤드가 붙는다.
**W5~W6 에서 파티션을 세분화할 때 이 항목을 다시 볼 것.**

**5. ZSTD > GZIP > SNAPPY.** ZSTD 가 가장 작고 가장 빠르다.
SNAPPY 는 27% 더 큰데 쓰기 이득이 없다.

## 다음 (W3)

- 1년 커버 데이터셋 구성 결정 (W1 문서의 A/B/C 안)
- Cassandra 실제 디스크 사용량 추정치 산출 — 정직한 분모 확보
- LocalStack S3 적재, 파일 크기별 업로드 시간
