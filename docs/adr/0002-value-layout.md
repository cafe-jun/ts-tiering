# ADR-0002 — 텔레메트리 값의 Parquet 표현

- 상태: 채택
- 일자: 2026-08-08
- 관련: W2, [벤치마크](../benchmark/w2-parquet-layout.md)

## 맥락

Parquet 컬럼은 타입이 하나로 고정된다. 그런데 텔레메트리 값은 키마다 타입이 다르다
(온도는 double, 가동상태는 boolean, 에러코드는 string). 어딘가에서 타협해야 한다.

## 후보

**A. SPARSE_TYPED** — ThingsBoard `ts_kv` 를 그대로 옮긴다. `bool_v / long_v / dbl_v / str_v`
컬럼을 두고 행마다 하나만 채운다. 나머지 75% 는 null.

**B. STRINGIFIED** — 모든 값을 문자열로 통일하고 타입 태그 컬럼을 둔다. 스키마가 단순하다.

**C. PER_KEY_TYPED** — 키별로 파일을 나누고 값 컬럼을 그 키의 실제 타입으로 둔다.
`key` 컬럼 자체가 사라진다.

## 측정 (1천만 건, 상세는 벤치마크 문서)

| 레이아웃 | ZSTD | vs 최선 | 파일 수 |
|---|---|---|---|
| **PER_KEY_TYPED** | **5.5 MiB** | **1.00x** | 5 |
| SPARSE_TYPED | 7.8 MiB | 1.41x | 1 |
| STRINGIFIED | 11.6 MiB | 2.10x | 1 |

열별로 보면 원인이 분명하다.

| | SPARSE_TYPED | STRINGIFIED |
|---|---|---|
| 값 컬럼 압축 후 | dbl_v 4,345K + long_v 1,676K + bool_v 80K + str_v 71K = **6,172K** | value_str **10,026K** |
| 값 컬럼 압축비 | 1.3x / 1.9x / 18.7x / 20.6x | **1.1x** |

숫자를 문자열로 바꾸면 델타·비트팩킹이 통째로 죽는다. `20.3` 은 double 로 8바이트지만
문자열로는 4바이트에 압축도 안 먹는다.

반대로 **SPARSE_TYPED 의 null 은 거의 공짜였다.** 75% 가 null 인데도 definition level 이
RLE 로 접혀서, `bool_v` 는 18.7x, `str_v` 는 20.6x 압축됐다. "null 이 많으면 낭비"라는
직관이 Parquet 에서는 성립하지 않는다.

PER_KEY_TYPED 가 이기는 이유는 두 가지다. 값 컬럼에 null 이 아예 없고,
`key` 컬럼(압축 후 49K)이 파일 경로로 대체되어 사라진다.

### 조건 주의 — STRINGIFIED 의 손해는 값 엔트로피에 달렸다

값의 고유값이 적거나 단조증가하면 딕셔너리·zstd 가 문자열/숫자 차이를 대부분 지운다.
실제로 고유값 100개짜리 합성 데이터에서는 두 레이아웃 크기가 거의 같았다.
위 2.10x 는 **실제 센서처럼 값이 넓게 흩어질 때**의 수치다.
이 조건 때문에 단위 테스트로 고정하지 않고 벤치마크 문서에만 남긴다.

## 결정

**PER_KEY_TYPED 를 기본으로 한다.**

```
s3://bucket/<파티션 경로>/key=temperature/part-N.parquet
  → tenant_id, profile, device_id, ts, value:double
```

**단, 키 개수에 상한을 두고 초과분은 SPARSE_TYPED 로 흘린다.**

PER_KEY_TYPED 의 대가는 파일 수다. 키가 5개인 지금은 무해하지만
키가 수백 개인 테넌트에서는 파일이 수백 배로 늘어 small file 문제가 된다.
"파일당 최소 크기" 기준을 두고, 그에 못 미치는 희소 키는 SPARSE_TYPED 파일 하나에
모아 담는 하이브리드로 간다.

임계값은 W5~W6 에서 파티셔닝 스킴과 함께 정한다 — 파티션이 잘게 쪼개질수록
키당 파일 크기가 작아지므로 두 결정이 얽혀 있다.

**STRINGIFIED 는 기각한다.** 2.1배 손해를 볼 이유가 없다.

## 코덱

같은 벤치마크에서 함께 측정했다.

| 코덱 | PER_KEY_TYPED | 쓰기 |
|---|---|---|
| **ZSTD** | **5.5 MiB** | 5.9s |
| GZIP | 5.6 MiB | 6.1s |
| SNAPPY | 7.0 MiB | 5.8s |

**ZSTD 를 쓴다.** GZIP 과 크기는 비슷하지만 더 빠르고, cold 계층은 쓰기 1회 / 읽기 드묾이라
압축률이 속도보다 중요하다. SNAPPY 는 27% 더 커지는데 쓰기 이득이 없어 탈락.

## 남은 질문 (W3~W6)

- 파일당 최소 크기 임계값 — PER_KEY/SPARSE 하이브리드 분기점
- `device_id` 를 UUID 문자열(36자) 대신 `FIXED_LEN_BYTE_ARRAY(16)` 으로?
  현재 딕셔너리로 8.3x 접히고 있어 이득이 작을 수 있다. 실측 후 판단
- row group 크기 — 지금 128MiB 고정. 프루닝 효율과 함께 W5~6 에서
