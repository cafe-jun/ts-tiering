# ADR-0001 — Parquet 라이브러리와 Hadoop 의존성

- 상태: 채택
- 일자: 2026-08-08
- 관련: W2

## 맥락

Java 에서 Parquet 을 쓰려면 사실상 `parquet-java`(구 `parquet-mr`)를 쓰게 된다.
문제는 이 라이브러리가 Hadoop 생태계에서 태어나 API 가 Hadoop 파일시스템 추상화에 묶여 있다는 점이다.
Hadoop 클러스터를 쓸 일이 전혀 없는데도 `hadoop-common` 과 그 전이 의존성(200MB+)이 딸려오면

- fat jar 가 불필요하게 커지고
- 쓰지도 않는 Hadoop CVE 가 취약점 스캔에 계속 잡히며
- Guava / Jackson 버전 충돌이 잦아진다

## 조사한 것

### 1. 아티팩트 트리는 이미 깨끗하다

`parquet-hadoop:1.17.1` 의 전이 의존성에 **Hadoop 이 없다.** 14개 jar, 19.7 MiB.

```
parquet-hadoop, parquet-column, parquet-common, parquet-encoding,
parquet-format-structures, parquet-jackson, snappy-java, zstd-jni,
aircompressor, commons-pool, jts-core, javax.annotation-api, slf4j-api
```

이름이 `parquet-hadoop` 일 뿐 Hadoop 자체는 들어오지 않는다.

### 2. 그런데 런타임에 세 군데서 Hadoop 을 요구한다

빌드가 되는 것과 도는 것은 별개였다. 순서대로 부딪힌 지점:

| # | 지점 | 원인 | 해결 |
|---|---|---|---|
| 1 | `WriteSupport.init(Configuration)` | 상위 클래스가 abstract 로 강제 (컴파일 시점) | `compileOnly` + `isTransitive=false` 로 jar 하나만. 런타임 불필요 |
| 2 | `CodecFactory.getCodec()` | `Class.forName` 으로 Hadoop 코덱 구현을 찾음 | **`CompressionCodecFactory` 를 직접 구현** ({@link PlainCodecFactory}) |
| 3 | `ParquetReadOptions.Builder` 생성자 | `ParquetInputFormat.getFilter()` 호출 → `ParquetInputFormat extends org.apache.hadoop.mapreduce.lib.input.FileInputFormat` | **우회 불가.** 푸터를 직접 파싱 |

2번이 핵심이었다. Hadoop 이 하던 일은 "설정에서 코덱 클래스 이름을 찾아 인스턴스화"뿐이고,
실제 압축은 이미 클래스패스에 있는 zstd-jni / snappy-java / JDK 가 한다.
그 간접층이 필요 없으므로 `CompressionCodecFactory` 인터페이스(메서드 3개)를 직접 구현했다.

3번은 라이브러리 한계다. `ParquetReadOptions.builder(ParquetConfiguration)` 조차
생성자에서 Hadoop MapReduce 클래스를 로드한다. Hadoop-free 진입점이 없다.

## 결정

**parquet-java 1.17.1 을 쓰되, 코덱 팩토리를 직접 구현하고 푸터는 직접 파싱한다.**

```java
ParquetWriter.Builder(new LocalOutputFile(path))
    .withConf(new PlainParquetConfiguration())   // Hadoop Configuration 아님
    .withCodecFactory(new PlainCodecFactory())   // 우리 구현
    .withCompressionCodec(codec)
```

의존성 선언:

```kotlin
api("org.apache.parquet:parquet-hadoop:1.17.1")
compileOnly("org.apache.hadoop:hadoop-common:3.4.1") { isTransitive = false }
implementation("com.github.luben:zstd-jni:1.5.7-3")
implementation("org.xerial.snappy:snappy-java:1.1.10.7")
```

**런타임 클래스패스에 Hadoop 이 없다.** `ParquetWriteTest` 가 도는 것 자체가 그 증명이다 —
쓰기 경로가 Hadoop 클래스를 하나라도 건드리면 `NoClassDefFoundError` 로 즉시 죽는다.

## 대안과 기각 사유

| 대안 | 기각 사유 |
|---|---|
| hadoop-common 전체를 런타임에 포함 | 정확히 피하려던 것. 200MB+, CVE 노이즈 |
| Apache Arrow Java (`arrow-dataset`) | Parquet 쓰기가 JNI 로 Arrow C++ 를 호출한다. 플랫폼별 네이티브 라이브러리가 필요해 배포가 더 복잡해진다 |
| Group API (`ExampleParquetWriter`) | Hadoop 문제는 같고, 행마다 `SimpleGroup` 을 할당해 GC 부담만 추가된다 |

## 결과

**좋은 점**

- 배포 아티팩트에 Hadoop 없음. 의존성 19.7 MiB
- 코덱을 우리가 통제한다 (zstd 레벨 조정 등)
- 표준 포맷 유지 — pyarrow 로 교차 검증 완료 (snappy/gzip/zstd 전부, 값 일치)

**대가**

- `PlainCodecFactory` 를 유지보수해야 한다 (약 130줄). parquet 이 코덱 인터페이스를 바꾸면 따라가야 함
- 컴파일에는 여전히 hadoop-common jar 하나가 필요하다
- **Java 에서 레코드 단위 읽기가 막혀 있다** — 아래 참고

## Phase 2 에 미치는 영향 (중요)

3번 때문에 `ParquetFileReader` 로 레코드를 읽을 수 없다. 다만 이게 설계상 문제가 되지 않는다.

- **쿼리 경로**는 애초에 DuckDB/Athena 가 담당한다 (W4, W8). Java 가 Parquet 을 읽을 일이 없다
- **`reconciler`** 는 건수 대조가 목적인데, 행 수는 **푸터에만 읽어도 나온다**.
  데이터 페이지를 건드릴 필요가 없으므로 이 제약과 무관하다

즉 Java 는 쓰기 전용, 읽기는 쿼리 엔진에 맡긴다.
만약 Phase 2 에서 Java 쪽 레코드 읽기가 정말 필요해지면 그때 hadoop-common 을
**그 모듈에만** 추가하는 것으로 국지화한다.
