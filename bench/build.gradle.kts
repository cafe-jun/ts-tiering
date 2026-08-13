plugins {
    application
}

dependencies {
    implementation(project(":core"))
    implementation(project(":storage-parquet"))
    implementation(project(":storage-s3"))
    implementation("com.fasterxml.jackson.core:jackson-core:2.18.2")

    // Cassandra 는 "정직한 분모"를 재기 위한 측정 대상일 뿐이다 (W3).
    // Phase 2 의 storage-cassandra(읽기 전용)와는 별개이므로 벤치 모듈에만 둔다.
    implementation("org.apache.cassandra:java-driver-core:4.19.3")

    // W4 쿼리 엔진. 네이티브 라이브러리를 품고 있지만 벤치 모듈에만 있으므로
    // ADR-0001 이 Arrow 를 기각한 이유(배포 아티팩트가 플랫폼별로 갈림)와는 상관이 없다.
    implementation("org.duckdb:duckdb_jdbc:1.3.1.0")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

application {
    mainClass.set("dev.tstiering.bench.GenerateMain")
}

// ./gradlew :bench:generate --args="--count=10_000_000 --out=data/raw.ndjson"
tasks.register<JavaExec>("generate") {
    group = "benchmark"
    description = "합성 텔레메트리를 NDJSON 으로 생성한다 (Parquet 이전의 크기 기준선)"
    mainClass.set("dev.tstiering.bench.GenerateMain")
    classpath = sourceSets["main"].runtimeClasspath
    workingDir = rootProject.projectDir
}

// ./gradlew :bench:parquetBench --args="--count=10_000_000"
// ./gradlew :bench:ingest --args="--days=365 --devices-per-tenant=17 --interval-seconds=60 --s3=true"
tasks.register<JavaExec>("ingest") {
    group = "benchmark"
    description = "파티션된 Parquet 트리로 적재하고 (선택적으로) S3 에 올린다. W3 측정 지표표를 찍는다"
    mainClass.set("dev.tstiering.bench.IngestMain")
    classpath = sourceSets["main"].runtimeClasspath
    workingDir = rootProject.projectDir
    maxHeapSize = "4g"
}

// docker compose -f deploy/docker-compose.dev.yml up -d
// ./gradlew :bench:query --args="--iterations=20"
tasks.register<JavaExec>("query") {
    group = "benchmark"
    description = "DuckDB 로 S3 Parquet 을 직접 조회한다. 쿼리 3종의 p50/p95 baseline (W4)"
    mainClass.set("dev.tstiering.bench.QueryBenchMain")
    classpath = sourceSets["main"].runtimeClasspath
    workingDir = rootProject.projectDir
    maxHeapSize = "4g"
}

// docker compose -f deploy/docker-compose.dev.yml up -d cassandra
// ./gradlew :bench:cassandraBaseline --args="--days=30 --devices-per-tenant=17 --interval-seconds=60"
tasks.register<JavaExec>("cassandraBaseline") {
    group = "benchmark"
    description = "ThingsBoard ts_kv 스키마로 Cassandra 에 적재한다. 압축률의 정직한 분모를 재기 위한 것"
    mainClass.set("dev.tstiering.bench.CassandraBaselineMain")
    classpath = sourceSets["main"].runtimeClasspath
    workingDir = rootProject.projectDir
    maxHeapSize = "2g"
}

tasks.register<JavaExec>("parquetBench") {
    group = "benchmark"
    description = "값 레이아웃 3종 × 코덱 3종의 크기/속도를 NDJSON 기준선과 비교한다 (ADR-0002)"
    mainClass.set("dev.tstiering.bench.ParquetBenchMain")
    classpath = sourceSets["main"].runtimeClasspath
    workingDir = rootProject.projectDir
    maxHeapSize = "4g"
}

// ./gradlew :bench:schemaProbe --args="<프리픽스> [<프리픽스> ...]"
tasks.register<JavaExec>("schemaProbe") {
    group = "benchmark"
    description = "파티션 스킴이 쿼리에 실제로 노출하는 열을 확인한다 (ADR-0004 의 profile 가림 근거)"
    mainClass.set("dev.tstiering.bench.SchemaProbe")
    classpath = sourceSets["main"].runtimeClasspath
    workingDir = rootProject.projectDir
}

// ./gradlew :bench:colProbe --args="<적재 디렉터리> [<적재 디렉터리> ...]"
tasks.register<JavaExec>("colProbe") {
    group = "benchmark"
    description = "적재 결과의 열별 크기와 인코딩을 찍는다 (W5 의 ts 인코딩 폴백 근거)"
    mainClass.set("dev.tstiering.bench.ColProbe")
    classpath = sourceSets["main"].runtimeClasspath
    workingDir = rootProject.projectDir
}
