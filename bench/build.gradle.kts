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
