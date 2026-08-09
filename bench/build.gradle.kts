plugins {
    application
}

dependencies {
    implementation(project(":core"))
    implementation(project(":storage-parquet"))
    implementation(project(":storage-s3"))
    implementation("com.fasterxml.jackson.core:jackson-core:2.18.2")
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

tasks.register<JavaExec>("parquetBench") {
    group = "benchmark"
    description = "값 레이아웃 3종 × 코덱 3종의 크기/속도를 NDJSON 기준선과 비교한다 (ADR-0002)"
    mainClass.set("dev.tstiering.bench.ParquetBenchMain")
    classpath = sourceSets["main"].runtimeClasspath
    workingDir = rootProject.projectDir
    maxHeapSize = "4g"
}
