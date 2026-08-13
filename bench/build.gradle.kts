plugins {
    application
}

dependencies {
    implementation(project(":core"))
    implementation(project(":storage-parquet"))
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
tasks.register<JavaExec>("parquetBench") {
    group = "benchmark"
    description = "값 레이아웃 3종 × 코덱 3종의 크기/속도를 NDJSON 기준선과 비교한다 (ADR-0002)"
    mainClass.set("dev.tstiering.bench.ParquetBenchMain")
    classpath = sourceSets["main"].runtimeClasspath
    workingDir = rootProject.projectDir
    maxHeapSize = "4g"
}
