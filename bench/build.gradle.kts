plugins {
    application
}

dependencies {
    implementation(project(":core"))
    implementation("com.fasterxml.jackson.core:jackson-core:2.18.2")
}

application {
    mainClass.set("dev.tstiering.bench.GenerateMain")
}

// ./gradlew :bench:generate --args="--count=10000000 --out=data/raw.ndjson"
tasks.register<JavaExec>("generate") {
    group = "benchmark"
    description = "합성 텔레메트리를 NDJSON 으로 생성한다 (Parquet 이전의 크기 기준선)"
    mainClass.set("dev.tstiering.bench.GenerateMain")
    classpath = sourceSets["main"].runtimeClasspath
    workingDir = rootProject.projectDir
}
