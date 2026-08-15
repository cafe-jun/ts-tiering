plugins {
    `java-library`
}

dependencies {
    api(project(":core"))
    implementation(project(":storage-parquet"))
    implementation(project(":storage-s3"))

    // Redpanda 는 Kafka API 호환이라 공식 클라이언트를 그대로 쓴다.
    api("org.apache.kafka:kafka-clients:4.3.0")

    // 텔레메트리는 JSON 으로 들어온다 (bench 의 NDJSON 과 같은 형태).
    implementation("com.fasterxml.jackson.core:jackson-core:2.18.2")

    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
