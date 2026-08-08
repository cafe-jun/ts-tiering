plugins {
    `java-library`
}

dependencies {
    api(project(":core"))

    // CompressionCodecName 등이 이 모듈의 공개 API 에 그대로 등장하므로 api 로 노출한다.
    api("org.apache.parquet:parquet-hadoop:1.17.1")

    // WriteSupport.init(org.apache.hadoop.conf.Configuration) 이 abstract 라 컴파일에만 필요하다.
    // 런타임에는 init(ParquetConfiguration) 오버로드만 호출되므로 Hadoop 은 클래스패스에 없어도 된다.
    // isTransitive=false 로 hadoop-common jar 하나만 가져온다 (전이 의존성은 200MB 가 넘는다).
    compileOnly("org.apache.hadoop:hadoop-common:3.4.1") { isTransitive = false }

    // PlainCodecFactory 가 직접 호출한다. parquet-hadoop 의 전이 의존성이기도 하지만
    // 직접 쓰는 이상 명시적으로 선언한다.
    implementation("com.github.luben:zstd-jni:1.5.7-3")
    implementation("org.xerial.snappy:snappy-java:1.1.10.7")
}

tasks.register("printCp") {
    val cp = configurations.named("runtimeClasspath")
    doLast {
        val files = cp.get().files
        println("jars=${files.size} bytes=${files.sumOf { it.length() }}")
        files.sortedBy { it.name }.forEach { println("  ${it.name}  ${it.length() / 1024}K") }
    }
}
