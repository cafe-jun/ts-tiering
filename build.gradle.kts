plugins {
    java
}

allprojects {
    group = "dev.tstiering"
    version = "0.1.0-SNAPSHOT"
}

subprojects {
    apply(plugin = "java")

    repositories {
        mavenCentral()
    }

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(17))
        }
    }

    dependencies {
        "testImplementation"(platform("org.junit:junit-bom:5.11.3"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testImplementation"("org.junit.jupiter:junit-jupiter-params")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()

        // Gradle 은 -D 를 테스트 JVM 으로 자동 전달하지 않는다. 넘겨주지 않으면
        // @EnabledIfSystemProperty 가 걸린 통합 테스트가 "통과"가 아니라 "건너뜀"이 된다.
        //   docker compose -f deploy/docker-compose.dev.yml up -d
        //   ./gradlew :storage-s3:test -Ds3.it=true
        System.getProperty("s3.it")?.let { systemProperty("s3.it", it) }
    }
}
