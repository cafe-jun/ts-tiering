plugins {
    `java-library`
}

dependencies {
    api(project(":core"))

    // S3Client 등이 이 모듈의 공개 API 에 등장하므로 BOM 도 api 로 내보낸다.
    // implementation 으로 두면 소비 모듈이 버전을 못 찾는다 (버전 없이 선언한 s3 가 미해결).
    api(platform("software.amazon.awssdk:bom:2.46.7"))
    api("software.amazon.awssdk:s3")

    // multipart 는 SDK 의 TransferManager 대신 직접 구현한다 (S3ObjectStore 주석 참고).
    // aws-crt-client 를 붙이면 플랫폼별 네이티브 라이브러리가 딸려오는데,
    // ADR-0001 에서 Arrow 를 기각한 이유와 같은 이유로 피한다.
}
