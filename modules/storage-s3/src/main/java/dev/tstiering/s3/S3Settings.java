package dev.tstiering.s3;

import java.net.URI;

/**
 * S3 접속 설정. LocalStack 과 실 AWS 를 같은 코드로 가리키기 위한 최소한의 스위치만 둔다.
 *
 * @param endpoint          {@code null} 이면 실제 AWS. LocalStack 은 {@code http://localhost:4566}
 * @param region            버킷 리전
 * @param bucket            버킷 이름
 * @param accessKey         {@code null} 이면 SDK 기본 자격증명 체인을 쓴다
 * @param secretKey         accessKey 와 짝
 * @param multipartThreshold 이 크기를 넘으면 multipart 로 올린다
 * @param partSize          multipart 파트 크기. S3 규격상 마지막 파트를 제외하면 5 MiB 이상이어야 한다
 */
public record S3Settings(
        URI endpoint,
        String region,
        String bucket,
        String accessKey,
        String secretKey,
        long multipartThreshold,
        int partSize
) {

    /** S3 가 강제하는 최소 파트 크기. 마지막 파트만 예외다. */
    public static final int MIN_PART_SIZE = 5 * 1024 * 1024;

    public static final int DEFAULT_PART_SIZE = 8 * 1024 * 1024;

    public S3Settings {
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("bucket 은 비어 있을 수 없다");
        }
        if (partSize < MIN_PART_SIZE) {
            throw new IllegalArgumentException(
                    "partSize 는 " + MIN_PART_SIZE + " 이상이어야 한다 (S3 규격), got " + partSize);
        }
        if (multipartThreshold < partSize) {
            throw new IllegalArgumentException(
                    "multipartThreshold(" + multipartThreshold + ") 가 partSize(" + partSize + ") 보다 작으면"
                            + " 파트가 하나뿐인 multipart 가 생긴다");
        }
    }

    /** {@code deploy/docker-compose.dev.yml} 이 띄우는 LocalStack 을 가리킨다. */
    public static S3Settings localstack(String bucket) {
        return new S3Settings(
                URI.create("http://localhost:4566"),
                "ap-northeast-2",
                bucket,
                "test",
                "test",
                DEFAULT_PART_SIZE,
                DEFAULT_PART_SIZE);
    }

    /** LocalStack 은 가상호스트 스타일(bucket.localhost)을 쓰기 번거로워 path-style 로 붙는다. */
    public boolean pathStyleAccess() {
        return endpoint != null;
    }

    public S3Settings withPartSize(int newPartSize) {
        return new S3Settings(endpoint, region, bucket, accessKey, secretKey,
                Math.max(multipartThreshold, newPartSize), newPartSize);
    }

    public S3Settings withMultipartThreshold(long newThreshold) {
        return new S3Settings(endpoint, region, bucket, accessKey, secretKey, newThreshold, partSize);
    }
}
