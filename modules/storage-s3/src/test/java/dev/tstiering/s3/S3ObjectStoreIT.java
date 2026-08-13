package dev.tstiering.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 로컬 S3(MinIO)가 떠 있어야 도는 통합 테스트. CI 는 Docker 를 띄우지 않으므로 기본은 꺼져 있다.
 *
 * <pre>
 * docker compose -f deploy/docker-compose.dev.yml up -d
 * ./gradlew :storage-s3:test -Ds3.it=true
 * </pre>
 */
@EnabledIfSystemProperty(named = "s3.it", matches = "true")
class S3ObjectStoreIT {

    private static final String BUCKET = "ts-tiering-it";

    @Test
    void singlePutRoundTrips(@TempDir Path dir) throws IOException {
        Path file = randomFile(dir.resolve("small.bin"), 1024);

        try (S3ObjectStore store = S3ObjectStore.open(S3Settings.local(BUCKET))) {
            store.createBucketIfAbsent();
            UploadResult r = store.put("single/small.bin", file);

            assertFalse(r.multipart(), "1 KiB 는 multipart 임계값을 넘지 않는다");
            assertEquals(1, r.parts());
            assertEquals(1024, r.bytes());
        }
    }

    /** 파트 크기 5 MiB, 파일 12 MiB → 5+5+2 로 3 파트. 마지막 파트만 5 MiB 미만이어도 된다. */
    @Test
    void multipartSplitsIntoExpectedParts(@TempDir Path dir) throws IOException {
        int partSize = S3Settings.MIN_PART_SIZE;
        Path file = randomFile(dir.resolve("big.bin"), 12 * 1024 * 1024);

        var settings = S3Settings.local(BUCKET)
                .withPartSize(partSize)
                .withMultipartThreshold(partSize);

        try (S3ObjectStore store = S3ObjectStore.open(settings)) {
            store.createBucketIfAbsent();
            UploadResult r = store.putMultipart("multi/big.bin", file);

            assertTrue(r.multipart());
            assertEquals(3, r.parts());
            assertEquals(12L * 1024 * 1024, r.bytes());

            List<S3ObjectStore.ObjectSummary> found = store.list("multi/");
            assertEquals(1, found.size());
            assertEquals(12L * 1024 * 1024, found.get(0).size(),
                    "파트가 하나라도 누락되면 크기가 어긋난다");
        }
    }

    /** 디렉터리 구조가 그대로 객체 키가 되어야 파티션 경로가 S3 에서 의미를 가진다. */
    @Test
    void putTreeKeepsDirectoryStructureAsKeys(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir.resolve("date=2026-01-01/hour=00"));
        randomFile(dir.resolve("date=2026-01-01/hour=00/part-0.parquet"), 128);
        randomFile(dir.resolve("date=2026-01-01/hour=00/part-1.parquet"), 128);

        try (S3ObjectStore store = S3ObjectStore.open(S3Settings.local(BUCKET))) {
            store.createBucketIfAbsent();
            store.putTree(dir, "tree");

            List<String> keys = store.list("tree/").stream().map(S3ObjectStore.ObjectSummary::key).sorted().toList();
            assertEquals(List.of(
                    "tree/date=2026-01-01/hour=00/part-0.parquet",
                    "tree/date=2026-01-01/hour=00/part-1.parquet"), keys);
        }
    }

    private static Path randomFile(Path path, int size) throws IOException {
        Files.createDirectories(path.getParent());
        byte[] buf = new byte[size];
        new Random(42).nextBytes(buf); // 고정 시드 — 압축 가능한 데이터가 아니어야 크기 비교가 정확하다
        Files.write(path, buf);
        return path;
    }
}
