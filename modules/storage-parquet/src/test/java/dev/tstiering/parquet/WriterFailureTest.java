package dev.tstiering.parquet;

import dev.tstiering.core.Datapoint;
import dev.tstiering.core.HivePartitionSpecs;
import dev.tstiering.core.TsValue;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IO 실패 경로. 장기 실행 archiver 에서는 디스크 풀·EMFILE 같은 일시적 오류가 반드시 나고,
 * 그때 파일 핸들이 새거나 반쯤 죽은 슬롯이 남으면 그 뒤로 계속 무너진다.
 *
 * <p>실패는 디렉터리를 읽기 전용으로 만들어 주입한다 — 새 파일을 만들 수 없게 된다.
 * 이미 열린 파일에 쓰는 것과 닫는 것은 영향을 받지 않으므로, "새로 여는 것만" 실패시킬 수 있다.
 */
class WriterFailureTest {

    private static final UUID TENANT = new UUID(0x7E4A47L, 0);
    private static final long TS = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();

    private static Datapoint dp(String key, long ts) {
        return new Datapoint(TENANT, "industrial-sensor", new UUID(0xDE71CEL, 0), key, ts,
                new TsValue.DoubleValue(20.0));
    }

    private static PartitionedParquetWriter writer(Path root) {
        return new PartitionedParquetWriter(PartitionedParquetWriter.Config.of(
                root, HivePartitionSpecs.dateOnly(), CompressionCodecName.ZSTD));
    }

    private static long countFiles(Path root) throws IOException {
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> p.toString().endsWith(".parquet")).count();
        }
    }

    /** close() 가 실패해도 재호출이 안전해야 한다. 예전에는 첫 호출이 closed 를 세워 재시도가 무의미했다. */
    @Test
    void closeIsSafeToCallTwice(@TempDir Path root) throws IOException {
        var w = writer(root);
        w.write(dp("temperature", TS));
        w.close();
        assertDoesNotThrow(w::close);
        assertEquals(1, countFiles(root));
    }

    /**
     * 파일을 못 여는 상황에서도 이미 열린 파티션은 정상적으로 닫혀야 한다.
     * 하나가 실패했다고 나머지를 누출시키면 안 된다.
     */
    @Test
    void failureToOpenDoesNotLoseAlreadyOpenPartitions(@TempDir Path root) throws IOException {
        var w = writer(root);
        w.write(dp("temperature", TS));
        w.write(dp("humidity", TS));

        // 새 key= 디렉터리를 만들지 못하도록 날짜 디렉터리를 잠근다.
        // root 를 잠가봐야 소용없다 — 날짜 디렉터리는 이미 있고 그 안에 생성되기 때문이다.
        Path dateDir = root.resolve("date=2026-01-01");
        assertTrue(Files.isDirectory(dateDir), "날짜 파티션 디렉터리가 없다: " + dateDir);
        assertTrue(dateDir.toFile().setWritable(false), "읽기 전용 설정이 되지 않았다");

        try {
            assertThrows(Exception.class, () -> w.write(dp("power_wh", TS)),
                    "새 파티션을 열 수 없어야 한다");
        } finally {
            dateDir.toFile().setWritable(true);
        }

        w.close();
        assertEquals(2, countFiles(root), "실패 이후 이미 열려 있던 파티션이 사라졌다");
    }

    /**
     * <b>반쯤 죽은 슬롯이 생기는 실제 경로는 롤오버다.</b>
     *
     * <p>새 파티션은 {@code openFile} 이 실패하면 {@code open} 맵에 들어가기 전이라 흔적이 없다.
     * 그런데 롤오버는 {@code closeFile} 로 슬롯을 비운 뒤 {@code openFile} 을 부르므로,
     * 그 사이에 실패하면 {@code writer == null} 인 슬롯이 맵에 남는다.
     * 가드가 없으면 다음 write 가 NPE 를 내고, 그 NPE 는 {@code IOException} 이 아니라서
     * close() 루프까지 중단시켜 나머지 슬롯 전부를 누출시킨다.
     */
    @Test
    void writeAfterFailedRolloverReopensInsteadOfThrowingNpe(@TempDir Path root) throws IOException {
        // targetFileBytes=1 이면 크기 검사(8,192행 간격)마다 롤오버가 걸린다.
        var w = new PartitionedParquetWriter(PartitionedParquetWriter.Config
                .of(root, HivePartitionSpecs.dateOnly(), CompressionCodecName.ZSTD)
                .withTargetFileBytes(1));

        for (int i = 0; i < 8_192; i++) {
            w.write(dp("temperature", TS + i));          // 첫 롤오버는 정상
        }

        Path keyDir = root.resolve("date=2026-01-01/key=temperature");
        assertTrue(Files.isDirectory(keyDir), "키 디렉터리가 없다: " + keyDir);
        assertTrue(keyDir.toFile().setWritable(false), "읽기 전용 설정이 되지 않았다");

        try {
            assertThrows(Exception.class, () -> {
                for (int i = 0; i < 8_192; i++) {
                    w.write(dp("temperature", TS + 10_000 + i));   // 두 번째 롤오버에서 실패
                }
            }, "새 part 파일을 열 수 없어야 한다");
        } finally {
            keyDir.toFile().setWritable(true);
        }

        // 여기서 슬롯은 writer == null 로 남아 있다. 가드가 없으면 아래에서 NPE 가 난다.
        assertDoesNotThrow(() -> w.write(dp("temperature", TS + 30_000)));
        assertDoesNotThrow(w::close);
        assertTrue(countFiles(root) >= 2, "롤오버된 파일들이 남아 있어야 한다");
    }
}
