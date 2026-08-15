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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 2 의 archiver 는 Kafka 에서 임의의 키를 받는다. 그 값이 그대로 객체 경로가 되면
 * root 밖으로 나갈 수 있다. 라이터가 실제로 막는지 확인한다 —
 * 검증 함수의 단위 테스트만으로는 "라이터가 그걸 호출하는가"를 보장하지 못한다.
 */
class PathEscapeTest {

    private static final UUID TENANT = new UUID(0x7E4A47L, 0);
    private static final long TS = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();

    private static Datapoint dp(String key, String profile) {
        return new Datapoint(TENANT, profile, new UUID(0xDE71CEL, 0), key, TS,
                new TsValue.DoubleValue(20.0));
    }

    private static PartitionedParquetWriter writer(Path root) {
        return new PartitionedParquetWriter(PartitionedParquetWriter.Config.of(
                root, HivePartitionSpecs.tenantDate(), CompressionCodecName.ZSTD));
    }

    @Test
    void rejectsKeyThatWouldEscapeTheRoot(@TempDir Path root) throws IOException {
        try (var w = writer(root)) {
            var e = assertThrows(IllegalArgumentException.class,
                    () -> w.write(dp("../../escaped", "industrial-sensor")));
            assertTrue(e.getMessage().contains("../../escaped"));
        }
        assertEquals(0, countFiles(root), "거부됐는데 파일이 생겼다");
    }

    @Test
    void rejectsProfileThatWouldEscapeTheRoot(@TempDir Path root) throws IOException {
        try (var w = writer(root)) {
            assertThrows(IllegalArgumentException.class,
                    () -> w.write(dp("temperature", "../evil")));
        }
        assertEquals(0, countFiles(root));
    }

    /** 슬래시가 든 키는 파티션 깊이를 바꿔 스킴을 깨뜨린다. */
    @Test
    void rejectsKeyWithSeparator(@TempDir Path root) throws IOException {
        try (var w = writer(root)) {
            assertThrows(IllegalArgumentException.class,
                    () -> w.write(dp("sensor/temp", "industrial-sensor")));
        }
    }

    /** = 는 Hive 파티션 파싱을 깨뜨린다. */
    @Test
    void rejectsKeyWithEqualsSign(@TempDir Path root) throws IOException {
        try (var w = writer(root)) {
            assertThrows(IllegalArgumentException.class,
                    () -> w.write(dp("a=b", "industrial-sensor")));
        }
    }

    /** 정상 키는 그대로 통과해야 한다 — 검증이 과하면 실제 데이터를 막는다. */
    @Test
    void acceptsNormalKeys(@TempDir Path root) throws IOException {
        var w = writer(root);
        for (String key : new String[]{"temperature", "power_wh", "batt.level", "rssi-dbm"}) {
            w.write(dp(key, "industrial-sensor"));
        }
        w.close();
        assertEquals(4, w.stats().partitions());
        assertEquals(4, countFiles(root));
    }

    private static long countFiles(Path root) throws IOException {
        if (!Files.exists(root)) return 0;
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> p.toString().endsWith(".parquet")).count();
        }
    }
}
