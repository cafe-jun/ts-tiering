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
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionedParquetWriterTest {

    private static final UUID TENANT = new UUID(0x7E4A47L, 0);
    private static final long START = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();

    private static Datapoint point(int deviceIdx, String key, long ts, TsValue value) {
        return new Datapoint(TENANT, "industrial-sensor", new UUID(0xDE71CEL, deviceIdx), key, ts, value);
    }

    private static List<Path> parquetFiles(Path root) throws IOException {
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> p.toString().endsWith(".parquet")).sorted().toList();
        }
    }

    @Test
    void writesHiveStylePartitionTreeWithKeyInnermost(@TempDir Path root) throws IOException {
        var config = PartitionedParquetWriter.Config.of(
                root, HivePartitionSpecs.tenantProfileDateHour(), CompressionCodecName.ZSTD);

        try (var writer = new PartitionedParquetWriter(config)) {
            writer.write(point(0, "temperature", START, new TsValue.DoubleValue(20.1)));
            writer.write(point(0, "running", START, new TsValue.BoolValue(true)));
            // 다음 시간대 → 새 파티션
            writer.write(point(0, "temperature", START + 3_600_000L, new TsValue.DoubleValue(20.4)));
        }

        List<String> rel = parquetFiles(root).stream().map(p -> root.relativize(p).toString()).toList();
        assertEquals(List.of(
                "tenant=" + TENANT + "/profile=industrial-sensor/date=2026-01-01/hour=00/key=running/part-0.parquet",
                "tenant=" + TENANT + "/profile=industrial-sensor/date=2026-01-01/hour=00/key=temperature/part-0.parquet",
                "tenant=" + TENANT + "/profile=industrial-sensor/date=2026-01-01/hour=01/key=temperature/part-0.parquet"
        ), rel);
    }

    /**
     * 롤링이 도는지뿐 아니라 {@code ParquetWriter.getDataSize()} 가 <b>아직 flush 되지 않은</b>
     * row group 버퍼까지 포함하는지를 함께 확인한다. flush 된 분만 센다면 row group(32 MiB)에
     * 못 미치는 파일은 영원히 롤링되지 않고, 크기 상한이 조용히 무력해진다.
     */
    @Test
    void rollsFileWhenTargetSizeExceededBeforeAnyRowGroupFlush(@TempDir Path root) throws IOException {
        var config = PartitionedParquetWriter.Config
                .of(root, HivePartitionSpecs.dateHour(), CompressionCodecName.ZSTD)
                .withTargetFileBytes(8 * 1024)
                .withRowGroupSize(32 * 1024 * 1024);

        // 50,000 행 × 행당 100 바이트 남짓 = 5 MiB 대. row group(32 MiB)에 한참 못 미치므로
        // flush 는 한 번도 일어나지 않는다. 그런데도 롤링이 걸려야 dataSize() 가 버퍼를 센다는 뜻이다.
        try (var writer = new PartitionedParquetWriter(config)) {
            // 같은 시간대에 몰아넣어 파티션은 하나로 고정하고, 롤링만 변수로 남긴다.
            for (int i = 0; i < 50_000; i++) {
                writer.write(point(i % 50, "temperature", START, new TsValue.DoubleValue(i * 0.37)));
            }
        }

        assertTrue(parquetFiles(root).size() > 1,
                "8 KiB 상한에 50,000 행이면 파일이 여러 개여야 한다. 하나라면 dataSize() 가 "
                        + "flush 된 row group 만 세고 있다는 뜻이다");
    }

    @Test
    void statsCountRowsFilesAndRollovers(@TempDir Path root) throws IOException {
        var config = PartitionedParquetWriter.Config
                .of(root, HivePartitionSpecs.dateHour(), CompressionCodecName.ZSTD)
                .withTargetFileBytes(8 * 1024);

        var writer = new PartitionedParquetWriter(config);
        for (int i = 0; i < 50_000; i++) {
            writer.write(point(i % 50, "temperature", START, new TsValue.DoubleValue(i * 0.37)));
        }
        writer.close();

        var stats = writer.stats();
        assertEquals(50_000, stats.rows());
        assertEquals(parquetFiles(root).size(), stats.files());
        assertTrue(stats.rollovers() >= 1, "rollovers=" + stats.rollovers());
        assertEquals(0, stats.evictions(), "파티션이 하나뿐이라 축출은 없어야 한다");
        assertTrue(stats.bytes() > 0);
    }

    /**
     * 팬아웃이 상한을 넘으면 LRU 로 닫고, 그 파티션에 데이터가 다시 오면 새 part 가 열린다.
     * 축출 자체가 small file 을 만든다는 것이 이 테스트의 요점이다.
     */
    @Test
    void evictsLeastRecentlyUsedAndReopensAsNewPart(@TempDir Path root) throws IOException {
        var config = PartitionedParquetWriter.Config
                .of(root, HivePartitionSpecs.tenantDeviceDate(), CompressionCodecName.ZSTD)
                .withMaxOpenWriters(2);

        var writer = new PartitionedParquetWriter(config);
        // 디바이스 3개를 번갈아 쓰면 상한 2를 계속 넘긴다.
        for (int round = 0; round < 3; round++) {
            for (int device = 0; device < 3; device++) {
                writer.write(point(device, "temperature", START, new TsValue.DoubleValue(round + device)));
            }
        }
        writer.close();

        var stats = writer.stats();
        assertEquals(9, stats.rows());
        assertEquals(3, stats.partitions());
        assertTrue(stats.evictions() > 0, "상한 2에 파티션 3개면 축출이 있어야 한다");
        assertEquals(0, stats.rollovers(), "크기 상한에는 걸리지 않았다");
        assertTrue(stats.reopens() > 0,
                "축출된 파티션에 데이터가 다시 오면 part 가 늘어난다. files=" + stats.files());
        assertEquals(stats.files() - stats.partitions() - stats.rollovers(), stats.reopens());
    }

    @Test
    void rejectsTypeChangeWithinSameKey(@TempDir Path root) throws IOException {
        var config = PartitionedParquetWriter.Config.of(
                root, HivePartitionSpecs.dateHour(), CompressionCodecName.ZSTD);

        try (var writer = new PartitionedParquetWriter(config)) {
            writer.write(point(0, "temperature", START, new TsValue.DoubleValue(20.1)));
            var e = assertThrows(IllegalStateException.class,
                    () -> writer.write(point(0, "temperature", START, new TsValue.LongValue(20))));
            assertTrue(e.getMessage().contains("temperature"));
        }
    }
}
