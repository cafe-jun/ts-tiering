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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * W5~W6 의 "정렬 순서 × 행 그룹 프루닝" 축이 실제로 측정 가능한 상태인지 고정한다.
 *
 * <p>여기서 확인하는 것은 두 가지다. (1) 행 그룹이 파일당 여러 개 만들어지는가 —
 * 하나뿐이면 정렬을 어떻게 하든 건너뛸 대상이 없다. (2) 정렬이 실제로 {@code device_id} 의
 * 행 그룹 범위를 좁히는가.
 */
class RowGroupPruningTest {

    private static final UUID TENANT = new UUID(0x7E4A47L, 0);
    private static final long START = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
    private static final int DEVICES = 20;

    /** 60초 간격으로 하루는 1,440틱이다. 이 안에 머물러야 날짜 파티션이 하나로 유지된다. */
    private static final int TICKS = 1_400;

    private static UUID device(int idx) {
        return new UUID(0xDE71CEL, idx);
    }

    /** 생성기와 같은 도착 순서: 바깥이 ts, 안쪽이 디바이스. */
    private static void writeArrivalOrder(PartitionedParquetWriter w) throws IOException {
        for (int tick = 0; tick < TICKS; tick++) {
            for (int d = 0; d < DEVICES; d++) {
                w.write(new Datapoint(TENANT, "industrial-sensor", device(d), "temperature",
                        START + tick * 60_000L, new TsValue.DoubleValue(20 + (tick % 100) * 0.01)));
            }
        }
    }

    private static Path onlyFile(Path root) throws IOException {
        try (Stream<Path> s = Files.walk(root)) {
            List<Path> files = s.filter(p -> p.toString().endsWith(".parquet")).sorted().toList();
            assertEquals(1, files.size(), "이 테스트는 파티션이 하나라고 가정한다");
            return files.get(0);
        }
    }

    private static Path write(Path root, boolean sorted) throws IOException {
        var config = PartitionedParquetWriter.Config
                .of(root, HivePartitionSpecs.dateOnly(), CompressionCodecName.ZSTD)
                .withRowGroupSize(64 * 1024)   // 파일당 행 그룹을 여러 개 만들기 위한 값
                .withSortWithinFile(sorted);

        try (var w = new PartitionedParquetWriter(config)) {
            writeArrivalOrder(w);
        }
        return onlyFile(root);
    }

    /**
     * 32 MiB 기본값으로는 이 정도 데이터가 행 그룹 하나에 다 들어간다.
     * 그러면 min/max 가 파일 전체 범위와 같아져 프루닝이라는 개념 자체가 성립하지 않는다.
     */
    @Test
    void defaultRowGroupSizeYieldsASingleGroup(@TempDir Path root) throws IOException {
        var config = PartitionedParquetWriter.Config
                .of(root, HivePartitionSpecs.dateOnly(), CompressionCodecName.ZSTD);
        try (var w = new PartitionedParquetWriter(config)) {
            writeArrivalOrder(w);
        }
        assertEquals(1, ParquetStats.read(onlyFile(root)).rowGroups(),
                "행 그룹이 하나면 W5 의 정렬 순서 축을 측정할 수 없다");
    }

    @Test
    void smallRowGroupSizeYieldsManyGroups(@TempDir Path root) throws IOException {
        Path file = write(root, false);
        assertTrue(ParquetStats.read(file).rowGroups() > 1,
                "행 그룹이 " + ParquetStats.read(file).rowGroups() + "개뿐이다");
    }

    /**
     * 이 테스트가 W5 의 전제다.
     *
     * <p>도착 순서(ts 바깥루프)로 쓰면 모든 행 그룹에 모든 디바이스가 섞여 들어가므로
     * 디바이스 하나로 걸러도 <b>행 그룹을 하나도 못 건너뛴다.</b>
     * {@code (device_id, ts)} 로 정렬하면 디바이스가 뭉쳐 대부분을 건너뛸 수 있다.
     */
    @Test
    void sortingClustersDeviceIdSoRowGroupsCanBeSkipped(@TempDir Path unsortedRoot,
                                                        @TempDir Path sortedRoot) throws IOException {
        Path unsorted = write(unsortedRoot, false);
        Path sorted = write(sortedRoot, true);

        String target = device(0).toString();
        int totalUnsorted = ParquetStats.read(unsorted).rowGroups();
        int totalSorted = ParquetStats.read(sorted).rowGroups();

        int hitUnsorted = ParquetStats.rowGroupsMatching(unsorted, "device_id", target);
        int hitSorted = ParquetStats.rowGroupsMatching(sorted, "device_id", target);

        assertEquals(totalUnsorted, hitUnsorted,
                "도착 순서에서는 모든 행 그룹이 모든 디바이스를 포함하므로 프루닝이 0이어야 한다");
        assertTrue(hitSorted < totalSorted,
                "정렬 후에는 건너뛸 수 있어야 한다. " + hitSorted + "/" + totalSorted);
    }

    /** 정렬은 순서만 바꾼다. 행이 사라지거나 늘어나면 측정이 무의미해진다. */
    @Test
    void sortingPreservesEveryRow(@TempDir Path root) throws IOException {
        var config = PartitionedParquetWriter.Config
                .of(root, HivePartitionSpecs.dateOnly(), CompressionCodecName.ZSTD)
                .withRowGroupSize(64 * 1024)
                .withSortWithinFile(true);

        PartitionedParquetWriter w = new PartitionedParquetWriter(config);
        writeArrivalOrder(w);
        w.close();

        assertEquals((long) TICKS * DEVICES, w.stats().rows());
        assertEquals((long) TICKS * DEVICES, ParquetStats.read(onlyFile(root)).rows());
    }
}
