package dev.tstiering.parquet;

import dev.tstiering.core.Datapoint;
import dev.tstiering.core.HivePartitionSpecs;
import dev.tstiering.core.TsValue;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 파티션 닫기 정책. Phase 1 에는 이 개념이 없어 LRU 축출이 우연한 유예 기간이었고,
 * 지연 도착이 생기는 순간 재개봉이 폭증한다.
 */
class ClosePolicyTest {

    private static final UUID TENANT = new UUID(0x7E4A47L, 0);
    private static final UUID DEVICE = new UUID(0xDE71CEL, 0);
    private static final long DAY0 = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
    private static final long DAY = Duration.ofDays(1).toMillis();

    private static Datapoint at(long ts) {
        return new Datapoint(TENANT, "industrial-sensor", DEVICE, "temperature", ts,
                new TsValue.DoubleValue(20.0));
    }

    private static PartitionedParquetWriter.Config config(Path root, ClosePolicy policy) {
        return PartitionedParquetWriter.Config
                .of(root, HivePartitionSpecs.dateOnly(), CompressionCodecName.ZSTD)
                .withClosePolicy(policy);
    }

    /**
     * 스윕은 {@code rows % 8192} 에서만 돈다. 테스트가 그 간격에 의존하지 않도록
     * 충분히 많은 행을 흘려 스윕을 확실히 트리거한다.
     */
    private static void writeDays(PartitionedParquetWriter w, int days, int perDay) throws IOException {
        for (int d = 0; d < days; d++) {
            for (int i = 0; i < perDay; i++) {
                w.write(at(DAY0 + d * DAY + i * 1000L));
            }
        }
    }

    /** Phase 1 의 동작. 시간으로는 닫지 않으므로 워터마크 닫기가 0이어야 한다. */
    @Test
    void lruOnlyNeverClosesByTime(@TempDir Path root) throws IOException {
        assertFalse(ClosePolicy.LRU_ONLY.timeBased());

        var writer = new PartitionedParquetWriter(config(root, ClosePolicy.LRU_ONLY));
        writeDays(writer, 5, 10_000);
        writer.write(at(DAY0 + 30_000L));   // 첫날로 되돌아와도
        writer.close();

        var stats = writer.stats();
        assertEquals(0, stats.watermarkCloses());
        assertEquals(0, stats.reopens(), "닫힌 적이 없으니 다시 열릴 일도 없다");
        assertEquals(0, stats.dropped());
    }

    /** 워터마크가 지나간 파티션은 닫혀야 한다. 안 닫히면 정책이 동작하지 않는 것이다. */
    @Test
    void watermarkClosesPartitionsLeftBehind(@TempDir Path root) throws IOException {
        var policy = ClosePolicy.watermarkClose(Duration.ofHours(1));
        var writer = new PartitionedParquetWriter(config(root, policy));
        writeDays(writer, 5, 10_000);
        writer.close();

        var stats = writer.stats();
        assertTrue(stats.watermarkCloses() > 0, "워터마크 닫기가 한 번도 일어나지 않았다");
        assertEquals(5, stats.partitions());
        assertEquals(50_000, stats.rows());
    }

    /**
     * 이 테스트가 W1 의 요점이다 — 늦은 데이터가 닫힌 파티션에 오면 재개봉이 생기고,
     * 그게 곧 small file 이다. 정책은 그 대가를 <b>드러내야</b> 한다.
     */
    @Test
    void lateDataReopensClosedPartition(@TempDir Path root) throws IOException {
        var writer = new PartitionedParquetWriter(
                config(root, ClosePolicy.watermarkClose(Duration.ofHours(1))));
        writeDays(writer, 3, 10_000);
        writer.write(at(DAY0 + 30_000L));   // 첫날로 되돌아온 지연 도착
        writer.close();

        var stats = writer.stats();
        assertEquals(3, stats.partitions());
        assertTrue(stats.reopens() > 0, "재개봉이 0이면 파티션이 닫히지 않았다는 뜻이다");
        assertEquals(30_001, stats.rows(), "재개봉이 일어나도 행은 하나도 잃지 않는다");
        assertEquals(0, stats.dropped());
    }

    /** drop 모드는 파일 수를 지키는 대신 유실을 낸다. 그 교환이 숫자로 보여야 한다. */
    @Test
    void dropModeDiscardsLateDataInsteadOfReopening(@TempDir Path root) throws IOException {
        var writer = new PartitionedParquetWriter(
                config(root, ClosePolicy.watermarkDrop(Duration.ofHours(1))));
        writeDays(writer, 3, 10_000);
        writer.write(at(DAY0 + 30_000L));   // 같은 지연 도착
        writer.close();

        var stats = writer.stats();
        assertEquals(1, stats.dropped(), "워터마크를 벗어난 데이터가 버려지지 않았다");
        assertEquals(30_000, stats.rows());
        assertEquals(0, stats.reopens(), "버렸으면 재개봉은 없어야 한다");
    }

    /** 새 최대 ts 는 스스로에게 걸려 버려지면 안 된다 — 워터마크를 먼저 올리는 이유. */
    @Test
    void newMaximumTimestampIsNeverDropped(@TempDir Path root) throws IOException {
        var writer = new PartitionedParquetWriter(
                config(root, ClosePolicy.watermarkDrop(Duration.ofMinutes(1))));
        for (int d = 0; d < 4; d++) {
            writer.write(at(DAY0 + d * DAY));   // 매번 하루씩 건너뛴다 = 매번 새 최대값
        }
        writer.close();

        assertEquals(0, writer.stats().dropped());
        assertEquals(4, writer.stats().rows());
    }

    @Test
    void rejectsDropWithoutTimeBasedClosing() {
        assertThrows(IllegalArgumentException.class,
                () -> new ClosePolicy("bad", Long.MAX_VALUE, true));
    }
}
