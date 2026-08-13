package dev.tstiering.parquet;

import dev.tstiering.core.Datapoint;
import dev.tstiering.core.TsValue;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 이 테스트가 도는 것 자체가 ADR-0001 의 검증이다 —
 * 런타임 클래스패스에 hadoop-common 이 없으므로, 쓰기 경로가 Hadoop 클래스를 하나라도 건드리면
 * NoClassDefFoundError 로 즉시 실패한다.
 */
class ParquetWriteTest {

    @TempDir
    Path tmp;

    private static final UUID TENANT = new UUID(1, 1);
    private static final UUID DEVICE = new UUID(2, 2);

    private static List<Datapoint> sample(int n) {
        return java.util.stream.IntStream.range(0, n)
                .mapToObj(i -> new Datapoint(TENANT, "p", DEVICE, "temperature",
                        1_767_225_600_000L + i * 10_000L,
                        new TsValue.DoubleValue(20.0 + (i % 100) / 10.0)))
                .toList();
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodecName.class,
            names = {"UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD"})
    @DisplayName("Hadoop 없이 코덱별로 쓰이고, 푸터가 다시 읽힌다")
    void writesAndReadsFooterWithoutHadoop(CompressionCodecName codec) throws IOException {
        Path file = tmp.resolve(codec.name() + ".parquet");
        List<Datapoint> rows = sample(5_000);

        try (var writer = ParquetDatapointWriter.open(file, ValueLayout.SPARSE_TYPED, null, codec)) {
            for (Datapoint dp : rows) {
                writer.write(dp);
            }
        }

        assertTrue(Files.size(file) > 0, "파일이 비어 있다");

        var stat = ParquetStats.read(file);
        assertEquals(rows.size(), stat.rows());
        assertTrue(stat.rowGroups() >= 1);

        var dbl = stat.columns().stream().filter(c -> c.path().equals("dbl_v")).findFirst().orElseThrow();
        assertTrue(dbl.compressed() > 0, "dbl_v 가 비었다");

        // 값이 들어간 컬럼만 존재해야 한다 — SPARSE_TYPED 는 나머지를 null 로 둔다
        assertNotNull(stat.columns().stream().filter(c -> c.path().equals("long_v")).findFirst().orElse(null));
    }

    @Test
    @DisplayName("PER_KEY_TYPED 는 키마다 파일을 만들고 key 컬럼을 없앤다")
    void perKeyLayoutSplitsFiles() throws IOException {
        Path dir = tmp.resolve("perkey");
        try (var writer = new PerKeyParquetWriter(dir, CompressionCodecName.ZSTD)) {
            for (int i = 0; i < 1_000; i++) {
                long ts = 1_767_225_600_000L + i * 10_000L;
                writer.write(new Datapoint(TENANT, "p", DEVICE, "temperature", ts,
                        new TsValue.DoubleValue(21.5)));
                writer.write(new Datapoint(TENANT, "p", DEVICE, "running", ts,
                        new TsValue.BoolValue(true)));
            }
        }

        Path temp = dir.resolve("key=temperature/part-0.parquet");
        Path run = dir.resolve("key=running/part-0.parquet");
        assertTrue(Files.exists(temp) && Files.exists(run), "키별 파일이 만들어지지 않았다");

        var stat = ParquetStats.read(temp);
        assertEquals(1_000, stat.rows());
        assertTrue(stat.columns().stream().noneMatch(c -> c.path().equals("key")),
                "PER_KEY_TYPED 에는 key 컬럼이 없어야 한다");
        assertTrue(stat.columns().stream().anyMatch(c -> c.path().equals("value")));
    }
}
