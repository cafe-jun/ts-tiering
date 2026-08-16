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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ADR-0006 결정 8 — <b>파일이 어느 오프셋 구간을 담았는지 파일 자체가 알고 있어야 한다.</b>
 *
 * <p>{@code kill -9} 측정에서 중복 100% 가 나온 직접 원인이 이 정보의 부재였다.
 * 재시작 복구는 {@code ready/} 에 남은 파일이 어차피 재생될 구간인지 알 수 없어
 * "올리는 쪽"을 택할 수밖에 없었고, 그건 확정적 중복이다.
 */
class FooterMetadataTest {

    private static final UUID TENANT = new UUID(0x7E4A47L, 0);
    private static final long START = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();

    private static Datapoint point(long ts) {
        return new Datapoint(TENANT, "industrial-sensor", new UUID(0xDE71CEL, 0),
                "temperature", ts, new TsValue.DoubleValue(20.1));
    }

    private static List<Path> parquetFiles(Path root) throws IOException {
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> p.toString().endsWith(".parquet")).sorted().toList();
        }
    }

    @Test
    void closedFileCarriesSuppliedMetadataInItsFooter(@TempDir Path root) throws IOException {
        var config = PartitionedParquetWriter.Config
                .of(root, HivePartitionSpecs.tenantDate(), CompressionCodecName.ZSTD)
                .withFileMetadata(slotDir -> Map.of(
                        "ts-tiering.kafka.topic", "telemetry",
                        "ts-tiering.kafka.partition", "3",
                        "ts-tiering.kafka.min_offset", "1000",
                        "ts-tiering.kafka.max_offset", "1999"));

        try (var writer = new PartitionedParquetWriter(config)) {
            writer.write(point(START));
        }

        Path file = parquetFiles(root).get(0);
        Map<String, String> meta = ParquetStats.footerMetadata(file);

        assertEquals("telemetry", meta.get("ts-tiering.kafka.topic"));
        assertEquals("3", meta.get("ts-tiering.kafka.partition"));
        assertEquals("1000", meta.get("ts-tiering.kafka.min_offset"));
        assertEquals("1999", meta.get("ts-tiering.kafka.max_offset"));
        // 레이아웃은 init 에서 넣는 값이다. finalizeWrite 가 그걸 지우면 안 된다.
        assertEquals("PER_KEY_TYPED", meta.get("ts-tiering.layout"));
    }

    /**
     * 파일명의 닫힌 시각과 푸터의 닫힌 시각은 <b>같은 값이어야 한다.</b>
     *
     * <p>조회 쪽 "나중 파일이 이긴다"는 파일명으로 판정한다({@code ORDER BY filename DESC}).
     * 푸터가 다른 값을 말하면 compaction 과 조회가 서로 다른 순서를 보게 된다.
     */
    @Test
    void closeEpochInFilenameMatchesFooter(@TempDir Path root) throws IOException {
        var config = PartitionedParquetWriter.Config
                .of(root, HivePartitionSpecs.tenantDate(), CompressionCodecName.ZSTD);

        try (var writer = new PartitionedParquetWriter(config)) {
            writer.write(point(START));
        }

        Path file = parquetFiles(root).get(0);
        String name = file.getFileName().toString();
        String fromName = name.substring("part-".length(), name.indexOf('-', "part-".length()));

        assertEquals(fromName, ParquetStats.footerMetadata(file).get("ts-tiering.close_epoch"));
        assertFalse(ParquetStats.footerMetadata(file).get("ts-tiering.writer_id").isEmpty());
    }

    /** 임시 이름(part-open-…)이 남으면 업로드 스캔에 잡혀 푸터 없는 객체가 cold 로 간다. */
    @Test
    void noOpenNamedFileSurvivesClose(@TempDir Path root) throws IOException {
        var config = PartitionedParquetWriter.Config
                .of(root, HivePartitionSpecs.tenantDate(), CompressionCodecName.ZSTD);

        try (var writer = new PartitionedParquetWriter(config)) {
            for (int day = 0; day < 3; day++) {
                writer.write(point(START + day * 86_400_000L));
            }
        }

        for (Path p : parquetFiles(root)) {
            assertFalse(p.getFileName().toString().startsWith("part-open-"),
                    "닫힌 뒤에도 임시 이름이 남았다: " + p);
        }
    }

    /**
     * 리스너가 받는 경로는 <b>최종 경로</b>여야 한다. 임시 경로를 받으면 archiver 가
     * 존재하지 않는 파일을 {@code ready/} 로 옮기려 든다.
     */
    @Test
    void listenerReceivesFinalPath(@TempDir Path root) throws IOException {
        List<Path> announced = new ArrayList<>();
        var config = PartitionedParquetWriter.Config
                .of(root, HivePartitionSpecs.tenantDate(), CompressionCodecName.ZSTD)
                .withClosedFileListener((slotDir, file, rows) -> announced.add(file));

        try (var writer = new PartitionedParquetWriter(config)) {
            writer.write(point(START));
        }

        assertEquals(1, announced.size());
        assertTrue(Files.exists(announced.get(0)), "리스너가 받은 경로가 실재하지 않는다");
        assertEquals(parquetFiles(root), announced);
    }
}
