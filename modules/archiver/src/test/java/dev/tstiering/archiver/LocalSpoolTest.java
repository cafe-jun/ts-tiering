package dev.tstiering.archiver;

import dev.tstiering.core.Datapoint;
import dev.tstiering.core.TsValue;
import dev.tstiering.parquet.ParquetDatapointWriter;
import dev.tstiering.parquet.ValueLayout;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ADR-0006 의 불변식 셋을 고정한다.
 *
 * <p>실제 Parquet 파일로 검증한다 — 빈 파일이나 더미로는 "푸터가 있다"를 확인할 수 없고,
 * 그게 이 클래스의 존재 이유다.
 */
class LocalSpoolTest {

    private static final UUID TENANT = new UUID(0x7E4A47L, 0);
    private static final UUID DEVICE = new UUID(0xDE71CEL, 0);
    private static final long TS = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
    private static final String REL = "tenant=" + TENANT + "/date=2026-01-01/key=temperature/part-0.parquet";

    /** 실제로 닫힌 Parquet 파일을 만든다. 푸터가 있어야 불변식 1을 검증할 수 있다. */
    private static void writeClosedParquet(Path path) throws IOException {
        try (var w = ParquetDatapointWriter.open(path, ValueLayout.PER_KEY_TYPED,
                TsValue.Kind.DOUBLE, CompressionCodecName.ZSTD)) {
            w.write(new Datapoint(TENANT, "industrial-sensor", DEVICE, "temperature", TS,
                    new TsValue.DoubleValue(20.1)));
        }
    }

    /** 푸터 없는 조각. 크래시 순간 열려 있던 파일을 흉내낸다. */
    private static void writeTruncatedFile(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        Files.write(path, "PAR1 not a real footer".getBytes());
    }

    @Test
    void promoteMovesFileFromInflightToReady(@TempDir Path root) throws IOException {
        var spool = new LocalSpool(root);
        Path inflight = spool.inflight(REL);
        writeClosedParquet(inflight);

        Path ready = spool.promote(inflight);

        assertFalse(Files.exists(inflight), "승격 후 inflight 에 남아 있으면 안 된다");
        assertTrue(Files.exists(ready));
        assertTrue(ready.startsWith(spool.readyRoot()));
        assertTrue(ready.toString().endsWith(REL), "파티션 경로가 보존돼야 한다");
    }

    /**
     * <b>불변식 2 + 재시작 복구.</b> 쓰는 중이던 파일은 폐기되고, 닫힌 파일만 업로드 큐에 남는다.
     * 이게 안 되면 푸터 없는 객체가 cold 에 올라가 그 파티션 조회 전체가 실패한다.
     */
    @Test
    void recoverDiscardsInflightAndReturnsOnlyReadyFiles(@TempDir Path root) throws IOException {
        var spool = new LocalSpool(root);

        // 닫힌 파일 하나는 승격까지 끝냈다
        Path done = spool.inflight(REL);
        writeClosedParquet(done);
        Path ready = spool.promote(done);

        // 크래시 순간 쓰는 중이던 조각이 inflight 에 남아 있다
        writeTruncatedFile(spool.inflight("tenant=" + TENANT + "/date=2026-01-02/key=temperature/part-0.parquet"));

        List<Path> queue = spool.recover();

        assertEquals(List.of(ready), queue, "업로드 큐에는 ready 의 파일만 들어가야 한다");
        try (var s = Files.walk(root.resolve("inflight"))) {
            assertEquals(0, s.filter(Files::isRegularFile).count(), "inflight 는 비워져야 한다");
        }
    }

    /** <b>불변식 1.</b> ready 의 파일은 전부 푸터가 있어야 한다 — 유일한 검증 수단이다. */
    @Test
    void verifyReadyPassesForProperlyClosedFiles(@TempDir Path root) throws IOException {
        var spool = new LocalSpool(root);
        Path inflight = spool.inflight(REL);
        writeClosedParquet(inflight);
        spool.promote(inflight);

        assertEquals(List.of(), spool.verifyReady());
    }

    /** 깨진 파일이 ready 에 있으면 반드시 잡아야 한다. 못 잡으면 검증이 무의미하다. */
    @Test
    void verifyReadyDetectsFileWithoutFooter(@TempDir Path root) throws IOException {
        var spool = new LocalSpool(root);
        Path broken = spool.readyRoot().resolve(REL);
        writeTruncatedFile(broken);

        assertEquals(List.of(broken), spool.verifyReady(),
                "푸터 없는 파일을 못 잡으면 불변식 1을 확인할 방법이 없다");
    }

    /** <b>불변식 3.</b> 업로드 후 지워야 "ready 에 있다 = 아직 안 올라감"이 성립한다. */
    @Test
    void releasedRemovesFileAndPrunesEmptyDirectories(@TempDir Path root) throws IOException {
        var spool = new LocalSpool(root);
        Path inflight = spool.inflight(REL);
        writeClosedParquet(inflight);
        Path ready = spool.promote(inflight);

        spool.released(ready);

        assertFalse(Files.exists(ready));
        assertEquals(List.of(), spool.recover(), "업로드가 끝났으면 큐가 비어야 한다");
        assertFalse(Files.exists(ready.getParent()), "빈 파티션 디렉터리가 쌓이면 스캔이 느려진다");
        assertTrue(Files.isDirectory(spool.readyRoot()), "ready 루트까지 지우면 안 된다");
    }

    /** 재시작을 여러 번 해도 같은 결과여야 한다. */
    @Test
    void recoverIsIdempotent(@TempDir Path root) throws IOException {
        var spool = new LocalSpool(root);
        Path inflight = spool.inflight(REL);
        writeClosedParquet(inflight);
        Path ready = spool.promote(inflight);

        assertEquals(List.of(ready), spool.recover());
        assertEquals(List.of(ready), spool.recover());
    }

    /** inflight 는 업로드 스캔에 절대 걸리면 안 된다 — ready 의 형제여야 하는 이유다. */
    @Test
    void inflightIsNotUnderReadyRoot(@TempDir Path root) throws IOException {
        var spool = new LocalSpool(root);
        assertFalse(spool.inflight(REL).startsWith(spool.readyRoot()),
                "inflight 가 ready 아래 있으면 업로드 스캔이 쓰는 중인 파일을 집어간다");
    }

    @Test
    void recoverOnEmptySpoolReturnsNothing(@TempDir Path root) throws IOException {
        assertEquals(List.of(), new LocalSpool(root).recover());
    }
}
