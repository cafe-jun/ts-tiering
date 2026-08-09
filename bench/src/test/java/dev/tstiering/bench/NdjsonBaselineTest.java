package dev.tstiering.bench;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NdjsonBaselineTest {

    private static final String[] ARGS = {
            "--tenants=2", "--devices-per-tenant=3", "--interval-seconds=10"
    };

    /**
     * 스트리밍 카운트가 실제 파일과 <b>한 바이트도</b> 달라선 안 된다.
     * 이 값이 W1/W2 표의 분모로 그대로 들어가므로, 어긋나면 배수 전체가 조용히 틀어진다.
     */
    @Test
    void countingSinkMatchesFileByteForByte(@TempDir Path dir) throws IOException {
        var generator = BenchArgs.parse(ARGS).generator();
        Path file = dir.resolve("baseline.ndjson");

        try (NdjsonDatapointWriter w = new NdjsonDatapointWriter(file)) {
            generator.generate(100_000, w::write);
        }

        CountingOutputStream sink = new CountingOutputStream();
        try (NdjsonDatapointWriter w = NdjsonDatapointWriter.counting(sink)) {
            generator.generate(100_000, w::write);
        }

        assertEquals(Files.size(file), sink.bytes());
    }

    /** --days 는 커버 기간을 정확히 맞춘다. 하루 = 8640틱(10초) × 틱당 포인트. */
    @Test
    void daysOverridesCountAndCoversExactly() {
        var opts = BenchArgs.parse(new String[]{"--tenants=2", "--devices-per-tenant=3",
                "--interval-seconds=10", "--days=1", "--count=999"});
        var generator = opts.generator();

        long pointsPerTick = generator.pointsPerTick(); // 2 × 3 × 5키 = 30
        assertEquals(30, pointsPerTick);
        assertEquals(8640 * 30, opts.countFor(generator));
    }

    /** --days 가 없으면 기존대로 --count 를 쓴다. W1/W2 재현이 깨지면 안 된다. */
    @Test
    void countStillWinsWhenDaysAbsent() {
        var opts = BenchArgs.parse(new String[]{"--count=10_000_000"});
        assertEquals(10_000_000, opts.countFor(opts.generator()));
    }
}
