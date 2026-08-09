package dev.tstiering.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * NDJSON 으로 합성 데이터를 떨군다. 이 파일 크기가 <b>압축률 비교의 기준선</b>이다.
 *
 * <pre>
 * ./gradlew :bench:generate --args="--count=10_000_000 --out=data/raw.ndjson"
 * </pre>
 */
public final class GenerateMain {

    public static void main(String[] args) throws IOException {
        BenchArgs opts = BenchArgs.parse(args);

        var generator = opts.generator();
        long count = opts.countFor(generator);
        String outArg = opts.string("out", "data/raw.ndjson");

        // --out=none 이면 디스크에 쓰지 않고 크기만 잰다. 1년치 25 GB 를 만들 이유가 없을 때 쓴다.
        boolean toDisk = !"none".equals(outArg);
        Path out = toDisk ? Path.of(outArg) : null;

        System.out.printf("생성 시작: count=%,d tenants=%d devices/tenant=%d interval=%ds%n",
                count, opts.tenants(), opts.devicesPerTenant(), opts.intervalMillis() / 1000);
        System.out.printf("  틱당 포인트=%,d  →  커버 기간 약 %s%n",
                generator.pointsPerTick(),
                BenchArgs.humanDuration(Duration.ofMillis(
                        count / Math.max(1, generator.pointsPerTick()) * opts.intervalMillis())));

        long startedAt = System.nanoTime();
        long bytes;
        if (toDisk) {
            try (NdjsonDatapointWriter writer = new NdjsonDatapointWriter(out)) {
                generator.generate(count, writer::write);
            }
            bytes = Files.size(out);
        } else {
            CountingOutputStream sink = new CountingOutputStream();
            try (NdjsonDatapointWriter writer = NdjsonDatapointWriter.counting(sink)) {
                generator.generate(count, writer::write);
            }
            bytes = sink.bytes();
        }
        double seconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;

        System.out.println("---");
        System.out.printf("파일      : %s%n", toDisk ? out.toAbsolutePath() : "(없음 — 크기만 측정)");
        System.out.printf("건수      : %,d%n", count);
        System.out.printf("크기      : %,d bytes (%.2f MiB)%n", bytes, bytes / 1024.0 / 1024.0);
        System.out.printf("건당 크기 : %.1f bytes%n", bytes / (double) count);
        System.out.printf("소요      : %.1fs (%,.0f pt/s)%n", seconds, count / seconds);
        System.out.println();
        System.out.println("이 크기가 Parquet 압축률의 분모다. docs/benchmark/ 에 기록할 것.");
    }
}
