package dev.tstiering.bench;

import dev.tstiering.core.Datapoint;
import dev.tstiering.core.HivePartitionSpecs;
import dev.tstiering.core.PartitionSpec;
import dev.tstiering.parquet.PartitionedParquetWriter;
import dev.tstiering.s3.S3ObjectStore;
import dev.tstiering.s3.S3Settings;
import dev.tstiering.s3.UploadResult;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * W3 적재 하네스. 합성 데이터를 파티션된 Parquet 트리로 쓰고, 선택적으로 S3 에 올린 뒤
 * PHASE1.md 의 측정 지표표를 그대로 찍는다.
 *
 * <pre>
 * # 1년 A안, 로컬만 (S3 없이 파일 수·크기·처리량만)
 * ./gradlew :bench:ingest --args="--days=365 --devices-per-tenant=17 --interval-seconds=60"
 *
 * # LocalStack 에 올리기까지
 * docker compose -f deploy/docker-compose.dev.yml up -d
 * ./gradlew :bench:ingest --args="--days=365 --devices-per-tenant=17 --interval-seconds=60 --s3=true"
 *
 * # 파티션 granularity 비교 (small file 곡선)
 * ./gradlew :bench:ingest --args="--days=30 --scheme=tenant-profile-date-hour"
 * ./gradlew :bench:ingest --args="--days=30 --scheme=tenant-profile-date"
 * </pre>
 */
public final class IngestMain {

    public static void main(String[] args) throws IOException {
        BenchArgs opts = BenchArgs.parse(args);

        var generator = opts.generator();
        long count = opts.countFor(generator);
        Path outDir = Path.of(opts.string("out-dir", "data/ingest"));
        PartitionSpec spec = scheme(opts.string("scheme", "tenant-profile-date-hour"));
        CompressionCodecName codec = CompressionCodecName.valueOf(
                opts.string("codec", "ZSTD").toUpperCase());
        boolean toS3 = Boolean.parseBoolean(opts.string("s3", "false"));

        var config = new PartitionedParquetWriter.Config(
                outDir, spec, codec,
                opts.number("target-file-mib", 128) * 1024 * 1024,
                (int) opts.number("max-open-writers", PartitionedParquetWriter.DEFAULT_MAX_OPEN_WRITERS),
                (int) opts.number("row-group-mib", 32) * 1024 * 1024);

        System.out.printf("count=%,d  스킴=%s  코덱=%s  목표 파일=%,d MiB  라이터 상한=%d%n",
                count, spec.name(), codec, config.targetFileBytes() / 1024 / 1024, config.maxOpenWriters());

        deleteRecursively(outDir);

        // --- 1) NDJSON 기준선 (디스크에 쓰지 않는다) ---
        CountingOutputStream ndjsonSink = new CountingOutputStream();
        long ndjsonStart = System.nanoTime();
        try (NdjsonDatapointWriter w = NdjsonDatapointWriter.counting(ndjsonSink)) {
            generator.generate(count, w::write);
        }
        double ndjsonSeconds = seconds(ndjsonStart);

        // --- 2) 파티션 Parquet 적재 ---
        long writeStart = System.nanoTime();
        PartitionedParquetWriter writer = new PartitionedParquetWriter(config);
        generator.generate(count, dp -> uncheckedWrite(writer, dp));
        writer.close();
        double writeSeconds = seconds(writeStart);
        var stats = writer.stats();

        // --- 3) S3 업로드 (선택) ---
        List<UploadResult> uploads = List.of();
        double uploadSeconds = 0;
        if (toS3) {
            var settings = S3Settings.localstack(opts.string("bucket", "ts-tiering-cold"))
                    .withPartSize((int) opts.number("part-size-mib", 8) * 1024 * 1024);
            long uploadStart = System.nanoTime();
            try (S3ObjectStore store = S3ObjectStore.open(settings)) {
                store.createBucketIfAbsent();
                uploads = store.putTree(outDir, spec.name());
            }
            uploadSeconds = seconds(uploadStart);
        }

        report(count, ndjsonSink.bytes(), ndjsonSeconds, stats, writeSeconds, uploads, uploadSeconds);
    }

    // --- 출력 -----------------------------------------------------------------

    private static void report(long count,
                               long ndjsonBytes,
                               double ndjsonSeconds,
                               PartitionedParquetWriter.Stats stats,
                               double writeSeconds,
                               List<UploadResult> uploads,
                               double uploadSeconds) {
        System.out.println("\n=== 적재 결과 ===");
        System.out.printf("건수            : %,d%n", count);
        System.out.printf("NDJSON 기준선   : %,d bytes (%.1f MiB, %.1f bytes/건, %.1fs)%n",
                ndjsonBytes, mib(ndjsonBytes), ndjsonBytes / (double) count, ndjsonSeconds);
        System.out.printf("Parquet 총 크기 : %,d bytes (%.1f MiB) — NDJSON 대비 %.1f배, 건당 %.2f bytes%n",
                stats.bytes(), mib(stats.bytes()),
                ndjsonBytes / (double) stats.bytes(), stats.bytes() / (double) count);
        System.out.printf("적재 처리량     : %,.0f pt/s (%.1fs)%n", count / writeSeconds, writeSeconds);

        System.out.println("\n--- small file ---");
        System.out.printf("파티션 수       : %,d  (이상적인 파일 수의 하한)%n", stats.partitions());
        System.out.printf("파일 수         : %,d%n", stats.files());
        System.out.printf("평균 파일 크기  : %,.1f KiB%n", stats.averageFileBytes() / 1024.0);
        System.out.printf("롤오버 / 재개봉 : %,d / %,d   (축출 %,d회)%n",
                stats.rollovers(), stats.reopens(), stats.evictions());
        if (stats.reopens() > 0) {
            System.out.println("  ⚠️ 재개봉이 있었다 — 축출된 파티션에 데이터가 다시 와서 파일이 쪼개졌다.");
            System.out.println("     --max-open-writers 를 올려야 스킴 간 비교가 공정해진다.");
        }

        if (!uploads.isEmpty()) {
            System.out.println("\n--- S3 업로드 ---");
            long totalBytes = uploads.stream().mapToLong(UploadResult::bytes).sum();
            long multipartCount = uploads.stream().filter(UploadResult::multipart).count();
            System.out.printf("객체 수         : %,d (multipart %,d)%n", uploads.size(), multipartCount);
            System.out.printf("총 업로드       : %.1f MiB, %.1fs (%.1f MiB/s)%n",
                    mib(totalBytes), uploadSeconds, mib(totalBytes) / uploadSeconds);

            System.out.println("\n느린 순 상위 10:");
            System.out.printf("  %-10s %8s %6s  %s%n", "MiB", "ms", "파트", "키");
            uploads.stream()
                    .sorted(Comparator.comparing(UploadResult::elapsed).reversed())
                    .limit(10)
                    .forEach(r -> System.out.printf("  %-10.2f %8d %6d  %s%n",
                            r.mib(), r.elapsed().toMillis(), r.parts(), r.key()));

            System.out.println("\n⚠️ LocalStack 은 실 S3 가 아니다. 위 시간은 루프백 + LocalStack 오버헤드이고");
            System.out.println("   네트워크 지연을 포함하지 않는다. 절대 처리량은 W8(실 AWS)에서 다시 잰다.");
        }
    }

    // --- 보조 -----------------------------------------------------------------

    private static PartitionSpec scheme(String name) {
        return switch (name) {
            case "date-hour" -> HivePartitionSpecs.dateHour();
            case "tenant-profile-date-hour" -> HivePartitionSpecs.tenantProfileDateHour();
            case "tenant-profile-date" -> HivePartitionSpecs.tenantProfileDate();
            case "tenant-device-date" -> HivePartitionSpecs.tenantDeviceDate();
            default -> throw new IllegalArgumentException("알 수 없는 스킴: " + name
                    + " (date-hour / tenant-profile-date-hour / tenant-profile-date / tenant-device-date)");
        };
    }

    private static void uncheckedWrite(PartitionedParquetWriter w, Datapoint dp) {
        try {
            w.write(dp);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static double seconds(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000_000.0;
    }

    private static double mib(long bytes) {
        return bytes / 1024.0 / 1024.0;
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (Stream<Path> s = Files.walk(dir)) {
            s.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }
}
