package dev.tstiering.bench;

import dev.tstiering.core.Datapoint;
import dev.tstiering.core.HivePartitionSpecs;
import dev.tstiering.core.PartitionSpec;
import dev.tstiering.parquet.ParquetStats;
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
 * # 로컬 S3(MinIO)에 올리기까지
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
        LateArrival late = LateArrival.from(opts);

        // 파티션을 언제 닫을지. Phase 1 은 정책이 없어 LRU 축출이 우연한 유예 기간이었다.
        long lagMinutes = opts.number("close-lag-minutes", 60);
        var closePolicy = switch (opts.string("close-policy", "lru")) {
            case "lru" -> dev.tstiering.parquet.ClosePolicy.LRU_ONLY;
            case "watermark-close" -> dev.tstiering.parquet.ClosePolicy.watermarkClose(
                    java.time.Duration.ofMinutes(lagMinutes));
            case "watermark-drop" -> dev.tstiering.parquet.ClosePolicy.watermarkDrop(
                    java.time.Duration.ofMinutes(lagMinutes));
            default -> throw new IllegalArgumentException(
                    "알 수 없는 닫기 정책: " + opts.string("close-policy", "lru")
                            + " (lru / watermark-close / watermark-drop)");
        };

        // 행 그룹은 KiB 단위로도 지정할 수 있어야 한다. 파일이 32 KiB 대인데 기본 32 MiB 를 쓰면
        // 파일당 행 그룹이 1개뿐이라, 정렬 순서를 바꿔도 행 그룹 프루닝이 성립하지 않는다.
        long rowGroupBytes = opts.number("row-group-kib", 0) > 0
                ? opts.number("row-group-kib", 0) * 1024
                : opts.number("row-group-mib", 32) * 1024 * 1024;
        boolean sort = Boolean.parseBoolean(opts.string("sort", "false"));

        // ADR-0004 에 따라 v2 가 기본이다. v1 은 비교 재현용으로만 남긴다 —
        // v1 에는 DELTA_BINARY_PACKED 가 없어 정렬된 ts 가 PLAIN 으로 폴백한다 (W5).
        var writerVersion = "v1".equalsIgnoreCase(opts.string("writer-version", "v2"))
                ? org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0
                : org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

        var config = new PartitionedParquetWriter.Config(
                outDir, spec, codec,
                opts.number("target-file-mib", 128) * 1024 * 1024,
                (int) opts.number("max-open-writers", PartitionedParquetWriter.DEFAULT_MAX_OPEN_WRITERS),
                (int) rowGroupBytes,
                sort,
                writerVersion,
                closePolicy);

        // 스킴/granularity/정렬 조합마다 프리픽스를 갈라야 매트릭스를 한 버킷에 담을 수 있다.
        String s3Prefix = opts.string("s3-prefix", spec.name() + (sort ? "-sorted" : ""));

        System.out.printf("count=%,d  스킴=%s  코덱=%s  행그룹=%,d KiB  정렬=%s  라이터=%s  상한=%d%n",
                count, spec.name(), codec, rowGroupBytes / 1024,
                sort ? "(device_id, ts)" : "도착 순서(ts)", writerVersion, config.maxOpenWriters());
        System.out.printf("S3 프리픽스=%s%n지연 도착=%s  닫기 정책=%s%n",
                s3Prefix, late.describe(), closePolicy.name());

        deleteRecursively(outDir);

        // --- 1) NDJSON 기준선 (디스크에 쓰지 않는다) ---
        CountingOutputStream ndjsonSink = new CountingOutputStream();
        long ndjsonStart = System.nanoTime();
        try (NdjsonDatapointWriter w = NdjsonDatapointWriter.counting(ndjsonSink)) {
            generator.generate(count, w::write);   // 기준선은 순서와 무관하므로 지연을 주지 않는다
        }
        double ndjsonSeconds = seconds(ndjsonStart);

        // --- 2) 파티션 Parquet 적재 ---
        long writeStart = System.nanoTime();
        PartitionedParquetWriter writer = new PartitionedParquetWriter(config);
        generator.generate(count, late, dp -> uncheckedWrite(writer, dp));
        writer.close();
        double writeSeconds = seconds(writeStart);
        var stats = writer.stats();

        // --- 3) S3 업로드 (선택) ---
        List<UploadResult> uploads = List.of();
        double uploadSeconds = 0;
        if (toS3) {
            var settings = S3Settings.local(opts.string("bucket", "ts-tiering-cold"))
                    .withPartSize((int) opts.number("part-size-mib", 8) * 1024 * 1024);
            long uploadStart = System.nanoTime();
            try (S3ObjectStore store = S3ObjectStore.open(settings)) {
                store.createBucketIfAbsent();
                uploads = store.putTree(outDir, s3Prefix);
            }
            uploadSeconds = seconds(uploadStart);
        }

        report(count, ndjsonSink.bytes(), ndjsonSeconds, stats, writeSeconds, uploads, uploadSeconds);
        reportRowGroups(writer.files(), generator.deviceId(0, 0).toString());
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
        System.out.printf("롤오버 / 재개봉 : %,d / %,d   (축출 %,d회, 워터마크 닫기 %,d회)%n",
                stats.rollovers(), stats.reopens(), stats.evictions(), stats.watermarkCloses());
        System.out.printf("동시 열린 최대  : %,d 파티션  (메모리 대리 지표)%n", stats.maxOpenObserved());
        if (stats.dropped() > 0) {
            System.out.printf("유실           : %,d건 (%.3f%%) — 워터마크를 벗어난 지연 도착%n",
                    stats.dropped(), 100.0 * stats.dropped() / (stats.rows() + stats.dropped()));
        }
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

            System.out.println("\n⚠️ MinIO 는 실 S3 가 아니다. 위 시간은 루프백 + MinIO 오버헤드이고");
            System.out.println("   네트워크 지연을 포함하지 않는다. AWS 는 범위 밖이라 절대 처리량은 미검증이다.");
        }
    }

    /**
     * 행 그룹 통계로 프루닝 가능성을 시뮬레이션한다.
     *
     * <p>쿼리 엔진은 행 그룹의 {@code device_id} min/max 를 보고 통째로 건너뛴다.
     * 파일당 행 그룹이 1개면 건너뛸 대상이 없고, 여러 개여도 도착 순서(ts 바깥루프)로 쓰면
     * 모든 행 그룹에 모든 디바이스가 섞여 있어 역시 하나도 못 건너뛴다.
     * 그래서 <b>행 그룹 수와 프루닝 비율을 함께</b> 봐야 한다.
     */
    private static void reportRowGroups(List<Path> files, String targetDevice) throws IOException {
        if (files.isEmpty()) return;

        // 파일이 수천 개일 수 있으므로 표본만 본다. 파일 간 특성이 동일하므로 이걸로 충분하다.
        List<Path> sample = files.size() <= 50 ? files : files.subList(0, 50);

        long allGroups = 0;
        int minGroups = Integer.MAX_VALUE;
        int maxGroups = 0;

        // 대상 디바이스를 아예 담고 있지 않은 파일은 행 그룹 지표에서 뺀다.
        // 그런 파일은 파티션(테넌트/날짜)에서 이미 걸러지는 것이라, 함께 세면 파티션 프루닝이
        // 행 그룹 프루닝인 것처럼 부풀려진다. 실제로 처음엔 이 교란 때문에
        // 정렬하지 않은 경우에도 66.7% 가 걸리는 것처럼 보였다.
        long relevantGroups = 0;
        long matchedGroups = 0;
        int relevantFiles = 0;

        for (Path f : sample) {
            int groups = ParquetStats.read(f).rowGroups();
            allGroups += groups;
            minGroups = Math.min(minGroups, groups);
            maxGroups = Math.max(maxGroups, groups);

            int matched = ParquetStats.rowGroupsMatching(f, "device_id", targetDevice);
            if (matched > 0) {
                relevantFiles++;
                relevantGroups += groups;
                matchedGroups += matched;
            }
        }

        double avgGroups = allGroups / (double) sample.size();
        double pruned = relevantGroups == 0 ? 0 : 100.0 * (relevantGroups - matchedGroups) / relevantGroups;

        System.out.println("\n--- row group (표본 " + sample.size() + "개 파일) ---");
        System.out.printf("파일당 행 그룹    : 평균 %.1f (최소 %d / 최대 %d)%n", avgGroups, minGroups, maxGroups);
        System.out.printf("device_id 프루닝  : %,d/%,d 행 그룹 (%.1f%% 건너뜀) — 해당 디바이스를 담은 파일 %d개 기준%n",
                matchedGroups, relevantGroups, pruned, relevantFiles);
        System.out.printf("                    나머지 %d개 파일은 그 디바이스가 아예 없다 (파티션 프루닝 몫)%n",
                sample.size() - relevantFiles);
        if (avgGroups < 1.5) {
            System.out.println("  ⚠️ 파일당 행 그룹이 사실상 1개다 — 정렬 순서를 바꿔도 건너뛸 대상이 없다.");
            System.out.println("     --row-group-kib 를 낮춰야 이 축이 측정 가능해진다.");
        }
    }

    // --- 보조 -----------------------------------------------------------------

    private static PartitionSpec scheme(String name) {
        return switch (name) {
            case "date-hour" -> HivePartitionSpecs.dateHour();
            case "date" -> HivePartitionSpecs.dateOnly();
            case "tenant-date" -> HivePartitionSpecs.tenantDate();
            case "tenant-profile-date-hour" -> HivePartitionSpecs.tenantProfileDateHour();
            case "tenant-profile-date" -> HivePartitionSpecs.tenantProfileDate();
            case "tenant-device-date" -> HivePartitionSpecs.tenantDeviceDate();
            default -> throw new IllegalArgumentException("알 수 없는 스킴: " + name
                    + " (tenant-date[ADR-0004 기본] / date / date-hour"
                    + " / tenant-profile-date / tenant-profile-date-hour / tenant-device-date)");
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
