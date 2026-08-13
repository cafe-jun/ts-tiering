package dev.tstiering.bench;

import dev.tstiering.core.Datapoint;
import dev.tstiering.parquet.ParquetDatapointWriter;
import dev.tstiering.parquet.ParquetStats;
import dev.tstiering.parquet.PerKeyParquetWriter;
import dev.tstiering.parquet.ValueLayout;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * ADR-0002 의 근거를 만든다. 값 레이아웃 3종 × 코덱 3종을 NDJSON 기준선과 비교한다.
 *
 * <pre>
 * ./gradlew :bench:parquetBench --args="--count=10_000_000"
 * </pre>
 */
public final class ParquetBenchMain {

    private static final List<CompressionCodecName> CODECS =
            List.of(CompressionCodecName.SNAPPY, CompressionCodecName.GZIP, CompressionCodecName.ZSTD);

    record Result(String layout, String codec, int files, long bytes, double seconds) {
    }

    public static void main(String[] args) throws IOException {
        BenchArgs opts = BenchArgs.parse(args);
        long count = opts.number("count", 1_000_000);
        Path outDir = Path.of(opts.string("out-dir", "data/parquet-bench"));

        var generator = opts.generator();
        deleteRecursively(outDir);

        System.out.printf("count=%,d  tenants=%d  devices/tenant=%d  interval=%ds%n%n",
                count, opts.tenants(), opts.devicesPerTenant(), opts.intervalMillis() / 1000);

        List<Result> results = new ArrayList<>();

        // 기준선: NDJSON
        Path ndjson = outDir.resolve("baseline.ndjson");
        results.add(time("NDJSON", "none", 1, () -> {
            try (NdjsonDatapointWriter w = new NdjsonDatapointWriter(ndjson)) {
                generator.generate(count, w::write);
            }
            return sizeOf(ndjson);
        }));

        // 단일 파일 레이아웃
        for (ValueLayout layout : List.of(ValueLayout.SPARSE_TYPED, ValueLayout.STRINGIFIED)) {
            for (CompressionCodecName codec : CODECS) {
                Path file = outDir.resolve(layout.name().toLowerCase() + "-" + codec.name().toLowerCase() + ".parquet");
                results.add(time(layout.name(), codec.name(), 1, () -> {
                    // W2 재현 전용이라 라이터 버전을 v1 로 고정한다. 그 표가 v1 에서 나왔다.
                    try (ParquetDatapointWriter w = ParquetDatapointWriter.open(
                            file, layout, null, codec,
                            ParquetDatapointWriter.DEFAULT_ROW_GROUP_SIZE,
                            org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0)) {
                        generator.generate(count, dp -> uncheckedWrite(w, dp));
                    }
                    return sizeOf(file);
                }));
            }
        }

        // 키별 팬아웃 레이아웃
        for (CompressionCodecName codec : CODECS) {
            Path dir = outDir.resolve("per_key-" + codec.name().toLowerCase());
            results.add(time(ValueLayout.PER_KEY_TYPED.name(), codec.name(), 5, () -> {
                try (PerKeyParquetWriter w = new PerKeyParquetWriter(dir, codec)) {
                    generator.generate(count, dp -> uncheckedWrite(w, dp));
                }
                return dirSize(dir);
            }));
        }

        printTable(results, count);
        printColumnBreakdown(outDir);
    }

    // --- 출력 -----------------------------------------------------------------

    private static void printTable(List<Result> results, long count) {
        long baseline = results.get(0).bytes();

        System.out.println("=== 레이아웃 × 코덱 ===");
        System.out.printf("%-14s %-8s %6s %14s %10s %8s %10s%n",
                "레이아웃", "코덱", "파일", "bytes", "MiB", "압축비", "쓰기");
        System.out.println("-".repeat(78));
        for (Result r : results) {
            System.out.printf("%-14s %-8s %6d %14d %10.1f %7.1fx %8.1fs%n",
                    r.layout(), r.codec(), r.files(), r.bytes(),
                    r.bytes() / 1024.0 / 1024.0,
                    baseline / (double) r.bytes(),
                    r.seconds());
        }

        Result best = results.stream()
                .filter(r -> !r.layout().equals("NDJSON"))
                .min(Comparator.comparingLong(Result::bytes))
                .orElseThrow();
        System.out.printf("%n최소 용량: %s + %s → %.1f MiB (기준선 대비 %.1f배, 건당 %.1f bytes)%n",
                best.layout(), best.codec(), best.bytes() / 1024.0 / 1024.0,
                baseline / (double) best.bytes(), best.bytes() / (double) count);
    }

    private static void printColumnBreakdown(Path outDir) throws IOException {
        System.out.println("\n=== 열별 내역 (ZSTD 기준) ===");
        for (ValueLayout layout : List.of(ValueLayout.SPARSE_TYPED, ValueLayout.STRINGIFIED)) {
            Path file = outDir.resolve(layout.name().toLowerCase() + "-zstd.parquet");
            if (!Files.exists(file)) continue;
            var stat = ParquetStats.read(file);
            System.out.printf("%n[%s]  rows=%,d  rowGroups=%d%n", layout, stat.rows(), stat.rowGroups());
            System.out.printf("  %-12s %12s %12s %8s  %s%n", "열", "압축전", "압축후", "배율", "인코딩");
            for (var c : stat.columns()) {
                System.out.printf("  %-12s %11dK %11dK %7.1fx  %s%n",
                        c.path(), c.uncompressed() / 1024, c.compressed() / 1024, c.ratio(), c.encodings());
            }
        }

        Path perKeyDir = outDir.resolve("per_key-zstd");
        if (Files.isDirectory(perKeyDir)) {
            System.out.printf("%n[PER_KEY_TYPED]  키별 파일 크기%n");
            try (Stream<Path> files = Files.walk(perKeyDir)) {
                for (Path f : files.filter(p -> p.toString().endsWith(".parquet")).sorted().toList()) {
                    var stat = ParquetStats.read(f);
                    System.out.printf("  %-28s %8.2f MiB  rows=%,d%n",
                            perKeyDir.relativize(f), Files.size(f) / 1024.0 / 1024.0, stat.rows());
                }
            }
        }
    }

    // --- 보조 -----------------------------------------------------------------

    @FunctionalInterface
    private interface SizedRun {
        long run() throws IOException;
    }

    private static Result time(String layout, String codec, int files, SizedRun body) throws IOException {
        long startedAt = System.nanoTime();
        long bytes = body.run();
        double seconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
        System.out.printf("  완료: %-14s %-8s %8.1f MiB  %5.1fs%n",
                layout, codec, bytes / 1024.0 / 1024.0, seconds);
        return new Result(layout, codec, files, bytes, seconds);
    }

    private static void uncheckedWrite(ParquetDatapointWriter w, Datapoint dp) {
        try {
            w.write(dp);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void uncheckedWrite(PerKeyParquetWriter w, Datapoint dp) {
        try {
            w.write(dp);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long sizeOf(Path p) throws IOException {
        return Files.size(p);
    }

    private static long dirSize(Path dir) throws IOException {
        try (Stream<Path> s = Files.walk(dir)) {
            return s.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).sum();
        }
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
