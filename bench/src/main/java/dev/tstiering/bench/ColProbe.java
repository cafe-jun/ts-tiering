package dev.tstiering.bench;

import dev.tstiering.parquet.ParquetStats;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * 적재 결과 트리에서 파일 하나를 골라 <b>열별 크기와 인코딩</b>을 찍는다.
 *
 * <p>총 크기만 보면 "왜 커졌는가"에 답할 수 없다. W5 에서 정렬을 켰더니 용량이 1.55배로
 * 뛰었는데, 이 도구로 뜯어보니 원인이 {@code ts} 한 열이었다 —
 * 행 그룹 안에서 ts 가 고유해지면서 딕셔너리가 {@code PLAIN}(8바이트/행)으로 폴백했고,
 * Parquet v2 의 {@code DELTA_BINARY_PACKED} 로 바꾸자 사라졌다.
 *
 * <pre>
 * ./gradlew :bench:colProbe --args="data/w5/A-date data/w5/A-date-sorted"
 * </pre>
 */
public final class ColProbe {

    private ColProbe() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("사용법: colProbe <적재 디렉터리> [<적재 디렉터리> ...]");
            return;
        }

        for (String dir : args) {
            Path file = firstTemperatureFile(Path.of(dir));
            var stat = ParquetStats.read(file);

            System.out.printf("%n=== %s ===%n  %s%n  rows=%,d  rowGroups=%d%n",
                    dir, file.getFileName(), stat.rows(), stat.rowGroups());
            System.out.printf("  %-12s %12s %12s %8s  %s%n", "열", "압축전", "압축후", "배율", "인코딩");
            for (var c : stat.columns()) {
                System.out.printf("  %-12s %11dK %11dK %7.1fx  %s%n",
                        c.path(), c.uncompressed() / 1024, c.compressed() / 1024, c.ratio(), c.encodings());
            }
            System.out.printf("  합계 압축후: %,d bytes%n", stat.totalCompressed());
        }
    }

    /** 키마다 특성이 달라 비교가 흐려지므로 연속값 센서 하나로 고정한다. */
    private static Path firstTemperatureFile(Path root) throws IOException {
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(f -> f.toString().contains("key=temperature"))
                    .filter(f -> f.toString().endsWith(".parquet"))
                    .sorted()
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            root + " 아래에 key=temperature Parquet 파일이 없다"));
        }
    }
}
