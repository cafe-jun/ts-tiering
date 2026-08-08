package dev.tstiering.parquet;

import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parquet 푸터에서 열별 크기와 인코딩을 뽑는다. 어느 열이 용량을 먹는지 보려고 쓴다.
 *
 * <p><b>푸터를 직접 파싱하는 이유</b> (ADR-0001): {@code ParquetFileReader.open()} 은
 * {@code ParquetReadOptions.Builder} 를 거치는데, 그 생성자가 {@code ParquetInputFormat.getFilter()} 를
 * 호출한다. {@code ParquetInputFormat} 은 Hadoop MapReduce 의 {@code FileInputFormat} 을 상속하므로
 * 클래스 로딩만으로 hadoop-mapreduce-client-core → hadoop-common 전체가 필요해진다.
 * 쓰기 경로는 {@link PlainCodecFactory} 로 떼어냈지만 읽기 옵션은 우회 지점이 없다.
 *
 * <p>푸터 포맷 자체는 단순하다 — 파일 끝 8바이트가 {@code [footerLength:int32-LE][MAGIC:"PAR1"]} 이고,
 * 그 앞이 Thrift 로 직렬화된 FileMetaData 다. parquet-format-structures 의
 * {@code Util.readFileMetaData} 는 Hadoop 과 무관하므로 그대로 쓸 수 있다.
 */
public final class ParquetStats {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);
    private static final int FOOTER_TAIL = 8; // footerLength(4) + MAGIC(4)

    private ParquetStats() {
    }

    public record ColumnStat(String path, long uncompressed, long compressed, String encodings) {
        public double ratio() {
            return compressed == 0 ? 0 : uncompressed / (double) compressed;
        }
    }

    public record FileStat(long rows, int rowGroups, List<ColumnStat> columns) {
        public long totalCompressed() {
            return columns.stream().mapToLong(ColumnStat::compressed).sum();
        }

        public long totalUncompressed() {
            return columns.stream().mapToLong(ColumnStat::uncompressed).sum();
        }
    }

    public static FileStat read(Path file) throws IOException {
        ParquetMetadata footer = readFooter(new LocalInputFile(file));

        // 열별로 모든 row group 의 수치를 합산한다.
        Map<String, long[]> sizes = new LinkedHashMap<>();
        Map<String, Set<String>> encodings = new LinkedHashMap<>();
        long rows = 0;

        for (BlockMetaData block : footer.getBlocks()) {
            rows += block.getRowCount();
            for (ColumnChunkMetaData col : block.getColumns()) {
                String path = String.join(".", col.getPath().toArray());
                long[] acc = sizes.computeIfAbsent(path, k -> new long[2]);
                acc[0] += col.getTotalUncompressedSize();
                acc[1] += col.getTotalSize();
                Set<String> enc = encodings.computeIfAbsent(path, k -> new LinkedHashSet<>());
                col.getEncodings().forEach(e -> enc.add(e.name()));
            }
        }

        List<ColumnStat> columns = new ArrayList<>();
        sizes.forEach((path, acc) ->
                columns.add(new ColumnStat(path, acc[0], acc[1], String.join(",", encodings.get(path)))));

        return new FileStat(rows, footer.getBlocks().size(), columns);
    }

    /** Hadoop 없이 푸터만 읽는다. */
    public static ParquetMetadata readFooter(InputFile input) throws IOException {
        long length = input.getLength();
        if (length < FOOTER_TAIL + MAGIC.length) {
            throw new IOException("Parquet 파일로 보기엔 너무 작다: " + length + " bytes");
        }

        try (SeekableInputStream in = input.newStream()) {
            in.seek(length - FOOTER_TAIL);
            byte[] tail = new byte[FOOTER_TAIL];
            in.readFully(tail);

            if (!Arrays.equals(Arrays.copyOfRange(tail, 4, 8), MAGIC)) {
                throw new IOException("파일 끝에 PAR1 매직이 없다 — Parquet 파일이 아니다");
            }
            int footerLength = ByteBuffer.wrap(tail, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
            if (footerLength <= 0 || footerLength > length - FOOTER_TAIL) {
                throw new IOException("푸터 길이가 비정상이다: " + footerLength);
            }

            in.seek(length - FOOTER_TAIL - footerLength);
            return new ParquetMetadataConverter().fromParquetMetadata(Util.readFileMetaData(in));
        }
    }
}
