package dev.tstiering.parquet;

import dev.tstiering.core.Datapoint;
import dev.tstiering.core.TsValue;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Parquet 파일 하나에 Datapoint 를 쓴다.
 *
 * <p>Hadoop 클래스패스를 요구하지 않는다 — {@link LocalOutputFile} 과
 * {@link PlainParquetConfiguration} 만 쓰고 Hadoop {@code Configuration} 경로는 타지 않는다.
 * (ADR-0001)
 */
public final class ParquetDatapointWriter implements Closeable {

    /** 기본 128MiB. row group 이 클수록 압축률과 프루닝 효율이 오르지만 쓰기 메모리를 더 쓴다. */
    public static final int DEFAULT_ROW_GROUP_SIZE = 128 * 1024 * 1024;

    /**
     * ADR-0004 에 따라 v2 가 기본이다. parquet-java 의 기본값은 {@code PARQUET_1_0} 인데,
     * 그 버전에는 {@code DELTA_BINARY_PACKED} 가 없어 정렬된 {@code ts} 가
     * {@code PLAIN}(8바이트/행)으로 폴백해 저장이 1.55배로 뛴다 (W5).
     *
     * <p>이 상수를 {@link PartitionedParquetWriter} 에만 두면 4-인자 {@code open()} 을 쓰는
     * 호출자가 조용히 v1 을 받는다 — 프로젝트가 스스로 발견한 함정을 자기 기본값으로 반복하는 꼴이다.
     * W2 재현처럼 v1 이 필요한 곳은 명시적으로 넘길 것.
     */
    public static final ParquetProperties.WriterVersion DEFAULT_WRITER_VERSION =
            ParquetProperties.WriterVersion.PARQUET_2_0;

    private final ParquetWriter<Datapoint> writer;
    private final Path path;

    private ParquetDatapointWriter(ParquetWriter<Datapoint> writer, Path path) {
        this.writer = writer;
        this.path = path;
    }

    public static ParquetDatapointWriter open(Path path,
                                              ValueLayout layout,
                                              TsValue.Kind fixedKind,
                                              CompressionCodecName codec) throws IOException {
        return open(path, layout, fixedKind, codec, DEFAULT_ROW_GROUP_SIZE);
    }

    public static ParquetDatapointWriter open(Path path,
                                              ValueLayout layout,
                                              TsValue.Kind fixedKind,
                                              CompressionCodecName codec,
                                              int rowGroupSize) throws IOException {
        return open(path, layout, fixedKind, codec, rowGroupSize, DEFAULT_WRITER_VERSION);
    }

    /**
     * @param writerVersion {@code PARQUET_1_0} 은 딕셔너리 + 비트팩킹만 쓴다.
     *                      {@code PARQUET_2_0} 에서 {@code DELTA_BINARY_PACKED} 가 열리는데,
     *                      오름차순 정수(정렬된 {@code ts})에서 차이가 크다 — W5 참고.
     */
    public static ParquetDatapointWriter open(Path path,
                                              ValueLayout layout,
                                              TsValue.Kind fixedKind,
                                              CompressionCodecName codec,
                                              int rowGroupSize,
                                              ParquetProperties.WriterVersion writerVersion) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Files.deleteIfExists(path); // Parquet 은 append 를 지원하지 않는다

        MessageType schema = layout == ValueLayout.PER_KEY_TYPED
                ? ValueLayout.schemaForKind(fixedKind)
                : layout.schema();

        ParquetWriter<Datapoint> writer = new Builder(new LocalOutputFile(path), schema, layout, fixedKind)
                .withConf(new PlainParquetConfiguration())
                .withCompressionCodec(codec)
                // 기본 CodecFactory 는 Class.forName 으로 Hadoop 코덱을 찾는다 (ADR-0001)
                .withCodecFactory(new PlainCodecFactory())
                .withRowGroupSize((long) rowGroupSize)
                .withDictionaryEncoding(true)
                .withWriterVersion(writerVersion)
                .build();

        return new ParquetDatapointWriter(writer, path);
    }

    public void write(Datapoint dp) throws IOException {
        writer.write(dp);
    }

    /**
     * 지금까지 기록된 압축 후 바이트. 아직 flush 되지 않은 row group 버퍼까지 포함하므로
     * 첫 row group 이 차기 전에도 값이 늘어난다 — {@link PartitionedParquetWriter} 의 크기 롤링이
     * 이 성질에 기대고 있고, {@code PartitionedParquetWriterTest} 가 회귀로 고정한다.
     * 푸터는 아직 안 쓰였으므로 close 후 실제 파일 크기보다는 조금 작다.
     */
    public long dataSize() {
        return writer.getDataSize();
    }

    public Path path() {
        return path;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    private static final class Builder extends ParquetWriter.Builder<Datapoint, Builder> {

        private final MessageType schema;
        private final ValueLayout layout;
        private final TsValue.Kind fixedKind;

        private Builder(OutputFile file, MessageType schema, ValueLayout layout, TsValue.Kind fixedKind) {
            super(file);
            this.schema = schema;
            this.layout = layout;
            this.fixedKind = fixedKind;
        }

        @Override
        protected Builder self() {
            return this;
        }

        /** 상위 클래스가 abstract 로 강제하는 Hadoop 경로. 타지 않는다. */
        @Override
        protected WriteSupport<Datapoint> getWriteSupport(org.apache.hadoop.conf.Configuration conf) {
            throw new UnsupportedOperationException(
                    "Hadoop Configuration 경로는 지원하지 않는다. withConf(ParquetConfiguration) 을 쓸 것");
        }

        @Override
        protected WriteSupport<Datapoint> getWriteSupport(ParquetConfiguration conf) {
            return new DatapointWriteSupport(schema, layout, fixedKind);
        }
    }
}
