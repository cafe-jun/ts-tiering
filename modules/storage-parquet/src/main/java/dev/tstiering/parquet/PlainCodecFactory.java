package dev.tstiering.parquet;

import com.github.luben.zstd.Zstd;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Hadoop 없이 도는 Parquet 코덱 팩토리. <b>ADR-0001 의 결론.</b>
 *
 * <p>parquet-java 1.17.1 은 아티팩트 트리에는 Hadoop 을 넣지 않지만,
 * 기본 {@code CodecFactory} 가 {@code Class.forName} 으로 Hadoop 코덱 구현을 찾는다.
 * 그 코덱들은 {@code org.apache.hadoop.io.compress.CompressionCodec} 을 구현하므로
 * 결국 hadoop-common 과 그 전이 의존성(200MB+)을 끌고 와야 한다.
 *
 * <p>그런데 압축 자체는 이미 클래스패스에 있는 zstd-jni / snappy-java / JDK 로 전부 된다.
 * Hadoop 이 하던 일은 "코덱 클래스를 설정에서 찾아 인스턴스화" 뿐이었고, 우리는 그 간접층이 필요 없다.
 * 그래서 {@link CompressionCodecFactory} 를 직접 구현해 Hadoop 을 완전히 제거한다.
 *
 * <p>바이트 포맷은 표준 그대로다 — snappy raw block, zstd frame, gzip.
 * 따라서 DuckDB/Athena/pyarrow 가 읽는 데 아무 문제가 없다 (교차 검증은 ParquetInteropTest 참고).
 */
public final class PlainCodecFactory implements CompressionCodecFactory {

    /** zstd 기본 레벨. 3 은 압축률/속도 균형점으로 zstd 자체의 기본값이기도 하다. */
    public static final int DEFAULT_ZSTD_LEVEL = 3;

    private final int zstdLevel;

    public PlainCodecFactory() {
        this(DEFAULT_ZSTD_LEVEL);
    }

    public PlainCodecFactory(int zstdLevel) {
        this.zstdLevel = zstdLevel;
    }

    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
        ByteCodec codec = codecFor(codecName);
        return new BytesInputCompressor() {
            @Override
            public BytesInput compress(BytesInput bytes) throws IOException {
                return BytesInput.from(codec.compress(bytes.toByteArray()));
            }

            @Override
            public CompressionCodecName getCodecName() {
                return codecName;
            }

            @Override
            public void release() {
            }
        };
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
        ByteCodec codec = codecFor(codecName);
        return new BytesInputDecompressor() {
            @Override
            public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
                return BytesInput.from(codec.decompress(bytes.toByteArray(), uncompressedSize));
            }

            @Override
            public void decompress(ByteBuffer input, int compressedSize,
                                   ByteBuffer output, int uncompressedSize) throws IOException {
                byte[] in = new byte[compressedSize];
                input.get(in);
                output.put(codec.decompress(in, uncompressedSize), 0, uncompressedSize);
            }

            @Override
            public void release() {
            }
        };
    }

    @Override
    public void release() {
    }

    private ByteCodec codecFor(CompressionCodecName name) {
        return switch (name) {
            case UNCOMPRESSED -> new ByteCodec() {
                @Override public byte[] compress(byte[] in) { return in; }
                @Override public byte[] decompress(byte[] in, int size) { return in; }
            };
            case SNAPPY -> new ByteCodec() {
                @Override public byte[] compress(byte[] in) throws IOException { return Snappy.compress(in); }
                @Override public byte[] decompress(byte[] in, int size) throws IOException { return Snappy.uncompress(in); }
            };
            case ZSTD -> new ByteCodec() {
                @Override public byte[] compress(byte[] in) { return Zstd.compress(in, zstdLevel); }
                @Override public byte[] decompress(byte[] in, int size) { return Zstd.decompress(in, size); }
            };
            case GZIP -> new ByteCodec() {
                @Override
                public byte[] compress(byte[] in) throws IOException {
                    var out = new ByteArrayOutputStream(Math.max(32, in.length / 4));
                    try (var gz = new GZIPOutputStream(out)) {
                        gz.write(in);
                    }
                    return out.toByteArray();
                }

                @Override
                public byte[] decompress(byte[] in, int size) throws IOException {
                    try (var gz = new GZIPInputStream(new ByteArrayInputStream(in))) {
                        return gz.readNBytes(size);
                    }
                }
            };
            default -> throw new UnsupportedOperationException(
                    name + " 은 지원하지 않는다. 필요하면 해당 압축 라이브러리를 의존성에 추가하고 여기에 분기를 더할 것");
        };
    }

    private interface ByteCodec {
        byte[] compress(byte[] in) throws IOException;

        byte[] decompress(byte[] in, int uncompressedSize) throws IOException;
    }
}
