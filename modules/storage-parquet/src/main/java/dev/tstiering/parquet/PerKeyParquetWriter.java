package dev.tstiering.parquet;

import dev.tstiering.core.Datapoint;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link ValueLayout#PER_KEY_TYPED} 용 팬아웃 라이터. 키마다 파일을 하나씩 연다.
 *
 * <p>파일 수가 키 개수만큼 늘어난다는 게 이 레이아웃의 대가다.
 * 지금은 키가 5개뿐이라 무해하지만, 키가 수백 개인 테넌트에서는 small file 문제로 직결된다.
 * ADR-0002 는 이 트레이드오프를 압축 이득과 견줘서 판단한다.
 */
public final class PerKeyParquetWriter implements Closeable {

    private final Path dir;
    private final CompressionCodecName codec;
    private final Map<String, ParquetDatapointWriter> writers = new LinkedHashMap<>();

    public PerKeyParquetWriter(Path dir, CompressionCodecName codec) {
        this.dir = dir;
        this.codec = codec;
    }

    public void write(Datapoint dp) throws IOException {
        writers.computeIfAbsent(dp.key(), key -> {
            try {
                return ParquetDatapointWriter.open(
                        dir.resolve("key=" + key + "/part-0.parquet"),
                        ValueLayout.PER_KEY_TYPED,
                        dp.value().kind(),
                        codec);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).write(dp);
    }

    public Map<String, Path> files() {
        Map<String, Path> out = new LinkedHashMap<>();
        writers.forEach((k, w) -> out.put(k, w.path()));
        return out;
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        for (ParquetDatapointWriter w : writers.values()) {
            try {
                w.close();
            } catch (IOException e) {
                if (first == null) first = e;
                else first.addSuppressed(e);
            }
        }
        if (first != null) throw first;
    }
}
