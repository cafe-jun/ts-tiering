package dev.tstiering.parquet;

import dev.tstiering.core.Datapoint;
import dev.tstiering.core.PartitionSpec;
import dev.tstiering.core.TsValue;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Datapoint 를 파티션 경로 × 텔레메트리 키로 갈라 Parquet 파일 트리로 쓴다.
 *
 * <pre>
 * &lt;root&gt;/&lt;PartitionSpec 경로&gt;/key=&lt;키&gt;/part-&lt;n&gt;.parquet
 * </pre>
 *
 * <p>{@link PerKeyParquetWriter} 는 키당 파일 하나를 끝까지 열어두는 W2 벤치마크용이다.
 * 1 년치를 적재하려면 두 가지가 더 필요하다.
 *
 * <p><b>1. 크기 기반 롤링.</b> 파일 하나가 무한정 커지면 안 된다.
 * {@code targetFileBytes} 를 넘으면 닫고 다음 part 를 연다.
 *
 * <p><b>2. 열린 라이터 수 상한.</b> Parquet 라이터는 row group 을 메모리에 모았다가 flush 하므로
 * 동시에 열린 만큼 버퍼가 잡힌다. 파티션 스킴에 따라 팬아웃이 크게 달라진다 —
 * 스킴 B(tenant/profile/date/hour)는 테넌트×키만큼(십여 개)이지만,
 * 스킴 C(tenant/device/date)는 디바이스×키만큼 동시에 열린다. 상한을 넘으면 LRU 로 닫는다.
 *
 * <p><b>축출은 공짜가 아니다.</b> Parquet 은 append 를 지원하지 않으므로, 닫힌 파티션에
 * 데이터가 다시 오면 새 part 파일이 열린다. 축출이 잦을수록 작은 파일이 늘어난다 —
 * 즉 {@code maxOpenWriters} 는 메모리와 small file 사이의 교환이고,
 * 그 교환비를 재는 것이 W5~W6 의 일이다.
 */
public final class PartitionedParquetWriter implements Closeable {

    /**
     * 단일 파일을 쓸 때(W2)는 128 MiB 였지만 여기서는 라이터가 여러 개 동시에 열린다.
     * 32 MiB × maxOpenWriters 가 힙 상한의 기준이 된다.
     */
    public static final int DEFAULT_ROW_GROUP_SIZE = 32 * 1024 * 1024;

    public static final long DEFAULT_TARGET_FILE_BYTES = 128L * 1024 * 1024;

    public static final int DEFAULT_MAX_OPEN_WRITERS = 64;

    /**
     * ADR-0004 에 따라 v2 를 기본으로 한다. parquet-java 의 기본값은 {@code PARQUET_1_0} 인데,
     * 그 버전에는 {@code DELTA_BINARY_PACKED} 가 없어 정렬된 {@code ts} 가
     * {@code PLAIN}(8바이트/행)으로 폴백한다 — 정렬의 이점을 통째로 가린다 (W5).
     */
    public static final org.apache.parquet.column.ParquetProperties.WriterVersion DEFAULT_WRITER_VERSION =
            ParquetDatapointWriter.DEFAULT_WRITER_VERSION;

    /** 행마다 dataSize() 를 부르면 그 자체가 비싸다. 이 간격으로만 확인한다. */
    private static final int SIZE_CHECK_INTERVAL = 8192;

    /**
     * @param sortWithinFile 파일 안에서 {@code (device_id, ts)} 로 정렬해서 쓴다.
     *                       켜면 파티션의 행을 전부 모았다가 닫을 때 한꺼번에 쓰므로
     *                       <b>크기 기반 롤링이 동작하지 않는다</b> (아래 참고).
     */
    public record Config(
            Path root,
            PartitionSpec spec,
            CompressionCodecName codec,
            long targetFileBytes,
            int maxOpenWriters,
            int rowGroupSize,
            boolean sortWithinFile,
            org.apache.parquet.column.ParquetProperties.WriterVersion writerVersion
    ) {
        public Config {
            if (targetFileBytes <= 0) throw new IllegalArgumentException("targetFileBytes must be > 0");
            if (maxOpenWriters <= 0) throw new IllegalArgumentException("maxOpenWriters must be > 0");
            if (rowGroupSize <= 0) throw new IllegalArgumentException("rowGroupSize must be > 0");
        }

        public static Config of(Path root, PartitionSpec spec, CompressionCodecName codec) {
            return new Config(root, spec, codec,
                    DEFAULT_TARGET_FILE_BYTES, DEFAULT_MAX_OPEN_WRITERS, DEFAULT_ROW_GROUP_SIZE, false,
                    DEFAULT_WRITER_VERSION);
        }

        public Config withTargetFileBytes(long bytes) {
            return new Config(root, spec, codec, bytes, maxOpenWriters, rowGroupSize, sortWithinFile, writerVersion);
        }

        public Config withMaxOpenWriters(int max) {
            return new Config(root, spec, codec, targetFileBytes, max, rowGroupSize, sortWithinFile, writerVersion);
        }

        public Config withRowGroupSize(int size) {
            return new Config(root, spec, codec, targetFileBytes, maxOpenWriters, size, sortWithinFile, writerVersion);
        }

        public Config withSortWithinFile(boolean sort) {
            return new Config(root, spec, codec, targetFileBytes, maxOpenWriters, rowGroupSize, sort, writerVersion);
        }
    }

    /**
     * @param rows       쓴 행 수
     * @param files      만들어진 파일 수
     * @param bytes      파일 크기 합
     * @param partitions 서로 다른 (파티션 경로 × 키) 조합의 수. 이상적인 파일 수의 하한이다
     * @param rollovers  크기 상한에 걸려 파일을 바꾼 횟수
     * @param evictions  라이터 상한에 걸려 강제로 닫은 횟수
     */
    public record Stats(long rows, int files, long bytes, int partitions, int rollovers, int evictions) {

        public double averageFileBytes() {
            return files == 0 ? 0 : bytes / (double) files;
        }

        /**
         * 축출된 파티션에 데이터가 다시 와서 새 part 를 열어야 했던 횟수.
         *
         * <p>{@code evictions} 자체는 해로운 지표가 아니다. 시간 축으로 파티션하면 생성기가
         * ts 오름차순이라 닫힌 파티션에 데이터가 다시 오지 않고, 축출이 수천 번 일어나도
         * 파일은 하나도 늘지 않는다. <b>파일을 쪼개는 것은 재개봉이지 축출이 아니다.</b>
         */
        public int reopens() {
            return files - partitions - rollovers;
        }
    }

    /**
     * 파일 안 정렬 순서. 행 그룹 통계(min/max)가 선택적이 되려면 필터 대상 열이 뭉쳐 있어야 한다.
     *
     * <p>생성기는 ts 를 바깥 루프로 돌기 때문에 기본 순서에서는 한 파일 안에 모든 디바이스가
     * 균등하게 섞인다 — 어느 행 그룹을 봐도 {@code device_id} 의 min/max 가 전체 범위라
     * 디바이스로 거르는 질의에서 행 그룹을 하나도 건너뛸 수 없다.
     */
    private static final Comparator<Datapoint> BY_DEVICE_THEN_TS =
            Comparator.comparing(Datapoint::entityId).thenComparingLong(Datapoint::ts);

    private static final class Slot {
        final String dir;
        final TsValue.Kind kind;
        ParquetDatapointWriter writer;
        long rowsInFile;

        /** {@link Config#sortWithinFile()} 일 때만 채운다. 닫을 때 정렬해서 한꺼번에 쓴다. */
        List<Datapoint> buffer;

        Slot(String dir, TsValue.Kind kind, boolean buffered) {
            this.dir = dir;
            this.kind = kind;
            this.buffer = buffered ? new ArrayList<>() : null;
        }
    }

    private final Config config;

    /** accessOrder=true 라 iterator 의 처음이 가장 오래 안 쓰인 항목이다. */
    private final Map<String, Slot> open = new LinkedHashMap<>(16, 0.75f, true);

    /** 슬롯별 다음 part 번호. 축출/롤링 후 다시 열릴 때 파일을 덮어쓰지 않게 한다. */
    private final Map<String, Integer> nextPart = new java.util.HashMap<>();

    private final List<Path> closedFiles = new ArrayList<>();

    private long rows;
    private int rollovers;
    private int evictions;
    private boolean closed;

    public PartitionedParquetWriter(Config config) {
        this.config = config;
    }

    public void write(Datapoint dp) throws IOException {
        if (closed) throw new IllegalStateException("이미 닫힌 라이터다");

        String dir = config.spec().path(dp) + "/key=" + dp.key();
        Slot slot = open.get(dir);

        if (slot == null) {
            slot = new Slot(dir, dp.value().kind(), config.sortWithinFile());
            openFile(slot);
            open.put(dir, slot);
            evictUntilWithinLimit();
        } else if (slot.kind != dp.value().kind()) {
            // PER_KEY_TYPED 는 파일 하나에 한 타입만 담는다. 섞이면 스키마와 값이 어긋난다.
            throw new IllegalStateException(
                    "키 '" + dp.key() + "' 의 값 타입이 " + slot.kind + " 에서 " + dp.value().kind()
                            + " 로 바뀌었다. PER_KEY_TYPED 는 키당 단일 타입을 전제한다");
        }

        rows++;
        slot.rowsInFile++;

        if (slot.buffer != null) {
            // 정렬 모드에서는 닫을 때까지 모은다. 크기를 알 수 없으므로 롤링은 걸 수 없다.
            slot.buffer.add(dp);
            return;
        }

        slot.writer.write(dp);

        if (slot.rowsInFile % SIZE_CHECK_INTERVAL == 0
                && slot.writer.dataSize() >= config.targetFileBytes()) {
            closeFile(slot);
            openFile(slot);
            rollovers++;
        }
    }

    public Stats stats() {
        if (!closed) throw new IllegalStateException("close() 이후에만 정확하다 — 버퍼가 아직 flush 되지 않았다");
        long bytes = 0;
        for (Path p : closedFiles) {
            try {
                bytes += Files.size(p);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
        return new Stats(rows, closedFiles.size(), bytes, nextPart.size(), rollovers, evictions);
    }

    /** 만들어진 파일 목록. 파일별 크기 분포를 보려면 이걸 쓴다 (small file 정량화). */
    public List<Path> files() {
        return List.copyOf(closedFiles);
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;

        IOException first = null;
        for (Slot slot : open.values()) {
            try {
                closeFile(slot);
            } catch (IOException e) {
                if (first == null) first = e;
                else first.addSuppressed(e);
            }
        }
        open.clear();
        if (first != null) throw first;
    }

    // --- 내부 -----------------------------------------------------------------

    private void openFile(Slot slot) throws IOException {
        int part = nextPart.merge(slot.dir, 1, Integer::sum) - 1;
        Path path = config.root().resolve(slot.dir).resolve("part-" + part + ".parquet");
        slot.writer = ParquetDatapointWriter.open(
                path, ValueLayout.PER_KEY_TYPED, slot.kind, config.codec(),
                config.rowGroupSize(), config.writerVersion());
        slot.rowsInFile = 0;
    }

    private void closeFile(Slot slot) throws IOException {
        if (slot.buffer != null) {
            slot.buffer.sort(BY_DEVICE_THEN_TS);
            for (Datapoint dp : slot.buffer) {
                slot.writer.write(dp);
            }
            slot.buffer = new ArrayList<>();
        }
        slot.writer.close();
        closedFiles.add(slot.writer.path());
        slot.writer = null;
    }

    private void evictUntilWithinLimit() throws IOException {
        while (open.size() > config.maxOpenWriters()) {
            Iterator<Map.Entry<String, Slot>> it = open.entrySet().iterator();
            Slot eldest = it.next().getValue();
            it.remove();
            closeFile(eldest);
            evictions++;
        }
    }
}
