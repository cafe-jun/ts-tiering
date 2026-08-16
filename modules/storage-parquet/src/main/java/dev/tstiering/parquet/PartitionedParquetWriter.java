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
 * &lt;root&gt;/&lt;PartitionSpec 경로&gt;/key=&lt;키&gt;/part-&lt;writerId&gt;-&lt;n&gt;.parquet
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
     * 슬롯이 닫혀 파일이 완결된 순간. <b>이 시점이 archiver 의 두 가지 일이 걸리는 자리다</b> —
     * 오프셋 소유권을 슬롯에서 파일로 옮기고(ADR-0006 결정 1), 파일을 업로드 후보로
     * 승격한다(결정 7). 둘 다 "닫혔다"를 알아야만 할 수 있다.
     *
     * <p>여기서 던지면 {@code closeFile} 의 실패 경로를 타므로 핸들은 이미 닫힌 뒤다.
     */
    @FunctionalInterface
    public interface ClosedFileListener {
        void onClosed(String slotDir, Path file, long rows) throws IOException;

        ClosedFileListener NONE = (dir, file, rows) -> {
        };
    }

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
            org.apache.parquet.column.ParquetProperties.WriterVersion writerVersion,
            ClosePolicy closePolicy,
            ClosedFileListener closedFileListener
    ) {
        public Config {
            if (targetFileBytes <= 0) throw new IllegalArgumentException("targetFileBytes must be > 0");
            if (maxOpenWriters <= 0) throw new IllegalArgumentException("maxOpenWriters must be > 0");
            if (rowGroupSize <= 0) throw new IllegalArgumentException("rowGroupSize must be > 0");
        }

        public static Config of(Path root, PartitionSpec spec, CompressionCodecName codec) {
            return new Config(root, spec, codec,
                    DEFAULT_TARGET_FILE_BYTES, DEFAULT_MAX_OPEN_WRITERS, DEFAULT_ROW_GROUP_SIZE, false,
                    DEFAULT_WRITER_VERSION, ClosePolicy.LRU_ONLY, ClosedFileListener.NONE);
        }

        public Config withTargetFileBytes(long bytes) {
            return new Config(root, spec, codec, bytes, maxOpenWriters, rowGroupSize, sortWithinFile, writerVersion, closePolicy, closedFileListener);
        }

        public Config withMaxOpenWriters(int max) {
            return new Config(root, spec, codec, targetFileBytes, max, rowGroupSize, sortWithinFile, writerVersion, closePolicy, closedFileListener);
        }

        public Config withRowGroupSize(int size) {
            return new Config(root, spec, codec, targetFileBytes, maxOpenWriters, size, sortWithinFile, writerVersion, closePolicy, closedFileListener);
        }

        public Config withClosePolicy(ClosePolicy policy) {
            return new Config(root, spec, codec, targetFileBytes, maxOpenWriters, rowGroupSize,
                    sortWithinFile, writerVersion, policy, closedFileListener);
        }

        public Config withClosedFileListener(ClosedFileListener listener) {
            return new Config(root, spec, codec, targetFileBytes, maxOpenWriters, rowGroupSize,
                    sortWithinFile, writerVersion, closePolicy, listener);
        }

        public Config withSortWithinFile(boolean sort) {
            return new Config(root, spec, codec, targetFileBytes, maxOpenWriters, rowGroupSize, sort, writerVersion, closePolicy, closedFileListener);
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
    /**
     * @param maxOpenObserved 동시에 열려 있던 파티션의 최대치. <b>메모리의 대리 지표</b>다 —
     *                        Parquet 라이터마다 row group 버퍼를 들고 있으므로
     *                        이 값 × rowGroupSize 가 힙 상한의 기준이 된다
     */
    public record Stats(long rows, int files, long bytes, int partitions,
                        int rollovers, int evictions, int watermarkCloses, long dropped,
                        int maxOpenObserved) {

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

        /** 이 파티션에서 마지막으로 본 이벤트 시각. 워터마크 대비 뒤처지면 닫는다. */
        long lastTs = Long.MIN_VALUE;

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

    /**
     * 이 라이터 인스턴스의 식별자. 파일명에 들어간다.
     *
     * <p>{@link #nextPart} 는 인메모리라 <b>새 프로세스는 part 번호를 0부터 다시 센다.</b>
     * 파일명이 번호만으로 정해지면 재시작한 archiver 가 이전 part-0 을 덮어쓰게 되고,
     * 그건 로그도 남지 않는 유실이다. 인스턴스마다 다른 값을 섞어 그 충돌을 구조적으로 없앤다.
     * (같은 파티션에 여러 part 가 생기는 것은 정상이다 — 재개봉·롤오버가 그렇게 동작한다.)
     */
    private final String writerId = java.util.UUID.randomUUID().toString().substring(0, 8);

    private final List<Path> closedFiles = new ArrayList<>();

    /**
     * 이미 검증한 값. 키와 프로파일은 몇 개뿐인데 행마다 정규식을 돌리면 그 자체가 비용이다.
     * 실패한 값은 담지 않으므로 다음에도 같은 예외가 난다.
     */
    private final java.util.Set<String> validated = new java.util.HashSet<>();

    private long rows;
    private int rollovers;
    private int evictions;
    private int watermarkCloses;
    private long dropped;
    private int maxOpenObserved;

    /** 지금까지 본 최대 이벤트 시각. 워터마크의 정의다. */
    private long watermark = Long.MIN_VALUE;

    private boolean closed;

    public PartitionedParquetWriter(Config config) {
        this.config = config;
    }

    /**
     * @return 이 레코드가 들어간 슬롯의 키(파티션 경로). archiver 가 오프셋을 이 키에 붙이고,
     *         {@link ClosedFileListener} 가 같은 키로 소유권 이전을 알린다.
     *         정책이 이 레코드를 버렸으면 {@code null}
     */
    public String write(Datapoint dp) throws IOException {
        if (closed) throw new IllegalStateException("이미 닫힌 라이터다");

        ClosePolicy policy = config.closePolicy();
        if (policy.timeBased()) {
            // 워터마크를 먼저 올린다. 그래야 새 최대값이 스스로에게 걸려 버려지지 않는다.
            watermark = Math.max(watermark, dp.ts());
            if (policy.dropLate() && dp.ts() < watermark - policy.lagMillis()) {
                dropped++;
                return null;
            }
        }

        // 경로를 만들기 직전에 검증한다. 도메인 객체에서 막지 않는 이유는 슬래시가 든 키가
        // 텔레메트리로는 정당할 수 있기 때문이다 — 문제는 그게 객체 키가 될 때 생긴다.
        validateOnce(dp.key(), "텔레메트리 키");
        validateOnce(dp.deviceProfile(), "디바이스 프로파일");

        String dir = config.spec().path(dp) + "/key=" + dp.key();
        Slot slot = open.get(dir);

        if (slot == null) {
            slot = new Slot(dir, dp.value().kind(), config.sortWithinFile());
            openFile(slot);
            open.put(dir, slot);
            evictUntilWithinLimit();
            maxOpenObserved = Math.max(maxOpenObserved, open.size());
        } else if (slot.writer == null) {
            // 이전 닫기/롤오버가 실패해 비어 있는 슬롯. 반쯤 죽은 채로 두면 다음 write 가 NPE 를 낸다.
            openFile(slot);
        } else if (slot.kind != dp.value().kind()) {
            // PER_KEY_TYPED 는 파일 하나에 한 타입만 담는다. 섞이면 스키마와 값이 어긋난다.
            throw new IllegalStateException(
                    "키 '" + dp.key() + "' 의 값 타입이 " + slot.kind + " 에서 " + dp.value().kind()
                            + " 로 바뀌었다. PER_KEY_TYPED 는 키당 단일 타입을 전제한다");
        }

        rows++;
        slot.rowsInFile++;
        slot.lastTs = Math.max(slot.lastTs, dp.ts());

        // 워터마크 스윕은 행마다 하면 O(열린 슬롯)이 곱해진다. 간격을 두고 훑는다.
        if (config.closePolicy().timeBased() && rows % SIZE_CHECK_INTERVAL == 0) {
            closeSlotsBehindWatermark();
        }

        if (slot.buffer != null) {
            // 정렬 모드에서는 닫을 때까지 모은다. 크기를 알 수 없으므로 롤링은 걸 수 없다.
            slot.buffer.add(dp);
            return dir;
        }

        slot.writer.write(dp);

        if (slot.rowsInFile % SIZE_CHECK_INTERVAL == 0
                && slot.writer.dataSize() >= config.targetFileBytes()) {
            closeFile(slot);
            openFile(slot);
            rollovers++;
        }
        return dir;
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
        return new Stats(rows, closedFiles.size(), bytes, nextPart.size(),
                rollovers, evictions, watermarkCloses, dropped, maxOpenObserved);
    }

    /** 만들어진 파일 목록. 파일별 크기 분포를 보려면 이걸 쓴다 (small file 정량화). */
    public List<Path> files() {
        return List.copyOf(closedFiles);
    }

    /**
     * 열린 파티션을 전부 닫는다. <b>하나가 실패해도 나머지를 계속 닫는다.</b>
     *
     * <p>예전에는 {@code closed = true} 를 루프 앞에 세워서, 실패 후 재호출하면 즉시 반환해
     * 아직 안 닫힌 슬롯이 영영 남았다. {@link IOException} 만 잡은 것도 문제였다 —
     * 반쯤 죽은 슬롯이 던지는 {@code NullPointerException} 이 루프를 중단시켜
     * 그 뒤 슬롯 전부를 누출시켰다.
     */
    @Override
    public void close() throws IOException {
        if (closed) return;

        IOException first = null;
        Iterator<Map.Entry<String, Slot>> it = open.entrySet().iterator();
        while (it.hasNext()) {
            Slot slot = it.next().getValue();
            try {
                closeFile(slot);
            } catch (IOException | RuntimeException e) {
                IOException wrapped = e instanceof IOException io
                        ? io
                        : new IOException("파티션 '" + slot.dir + "' 닫기 실패", e);
                if (first == null) first = wrapped;
                else first.addSuppressed(wrapped);
            } finally {
                // closeFile 이 핸들을 닫는 것을 보장하므로 실패해도 남겨둘 이유가 없다.
                it.remove();
            }
        }

        closed = true;
        if (first != null) throw first;
    }

    // --- 내부 -----------------------------------------------------------------

    private void validateOnce(String value, String what) {
        if (!validated.contains(value)) {
            dev.tstiering.core.PartitionValues.requireValid(value, what);
            validated.add(value);
        }
    }

    private void openFile(Slot slot) throws IOException {
        int part = nextPart.merge(slot.dir, 1, Integer::sum) - 1;
        Path path = config.root().resolve(slot.dir)
                .resolve("part-" + writerId + "-" + part + ".parquet");
        slot.writer = ParquetDatapointWriter.open(
                path, ValueLayout.PER_KEY_TYPED, slot.kind, config.codec(),
                config.rowGroupSize(), config.writerVersion());
        slot.rowsInFile = 0;
    }

    /**
     * 슬롯의 파일을 닫는다. <b>어떤 경로로 실패하든 파일 핸들은 닫는다.</b>
     *
     * <p>예전에는 정렬 버퍼를 흘리다 실패하면 {@code writer.close()} 에 도달하지 못해
     * fd 가 영구히 샜다. 장기 실행 archiver 에서는 일시적 IO 오류 하나가 ulimit 소진까지
     * 누적된다. 게다가 닫히지 않은 파일은 푸터가 없어 읽을 수도 없는데
     * {@code closedFiles} 에도 안 잡혀 업로드 후보에서조차 빠진다.
     *
     * <p>{@code slot.writer} 를 먼저 떼어내는 것이 요점이다 — 실패해도 반쯤 죽은 참조가 남지 않는다.
     * 비어 있는 슬롯에 다시 쓰기가 오면 {@link #write} 가 파일을 새로 연다.
     */
    private void closeFile(Slot slot) throws IOException {
        ParquetDatapointWriter writer = slot.writer;
        if (writer == null) {
            return;   // 이전 실패로 비어 있는 슬롯. 닫을 것이 없다
        }
        slot.writer = null;
        long rowsInFile = slot.rowsInFile;

        try {
            if (slot.buffer != null) {
                slot.buffer.sort(BY_DEVICE_THEN_TS);
                for (Datapoint dp : slot.buffer) {
                    writer.write(dp);
                }
                slot.buffer = new ArrayList<>();
            }
            writer.close();
            closedFiles.add(writer.path());
            config.closedFileListener().onClosed(slot.dir, writer.path(), rowsInFile);
        } catch (IOException | RuntimeException e) {
            try {
                writer.close();
            } catch (Exception alreadyFailing) {
                e.addSuppressed(alreadyFailing);
            }
            throw e;
        }
    }

    /**
     * 워터마크보다 {@code lag} 이상 뒤처진 파티션을 닫는다.
     *
     * <p>LRU 축출과 다른 점은 <b>이유가 명시적</b>이라는 것이다. 축출은 "자리가 모자라서"이고
     * 그 유예 기간은 {@code maxOpenWriters ÷ 동시 파티션 수} 라는 우연한 값이 된다.
     * 여기서는 "이벤트 시각이 충분히 지나서"이고 그 기준이 설정값으로 드러난다.
     */
    private void closeSlotsBehindWatermark() throws IOException {
        long cutoff = watermark - config.closePolicy().lagMillis();
        Iterator<Map.Entry<String, Slot>> it = open.entrySet().iterator();
        while (it.hasNext()) {
            Slot slot = it.next().getValue();
            if (slot.lastTs < cutoff) {
                closeFile(slot);
                it.remove();
                watermarkCloses++;
            }
        }
    }

    private void evictUntilWithinLimit() throws IOException {
        while (open.size() > config.maxOpenWriters()) {
            Iterator<Map.Entry<String, Slot>> it = open.entrySet().iterator();
            Slot eldest = it.next().getValue();
            // 닫기가 성공한 뒤에 지운다. 먼저 지우면 실패한 슬롯을 아무도 다시 만지지 못한다.
            closeFile(eldest);
            it.remove();
            evictions++;
        }
    }
}
