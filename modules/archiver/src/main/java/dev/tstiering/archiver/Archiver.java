package dev.tstiering.archiver;

import dev.tstiering.core.Datapoint;
import dev.tstiering.core.HivePartitionSpecs;
import dev.tstiering.parquet.ClosePolicy;
import dev.tstiering.parquet.PartitionedParquetWriter;
import dev.tstiering.s3.S3ObjectStore;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka → Parquet → S3.
 *
 * <p>설계는 [ADR-0006](../../../../../../../docs/adr/0006-archiver-durability.md) 에 있다.
 * 코드에서 지켜야 할 것 넷:
 *
 * <ol>
 *   <li><b>라이터는 Kafka 파티션마다 하나.</b> 워터마크가 라이터당 단일 필드라
 *       한 라이터에 여러 파티션이 섞이면 랙 편차가 그대로 오염된다 —
 *       지연 도착이 없어도 재개봉이 터진다</li>
 *   <li><b>커밋은 저수위선으로만.</b> "방금 올린 파일의 최대 오프셋"이 아니다</li>
 *   <li><b>로컬 상태는 경로로.</b> {@code inflight/} 에 쓰고 닫히면 {@code ready/} 로 옮긴다</li>
 *   <li><b>한 건이 깨져도 멈추지 않는다.</b> 파싱 실패나 검증 실패가 예외로 올라오면
 *       그 메시지가 파티션을 영구히 막는다</li>
 * </ol>
 *
 * <p>단일 스레드다. {@link PartitionedParquetWriter} 와 {@link HivePartitionSpecs} 가
 * 스레드 안전하지 않으므로 컨슈머 스레드가 전부 소유한다.
 */
public final class Archiver implements Closeable {

    /**
     * 처리 결과. W2 종료 조건의 지표가 여기서 나온다.
     *
     * @param recoveredForUpload 크래시 잔여물 중 <b>올린</b> 파일 수
     * @param discardedOnRecovery 크래시 잔여물 중 <b>버린</b> 파일 수 —
     *                            오프셋이 커밋되지 않아 어차피 재생되는 것들이다.
     *                            이 값만큼 중복이 안 생겼다는 뜻이다 (ADR-0006 결정 8)
     */
    public record Stats(long consumed, long written, long undecodable, long rejected,
                        long filesUploaded, long committed, long maxUncommittedSpan,
                        long recoveredForUpload, long discardedOnRecovery) {
    }

    /** 푸터에 박는 오프셋 좌표. 재시작 복구가 이 값으로 재생 여부를 판단한다. */
    static final String META_TOPIC = "ts-tiering.kafka.topic";
    static final String META_PARTITION = "ts-tiering.kafka.partition";
    static final String META_MIN_OFFSET = "ts-tiering.kafka.min_offset";
    static final String META_MAX_OFFSET = "ts-tiering.kafka.max_offset";

    private final ArchiverConfig config;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final LocalSpool spool;
    private final S3ObjectStore s3;
    private final String s3Prefix;
    private final ClosePolicy closePolicy;

    /** 파티션마다 라이터 하나. 워터마크 오염을 막는 유일한 방법이다 (ADR-0006 결정 5). */
    private final Map<TopicPartition, PartitionedParquetWriter> writers = new HashMap<>();

    private final OffsetLedger ledger = new OffsetLedger();

    /** 업로드 대기. 닫힌 순서를 유지해야 재현 가능하다. */
    private final Map<Path, TopicPartition> uploadQueue = new LinkedHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(true);

    private long consumed;
    private long written;
    private long undecodable;
    private long rejected;
    private long filesUploaded;
    private long committed;
    private long maxUncommittedSpan;
    private long recoveredForUpload;
    private long discardedOnRecovery;

    /** 빈 poll 이 이만큼 이어지면 더 올 것이 없다고 본다. poll 타임아웃 1초 기준 5초. */
    public static final int DEFAULT_IDLE_POLLS_BEFORE_DRAIN = 5;

    public Archiver(ArchiverConfig config, LocalSpool spool, S3ObjectStore s3,
                    String s3Prefix, ClosePolicy closePolicy) {
        this.config = config;
        this.spool = spool;
        this.s3 = s3;
        this.s3Prefix = s3Prefix;
        this.closePolicy = closePolicy;
        this.consumer = new KafkaConsumer<>(config.consumerProperties());
    }

    /**
     * 재시작 복구 후 소비를 시작한다.
     *
     * @param maxRecords 이만큼 쓰면 멈춘다. 테스트와 벤치가 끝을 정할 수 있어야 한다
     */
    public Stats run(long maxRecords) throws IOException {
        return run(maxRecords, DEFAULT_IDLE_POLLS_BEFORE_DRAIN);
    }

    /**
     * 상시 실행이 아니라 <b>끝이 있는 실행</b>을 위한 진입점.
     *
     * <p>토픽을 다 읽어도 슬롯은 열려 있다 — 닫기 창이 지나야 닫히기 때문이다.
     * 그래서 "진행 중인 단위가 없을 때 끝낸다"로는 영원히 끝나지 않는다.
     * 빈 poll 이 연속으로 이어지면 더 올 것이 없다고 보고 남은 것을 내보낸다.
     *
     * @param idlePollsBeforeDrain 이만큼 연속으로 빈 poll 이면 마무리한다
     */
    public Stats run(long maxRecords, int idlePollsBeforeDrain) throws IOException {
        recoverReadyFiles();

        consumer.subscribe(List.of(config.topic()), new RebalanceHandler());

        int idlePolls = 0;
        while (running.get() && written < maxRecords) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(config.pollTimeout());
            if (records.isEmpty()) {
                drainUploads();
                commitSafeOffsets();
                // 빈 poll 횟수로 끝을 판단하면 안 된다 — 그룹에 조인하는 동안에도 비어 있어서
                // 아무것도 안 읽고 끝나버린다(실제로 그랬다). 로그 끝에 닿았는지로 판단한다.
                if (caughtUp() && ++idlePolls >= idlePollsBeforeDrain) break;
                continue;
            }
            idlePolls = 0;

            for (ConsumerRecord<byte[], byte[]> record : records) {
                consumed++;
                handle(record);
                if (written >= maxRecords) break;
            }

            enforceUncommittedSpanLimit();
            drainUploads();
            commitSafeOffsets();
        }

        // 남은 것을 전부 내보낸다.
        closeAllWriters();
        drainUploads();
        commitSafeOffsets();

        return stats();
    }

    // --- 크래시 복구 ------------------------------------------------------------

    /**
     * 크래시 잔여물 정리. {@code inflight/} 는 버리고, {@code ready/} 는 <b>푸터를 보고 가른다</b>.
     *
     * <p>예전에는 남은 {@code ready} 를 전부 올렸다. 유실을 막는 안전한 선택이지만
     * <b>중복을 보장한다</b> — 그 파일들의 오프셋이 커밋되지 않았다면 재생으로 같은 데이터가
     * 다시 들어오기 때문이다. 실제로 {@code kill -9} 측정에서 중복 100% 가 이 경로에서 나왔다.
     *
     * <p>푸터에 오프셋 구간이 있으면 규칙이 하나로 정해진다 (ADR-0006 결정 8).
     *
     * <pre>
     * 커밋 오프셋 &gt; maxOffset  →  재생되지 않는다. 반드시 올린다
     * 커밋 오프셋 ≤ minOffset  →  통째로 재생된다. 버린다
     * 그 사이                  →  부분 겹침. 저수위선 규칙에서는 생기지 않는다 (아래 참고)
     * </pre>
     */
    private void recoverReadyFiles() throws IOException {
        List<Path> ready = spool.recover();
        if (ready.isEmpty()) return;

        List<Recovered> known = new ArrayList<>();
        List<Path> unknown = new ArrayList<>();
        for (Path file : ready) {
            Recovered r = readOffsets(file);
            if (r == null) unknown.add(file);
            else known.add(r);
        }

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = fetchCommitted(known);

        for (Recovered r : known) {
            OffsetAndMetadata at = committedOffsets.get(r.tp());

            if (at == null || at.offset() <= r.minOffset()) {
                // 커밋이 없거나 이 파일 앞이다 → 이 구간은 통째로 다시 읽힌다. 올리면 확정적 중복이다.
                spool.discard(r.file());
                discardedOnRecovery++;
                continue;
            }
            if (at.offset() <= r.maxOffset()) {
                // 커밋값은 진행 중인 단위의 minOffset 이하여야 하므로(결정 1) 여기 오면 안 된다.
                // 그래도 왔다면 유실 쪽으로 기울지 않는다 — 올리고 시끄럽게 남긴다.
                System.out.printf("  ⚠️ 복구: %s 가 커밋 오프셋 %d 를 걸친다 (파일 %d~%d). "
                                + "저수위선 규칙이 깨졌다는 뜻이다. 중복을 감수하고 올린다%n",
                        r.file().getFileName(), at.offset(), r.minOffset(), r.maxOffset());
            }
            uploadQueue.put(r.file(), r.tp());
            recoveredForUpload++;
        }

        for (Path file : unknown) {
            // 오프셋 좌표가 없으면 판단할 수 없다. 유실보다 중복이 낫다.
            uploadQueue.put(file, null);
            recoveredForUpload++;
        }

        System.out.printf("복구: ready %,d개 → 올림 %,d / 버림 %,d%s%n",
                ready.size(), recoveredForUpload, discardedOnRecovery,
                unknown.isEmpty() ? "" : "  (오프셋 좌표 없는 파일 " + unknown.size() + "개 포함)");
    }

    private record Recovered(Path file, TopicPartition tp, long minOffset, long maxOffset) {
    }

    /** 푸터에서 오프셋 좌표를 읽는다. 하나라도 없으면 {@code null} — 판단할 수 없다는 뜻이다. */
    private Recovered readOffsets(Path file) {
        Map<String, String> meta;
        try {
            meta = dev.tstiering.parquet.ParquetStats.footerMetadata(file);
        } catch (IOException | RuntimeException e) {
            return null;   // 푸터가 깨졌다. verifyReady 가 따로 잡는다
        }
        String topic = meta.get(META_TOPIC);
        String partition = meta.get(META_PARTITION);
        String min = meta.get(META_MIN_OFFSET);
        String max = meta.get(META_MAX_OFFSET);
        if (topic == null || partition == null || min == null || max == null) return null;

        try {
            return new Recovered(file, new TopicPartition(topic, Integer.parseInt(partition)),
                    Long.parseLong(min), Long.parseLong(max));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 배정 전에 커밋 오프셋을 묻는다. 코디네이터에게 직접 묻는 것이라 구독 없이도 된다.
     *
     * <p>실패하면 <b>빈 맵</b>이 아니라 예외를 던져야 할 것 같지만 그렇지 않다 —
     * 빈 맵은 "커밋 없음"으로 읽혀 전부 버리게 되고, 그건 유실이다. 실패하면 전부 올린다.
     */
    private Map<TopicPartition, OffsetAndMetadata> fetchCommitted(List<Recovered> known) {
        if (known.isEmpty()) return Map.of();
        var partitions = new java.util.HashSet<TopicPartition>();
        known.forEach(r -> partitions.add(r.tp()));
        try {
            return consumer.committed(partitions);
        } catch (RuntimeException e) {
            System.out.println("  ⚠️ 복구: 커밋 오프셋을 못 읽었다 (" + e + "). 남은 ready 를 전부 올린다");
            var all = new HashMap<TopicPartition, OffsetAndMetadata>();
            // 커밋이 파일 뒤에 있다고 보면 "올린다" 분기로 간다.
            known.forEach(r -> all.merge(r.tp(), new OffsetAndMetadata(r.maxOffset() + 1),
                    (a, b) -> a.offset() >= b.offset() ? a : b));
            return all;
        }
    }

    // --- 레코드 하나 -----------------------------------------------------------

    private void handle(ConsumerRecord<byte[], byte[]> record) throws IOException {
        var tp = new TopicPartition(record.topic(), record.partition());
        ledger.observe(tp, record.offset());

        Datapoint dp = TelemetryCodec.decode(record.value());
        if (dp == null) {
            // 깨진 메시지 하나가 파티션을 영구히 막으면 안 된다.
            undecodable++;
            return;
        }

        String slot;
        try {
            slot = writerFor(tp).write(dp);
        } catch (IllegalArgumentException e) {
            // PartitionValues 검증 실패 — 경로에 넣을 수 없는 키. 같은 이유로 멈추지 않는다.
            rejected++;
            return;
        }

        if (slot == null) {
            rejected++;   // 정책이 버렸다 (watermark-drop)
            return;
        }
        written++;
        ledger.track(slotKey(tp, slot), tp, record.offset());
    }

    /**
     * 파티션별 라이터. {@link HivePartitionSpecs} 도 인스턴스마다 새로 만든다 —
     * 내부 캐시를 들고 있어 공유하면 스레드 안전성과 별개로 상태가 섞인다.
     */
    private PartitionedParquetWriter writerFor(TopicPartition tp) {
        return writers.computeIfAbsent(tp, key -> {
            Path root = spool.inflight("kafka-" + key.partition());
            var cfg = PartitionedParquetWriter.Config
                    .of(root, HivePartitionSpecs.tenantDate(), CompressionCodecName.ZSTD)
                    .withSortWithinFile(true)
                    .withClosePolicy(closePolicy)
                    .withClosedFileListener((slotDir, file, rows) -> onFileClosed(key, slotDir, file))
                    // 닫히는 순간 오프셋 구간을 푸터에 박는다. 재시작 복구가 이걸로 판단한다.
                    .withFileMetadata(slotDir -> kafkaMetadata(key, slotDir));
            return new PartitionedParquetWriter(cfg);
        });
    }

    /**
     * 파일이 담은 Kafka 좌표. <b>{@code onFileClosed} 보다 먼저</b> 불린다 —
     * 아직 오프셋 소유권이 슬롯에 있을 때라 구간을 읽을 수 있다.
     */
    private Map<String, String> kafkaMetadata(TopicPartition tp, String slotDir) {
        var range = ledger.offsetRange(slotKey(tp, slotDir));
        if (range.isEmpty()) return Map.of();
        return Map.of(
                META_TOPIC, tp.topic(),
                META_PARTITION, Integer.toString(tp.partition()),
                META_MIN_OFFSET, Long.toString(range.get().minOffset()),
                META_MAX_OFFSET, Long.toString(range.get().maxOffset()));
    }

    /** 슬롯이 닫혔다. 오프셋 소유권을 파일로 넘기고 업로드 큐에 넣는다. */
    private void onFileClosed(TopicPartition tp, String slotDir, Path inflightFile) throws IOException {
        Path ready = spool.promote(inflightFile);
        // 소유권 이전이 원자적이어야 저수위선이 잠깐이라도 앞으로 튀지 않는다.
        ledger.transfer(slotKey(tp, slotDir), ready);
        uploadQueue.put(ready, tp);
    }

    /** 슬롯 키는 파티션마다 갈라야 한다 — 다른 파티션의 같은 경로와 섞이면 오프셋이 뒤엉킨다. */
    private static String slotKey(TopicPartition tp, String slotDir) {
        return tp.partition() + "|" + slotDir;
    }

    /**
     * 할당된 모든 파티션에서 로그 끝에 닿았는가.
     *
     * <p>배정 전에는 {@code false} 다 — 조인 중의 빈 poll 을 "더 올 것이 없다"로 착각하면
     * 한 건도 안 읽고 끝난다.
     */
    private boolean caughtUp() {
        var assignment = consumer.assignment();
        if (assignment.isEmpty()) return false;

        var end = consumer.endOffsets(assignment);
        for (TopicPartition tp : assignment) {
            if (consumer.position(tp) < end.getOrDefault(tp, 0L)) return false;
        }
        return true;
    }

    // --- 업로드와 커밋 ----------------------------------------------------------

    private void drainUploads() throws IOException {
        var it = uploadQueue.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            Path ready = entry.getKey();
            String key = s3Prefix + "/" + spool.readyRoot().relativize(ready).toString()
                    .replace(java.io.File.separatorChar, '/');

            s3.put(key, ready);
            filesUploaded++;

            // 업로드 성공 후에만 오프셋을 놓아준다 (ADR-0006 결정 1).
            ledger.complete(ready);
            spool.released(ready);
            it.remove();
        }
    }

    private void commitSafeOffsets() {
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        for (TopicPartition tp : consumer.assignment()) {
            maxUncommittedSpan = Math.max(maxUncommittedSpan, ledger.uncommittedSpan(tp));
            ledger.committableOffset(tp)
                    .ifPresent(offset -> toCommit.put(tp, new OffsetAndMetadata(offset)));
        }
        if (!toCommit.isEmpty()) {
            consumer.commitSync(toCommit);
            committed += toCommit.size();
        }
    }

    /**
     * 미커밋 오프셋이 너무 쌓이면 가장 오래된 슬롯부터 강제로 닫는다 (ADR-0006 결정 6).
     *
     * <p>파일 품질(이벤트 시각으로 모아야 잘 압축된다)과 복구 경계(오프셋 lag 이 곧 재생량)는
     * 서로 다른 축인데, ADR-0005 의 워터마크는 전자만 통제한다.
     */
    private void enforceUncommittedSpanLimit() throws IOException {
        for (TopicPartition tp : consumer.assignment()) {
            if (ledger.uncommittedSpan(tp) <= config.uncommittedSpanLimit()) continue;

            PartitionedParquetWriter writer = writers.get(tp);
            if (writer == null) continue;

            // 가장 오래된 슬롯을 닫으면 저수위선이 실제로 움직인다.
            writer.close();
            writers.remove(tp);
        }
    }

    private void closeAllWriters() throws IOException {
        IOException first = null;
        for (var entry : List.copyOf(writers.entrySet())) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                if (first == null) first = e;
                else first.addSuppressed(e);
            }
        }
        writers.clear();
        if (first != null) throw first;
    }

    public void stop() {
        running.set(false);
        consumer.wakeup();
    }

    public Stats stats() {
        return new Stats(consumed, written, undecodable, rejected,
                filesUploaded, committed, maxUncommittedSpan,
                recoveredForUpload, discardedOnRecovery);
    }

    @Override
    public void close() throws IOException {
        try {
            closeAllWriters();
        } finally {
            consumer.close();
        }
    }

    /**
     * 리밸런스. <b>회수 전에 동기로 끝내야 한다</b> — {@code commitAsync} 는 리밸런스 완료 후
     * 도착하면 버려지고, 그 사이 새 소유자가 이미 읽기 시작한다.
     */
    private final class RebalanceHandler implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
            try {
                for (TopicPartition tp : revoked) {
                    PartitionedParquetWriter writer = writers.remove(tp);
                    if (writer != null) writer.close();
                }
                drainUploads();
                commitSafeOffsets();
                revoked.forEach(ledger::forget);
            } catch (IOException e) {
                throw new UncheckedIOException("리밸런스 중 플러시 실패", e);
            }
        }

        /**
         * 시작 위치를 <b>명시적으로</b> 정한다.
         *
         * <p>{@code auto.offset.reset=none} 은 트렁케이션을 시끄럽게 만들려고 둔 것인데,
         * 그대로 두면 커밋 이력이 없는 첫 기동도 막힌다. 그래서 두 경우를 손으로 가른다.
         *
         * <ul>
         *   <li><b>커밋이 없다</b> → 첫 기동. 처음부터 읽는다. 조용히 넘어가지 않고 남긴다</li>
         *   <li><b>커밋 &lt; 로그 시작</b> → 보존이 지나 잘려나갔다. <b>거부한다.</b>
         *       여기서 처음부터 읽으면 전부 재처리하고, 끝에서 읽으면 그 구간이 조용히 사라진다.
         *       어느 쪽도 자동으로 고를 수 없다 (ADR-0006 결정 3)</li>
         * </ul>
         */
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> assigned) {
            if (assigned.isEmpty()) return;

            var committedOffsets = consumer.committed(new java.util.HashSet<>(assigned));
            var earliest = consumer.beginningOffsets(assigned);

            for (TopicPartition tp : assigned) {
                OffsetAndMetadata committedAt = committedOffsets.get(tp);
                long begin = earliest.getOrDefault(tp, 0L);

                if (committedAt == null) {
                    consumer.seekToBeginning(List.of(tp));
                    System.out.printf("  %s: 커밋 이력 없음 → 처음(%d)부터 읽는다%n", tp, begin);
                    continue;
                }
                if (committedAt.offset() < begin) {
                    throw new IllegalStateException(String.format(
                            "%s: 커밋한 오프셋 %d 가 로그 시작 %d 보다 앞선다. 보존 기간이 지나 잘려나갔고"
                                    + " %d건이 유실됐다. 보존을 닫기 창의 2배 이상으로 두어야 한다 (ADR-0006)",
                            tp, committedAt.offset(), begin, begin - committedAt.offset()));
                }
            }
        }
    }
}
