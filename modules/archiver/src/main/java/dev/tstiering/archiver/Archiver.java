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

    /** 처리 결과. W2 종료 조건의 지표가 여기서 나온다. */
    public record Stats(long consumed, long written, long undecodable, long rejected,
                        long filesUploaded, long committed, long maxUncommittedSpan) {
    }

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
        // 1. 크래시 잔여물 정리 — inflight 는 버리고 ready 는 업로드 큐로 되살린다.
        for (Path ready : spool.recover()) {
            uploadQueue.put(ready, null);   // 파티션을 모르므로 오프셋 추적 대상이 아니다
        }

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
                    .withClosedFileListener((slotDir, file, rows) -> onFileClosed(key, slotDir, file));
            return new PartitionedParquetWriter(cfg);
        });
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
                filesUploaded, committed, maxUncommittedSpan);
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
