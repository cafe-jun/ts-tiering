package dev.tstiering.archiver;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ADR-0006 의 1번 결정을 고정한다.
 *
 * <p>가장 중요한 것은 {@link #committingHighestUploadedOffsetWouldLoseData} 다 —
 * "방금 올린 파일의 최대 오프셋"을 커밋하는 흔한 실수가 실제로 유실을 만든다는 것을
 * 숫자로 보여준다. 그 구분이 무너지면 무손실이라는 전제가 통째로 무너진다.
 */
class OffsetLedgerTest {

    private static final TopicPartition TP = new TopicPartition("telemetry", 0);
    private static final TopicPartition TP1 = new TopicPartition("telemetry", 1);

    /** 아무것도 진행 중이 아니면 읽은 데까지 전부 끝난 것이다. */
    @Test
    void commitsPastEverythingWhenNothingIsPending() {
        var ledger = new OffsetLedger();
        ledger.observe(TP, 100);
        ledger.observe(TP, 101);

        assertEquals(102, ledger.committableOffset(TP).orElseThrow(),
                "Kafka 규약상 커밋값은 '다음에 읽을 위치'다");
        assertEquals(0, ledger.uncommittedSpan(TP));
    }

    @Test
    void nothingToCommitForUntouchedPartition() {
        assertTrue(new OffsetLedger().committableOffset(TP).isEmpty());
    }

    /**
     * <b>이 프로젝트에서 가장 틀리기 쉬운 지점.</b>
     *
     * <p>오프셋 10~20 이 슬롯 A 로, 30~40 이 슬롯 B 로 갔다. B 가 먼저 닫혀 S3 로 갔다.
     * "방금 올린 파일의 최대 오프셋"인 41 을 커밋하면 A 의 10~20 이 힙에 남은 채
     * 커밋되어 버린다 — 죽으면 그 구간은 Kafka·로컬·S3 어디에도 없다.
     */
    @Test
    void committingHighestUploadedOffsetWouldLoseData() {
        var ledger = new OffsetLedger();
        for (long o = 10; o <= 20; o++) ledger.track("slot-A", TP, o);
        for (long o = 30; o <= 40; o++) ledger.track("slot-B", TP, o);

        ledger.complete("slot-B");   // B 만 S3 에 도착

        assertEquals(10, ledger.committableOffset(TP).orElseThrow(),
                "B 를 올렸어도 A 가 남아 있으므로 저수위선은 10 이다");
        assertEquals(31, ledger.uncommittedSpan(TP), "재생 구간이 31건이라는 뜻");
    }

    /** A 까지 끝나야 비로소 읽은 데까지 커밋할 수 있다. */
    @Test
    void advancesOnlyAfterTheEarliestUnitCompletes() {
        var ledger = new OffsetLedger();
        for (long o = 10; o <= 20; o++) ledger.track("slot-A", TP, o);
        for (long o = 30; o <= 40; o++) ledger.track("slot-B", TP, o);

        ledger.complete("slot-B");
        assertEquals(10, ledger.committableOffset(TP).orElseThrow());

        ledger.complete("slot-A");
        assertEquals(41, ledger.committableOffset(TP).orElseThrow());
        assertEquals(0, ledger.pendingUnits());
    }

    /**
     * 슬롯이 닫혀 파일이 되고, 그 파일이 업로드되는 흐름.
     * 소유권 이전이 원자적이지 않으면 그 사이에 저수위선이 앞으로 튄다.
     */
    @Test
    void transferKeepsTheLowWaterMarkPinned() {
        var ledger = new OffsetLedger();
        for (long o = 10; o <= 20; o++) ledger.track("slot-A", TP, o);
        ledger.observe(TP, 50);

        ledger.transfer("slot-A", "file-A.parquet");

        assertEquals(10, ledger.committableOffset(TP).orElseThrow(),
                "슬롯에서 파일로 넘어가도 아직 S3 에 없으므로 저수위선은 그대로여야 한다");
        assertEquals(1, ledger.pendingUnits());

        ledger.complete("file-A.parquet");
        assertEquals(51, ledger.committableOffset(TP).orElseThrow());
    }

    /** 폐기(inflight 삭제)는 완료와 같은 효과다 — 그 오프셋은 커밋된 적이 없으므로 재생된다. */
    @Test
    void discardReleasesTheLowWaterMarkLikeCompletion() {
        var ledger = new OffsetLedger();
        for (long o = 10; o <= 20; o++) ledger.track("slot-A", TP, o);
        ledger.observe(TP, 50);

        ledger.discard("slot-A");
        assertEquals(51, ledger.committableOffset(TP).orElseThrow());
    }

    /** 파티션끼리 섞이면 안 된다 — 한쪽의 지연이 다른 쪽 커밋을 막으면 랙이 전파된다. */
    @Test
    void partitionsAreIndependent() {
        var ledger = new OffsetLedger();
        ledger.track("slot-A", TP, 10);
        ledger.observe(TP, 20);
        ledger.observe(TP1, 500);

        assertEquals(10, ledger.committableOffset(TP).orElseThrow());
        assertEquals(501, ledger.committableOffset(TP1).orElseThrow(),
                "TP 가 막혀 있어도 TP1 은 진행할 수 있어야 한다");
    }

    /** 리밸런스로 파티션을 잃으면 그 상태를 들고 있으면 안 된다. */
    @Test
    void forgetDropsPartitionState() {
        var ledger = new OffsetLedger();
        ledger.track("slot-A", TP, 10);
        ledger.forget(TP);

        assertTrue(ledger.committableOffset(TP).isEmpty());
        assertEquals(0, ledger.pendingUnits());
    }

    /** 강제로 닫을 슬롯은 오프셋이 가장 오래된 것부터 골라야 저수위선이 실제로 움직인다. */
    @Test
    void ownersAreOrderedByOldestOffsetFirst() {
        var ledger = new OffsetLedger();
        ledger.track("slot-C", TP, 300);
        ledger.track("slot-A", TP, 100);
        ledger.track("slot-B", TP, 200);

        assertEquals(java.util.List.of("slot-A", "slot-B", "slot-C"), ledger.ownersByAge(TP));
    }

    /** 같은 슬롯에 오프셋이 계속 들어와도 최소값이 유지돼야 한다. */
    @Test
    void trackKeepsTheMinimumOffsetPerOwner() {
        var ledger = new OffsetLedger();
        ledger.track("slot-A", TP, 50);
        ledger.track("slot-A", TP, 10);   // 지연 도착으로 더 이른 오프셋이 같은 슬롯에 들어옴
        ledger.track("slot-A", TP, 90);

        assertEquals(10, ledger.committableOffset(TP).orElseThrow());
    }

    /**
     * 슬롯이 담은 오프셋 구간을 닫는 순간에 읽을 수 있어야 한다 — 그 값이 푸터로 가고,
     * 재시작 복구가 그걸로 "어차피 재생되는 파일"을 가른다 (ADR-0006 결정 8).
     */
    @Test
    void offsetRangeSpansEverythingTrackedForThatOwner() {
        var ledger = new OffsetLedger();
        ledger.track("slot-A", TP, 50);
        ledger.track("slot-A", TP, 10);
        ledger.track("slot-A", TP, 90);
        ledger.track("slot-B", TP, 70);

        var range = ledger.offsetRange("slot-A").orElseThrow();
        assertEquals(10, range.minOffset());
        assertEquals(90, range.maxOffset());

        assertTrue(ledger.offsetRange("slot-없음").isEmpty());
    }

    /** 소유권이 파일로 넘어가도 구간은 그대로 따라가야 한다. */
    @Test
    void offsetRangeSurvivesTransfer() {
        var ledger = new OffsetLedger();
        ledger.track("slot-A", TP, 10);
        ledger.track("slot-A", TP, 90);
        ledger.transfer("slot-A", "ready/part-0.parquet");

        var range = ledger.offsetRange("ready/part-0.parquet").orElseThrow();
        assertEquals(10, range.minOffset());
        assertEquals(90, range.maxOffset());
    }
}
