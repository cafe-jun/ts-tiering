package dev.tstiering.archiver;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

/**
 * 커밋해도 안전한 오프셋을 계산한다. <b>ADR-0006 의 1번 결정이 여기 있다.</b>
 *
 * <p>Kafka 는 파티션당 오프셋 스칼라 하나만 커밋한다 — 표현할 수 있는 것은
 * "이 오프셋 앞은 전부 끝났다"는 프리픽스뿐이다. 그런데 이 파이프라인의 내구성 경계는 희소하다.
 * 정렬 모드에서 행은 파티션이 닫힐 때까지 힙에 있고, 파티션은 이벤트 시각으로 닫힌다.
 * 한 poll 배치는 여러 날짜 슬롯으로 흩어지고 각각 며칠 간격으로 따로 닫힌다.
 *
 * <p><b>그래서 "방금 S3 에 올린 파일의 최대 오프셋"을 커밋하면 유실이 난다.</b>
 * 그 파일보다 오프셋이 작은 레코드가 아직 다른 슬롯의 힙 버퍼에 남아 있을 수 있기 때문이다.
 * 커밋해도 되는 값은 <b>아직 끝나지 않은 것들의 최소값</b> — 저수위선이다.
 *
 * <p>이 클래스는 오프셋 집합 전체를 들지 않는다. 진행 중인 단위(슬롯 또는 업로드 대기 파일)마다
 * 그 단위가 담은 <b>최소 오프셋 하나</b>만 기억하면 충분하다.
 *
 * <p>스레드 안전하지 않다. 컨슈머 스레드 하나가 소유한다.
 */
public final class OffsetLedger {

    /** 진행 중인 작업 단위. 슬롯이든 업로드 대기 파일이든 같은 취급을 받는다. */
    private record Pending(long minOffset, long maxOffset) {
    }

    /** 파티션별 미완료 단위. 값이 최소 오프셋이므로 정렬해두면 저수위선이 first 다. */
    private final Map<TopicPartition, Map<Object, Pending>> pending = new HashMap<>();

    /** 파티션별로 마지막으로 읽은 오프셋. 미완료가 하나도 없으면 여기 +1 이 커밋값이다. */
    private final Map<TopicPartition, Long> lastPolled = new HashMap<>();

    /** poll 로 받은 레코드를 기록한다. 아직 어느 단위에도 속하지 않은 상태다. */
    public void observe(TopicPartition tp, long offset) {
        lastPolled.merge(tp, offset, Math::max);
    }

    /**
     * 진행 중인 단위를 등록하거나 갱신한다.
     *
     * @param owner 단위의 식별자. 슬롯 경로나 파일 경로처럼 안정적인 값이어야 한다
     */
    public void track(Object owner, TopicPartition tp, long offset) {
        pending.computeIfAbsent(tp, k -> new LinkedHashMap<>())
                .merge(owner, new Pending(offset, offset),
                        (a, b) -> new Pending(Math.min(a.minOffset(), b.minOffset()),
                                Math.max(a.maxOffset(), b.maxOffset())));
        observe(tp, offset);
    }

    /**
     * 단위의 소유권을 넘긴다. 슬롯이 닫혀 파일이 되고, 그 파일이 업로드 큐로 가는 흐름을 표현한다.
     *
     * <p>이 연산이 <b>원자적이어야</b> 저수위선이 잠깐이라도 앞으로 튀지 않는다.
     * 떼었다가 다시 붙이면 그 사이에 커밋이 일어나 아직 안 끝난 데이터를 커밋할 수 있다.
     */
    public void transfer(Object from, Object to) {
        for (Map<Object, Pending> byOwner : pending.values()) {
            Pending p = byOwner.remove(from);
            if (p != null) {
                byOwner.merge(to, p, (a, b) -> new Pending(
                        Math.min(a.minOffset(), b.minOffset()),
                        Math.max(a.maxOffset(), b.maxOffset())));
            }
        }
    }

    /** 단위가 S3 까지 갔다. 이제 저수위선을 밀 수 있다. */
    public void complete(Object owner) {
        pending.values().forEach(byOwner -> byOwner.remove(owner));
    }

    /** 폐기된 단위(inflight 삭제 등). 오프셋은 커밋되지 않았으므로 재생으로 복구된다. */
    public void discard(Object owner) {
        complete(owner);
    }

    /**
     * 커밋해도 안전한 오프셋. <b>Kafka 규약대로 "다음에 읽을 위치"</b>다.
     *
     * @return 커밋할 것이 없으면 비어 있다
     */
    public Optional<Long> committableOffset(TopicPartition tp) {
        Map<Object, Pending> byOwner = pending.get(tp);

        if (byOwner != null && !byOwner.isEmpty()) {
            // 미완료가 있으면 그중 가장 이른 것 앞까지만 안전하다.
            long lowest = byOwner.values().stream().mapToLong(Pending::minOffset).min().orElseThrow();
            return Optional.of(lowest);
        }

        // 미완료가 없으면 읽은 데까지 전부 끝난 것이다.
        Long polled = lastPolled.get(tp);
        return polled == null ? Optional.empty() : Optional.of(polled + 1);
    }

    /** 재생해야 할 구간의 크기. 창 크기의 대가를 드러내는 지표다 (ADR-0006). */
    public long uncommittedSpan(TopicPartition tp) {
        Long polled = lastPolled.get(tp);
        if (polled == null) return 0;
        return committableOffset(tp).map(c -> polled + 1 - c).orElse(0L);
    }

    /** 파티션을 잃었다(리밸런스). 그 파티션의 진행 상태를 전부 버린다. */
    public void forget(TopicPartition tp) {
        pending.remove(tp);
        lastPolled.remove(tp);
    }

    /** 진행 중인 단위 수. 0이면 모든 데이터가 S3 에 갔다는 뜻이다. */
    public int pendingUnits() {
        return pending.values().stream().mapToInt(Map::size).sum();
    }

    /** 오프셋 span 이 큰 순서. 강제로 닫을 슬롯을 고르는 데 쓴다 (ADR-0006 결정 6). */
    public java.util.List<Object> ownersByAge(TopicPartition tp) {
        Map<Object, Pending> byOwner = pending.get(tp);
        if (byOwner == null) return java.util.List.of();
        var sorted = new TreeSet<Map.Entry<Object, Pending>>(
                java.util.Comparator.comparingLong(e -> e.getValue().minOffset()));
        sorted.addAll(byOwner.entrySet());
        return sorted.stream().map(Map.Entry::getKey).toList();
    }
}
