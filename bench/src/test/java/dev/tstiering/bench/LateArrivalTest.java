package dev.tstiering.bench;

import dev.tstiering.core.Datapoint;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 지연 도착 주입이 <b>순서만</b> 바꾸는지 고정한다.
 *
 * <p>이게 깨지면 W1 의 전략 비교가 무의미해진다 — 전략의 차이인지 데이터의 차이인지
 * 구분할 수 없게 되기 때문이다.
 */
class LateArrivalTest {

    private static final String[] ARGS = {
            "--tenants=2", "--devices-per-tenant=3", "--interval-seconds=60"
    };
    private static final long COUNT = 30_000;

    private static List<Datapoint> emit(LateArrival late) {
        var generator = BenchArgs.parse(ARGS).generator();
        List<Datapoint> out = new ArrayList<>();
        generator.generate(COUNT, late, out::add);
        return out;
    }

    /** 건수가 하나라도 새면 유실인지 지연인지 구분할 수 없다. */
    @Test
    void emitsEveryPointExactlyOnce() {
        for (double ratio : new double[]{0.01, 0.05, 0.20}) {
            var late = new LateArrival(ratio, 10, 60, 1);
            assertEquals(COUNT, emit(late).size(), "ratio=" + ratio);
        }
    }

    /**
     * 지연 비율을 바꿔도 <b>데이터셋 자체는 동일</b>해야 한다.
     * 순서를 무시하고 비교하면 지연 없는 실행과 완전히 같아야 한다.
     */
    @Test
    void producesIdenticalDataRegardlessOfDelay() {
        Map<Datapoint, Integer> baseline = countBy(emit(LateArrival.NONE));

        for (double ratio : new double[]{0.01, 0.05, 0.20}) {
            assertEquals(baseline, countBy(emit(new LateArrival(ratio, 10, 60, 1))),
                    "ratio=" + ratio + " 에서 데이터가 달라졌다");
        }
    }

    /** 순서는 실제로 흐트러져야 한다. 안 그러면 지연을 주입한 게 아니다. */
    @Test
    void actuallyReordersTheStream() {
        List<Datapoint> ordered = emit(LateArrival.NONE);
        List<Datapoint> delayed = emit(new LateArrival(0.20, 10, 60, 1));

        assertNotEquals(ordered, delayed, "순서가 그대로면 지연이 주입되지 않은 것이다");

        // ts 가 뒤로 갔다가 되돌아오는 지점이 실제로 생겨야 한다 — 그게 재개봉을 유발한다.
        long backwards = 0;
        for (int i = 1; i < delayed.size(); i++) {
            if (delayed.get(i).ts() < delayed.get(i - 1).ts()) backwards++;
        }
        assertTrue(backwards > 0, "ts 역행이 한 번도 없다면 파티션이 다시 열릴 일도 없다");
    }

    /** 같은 시드면 같은 순서여야 전략 간 비교가 공정하다. */
    @Test
    void sameSeedGivesSameOrder() {
        assertEquals(emit(new LateArrival(0.20, 10, 60, 42)),
                emit(new LateArrival(0.20, 10, 60, 42)));
    }

    @Test
    void differentSeedGivesDifferentOrder() {
        assertNotEquals(emit(new LateArrival(0.20, 10, 60, 1)),
                emit(new LateArrival(0.20, 10, 60, 2)));
    }

    /** 상한을 넘는 지연은 없어야 버퍼가 유계다. */
    @Test
    void delayNeverExceedsConfiguredMaximum() {
        var late = new LateArrival(0.50, 5, 12, 7);
        var rnd = new java.util.Random(late.seed());
        for (int i = 0; i < 100_000; i++) {
            int d = late.drawDelayTicks(rnd);
            assertTrue(d >= 1 && d <= 12, "지연 " + d + "틱이 상한 12 를 벗어났다");
        }
    }

    @Test
    void noneMeansStrictlyAscendingTimestamps() {
        List<Datapoint> ordered = emit(LateArrival.NONE);
        for (int i = 1; i < ordered.size(); i++) {
            assertTrue(ordered.get(i).ts() >= ordered.get(i - 1).ts(),
                    "지연이 없으면 ts 는 단조 비감소여야 한다 (Phase 1 의 전제)");
        }
    }

    private static Map<Datapoint, Integer> countBy(List<Datapoint> points) {
        Map<Datapoint, Integer> counts = new HashMap<>();
        for (Datapoint dp : points) {
            counts.merge(dp, 1, Integer::sum);
        }
        return counts;
    }
}
