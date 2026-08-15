package dev.tstiering.bench;

import java.util.Random;

/**
 * 지연 도착 모델. Kafka 로 들어오는 텔레메트리가 이벤트 시각 순으로 오지 않는다는 사실을 흉내낸다.
 *
 * <p><b>이벤트 시각({@code ts})은 바꾸지 않는다.</b> 늦게 도착한다는 건 값이 달라지는 게 아니라
 * 라이터에게 <b>더 나중에 건네진다</b>는 뜻이다. 그래서 {@link SyntheticDataGenerator} 는
 * 같은 데이터를 순서만 바꿔 방출한다 — 지연 비율을 바꿔도 결과 데이터셋은 동일해야 한다.
 *
 * <p>이게 Phase 2 의 첫 관문인 이유는 Phase 1 의 결과가 "ts 오름차순 도착"에 기대고 있기 때문이다.
 * 축출이 5,411회 일어나도 재개봉이 0이었던 것은 닫힌 파티션에 데이터가 다시 오지 않아서였다.
 *
 * @param ratio          지연시킬 비율 (0.0 ~ 1.0)
 * @param meanDelayTicks 평균 지연. 틱 단위이므로 실제 시간은 {@code × intervalMillis}
 * @param maxDelayTicks  지연 상한. 꼬리를 자르지 않으면 버퍼가 무한정 자란다
 * @param seed           고정 시드. 실행마다 바뀌면 전략 간 비교가 성립하지 않는다
 */
public record LateArrival(double ratio, int meanDelayTicks, int maxDelayTicks, long seed) {

    public static final LateArrival NONE = new LateArrival(0, 0, 0, 0);

    public LateArrival {
        if (ratio < 0 || ratio > 1) {
            throw new IllegalArgumentException("ratio 는 0..1 이어야 한다: " + ratio);
        }
        if (ratio > 0) {
            if (meanDelayTicks <= 0) throw new IllegalArgumentException("meanDelayTicks must be > 0");
            if (maxDelayTicks < meanDelayTicks) {
                throw new IllegalArgumentException("maxDelayTicks 가 meanDelayTicks 보다 작다");
            }
        }
    }

    public boolean enabled() {
        return ratio > 0;
    }

    /** {@code --late-ratio=0.05 --late-mean-ticks=10 --late-max-ticks=60} */
    public static LateArrival from(BenchArgs opts) {
        double ratio = Double.parseDouble(opts.string("late-ratio", "0"));
        if (ratio <= 0) return NONE;
        int mean = (int) opts.number("late-mean-ticks", 10);
        int max = (int) opts.number("late-max-ticks", Math.max(mean * 6L, mean));
        return new LateArrival(ratio, mean, max, opts.number("late-seed", 20260813));
    }

    /**
     * 지연 길이를 뽑는다. 지수분포를 상한에서 자른다 — 실제 지연은 대부분 짧고 꼬리가 길다.
     * 균등분포로 하면 "아주 늦게 오는 소수"라는 성질이 사라져 전략 비교가 무의미해진다.
     */
    int drawDelayTicks(Random rnd) {
        double u = rnd.nextDouble();
        int ticks = 1 + (int) (-meanDelayTicks * Math.log(1 - u));
        return Math.min(ticks, maxDelayTicks);
    }

    public String describe() {
        return enabled()
                ? String.format("비율 %.0f%%, 평균 %d틱 지연 (상한 %d틱)", ratio * 100, meanDelayTicks, maxDelayTicks)
                : "없음 (ts 오름차순)";
    }
}
