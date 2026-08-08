package dev.tstiering.core;

import java.time.Duration;
import java.time.Instant;

/**
 * 반열린 시간 구간 [from, to). epoch millis.
 *
 * <p>반열린 구간을 쓰는 이유: hot/cold 경계에서 같은 ts 가 양쪽에 중복으로 잡히는 것을 막는다.
 * 닫힌 구간으로 두면 경계값 하나가 두 번 조회되고, 이게 Phase 2 라우터에서 중복 원인이 된다.
 */
public record TimeRange(long fromInclusive, long toExclusive) {

    public TimeRange {
        if (fromInclusive < 0) {
            throw new IllegalArgumentException("fromInclusive must be >= 0, got " + fromInclusive);
        }
        if (toExclusive <= fromInclusive) {
            throw new IllegalArgumentException(
                    "toExclusive must be > fromInclusive, got [" + fromInclusive + ", " + toExclusive + ")");
        }
    }

    public static TimeRange of(Instant from, Instant to) {
        return new TimeRange(from.toEpochMilli(), to.toEpochMilli());
    }

    public boolean contains(long ts) {
        return ts >= fromInclusive && ts < toExclusive;
    }

    public Duration duration() {
        return Duration.ofMillis(toExclusive - fromInclusive);
    }
}
