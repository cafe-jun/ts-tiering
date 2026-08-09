package dev.tstiering.bench;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** {@code --key=value} 만 받는다. 벤치마크 진입점들이 공유한다. */
public final class BenchArgs {

    /** 고정 시작 시각. 실행할 때마다 바뀌면 벤치마크를 재현할 수 없다. */
    private static final Instant DEFAULT_START = Instant.parse("2026-01-01T00:00:00Z");

    private final Map<String, String> opts;

    private BenchArgs(Map<String, String> opts) {
        this.opts = opts;
    }

    public static BenchArgs parse(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (String arg : args) {
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("알 수 없는 인자: " + arg);
            }
            int eq = arg.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("--key=value 형식이어야 한다: " + arg);
            }
            opts.put(arg.substring(2, eq), arg.substring(eq + 1));
        }
        return new BenchArgs(opts);
    }

    public String string(String key, String fallback) {
        return opts.getOrDefault(key, fallback);
    }

    /** {@code --count=10_000_000} 처럼 읽기 좋게 쓴 숫자를 허용한다. */
    public long number(String key, long fallback) {
        String v = opts.get(key);
        return v == null ? fallback : Long.parseLong(v.replace("_", ""));
    }

    public int tenants() {
        return (int) number("tenants", 3);
    }

    public int devicesPerTenant() {
        return (int) number("devices-per-tenant", 200);
    }

    public long intervalMillis() {
        return number("interval-seconds", 10) * 1000L;
    }

    public long startTs() {
        String v = opts.get("start");
        return v == null ? DEFAULT_START.toEpochMilli() : Instant.parse(v).toEpochMilli();
    }

    /**
     * {@code --days=365} 를 주면 {@code --count} 대신 <b>커버 기간</b>으로 건수를 정한다.
     *
     * <p>W4 쿼리 Q2(1년 일평균)/Q3(1개월 평균)를 재려면 커버 기간이 정확해야 하는데,
     * 건수로 지정하면 디바이스 수나 주기를 건드릴 때마다 커버 기간이 조용히 어긋난다.
     */
    public long days() {
        return number("days", 0);
    }

    /** {@code --days} 가 있으면 그쪽이 이긴다. 없으면 {@code --count}. */
    public long countFor(SyntheticDataGenerator generator) {
        long days = days();
        if (days <= 0) {
            return number("count", 1_000_000);
        }
        long ticks = days * 86_400_000L / intervalMillis();
        return ticks * generator.pointsPerTick();
    }

    public SyntheticDataGenerator generator() {
        var config = new SyntheticDataGenerator.Config(
                tenants(), devicesPerTenant(), "industrial-sensor", intervalMillis(), startTs());
        return new SyntheticDataGenerator(config, Sensors.defaultProfile());
    }

    public static String humanDuration(Duration d) {
        long days = d.toDays();
        if (days > 0) return days + "일";
        long hours = d.toHours();
        if (hours > 0) return hours + "시간";
        return d.toMinutes() + "분";
    }
}
