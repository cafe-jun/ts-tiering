package dev.tstiering.bench;

import dev.tstiering.core.Datapoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * 합성 텔레메트리 생성기.
 *
 * <p>방출 순서는 <b>도착 순서</b>다 — 바깥 루프가 시각, 안쪽 루프가 디바이스.
 * 실제 Kafka 로 들어오는 순서가 이렇기 때문이고, 이 순서 그대로 쓰는 것과
 * (deviceId, ts) 로 정렬해서 쓰는 것의 압축률 차이가 W5~W6 비교 대상이다.
 * 여기서 미리 정렬해버리면 그 비교가 불가능해진다.
 */
public final class SyntheticDataGenerator {

    private final Config config;
    private final List<SensorModel> sensors;
    private final UUID[] tenantIds;
    private final UUID[][] deviceIds;

    public record Config(
            int tenants,
            int devicesPerTenant,
            String deviceProfile,
            long intervalMillis,
            long startTs
    ) {
        public Config {
            if (tenants <= 0) throw new IllegalArgumentException("tenants must be > 0");
            if (devicesPerTenant <= 0) throw new IllegalArgumentException("devicesPerTenant must be > 0");
            if (intervalMillis <= 0) throw new IllegalArgumentException("intervalMillis must be > 0");
        }
    }

    public SyntheticDataGenerator(Config config, List<SensorModel> sensors) {
        this.config = config;
        this.sensors = List.copyOf(sensors);
        this.tenantIds = new UUID[config.tenants()];
        this.deviceIds = new UUID[config.tenants()][config.devicesPerTenant()];

        // UUID 는 고정 시드로 만든다. 실행할 때마다 바뀌면 벤치마크를 재현할 수 없다.
        for (int t = 0; t < config.tenants(); t++) {
            tenantIds[t] = new UUID(0x7E4A47L, t);
            for (int d = 0; d < config.devicesPerTenant(); d++) {
                deviceIds[t][d] = new UUID(0xDE71CEL + t, d);
            }
        }
    }

    /** 디바이스별 고정 시드. SensorModel 이 무상태로 값을 낼 수 있게 하는 유일한 입력. */
    private static long deviceSeed(int tenantIdx, int deviceIdx) {
        return ((long) tenantIdx << 32) ^ deviceIdx;
    }

    /**
     * 벤치마크 쿼리가 WHERE 절에 박을 식별자. 생성기와 같은 자리에서 만들어야
     * 디바이스 수를 바꿨을 때 쿼리가 조용히 0행을 반환하는 일이 없다.
     */
    public UUID tenantId(int tenantIdx) {
        return tenantIds[tenantIdx];
    }

    public UUID deviceId(int tenantIdx, int deviceIdx) {
        return deviceIds[tenantIdx][deviceIdx];
    }

    public long pointsPerTick() {
        return (long) config.tenants() * config.devicesPerTenant() * sensors.size();
    }

    /** targetCount 에 도달할 때까지 생성한다. 마지막 틱은 중간에 잘릴 수 있다. */
    public void generate(long targetCount, Consumer<Datapoint> sink) {
        generate(targetCount, LateArrival.NONE, sink);
    }

    /**
     * 지연 도착을 섞어 방출한다.
     *
     * <p>생성되는 데이터는 {@code late} 와 무관하게 동일하고 <b>순서만 달라진다</b>.
     * 그래야 전략 비교에서 데이터 차이가 아니라 순서 차이만 남는다
     * ({@code LateArrivalTest} 가 이 성질을 고정한다).
     */
    public void generate(long targetCount, LateArrival late, Consumer<Datapoint> sink) {
        long generated = 0;
        long ts = config.startTs();
        long tick = 0;

        Random rnd = late.enabled() ? new Random(late.seed()) : null;
        // 틱 번호 → 그 틱에 풀어놓을 지연분. 순서가 안정적이어야 재현이 된다.
        Map<Long, List<Datapoint>> deferred = new HashMap<>();

        while (generated < targetCount) {
            // 이번 틱에 만기가 된 지연분을 먼저 흘린다 — 실제 컨슈머도 뒤늦게 받는다.
            if (late.enabled()) {
                List<Datapoint> due = deferred.remove(tick);
                if (due != null) due.forEach(sink);
            }

            for (int t = 0; t < config.tenants() && generated < targetCount; t++) {
                for (int d = 0; d < config.devicesPerTenant() && generated < targetCount; d++) {
                    long seed = deviceSeed(t, d);
                    for (SensorModel sensor : sensors) {
                        if (generated >= targetCount) break;
                        Datapoint dp = new Datapoint(
                                tenantIds[t],
                                config.deviceProfile(),
                                deviceIds[t][d],
                                sensor.key(),
                                ts,
                                sensor.valueAt(ts, seed)
                        );
                        generated++;

                        if (rnd != null && rnd.nextDouble() < late.ratio()) {
                            long releaseAt = tick + late.drawDelayTicks(rnd);
                            deferred.computeIfAbsent(releaseAt, k -> new ArrayList<>()).add(dp);
                        } else {
                            sink.accept(dp);
                        }
                    }
                }
            }
            ts += config.intervalMillis();
            tick++;
        }

        // 남은 지연분을 만기 순서로 흘린다. 하나라도 빠뜨리면 건수가 안 맞는다.
        deferred.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> e.getValue().forEach(sink));
    }
}
