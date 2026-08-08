package dev.tstiering.bench;

import dev.tstiering.core.Datapoint;

import java.util.List;
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

    public long pointsPerTick() {
        return (long) config.tenants() * config.devicesPerTenant() * sensors.size();
    }

    /** targetCount 에 도달할 때까지 생성한다. 마지막 틱은 중간에 잘릴 수 있다. */
    public void generate(long targetCount, Consumer<Datapoint> sink) {
        long emitted = 0;
        long ts = config.startTs();

        while (emitted < targetCount) {
            for (int t = 0; t < config.tenants() && emitted < targetCount; t++) {
                for (int d = 0; d < config.devicesPerTenant() && emitted < targetCount; d++) {
                    long seed = deviceSeed(t, d);
                    for (SensorModel sensor : sensors) {
                        if (emitted >= targetCount) break;
                        sink.accept(new Datapoint(
                                tenantIds[t],
                                config.deviceProfile(),
                                deviceIds[t][d],
                                sensor.key(),
                                ts,
                                sensor.valueAt(ts, seed)
                        ));
                        emitted++;
                    }
                }
            }
            ts += config.intervalMillis();
        }
    }
}
