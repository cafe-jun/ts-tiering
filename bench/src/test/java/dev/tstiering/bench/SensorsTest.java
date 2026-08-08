package dev.tstiering.bench;

import dev.tstiering.core.TsValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 여기서 지키는 건 두 가지다.
 * 1) 무상태 — 호출 순서가 값에 영향을 주면 벤치마크를 재현할 수 없다.
 * 2) 압축 가능한 분포 — 값이 균등 난수면 W3 의 압축률 숫자가 통째로 무의미해진다.
 */
class SensorsTest {

    private static final long T0 = java.time.Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
    private static final long TICK = TimeUnit.SECONDS.toMillis(10);

    @Test
    @DisplayName("호출 순서를 뒤집어도 같은 (ts, seed) 는 같은 값을 낸다")
    void isStateless() {
        for (SensorModel sensor : Sensors.defaultProfile()) {
            List<Long> ticks = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                ticks.add(T0 + i * TICK);
            }

            List<TsValue> forward = ticks.stream().map(ts -> sensor.valueAt(ts, 42L)).toList();

            Collections.shuffle(ticks, new java.util.Random(1));
            for (long ts : ticks) {
                int idx = (int) ((ts - T0) / TICK);
                assertEquals(forward.get(idx), sensor.valueAt(ts, 42L),
                        sensor.key() + " 가 호출 순서에 의존한다");
            }
        }
    }

    @Test
    @DisplayName("온도는 인접 틱 사이에서 완만하게 변한다 (델타 인코딩 전제)")
    void temperatureIsSmooth() {
        SensorModel sensor = new Sensors.Temperature();
        double maxJump = 0;
        double prev = value(sensor, T0, 7L);
        for (int i = 1; i < 2000; i++) {
            double cur = value(sensor, T0 + i * TICK, 7L);
            maxJump = Math.max(maxJump, Math.abs(cur - prev));
            prev = cur;
        }
        assertTrue(maxJump < 1.0, "10초 간격 온도 변화가 " + maxJump + "도 — 노이즈가 과하다");
    }

    @Test
    @DisplayName("가동상태는 값이 길게 이어진다 (RLE 전제)")
    void runningStateHasLongRuns() {
        SensorModel sensor = new Sensors.RunningState();
        int flips = 0;
        boolean prev = bool(sensor, T0, 7L);
        int samples = 10_000;
        for (int i = 1; i < samples; i++) {
            boolean cur = bool(sensor, T0 + i * TICK, 7L);
            if (cur != prev) flips++;
            prev = cur;
        }
        // 10초 틱 10,000 회 = 약 28시간. 4시간 단위 런이면 전환은 한 자릿수여야 한다.
        assertTrue(flips < 20, "전환이 " + flips + "회 — 런이 너무 짧아 RLE 가 먹지 않는다");
    }

    @Test
    @DisplayName("에러코드는 대부분 OK 다 (딕셔너리 인코딩 전제)")
    void errorCodeIsSkewed() {
        SensorModel sensor = new Sensors.ErrorCode();
        int ok = 0;
        int samples = 10_000;
        for (int i = 0; i < samples; i++) {
            if ("OK".equals(((TsValue.StringValue) sensor.valueAt(T0 + i * TICK, 7L)).value())) ok++;
        }
        assertTrue(ok > samples * 0.9, "OK 비율이 " + (ok * 100.0 / samples) + "% — 분포가 너무 평평하다");
    }

    @Test
    @DisplayName("연속형 센서는 특정 값에 표본이 몰리지 않는다 (하드 클램프 회귀 방지)")
    void continuousSensorsHaveNoSpike() {
        long hour = TimeUnit.HOURS.toMillis(1);
        int samples = 20_000; // 시간 단위로 약 2.3년 — 계절을 모두 지난다

        for (SensorModel sensor : List.of(new Sensors.Temperature(), new Sensors.Humidity())) {
            var counts = new java.util.HashMap<Double, Integer>();
            for (int i = 0; i < samples; i++) {
                counts.merge(value(sensor, T0 + (long) i * hour, 7L), 1, Integer::sum);
            }
            var top = Collections.max(counts.entrySet(), java.util.Map.Entry.comparingByValue());
            double share = top.getValue() * 100.0 / samples;
            assertTrue(share < 2.0,
                    sensor.key() + " 최빈값 " + top.getKey() + " 가 " + String.format("%.1f", share)
                            + "% — 경계에 표본이 쌓이면 압축률이 실제보다 좋게 측정된다");
        }
    }

    private static double value(SensorModel s, long ts, long seed) {
        return ((TsValue.DoubleValue) s.valueAt(ts, seed)).value();
    }

    private static boolean bool(SensorModel s, long ts, long seed) {
        return ((TsValue.BoolValue) s.valueAt(ts, seed)).value();
    }
}
