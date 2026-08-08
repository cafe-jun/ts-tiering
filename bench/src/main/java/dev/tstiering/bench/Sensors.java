package dev.tstiering.bench;

import dev.tstiering.core.TsValue;

import java.util.List;
import java.util.concurrent.TimeUnit;

/** 산업용 IoT 에서 흔한 다섯 가지 키 패턴. 압축 특성이 서로 다르도록 골랐다. */
public final class Sensors {

    private Sensors() {
    }

    public static List<SensorModel> defaultProfile() {
        return List.of(
                new Temperature(),
                new Humidity(),
                new PowerWh(),
                new RunningState(),
                new ErrorCode()
        );
    }

    // --- 결정적 해시 노이즈 -------------------------------------------------
    // java.util.Random 을 쓰면 호출 순서에 값이 의존해 무상태 조건이 깨진다.

    private static long mix(long z) {
        z += 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /** [0, 1) 균등. (a, b) 에만 의존한다. */
    static double unit(long a, long b) {
        return (mix(a ^ mix(b)) >>> 11) * 0x1.0p-53;
    }

    /** [-1, 1) */
    static double noise(long a, long b) {
        return unit(a, b) * 2.0 - 1.0;
    }

    private static double hourOfDay(long ts) {
        return (ts % TimeUnit.DAYS.toMillis(1)) / (double) TimeUnit.HOURS.toMillis(1);
    }

    private static double dayOfYear(long ts) {
        return (ts / (double) TimeUnit.DAYS.toMillis(1)) % 365.0;
    }

    private static double round1(double v) {
        return Math.round(v * 10.0) / 10.0;
    }

    /**
     * 경계에 점근적으로 다가가는 클램프. 가운데에서는 항등함수에 가깝다.
     *
     * <p>{@code Math.min/max} 로 자르면 경계값 하나에 표본이 쌓여 그 값이 단일 최빈값이 된다.
     * 실제 센서에는 없는 스파이크이고, 딕셔너리/RLE 가 그 값을 공짜로 먹어서
     * 압축률이 실제보다 좋게 측정된다.
     */
    private static double softClamp(double v, double lo, double hi) {
        double mid = (lo + hi) / 2.0;
        double half = (hi - lo) / 2.0;
        return mid + half * Math.tanh((v - mid) / half);
    }

    // --- 모델 ---------------------------------------------------------------

    /** 일주기 + 계절성 + 디바이스별 오프셋. 소수 1자리. 델타 인코딩이 잘 먹는 전형. */
    static final class Temperature implements SensorModel {
        @Override public String key() { return "temperature"; }

        @Override public TsValue valueAt(long ts, long deviceSeed) {
            double offset = noise(deviceSeed, 0x7E11) * 5.0;
            double daily = 6.0 * Math.sin(2 * Math.PI * (hourOfDay(ts) - 9.0) / 24.0);
            double seasonal = 8.0 * Math.sin(2 * Math.PI * (dayOfYear(ts) - 100.0) / 365.0);
            // 센서 노이즈는 틱 단위로 잘게 흔들리되 진폭이 작다
            double jitter = noise(ts, deviceSeed) * 0.3;
            return new TsValue.DoubleValue(round1(18.0 + offset + daily + seasonal + jitter));
        }
    }

    /**
     * 온도와 음의 상관. 다만 온도의 완전한 함수는 아니다 —
     * 독립적인 일주기 성분을 섞어야 두 컬럼의 상관이 현실적인 수준으로 느슨해진다.
     */
    static final class Humidity implements SensorModel {
        private final Temperature temp = new Temperature();

        @Override public String key() { return "humidity"; }

        @Override public TsValue valueAt(long ts, long deviceSeed) {
            double t = ((TsValue.DoubleValue) temp.valueAt(ts, deviceSeed)).value();
            double coupled = 70.0 - (t - 18.0) * 1.2;
            double independent = 8.0 * Math.sin(2 * Math.PI * (hourOfDay(ts) - 3.0) / 24.0);
            double jitter = noise(ts, deviceSeed ^ 0x5EED) * 2.0;
            return new TsValue.DoubleValue(round1(softClamp(coupled + independent + jitter, 20.0, 95.0)));
        }
    }

    /** 업무시간 패턴 + 드문 스파이크. 정수라 비트팩킹이 먹는다. */
    static final class PowerWh implements SensorModel {
        @Override public String key() { return "power_wh"; }

        @Override public TsValue valueAt(long ts, long deviceSeed) {
            double h = hourOfDay(ts);
            double duty = (h >= 8 && h < 20) ? 1.0 : 0.25;
            double base = 400.0 * duty * (0.8 + unit(deviceSeed, 0x9051) * 0.4);
            double spike = unit(ts, deviceSeed ^ 0x5A1CE) < 0.002 ? 1500.0 : 0.0;
            return new TsValue.LongValue(Math.round(base + spike + noise(ts, deviceSeed) * 20.0));
        }
    }

    /** 평균 4시간 단위로만 바뀐다. RLE 가 극단적으로 잘 먹는 케이스. */
    static final class RunningState implements SensorModel {
        private static final long RUN_MILLIS = TimeUnit.HOURS.toMillis(4);

        @Override public String key() { return "running"; }

        @Override public TsValue valueAt(long ts, long deviceSeed) {
            long runIndex = ts / RUN_MILLIS;
            // 가동률 85% — 대부분 true 가 길게 이어진다
            return new TsValue.BoolValue(unit(runIndex, deviceSeed) < 0.85);
        }
    }

    /** 카디널리티 5. 98% 가 "OK". 딕셔너리 인코딩이 먹는 케이스. */
    static final class ErrorCode implements SensorModel {
        private static final String[] CODES = {"E_OVERHEAT", "E_COMM_LOST", "E_SENSOR_DRIFT", "E_POWER_DIP"};
        private static final long RUN_MILLIS = TimeUnit.MINUTES.toMillis(30);

        @Override public String key() { return "error_code"; }

        @Override public TsValue valueAt(long ts, long deviceSeed) {
            long runIndex = ts / RUN_MILLIS;
            double u = unit(runIndex, deviceSeed ^ 0xE770);
            if (u >= 0.02) {
                return new TsValue.StringValue("OK");
            }
            return new TsValue.StringValue(CODES[(int) (u * 1000) % CODES.length]);
        }
    }
}
