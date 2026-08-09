package dev.tstiering.core;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * PHASE1.md W5~W6 이 비교하려는 파티션 스킴 3종.
 *
 * <p>모두 Hive 스타일({@code key=value})이다. DuckDB 의 {@code hive_partitioning=1} 과
 * Athena/Glue 가 경로에서 파티션 열을 복원하려면 이 형식이어야 한다 — 그래야 W4 이후
 * "프루닝이 실제로 걸렸는가"를 스캔 바이트로 확인할 수 있다.
 *
 * <p><b>W3 에서 실제로 쓰는 것은 {@link #tenantProfileDateHour()} 하나뿐이다.</b>
 * 나머지 둘은 라이터가 스킴에 독립적인지 보이기 위해 함께 둔다. 셋을 실제로 돌려 비교하고
 * 승자를 고르는 것은 W5~W6 이고, 그 결과가 ADR-0003 이 된다.
 *
 * <p><b>텔레메트리 키({@code key=temperature})는 여기에 없다.</b> 그건 값 레이아웃
 * (ADR-0002 의 PER_KEY_TYPED)이 만들어내는 축이지 파티션 스킴이 정하는 축이 아니다.
 * 라이터가 이 경로 뒤에 덧붙인다.
 *
 * <p>구현체는 <b>스레드 안전하지 않다</b>. 인스턴스당 캐시를 들고 있고, 적재 파이프라인이
 * 단일 스레드라 그렇게 뒀다. 병렬 적재를 붙이면 스레드당 인스턴스를 만들 것.
 */
public final class HivePartitionSpecs {

    private HivePartitionSpecs() {
    }

    /** 스킴 A — 시간만. 디바이스 하나를 보려고 전체를 스캔하게 된다. */
    public static PartitionSpec dateHour() {
        HourCache hour = new HourCache();
        return new Spec("date-hour", dp -> hour.forTs(dp.ts()));
    }

    /** 스킴 B — 균형점 후보. W3 적재의 기준안. */
    public static PartitionSpec tenantProfileDateHour() {
        HourCache hour = new HourCache();
        Map<UUID, String> tenant = new HashMap<>();
        return new Spec("tenant-profile-date-hour", dp ->
                tenant.computeIfAbsent(dp.tenantId(), id -> "tenant=" + id)
                        + "/profile=" + dp.deviceProfile()
                        + "/" + hour.forTs(dp.ts()));
    }

    /**
     * 스킴 B 의 굵은 변형 — 시(hour) 를 뺀다.
     *
     * <p>granularity 는 스킴 선택과 <b>독립적인 축</b>이고, 데이터셋이 작을수록 이쪽이 더 크게 작용한다.
     * 파티션을 잘게 쪼갤수록 파일당 행 수가 줄어 Parquet 의 파일 고정 오버헤드(푸터, 딕셔너리 페이지,
     * 열별 메타데이터)가 데이터를 압도하기 때문이다. W2 에서 이미 조짐이 보였다 —
     * 값이 하나뿐인 {@code profile} 열은 압축하면 오히려 커졌다(14K → 18K).
     * 그 오버헤드가 파일마다 붙는다.
     */
    public static PartitionSpec tenantProfileDate() {
        DateCache date = new DateCache();
        Map<UUID, String> tenant = new HashMap<>();
        return new Spec("tenant-profile-date", dp ->
                tenant.computeIfAbsent(dp.tenantId(), id -> "tenant=" + id)
                        + "/profile=" + dp.deviceProfile()
                        + "/" + date.forTs(dp.ts()));
    }

    /**
     * 스킴 C — 디바이스별. 디렉터리가 디바이스 수 × 날짜 수로 폭발한다.
     *
     * <p>적재 쪽 함정도 여기 있다. 생성기는 ts 를 바깥 루프로 돌아 시간 파티션이 순차적으로
     * 닫히지만, 이 스킴은 같은 시각에 <b>모든 디바이스</b>의 파티션이 동시에 열려 있다.
     * 디바이스 × 키만큼 Parquet 라이터가 동시에 살아 있고 각자 row group 버퍼를 들고 있으므로,
     * 롤링 라이터의 축출 정책 없이는 이 스킴에서 먼저 죽는다.
     */
    public static PartitionSpec tenantDeviceDate() {
        DateCache date = new DateCache();
        Map<UUID, String> tenant = new HashMap<>();
        Map<UUID, String> device = new HashMap<>();
        return new Spec("tenant-device-date", dp ->
                tenant.computeIfAbsent(dp.tenantId(), id -> "tenant=" + id)
                        + "/" + device.computeIfAbsent(dp.entityId(), id -> "device=" + id)
                        + "/" + date.forTs(dp.ts()));
    }

    // --- 구현 -----------------------------------------------------------------

    private record Spec(String name, java.util.function.Function<Datapoint, String> fn)
            implements PartitionSpec {
        @Override
        public String path(Datapoint dp) {
            return fn.apply(dp);
        }
    }

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    /**
     * ts 마다 날짜를 포맷하면 1억 건에서 그 자체가 벤치마크를 왜곡한다.
     * 생성기가 ts 오름차순이므로 단일 슬롯 캐시로 사실상 100% 적중한다.
     */
    private static final class HourCache {
        private long lastEpochHour = Long.MIN_VALUE;
        private String cached;

        String forTs(long ts) {
            long epochHour = Math.floorDiv(ts, 3_600_000L);
            if (epochHour != lastEpochHour) {
                Instant at = Instant.ofEpochSecond(epochHour * 3600);
                cached = "date=" + DATE.format(at)
                        + "/hour=" + String.format("%02d", at.atZone(ZoneOffset.UTC).getHour());
                lastEpochHour = epochHour;
            }
            return cached;
        }
    }

    private static final class DateCache {
        private long lastEpochDay = Long.MIN_VALUE;
        private String cached;

        String forTs(long ts) {
            long epochDay = Math.floorDiv(ts, 86_400_000L);
            if (epochDay != lastEpochDay) {
                cached = "date=" + DATE.format(Instant.ofEpochSecond(epochDay * 86_400));
                lastEpochDay = epochDay;
            }
            return cached;
        }
    }
}
