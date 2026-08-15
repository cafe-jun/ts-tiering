package dev.tstiering.parquet;

import java.time.Duration;

/**
 * 파티션을 언제 닫을 것인가.
 *
 * <p>Phase 1 의 라이터에는 이 개념이 없었다. 파티션은 LRU 축출·크기 롤링·최종 close 로만 닫혔고,
 * 그래서 {@code maxOpenWriters ÷ 일당 파티션 수} 가 <b>우연한 유예 기간</b>으로 작동했다.
 * 합성 생성기가 ts 오름차순이라 닫힌 파티션에 데이터가 다시 올 일이 없어서 드러나지 않았을 뿐이다.
 *
 * <p>Kafka 에서는 지연 도착이 생긴다. 30일치에 지연 5%(평균 3일)를 주면 재개봉이 20만 번 일어나
 * 파일이 450개에서 203,449개로 늘고 Parquet 의 압축 이점이 사라진다.
 *
 * <p><b>워터마크는 이벤트 시각 기준이다.</b> 파티션의 시간 범위를 알 필요가 없다 —
 * {@link dev.tstiering.core.PartitionSpec} 은 경로 문자열만 주므로 그걸 파싱하면 스킴에 묶인다.
 * 대신 슬롯마다 마지막으로 본 ts 를 기억하고, 전역 최대 ts 보다 {@code lag} 이상 뒤처진 슬롯을 닫는다.
 *
 * @param name      결과표에 찍히는 이름
 * @param lagMillis 워터마크 뒤로 이만큼은 열어둔다. {@link Long#MAX_VALUE} 면 시간 기준으로 닫지 않는다
 * @param dropLate  워터마크를 벗어난 늦은 데이터를 버릴지. {@code false} 면 파티션을 다시 연다
 */
public record ClosePolicy(String name, long lagMillis, boolean dropLate) {

    /** Phase 1 의 동작. 시간으로는 닫지 않고 LRU 축출에만 맡긴다. 비교 기준선으로만 쓴다. */
    public static final ClosePolicy LRU_ONLY = new ClosePolicy("lru", Long.MAX_VALUE, false);

    /**
     * ADR-0005 의 기본값. 디바이스가 일주일 오프라인이었다가 일괄 전송하는 경우까지 덮는다.
     *
     * <p>슬롯 수는 이 창에 맞춰 잡아야 한다 — {@code 창(일) × 일당 파티션 수 × 1.2}.
     * 창이 지연 상한을 덮으면 지연 비율은 무관하고, 못 덮으면 비율만큼 재개봉이 터진다.
     */
    public static final ClosePolicy DEFAULT = watermarkClose(Duration.ofDays(7));

    public ClosePolicy {
        if (lagMillis <= 0) throw new IllegalArgumentException("lagMillis must be > 0");
        if (lagMillis == Long.MAX_VALUE && dropLate) {
            throw new IllegalArgumentException("시간 기준으로 닫지 않으면서 늦은 데이터를 버릴 수는 없다");
        }
    }

    /** 워터마크로 닫되 늦은 데이터가 오면 파티션을 다시 연다 — 유실은 없고 파일이 쪼개진다. */
    public static ClosePolicy watermarkClose(Duration lag) {
        return new ClosePolicy("watermark-close(" + human(lag) + ")", lag.toMillis(), false);
    }

    /** 워터마크로 닫고 늦은 데이터는 버린다 — 파일 수는 지켜지고 유실이 생긴다. */
    public static ClosePolicy watermarkDrop(Duration lag) {
        return new ClosePolicy("watermark-drop(" + human(lag) + ")", lag.toMillis(), true);
    }

    /** 시간 기준으로 닫는가. */
    public boolean timeBased() {
        return lagMillis != Long.MAX_VALUE;
    }

    private static String human(Duration d) {
        long days = d.toDays();
        if (days > 0) return days + "d";
        long hours = d.toHours();
        if (hours > 0) return hours + "h";
        return d.toMinutes() + "m";
    }
}
