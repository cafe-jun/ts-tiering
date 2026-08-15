package dev.tstiering.core;

/**
 * 파티션 경로에 들어가는 값의 규칙.
 *
 * <p>텔레메트리 키와 프로파일명은 <b>외부에서 온다</b> — Phase 2 의 archiver 는 Kafka 에서
 * 임의의 ThingsBoard 키를 받는다. 그 값이 검증 없이 객체 키가 되면 세 가지가 깨진다.
 *
 * <ol>
 *   <li>{@code ../../x} — root 밖으로 나가 로컬 파일시스템에 쓰고, 그 경로가 그대로 S3 키가 된다</li>
 *   <li>{@code a/b} — 디렉터리를 한 단계 더 파서 파티션 깊이가 달라진다</li>
 *   <li>{@code a=b} 나 공백 — Hive 파티션 파싱과 쿼리 엔진의 glob 을 깨뜨린다</li>
 * </ol>
 *
 * <p><b>거부하고 인코딩하지 않는다.</b> 퍼센트 인코딩을 택하면 쓰기와 읽기가 같은 규칙을 써야 하는
 * 양쪽 계약이 되고, 쿼리 쪽 glob 생성까지 그 규칙을 알아야 한다. 그 복잡도를 지금 감당할 이유가 없다 —
 * ThingsBoard 의 실제 키는 거의 전부 이 화이트리스트 안에 들어온다.
 * 벗어나는 키가 실제로 관측되면 그때 격리 파티션으로 흘리는 경로를 만든다.
 *
 * <p>Phase 1 에서는 이 축이 존재하는 줄도 몰랐다. 합성 생성기가 안전한 키 5개만 썼기 때문이다.
 */
public final class PartitionValues {

    /** Hive 파티션 값으로도, 파일 경로로도, S3 객체 키로도 안전한 문자만 남긴다. */
    private static final String ALLOWED = "[A-Za-z0-9._-]+";

    /** 경로 세그먼트가 지나치게 길면 파일시스템 상한(255바이트)에 걸린다. */
    private static final int MAX_LENGTH = 128;

    private PartitionValues() {
    }

    public static boolean isValid(String value) {
        return value != null
                && !value.isEmpty()
                && value.length() <= MAX_LENGTH
                && !".".equals(value)
                && !"..".equals(value)
                && value.matches(ALLOWED);
    }

    /**
     * @throws IllegalArgumentException 경로에 넣을 수 없는 값일 때. 조용히 정규화하지 않는다 —
     *                                  정규화하면 서로 다른 키가 같은 파티션에 섞인다
     */
    public static String requireValid(String value, String what) {
        if (!isValid(value)) {
            throw new IllegalArgumentException(
                    what + " 를 파티션 경로에 넣을 수 없다: '" + value + "'"
                            + " (허용: " + ALLOWED + ", 최대 " + MAX_LENGTH + "자)");
        }
        return value;
    }
}
