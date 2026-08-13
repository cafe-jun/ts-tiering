package dev.tstiering.bench;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/**
 * ThingsBoard 의 Cassandra 텔레메트리 스키마를 그대로 옮긴 것.
 *
 * <p>압축률의 분모를 재는 것이 목적이므로 <b>충실도가 전부</b>다.
 * 컬럼 하나를 빠뜨리거나 파티션 규칙을 다르게 잡으면 그대로 틀린 숫자가 나온다.
 *
 * <p>원본과 다른 점은 하나뿐이다 — {@code entity_id} 를 {@code timeuuid} 가 아니라 {@code uuid}
 * 로 뒀다. 합성 UUID 가 버전 1이 아니라서인데, 두 타입 모두 저장은 16바이트라 크기에는 영향이 없다.
 */
public final class CassandraSchema {

    public static final String KEYSPACE = "thingsboard";
    public static final String TABLE = "ts_kv_cf";

    private CassandraSchema() {
    }

    /** RF=1 단일 노드. 복제본 수는 곱셈이므로 1로 재고 나중에 곱한다. */
    public static String createKeyspace() {
        return "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE
                + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}";
    }

    /**
     * 압축 설정은 <b>건드리지 않는다.</b> Cassandra 기본값(LZ4, 16KB 청크)이
     * 실제 운영에서 쓰이는 값이고, 여기서 바꾸면 분모가 우리에게 유리하게 왜곡된다.
     */
    public static String createTable() {
        return "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE + " ("
                + "entity_type text, "
                + "entity_id uuid, "
                + "key text, "
                + "partition bigint, "
                + "ts bigint, "
                + "bool_v boolean, "
                + "str_v text, "
                + "long_v bigint, "
                + "dbl_v double, "
                + "json_v text, "
                + "PRIMARY KEY ((entity_type, entity_id, key, partition), ts)"
                + ")";
    }

    /**
     * ThingsBoard 의 파티션 키 산출. 기본 설정 {@code TS_KV_PARTITIONING=MONTHS} 를 따른다.
     *
     * <p>파티션을 어떻게 자르느냐가 크기에 직접 영향을 준다 — 파티션이 잘면 파티션 헤더가
     * 행 수에 비해 커지고, 굵으면 그 반대다. 임의로 고르면 안 되고 TB 기본값을 따라야 한다.
     */
    public static long partitionOf(long ts) {
        return Instant.ofEpochMilli(ts)
                .atZone(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.DAYS)
                .withDayOfMonth(1)
                .toInstant()
                .toEpochMilli();
    }
}
