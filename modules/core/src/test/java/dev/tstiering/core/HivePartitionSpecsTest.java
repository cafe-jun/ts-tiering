package dev.tstiering.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HivePartitionSpecsTest {

    private static final UUID TENANT = new UUID(0x7E4A47L, 0);
    private static final UUID DEVICE = new UUID(0xDE71CEL, 3);

    private static Datapoint at(String iso) {
        return new Datapoint(TENANT, "industrial-sensor", DEVICE, "temperature",
                Instant.parse(iso).toEpochMilli(), new TsValue.DoubleValue(20.3));
    }

    @Test
    void schemeBProducesHiveStylePath() {
        assertEquals(
                "tenant=" + TENANT + "/profile=industrial-sensor/date=2026-03-14/hour=09",
                HivePartitionSpecs.tenantProfileDateHour().path(at("2026-03-14T09:41:12Z")));
    }

    @Test
    void schemeAOmitsTenantAndSchemeCOmitsHour() {
        assertEquals("date=2026-03-14/hour=09",
                HivePartitionSpecs.dateHour().path(at("2026-03-14T09:41:12Z")));
        assertEquals("tenant=" + TENANT + "/device=" + DEVICE + "/date=2026-03-14",
                HivePartitionSpecs.tenantDeviceDate().path(at("2026-03-14T09:41:12Z")));
    }

    /**
     * 시각 캐시는 ts 오름차순을 전제로 단일 슬롯이다. 경계를 넘을 때 갱신되지 않으면
     * 한 시간치가 통째로 앞 파티션에 섞여 들어가고, 그건 프루닝 측정을 조용히 망친다.
     */
    @Test
    void hourCacheUpdatesAcrossBoundaries() {
        PartitionSpec spec = HivePartitionSpecs.dateHour();

        assertEquals("date=2026-03-14/hour=09", spec.path(at("2026-03-14T09:59:59.999Z")));
        assertEquals("date=2026-03-14/hour=10", spec.path(at("2026-03-14T10:00:00Z")));
        assertEquals("date=2026-03-15/hour=00", spec.path(at("2026-03-15T00:00:00Z")));
        // 되돌아가도 정확해야 한다 — 적재를 재시도하면 ts 가 앞으로 돌아온다
        assertEquals("date=2026-03-14/hour=10", spec.path(at("2026-03-14T10:30:00Z")));
    }

    /** 로컬 타임존이 무엇이든 파티션은 UTC 여야 한다. 아니면 재현이 기계마다 달라진다. */
    @Test
    void partitionsAreUtcNotLocalTime() {
        assertEquals("date=2026-01-01/hour=00",
                HivePartitionSpecs.dateHour().path(at("2026-01-01T00:00:00Z")));
        assertEquals("date=2025-12-31/hour=23",
                HivePartitionSpecs.dateHour().path(at("2025-12-31T23:00:00Z")));
    }

    @Test
    void namesAreStableForResultTables() {
        assertEquals("date-hour", HivePartitionSpecs.dateHour().name());
        assertEquals("tenant-profile-date-hour", HivePartitionSpecs.tenantProfileDateHour().name());
        assertEquals("tenant-device-date", HivePartitionSpecs.tenantDeviceDate().name());
    }
}
