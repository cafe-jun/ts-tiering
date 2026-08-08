package dev.tstiering.core;

import java.util.Objects;
import java.util.UUID;

/**
 * 시계열 한 점. hot/cold 어느 계층에 있든 이 형태로 다룬다.
 *
 * @param tenantId      테넌트 (파티션 최상위 후보)
 * @param deviceProfile 디바이스 프로파일명. 같은 프로파일은 키 집합이 동일하다 — 파티션 키 후보
 * @param entityId      디바이스 식별자
 * @param key           텔레메트리 키 (temperature, humidity, ...)
 * @param ts            epoch millis
 * @param value         값
 */
public record Datapoint(
        UUID tenantId,
        String deviceProfile,
        UUID entityId,
        String key,
        long ts,
        TsValue value
) {
    public Datapoint {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(deviceProfile, "deviceProfile");
        Objects.requireNonNull(entityId, "entityId");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (ts < 0) {
            throw new IllegalArgumentException("ts must be >= 0, got " + ts);
        }
    }
}
