package dev.tstiering.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 파티션 값 검증. Phase 2 의 archiver 는 Kafka 에서 임의의 키를 받으므로
 * 이 규칙이 경로 이탈과 파티션 스킴 붕괴를 막는 유일한 지점이다.
 */
class PartitionValuesTest {

    /** ThingsBoard 에서 실제로 쓰이는 형태들. 전부 통과해야 한다. */
    @ParameterizedTest
    @ValueSource(strings = {"temperature", "power_wh", "error_code", "batt.level", "rssi-dbm",
            "industrial-sensor", "A1", "9"})
    void acceptsRealisticKeys(String key) {
        assertTrue(PartitionValues.isValid(key), key);
        assertEquals(key, PartitionValues.requireValid(key, "키"));
    }

    /** 경로를 벗어나거나 파티션 파싱을 깨뜨리는 값들. */
    @ParameterizedTest
    @ValueSource(strings = {
            "../../etc/passwd",   // 경로 이탈
            "..",                 // 상위 디렉터리
            ".",                  // 현재 디렉터리
            "a/b",                // 디렉터리를 더 판다
            "a=b",                // Hive 파티션 파싱을 깨뜨린다
            "with space",         // glob 과 URL 에서 문제
            "한글키",              // 객체 키 인코딩이 갈린다
            "a*b",                // glob 메타문자
            "",                   // 빈 세그먼트
    })
    void rejectsPathBreakingValues(String value) {
        assertFalse(PartitionValues.isValid(value), value);
        assertThrows(IllegalArgumentException.class, () -> PartitionValues.requireValid(value, "키"));
    }

    @Test
    void rejectsNullAndOverlyLongValues() {
        assertFalse(PartitionValues.isValid(null));
        assertFalse(PartitionValues.isValid("a".repeat(129)));
        assertTrue(PartitionValues.isValid("a".repeat(128)));
    }

    /** 조용히 고치지 않는다 — 정규화하면 서로 다른 키가 같은 파티션에 섞인다. */
    @Test
    void errorMessageNamesTheOffendingValue() {
        var e = assertThrows(IllegalArgumentException.class,
                () -> PartitionValues.requireValid("../evil", "텔레메트리 키"));
        assertTrue(e.getMessage().contains("../evil"));
        assertTrue(e.getMessage().contains("텔레메트리 키"));
    }
}
