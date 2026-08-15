package dev.tstiering.archiver;

import dev.tstiering.core.Datapoint;
import dev.tstiering.core.TsValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 왕복이 값을 보존해야 archiver 가 흘린 것과 Phase 1 이 잰 것이 같은 데이터가 된다.
 *
 * <p>깨진 메시지에 예외를 던지지 않는 것도 함께 고정한다 — archiver 에서 한 건이
 * 예외를 던지면 그 메시지가 파티션 전체를 영구히 막는다.
 */
class TelemetryCodecTest {

    private static final UUID TENANT = new UUID(0x7E4A47L, 0);
    private static final UUID DEVICE = new UUID(0xDE71CEL, 0);
    private static final long TS = 1767225600000L;

    private static Datapoint dp(TsValue value) {
        return new Datapoint(TENANT, "industrial-sensor", DEVICE, "temperature", TS, value);
    }

    private static void roundTrip(TsValue value) {
        Datapoint original = dp(value);
        Datapoint decoded = TelemetryCodec.decode(TelemetryCodec.encode(original));
        assertEquals(original, decoded, TelemetryCodec.toJson(original));
    }

    @Test
    void roundTripsEveryValueKind() {
        roundTrip(new TsValue.DoubleValue(20.3));
        roundTrip(new TsValue.LongValue(1234));
        roundTrip(new TsValue.BoolValue(true));
        roundTrip(new TsValue.BoolValue(false));
        roundTrip(new TsValue.StringValue("OK"));
        roundTrip(new TsValue.JsonValue("{\"a\":1}"));
    }

    /** 정수로 보이는 double 이 LONG 으로 돌아오면 Parquet 스키마가 갈린다 (ADR-0002). */
    @Test
    void preservesDoubleThatLooksLikeInteger() {
        Datapoint decoded = TelemetryCodec.decode(TelemetryCodec.encode(dp(new TsValue.DoubleValue(20.0))));
        assertEquals(TsValue.Kind.DOUBLE, decoded.value().kind());
    }

    /** 한 건이 깨졌다고 컨슈머가 멈추면 그 메시지가 파티션을 영구히 막는다. */
    @ParameterizedTest
    @ValueSource(strings = {
            "",                                        // 빈 메시지
            "not json",                                // 파싱 불가
            "{}",                                      // 필드 없음
            "{\"tenantId\":\"not-a-uuid\"}",           // UUID 형식 오류
            "[1,2,3]",                                 // 객체가 아님
            "{\"tenantId\":\"00000000-007e-4a47-0000-000000000000\"}",   // 필드 누락
    })
    void returnsNullForBrokenMessagesInsteadOfThrowing(String payload) {
        assertNull(TelemetryCodec.decode(payload.getBytes()));
    }

    @Test
    void returnsNullForNullAndEmptyPayload() {
        assertNull(TelemetryCodec.decode(null));
        assertNull(TelemetryCodec.decode(new byte[0]));
    }

    /** type 과 실제 값이 어긋나면 조용히 캐스팅하지 않는다 — 스키마가 오염된다. */
    @Test
    void rejectsTypeMismatch() {
        String json = "{\"tenantId\":\"" + TENANT + "\",\"profile\":\"p\",\"deviceId\":\"" + DEVICE
                + "\",\"key\":\"temperature\",\"ts\":" + TS + ",\"type\":\"BOOL\",\"value\":20.3}";
        assertNull(TelemetryCodec.decode(json.getBytes()));
    }

    /** 음수 ts 는 Datapoint 가 거부한다. 그 예외가 밖으로 새면 안 된다. */
    @Test
    void rejectsNegativeTimestampWithoutThrowing() {
        String json = "{\"tenantId\":\"" + TENANT + "\",\"profile\":\"p\",\"deviceId\":\"" + DEVICE
                + "\",\"key\":\"temperature\",\"ts\":-1,\"type\":\"DOUBLE\",\"value\":20.3}";
        assertNull(TelemetryCodec.decode(json.getBytes()));
    }
}
