package dev.tstiering.archiver;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import dev.tstiering.core.Datapoint;
import dev.tstiering.core.TsValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Kafka 메시지 ↔ {@link Datapoint}. bench 의 NDJSON 과 같은 형태를 쓴다 —
 * 그래야 Phase 1 에서 만든 합성 데이터를 그대로 Kafka 에 흘려 비교할 수 있다.
 *
 * <pre>
 * {"tenantId":"...","profile":"industrial-sensor","deviceId":"...",
 *  "key":"temperature","ts":1767225600000,"type":"DOUBLE","value":20.3}
 * </pre>
 *
 * <p><b>파싱 실패를 예외로 던지지 않는다.</b> archiver 는 한 건이 깨졌다고 컨슈머를 멈추면
 * 안 되기 때문이다 — 그 메시지 하나가 파티션 전체를 영구히 막는다.
 * 대신 {@code null} 을 돌려주고 호출자가 세어서 격리하도록 한다.
 */
public final class TelemetryCodec {

    private static final JsonFactory JSON = new JsonFactory();

    private TelemetryCodec() {
    }

    /** @return 파싱할 수 없으면 {@code null}. 던지지 않는다 */
    public static Datapoint decode(byte[] payload) {
        if (payload == null || payload.length == 0) return null;

        UUID tenantId = null;
        UUID entityId = null;
        String profile = null;
        String key = null;
        Long ts = null;
        String type = null;
        Object rawValue = null;

        try (JsonParser p = JSON.createParser(payload)) {
            if (p.nextToken() != JsonToken.START_OBJECT) return null;
            while (p.nextToken() != JsonToken.END_OBJECT) {
                String field = p.currentName();
                p.nextToken();
                switch (field) {
                    case "tenantId" -> tenantId = UUID.fromString(p.getText());
                    case "deviceId" -> entityId = UUID.fromString(p.getText());
                    case "profile" -> profile = p.getText();
                    case "key" -> key = p.getText();
                    case "ts" -> ts = p.getLongValue();
                    case "type" -> type = p.getText();
                    case "value" -> rawValue = readValue(p);
                    default -> p.skipChildren();
                }
            }
        } catch (IOException | IllegalArgumentException | NullPointerException e) {
            return null;
        }

        if (tenantId == null || entityId == null || profile == null
                || key == null || ts == null || type == null || rawValue == null) {
            return null;
        }

        TsValue value = toTsValue(type, rawValue);
        if (value == null) return null;

        try {
            return new Datapoint(tenantId, profile, entityId, key, ts, value);
        } catch (IllegalArgumentException e) {
            return null;   // ts < 0 등 도메인 규칙 위반
        }
    }

    private static Object readValue(JsonParser p) throws IOException {
        return switch (p.currentToken()) {
            case VALUE_TRUE, VALUE_FALSE -> p.getBooleanValue();
            case VALUE_NUMBER_INT -> p.getLongValue();
            case VALUE_NUMBER_FLOAT -> p.getDoubleValue();
            case VALUE_STRING -> p.getText();
            // JSON 값은 중첩 구조로 들어온다. 하위 트리를 그대로 문자열로 되살린다 —
            // jackson-databind 없이 jackson-core 만으로 하려면 이 방법뿐이다.
            case START_OBJECT, START_ARRAY -> {
                var out = new ByteArrayOutputStream(128);
                try (JsonGenerator g = JSON.createGenerator(out)) {
                    g.copyCurrentStructure(p);
                }
                yield out.toString(StandardCharsets.UTF_8);
            }
            default -> null;
        };
    }

    /** {@code type} 이 실제 값과 어긋나면 {@code null} — 조용히 캐스팅하면 스키마가 오염된다. */
    private static TsValue toTsValue(String type, Object raw) {
        try {
            return switch (type) {
                case "BOOL" -> new TsValue.BoolValue((Boolean) raw);
                case "LONG" -> new TsValue.LongValue(((Number) raw).longValue());
                case "DOUBLE" -> new TsValue.DoubleValue(((Number) raw).doubleValue());
                case "STRING" -> new TsValue.StringValue((String) raw);
                case "JSON" -> new TsValue.JsonValue((String) raw);
                default -> null;
            };
        } catch (ClassCastException e) {
            return null;
        }
    }

    /** 테스트와 데이터 주입용. bench 의 NdjsonDatapointWriter 와 같은 필드 순서를 지킨다. */
    public static byte[] encode(Datapoint dp) {
        var out = new ByteArrayOutputStream(192);
        try (JsonGenerator g = JSON.createGenerator(out)) {
            g.setPrettyPrinter(new MinimalPrettyPrinter());
            g.writeStartObject();
            g.writeStringField("tenantId", dp.tenantId().toString());
            g.writeStringField("profile", dp.deviceProfile());
            g.writeStringField("deviceId", dp.entityId().toString());
            g.writeStringField("key", dp.key());
            g.writeNumberField("ts", dp.ts());
            g.writeStringField("type", dp.value().kind().name());
            writeValue(g, dp.value());
            g.writeEndObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    // Java 17 이라 sealed interface 에 대한 switch 패턴 매칭을 못 쓴다 (Java 21 부터).
    private static void writeValue(JsonGenerator g, TsValue value) throws IOException {
        if (value instanceof TsValue.BoolValue v) {
            g.writeBooleanField("value", v.value());
        } else if (value instanceof TsValue.LongValue v) {
            g.writeNumberField("value", v.value());
        } else if (value instanceof TsValue.DoubleValue v) {
            g.writeNumberField("value", v.value());
        } else if (value instanceof TsValue.StringValue v) {
            g.writeStringField("value", v.value());
        } else if (value instanceof TsValue.JsonValue v) {
            g.writeFieldName("value");
            g.writeRawValue(v.value());
        } else {
            throw new IllegalStateException("unhandled TsValue: " + value.getClass());
        }
    }

    public static String toJson(Datapoint dp) {
        return new String(encode(dp), StandardCharsets.UTF_8);
    }
}
