package dev.tstiering.parquet;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

/**
 * 텔레메트리 "값"을 Parquet 스키마로 어떻게 표현할지에 대한 후보들. ADR-0002 의 비교 대상.
 *
 * <p>값이 타입별로 다르다는 것이 시계열 저장의 근본 문제다. Parquet 컬럼은 타입이 하나로 고정되므로
 * 다형적인 값을 담으려면 어딘가에서 타협해야 한다.
 */
public enum ValueLayout {

    /**
     * ThingsBoard {@code ts_kv} 를 그대로 옮긴 형태. 타입별 컬럼을 두고 나머지는 null.
     * 행마다 4개 중 1개만 채워지므로 75% 가 null 이다 —
     * Parquet 의 null 은 definition level 로 RLE 압축되므로 생각만큼 비싸지 않을 수 있다.
     */
    SPARSE_TYPED,

    /**
     * 모든 값을 문자열로 통일. 스키마가 단순하고 어떤 타입이 와도 받아낸다.
     * 대신 숫자에 대한 델타/비트팩킹 인코딩을 포기한다.
     */
    STRINGIFIED,

    /**
     * 키별로 파일을 나누고 값 컬럼을 그 키의 실제 타입으로 둔다.
     * null 도 없고 타입도 정확하지만, 파일 수가 키 개수만큼 늘어난다 (small file 문제와 직결).
     * {@code key} 컬럼 자체가 사라지는 것도 이 레이아웃의 이득이다.
     */
    PER_KEY_TYPED;

    private static Types.MessageTypeBuilder head() {
        return Types.buildMessage();
    }

    private static Types.MessageTypeBuilder withCommon(Types.MessageTypeBuilder b, boolean includeKey) {
        b.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("tenant_id");
        b.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("profile");
        b.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("device_id");
        if (includeKey) {
            b.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key");
        }
        b.required(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("ts");
        return b;
    }

    /** SPARSE_TYPED / STRINGIFIED 용 스키마. PER_KEY_TYPED 는 키 타입을 알아야 하므로 별도 메서드를 쓴다. */
    public MessageType schema() {
        return switch (this) {
            case SPARSE_TYPED -> {
                var b = withCommon(head(), true);
                b.optional(PrimitiveTypeName.BOOLEAN).named("bool_v");
                b.optional(PrimitiveTypeName.INT64).named("long_v");
                b.optional(PrimitiveTypeName.DOUBLE).named("dbl_v");
                b.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("str_v");
                yield b.named("datapoint");
            }
            case STRINGIFIED -> {
                var b = withCommon(head(), true);
                b.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value_type");
                b.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value_str");
                yield b.named("datapoint");
            }
            case PER_KEY_TYPED -> throw new IllegalStateException(
                    "PER_KEY_TYPED 은 키의 타입을 알아야 한다. schemaForKind(kind) 를 쓸 것");
        };
    }

    /** PER_KEY_TYPED 전용. 파일 하나가 키 하나를 담으므로 key 컬럼이 없다. */
    public static MessageType schemaForKind(dev.tstiering.core.TsValue.Kind kind) {
        var b = withCommon(head(), false);
        switch (kind) {
            case BOOL -> b.required(PrimitiveTypeName.BOOLEAN).named("value");
            case LONG -> b.required(PrimitiveTypeName.INT64).named("value");
            case DOUBLE -> b.required(PrimitiveTypeName.DOUBLE).named("value");
            case STRING, JSON -> b.required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType()).named("value");
        }
        return b.named("datapoint");
    }
}
