package dev.tstiering.core;

import java.util.Objects;

/**
 * 텔레메트리 값. ThingsBoard {@code ts_kv} 의 bool_v / str_v / long_v / dbl_v / json_v 에 대응한다.
 *
 * <p>Parquet 에서 이걸 어떻게 표현할지 — 4~5 개 sparse 컬럼으로 쪼갤지, 단일 컬럼 + 타입 태그로 갈지 —
 * 는 W2 에서 압축률을 재보고 정한다(ADR-0002). 여기서는 표현 방식을 앞질러 결정하지 않는다.
 */
public sealed interface TsValue {

    /** 타입 판별자. Parquet 매핑 실험에서 태그 컬럼 후보로 쓴다. */
    Kind kind();

    enum Kind { BOOL, LONG, DOUBLE, STRING, JSON }

    record BoolValue(boolean value) implements TsValue {
        @Override public Kind kind() { return Kind.BOOL; }
    }

    record LongValue(long value) implements TsValue {
        @Override public Kind kind() { return Kind.LONG; }
    }

    record DoubleValue(double value) implements TsValue {
        @Override public Kind kind() { return Kind.DOUBLE; }
    }

    record StringValue(String value) implements TsValue {
        public StringValue {
            Objects.requireNonNull(value, "value");
        }
        @Override public Kind kind() { return Kind.STRING; }
    }

    record JsonValue(String value) implements TsValue {
        public JsonValue {
            Objects.requireNonNull(value, "value");
        }
        @Override public Kind kind() { return Kind.JSON; }
    }
}
