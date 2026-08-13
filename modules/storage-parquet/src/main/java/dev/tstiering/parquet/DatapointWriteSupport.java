package dev.tstiering.parquet;

import dev.tstiering.core.Datapoint;
import dev.tstiering.core.TsValue;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

/**
 * Datapoint 를 Group 객체 생성 없이 바로 RecordConsumer 에 흘려보낸다.
 *
 * <p>{@code ExampleParquetWriter} 의 Group API 를 쓰면 행마다 SimpleGroup 을 할당해야 한다.
 * 수천만 행을 쓰는 벤치마크에서는 그 할당이 그대로 GC 부담이 된다.
 */
final class DatapointWriteSupport extends WriteSupport<Datapoint> {

    private final MessageType schema;
    private final ValueLayout layout;
    /** PER_KEY_TYPED 일 때만 쓰인다. 파일이 담당하는 키의 타입. */
    private final TsValue.Kind fixedKind;

    private RecordConsumer consumer;

    DatapointWriteSupport(MessageType schema, ValueLayout layout, TsValue.Kind fixedKind) {
        this.schema = schema;
        this.layout = layout;
        this.fixedKind = fixedKind;
    }

    /**
     * Hadoop Configuration 을 받는 오버로드. 상위 클래스에서 abstract 라 구현은 강제되지만,
     * {@code ParquetWriter.Builder#withConf(ParquetConfiguration)} 경로로만 쓰므로 호출되지 않는다.
     * 호출된다면 Hadoop 경로로 잘못 들어왔다는 뜻이라 조용히 넘어가지 않고 즉시 터뜨린다.
     */
    @Override
    public WriteContext init(org.apache.hadoop.conf.Configuration configuration) {
        throw new UnsupportedOperationException(
                "Hadoop Configuration 경로는 지원하지 않는다. withConf(ParquetConfiguration) 을 쓸 것");
    }

    @Override
    public WriteContext init(ParquetConfiguration configuration) {
        return new WriteContext(schema, Map.of("ts-tiering.layout", layout.name()));
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.consumer = recordConsumer;
    }

    @Override
    public void write(Datapoint dp) {
        consumer.startMessage();
        int f = 0;
        string(f++, "tenant_id", dp.tenantId().toString());
        string(f++, "profile", dp.deviceProfile());
        string(f++, "device_id", dp.entityId().toString());
        if (layout != ValueLayout.PER_KEY_TYPED) {
            string(f++, "key", dp.key());
        }
        consumer.startField("ts", f);
        consumer.addLong(dp.ts());
        consumer.endField("ts", f);
        f++;

        switch (layout) {
            case SPARSE_TYPED -> writeSparse(dp.value(), f);
            case STRINGIFIED -> writeStringified(dp.value(), f);
            case PER_KEY_TYPED -> writeTyped(dp.value(), f);
        }
        consumer.endMessage();
    }

    /** 채워진 타입의 컬럼 하나만 쓴다. 나머지는 필드를 아예 열지 않으면 null 이 된다. */
    private void writeSparse(TsValue v, int base) {
        if (v instanceof TsValue.BoolValue b) {
            consumer.startField("bool_v", base);
            consumer.addBoolean(b.value());
            consumer.endField("bool_v", base);
        } else if (v instanceof TsValue.LongValue l) {
            consumer.startField("long_v", base + 1);
            consumer.addLong(l.value());
            consumer.endField("long_v", base + 1);
        } else if (v instanceof TsValue.DoubleValue d) {
            consumer.startField("dbl_v", base + 2);
            consumer.addDouble(d.value());
            consumer.endField("dbl_v", base + 2);
        } else {
            consumer.startField("str_v", base + 3);
            consumer.addBinary(Binary.fromString(asString(v)));
            consumer.endField("str_v", base + 3);
        }
    }

    private void writeStringified(TsValue v, int base) {
        string(base, "value_type", v.kind().name());
        string(base + 1, "value_str", asString(v));
    }

    private void writeTyped(TsValue v, int idx) {
        consumer.startField("value", idx);
        switch (fixedKind) {
            case BOOL -> consumer.addBoolean(((TsValue.BoolValue) v).value());
            case LONG -> consumer.addLong(((TsValue.LongValue) v).value());
            case DOUBLE -> consumer.addDouble(((TsValue.DoubleValue) v).value());
            case STRING, JSON -> consumer.addBinary(Binary.fromString(asString(v)));
        }
        consumer.endField("value", idx);
    }

    private void string(int idx, String name, String value) {
        consumer.startField(name, idx);
        consumer.addBinary(Binary.fromString(value));
        consumer.endField(name, idx);
    }

    private static String asString(TsValue v) {
        if (v instanceof TsValue.BoolValue b) return Boolean.toString(b.value());
        if (v instanceof TsValue.LongValue l) return Long.toString(l.value());
        if (v instanceof TsValue.DoubleValue d) return Double.toString(d.value());
        if (v instanceof TsValue.StringValue s) return s.value();
        if (v instanceof TsValue.JsonValue j) return j.value();
        throw new IllegalStateException("unhandled TsValue: " + v.getClass());
    }
}
