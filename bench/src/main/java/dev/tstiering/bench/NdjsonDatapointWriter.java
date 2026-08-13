package dev.tstiering.bench;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import dev.tstiering.core.Datapoint;
import dev.tstiering.core.TsValue;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** NDJSON 기준선 라이터. Parquet 압축률의 분모를 만드는 것이 유일한 목적이다. */
public final class NdjsonDatapointWriter implements Closeable {

    private final OutputStream out;
    private final JsonGenerator json;

    public NdjsonDatapointWriter(Path path) throws IOException {
        this(openFile(path));
    }

    private NdjsonDatapointWriter(OutputStream out) throws IOException {
        this.out = out;
        this.json = new JsonFactory().createGenerator(out);
        MinimalPrettyPrinter printer = new MinimalPrettyPrinter();
        printer.setRootValueSeparator("\n");
        json.setPrettyPrinter(printer);
    }

    /**
     * 디스크에 쓰지 않고 바이트 수만 센다.
     *
     * <p>1 년치 기준선(A안 = 1.34억 건)은 NDJSON 으로 약 25 GB 다. 그런데 W1/W2 문서가
     * 결론 낸 대로 NDJSON 은 <b>보고용 분모가 아니고</b>(정직한 분모는 Cassandra 실디스크),
     * W1/W2 와의 연속성을 위해 숫자만 있으면 된다. 25 GB 를 실제로 만들 이유가 없다.
     */
    public static NdjsonDatapointWriter counting(CountingOutputStream sink) throws IOException {
        return new NdjsonDatapointWriter(sink);
    }

    private static OutputStream openFile(Path path) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        return new BufferedOutputStream(Files.newOutputStream(path), 1 << 20);
    }

    public void write(Datapoint dp) {
        try {
            json.writeStartObject();
            json.writeStringField("tenantId", dp.tenantId().toString());
            json.writeStringField("profile", dp.deviceProfile());
            json.writeStringField("deviceId", dp.entityId().toString());
            json.writeStringField("key", dp.key());
            json.writeNumberField("ts", dp.ts());
            json.writeStringField("type", dp.value().kind().name());
            writeValue(dp.value());
            json.writeEndObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // Java 17 이라 sealed interface 에 대한 switch 패턴 매칭을 못 쓴다 (Java 21 부터).
    private void writeValue(TsValue value) throws IOException {
        if (value instanceof TsValue.BoolValue v) {
            json.writeBooleanField("value", v.value());
        } else if (value instanceof TsValue.LongValue v) {
            json.writeNumberField("value", v.value());
        } else if (value instanceof TsValue.DoubleValue v) {
            json.writeNumberField("value", v.value());
        } else if (value instanceof TsValue.StringValue v) {
            json.writeStringField("value", v.value());
        } else if (value instanceof TsValue.JsonValue v) {
            json.writeFieldName("value");
            json.writeRawValue(v.value());
        } else {
            throw new IllegalStateException("unhandled TsValue: " + value.getClass());
        }
    }

    @Override
    public void close() throws IOException {
        json.close();
        out.close();
    }
}
