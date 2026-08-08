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
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        this.out = new BufferedOutputStream(Files.newOutputStream(path), 1 << 20);
        this.json = new JsonFactory().createGenerator(out);
        MinimalPrettyPrinter printer = new MinimalPrettyPrinter();
        printer.setRootValueSeparator("\n");
        json.setPrettyPrinter(printer);
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
