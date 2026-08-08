package dev.tstiering.bench;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import dev.tstiering.core.Datapoint;
import dev.tstiering.core.TsValue;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * NDJSON 으로 합성 데이터를 떨군다. 이 파일 크기가 <b>압축률 비교의 기준선</b>이다.
 *
 * <pre>
 * ./gradlew :bench:generate --args="--count=10000000 --out=data/raw.ndjson"
 * </pre>
 */
public final class GenerateMain {

    private static final Instant DEFAULT_START = Instant.parse("2026-01-01T00:00:00Z");

    public static void main(String[] args) throws IOException {
        Map<String, String> opts = parseArgs(args);

        long count = number(opts, "count", "1_000_000");
        Path out = Path.of(opts.getOrDefault("out", "data/raw.ndjson"));
        int tenants = (int) number(opts, "tenants", "3");
        int devices = (int) number(opts, "devices-per-tenant", "200");
        long intervalMs = number(opts, "interval-seconds", "10") * 1000L;
        long start = opts.containsKey("start")
                ? Instant.parse(opts.get("start")).toEpochMilli()
                : DEFAULT_START.toEpochMilli();

        var config = new SyntheticDataGenerator.Config(tenants, devices, "industrial-sensor", intervalMs, start);
        var generator = new SyntheticDataGenerator(config, Sensors.defaultProfile());

        if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
        }

        System.out.printf("생성 시작: count=%,d tenants=%d devices/tenant=%d interval=%ds%n",
                count, tenants, devices, intervalMs / 1000);
        System.out.printf("  틱당 포인트=%,d  →  커버 기간 약 %s%n",
                generator.pointsPerTick(),
                humanDuration(Duration.ofMillis(count / Math.max(1, generator.pointsPerTick()) * intervalMs)));

        long startedAt = System.nanoTime();
        try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(out), 1 << 20);
             JsonGenerator json = newNdjsonGenerator(os)) {
            generator.generate(count, dp -> writeDatapoint(json, dp));
        }
        long elapsedNanos = System.nanoTime() - startedAt;

        long bytes = Files.size(out);
        double seconds = elapsedNanos / 1_000_000_000.0;

        System.out.println("---");
        System.out.printf("파일      : %s%n", out.toAbsolutePath());
        System.out.printf("건수      : %,d%n", count);
        System.out.printf("크기      : %,d bytes (%.2f MiB)%n", bytes, bytes / 1024.0 / 1024.0);
        System.out.printf("건당 크기 : %.1f bytes%n", bytes / (double) count);
        System.out.printf("소요      : %.1fs (%,.0f pt/s)%n", seconds, count / seconds);
        System.out.println();
        System.out.println("이 크기가 W3 Parquet 압축률의 분모다. docs/benchmark/ 에 기록할 것.");
    }

    private static JsonGenerator newNdjsonGenerator(OutputStream os) throws IOException {
        JsonGenerator json = new JsonFactory().createGenerator(os);
        MinimalPrettyPrinter printer = new MinimalPrettyPrinter();
        printer.setRootValueSeparator("\n");
        json.setPrettyPrinter(printer);
        return json;
    }

    private static void writeDatapoint(JsonGenerator json, Datapoint dp) {
        try {
            json.writeStartObject();
            json.writeStringField("tenantId", dp.tenantId().toString());
            json.writeStringField("profile", dp.deviceProfile());
            json.writeStringField("deviceId", dp.entityId().toString());
            json.writeStringField("key", dp.key());
            json.writeNumberField("ts", dp.ts());
            json.writeStringField("type", dp.value().kind().name());
            writeValue(json, dp.value());
            json.writeEndObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // Java 17 이라 sealed interface 에 대한 switch 패턴 매칭을 못 쓴다 (Java 21 부터).
    private static void writeValue(JsonGenerator json, TsValue value) throws IOException {
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

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (String arg : args) {
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("알 수 없는 인자: " + arg);
            }
            int eq = arg.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("--key=value 형식이어야 한다: " + arg);
            }
            opts.put(arg.substring(2, eq), arg.substring(eq + 1));
        }
        return opts;
    }

    /** {@code --count=10_000_000} 처럼 읽기 좋게 쓴 숫자를 허용한다. */
    private static long number(Map<String, String> opts, String key, String fallback) {
        return Long.parseLong(opts.getOrDefault(key, fallback).replace("_", ""));
    }

    private static String humanDuration(Duration d) {
        long days = d.toDays();
        if (days > 0) return days + "일";
        long hours = d.toHours();
        if (hours > 0) return hours + "시간";
        return d.toMinutes() + "분";
    }
}
