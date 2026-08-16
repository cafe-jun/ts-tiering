package dev.tstiering.bench;

import dev.tstiering.archiver.Archiver;
import dev.tstiering.archiver.ArchiverConfig;
import dev.tstiering.archiver.LocalSpool;
import dev.tstiering.archiver.TelemetryCodec;
import dev.tstiering.parquet.ClosePolicy;
import dev.tstiering.s3.S3ObjectStore;
import dev.tstiering.s3.S3Settings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;

/**
 * W2 종단 하네스. Kafka 로 흘리고, archiver 로 S3 까지 보내고, 지표를 찍는다.
 *
 * <pre>
 * docker compose -f deploy/docker-compose.dev.yml up -d
 *
 * # 1) 데이터 주입
 * ./gradlew :bench:archive --args="--mode=produce --days=1 --devices-per-tenant=17 --interval-seconds=60"
 *
 * # 2) 적재 (--max-records 로 중간에 끊어 크래시를 흉내낸다)
 * ./gradlew :bench:archive --args="--mode=archive --max-records=200000"
 *
 * # 3) 이어서 재시작 — 건수가 맞아야 한다
 * ./gradlew :bench:archive --mode=archive
 * </pre>
 */
public final class ArchiveMain {

    public static void main(String[] args) throws Exception {
        BenchArgs opts = BenchArgs.parse(args);
        String mode = opts.string("mode", "archive");

        switch (mode) {
            case "produce" -> produce(opts);
            case "archive" -> archive(opts);
            case "verify" -> verify(opts);
            default -> throw new IllegalArgumentException(
                    "알 수 없는 모드: " + mode + " (produce / archive / verify)");
        }
    }

    // --- 주입 -----------------------------------------------------------------

    private static void produce(BenchArgs opts) {
        var generator = opts.generator();
        long count = opts.countFor(generator);
        LateArrival late = LateArrival.from(opts);
        String topic = opts.string("topic", ArchiverConfig.DEFAULT_TOPIC);

        System.out.printf("주입: count=%,d  토픽=%s  지연=%s%n", count, topic, late.describe());

        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, opts.string("bootstrap", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 256 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        // 유실이 있으면 건수 대조가 무의미해진다.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 오염 주입: archiver 가 정말 세고 넘어가는지 확인하려면 실제로 흘려봐야 한다.
        long poison = opts.number("poison", 0);

        long startedAt = System.nanoTime();
        try (var producer = new KafkaProducer<byte[], byte[]>(props)) {
            for (long i = 0; i < poison; i++) {
                producer.send(new ProducerRecord<>(topic, ("poison-" + i).getBytes(),
                        ("{\"broken\":" + i + "}").getBytes()));
            }
            generator.generate(count, late, dp ->
                    // 같은 디바이스는 같은 파티션으로 — 순서가 뒤집히면 워터마크가 더 흔들린다.
                    producer.send(new ProducerRecord<>(topic,
                            dp.entityId().toString().getBytes(), TelemetryCodec.encode(dp))));
        }
        if (poison > 0) System.out.printf("  (깨진 메시지 %,d건 함께 주입)%n", poison);
        double seconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
        System.out.printf("완료: %,d건 %.1fs (%,.0f msg/s)%n", count, seconds, count / seconds);
    }

    // --- 적재 -----------------------------------------------------------------

    private static void archive(BenchArgs opts) throws IOException {
        long maxRecords = opts.number("max-records", Long.MAX_VALUE);
        Path spoolRoot = Path.of(opts.string("spool", "data/archiver"));
        String bucket = opts.string("bucket", "ts-tiering-cold");
        String prefix = opts.string("s3-prefix", "archiver");

        var config = ArchiverConfig.local()
                .withTopic(opts.string("topic", ArchiverConfig.DEFAULT_TOPIC))
                .withGroupId(opts.string("group", "ts-tiering-archiver"))
                .withUncommittedSpanLimit(opts.number("uncommitted-span-limit", 2_000_000));

        var closePolicy = ClosePolicy.watermarkClose(
                Duration.ofMinutes(opts.number("close-lag-minutes", 1440)));

        System.out.printf("적재: 토픽=%s  그룹=%s  스풀=%s  최대=%s%n닫기=%s  미커밋 상한=%,d%n%n",
                config.topic(), config.groupId(), spoolRoot,
                maxRecords == Long.MAX_VALUE ? "무제한" : String.format("%,d", maxRecords),
                closePolicy.name(), config.uncommittedSpanLimit());

        var spool = new LocalSpool(spoolRoot);
        long startedAt = System.nanoTime();

        Archiver.Stats stats;
        try (var s3 = S3ObjectStore.open(S3Settings.local(bucket))) {
            s3.createBucketIfAbsent();
            try (var archiver = new Archiver(config, spool, s3, prefix, closePolicy)) {
                stats = archiver.run(maxRecords);
            }
        }
        double seconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;

        System.out.println("=== 결과 ===");
        System.out.printf("소비        : %,d%n", stats.consumed());
        System.out.printf("기록        : %,d%n", stats.written());
        System.out.printf("파싱 실패   : %,d%n", stats.undecodable());
        System.out.printf("거부        : %,d%n", stats.rejected());
        System.out.printf("업로드 파일 : %,d%n", stats.filesUploaded());
        System.out.printf("복구 올림   : %,d  /  복구 버림 : %,d  ← 버린 만큼 중복이 안 생겼다%n",
                stats.recoveredForUpload(), stats.discardedOnRecovery());
        System.out.printf("최대 미커밋 : %,d 오프셋  ← 재생 구간의 크기%n", stats.maxUncommittedSpan());
        System.out.printf("소요        : %.1fs%n", seconds);

        var broken = spool.verifyReady();
        System.out.printf("%nready 잔여 : %,d개 (푸터 깨진 것 %,d개)%n",
                spool.recover().size(), broken.size());
        if (!broken.isEmpty()) {
            System.out.println("  ⚠️ 푸터가 깨진 파일이 있다 — 불변식 1 위반");
            broken.forEach(p -> System.out.println("     " + p));
        }
    }

    // --- 검증 -----------------------------------------------------------------

    /** S3 에 올라간 행 수를 푸터로 센다. ADR-0001 때문에 레코드는 못 읽지만 푸터는 읽는다. */
    private static void verify(BenchArgs opts) throws Exception {
        String bucket = opts.string("bucket", "ts-tiering-cold");
        String prefix = opts.string("s3-prefix", "archiver");

        try (var s3 = S3ObjectStore.open(S3Settings.local(bucket))) {
            var objects = s3.list(prefix + "/");
            long bytes = objects.stream().mapToLong(S3ObjectStore.ObjectSummary::size).sum();
            System.out.printf("S3 객체: %,d개 / %.1f MiB%n", objects.size(), bytes / 1048576.0);
        }

        try (var conn = DuckDb.openLocal();
             var st = conn.createStatement();
             var rs = st.executeQuery("SELECT count(*) FROM read_parquet('s3://" + bucket + "/"
                     + prefix + "/**/*.parquet', hive_partitioning = 1)")) {
            rs.next();
            System.out.printf("총 행 수: %,d%n", rs.getLong(1));
        }

        try (var conn = DuckDb.openLocal();
             var st = conn.createStatement();
             var rs = st.executeQuery("SELECT count(*) FROM ("
                     + "SELECT DISTINCT tenant_id, device_id, ts, key FROM read_parquet('s3://" + bucket + "/"
                     + prefix + "/**/*.parquet', hive_partitioning = 1))")) {
            rs.next();
            System.out.printf("고유 행 수: %,d  ← 중복 제거 후%n", rs.getLong(1));
        }

        // ADR-0006 결정 8 의 목적지. 파일명 앞의 닫힌 시각으로 "나중 파일이 이긴다"를 정한다.
        // DISTINCT 와 달리 값이 다른 중복에서도 어느 쪽이 이길지가 정해진다.
        try (var conn = DuckDb.openLocal();
             var st = conn.createStatement();
             var rs = st.executeQuery("SELECT count(*) FROM ("
                     + "SELECT * FROM read_parquet('s3://" + bucket + "/" + prefix
                     + "/**/*.parquet', hive_partitioning = 1, filename = 1) "
                     + "QUALIFY row_number() OVER ("
                     + "  PARTITION BY tenant_id, device_id, key, ts ORDER BY filename DESC) = 1)")) {
            rs.next();
            System.out.printf("dedup 후 행 수: %,d  ← 파일명 순서로 해소%n", rs.getLong(1));
        }
    }
}
