package dev.tstiering.bench;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import dev.tstiering.core.Datapoint;
import dev.tstiering.core.TsValue;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 압축률의 <b>정직한 분모</b>를 만든다.
 *
 * <p>W1·W2·W3 문서가 반복해서 지적한 문제 — NDJSON 대비 배수는 아무도 실제로 쓰지 않는
 * 포맷을 분모로 삼은 것이라 의미가 약하다. 회사를 설득할 때 필요한 숫자는
 * "지금 Cassandra 가 먹고 있는 디스크 대비 얼마나 줄어드는가"다.
 *
 * <p>Phase 1 은 회사 Cassandra 에 접속하지 않는다. 대신 ThingsBoard 스키마를 로컬에 그대로
 * 세우고 같은 데이터를 넣어 SSTable 크기를 <b>실측</b>한다. 해석적 모델(행 오버헤드 × 행 수)을
 * 쓰지 않는 이유는 셀 헤더·타임스탬프 델타·LZ4 청크 경계가 얽혀 2배쯤 틀리기 쉽기 때문이다.
 *
 * <pre>
 * docker compose -f deploy/docker-compose.dev.yml up -d cassandra
 * ./gradlew :bench:cassandraBaseline --args="--days=30 --devices-per-tenant=17 --interval-seconds=60"
 *
 * # 적재 후 크기는 nodetool 로 읽는다 (flush → compact 로 정상 상태를 만든 뒤)
 * docker exec ts-tiering-cassandra nodetool flush thingsboard ts_kv_cf
 * docker exec ts-tiering-cassandra nodetool compact thingsboard ts_kv_cf
 * docker exec ts-tiering-cassandra nodetool tablestats thingsboard.ts_kv_cf
 * </pre>
 */
public final class CassandraBaselineMain {

    /** 동시에 떠 있는 비동기 쓰기 수. 너무 크면 코디네이터가 OverloadedException 을 던진다. */
    private static final int MAX_INFLIGHT = 512;

    public static void main(String[] args) throws Exception {
        BenchArgs opts = BenchArgs.parse(args);
        var generator = opts.generator();
        long count = opts.countFor(generator);

        String host = opts.string("cassandra-host", "127.0.0.1");
        int port = (int) opts.number("cassandra-port", 9042);

        System.out.printf("count=%,d  대상=%s:%d  키스페이스=%s.%s%n",
                count, host, port, CassandraSchema.KEYSPACE, CassandraSchema.TABLE);

        var loader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 4)
                .build();

        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter(opts.string("cassandra-dc", "datacenter1"))
                .withConfigLoader(loader)
                .build()) {

            session.execute(CassandraSchema.createKeyspace());
            session.execute(CassandraSchema.createTable());
            // 이전 실행이 남아 있으면 크기가 누적돼 측정이 무의미해진다.
            session.execute("TRUNCATE " + CassandraSchema.KEYSPACE + "." + CassandraSchema.TABLE);
            System.out.println("스키마 준비 완료 (기존 데이터 TRUNCATE)");

            var writer = new AsyncWriter(session);
            long startedAt = System.nanoTime();
            generator.generate(count, writer::write);
            writer.drain();
            double seconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;

            System.out.println("---");
            System.out.printf("적재 건수 : %,d%n", count);
            System.out.printf("소요      : %.1fs (%,.0f rows/s)%n", seconds, count / seconds);
            System.out.println();
            System.out.println("이제 SSTable 크기를 읽는다:");
            System.out.println("  docker exec ts-tiering-cassandra nodetool flush thingsboard ts_kv_cf");
            System.out.println("  docker exec ts-tiering-cassandra nodetool compact thingsboard ts_kv_cf");
            System.out.println("  docker exec ts-tiering-cassandra nodetool tablestats thingsboard.ts_kv_cf");
        }
    }

    /**
     * 비동기 쓰기를 세마포어로 제한한다. 동기 실행은 왕복 지연에 묶여 1만 rows/s 를 넘기 어렵고,
     * 제한 없는 비동기는 큐가 터진다.
     */
    private static final class AsyncWriter {

        private final CqlSession session;
        private final PreparedStatement boolStmt;
        private final PreparedStatement longStmt;
        private final PreparedStatement doubleStmt;
        private final PreparedStatement strStmt;
        private final PreparedStatement jsonStmt;
        private final Semaphore inflight = new Semaphore(MAX_INFLIGHT);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        AsyncWriter(CqlSession session) {
            this.session = session;
            this.boolStmt = prepare("bool_v");
            this.longStmt = prepare("long_v");
            this.doubleStmt = prepare("dbl_v");
            this.strStmt = prepare("str_v");
            this.jsonStmt = prepare("json_v");
        }

        /** 값 컬럼 하나만 채우는 INSERT. TB 도 이렇게 쓰기 때문에 나머지 컬럼은 셀 자체가 생기지 않는다. */
        private PreparedStatement prepare(String valueColumn) {
            return session.prepare("INSERT INTO " + CassandraSchema.KEYSPACE + "." + CassandraSchema.TABLE
                    + " (entity_type, entity_id, key, partition, ts, " + valueColumn + ")"
                    + " VALUES (?, ?, ?, ?, ?, ?)");
        }

        void write(Datapoint dp) {
            Throwable seen = failure.get();
            if (seen != null) throw new IllegalStateException("쓰기 실패", seen);

            BoundStatement bound = bind(dp);
            try {
                inflight.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("중단됨", e);
            }
            session.executeAsync(bound).whenComplete((r, t) -> {
                if (t != null) failure.compareAndSet(null, t);
                inflight.release();
            });
        }

        private BoundStatement bind(Datapoint dp) {
            long partition = CassandraSchema.partitionOf(dp.ts());
            TsValue v = dp.value();

            // Java 17 이라 sealed interface 에 대한 switch 패턴 매칭을 못 쓴다 (Java 21 부터).
            PreparedStatement stmt;
            Object value;
            if (v instanceof TsValue.BoolValue b) {
                stmt = boolStmt;
                value = b.value();
            } else if (v instanceof TsValue.LongValue l) {
                stmt = longStmt;
                value = l.value();
            } else if (v instanceof TsValue.DoubleValue d) {
                stmt = doubleStmt;
                value = d.value();
            } else if (v instanceof TsValue.StringValue s) {
                stmt = strStmt;
                value = s.value();
            } else if (v instanceof TsValue.JsonValue j) {
                stmt = jsonStmt;
                value = j.value();
            } else {
                throw new IllegalStateException("unhandled TsValue: " + v.getClass());
            }

            return stmt.bind("DEVICE", dp.entityId(), dp.key(), partition, dp.ts(), value);
        }

        /** 모든 in-flight 가 끝날 때까지 기다린다. 이걸 빼먹으면 flush 전에 프로세스가 죽는다. */
        void drain() throws InterruptedException {
            inflight.acquire(MAX_INFLIGHT);
            Throwable seen = failure.get();
            if (seen != null) throw new IllegalStateException("쓰기 실패", seen);
        }
    }
}
