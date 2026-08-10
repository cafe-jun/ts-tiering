package dev.tstiering.bench;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * W4 — DuckDB 로 S3 의 Parquet 을 직접 조회한다.
 *
 * <p>여기서 정의하는 쿼리 3종은 <b>W5~W6 파티셔닝 비교에서도 그대로 쓰인다.</b>
 * 스킴이 바뀌면 경로만 바뀌고 쿼리 본문은 고정이어야 비교가 성립한다.
 *
 * <pre>
 * docker compose -f deploy/docker-compose.dev.yml up -d
 * ./gradlew :bench:query --args="--iterations=20"
 * ./gradlew :bench:query --args="--iterations=1 --explain=true"
 * </pre>
 */
public final class QueryBenchMain {

    record Query(String id, String description, String sql) {
    }

    /**
     * DuckDB 가 EXPLAIN ANALYZE 에 찍는 {@code Scanning Files: X/Y}.
     * 파티션 프루닝이 실제로 걸렸는지에 대한 직접 증거다.
     */
    record ScanStats(int filesScanned, int filesListed) {
        double prunedAway() {
            return filesListed == 0 ? 0 : 100.0 * (filesListed - filesScanned) / filesListed;
        }
    }

    record Result(Query query, long rows, double coldMs, double[] warmMs, ScanStats scan) {
        double p50() {
            return percentile(warmMs, 0.50);
        }

        double p95() {
            return percentile(warmMs, 0.95);
        }

        double min() {
            return warmMs.length == 0 ? 0 : warmMs[0];
        }
    }

    public static void main(String[] args) throws Exception {
        BenchArgs opts = BenchArgs.parse(args);

        String bucket = opts.string("bucket", "ts-tiering-cold");
        String prefix = opts.string("prefix", "tenant-profile-date");
        int iterations = (int) opts.number("iterations", 20);
        int warmup = (int) opts.number("warmup", 3);
        boolean explain = Boolean.parseBoolean(opts.string("explain", "false"));

        String key = opts.string("key", "temperature");

        // 경로 규칙이 스킴마다 달라 깊이가 다르므로 ** 로 받는다.
        String root = "s3://" + bucket + "/" + prefix;
        String allKeysSource = "read_parquet('" + root + "/**/*.parquet', hive_partitioning = 1)";

        // 키를 glob 에 박는다. PER_KEY_TYPED(ADR-0002)에서 value 의 물리 타입이 키마다 다르기 때문이다 —
        // 키를 가로지르는 glob 을 쓰면 DuckDB 가 스키마를 통합하면서 value 를 VARCHAR 로 내려버리고
        // avg(value) 가 바인딩 에러를 낸다. 실제 쿼리 라우터도 키를 알고 들어오므로 이쪽이 현실적이다.
        String glob = root + "/**/key=" + key + "/*.parquet";
        String source = "read_parquet('" + glob + "', hive_partitioning = 1)";

        var generator = opts.generator();
        String tenant = generator.tenantId(0).toString();
        String device = generator.deviceId(0, 0).toString();

        System.out.printf("소스=%s%n반복=%d (워밍업 %d)%n%n", glob, iterations, warmup);

        List<Query> queries = defineQueries(source, tenant, device);

        // 파일당 평균 크기는 S3 쪽에서 정확히 받아온다 — 프루닝된 바이트를 환산하는 데 쓴다.
        long[] objects = objectStats(bucket, prefix + "/", "/key=" + key + "/");
        double avgFileBytes = objects[0] == 0 ? 0 : objects[1] / (double) objects[0];
        System.out.printf("키 '%s' 객체: %,d개 / %.1f MiB (평균 %.1f KiB)%n%n",
                key, objects[0], objects[1] / 1024.0 / 1024.0, avgFileBytes / 1024.0);

        try (Connection conn = DuckDb.openLocal()) {
            sanityCheck(conn, allKeysSource);

            double listMs = measureListing(conn, glob);
            System.out.printf("glob 나열만: %.0fms (%,d개 객체)%n%n", listMs, objects[0]);

            List<Result> results = new ArrayList<>();
            for (Query q : queries) {
                results.add(run(conn, q, iterations, warmup));
            }

            report(results, avgFileBytes, listMs);

            if (explain) {
                explainAll(conn, queries);
            }
        }
    }

    // --- 쿼리 정의 -------------------------------------------------------------

    /**
     * PHASE1.md 가 정한 3종. 좁은 범위 / 긴 범위 + 집계 / 넓은 범위.
     *
     * <p>키는 WHERE 절이 아니라 <b>glob 경로</b>에 있다 (위 참고). 나머지 파티션 열
     * ({@code tenant}, {@code date})은 경로에서 나오므로 프루닝이 걸리고,
     * {@code device_id} 는 파일 안의 열이라 행 필터다 — 스킴 B 에서는 그렇다.
     * 스킴 C(`tenant/device/date`)로 바꾸면 {@code device_id} 도 프루닝 대상이 된다.
     * <b>그 차이를 재는 것이 W5~W6 의 목적이므로 쿼리 본문은 여기서 고정한다.</b>
     */
    private static List<Query> defineQueries(String source, String tenant, String device) {
        return List.of(
                new Query("Q1", "단일 디바이스 / 단일 키 / 7일 (좁은 범위)", """
                        SELECT ts, value
                        FROM %s
                        WHERE tenant = '%s'
                          AND device_id = '%s'
                          AND date >= '2026-03-01' AND date < '2026-03-08'
                        ORDER BY ts
                        """.formatted(source, tenant, device)),

                new Query("Q2", "단일 디바이스 / 단일 키 / 1년 일평균 (긴 범위 + 집계)", """
                        SELECT date, avg(value) AS avg_value
                        FROM %s
                        WHERE tenant = '%s'
                          AND device_id = '%s'
                        GROUP BY date
                        ORDER BY date
                        """.formatted(source, tenant, device)),

                new Query("Q3", "프로파일 전체 / 단일 키 / 1개월 평균 (넓은 범위)", """
                        SELECT avg(value) AS avg_value
                        FROM %s
                        WHERE profile = 'industrial-sensor'
                          AND date >= '2026-03-01' AND date < '2026-04-01'
                        """.formatted(source))
        );
    }

    // --- 실행 -----------------------------------------------------------------

    private static void sanityCheck(Connection conn, String source) throws SQLException {
        long startedAt = System.nanoTime();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM " + source)) {
            rs.next();
            long rows = rs.getLong(1);
            System.out.printf("연결 확인: 전체 %,d 행 (%.1fs)%n", rows, seconds(startedAt));
            if (rows == 0) {
                throw new IllegalStateException(
                        "0행이다. 적재가 안 됐거나 prefix 가 틀렸다 — :bench:ingest --s3=true 를 먼저 돌릴 것");
            }
        }
        System.out.println();
    }

    private static Result run(Connection conn, Query q, int iterations, int warmup) throws SQLException {
        // 첫 실행은 따로 잰다. DuckDB 가 Parquet 푸터와 HTTP 메타데이터를 캐시하므로
        // 2회차부터는 성격이 다른 숫자가 된다. 둘을 섞으면 무엇을 쟀는지 흐려진다.
        long startedAt = System.nanoTime();
        long rows = execute(conn, q.sql());
        double coldMs = millis(startedAt);

        for (int i = 0; i < warmup; i++) {
            execute(conn, q.sql());
        }

        double[] warm = new double[iterations];
        for (int i = 0; i < iterations; i++) {
            long t = System.nanoTime();
            execute(conn, q.sql());
            warm[i] = millis(t);
        }
        Arrays.sort(warm);

        ScanStats scan = scanStats(conn, q);

        System.out.printf("  %s 완료 — %,d행, 파일 %d/%d, cold %.0fms, warm p50 %.0fms%n",
                q.id(), rows, scan.filesScanned(), scan.filesListed(), coldMs, percentile(warm, 0.50));
        return new Result(q, rows, coldMs, warm, scan);
    }

    private static final java.util.regex.Pattern SCANNING_FILES =
            java.util.regex.Pattern.compile("ScanningFiles:(\\d+)/(\\d+)");

    /** EXPLAIN ANALYZE 출력의 박스 문자를 걷어내고 {@code Scanning Files: X/Y} 를 뽑는다. */
    private static ScanStats scanStats(Connection conn, Query q) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("EXPLAIN ANALYZE " + q.sql())) {
            while (rs.next()) {
                plan.append(rs.getString(rs.getMetaData().getColumnCount()));
            }
        }
        String flat = plan.toString().replaceAll("[│┌┐└┘├┤─\\s]", "");
        var m = SCANNING_FILES.matcher(flat);
        return m.find()
                ? new ScanStats(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)))
                : new ScanStats(-1, -1);
    }

    private static long execute(Connection conn, String sql) throws SQLException {
        long rows = 0;
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            // 결과를 끝까지 소비해야 실제 스캔 시간이 포함된다.
            while (rs.next()) {
                rows++;
            }
        }
        return rows;
    }

    // --- 출력 -----------------------------------------------------------------

    private static void report(List<Result> results, double avgFileBytes, double listMs) {
        System.out.println("\n=== 쿼리 baseline ===");
        System.out.printf("%-4s %8s %12s %9s %10s %9s %9s%n",
                "", "행", "파일", "프루닝", "스캔량", "p50", "p95");
        System.out.println("-".repeat(66));
        for (Result r : results) {
            System.out.printf("%-4s %8d %6d/%-5d %8.1f%% %8.2fMiB %7.1fms %7.1fms%n",
                    r.query().id(), r.rows(),
                    r.scan().filesScanned(), r.scan().filesListed(), r.scan().prunedAway(),
                    r.scan().filesScanned() * avgFileBytes / 1024.0 / 1024.0,
                    r.p50(), r.p95());
        }
        System.out.println();
        for (Result r : results) {
            System.out.printf("  %s  %s%n", r.query().id(), r.query().description());
        }

        System.out.printf("%n스캔량은 (스캔 파일 수 × 평균 파일 크기 %.1f KiB) 환산치다.%n",
                avgFileBytes / 1024.0);
        System.out.println("p50/p95 는 워밍업 후 반복 실행이고, cold(첫 실행)는 위 진행 로그에 있다.");
        System.out.printf("%n⚠️ 세 쿼리의 스캔량은 %.0f배 차이인데 지연은 거의 같다.%n",
                results.get(1).scan().filesScanned() / (double) results.get(0).scan().filesScanned());
        System.out.printf("   glob 나열만으로 %.0fms 가 든다 — 지연을 지배하는 것은 스캔이 아니라 객체 나열이다.%n", listMs);
    }

    /** 스캔 없이 파일 목록만 얻는 비용. 지연의 하한선이 된다. */
    private static double measureListing(Connection conn, String glob) throws SQLException {
        long best = Long.MAX_VALUE;
        for (int i = 0; i < 5; i++) {
            long t = System.nanoTime();
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM glob('" + glob + "')")) {
                rs.next();
            }
            best = Math.min(best, System.nanoTime() - t);
        }
        return best / 1_000_000.0;
    }

    /** @return {객체 수, 총 바이트} */
    private static long[] objectStats(String bucket, String prefix, String keySegment) {
        try (var store = dev.tstiering.s3.S3ObjectStore.open(
                dev.tstiering.s3.S3Settings.local(bucket))) {
            long count = 0;
            long bytes = 0;
            for (var o : store.list(prefix)) {
                if (o.key().contains(keySegment)) {
                    count++;
                    bytes += o.size();
                }
            }
            return new long[]{count, bytes};
        }
    }

    private static void explainAll(Connection conn, List<Query> queries) throws SQLException {
        for (Query q : queries) {
            System.out.printf("%n=== EXPLAIN ANALYZE %s ===%n", q.id());
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("EXPLAIN ANALYZE " + q.sql())) {
                while (rs.next()) {
                    System.out.println(rs.getString(rs.getMetaData().getColumnCount()));
                }
            }
        }
    }

    // --- 보조 -----------------------------------------------------------------

    private static double percentile(double[] sorted, double p) {
        if (sorted.length == 0) return 0;
        int idx = (int) Math.ceil(p * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, idx))];
    }

    private static double millis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000.0;
    }

    private static double seconds(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000_000.0;
    }
}
