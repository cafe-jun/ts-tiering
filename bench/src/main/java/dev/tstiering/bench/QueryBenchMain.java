package dev.tstiering.bench;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * W4~W6 — DuckDB 로 S3 의 Parquet 을 직접 조회한다.
 *
 * <p>쿼리 3종의 <b>의미</b>는 스킴이 바뀌어도 고정이다. 다만 술어가 참조하는 열은 스킴마다
 * 달라진다 — 경로에 `tenant=` 가 있으면 파티션 열로 걸러지고, 없으면 파일 안의 `tenant_id` 를
 * 읽어야 한다. 그 차이가 곧 스킴의 성능이므로, 억지로 같은 SQL 을 쓰는 것이 오히려 비교를 망친다.
 * ({@link SchemeShape} 가 실행 시점에 무엇이 있는지 물어본다.)
 *
 * <pre>
 * docker compose -f deploy/docker-compose.dev.yml up -d
 * ./gradlew :bench:query --args="--iterations=20"
 * ./gradlew :bench:query --args="--prefixes=A-date,B-date,C-date --iterations=10"
 * </pre>
 */
public final class QueryBenchMain {

    record Query(String id, String description, String sql) {
    }

    /**
     * DuckDB 가 {@code EXPLAIN ANALYZE} 에 찍는 스캔 실적.
     *
     * @param httpBytesIn 캐시가 빈 새 연결에서 잰 <b>실제 전송 바이트</b>
     */
    record ScanStats(int filesScanned, int filesListed, long httpBytesIn, int httpGets) {
        double prunedAway() {
            return filesListed <= 0 ? 0 : 100.0 * (filesListed - filesScanned) / filesListed;
        }

        double mibIn() {
            return httpBytesIn < 0 ? 0 : httpBytesIn / 1024.0 / 1024.0;
        }
    }

    record Result(Query query, long rows, double coldMs, double[] warmMs, ScanStats scan) {
        double p50() {
            return percentile(warmMs, 0.50);
        }

        double p95() {
            return percentile(warmMs, 0.95);
        }
    }

    record PrefixResult(String prefix, SchemeShape shape, long objectCount, long objectBytes,
                        double listMs, List<Result> results) {
        double avgFileKiB() {
            return objectCount == 0 ? 0 : objectBytes / (double) objectCount / 1024.0;
        }
    }

    public static void main(String[] args) throws Exception {
        BenchArgs opts = BenchArgs.parse(args);

        String bucket = opts.string("bucket", "ts-tiering-cold");
        String key = opts.string("key", "temperature");
        int iterations = (int) opts.number("iterations", 20);
        int warmup = (int) opts.number("warmup", 3);
        // off = glob(현행) / full = 목록 전부 전달 / pruned = 카탈로그가 프루닝까지 수행
        String manifest = opts.string("manifest", "off");

        List<String> prefixes = Arrays.stream(
                        opts.string("prefixes", opts.string("prefix", "tenant-profile-date")).split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toList();

        var generator = opts.generator();
        String tenant = generator.tenantId(0).toString();
        String device = generator.deviceId(0, 0).toString();

        System.out.printf("버킷=%s  키=%s  프리픽스 %d개  반복=%d (워밍업 %d)  소스=%s%n%n",
                bucket, key, prefixes.size(), iterations, warmup,
                switch (manifest) {
                    case "full" -> "매니페스트(목록 전부)";
                    case "pruned" -> "매니페스트(카탈로그가 프루닝)";
                    default -> "glob";
                });

        List<PrefixResult> all = new ArrayList<>();
        for (String prefix : prefixes) {
            all.add(measure(bucket, prefix, key, tenant, device, iterations, warmup, manifest));
        }

        if (all.size() > 1) {
            printMatrix(all);
        }
    }

    // --- 프리픽스 하나 측정 ------------------------------------------------------

    private static PrefixResult measure(String bucket, String prefix, String key,
                                        String tenant, String device,
                                        int iterations, int warmup, String manifest) throws SQLException {
        String root = "s3://" + bucket + "/" + prefix;
        String glob = root + "/**/key=" + key + "/*.parquet";

        ObjectSet objects = objectStats(bucket, prefix + "/", "/key=" + key + "/");

        System.out.println("═".repeat(78));
        System.out.printf("[%s]  객체 %,d개 / %.1f MiB (평균 %.1f KiB)%n",
                prefix, objects.count(), objects.bytes() / 1024.0 / 1024.0,
                objects.count() == 0 ? 0 : objects.bytes() / (double) objects.count() / 1024.0);

        if (objects.count() == 0) {
            throw new IllegalStateException("프리픽스 '" + prefix + "' 에 객체가 없다 — 적재를 먼저 할 것");
        }

        boolean useManifest = !"off".equals(manifest);
        String source = useManifest
                ? manifestSource(bucket, objects.keys())
                : "read_parquet('" + glob + "', hive_partitioning = 1)";

        try (Connection conn = DuckDb.openLocal()) {
            SchemeShape shape = SchemeShape.detect(conn, source);
            System.out.printf("  술어 열: %s   날짜 범위: %s ~ %s%n",
                    shape.describe(), shape.firstDate(), shape.lastDate());

            double listMs = useManifest ? 0 : measureListing(conn, glob);
            System.out.printf("  %s: %.0fms%n", useManifest ? "나열 없음(목록 전달)" : "glob 나열만", listMs);

            List<Query> queries = defineQueries(source, shape, tenant, device);
            if ("pruned".equals(manifest)) {
                queries = pruneManifests(queries, bucket, objects.keys(), shape, tenant, device);
            }
            List<Result> results = new ArrayList<>();
            for (Query q : queries) {
                results.add(run(conn, q, iterations, warmup, objects.count()));
            }

            printOne(results, listMs);
            return new PrefixResult(prefix, shape, objects.count(), objects.bytes(), listMs, results);
        }
    }

    /**
     * 카탈로그가 <b>메타데이터 단계에서 프루닝까지</b> 수행하는 경우를 흉내낸다.
     *
     * <p>Iceberg 의 매니페스트는 파일마다 파티션 값을 들고 있어, 엔진에 넘기기 전에
     * 조건에 맞는 파일만 골라낼 수 있다. 목록 전부를 넘기는 {@code full} 모드는 나열은 없애지만
     * 경로 N개를 파싱하고 술어를 평가하는 비용이 남는데, 그건 실제 카탈로그가 내는 비용이 아니다.
     *
     * <p>여기서는 경로의 {@code key=value} 를 직접 읽어 거른다. 경로에 없는 축은 거르지 않는다 —
     * 그게 곧 그 스킴이 카탈로그에 줄 수 있는 정보의 한계이기 때문이다.
     */
    private static List<Query> pruneManifests(List<Query> queries, String bucket, List<String> keys,
                                              SchemeShape shape, String tenant, String device) {
        long span = shape.lastDate().toEpochDay() - shape.firstDate().toEpochDay() + 1;
        long offset = span / 4;
        long width = Math.max(1, Math.min(7, span - offset));
        var from = shape.firstDate().plusDays(offset);
        var to = from.plusDays(width);

        List<Query> out = new ArrayList<>();
        for (Query q : queries) {
            // Q3 는 파티션 술어가 없어 전부 읽는다. Q1 만 날짜 창이 있다.
            boolean byIdentity = !q.id().equals("Q3");
            boolean byDate = q.id().equals("Q1");

            List<String> kept = new ArrayList<>();
            for (String k : keys) {
                if (byIdentity && !matches(k, "tenant", tenant)) continue;
                if (byIdentity && !matches(k, "device", device)) continue;
                if (byDate && !dateInRange(k, from.toString(), to.toString())) continue;
                kept.add(k);
            }
            if (kept.isEmpty()) {
                throw new IllegalStateException(q.id() + " 의 프루닝 결과가 비었다 — 경로 파싱이 틀렸다");
            }
            System.out.printf("    %s 매니페스트 프루닝: %,d → %,d 파일%n", q.id(), keys.size(), kept.size());
            out.add(new Query(q.id(), q.description(),
                    q.sql().replace(manifestSource(bucket, keys), manifestSource(bucket, kept))));
        }
        return out;
    }

    /** 경로에 해당 축이 없으면 거르지 않는다 (그 스킴은 그 정보를 카탈로그에 줄 수 없다). */
    private static boolean matches(String key, String axis, String value) {
        int at = key.indexOf(axis + "=");
        if (at < 0) return true;
        int start = at + axis.length() + 1;
        int end = key.indexOf('/', start);
        return key.substring(start, end < 0 ? key.length() : end).equals(value);
    }

    private static boolean dateInRange(String key, String fromInclusive, String toExclusive) {
        int at = key.indexOf("date=");
        if (at < 0) return true;
        int start = at + 5;
        int end = key.indexOf('/', start);
        String d = key.substring(start, end < 0 ? key.length() : end);
        return d.compareTo(fromInclusive) >= 0 && d.compareTo(toExclusive) < 0;
    }

    /**
     * 파일 목록을 그대로 넘기는 소스. <b>매니페스트 카탈로그의 시뮬레이션</b>이다.
     *
     * <p>Iceberg / Delta / Glue 는 파일 목록을 메타데이터로 들고 있어 조회 때 S3 를
     * 나열하지 않는다. 여기서는 목록을 미리 얻어 SQL 에 그대로 박는다 —
     * 그 나열은 쿼리 시간 밖에서 한 번만 일어나므로 카탈로그가 하는 일과 같다.
     *
     * <p>목록은 <b>프루닝하지 않고 전부</b> 넘긴다. 파티션 프루닝은 여전히 DuckDB 가
     * 경로에서 파티션 값을 읽어 수행하므로, glob 모드와의 차이는 정확히 "나열 비용" 하나다.
     */
    private static String manifestSource(String bucket, List<String> keys) {
        StringBuilder sb = new StringBuilder("read_parquet([");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append("'s3://").append(bucket).append('/').append(keys.get(i)).append('\'');
        }
        return sb.append("], hive_partitioning = 1)").toString();
    }

    // --- 쿼리 정의 -------------------------------------------------------------

    /**
     * PHASE1.md 가 정한 3종. 좁은 범위 / 긴 범위 + 집계 / 넓은 범위.
     *
     * <p>날짜 창은 <b>데이터에서 뽑은 실제 범위</b>로 만든다. 예전에는 리터럴을 박아뒀는데,
     * 데이터셋을 30일로 줄이자 Q1 이 조용히 0행을 반환했다.
     */
    private static List<Query> defineQueries(String source, SchemeShape shape,
                                             String tenant, String device) {
        // 커버 기간의 1/4 지점에서 시작하는 7일 창. 데이터가 짧으면 그에 맞춰 줄인다 —
        // 고정 리터럴을 쓰면 데이터셋 길이를 바꿀 때마다 0행이 된다.
        long span = shape.lastDate().toEpochDay() - shape.firstDate().toEpochDay() + 1;
        long offset = span / 4;
        long width = Math.max(1, Math.min(7, span - offset));
        var from = shape.firstDate().plusDays(offset);
        var to = from.plusDays(width);

        String tenantPred = shape.tenantColumn() + "::VARCHAR = '" + tenant + "'";
        String devicePred = shape.deviceColumn() + "::VARCHAR = '" + device + "'";

        return List.of(
                new Query("Q1", "단일 디바이스 / 7일 (좁은 범위)", """
                        SELECT ts, value
                        FROM %s
                        WHERE %s AND %s
                          AND date >= '%s' AND date < '%s'
                        ORDER BY ts
                        """.formatted(source, tenantPred, devicePred, from, to)),

                new Query("Q2", "단일 디바이스 / 전 기간 일평균 (긴 범위 + 집계)", """
                        SELECT date, avg(value) AS avg_value
                        FROM %s
                        WHERE %s AND %s
                        GROUP BY date
                        ORDER BY date
                        """.formatted(source, tenantPred, devicePred)),

                new Query("Q3", "프로파일 전체 / 전 기간 평균 (넓은 범위)", """
                        SELECT avg(value) AS avg_value
                        FROM %s
                        WHERE profile = 'industrial-sensor'
                        """.formatted(source))
        );
    }

    // --- 실행 -----------------------------------------------------------------

    private static Result run(Connection conn, Query q, int iterations, int warmup,
                              int totalFiles) throws SQLException {
        long startedAt = System.nanoTime();
        long rows = execute(conn, q.sql());
        double coldMs = millis(startedAt);

        if (rows == 0) {
            throw new IllegalStateException(q.id() + " 이 0행을 반환했다 — 술어가 데이터와 맞지 않는다:\n" + q.sql());
        }

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

        // DuckDB 는 파일 필터가 하나도 안 걸리거나 파일이 하나뿐일 때 Scanning Files 줄을
        // 아예 찍지 않는다. 그건 "전부 읽었다"는 뜻이므로 전체 파일 수로 채운다.
        ScanStats raw = scanStats(q);
        ScanStats scan = raw.filesScanned() >= 0 ? raw
                : new ScanStats(totalFiles, totalFiles, raw.httpBytesIn(), raw.httpGets());

        System.out.printf("    %s — %,d행, 파일 %d/%d, 전송 %.2f MiB (%d GET), p50 %.0fms%n",
                q.id(), rows, scan.filesScanned(), scan.filesListed(),
                scan.mibIn(), scan.httpGets(), percentile(warm, 0.50));
        return new Result(q, rows, coldMs, warm, scan);
    }

    private static long execute(Connection conn, String sql) throws SQLException {
        long rows = 0;
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                rows++;
            }
        }
        return rows;
    }

    private static final java.util.regex.Pattern SCANNING_FILES =
            java.util.regex.Pattern.compile("ScanningFiles:(\\d+)/(\\d+)");
    private static final java.util.regex.Pattern HTTP_IN =
            java.util.regex.Pattern.compile("in:([\\d.]+)(bytes|KiB|MiB|GiB)");
    private static final java.util.regex.Pattern HTTP_GET =
            java.util.regex.Pattern.compile("#GET:(\\d+)");

    /**
     * {@code EXPLAIN ANALYZE} 를 <b>새 연결</b>에서 돌려 계획과 실제 전송량을 함께 뽑는다.
     *
     * <p>연결을 새로 여는 것이 핵심이다. DuckDB 는 Parquet 푸터와 HTTP 응답을 연결 단위로
     * 캐시하므로 재사용하면 {@code HTTPFS HTTP Stats} 가 전부 0 으로 나온다.
     */
    private static ScanStats scanStats(Query q) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Connection cold = DuckDb.openLocal();
             Statement st = cold.createStatement();
             ResultSet rs = st.executeQuery("EXPLAIN ANALYZE " + q.sql())) {
            while (rs.next()) {
                plan.append(rs.getString(rs.getMetaData().getColumnCount()));
            }
        }
        String flat = plan.toString().replaceAll("[│┌┐└┘├┤─\\s]", "");

        int scanned = -1;
        int listed = -1;
        var files = SCANNING_FILES.matcher(flat);
        if (files.find()) {
            scanned = Integer.parseInt(files.group(1));
            listed = Integer.parseInt(files.group(2));
        }

        long bytesIn = -1;
        var in = HTTP_IN.matcher(flat);
        if (in.find()) {
            bytesIn = toBytes(Double.parseDouble(in.group(1)), in.group(2));
        }

        int gets = -1;
        var get = HTTP_GET.matcher(flat);
        if (get.find()) {
            gets = Integer.parseInt(get.group(1));
        }

        return new ScanStats(scanned, listed, bytesIn, gets);
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

    // --- 출력 -----------------------------------------------------------------

    private static void printOne(List<Result> results, double listMs) {
        System.out.printf("  %-4s %8s %12s %8s %10s %7s %9s %9s%n",
                "", "행", "파일", "프루닝", "전송", "GET", "p50", "p95");
        System.out.println("  " + "-".repeat(74));
        for (Result r : results) {
            System.out.printf("  %-4s %8d %6d/%-5d %7.1f%% %8.2fMiB %7d %7.1fms %7.1fms%n",
                    r.query().id(), r.rows(),
                    r.scan().filesScanned(), r.scan().filesListed(), r.scan().prunedAway(),
                    r.scan().mibIn(), r.scan().httpGets(), r.p50(), r.p95());
        }
        System.out.printf("  (glob 나열만 %.0fms)%n%n", listMs);
    }

    private static void printMatrix(List<PrefixResult> all) {
        System.out.println("═".repeat(78));
        System.out.println("=== 매트릭스 ===\n");

        for (String metric : new String[]{"파일(스캔/전체)", "전송 MiB", "p50 ms"}) {
            System.out.println("[" + metric + "]");
            System.out.printf("  %-24s %14s %14s %14s%n", "프리픽스", "Q1", "Q2", "Q3");
            System.out.println("  " + "-".repeat(70));
            for (PrefixResult p : all) {
                System.out.printf("  %-24s", p.prefix());
                for (Result r : p.results()) {
                    System.out.printf(" %14s", switch (metric) {
                        case "파일(스캔/전체)" -> r.scan().filesScanned() + "/" + r.scan().filesListed();
                        case "전송 MiB" -> String.format("%.2f", r.scan().mibIn());
                        default -> String.format("%.0f", r.p50());
                    });
                }
                System.out.println();
            }
            System.out.println();
        }

        System.out.println("[형상]");
        System.out.printf("  %-24s %10s %10s %10s  %s%n", "프리픽스", "객체", "평균KiB", "나열ms", "술어 열");
        System.out.println("  " + "-".repeat(74));
        for (PrefixResult p : all) {
            System.out.printf("  %-24s %10d %10.1f %10.0f  %s%n",
                    p.prefix(), p.objectCount(), p.avgFileKiB(), p.listMs(), p.shape().describe());
        }
    }

    // --- 보조 -----------------------------------------------------------------

    /**
     * 프리픽스 아래 대상 키의 객체 목록. <b>매니페스트 모드의 입력</b>이기도 하다.
     *
     * <p>이 나열은 쿼리 시간에 포함되지 않는다 — 카탈로그가 이미 들고 있는 정보를
     * 흉내내는 것이므로, 측정 대상은 "그 목록이 있을 때의 쿼리"다.
     */
    record ObjectSet(List<String> keys, long bytes) {
        int count() {
            return keys.size();
        }
    }

    private static ObjectSet objectStats(String bucket, String prefix, String keySegment) {
        try (var store = dev.tstiering.s3.S3ObjectStore.open(
                dev.tstiering.s3.S3Settings.local(bucket))) {
            List<String> keys = new ArrayList<>();
            long bytes = 0;
            for (var o : store.list(prefix)) {
                if (o.key().contains(keySegment)) {
                    keys.add(o.key());
                    bytes += o.size();
                }
            }
            keys.sort(String::compareTo);   // 실행마다 같은 순서여야 비교가 된다
            return new ObjectSet(keys, bytes);
        }
    }

    private static long toBytes(double value, String unit) {
        return switch (unit) {
            case "KiB" -> (long) (value * 1024);
            case "MiB" -> (long) (value * 1024 * 1024);
            case "GiB" -> (long) (value * 1024 * 1024 * 1024);
            default -> (long) value;
        };
    }

    private static double percentile(double[] sorted, double p) {
        if (sorted.length == 0) return 0;
        int idx = (int) Math.ceil(p * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, idx))];
    }

    private static double millis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000.0;
    }
}
