package dev.tstiering.bench;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 일회성 진단. 파티션 스킴마다 DuckDB 가 어떤 컬럼을 노출하는지 확인한다.
 *
 * <p>경로에 {@code profile=} 을 넣어도 파일 안에 같은 이름의 열이 있으면 파티션 쪽이 죽는다.
 * 이름만 보고는 알 수 없어서 만든 도구이고, ADR-0004 가 {@code profile=} 계층을 뺀 근거다.
 *
 * <pre>
 * ./gradlew :bench:schemaProbe --args="tenant-profile-date C-date"
 * </pre>
 */
public class SchemaProbe {

    public static void main(String[] args) throws Exception {
        String[] prefixes = args.length > 0 ? args : new String[]{"tenant-profile-date"};

        try (Connection c = DuckDb.openLocal()) {
            for (String p : prefixes) {
                String src = "read_parquet('s3://ts-tiering-cold/" + p
                        + "/**/key=temperature/*.parquet', hive_partitioning=1)";

                System.out.println("=== " + p + " ===");
                try (Statement st = c.createStatement();
                     ResultSet rs = st.executeQuery("SELECT * FROM " + src + " LIMIT 0")) {
                    var md = rs.getMetaData();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        System.out.printf("  %-12s %s%n", md.getColumnName(i), md.getColumnTypeName(i));
                    }
                }
                for (String col : new String[]{"tenant", "tenant_id", "profile", "device", "device_id", "date"}) {
                    System.out.printf("  %-12s → %s%n", "WHERE " + col, probe(c, src, col));
                }
                System.out.println();
            }

            // 매니페스트 실험의 전제 확인: glob 대신 파일 목록을 받아주는가.
            System.out.println("=== read_parquet 이 파일 목록을 받는가 ===");
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("""
                         SELECT count(*) FROM read_parquet([
                           's3://ts-tiering-cold/B-date/tenant=00000000-007e-4a47-0000-000000000000/profile=industrial-sensor/date=2026-01-08/key=temperature/part-0.parquet',
                           's3://ts-tiering-cold/B-date/tenant=00000000-007e-4a47-0000-000000000000/profile=industrial-sensor/date=2026-01-09/key=temperature/part-0.parquet'
                         ], hive_partitioning=1)""")) {
                rs.next();
                System.out.printf("  받는다 — 2개 파일에서 %,d행%n", rs.getLong(1));
            } catch (SQLException e) {
                System.out.println("  실패: " + e.getMessage().split("\n")[0]);
            }
        }
    }

    private static String probe(Connection c, String src, String column) {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT count(*) FROM " + src + " WHERE " + column + "::VARCHAR = 'x'")) {
            rs.next();
            return "있음";
        } catch (SQLException e) {
            return "없음 (" + e.getMessage().split("\n")[0].replace("Binder Error: ", "") + ")";
        }
    }
}
