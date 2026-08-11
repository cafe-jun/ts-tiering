package dev.tstiering.bench;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** 일회성 진단. 파티션 스킴마다 DuckDB 가 어떤 컬럼을 노출하는지 확인한다. */
public class SchemaProbe {

    public static void main(String[] args) throws Exception {
        String[] prefixes = args.length > 0 ? args : new String[]{"probe-A", "tenant-profile-date"};

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
