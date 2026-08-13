package dev.tstiering.bench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * DuckDB 연결과 S3 자격증명 설정. 인메모리 DB 로 열고 Parquet 은 S3 에서 직접 읽는다.
 *
 * <p>ADR-0001 에서 Hadoop 을 걷어내며 Java 쪽 Parquet 레코드 읽기를 포기했고,
 * 그 결정은 "쿼리는 DuckDB/Athena 가 맡는다"를 전제로 했다. 여기가 그 전제를 실행하는 지점이다.
 */
public final class DuckDb {

    private DuckDb() {
    }

    /**
     * @param endpoint 호스트:포트. 스킴은 붙이지 않는다 (DuckDB 가 USE_SSL 로 판단한다)
     */
    public static Connection open(String endpoint, String region, String accessKey, String secretKey)
            throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("INSTALL httpfs");
            st.execute("LOAD httpfs");

            // DuckDB 0.10+ 의 방식. 예전 SET s3_* 계열은 폐기 예정이다.
            // MinIO 는 가상호스트 스타일을 쓰기 번거로워 path-style 로 붙는다 (S3ObjectStore 와 동일).
            st.execute("CREATE OR REPLACE SECRET localS3 ("
                    + "TYPE s3,"
                    + " KEY_ID '" + accessKey + "',"
                    + " SECRET '" + secretKey + "',"
                    + " ENDPOINT '" + endpoint + "',"
                    + " REGION '" + region + "',"
                    + " URL_STYLE 'path',"
                    + " USE_SSL false)");
        }
        return conn;
    }

    /** {@code deploy/docker-compose.dev.yml} 이 띄우는 MinIO. */
    public static Connection openLocal() throws SQLException {
        return open("localhost:9000", "ap-northeast-2", "minioadmin", "minioadmin");
    }
}
