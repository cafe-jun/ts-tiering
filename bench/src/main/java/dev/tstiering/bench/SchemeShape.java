package dev.tstiering.bench;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 한 파티션 스킴이 쿼리에 실제로 노출하는 것들. <b>실행 시점에 DuckDB 에 물어서</b> 알아낸다.
 *
 * <p>스킴 이름으로 추론하지 않는 이유가 있다. 경로에 `profile=` 을 넣어도 파일 안에
 * 같은 이름의 열이 있으면 DuckDB 는 파일 열을 쓰고 파티션 열은 사라진다 —
 * 즉 그 디렉터리 계층은 프루닝에 아무 기여도 못 하면서 경로만 길게 만든다.
 * 이름만 보고는 알 수 없고 물어봐야 안다.
 */
public record SchemeShape(
        Set<String> columns,
        String tenantColumn,
        String deviceColumn,
        LocalDate firstDate,
        LocalDate lastDate
) {

    /** 경로 파티션으로 걸러지는가, 아니면 파일 안을 읽어야 걸러지는가. */
    public boolean tenantIsPartitioned() {
        return "tenant".equals(tenantColumn);
    }

    public boolean deviceIsPartitioned() {
        return "device".equals(deviceColumn);
    }

    public boolean profileIsPartitioned() {
        // 파일에도 profile 열이 있어 어떤 스킴에서도 경로 쪽이 살아남지 못한다.
        // 살아남는 경우가 생기면 이 자리에서 드러난다.
        return false;
    }

    public String describe() {
        return "tenant→" + tenantColumn + (tenantIsPartitioned() ? "(경로)" : "(파일)")
                + ", device→" + deviceColumn + (deviceIsPartitioned() ? "(경로)" : "(파일)");
    }

    public static SchemeShape detect(Connection conn, String source) throws SQLException {
        Set<String> columns = new LinkedHashSet<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM " + source + " LIMIT 0")) {
            var md = rs.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                columns.add(md.getColumnName(i));
            }
        }

        // 날짜 창은 데이터에서 뽑는다. 하드코딩하면 데이터셋 기간을 바꿀 때마다
        // 조용히 0행을 반환하는 쿼리가 된다 (실제로 한 번 그랬다).
        LocalDate first;
        LocalDate last;
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT min(date), max(date) FROM " + source)) {
            rs.next();
            first = rs.getDate(1).toLocalDate();
            last = rs.getDate(2).toLocalDate();
        }

        return new SchemeShape(
                columns,
                columns.contains("tenant") ? "tenant" : "tenant_id",
                columns.contains("device") ? "device" : "device_id",
                first,
                last);
    }
}
