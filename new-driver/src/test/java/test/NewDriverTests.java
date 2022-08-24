package test;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseDriver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import java.util.TimeZone;

public class NewDriverTests {
    private final static String URL = "jdbc:clickhouse://localhost:28123/";
    private final static String USERNAME = "default";
    private final static String PASSWORD = "";

    static {
        System.out.println(ClickHouseDriver.class.getPackage().getImplementationVersion());
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Warsaw"));
    }

    @Test
    void testReading() throws SQLException {
        var dataSource = new ClickHouseDataSource(URL);
        try (var conn = dataSource.getConnection(USERNAME, PASSWORD)) {
            prepareTestTable(conn);

            execSql("INSERT INTO dates(d) VALUES (toDateTime(toUnixTimestamp('2021-08-13 11:00:00', 'UTC'), 'UTC'))", conn);

            try (var stmt = conn.createStatement()) {
                var rs = stmt.executeQuery("SELECT formatDateTime(d, '%F %T'), toUnixTimestamp(d), d FROM dates");
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals("2021-08-13 11:00:00", rs.getString(1));
                Assertions.assertEquals(Instant.parse("2021-08-13T11:00:00Z"), Instant.ofEpochSecond(rs.getLong(2)));
                var ts = rs.getTimestamp(3);
                // Assertion below fails with:
                // Expected :2021-08-13T11:00:00Z
                // Actual   :2021-08-13T09:00:00Z
                // looks like UTC date from server was treated as if it was in JVM TZ (Europe/Warsaw = UTC+2)
                Assertions.assertEquals(Instant.parse("2021-08-13T11:00:00Z"), ts.toInstant());
            }
        }
    }

    void prepareTestTable(ClickHouseConnection conn) throws SQLException {
        Assertions.assertEquals(TimeZone.getTimeZone("UTC"), conn.getServerTimeZone());
        Assertions.assertEquals(TimeZone.getTimeZone("Europe/Warsaw"), conn.getJvmTimeZone());
        Assertions.assertEquals(Optional.empty(), conn.getEffectiveTimeZone());

        execSql("DROP TABLE IF EXISTS dates", conn);
        execSql("CREATE TABLE dates (d DateTime) ENGINE=Memory", conn);
    }

    void execSql(String sql, Connection conn) throws SQLException {
        try (var stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}
