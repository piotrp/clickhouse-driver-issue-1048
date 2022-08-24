import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.TimeZone;

public class OldDriverTests {
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
                Assertions.assertEquals(Instant.parse("2021-08-13T11:00:00Z"), ts.toInstant());
            }
        }
    }

    void prepareTestTable(ClickHouseConnection conn) throws SQLException {
        Assertions.assertEquals(TimeZone.getTimeZone("UTC"), conn.getServerTimeZone());
        Assertions.assertEquals(TimeZone.getTimeZone("Europe/Warsaw"), TimeZone.getDefault());

        execSql("DROP TABLE IF EXISTS dates", conn);
        execSql("CREATE TABLE dates (d DateTime) ENGINE=Memory", conn);
    }

    void execSql(String sql, Connection conn) throws SQLException {
        try (var stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}
