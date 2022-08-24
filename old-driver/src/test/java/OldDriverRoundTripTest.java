import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;

public class OldDriverRoundTripTest {
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

            var timestamp = Timestamp.from(Instant.now().truncatedTo(ChronoUnit.SECONDS));
            System.out.println(timestamp);

            try (var stmt = conn.prepareStatement("INSERT INTO dates(d) VALUES (?)")) {
                stmt.setTimestamp(1, timestamp);
                stmt.executeUpdate();
            }

            try (var stmt = conn.createStatement()) {
                var rs = stmt.executeQuery("SELECT d FROM dates");
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals(timestamp, rs.getTimestamp(1));
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
