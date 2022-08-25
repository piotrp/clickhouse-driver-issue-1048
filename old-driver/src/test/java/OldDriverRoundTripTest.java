import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
    private final static String URL_SERVER_UTC = "jdbc:clickhouse://localhost:28123/";
    private final static String URL_SERVER_POLAND = "jdbc:clickhouse://localhost:38123/";
    private final static String USERNAME = "default";
    private final static String PASSWORD = "";

    static {
        System.out.println(ClickHouseDriver.class.getPackage().getImplementationVersion());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Europe/Warsaw", "UTC"})
    void testReadingFromUtcServer(String jvmTimeZoneID) throws SQLException {
        testReading(jvmTimeZoneID, "UTC", URL_SERVER_UTC);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Europe/Warsaw", "UTC"})
    void testReadingFromPolishServer(String jvmTimeZoneID) throws SQLException {
        testReading(jvmTimeZoneID, "Poland", URL_SERVER_POLAND);
    }

    private void testReading(String jvmTimeZoneID, String serverTimeZoneID, String serverUrl) throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone(jvmTimeZoneID));

        var dataSource = new ClickHouseDataSource(serverUrl);
        try (var conn = dataSource.getConnection(USERNAME, PASSWORD)) {
            Assertions.assertEquals(TimeZone.getTimeZone(serverTimeZoneID), conn.getServerTimeZone());
            Assertions.assertEquals(TimeZone.getTimeZone(jvmTimeZoneID), TimeZone.getDefault());

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
        execSql("DROP TABLE IF EXISTS dates", conn);
        execSql("CREATE TABLE dates (d DateTime) ENGINE=Memory", conn);
    }

    void execSql(String sql, Connection conn) throws SQLException {
        try (var stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}
