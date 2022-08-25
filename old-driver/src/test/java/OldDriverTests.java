import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseDriver;

import java.sql.SQLException;
import java.time.Instant;
import java.util.TimeZone;

public class OldDriverTests {
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
            Assertions.assertEquals(TimeZone.getTimeZone(serverTimeZoneID), conn.getTimeZone()); // use_server_time_zone is set to true by default

            try (var stmt = conn.createStatement()) {
                var rs = stmt.executeQuery(
                    """
                    SELECT formatDateTime(toDateTime('2021-08-13 11:00:00', 'UTC'), '%F %T'),
                           toUnixTimestamp(toDateTime('2021-08-13 11:00:00', 'UTC')),
                           toDateTime('2021-08-13 11:00:00', 'UTC')
                    """
                );
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals("2021-08-13 11:00:00", rs.getString(1));
                Assertions.assertEquals(Instant.parse("2021-08-13T11:00:00Z"), Instant.ofEpochSecond(rs.getLong(2)));
                Assertions.assertEquals(Instant.parse("2021-08-13T11:00:00Z"), rs.getObject(3, Instant.class));
                var ts = rs.getTimestamp(3);
                Assertions.assertEquals(Instant.parse("2021-08-13T11:00:00Z"), ts.toInstant());
            }
        }
    }
}
