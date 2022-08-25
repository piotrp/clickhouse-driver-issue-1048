package test;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseDriver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;

public class NewDriverTests {
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
            Assertions.assertEquals(TimeZone.getTimeZone(jvmTimeZoneID), conn.getJvmTimeZone());
            Assertions.assertEquals(TimeZone.getTimeZone(serverTimeZoneID), conn.getServerTimeZone());
            // bug: use_server_time_zone is ignored
            Assertions.assertEquals(Optional.empty(), conn.getEffectiveTimeZone());

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
                // Assertion below fails for non-UTC time zone with:
                // Expected :2021-08-13T11:00:00Z
                // Actual   :2021-08-13T09:00:00Z
                // looks like UTC date from server was treated as if it was in JVM TZ (Europe/Warsaw = UTC+2)
                Assertions.assertEquals(Instant.parse("2021-08-13T11:00:00Z"), ts.toInstant());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"Europe/Warsaw", "UTC"})
    void testReadingFromUtcServerWithForcedUtc(String jvmTimeZoneID) throws SQLException {
        testReadingWithForcedUtc(jvmTimeZoneID, "UTC", URL_SERVER_UTC);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Europe/Warsaw", "UTC"})
    void testReadingFromPolishServerWithForcedUtc(String jvmTimeZoneID) throws SQLException {
        testReadingWithForcedUtc(jvmTimeZoneID, "Poland", URL_SERVER_POLAND);
    }

    private void testReadingWithForcedUtc(String jvmTimeZoneID, String serverTimeZoneID, String serverUrl) throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone(jvmTimeZoneID));
        var props = new Properties();
        props.setProperty("use_time_zone", jvmTimeZoneID);
        props.setProperty("use_server_time_zone", "false");
        var dataSource = new ClickHouseDataSource(serverUrl, props);
        try (var conn = dataSource.getConnection(USERNAME, PASSWORD)) {
            Assertions.assertEquals(TimeZone.getTimeZone(jvmTimeZoneID), conn.getJvmTimeZone());
            Assertions.assertEquals(TimeZone.getTimeZone(serverTimeZoneID), conn.getServerTimeZone());
            Assertions.assertEquals(Optional.of(TimeZone.getTimeZone(jvmTimeZoneID)), conn.getEffectiveTimeZone());

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
                // Assertion below fails for non-UTC time zone with:
                // Expected :2021-08-13T11:00:00Z
                // Actual   :2021-08-13T09:00:00Z
                // looks like UTC date from server was treated as if it was in JVM TZ (Europe/Warsaw = UTC+2)
                Assertions.assertEquals(Instant.parse("2021-08-13T11:00:00Z"), ts.toInstant());
            }
        }
    }
}
