package com.powerreviews.jdbc;

import com.powerreviews.jdbc.redis.RedisClient;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import static org.mockito.Mockito.*;

/**
 * Created by dado on 4/8/16.
 */
public class PreparedStatementWrapperTest {
    @Test
    public void testVariables() throws SQLException, MalformedURLException {
        Calendar calendar = Calendar.getInstance();

        Connection connectionMock = mock(Connection.class);
        PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
        RedisClient redisClientMock = mock(RedisClient.class);

        String sql = "? ? something ? ? ? ? ? ? ? ? ? ? ? ? ? ? ?";

        PreparedStatementWrapper preparedStatementWrapper = new PreparedStatementWrapper(connectionMock, preparedStatementMock, sql, redisClientMock);
        preparedStatementWrapper.setNull(1, Types.VARCHAR);
        preparedStatementWrapper.setNull(2, Types.VARCHAR, "typeName");
        preparedStatementWrapper.setBoolean(3, true);
        preparedStatementWrapper.setByte(4, new Byte("10"));
        preparedStatementWrapper.setShort(5, new Short("1"));
        preparedStatementWrapper.setInt(6, 2);
        preparedStatementWrapper.setLong(7, 3L);
        preparedStatementWrapper.setFloat(8, new Float("4.4"));
        preparedStatementWrapper.setDouble(9, new Double("5.5"));
        preparedStatementWrapper.setBigDecimal(10, new BigDecimal("6.6"));
        preparedStatementWrapper.setString(11, "test");
        preparedStatementWrapper.setBytes(12, "bytes".getBytes());
        Date date = new Date(calendar.getTimeInMillis());
        preparedStatementWrapper.setDate(13, date);
        Time time = new Time(calendar.getTimeInMillis());
        preparedStatementWrapper.setTime(14, time);
        Timestamp timestamp = new Timestamp(calendar.getTimeInMillis());
        preparedStatementWrapper.setTimestamp(15, timestamp);
        URL url = new URL("http://www.powerreviews.com");
        preparedStatementWrapper.setURL(16, url);
        preparedStatementWrapper.setNString(17, "nstring");

        preparedStatementWrapper.executeQuery();

        StringBuilder expectedSql = new StringBuilder("null null something true 10 1 2 3 4.4 5.5 6.6 test bytes ")
                .append(date.toString())
                .append(" ")
                .append(time.toString())
                .append(" ")
                .append(timestamp.toString())
                .append(" ")
                .append("http://www.powerreviews.com")
                .append(" ")
                .append("nstring");
        verify(redisClientMock).executeQuery(preparedStatementMock, expectedSql.toString());
    }

    @Test
    public void testNonCacheable() throws SQLException {
        Connection connectionMock = mock(Connection.class);
        PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
        RedisClient redisClientMock = mock(RedisClient.class);

        String sql = "select something from somewhere where foo = ?";

        PreparedStatementWrapper preparedStatementWrapper = new PreparedStatementWrapper(connectionMock, preparedStatementMock, sql, redisClientMock);
        preparedStatementWrapper.setAsciiStream(1, new ByteArrayInputStream("test".getBytes()));
        preparedStatementWrapper.executeQuery();

        verify(redisClientMock).executeQuery(preparedStatementMock, null);
    }
}
