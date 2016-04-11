package com.powerreviews.jdbc.redis;

import static org.junit.Assert.*;
import com.mockrunner.mock.jdbc.MockResultSet;
import com.sun.rowset.CachedRowSetImpl;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import javax.sql.rowset.CachedRowSet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.mockito.Mockito.*;

/**
 * Created by dado on 4/7/16.
 */
public class RedisClientTest {

    @Test
    public void testConstructorNoHost() {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb";

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(null);

        new RedisClient(jdbcUrl, null, jedisFactoryMock);

        verify(jedisFactoryMock).createJedisClient(null, null);
    }

    @Test
    public void testConstructorHost() {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHost";

        Jedis jedisClientMock = mock(Jedis.class);

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        new RedisClient(jdbcUrl, null, jedisFactoryMock);

        verify(jedisFactoryMock).createJedisClient("redisHost", null);
        verify(jedisClientMock, never()).auth(anyString());
        verify(jedisClientMock).info();
    }

    @Test
    public void testConstructorHostPort() {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHost&redisPort=1234";

        Jedis jedisClientMock = mock(Jedis.class);

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        new RedisClient(jdbcUrl, null, jedisFactoryMock);

        verify(jedisFactoryMock).createJedisClient("redisHost", 1234);
        verify(jedisClientMock, never()).auth(anyString());
        verify(jedisClientMock).info();
    }

    @Test
    public void testConstructorHostPortAuth() {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisPassword=test&redisUrl=redisHost&redisPort=1234";

        Jedis jedisClientMock = mock(Jedis.class);

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        new RedisClient(jdbcUrl, null, jedisFactoryMock);

        verify(jedisFactoryMock).createJedisClient("redisHost", 1234);
        verify(jedisClientMock).auth("test");
        verify(jedisClientMock).info();
    }

    @Test
    public void testConstructorHostUrlProperties() {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHostURL";

        Jedis jedisClientMock = mock(Jedis.class);

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        Properties properties = new Properties();
        properties.put("redisUrl", "redisHostPROPERTIES");
        properties.put("redisPort", "1234");

        new RedisClient(jdbcUrl, properties, jedisFactoryMock);

        verify(jedisFactoryMock).createJedisClient("redisHostURL", 1234);
        verify(jedisClientMock, never()).auth(anyString());
        verify(jedisClientMock).info();
    }

    @Test
    public void testExecuteNonCachedQueryStatement() throws SQLException {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHost";
        String sql = "select * from test";

        Jedis jedisClientMock = mock(Jedis.class);
        when(jedisClientMock.get(sql.getBytes())).thenReturn(null);

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        MockResultSet resultSetMock = new MockResultSet("myMockRS");
        resultSetMock.addColumn("columnA", new Integer[]{1, 2});
        resultSetMock.addColumn("columnB", new String[]{"record1", "record2"});

        Statement statementMock = mock(Statement.class);
        when(statementMock.executeQuery(sql)).thenReturn(resultSetMock);

        RedisClient redisClient = new RedisClient(jdbcUrl, null, jedisFactoryMock);
        ResultSet result = redisClient.executeQuery(statementMock, sql);

        verify(statementMock).executeQuery(sql);
        verify(jedisClientMock).set(any(byte[].class), any(byte[].class));
        assertNotNull(result);

        int rowNumber = 1;
        while(result.next()) {
            assertEquals(rowNumber, result.getInt(1));
            assertEquals("record" + rowNumber, result.getString(2));
            rowNumber++;
        }
    }

    @Test
    public void testExecuteCachedQueryStatement() throws SQLException, IOException {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHost";
        String sql = "select * from test";

        MockResultSet resultSetMock = new MockResultSet("myMockRS");
        resultSetMock.addColumn("columnA", new Integer[]{1, 2});
        resultSetMock.addColumn("columnB", new String[]{"record1", "record2"});

        CachedRowSet cachedRowSet = new CachedRowSetImpl();
        cachedRowSet.populate(resultSetMock);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(cachedRowSet);

        Jedis jedisClientMock = mock(Jedis.class);
        when(jedisClientMock.get(sql.getBytes())).thenReturn(bos.toByteArray());

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        Statement statementMock = mock(Statement.class);

        RedisClient redisClient = new RedisClient(jdbcUrl, null, jedisFactoryMock);
        ResultSet result = redisClient.executeQuery(statementMock, sql);

        verify(statementMock, never()).executeQuery(anyString());
        verify(jedisClientMock, never()).set(any(byte[].class), any(byte[].class));
        assertNotNull(result);

        int rowNumber = 1;
        while(result.next()) {
            assertEquals(rowNumber, result.getInt(1));
            assertEquals("record" + rowNumber, result.getString(2));
            rowNumber++;
        }
    }

    @Test
    public void testExecuteNonCachedQueryStatementSizeLimit() throws SQLException {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHost&redisObjectMaxSizeKB=10";
        String sql = "select * from test";

        Jedis jedisClientMock = mock(Jedis.class);
        when(jedisClientMock.get(sql.getBytes())).thenReturn(null);

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        MockResultSet resultSetMock = new MockResultSet("myMockRS");
        Integer[] columnA = new Integer[100];
        String[] columnB = new String[100];
        for(int i = 0; i < 100; i++) {
            columnA[i] = i + 1;
            columnB[i] = "record" + (i + 1);
        }
        resultSetMock.addColumn("columnA", columnA);
        resultSetMock.addColumn("columnB", columnB);

        Statement statementMock = mock(Statement.class);
        when(statementMock.executeQuery(sql)).thenReturn(resultSetMock);

        RedisClient redisClient = new RedisClient(jdbcUrl, null, jedisFactoryMock);
        ResultSet result = redisClient.executeQuery(statementMock, sql);

        verify(statementMock).executeQuery(sql);
        verify(jedisClientMock, never()).set(any(byte[].class), any(byte[].class));
        assertNotNull(result);
    }

    @Test
    public void testExecuteValidationQuery() throws SQLException {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHost&poolValidationQuery=SELECT%201";
        String sql = "select 1";

        Jedis jedisClientMock = mock(Jedis.class);
        when(jedisClientMock.get(sql.getBytes())).thenReturn(null);

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        MockResultSet resultSetMock = new MockResultSet("myMockRS");
        resultSetMock.addColumn("columnA", new Integer[]{1});

        Statement statementMock = mock(Statement.class);
        when(statementMock.executeQuery(sql)).thenReturn(resultSetMock);

        RedisClient redisClient = new RedisClient(jdbcUrl, null, jedisFactoryMock);
        ResultSet result = redisClient.executeQuery(statementMock, sql);

        verify(statementMock).executeQuery(sql);
        verify(jedisClientMock, never()).set(any(byte[].class), any(byte[].class));
        assertNotNull(result);

        result.next();
        assertEquals(1, result.getInt(1));
    }

    @Test
    public void testExecuteNoRedisConnection() throws SQLException {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHost";
        String sql = "select * from test";

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(null);

        MockResultSet resultSetMock = new MockResultSet("myMockRS");
        resultSetMock.addColumn("columnA", new Integer[]{1, 2});
        resultSetMock.addColumn("columnB", new String[]{"record1", "record2"});

        Statement statementMock = mock(Statement.class);
        when(statementMock.executeQuery(sql)).thenReturn(resultSetMock);

        RedisClient redisClient = new RedisClient(jdbcUrl, null, jedisFactoryMock);
        ResultSet result = redisClient.executeQuery(statementMock, sql);

        verify(statementMock).executeQuery(sql);
        assertNotNull(result);

        int rowNumber = 1;
        while(result.next()) {
            assertEquals(rowNumber, result.getInt(1));
            assertEquals("record" + rowNumber, result.getString(2));
            rowNumber++;
        }
    }

    @Test
    public void testExecuteRedisGetConnectionIssue() throws SQLException {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHost";
        String sql = "select * from test";

        Jedis jedisClientMock = mock(Jedis.class);

        when(jedisClientMock.get(sql.getBytes())).thenThrow(new JedisConnectionException(""));

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        MockResultSet resultSetMock = new MockResultSet("myMockRS");
        resultSetMock.addColumn("columnA", new Integer[]{1, 2});
        resultSetMock.addColumn("columnB", new String[]{"record1", "record2"});

        Statement statementMock = mock(Statement.class);
        when(statementMock.executeQuery(sql)).thenReturn(resultSetMock);

        RedisClient redisClient = new RedisClient(jdbcUrl, null, jedisFactoryMock);
        ResultSet result = redisClient.executeQuery(statementMock, sql);

        verify(statementMock).executeQuery(sql);
        verify(jedisClientMock).set(any(byte[].class), any(byte[].class));
        assertNotNull(result);

        int rowNumber = 1;
        while(result.next()) {
            assertEquals(rowNumber, result.getInt(1));
            assertEquals("record" + rowNumber, result.getString(2));
            rowNumber++;
        }
    }

    @Test
    public void testExecuteRedisSetConnectionIssue() throws SQLException {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHost";
        String sql = "select * from test";

        Jedis jedisClientMock = mock(Jedis.class);
        when(jedisClientMock.get(sql.getBytes())).thenReturn(null);
        when(jedisClientMock.set(any(byte[].class), any(byte[].class))).thenThrow(new JedisConnectionException(""));

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        MockResultSet resultSetMock = new MockResultSet("myMockRS");
        resultSetMock.addColumn("columnA", new Integer[]{1, 2});
        resultSetMock.addColumn("columnB", new String[]{"record1", "record2"});

        Statement statementMock = mock(Statement.class);
        when(statementMock.executeQuery(sql)).thenReturn(resultSetMock);

        RedisClient redisClient = new RedisClient(jdbcUrl, null, jedisFactoryMock);
        ResultSet result = redisClient.executeQuery(statementMock, sql);

        verify(statementMock).executeQuery(sql);
        verify(jedisClientMock).set(any(byte[].class), any(byte[].class));
        assertNotNull(result);

        int rowNumber = 1;
        while(result.next()) {
            assertEquals(rowNumber, result.getInt(1));
            assertEquals("record" + rowNumber, result.getString(2));
            rowNumber++;
        }
    }

    @Test
    public void testExecuteNonCachedPreparedStatement() throws SQLException {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHost";
        String sql = "select * from test";

        Jedis jedisClientMock = mock(Jedis.class);
        when(jedisClientMock.get(sql.getBytes())).thenReturn(null);

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        MockResultSet resultSetMock = new MockResultSet("myMockRS");
        resultSetMock.addColumn("columnA", new Integer[]{1, 2});
        resultSetMock.addColumn("columnB", new String[]{"record1", "record2"});

        PreparedStatement statementMock = mock(PreparedStatement.class);
        when(statementMock.executeQuery()).thenReturn(resultSetMock);

        RedisClient redisClient = new RedisClient(jdbcUrl, null, jedisFactoryMock);
        ResultSet result = redisClient.executeQuery(statementMock, sql);

        verify(statementMock).executeQuery();
        verify(jedisClientMock).set(any(byte[].class), any(byte[].class));
        assertNotNull(result);

        int rowNumber = 1;
        while(result.next()) {
            assertEquals(rowNumber, result.getInt(1));
            assertEquals("record" + rowNumber, result.getString(2));
            rowNumber++;
        }
    }

    @Test
    public void testExecuteCachedPreparedStatement() throws SQLException, IOException {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHost";
        String sql = "select * from test";

        MockResultSet resultSetMock = new MockResultSet("myMockRS");
        resultSetMock.addColumn("columnA", new Integer[]{1, 2});
        resultSetMock.addColumn("columnB", new String[]{"record1", "record2"});

        CachedRowSet cachedRowSet = new CachedRowSetImpl();
        cachedRowSet.populate(resultSetMock);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(cachedRowSet);

        Jedis jedisClientMock = mock(Jedis.class);
        when(jedisClientMock.get(sql.getBytes())).thenReturn(bos.toByteArray());

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        PreparedStatement statementMock = mock(PreparedStatement.class);

        RedisClient redisClient = new RedisClient(jdbcUrl, null, jedisFactoryMock);
        ResultSet result = redisClient.executeQuery(statementMock, sql);

        verify(statementMock, never()).executeQuery(anyString());
        verify(jedisClientMock, never()).set(any(byte[].class), any(byte[].class));
        assertNotNull(result);

        int rowNumber = 1;
        while(result.next()) {
            assertEquals(rowNumber, result.getInt(1));
            assertEquals("record" + rowNumber, result.getString(2));
            rowNumber++;
        }
    }

    @Test
    public void testCloseConnected() {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHost";

        Jedis jedisClientMock = mock(Jedis.class);
        when(jedisClientMock.isConnected()).thenReturn(true);

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        RedisClient redisClient = new RedisClient(jdbcUrl, null, jedisFactoryMock);
        redisClient.close();

        verify(jedisClientMock).close();
    }

    @Test
    public void testCloseNotConnected() {
        String jdbcUrl = "jdbc:redshiftcached://redshiftHost:redshiftPort/redshiftDb?redisUrl=redisHost";

        Jedis jedisClientMock = mock(Jedis.class);
        when(jedisClientMock.isConnected()).thenReturn(false);

        JedisFactory jedisFactoryMock = mock(JedisFactory.class);
        when(jedisFactoryMock.createJedisClient(anyString(), anyInt())).thenReturn(jedisClientMock);

        RedisClient redisClient = new RedisClient(jdbcUrl, null, jedisFactoryMock);
        redisClient.close();

        verify(jedisClientMock, never()).close();
    }
}
