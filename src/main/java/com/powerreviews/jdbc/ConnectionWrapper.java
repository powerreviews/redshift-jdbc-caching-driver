package com.powerreviews.jdbc;

import com.powerreviews.jdbc.redis.RedisClient;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Created by dado on 3/31/16.
 */
public class ConnectionWrapper implements Connection {
    private Connection wrappedConnection;
    private RedisClient redisClient;

    public ConnectionWrapper(Connection wrappedConnection, RedisClient redisClient) {
        this.wrappedConnection = wrappedConnection;
        this.redisClient = redisClient;
    }

    public void setSchema(String schema) throws SQLException {
        wrappedConnection.setSchema(schema);
    }

    public String getSchema() throws SQLException {
        return wrappedConnection.getSchema();
    }

    public void abort(Executor executor) throws SQLException {
        wrappedConnection.abort(executor);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        wrappedConnection.setNetworkTimeout(executor, milliseconds);
    }

    public int getNetworkTimeout() throws SQLException {
        return wrappedConnection.getNetworkTimeout();
    }

    // Semi-copied from http://www.java2s.com/Open-Source/Java-Document/Database-JDBC-Connection-Pool/mysql/com/mysql/jdbc/jdbc2/optional/JDBC4PreparedStatementWrapper.java.htm
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            if (    "java.sql.Connection".equals(iface.getName())
                    || "java.sql.Wrapper.class".equals(iface.getName())) {
                return iface.cast(this);
            }

            return wrappedConnection.unwrap(iface);
        } catch (ClassCastException cce) {
            throw new SQLException("Unable to unwrap to " + iface.toString(), cce);
        }
    }

    public boolean isWrapperFor(Class iface) throws SQLException {
        if (    "java.sql.Connection".equals(iface.getName())
                || "java.sql.Wrapper.class".equals(iface.getName())) {
            return true;
        }
        return wrappedConnection.isWrapperFor(iface);
    }

    public Statement createStatement() throws SQLException {
        return new StatementWrapper(this, wrappedConnection.createStatement(), redisClient);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new PreparedStatementWrapper(this, wrappedConnection.prepareStatement(sql), sql, redisClient);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return wrappedConnection.prepareCall(sql);
    }

    public String nativeSQL(String sql) throws SQLException {
        return wrappedConnection.nativeSQL(sql);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        wrappedConnection.setAutoCommit(autoCommit);
    }

    public boolean getAutoCommit() throws SQLException {
        return wrappedConnection.getAutoCommit();
    }

    public void commit() throws SQLException {
        wrappedConnection.commit();
    }

    public void rollback() throws SQLException {
        wrappedConnection.rollback();
    }

    public void close() throws SQLException {
        wrappedConnection.close();
        redisClient.close();
    }

    public boolean isClosed() throws SQLException {
        return wrappedConnection.isClosed();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return wrappedConnection.getMetaData();
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        wrappedConnection.setReadOnly(readOnly);
    }

    public boolean isReadOnly() throws SQLException {
        return wrappedConnection.isReadOnly();
    }

    public void setCatalog(String catalog) throws SQLException {
        wrappedConnection.setCatalog(catalog);
    }

    public String getCatalog() throws SQLException {
        return wrappedConnection.getCatalog();
    }

    public void setTransactionIsolation(int level) throws SQLException {
        wrappedConnection.setTransactionIsolation(level);
    }

    public int getTransactionIsolation() throws SQLException {
        return wrappedConnection.getTransactionIsolation();
    }

    public SQLWarning getWarnings() throws SQLException {
        return wrappedConnection.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        wrappedConnection.clearWarnings();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new StatementWrapper(this, wrappedConnection.createStatement(resultSetType, resultSetConcurrency), redisClient);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return new PreparedStatementWrapper(this, wrappedConnection.prepareStatement(sql, resultSetType, resultSetConcurrency), sql, redisClient);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency) throws SQLException {
        return wrappedConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return wrappedConnection.getTypeMap();
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        wrappedConnection.setTypeMap(map);
    }

    public void setHoldability(int holdability) throws SQLException {
        wrappedConnection.setHoldability(holdability);
    }

    public int getHoldability() throws SQLException {
        return wrappedConnection.getHoldability();
    }

    public Savepoint setSavepoint() throws SQLException {
        return wrappedConnection.setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return wrappedConnection.setSavepoint(name);
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        wrappedConnection.rollback(savepoint);
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        wrappedConnection.releaseSavepoint(savepoint);
    }

    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return new StatementWrapper(this, wrappedConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), redisClient);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return new PreparedStatementWrapper(this, wrappedConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability), sql, redisClient);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return wrappedConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return new PreparedStatementWrapper(this, wrappedConnection.prepareStatement(sql, autoGeneratedKeys), sql, redisClient);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        return new PreparedStatementWrapper(this, wrappedConnection.prepareStatement(sql, columnIndexes), sql, redisClient);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        return new PreparedStatementWrapper(this, wrappedConnection.prepareStatement(sql, columnNames), sql, redisClient);
    }

    public Clob createClob() throws SQLException {
        return wrappedConnection.createClob();
    }

    public Blob createBlob() throws SQLException {
        return wrappedConnection.createBlob();
    }

    public NClob createNClob() throws SQLException {
        return wrappedConnection.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        return wrappedConnection.createSQLXML();
    }

    public boolean isValid(int timeout) throws SQLException {
        return wrappedConnection.isValid(timeout);
    }

    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        wrappedConnection.setClientInfo(name, value);
    }

    public void setClientInfo(Properties properties)
            throws SQLClientInfoException {
        wrappedConnection.setClientInfo(properties);
    }

    public String getClientInfo(String name) throws SQLException {
        return wrappedConnection.getClientInfo(name);
    }

    public Properties getClientInfo() throws SQLException {
        return wrappedConnection.getClientInfo();
    }

    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        return wrappedConnection.createArrayOf(typeName, elements);
    }

    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
        return wrappedConnection.createStruct(typeName, attributes);
    }
}
