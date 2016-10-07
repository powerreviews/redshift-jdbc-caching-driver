package com.powerreviews.jdbc;

import com.powerreviews.jdbc.redis.JedisFactory;
import com.powerreviews.jdbc.redis.RedisClient;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by dado on 3/31/16.
 */
public class DriverWrapper implements Driver {
    // The class of the driver that we're wrapping
    public static final String WRAPPED_DRIVER = "com.amazon.redshift.jdbc42.Driver";

    // The scheme of the driver we're wrapping. For instance, if the JDBC URL for
    // the DB we want to connect to is:
    //        jdbc:redshift://db_server.acme.com/production
    // then this string should be "jdbc:redshift:"
    public static final String WRAPPED_DRIVER_SCHEME = "jdbc:redshift:";

    // The scheme of THIS driver (the wrapper.) Clients use this scheme when they
    // want to use this driver.
    public static final String THIS_DRIVER_SCHEME = "jdbc:redshiftcached:";

    private Driver wrappedDriver;

    static {
        try {
            DriverManager.registerDriver(new DriverWrapper());
        } catch (Exception e) {}
    }

    public DriverWrapper() throws SQLException {
        try {
            wrappedDriver = (Driver) Class.forName(WRAPPED_DRIVER).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public boolean acceptsURL(String url) throws SQLException {
        // Remove our special stuff from the URL
        String fixedUrl = fixupUrl(url);
        // If the fixed URL is the same as the original URL, then it's NOT one of
        // our URLs and we shouldn't handle it.
        if (fixedUrl.equals(url)) {
            return false;
        }

        // Pass the corrected URL to the underlying driver-
        // if the underlying driver can accept the URL, then we can too!
        return wrappedDriver.acceptsURL(fixedUrl);
    }

    public Connection connect(String url, Properties info) throws SQLException {
        // Create the Redis client
        RedisClient redisClient = new RedisClient(url, info, new JedisFactory());

        // Remove our special stuff from the URL
        url = fixupUrl(url);
        // And pass through
        Connection conn = wrappedDriver.connect(url, info);
        return new ConnectionWrapper(conn, redisClient);
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException {
        return wrappedDriver.getPropertyInfo(url, info);
    }

    public int getMajorVersion() {
        return wrappedDriver.getMajorVersion();
    }

    public int getMinorVersion() {
        return wrappedDriver.getMinorVersion();
    }

    public boolean jdbcCompliant() {
        return wrappedDriver.jdbcCompliant();
    }

    private String fixupUrl(String url) {
        if (url.startsWith(THIS_DRIVER_SCHEME)) {
            url = WRAPPED_DRIVER_SCHEME + url.substring(THIS_DRIVER_SCHEME.length());
        }

        return url;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return wrappedDriver.getParentLogger();
    }
}
