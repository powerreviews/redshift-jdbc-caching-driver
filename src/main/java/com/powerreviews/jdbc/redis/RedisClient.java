package com.powerreviews.jdbc.redis;

import com.sun.rowset.CachedRowSetImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import javax.sql.rowset.CachedRowSet;
import java.io.*;
import java.net.URLDecoder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * Created by dado on 4/1/16.
 */
public class RedisClient {
    final private static Logger log = LogManager.getLogger(RedisClient.class);

    private Jedis jedisClient;
    private String validationQuery;
    private Double redisObjectMaxSize;
    private Integer redisExpiration;
    private Integer redisIndex;

    /**
     * Creates a RedisClient object and initializes a connection with the Redis server
     * @param url The JDBC connection URL
     * @param properties The JDBC connection properties
     */
    public RedisClient(String url, Properties properties, JedisFactory jedisFactory) {
        Properties fullProperties = parseJdbcConnectionURL(url, properties);

        // Get the Redis properties
        String redisUrl = (String)fullProperties.get("redisUrl");
        log.debug("Redis URL: {}", redisUrl);

        Integer redisPort = fullProperties.get("redisPort") != null ?
                            Integer.parseInt((String)fullProperties.get("redisPort")) : null;
        log.debug("Redis Port: {}", redisPort);

        String redisPassword = (String)fullProperties.get("redisPassword");
        log.debug("Redis Password: {}", redisPassword);

        this.redisExpiration = fullProperties.get("redisExpiration") != null ?
                                Integer.parseInt((String)fullProperties.get("redisExpiration")) : null;
        log.debug("Redis Expiration seconds: {}", redisExpiration);

        this.redisIndex = fullProperties.get("redisIndex") != null ?
                Integer.parseInt((String)fullProperties.get("redisIndex")) : null;
        log.debug("Redis Index: {}", redisIndex);

        this.redisObjectMaxSize = fullProperties.get("redisObjectMaxSizeKB") != null ?
                                Double.parseDouble((String)fullProperties.get("redisObjectMaxSizeKB")) : null;
        log.debug("Redis Object Max Size KB: {}", redisObjectMaxSize);

        this.validationQuery = (String)fullProperties.get("poolValidationQuery");
        log.debug("Pool Validation Query: {}", validationQuery);

        // Try to connect to Redis
        try {
            this.jedisClient = jedisFactory.createJedisClient(redisUrl, redisPort);

            if (this.jedisClient != null) {
                //Select the correct index if specified
                if(this.redisIndex != null) {
                    this.jedisClient.select(this.redisIndex);
                }
                // Authenticate if needed
                if(StringUtils.isNotEmpty(redisPassword)) {
                    this.jedisClient.auth(redisPassword);
                }
                // Make sure we are connected to the Redis server
                this.jedisClient.info();
            }
        } catch(JedisConnectionException jce) {
            log.error("Unable to connect to Redis server:");
            log.error(jce);
            this.jedisClient = null;
        }
    }

    /**
     * Executes the provided SQL query if has not been cached in Redis yet and caches the
     * result if possible
     * @param wrappedStatement The wrapped Statement that will run the query if needed
     * @param sql The SQL query to execute
     * @return A ResultSet containing the query result
     * @throws SQLException
     */
    public ResultSet executeQuery(Statement wrappedStatement, String sql) throws SQLException {
        // If this the connection validation query, simply run it
        if(StringUtils.isNotEmpty(this.validationQuery)) {
            if(StringUtils.equalsIgnoreCase(sql, this.validationQuery)) {
                return wrappedStatement.executeQuery(sql);
            }
        }

        // Check if the query has already been cached
        CachedRowSet cachedRowSet = getCachedRowSet(sql);
        if(cachedRowSet != null) {
            return cachedRowSet;
        }

        // If the query has not been cached, execute the query
        ResultSet resultSet = wrappedStatement.executeQuery(sql);

        // Cache the result
        cachedRowSet = cacheRowSet(sql, resultSet);

        return cachedRowSet;
    }

    /**
     * Executes a prepared statement if has not been cached in Redis yet and caches the
     * result if possible
     * @param wrappedStatement The wrapped Statement that will run the query if needed
     * @param sql The SQL query to execute
     * @return A ResultSet containing the query result
     * @throws SQLException
     */
    public ResultSet executeQuery(PreparedStatement wrappedStatement, String sql) throws SQLException {
        // Check if the query has already been cached
        CachedRowSet cachedRowSet = getCachedRowSet(sql);
        if(cachedRowSet != null) {
            return cachedRowSet;
        }

        // If the query has not been cached, execute the query
        ResultSet resultSet = wrappedStatement.executeQuery();

        // Cache the result
        cachedRowSet = cacheRowSet(sql, resultSet);

        return cachedRowSet;
    }

    /**
     * Closes the connection with the Redis server
     */
    public void close() {
        if(this.jedisClient != null && this.jedisClient.isConnected()) {
            try {
                this.jedisClient.close();
            } catch(JedisConnectionException jce) {
                log.error("Unable to close connection with Redis server:");
                log.error(jce);
            }
        }
    }

    /**
     * Checks if a query has been cached in Redis and returns the associated result
     * if present, null otherwise
     * @param sql A SQL query
     * @return A CachedRowSet if the query has already been cached in Redis, null otherwise
     */
    private CachedRowSet getCachedRowSet(String sql) {
        // Don't attempt to look for cached result
        // if we are not connected to Redis or we are running the pool connection validation query
        if(this.jedisClient == null ||
                StringUtils.isEmpty(sql) ||
                StringUtils.equalsIgnoreCase(this.validationQuery, sql)) {
            return null;
        }

        // Verify if the the query has been cached
        byte[] redisResultSet = null;
        try {
            redisResultSet = this.jedisClient.get(sql.getBytes());
            if(redisResultSet != null) {
                // Update the object expiration if specified
                if(this.redisExpiration != null) {
                    this.jedisClient.expire(sql.getBytes(), this.redisExpiration);
                }
            }
        } catch(JedisConnectionException jce) {
            log.error("Error retrieving object from Redis. Key: {}", sql);
            return null;
        }
        if(redisResultSet != null) {
            log.debug("Query result set found in Redis. Key: {}", sql);
            ByteArrayInputStream bis = null;
            ObjectInput in = null;
            try {
                bis = new ByteArrayInputStream(redisResultSet);
                in = new ObjectInputStream(bis);
                return (CachedRowSet) in.readObject();
            } catch (IOException | ClassNotFoundException e) {

            } finally {
                if(bis != null) {
                    try {
                        bis.close();
                    } catch (IOException e) {

                    }
                }
                if(in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {

                    }
                }
            }
        }

        log.debug("Query result set not found in Redis. Key: {}", sql);
        return null;
    }

    /**
     * Tries to cache the result set associated with a SQL query
     * @param sql The SQL query to use as cache key
     * @param resultSet The result set to cache
     * @return A CachedRowSet object containing the ResultSet
     * @throws SQLException
     */
    private CachedRowSet cacheRowSet(String sql, ResultSet resultSet) throws SQLException {
        // Transform the result set and try caching it into Redis
        CachedRowSet cachedRowSet = new CachedRowSetImpl();
        cachedRowSet.populate(resultSet);

        // Don't attempt to cache the result if we are not connected to Redis
        if(this.jedisClient == null ||
                StringUtils.isEmpty(sql)) {
            log.debug("Not connected to Redis: result set will not be cached. Key {}", sql);
            return cachedRowSet;
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(cachedRowSet);
            byte[] redisValue = bos.toByteArray();

            log.debug("Object size: {}KB", ((double)redisValue.length/1024));
            // Only cache the result if no Redis object max size has been specified or
            // the result set size is less than the object size limit
            if(this.redisObjectMaxSize == null ||
                    ((double)redisValue.length/1024) <= this.redisObjectMaxSize) {
                log.debug("Caching object with key \"{}\"", sql);
                this.jedisClient.set(sql.getBytes(), redisValue);
                // Set the object expiration if specified
                if(this.redisExpiration != null) {
                    this.jedisClient.expire(sql.getBytes(), this.redisExpiration);
                }
            } else {
                log.debug("Object not cached because size is too large. Key: {}", sql);
            }
        } catch (IOException e) {
            log.error(e);
            // Nothing to do, return the result set without caching
        } catch (JedisConnectionException jce) {
            log.error("Unable to cache object");
            log.error(jce);
            // Nothing to do, return the result set without caching
        } finally {
            if(out != null) {
                try {
                    out.close();
                } catch (IOException e) {

                }
            }
            try {
                bos.close();
            } catch (IOException e) {

            }
        }
        return cachedRowSet;
    }

    /**
     * Extracts the JDBC connection properties specified in the JDBC connection URL and
     * adds them to the properties specified in the JDBC connection properties object
     * @param url The JDBC connection URL
     * @param defaults The JDBC connection properties
     * @return A set of properties that is the union of the JDBC connection properties and the
     * properties specified in the JDBC connection URL. If a property id specified both in the
     * JDBC properties and in the URL, the value specified in the URL wins.
     */
    private Properties parseJdbcConnectionURL(String url, Properties defaults) {
        Properties urlProps = new Properties();

        if(defaults != null && defaults.size() > 0) {
            for(Map.Entry property : defaults.entrySet()) {
                urlProps.put(property.getKey(), property.getValue());
            }
        }

        /*
         * Parse parameters after the ? in the URL and remove them from the
         * original URL.
         */
        int index = url.indexOf("?");

        if (index != -1) {
            String paramString = url.substring(index + 1, url.length());

            StringTokenizer queryParams = new StringTokenizer(paramString, "&");

            while (queryParams.hasMoreTokens()) {
                String parameterValuePair = queryParams.nextToken();

                String[] valuePairParts = parameterValuePair.split("=");

                String parameter = null;
                String value = null;

                if (valuePairParts.length == 2) {
                    parameter = valuePairParts[0];
                    value = valuePairParts[1];
                }

                if ((value != null && value.length() > 0) && (parameter != null && parameter.length() > 0)) {
                    try {
                        urlProps.setProperty(parameter, URLDecoder.decode(value, "UTF-8"));
                    } catch (UnsupportedEncodingException badEncoding) {
                        // punt
                        urlProps.setProperty(parameter, URLDecoder.decode(value));
                    } catch (NoSuchMethodError nsme) {
                        // punt again
                        urlProps.setProperty(parameter, URLDecoder.decode(value));
                    }
                }
            }
        }

        return urlProps;
    }
}
