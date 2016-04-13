RedShift JDBC Cached Driver
----------

 - [Introduction and How To](#introduction)
 - [Build Instructions](#buildInstructions)
 - [Configuration Parameters](#configurationParameters)
 - [Know Issues](#knowIssues)
 - [TODOs](#todos)
 - [Additional Resources](#additionalResources)
 - [Credits](#credits)

----------
<a id="introduction"></a>
#### Introduction and How To
RedShift JDBC Cached Driver wraps the standard Amazon RedShift JDBC Driver and caches queries results to a Redis cache.
Every time a query is executed, the driver first checks if the result set associated with the query has already been cached in Redis. If this is
true, the driver returns the cached result set and does not interrogate RedShift. If the query has not been cached yet,
the driver runs the query against RedShift and caches the result set into Redis before returning it. The query itself is used
as the key to the cached result set.

At the moment caching only works for `executeQuery` methods on `Statement` and `PreparedStatement`. `CallableStatement` is not
supported since it is mostly used to invoke stored procedures.

The main idea behind this driver wrapper is that in a lot of scenarios data stored in RedShift is read only and is updated
by an ETL process with a pre-defined frequency. In this scenario, it makes
no sense to run expensive queries that aggregate large amount of data into small result sets more then once in between ETL
process executions. This is especially true when aggregated data is used for real time applications. This type of caching
can be easily implemented with ad-hoc code in your own code, but might be hard to deploy when using third party tools. This is
why we have decided to create a JDBC driver wrapper that can be seamlessly used with any Java application.

You should take care of invalidating the values cached in Redis at the appropriate time, usually after the ETL process has finished loading fresh data.
You could also warm up your cache after the ETL process has finished or at run time to have a higher number of cache hits.

In order to use this driver, simply set up a RedShift JDBC connection following the Amazon provided [instructions](http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html)
 and change the following properties:
* URL sub-protocol: `redshiftcached`
* driver class name: `com.powerreviews.jdbc.DriverWrapper`

Below is an example of a JDBC URL to connect using this driver:
```
jdbc:redshiftcached://redshifturl:5439/schemaName?redisUrl=localhost
```
<a id="buildInstructions"></a>
#### Build Instructions and Compiled libraries
Since the Amazon RedShift JDBC Driver is not available on Maven public repositories, you need to deploy the correct version of the driver into your local Maven repo:
```
mvn install:install-file -Dfile=<path-to-file> -DgroupId=com.amazonaws -DartifactId=redshift -Dversion=JDBC41-1.1.10.1010 -Dpackaging=jar
```
After you have installed the Amazon RedShift JDBC Driver locally, simply run:
```
mvn install
```
You will find two JAR files in the target directory, one with dependencies and one without: use the one most appropriate for your environment.

You can also find a pre-compiled version of the driver in the [libs](libs) folder of the project. These JARs have been compiled using Java 7.
<a id="configurationParameters"></a>
#### Configuration Parameters
The following parameters can be passed to the driver as JDBC driver properties or as part of the JDBC driver URL:
 * `redisUrl`: the Redis server URL. If this property is not specified, the driver will not connect to Redis and will not try to cache queries results.
 * `redisPort`: the Redis server port. Optional.
 * `redisPassword`: the Redis connection password. Optional.
 * `redisObjectMaxSizeKB`: if specified, the driver will not cache any result set that is larger than `redisObjectMaxSizeKB` kilobytes.
 * `redisExpiration`: if specified, an expiration time of `redisExpiration` seconds is set for all result sets added to Redis. The expiration of a key is updated every time the key is accessed.
 * `redisIndex`: if specified, the driver will select the specified Redis index after connecting to the cache server. This is like executing `SELECT <ix>` in Redis.
 * `poolValidationQuery`: if the driver is used in a connection pool, then we need to make sure we don't cache the connection validation query. The query specified in this property (e.g. `SELECT 1`) will never be cached.

This is how you can pass properties to the driver using the JDBC URL:
```
jdbc:redshiftcached://redshifturl:5439/schemaName?redisUrl=localhost&redisObjectMaxSizeKB=300&poolValidationQuery=SELECT%201
```
If a parameter is specified both in the JDBC driver properties and in the URL, the value specified in the URL takes precedence.
<a id="knowIssues"></a>
#### Know Issues
* If `?` is used as literal in a `PreparedStatement`, the SQL statement associated to the prepared statement that is used
as the cache key is incorrect. This might not be an issue as long as the key is unique.
* Caching is not supported for complex SQL types such as `CLOB`, `BLOB`, `ROWID`, `ARRAY`, etc. in `PreparedStatement`.
If methods such as `setBlob()`, `setArray()`, etc. are called on a `PreparedStatement`, the statement result will not be cached.
* Caching is not supported for `boolean execute(...)` methods in `Statement` and `PreparedStatement`. Only methods `ResultSet executeQuery(String sql)`
and `ResultSet executeQuery()` are supported at the moment.

<a id="todos"></a>
#### TODOs
* Optimize keys by cleaning up queries before using them as keys. Possibly add string compression for keys.
* Correctly support `setDate`, `setTime` and `setTimestamp` methods with `Calendar` parameter in `PreparedStatement`. At the
moment the provided `Date`/`Time`/`Timestamp` is used directly to compute the cache key instead of using the `Calendar`
to compute the correct value.
* Add ability to specify keys maximum size

<a id="additionalResources"></a>
#### Additional Resources
* [Configure a RedShift JDBC connection](http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html)
 
<a id="credits"></a>
#### Credits
This project makes used of code from the following projects:
* [Boilerplate JDBC Wrapper](https://www.redfin.com/blog/2011/03/boilerplate_jdbc_wrapper.html) by [Michael Smedberg](https://github.com/smedberg)
* [MySQL Connector J](https://github.com/mysql/mysql-connector-j)
