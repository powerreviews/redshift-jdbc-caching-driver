# redshift-jdbc-caching-driver — RepoDocs
_Generated on 2026-05-11 · Consolidated on 2026-05-19 against commit 8c90913_

## Summary

### Overview
The `redshift-jdbc-caching-driver` is a small in-house JDBC driver shim (`com.powerreviews:redshift-jdbc-cached-driver:0.3`) that wraps the Amazon Redshift JDBC42 driver and caches `executeQuery` result sets in Redis keyed by the rendered SQL text — built circa 2016 to relieve repeated expensive aggregation queries on Redshift between ETL refresh windows, with cache invalidation explicitly left to the calling ETL. It is a leaf library: no org repo declares it as a Maven dependency in the org-summary dependency map, and the org has migrated analytics from Redshift toward Snowflake (the warehouse used by `analytics-dbt`, `analytics-dbt-loader`, `data-science-analytics`, `pwr-streamlit`, `pwr-tableau`, etc.), leaving this driver as an orphaned, dormant artifact distributed by checking pre-built JARs into `libs/1.7/` and `libs/1.8/` rather than via Nexus.

### Tech Stack
| Category | Technology | Version |
|----------|-----------|---------|
| Language | Java | 1.8 source/target |
| Framework | JDBC SPI (`java.sql.Driver`) | JDBC 4.2 |
| Database | Amazon Redshift JDBC Driver (bundled JAR; not on Maven Central) | JDBC42-1.1.17.1017 |
| Database | Redis (Jedis client) | Jedis 2.8.0 |
| Build Tool | Apache Maven (assembly + install-file pre-clean) | maven-assembly-plugin, maven-install-plugin 2.5.2, maven-compiler-plugin 3.3 |
| CI/CD | GitHub Actions (TruffleHog secrets scan only — no build/publish pipeline) | scheduled cron `0 14 * * 1-5` |
| Cloud/Infra | AWS Redshift + AWS ElastiCache Redis (assumed deployment target; not configured in repo) | n/a |

### Consumers
| Consumer | Type | How They Use It |
|----------|------|----------------|
| _No org repo declares a Maven dep on `com.powerreviews:redshift-jdbc-cached-driver`_ | n/a | Not referenced by `parent-pom`, `pwr-spring-parent-bom`, `analytics-etl`, `data-mover-utilities`, `data-science-analytics`, `pwr-tableau`, `analytics-utilities`, `reporting`, `database`, or any other Redshift-touching repo per the org dependency map. The JAR is distributed by manual download from `libs/` and so consumers would be undeclared. |
| TruffleHog (`edplato/trufflehog-actions-scan`) + Slack `github-token-scan` channel | CI System / Comms | Scheduled weekday secrets scan; alerts on failure. |
| Mend/WhiteSource (`.whitesource`) | CI System | Org-wide vulnerability scanning via the policy in `whitesource-config`. |

### Dependencies on Org Repos
| Repo | Reason |
|------|--------|
| _none_ | `pom.xml` has no `com.powerreviews` deps; the project does not inherit from `parent-pom` or `pwr-spring-parent-bom` (predates both as a convention here), does not use `pwr-commons`/`pwr-java-utils`, does not consume `pwr-event-logs`, and is not deployed via `pwr-docker-service`/`pwr-service-deploy-orb`. The only org-level integration is the shared `whitesource-config` policy. |

### External Integrations
| Service | Purpose | Integration Type |
|---------|---------|-----------------|
| Amazon Redshift | Target database whose query results are cached; reached via the wrapped `com.amazon.redshift.jdbc42.Driver` | SDK (JDBC driver) |
| Redis | Cache backing store for serialized `CachedRowSet` objects, keyed by SQL string | SDK (Jedis client) |
| Slack (via `rtCamp/action-slack-notify`) | Posts secret-scan failure alerts | Webhook (outbound) |
| TruffleHog (`edplato/trufflehog-actions-scan`) | Secrets detection in CI | SDK (GitHub Action) |
| WhiteSource / Mend | Dependency vulnerability scanning config (`.whitesource` file added per git log) | SDK (config file) |

### Async & Scheduled Work
| Channel / Job | Type | Direction | Purpose |
|--------------|------|-----------|---------|
| GitHub Actions `secret-scan` | Scheduled job (cron `0 14 * * 1-5`) | N/A | Daily weekday TruffleHog scan of the repo for leaked secrets |

### Upgrade Alerts
| Dependency | Current Version | Issue | Severity |
|-----------|----------------|-------|----------|
| Amazon Redshift JDBC Driver | JDBC42-1.1.17.1017 (2016) | EOL; numerous CVEs fixed in later AWS releases; not on Maven Central, sideloaded from `libs/` | Critical |
| Jedis | 2.8.0 (2016) | Major-version EOL; CVE-relevant fixes accrued through 5.x; the rest of the org's Redis-using Ruby/Java stack has moved well past this | Critical |
| `com.sun.rowset.CachedRowSetImpl` (internal JDK API used in `RedisClient`) | n/a | Internal `com.sun.*` API removed/relocated in JDK 9+; will not run on a modern JDK without `--add-opens` flags. Hard blocker for any consumer adopting Java 11/17/21. | Critical |
| Java target | 1.8 | Oracle public-update EOL passed; most active org services target 11/17 | Severe |
| Apache Commons Lang 3 | 3.4 (2015) | Well behind current 3.x | Severe |
| Logback (classic/core) | 1.1.7 (Dependabot has open bumps per recent activity) | CVE-2023-6378 / CVE-2024-12798 fixed in 1.2.13+/1.5.x | Severe |
| Mockito | 1.10.19 (test scope) | Mockito 1.x abandoned | Severe |
| MockRunner-JDBC | 1.1.1 (test scope) | Long unmaintained | Severe |
| `actions/checkout@master` in `secrets-scan.yml` | floating ref | Mutable ref — supply-chain risk for GitHub Actions, contradicts the SHA-pinning pattern used elsewhere in the org | Severe |

### Coupling Profile
| Dependency | Protocol | Frequency Pattern | Failure Mode |
|-----------|----------|-------------------|--------------|
| Amazon Redshift (via wrapped `com.amazon.redshift.jdbc42.Driver`) | sync JDBC (Postgres wire) | per-request (every `executeQuery` not satisfied from cache) | hard — `connect()` and uncached `executeQuery` propagate `SQLException` to the caller without retry |
| Redis (via Jedis 2.8 — `RedisClient.java`) | sync TCP (Jedis SDK) | per-request (`GET` on lookup, `SET[EX]` on miss, `EXPIRE` refresh on hit) | soft — `RedisClient` catches `JedisConnectionException` on construction and on every op, logs, and silently falls through to direct Redshift execution; an unreachable Redis degrades into pass-through with no circuit breaker |
| Slack `github-token-scan` (CI) | webhook (outbound, `rtCamp/action-slack-notify`) | event-triggered (workflow failure) | soft — notification only |
| TruffleHog scan | GitHub Action runtime | scheduled (cron `0 14 * * 1-5`, weekdays 14:00 UTC) | soft — failure pings Slack, no enforcement |
| Mend/WhiteSource (`.whitesource`) | GitHub App scan | scheduled (org policy) | soft |

### Architectural Notes
- **Shared infrastructure**: This is one of seven repos the org-summary attributes to Redshift (`analytics-etl`, `analytics-utilities`, `data-mover-utilities`, `data-science-analytics`, `database`, `pwr-tableau`, `redshift-jdbc-caching-driver`). It is also conceptually a client of the shared ElastiCache Redis fleet that `pwr-redis` / `elasticache-infrastructure` provision — though this repo carries zero Terraform/Ansible and ships no deployment, so any actual Redis endpoint is supplied by whichever (undeclared) consumer embeds the JAR. The driver's design — opaque SQL-text key, no namespacing, no eviction policy beyond a per-key TTL — would not be safe to point at a shared multi-tenant Redis without a dedicated DB index (`redisIndex`) per consumer; nothing in the repo or org context confirms whether such isolation exists in production.
- **Bounded-context overlaps**: None. The driver has no domain model — it serializes opaque `CachedRowSetImpl` byte arrays. No overlap with the org's UGC/Review/Order/Merchant bounded contexts surfaced in `pwr-data-model`, `core-data-services`, `write-services`, or `enterprise-services`.
- **Architectural evolution**: The repo is effectively frozen — the last functional commit was `46f976a` "Modified logging system to Logback" in **January 2017** (~9 years ago); since then activity is limited to README tweaks (2018-2020), a TruffleHog workflow (2020), Dependabot logback/junit bumps, and a `.whitesource` file added 2026-03-23. Meanwhile the org has migrated its analytics warehouse from Redshift to Snowflake (visible in `analytics-dbt*`, `pwr-terraform-dms` DMS pipelines feeding Snowflake RAW, `analytics-etl`'s Snowflake sinks, `pwr-streamlit`), which strongly suggests this driver's original use case — caching Redshift aggregations between ETL refreshes — has been displaced by Snowflake-native materializations and Tableau extracts. The repo also predates the org's standardization on `parent-pom`/`pwr-spring-parent-bom`, `pwr-docker-service`, `pwr-nexus` artifact distribution, and `pwr-service-deploy-orb` deploys; nothing has been done to retrofit it. Given the JDK 9+ incompatibility of `com.sun.rowset.CachedRowSetImpl` plus the lack of any declared consumers in the org dependency map, this repo is a strong candidate for the "audit and prune dormant repos" priority called out in the org health score.

## API Reference

This is a JDBC driver library, not a service. The public surface is the standard `java.sql.Driver` SPI plus driver URL/property parameters.

### Driver registration
- **Driver class**: `com.powerreviews.jdbc.DriverWrapper`
- **URL scheme**: `jdbc:redshiftcached:` (rewritten internally to `jdbc:redshift:` and delegated to `com.amazon.redshift.jdbc42.Driver`)
- **Self-registers** via `static { DriverManager.registerDriver(new DriverWrapper()); }`

### `com.powerreviews.jdbc.DriverWrapper` (implements `java.sql.Driver`)
| Method | Signature | Behavior |
|---|---|---|
| `acceptsURL` | `boolean acceptsURL(String url)` | Returns true only for URLs starting with `jdbc:redshiftcached:` that the underlying Redshift driver also accepts after rewrite. |
| `connect` | `Connection connect(String url, Properties info)` | Builds `RedisClient(url, info, new JedisFactory())`, rewrites URL, delegates to Redshift driver, returns `ConnectionWrapper`. |
| `getPropertyInfo` | `DriverPropertyInfo[] getPropertyInfo(String, Properties)` | Delegated to wrapped driver. |
| `getMajorVersion` / `getMinorVersion` / `jdbcCompliant` / `getParentLogger` | standard JDBC | Delegated to wrapped driver. |

### `com.powerreviews.jdbc.ConnectionWrapper` (implements `java.sql.Connection`)
- Delegates all `Connection` methods to the wrapped Redshift `Connection`.
- Returns `StatementWrapper` from `createStatement(...)` and `PreparedStatementWrapper` from `prepareStatement(...)`, each given the shared `RedisClient`.
- `close()` closes both the underlying connection and the Redis client.

### `com.powerreviews.jdbc.StatementWrapper` (implements `java.sql.Statement`)
- `ResultSet executeQuery(String sql)` — delegates to `RedisClient.executeQuery(wrappedStatement, sql)` (cached path).
- All other `execute*` / `executeUpdate` / batch methods — pass through to wrapped statement (NOT cached).

### `com.powerreviews.jdbc.PreparedStatementWrapper` (implements `java.sql.PreparedStatement`)
- Holds `String statementSql` (tokenized via `SqlUtil.tokenizeStatement` — replaces `?` placeholders with `{0}`, `{1}`, …) and `SortedMap<Integer, Object> variables`.
- `setXxx(int parameterIndex, ...)` methods record bound parameters in `variables` AND delegate to the wrapped statement. The effective cache key is the tokenized SQL with bound values substituted via `MessageFormat`.
- `setBlob`, `setArray`, `setClob`, `setNClob`, `setRowId`, `setSQLXML`, etc. — set `cacheable = false`; the result will not be cached.
- `ResultSet executeQuery()` — if `cacheable`, delegates to `RedisClient.executeQuery(this, formattedSql)`; otherwise executes against the wrapped statement directly.

### `com.powerreviews.jdbc.redis.RedisClient`
- `RedisClient(String url, Properties properties, JedisFactory factory)` — parses URL/properties for `redisUrl`, `redisPort`, `redisPassword`, `redisExpiration`, `redisIndex`, `redisObjectMaxSizeKB`, `poolValidationQuery`; connects to Redis (silently degrades to no-cache if connection fails).
- `ResultSet executeQuery(Statement wrappedStatement, String sql)` — cache lookup; on miss runs query and caches result.
- `ResultSet executeQuery(PreparedStatement wrappedStatement, String sql)` — same, but invokes parameterless `executeQuery()` on the wrapped prepared statement.
- `void close()` — closes Jedis client.
- Cache encoding: `CachedRowSetImpl.populate(resultSet)` serialized via `ObjectOutputStream` → `byte[]` stored under SQL string key.

### `com.powerreviews.jdbc.redis.JedisFactory`
- `Jedis createJedisClient(String url, Integer port)` — returns `null` if URL empty; otherwise `new Jedis(url[, port])`.

### `com.powerreviews.jdbc.util.SqlUtil`
- `static String tokenizeStatement(String statement)` — replaces each `?` with `{N}` indexed from 0.

### Driver configuration parameters
Passable via JDBC URL query string or `Properties` (URL takes precedence):
| Parameter | Type | Purpose |
|---|---|---|
| `redisUrl` | String | Redis host; if absent, driver runs in pass-through mode (no caching) |
| `redisPort` | Integer | Redis port (optional) |
| `redisPassword` | String | Sent via `Jedis.auth()` |
| `redisIndex` | Integer | `Jedis.select(index)` after connect |
| `redisExpiration` | Integer (seconds) | TTL applied to keys on write AND refreshed on read |
| `redisObjectMaxSizeKB` | Double (KB) | Skip caching of result sets larger than this |
| `poolValidationQuery` | String | Query (e.g. `SELECT 1`) that bypasses the cache for connection pool health checks |

## Architecture

### System-context diagram
```
                                   ┌──────────────────────────┐
                                   │  Java application using  │
                                   │  jdbc:redshiftcached://  │
                                   └────────────┬─────────────┘
                                                │ JDBC SPI
                                   ┌────────────▼─────────────┐
                                   │  DriverWrapper           │
                                   │  ConnectionWrapper       │
                                   │  StatementWrapper        │
                                   │  PreparedStatementWrapper│
                                   └─────┬───────────────┬────┘
                                         │               │
                          executeQuery() │               │ all other JDBC ops
                                         │               │
                              ┌──────────▼───┐           │
                              │ RedisClient  │           │
                              │  (Jedis 2.8) │           │
                              └──┬───────┬───┘           │
                       cache hit │       │ cache miss    │
                                 │       │               │
                              ┌──▼──┐    │     ┌─────────▼──────────┐
                              │Redis│    └────►│ Amazon Redshift    │
                              │     │          │ JDBC42-1.1.17.1017 │
                              └─────┘          │ (com.amazon.       │
                                               │  redshift.jdbc42)  │
                                               └──────────┬─────────┘
                                                          │ Postgres wire
                                                          ▼
                                                  Amazon Redshift cluster
```

### Key components
- **`DriverWrapper`** — entry point; registers itself with `DriverManager` in a static block, claims the `jdbc:redshiftcached:` scheme, and rewrites it to `jdbc:redshift:` before delegating to the bundled Amazon Redshift JDBC42 driver. Creates one `RedisClient` per `connect()` call.
- **`ConnectionWrapper`** — thin pass-through that injects the per-connection `RedisClient` into every `Statement` / `PreparedStatement` it returns.
- **`StatementWrapper`** — only `executeQuery(String)` goes through the cache path; `execute(...)`, `executeUpdate(...)`, and batch operations bypass caching entirely.
- **`PreparedStatementWrapper`** — tokenizes the SQL on construction (`?` → `{0}`, `{1}`, …), records bound values in a `SortedMap`, and reconstructs a "rendered" SQL string via `MessageFormat.format(...)` as the cache key. Any `setBlob`/`setArray`/`setClob`/`setRowId`/`setSQLXML` call flips `cacheable=false`.
- **`RedisClient`** — orchestrates cache lookup/write. Serializes `CachedRowSetImpl` (a `com.sun.rowset` internal class) with `ObjectOutputStream`; key is the raw SQL string, value is the serialized rowset bytes. Honors a max-size limit and per-key TTL; refreshes TTL on cache hit.
- **`JedisFactory`** — single seam for unit testing Redis interaction; returns `null` Jedis when no URL is configured (pass-through mode).
- **`SqlUtil.tokenizeStatement`** — replaces each `?` placeholder in a prepared statement with positional tokens so the templated SQL plus bound parameter values can be re-rendered into a deterministic cache key.

### Data flow (cached query path)
1. Application opens connection via `DriverManager.getConnection("jdbc:redshiftcached://host:5439/db?redisUrl=...&...")`.
2. `DriverWrapper.connect` constructs `RedisClient` (which parses Redis params and dials Jedis), rewrites the URL scheme, delegates to the Amazon Redshift driver, wraps the returned `Connection`.
3. Application calls `statement.executeQuery(sql)` (or `pstmt.executeQuery()` after `setXxx` binds).
4. `RedisClient` short-circuits the pool validation query if configured, then does `GET sql` against Redis.
5. Cache hit → bytes are deserialized via `ObjectInputStream` into a `CachedRowSetImpl` and returned; TTL is bumped if `redisExpiration` is set.
6. Cache miss → query runs against Redshift, the live `ResultSet` is poured into a `CachedRowSetImpl`, the rowset is `ObjectOutputStream`-serialized, size-checked, and `SET` into Redis with optional TTL; the rowset is returned to the caller.
7. Cache invalidation is the caller's responsibility (per README — typically tied to the ETL refresh cadence).

### CI/CD tooling
**GitHub Actions** detected: `.github/workflows/secrets-scan.yml`. The only configured pipeline is a scheduled (`0 14 * * 1-5`, weekdays 14:00 UTC) TruffleHog secret scan that posts to Slack channel `github-token-scan` on failure via the `rtCamp/action-slack-notify` action. There is **no build, test, or publish pipeline** in this repo — JAR artifacts in `libs/1.7/` and `libs/1.8/` are checked into source control (versions 0.1, 0.2, 0.3) and the README instructs users to `mvn clean install` locally.

### Test architecture
Tests are JUnit 4.12 + Mockito 1.10.19 + MockRunner-JDBC 1.1.1, located under `src/test/java/com/powerreviews/jdbc/`:
- `redis/RedisClientTest.java` — exercises cache hit/miss/TTL/max-size logic with a mocked `Jedis` produced by a mocked `JedisFactory`.
- `PreparedStatementWrapperTest.java` — verifies parameter binding, tokenization, and the cache-disabling effect of `setBlob`/`setArray`/`setSQLXML`/etc.
- `util/SqlUtilTest.java` — covers placeholder tokenization.
- `src/test/resources/log4j2.xml` (stale — runtime now uses Logback per commit `46f976a`).

### Data model / database schema
Not applicable — this library does not own a schema. It serializes JDBC `CachedRowSetImpl` instances (via Java object serialization) into opaque `byte[]` values in Redis, keyed by the raw or rendered SQL text. No DDL is executed by this code.

### Auth & trust boundaries
- **Inbound auth**: None — this is an in-process JDBC driver. Trust is delegated to whatever process loads the JAR.
- **Outbound auth to Redshift**: Handled entirely by the wrapped `com.amazon.redshift.jdbc42.Driver` using standard JDBC user/password supplied in the `Properties`.
- **Outbound auth to Redis**: `Jedis.auth(redisPassword)` if `redisPassword` property is set; otherwise unauthenticated. Password is passed in the JDBC URL query string or `Properties` — and is `log.debug`-logged (`RedisClient.java:51`), which is a meaningful information-disclosure risk if DEBUG logging is enabled in any consumer.
- **Authorization model**: None at this layer — full pass-through to Redshift's grant model.

_Auth model not determinable from code beyond the JDBC pass-through described above._

### Data ownership
- **Redis**: **Cache** (not owner). Keyed by SQL text; values are serialized `CachedRowSetImpl` byte arrays. TTL is optional. The library never invalidates keys itself — invalidation is the caller's responsibility per README.
- **Amazon Redshift**: **Reader** (read-only). This driver only caches `executeQuery` results; `executeUpdate` and `execute(...)` pass straight through without caching and are not the focus of the library. The driver does not own or migrate Redshift schema.
- No sibling-repo data sharing is declared or inferable from the code.

### Deployment topology
_Deployment topology not in this repo._ This is a library distributed as a JAR; no Dockerfile, k8s manifests, Helm charts, or Terraform are present.

## Repo Activity
Derived from git history; current HEAD is `1fe44b6`.
- **Created**: 2016-04-11 (initial commit `dab404c`, author "dado").
- **Last meaningful change**: 2017-01-23 — `46f976a` "Modified logging system to Logback" / `c9ecc37` "Added new Java 1.8 JARs for version 0.3". Everything after that has been README/build-instruction tweaks, a secrets-scan workflow (2020-07-07/08), Dependabot bumps for `logback-core`, `logback-classic`, `junit`, and a `.whitesource` config file (2026-03-23). No functional code change has landed in ~9 years.
- **Activity level**: 1 commit in the last 90 days (`1b6b0c7` "Add .whitesource configuration file", 2026-03-23). Repo is effectively dormant.
- **Hot spots** (top files by churn over the last 6 months): only `.whitesource` was touched in the last 6 months. All-time top-churn files for context: `README.md` (11 revisions), `pom.xml` (9), `src/main/java/com/powerreviews/jdbc/redis/RedisClient.java` (4).
- **Recent major changes**: _No major changes in the last 6 months._ The only commit was adding a `.whitesource` dependency-scanner config file.
