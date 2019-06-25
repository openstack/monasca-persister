/*
 * Copyright (c) 2017 SUSE LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monasca.persister.repository.cassandra;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;

import monasca.common.configuration.CassandraDbConfiguration;
import monasca.persister.configuration.PersisterConfig;

public class CassandraCluster {

  private static final Logger logger = LoggerFactory.getLogger(CassandraCluster.class);

  private static final String MEASUREMENT_INSERT_CQL = "update monasca.measurements USING TTL ? "
      + "set value = ?, value_meta = ?, region = ?, tenant_id = ?, metric_name = ?, dimensions = ? "
      + "where metric_id = ? and time_stamp = ?";

  // TODO: Remove update statements, TTL issues
  private static final String MEASUREMENT_UPDATE_CQL = "update monasca.measurements USING TTL ? "
      + "set value = ?, value_meta = ? " + "where metric_id = ? and time_stamp = ?";

  private static final String METRICS_INSERT_CQL = "update monasca.metrics USING TTL ? "
      + "set metric_id = ?, created_at = ?, updated_at = ? "
      + "where region = ? and tenant_id = ? and metric_name = ? and dimensions = ? and dimension_names = ?";

  private static final String DIMENSION_INSERT_CQL = "insert into  monasca.dimensions "
      + "(region, tenant_id, name, value) values (?, ?, ?, ?)";

  private static final String DIMENSION_METRIC_INSERT_CQL = "insert into monasca.dimensions_metrics "
      + " (region, tenant_id, dimension_name, dimension_value, metric_name) values (?, ?, ?, ?, ?)";

  private static final String METRIC_DIMENSION_INSERT_CQL = "insert into monasca.metrics_dimensions "
      + " (region, tenant_id, metric_name, dimension_name, dimension_value) values (?, ?, ?, ?, ?)";

  private static final String INSERT_ALARM_STATE_HISTORY_SQL = "update monasca.alarm_state_history USING TTL ? "
      + " set metric = ?, old_state = ?, new_state = ?, sub_alarms = ?, reason = ?, reason_data = ?"
      + " where tenant_id = ? and alarm_id = ? and time_stamp = ?";

  private static final String RETRIEVE_METRIC_DIMENSION_CQL = "select region, tenant_id, metric_name, "
      + "dimension_name, dimension_value from metrics_dimensions "
      + "WHERE token(region, tenant_id, metric_name) > ? and token(region, tenant_id, metric_name) <= ? ";

  private static final String RETRIEVE_METRIC_ID_CQL = "select distinct metric_id from measurements WHERE token(metric_id) > ? and token(metric_id) <= ?";

  private static final String RETRIEVE_DIMENSION_CQL = "select region, tenant_id, name, value from dimensions";

  private static final String NAME = "name";
  private static final String VALUE = "value";
  private static final String METRIC_ID = "metric_id";
  private static final String TENANT_ID_COLUMN = "tenant_id";
  private static final String METRIC_NAME = "metric_name";
  private static final String DIMENSION_NAME = "dimension_name";
  private static final String DIMENSION_VALUE = "dimension_value";
  private static final String REGION = "region";

  private CassandraDbConfiguration dbConfig;
  private Cluster cluster;
  private Session metricsSession;
  private Session alarmsSession;

  private TokenAwarePolicy lbPolicy;

  private PreparedStatement measurementInsertStmt;
  private PreparedStatement measurementUpdateStmt;
  private PreparedStatement metricInsertStmt;
  private PreparedStatement dimensionStmt;
  private PreparedStatement dimensionMetricStmt;
  private PreparedStatement metricDimensionStmt;

  private PreparedStatement retrieveMetricDimensionStmt;
  private PreparedStatement retrieveMetricIdStmt;

  private PreparedStatement alarmHistoryInsertStmt;

  public Cache<String, Boolean> getMetricIdCache() {
    return metricIdCache;
  }

  public Cache<String, Boolean> getDimensionCache() {
    return dimensionCache;
  }

  public Cache<String, Boolean> getMetricDimensionCache() {
    return metricDimensionCache;
  }

  private final Cache<String, Boolean> metricIdCache;

  private final Cache<String, Boolean> dimensionCache;

  private final Cache<String, Boolean> metricDimensionCache;

  @Inject
  public CassandraCluster(final PersisterConfig config) {

    this.dbConfig = config.getCassandraDbConfiguration();

    QueryOptions qo = new QueryOptions();
    qo.setConsistencyLevel(ConsistencyLevel.valueOf(dbConfig.getConsistencyLevel()));
    qo.setDefaultIdempotence(true);

    String[] contactPoints = dbConfig.getContactPoints();
    int retries = dbConfig.getMaxWriteRetries();
    Builder builder = Cluster.builder().addContactPoints(contactPoints).withPort(dbConfig.getPort());
    builder
        .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(dbConfig.getConnectionTimeout())
            .setReadTimeoutMillis(dbConfig.getReadTimeout()));
    builder.withQueryOptions(qo).withRetryPolicy(new MonascaRetryPolicy(retries, retries, retries));

    lbPolicy = new TokenAwarePolicy(
        DCAwareRoundRobinPolicy.builder().withLocalDc(dbConfig.getLocalDataCenter()).build());
    builder.withLoadBalancingPolicy(lbPolicy);

    String user = dbConfig.getUser();
    if (user != null && !user.isEmpty()) {
      builder.withAuthProvider(new PlainTextAuthProvider(dbConfig.getUser(), dbConfig.getPassword()));
    }
    cluster = builder.build();

    PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();

    poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, dbConfig.getMaxConnections(),
        dbConfig.getMaxConnections()).setConnectionsPerHost(HostDistance.REMOTE,
            dbConfig.getMaxConnections(), dbConfig.getMaxConnections());

    poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, dbConfig.getMaxRequests())
        .setMaxRequestsPerConnection(HostDistance.REMOTE, dbConfig.getMaxRequests());

    metricsSession = cluster.connect(dbConfig.getKeySpace());

    measurementInsertStmt = metricsSession.prepare(MEASUREMENT_INSERT_CQL).setIdempotent(true);
    // TODO: Remove update statements, TTL issues
    measurementUpdateStmt = metricsSession.prepare(MEASUREMENT_UPDATE_CQL).setIdempotent(true);
    metricInsertStmt = metricsSession.prepare(METRICS_INSERT_CQL).setIdempotent(true);
    dimensionStmt = metricsSession.prepare(DIMENSION_INSERT_CQL).setIdempotent(true);
    dimensionMetricStmt = metricsSession.prepare(DIMENSION_METRIC_INSERT_CQL).setIdempotent(true);
    metricDimensionStmt = metricsSession.prepare(METRIC_DIMENSION_INSERT_CQL).setIdempotent(true);

    retrieveMetricIdStmt = metricsSession.prepare(RETRIEVE_METRIC_ID_CQL).setIdempotent(true);
    retrieveMetricDimensionStmt = metricsSession.prepare(RETRIEVE_METRIC_DIMENSION_CQL)
        .setIdempotent(true);

    alarmsSession = cluster.connect(dbConfig.getKeySpace());

    alarmHistoryInsertStmt = alarmsSession.prepare(INSERT_ALARM_STATE_HISTORY_SQL).setIdempotent(true);

    metricIdCache = CacheBuilder.newBuilder()
        .maximumSize(config.getCassandraDbConfiguration().getDefinitionMaxCacheSize()).build();

    dimensionCache = CacheBuilder.newBuilder()
        .maximumSize(config.getCassandraDbConfiguration().getDefinitionMaxCacheSize()).build();

    metricDimensionCache = CacheBuilder.newBuilder()
        .maximumSize(config.getCassandraDbConfiguration().getDefinitionMaxCacheSize()).build();

    logger.info("loading cached definitions from db");

    ExecutorService executor = Executors.newFixedThreadPool(250);

    //a majority of the ids are for metrics not actively receiving msgs anymore
    //loadMetricIdCache(executor);

    loadDimensionCache();

    loadMetricDimensionCache(executor);

    executor.shutdown();
  }

  public Session getMetricsSession() {
    return metricsSession;
  }

  public Session getAlarmsSession() {
    return alarmsSession;
  }

  public PreparedStatement getMeasurementInsertStmt() {
    return measurementInsertStmt;
  }

  // TODO: Remove update statements, TTL issues
  public PreparedStatement getMeasurementUpdateStmt() {
    return measurementUpdateStmt;
  }

  public PreparedStatement getMetricInsertStmt() {
    return metricInsertStmt;
  }

  public PreparedStatement getDimensionStmt() {
    return dimensionStmt;
  }

  public PreparedStatement getDimensionMetricStmt() {
    return dimensionMetricStmt;
  }

  public PreparedStatement getMetricDimensionStmt() {
    return metricDimensionStmt;
  }

  public PreparedStatement getAlarmHistoryInsertStmt() {
    return alarmHistoryInsertStmt;
  }

  public ProtocolOptions getProtocolOptions() {
    return cluster.getConfiguration().getProtocolOptions();
  }

  public CodecRegistry getCodecRegistry() {
    return cluster.getConfiguration().getCodecRegistry();
  }

  public Metadata getMetaData() {
    return cluster.getMetadata();
  }

  public TokenAwarePolicy getLoadBalancePolicy() {
    return lbPolicy;
  }

  private void loadMetricIdCache(ExecutorService executor) {
    final AtomicInteger tasks = new AtomicInteger(0);
    logger.info("Found token ranges: " + cluster.getMetadata().getTokenRanges().size());
    for (TokenRange range : cluster.getMetadata().getTokenRanges()) {
      List<BoundStatement> queries = rangeQuery(retrieveMetricIdStmt, range);
      for (BoundStatement query : queries) {
        tasks.incrementAndGet();
        logger.info("adding a metric id reading task, total: " + tasks.get());

        ResultSetFuture future = metricsSession.executeAsync(query);

        Futures.addCallback(future, new FutureCallback<ResultSet>() {
          @Override
          public void onSuccess(ResultSet result) {
            for (Row row : result) {
              String id = Bytes.toHexString(row.getBytes(METRIC_ID));
              if (id != null) {
                //remove '0x'
                metricIdCache.put(id.substring(2), Boolean.TRUE);
              }
            }

            tasks.decrementAndGet();

            logger.info("completed a metric id read task. Remaining tasks: " + tasks.get());
          }

          @Override
          public void onFailure(Throwable t) {
            logger.error("Failed to execute query to load metric id cache.", t);

            tasks.decrementAndGet();

            logger.info("Failed a metric id read task. Remaining tasks: " + tasks.get());
          }
        }, executor);

      }
    }

    while (tasks.get() > 0) {
      logger.debug("waiting for more metric id load tasks: " + tasks.get());

      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        logger.warn("load metric cache was interrupted", e);
      }
    }

    logger.info("loaded metric id cache from database: " + metricIdCache.size());
  }

  private List<BoundStatement> rangeQuery(PreparedStatement rangeStmt, TokenRange range) {
    List<BoundStatement> res = Lists.newArrayList();
    for (TokenRange subRange : range.unwrap()) {
      res.add(rangeStmt.bind(subRange.getStart(), subRange.getEnd()));
    }
    return res;
  }

  private void loadDimensionCache() {

    ResultSet results = metricsSession.execute(RETRIEVE_DIMENSION_CQL);

    for (Row row : results) {
      String key = getDimnesionEntryKey(row.getString(REGION), row.getString(TENANT_ID_COLUMN),
          row.getString(NAME), row.getString(VALUE));
      dimensionCache.put(key, Boolean.TRUE);
    }

    logger.info("loaded dimension cache from database: " + dimensionCache.size());
  }

  public String getDimnesionEntryKey(String region, String tenantId, String name, String value) {
    StringBuilder sb = new StringBuilder();
    sb.append(region).append('\0');
    sb.append(tenantId).append('\0');
    sb.append(name).append('\0');
    sb.append(value);
    return sb.toString();
  }

  private void loadMetricDimensionCache(ExecutorService executor) {

    final AtomicInteger tasks = new AtomicInteger(0);

    for (TokenRange range : cluster.getMetadata().getTokenRanges()) {
      List<BoundStatement> queries = rangeQuery(retrieveMetricDimensionStmt, range);
      for (BoundStatement query : queries) {
        tasks.incrementAndGet();

        logger.info("Adding a metric dimnesion read task, total: " + tasks.get());

        ResultSetFuture future = metricsSession.executeAsync(query);

        Futures.addCallback(future, new FutureCallback<ResultSet>() {
          @Override
          public void onSuccess(ResultSet result) {
            for (Row row : result) {
              String key = getMetricDimnesionEntryKey(row.getString(REGION),
                  row.getString(TENANT_ID_COLUMN), row.getString(METRIC_NAME),
                  row.getString(DIMENSION_NAME), row.getString(DIMENSION_VALUE));
              metricDimensionCache.put(key, Boolean.TRUE);
            }

            tasks.decrementAndGet();

            logger.info("Completed a metric dimension read task. Remaining tasks: " + tasks.get());
          }

          @Override
          public void onFailure(Throwable t) {
            logger.error("Failed to execute query to load metric id cache.", t);

            tasks.decrementAndGet();

            logger.info("Failed a metric dimension read task. Remaining tasks: " + tasks.get());
          }
        }, executor);

      }
    }

    while (tasks.get() > 0) {

      logger.debug("waiting for metric dimension cache to load ...");

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.warn("load metric dimension cache was interrupted", e);
      }
    }

    logger.info("loaded metric dimension cache from database: " + metricDimensionCache.size());
  }

  public String getMetricDimnesionEntryKey(String region, String tenantId, String metricName,
      String dimensionName, String dimensionValue) {
    StringBuilder sb = new StringBuilder();
    sb.append(region).append('\0');
    sb.append(tenantId).append('\0');
    sb.append(metricName).append('\0');
    sb.append(dimensionName).append('\0');
    sb.append(dimensionValue);
    return sb.toString();
  }
}
