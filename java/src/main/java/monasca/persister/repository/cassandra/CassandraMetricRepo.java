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

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.dropwizard.setup.Environment;
import monasca.common.model.metric.Metric;
import monasca.common.model.metric.MetricEnvelope;
import monasca.persister.configuration.PersisterConfig;
import monasca.persister.repository.Repo;
import monasca.persister.repository.RepoException;
import monasca.persister.repository.Sha1HashId;

public class CassandraMetricRepo extends CassandraRepo implements Repo<MetricEnvelope> {

  private static final Logger logger = LoggerFactory.getLogger(CassandraMetricRepo.class);

  public static final int MAX_COLUMN_LENGTH = 255;
  public static final int MAX_VALUE_META_LENGTH = 2048;

  private static final String TENANT_ID = "tenantId";
  private static final String REGION = "region";
  private static final String EMPTY_STR = "";

  private int retention;

  private CassandraMetricBatch batches;

  private int metricCount;

  private final ObjectMapper objectMapper = new ObjectMapper();

  public final Meter measurementMeter;
  public final Meter metricCacheMissMeter;
  public final Meter metricCacheHitMeter;
  public final Meter dimensionCacheMissMeter;
  public final Meter dimensionCacheHitMeter;
  public final Meter metricDimensionCacheMissMeter;
  public final Meter metricDimensionCacheHitMeter;

  @Inject
  public CassandraMetricRepo(CassandraCluster cluster, PersisterConfig config, Environment environment)
      throws NoSuchAlgorithmException, SQLException {

    super(cluster, environment, config.getCassandraDbConfiguration().getMaxWriteRetries(),
        config.getMetricConfiguration().getBatchSize());

    logger.debug("Instantiating " + this.getClass().getName());

    this.retention = config.getCassandraDbConfiguration().getRetentionPolicy() * 24 * 3600;

    this.measurementMeter = this.environment.metrics()
        .meter(this.getClass().getName() + "." + "measurement-meter");

    this.metricCacheMissMeter = this.environment.metrics()
        .meter(this.getClass().getName() + "." + "definition-cache-miss-meter");

    this.metricCacheHitMeter = this.environment.metrics()
        .meter(this.getClass().getName() + "." + "definition-cache-hit-meter");

    this.dimensionCacheMissMeter = this.environment.metrics()
        .meter(this.getClass().getName() + "." + "dimension-cache-miss-meter");

    this.dimensionCacheHitMeter = this.environment.metrics()
        .meter(this.getClass().getName() + "." + "dimension-cache-hit-meter");

    this.metricDimensionCacheMissMeter = this.environment.metrics()
        .meter(this.getClass().getName() + "." + "metric-dimension-cache-miss-meter");

    this.metricDimensionCacheHitMeter = this.environment.metrics()
        .meter(this.getClass().getName() + "." + "metric-dimension-cache-hit-meter");

    session = cluster.getMetricsSession();

    metricCount = 0;

    batches = new CassandraMetricBatch(cluster.getMetaData(), cluster.getProtocolOptions(),
        cluster.getCodecRegistry(), cluster.getLoadBalancePolicy(),
        config.getCassandraDbConfiguration().getMaxBatches());



    logger.debug(this.getClass().getName() + " is fully instantiated");
  }

  @Override
  public void addToBatch(MetricEnvelope metricEnvelope, String id) {
    Metric metric = metricEnvelope.metric;
    Map<String, Object> metaMap = metricEnvelope.meta;

    String tenantId = getMeta(TENANT_ID, metric, metaMap, id);
    String region = getMeta(REGION, metric, metaMap, id);
    String metricName = metric.getName();
    TreeMap<String, String> dimensions = metric.getDimensions() == null ? new TreeMap<String, String>()
        : new TreeMap<>(metric.getDimensions());

    StringBuilder sb = new StringBuilder(region).append(tenantId).append(metricName);

    Iterator<String> it = dimensions.keySet().iterator();
    while (it.hasNext()) {
      String k = it.next();
      sb.append(k).append(dimensions.get(k));
    }

    byte[] defIdSha = DigestUtils.sha(sb.toString());
    Sha1HashId defIdShaHash = new Sha1HashId(defIdSha);

    if (cluster.getMetricIdCache().getIfPresent(defIdShaHash.toHexString()) == null) {
      addDefinitionToBatch(defIdShaHash, metricName, dimensions, tenantId, region, id,
          metric.getTimestamp());
      batches.addMeasurementQuery(buildMeasurementInsertQuery(defIdShaHash, metric.getTimestamp(),
          metric.getValue(), metric.getValueMeta(), region, tenantId, metricName, dimensions, id));
    } else {
      metricCacheHitMeter.mark();
      // MUST update all relevant columns to ensure TTL consistency in a row
      batches.addMetricQuery(cluster.getMetricInsertStmt().bind(retention,
          defIdShaHash.getSha1HashByteBuffer(), new Timestamp(metric.getTimestamp()),
          new Timestamp(metric.getTimestamp()), region, tenantId, metricName,
          getDimensionList(dimensions), new ArrayList<>(dimensions.keySet())));
      batches.addMeasurementQuery(buildMeasurementUpdateQuery(defIdShaHash, metric.getTimestamp(),
          metric.getValue(), metric.getValueMeta(), id));
    }

    metricCount++;
  }

  private String getMeta(String name, Metric metric, Map<String, Object> meta, String id) {
    if (meta.containsKey(name)) {
      return (String) meta.get(name);
    } else {
      logger.warn(
          "[{}]: failed to find {} in message envelope meta data. metric message may be malformed. "
              + "setting {} to empty string.",
          id, name);
      logger.warn("[{}]: metric: {}", id, metric.toString());
      logger.warn("[{}]: meta: {}", id, meta.toString());
      return EMPTY_STR;
    }
  }

  private BoundStatement buildMeasurementUpdateQuery(Sha1HashId defId, long timeStamp, double value,
      Map<String, String> valueMeta, String id) {

    String valueMetaString = getValueMetaString(valueMeta, id);
    if (logger.isDebugEnabled()) {
      logger.debug("[{}]: adding metric to batch: metric id: {}, time: {}, value: {}, value meta {}",
          id, defId.toHexString(), timeStamp, value, valueMetaString);
    }

    return cluster.getMeasurementUpdateStmt().bind(retention, value, valueMetaString,
        defId.getSha1HashByteBuffer(), new Timestamp(timeStamp));
  }

  private BoundStatement buildMeasurementInsertQuery(Sha1HashId defId, long timeStamp, double value,
      Map<String, String> valueMeta, String region, String tenantId, String metricName,
      Map<String, String> dimensions, String id) {

    String valueMetaString = getValueMetaString(valueMeta, id);
    if (logger.isDebugEnabled()) {
      logger.debug("[{}]: adding metric to batch: metric id: {}, time: {}, value: {}, value meta {}",
          id, defId.toHexString(), timeStamp, value, valueMetaString);
    }

    measurementMeter.mark();
    return cluster.getMeasurementInsertStmt().bind(retention, value, valueMetaString, region, tenantId,
        metricName, getDimensionList(dimensions), defId.getSha1HashByteBuffer(),
        new Timestamp(timeStamp));
  }

  private String getValueMetaString(Map<String, String> valueMeta, String id) {

    String valueMetaString = "";

    if (valueMeta != null && !valueMeta.isEmpty()) {

      try {

        valueMetaString = this.objectMapper.writeValueAsString(valueMeta);
        if (valueMetaString.length() > MAX_VALUE_META_LENGTH) {
          logger.error("[{}]: Value meta length {} longer than maximum {}, dropping value meta", id,
              valueMetaString.length(), MAX_VALUE_META_LENGTH);
          return "";
        }

      } catch (JsonProcessingException e) {

        logger.error("[{}]: Failed to serialize value meta {}, dropping value meta from measurement",
            id, valueMeta);
      }
    }

    return valueMetaString;
  }

  private void addDefinitionToBatch(Sha1HashId defId, String metricName, Map<String, String> dimensions,
      String tenantId, String region, String id, long timestamp) {

    metricCacheMissMeter.mark();
    if (logger.isDebugEnabled()) {
      logger.debug("[{}]: adding definition to batch: defId: {}, name: {}, tenantId: {}, region: {}",
          id, defId.toHexString(), metricName, tenantId, region);
    }

    Timestamp ts = new Timestamp(timestamp);
    batches.addMetricQuery(
        cluster.getMetricInsertStmt().bind(retention, defId.getSha1HashByteBuffer(), ts, ts, region,
            tenantId, metricName, getDimensionList(dimensions), new ArrayList<>(dimensions.keySet())));

    for (Map.Entry<String, String> entry : dimensions.entrySet()) {
      String name = entry.getKey();
      String value = entry.getValue();

      String dimensionKey = cluster.getDimnesionEntryKey(region, tenantId, name, value);
      if (cluster.getDimensionCache().getIfPresent(dimensionKey) != null) {
        dimensionCacheHitMeter.mark();

      } else {
        dimensionCacheMissMeter.mark();
        if (logger.isDebugEnabled()) {
          logger.debug("[{}]: adding dimension to batch: defId: {}, name: {}, value: {}", id,
              defId.toHexString(), name, value);
        }
        batches.addDimensionQuery(cluster.getDimensionStmt().bind(region, tenantId, name, value));
        cluster.getDimensionCache().put(dimensionKey, Boolean.TRUE);
      }

      String metricDimensionKey = cluster.getMetricDimnesionEntryKey(region, tenantId, metricName, name, value);
      if (cluster.getMetricDimensionCache().getIfPresent(metricDimensionKey) != null) {
        metricDimensionCacheHitMeter.mark();
      } else {
        metricDimensionCacheMissMeter.mark();
        batches.addDimensionMetricQuery(
            cluster.getDimensionMetricStmt().bind(region, tenantId, name, value, metricName));

        batches.addMetricDimensionQuery(
            cluster.getMetricDimensionStmt().bind(region, tenantId, metricName, name, value));
        cluster.getMetricDimensionCache().put(metricDimensionKey, Boolean.TRUE);
      }
    }

    String metricId = defId.toHexString();
    cluster.getMetricIdCache().put(metricId, Boolean.TRUE);
  }

  public List<String> getDimensionList(Map<String, String> dimensions) {
    List<String> list = new ArrayList<>(dimensions.size());
    for (Entry<String, String> dim : dimensions.entrySet()) {
      list.add(new StringBuffer(dim.getKey()).append('\t').append(dim.getValue()).toString());
    }
    return list;
  }

  @Override
  public int flush(String id) throws RepoException {
    long startTime = System.nanoTime();
    List<ResultSetFuture> results = new ArrayList<>();
    List<Deque<BatchStatement>> list = batches.getAllBatches();
    for (Deque<BatchStatement> q : list) {
      BatchStatement b;
      while ((b = q.poll()) != null) {
        results.add(session.executeAsync(b));
      }
    }

    List<ListenableFuture<ResultSet>> futures = Futures.inCompletionOrder(results);

    boolean cancel = false;
    Exception ex = null;
    for (ListenableFuture<ResultSet> future : futures) {
      if (cancel) {
        future.cancel(false);
        continue;
      }
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        cancel = true;
        ex = e;
      }
    }

    this.commitTimer.update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);

    if (ex != null) {
      metricFailed.inc(metricCount);
      throw new RepoException(ex);
    }

    batches.clear();
    int flushCnt = metricCount;
    metricCount = 0;
    metricCompleted.inc(flushCnt);
    return flushCnt;
  }
}
