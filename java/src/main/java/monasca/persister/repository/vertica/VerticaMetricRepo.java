/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
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

package monasca.persister.repository.vertica;

import monasca.common.model.metric.Metric;
import monasca.common.model.metric.MetricEnvelope;
import monasca.persister.configuration.PersisterConfig;
import monasca.persister.pipeline.event.MetricHandler;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import org.apache.commons.codec.digest.DigestUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.PreparedBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import javax.inject.Inject;

import io.dropwizard.setup.Environment;
import monasca.persister.repository.Repo;

public class VerticaMetricRepo extends VerticaRepo implements Repo<MetricEnvelope> {

  private static final Logger logger = LoggerFactory.getLogger(VerticaMetricRepo.class);

  public static final int MAX_COLUMN_LENGTH = 255;

  private final SimpleDateFormat simpleDateFormat;

  private static final String TENANT_ID = "tenantId";
  private static final String REGION = "region";

  private final Environment environment;

  private final Cache<Sha1HashId, Sha1HashId> definitionsIdCache;
  private final Cache<Sha1HashId, Sha1HashId> dimensionsIdCache;
  private final Cache<Sha1HashId, Sha1HashId> definitionDimensionsIdCache;

  private final Set<Sha1HashId> definitionIdSet = new HashSet<>();
  private final Set<Sha1HashId> dimensionIdSet = new HashSet<>();
  private final Set<Sha1HashId> definitionDimensionsIdSet = new HashSet<>();

  private static final String SQL_INSERT_INTO_METRICS =
      "insert into MonMetrics.measurements (definition_dimensions_id, time_stamp, value) values (:definition_dimension_id, :time_stamp, :value)";

  private static final String DEFINITIONS_TEMP_STAGING_TABLE = "(" + "   id BINARY(20) NOT NULL,"
      + "   name VARCHAR(255) NOT NULL," + "   tenant_id VARCHAR(255) NOT NULL,"
      + "   region VARCHAR(255) NOT NULL" + ")";

  private static final String DIMENSIONS_TEMP_STAGING_TABLE = "("
      + "    dimension_set_id BINARY(20) NOT NULL," + "    name VARCHAR(255) NOT NULL,"
      + "    value VARCHAR(255) NOT NULL" + ")";

  private static final String DEFINITIONS_DIMENSIONS_TEMP_STAGING_TABLE = "("
      + "   id BINARY(20) NOT NULL," + "   definition_id BINARY(20) NOT NULL, "
      + "   dimension_set_id BINARY(20) NOT NULL " + ")";

  private PreparedBatch metricsBatch;
  private PreparedBatch stagedDefinitionsBatch;
  private PreparedBatch stagedDimensionsBatch;
  private PreparedBatch stagedDefinitionDimensionsBatch;

  private final String definitionsTempStagingTableName;
  private final String dimensionsTempStagingTableName;
  private final String definitionDimensionsTempStagingTableName;

  private final String definitionsTempStagingTableInsertStmt;
  private final String dimensionsTempStagingTableInsertStmt;
  private final String definitionDimensionsTempStagingTableInsertStmt;


  private final Counter metricCounter;
  private final Counter definitionCounter;
  private final Counter dimensionCounter;
  private final Counter definitionDimensionsCounter;

  private final Timer flushTimer;
  public final Meter measurementMeter;
  public final Meter definitionCacheMissMeter;
  public final Meter dimensionCacheMissMeter;
  public final Meter definitionDimensionCacheMissMeter;
  public final Meter definitionCacheHitMeter;
  public final Meter dimensionCacheHitMeter;
  public final Meter definitionDimensionCacheHitMeter;

  @Inject
  public VerticaMetricRepo(DBI dbi, PersisterConfig configuration,
                           Environment environment) throws NoSuchAlgorithmException, SQLException {
    super(dbi);
    logger.debug("Instantiating: " + this.getClass().getName());

    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT-0"));

    this.environment = environment;
    final String handlerName = String.format("%s[%d]", MetricHandler.class.getName(), new Random().nextInt());

    this.metricCounter =
        environment.metrics().counter(handlerName + "." + "metrics-added-to-batch-counter");
    this.definitionCounter =
        environment.metrics()
            .counter(handlerName + "." + "metric-definitions-added-to-batch-counter");
    this.dimensionCounter =
        environment.metrics()
            .counter(handlerName + "." + "metric-dimensions-added-to-batch-counter");
    this.definitionDimensionsCounter =
        environment.metrics()
            .counter(handlerName + "." + "metric-definition-dimensions-added-to-batch-counter");

    this.flushTimer =
        this.environment.metrics().timer(this.getClass().getName() + "." + "flush-timer");
    this.measurementMeter =
        this.environment.metrics().meter(this.getClass().getName() + "." + "measurement-meter");
    this.definitionCacheMissMeter =
        this.environment.metrics().meter(
            this.getClass().getName() + "." + "definition-cache-miss-meter");
    this.dimensionCacheMissMeter =
        this.environment.metrics().meter(
            this.getClass().getName() + "." + "dimension-cache-miss-meter");
    this.definitionDimensionCacheMissMeter =
        this.environment.metrics().meter(
            this.getClass().getName() + "." + "definition-dimension-cache-miss-meter");
    this.definitionCacheHitMeter =
        this.environment.metrics().meter(
            this.getClass().getName() + "." + "definition-cache-hit-meter");
    this.dimensionCacheHitMeter =
        this.environment.metrics().meter(
            this.getClass().getName() + "." + "dimension-cache-hit-meter");
    this.definitionDimensionCacheHitMeter =
        this.environment.metrics().meter(
            this.getClass().getName() + "." + "definition-dimension-cache-hit-meter");

    definitionsIdCache =
        CacheBuilder.newBuilder()
            .maximumSize(configuration.getVerticaMetricRepoConfig().getMaxCacheSize())
            .build();
    dimensionsIdCache =
        CacheBuilder.newBuilder()
            .maximumSize(configuration.getVerticaMetricRepoConfig().getMaxCacheSize())
            .build();
    definitionDimensionsIdCache =
        CacheBuilder.newBuilder()
            .maximumSize(configuration.getVerticaMetricRepoConfig().getMaxCacheSize())
            .build();

    logger.info("preparing database and building sql statements...");

    String uniqueName = this.toString().replaceAll("\\.", "_").replaceAll("\\@", "_");
    this.definitionsTempStagingTableName = uniqueName + "_staged_definitions";
    logger.debug("temp staging definitions table name: " + definitionsTempStagingTableName);

    this.dimensionsTempStagingTableName = uniqueName + "_staged_dimensions";
    logger.debug("temp staging dimensions table name:" + dimensionsTempStagingTableName);

    this.definitionDimensionsTempStagingTableName = uniqueName + "_staged_definitions_dimensions";
    logger.debug("temp staging definitionDimensions table name: "
        + definitionDimensionsTempStagingTableName);

    this.definitionsTempStagingTableInsertStmt =
        "insert into  MonMetrics.Definitions select distinct * from "
            + definitionsTempStagingTableName
            + " where id not in (select id from MonMetrics.Definitions)";
    logger.debug("definitions insert stmt: " + definitionsTempStagingTableInsertStmt);

    this.dimensionsTempStagingTableInsertStmt =
        "insert into MonMetrics.Dimensions select distinct * from "
            + dimensionsTempStagingTableName
            + " where dimension_set_id not in (select dimension_set_id from MonMetrics.Dimensions)";
    logger.debug("dimensions insert stmt: " + definitionsTempStagingTableInsertStmt);

    this.definitionDimensionsTempStagingTableInsertStmt =
        "insert into MonMetrics.definitionDimensions select distinct * from "
            + definitionDimensionsTempStagingTableName
            + " where id not in (select id from MonMetrics.definitionDimensions)";
    logger.debug("definitionDimensions insert stmt: "
        + definitionDimensionsTempStagingTableInsertStmt);

    logger.debug("dropping temp staging tables if they already exist...");
    handle.execute("drop table if exists " + definitionsTempStagingTableName + " cascade");
    handle.execute("drop table if exists " + dimensionsTempStagingTableName + " cascade");
    handle.execute("drop table if exists " + definitionDimensionsTempStagingTableName + " cascade");

    logger.debug("creating temp staging tables...");
    handle.execute("create local temp table " + definitionsTempStagingTableName + " "
        + DEFINITIONS_TEMP_STAGING_TABLE + " on commit preserve rows");
    handle.execute("create local temp table " + dimensionsTempStagingTableName + " "
        + DIMENSIONS_TEMP_STAGING_TABLE + " on commit preserve rows");
    handle.execute("create local temp table " + definitionDimensionsTempStagingTableName + " "
        + DEFINITIONS_DIMENSIONS_TEMP_STAGING_TABLE + " on commit preserve rows");

    handle.getConnection().setAutoCommit(false);

    logger.debug("preparing batches...");
    metricsBatch = handle.prepareBatch(SQL_INSERT_INTO_METRICS);
    stagedDefinitionsBatch =
        handle.prepareBatch("insert into " + definitionsTempStagingTableName
            + " values (:id, :name, :tenant_id, :region)");
    stagedDimensionsBatch =
        handle.prepareBatch("insert into " + dimensionsTempStagingTableName
            + " values (:dimension_set_id, :name, :value)");
    stagedDefinitionDimensionsBatch =
        handle.prepareBatch("insert into " + definitionDimensionsTempStagingTableName
            + " values (:id, :definition_id, :dimension_set_id)");

    logger.debug("opening transaction...");
    handle.begin();

    logger.debug("completed database preparations");

    logger.debug(this.getClass().getName() + "is fully instantiated");
  }

  @Override
  public void addToBatch(MetricEnvelope metricEnvelope) {

    Metric metric = metricEnvelope.metric;
    Map<String, Object> meta = metricEnvelope.meta;

    logger.debug("metric: {}", metric);
    logger.debug("meta: {}", meta);

    String tenantId = "";
    if (meta.containsKey(TENANT_ID)) {
      tenantId = (String) meta.get(TENANT_ID);
    } else {
      logger.warn(
          "Failed to find tenantId in message envelope meta data. Metric message may be malformed"
          + ". Setting tenantId to empty string.");
      logger.warn("metric: {}", metric.toString());
      logger.warn("meta: {}", meta.toString());
    }

    String region = "";
    if (meta.containsKey(REGION)) {
      region = (String) meta.get(REGION);
    } else {
      logger.warn(
          "Failed to find region in message envelope meta data. Metric message may be malformed. "
          + "Setting region to empty string.");
      logger.warn("metric: {}", metric.toString());
      logger.warn("meta: {}", meta.toString());
    }

    // Add the definition to the batch.
    StringBuilder
        definitionIdStringToHash =
        new StringBuilder(trunc(metric.getName(), MAX_COLUMN_LENGTH));
    definitionIdStringToHash.append(trunc(tenantId, MAX_COLUMN_LENGTH));
    definitionIdStringToHash.append(trunc(region, MAX_COLUMN_LENGTH));
    byte[] definitionIdSha1Hash = DigestUtils.sha(definitionIdStringToHash.toString());
    Sha1HashId definitionSha1HashId = new Sha1HashId((definitionIdSha1Hash));
    this.addDefinitionToBatch(definitionSha1HashId, trunc(metric.getName(), MAX_COLUMN_LENGTH),
                              trunc(tenantId, MAX_COLUMN_LENGTH), trunc(region, MAX_COLUMN_LENGTH));
    definitionCounter.inc();

    // Calculate dimensions sha1 hash id.
    StringBuilder dimensionIdStringToHash = new StringBuilder();
    Map<String, String> preppedDimMap = prepDimensions(metric.getDimensions());
    for (Map.Entry<String, String> entry : preppedDimMap.entrySet()) {
      dimensionIdStringToHash.append(entry.getKey());
      dimensionIdStringToHash.append(entry.getValue());
    }
    byte[] dimensionIdSha1Hash = DigestUtils.sha(dimensionIdStringToHash.toString());
    Sha1HashId dimensionsSha1HashId = new Sha1HashId(dimensionIdSha1Hash);

    // Add the dimension name/values to the batch.
    this.addDimensionsToBatch(dimensionsSha1HashId, preppedDimMap);

    // Add the definition dimensions to the batch.
    StringBuilder
        definitionDimensionsIdStringToHash =
        new StringBuilder(definitionSha1HashId.toHexString());
    definitionDimensionsIdStringToHash.append(dimensionsSha1HashId.toHexString());
    byte[]
        definitionDimensionsIdSha1Hash =
        DigestUtils.sha(definitionDimensionsIdStringToHash.toString());
    Sha1HashId definitionDimensionsSha1HashId = new Sha1HashId(definitionDimensionsIdSha1Hash);
    this.addDefinitionDimensionToBatch(definitionDimensionsSha1HashId, definitionSha1HashId,
                                       dimensionsSha1HashId);
    definitionDimensionsCounter.inc();

    // Add the measurement to the batch.
    String timeStamp = simpleDateFormat.format(new Date(metric.getTimestamp()));
    double value = metric.getValue();
    this.addMetricToBatch(definitionDimensionsSha1HashId, timeStamp, value, metric.getValueMeta());

    this.metricCounter.inc();
  }

  public void addMetricToBatch(Sha1HashId defDimsId, String timeStamp, double value,
                               Map<String, String> valueMeta) {
    // TODO: Actually handle valueMeta
    logger.debug("Adding metric to batch: defDimsId: {}, time: {}, value: {}",
                 defDimsId.toHexString(), timeStamp, value);
    metricsBatch.add().bind("definition_dimension_id", defDimsId.getSha1Hash())
        .bind("time_stamp", timeStamp).bind("value", value);
    measurementMeter.mark();
  }

  private void addDefinitionToBatch(Sha1HashId defId, String name, String tenantId, String region) {

    if (definitionsIdCache.getIfPresent(defId) == null) {

      definitionCacheMissMeter.mark();

      if (!definitionIdSet.contains(defId)) {

        logger.debug("Adding definition to batch: defId: {}, name: {}, tenantId: {}, region: {}",
                     defId.toHexString(), name, tenantId, region);
        stagedDefinitionsBatch.add().bind("id", defId.getSha1Hash()).bind("name", name)
            .bind("tenant_id", tenantId).bind("region", region);
        definitionIdSet.add(defId);
      }

    } else {

      definitionCacheHitMeter.mark();

    }
  }

  private void addDimensionsToBatch(Sha1HashId dimSetId, Map<String, String> dimMap) {

    if (dimensionsIdCache.getIfPresent(dimSetId) == null) {

      dimensionCacheMissMeter.mark();

      if (!dimensionIdSet.contains(dimSetId)) {

        for (Map.Entry<String, String> entry : dimMap.entrySet()) {

          String name = entry.getKey();
          String value = entry.getValue();

          logger.debug("Adding dimension to batch: dimSetId: {}, name: {}, value: {}", dimSetId.toHexString(), name, value);

          stagedDimensionsBatch.add().bind("dimension_set_id", dimSetId.getSha1Hash())
              .bind("name", name).bind("value", value);
        }

        dimensionIdSet.add(dimSetId);
      }

    } else {

      dimensionCacheHitMeter.mark();

    }
  }

  private void addDefinitionDimensionToBatch(Sha1HashId defDimsId, Sha1HashId defId,
                                            Sha1HashId dimId) {

    if (definitionDimensionsIdCache.getIfPresent(defDimsId) == null) {

      definitionDimensionCacheMissMeter.mark();

      if (!definitionDimensionsIdSet.contains(defDimsId)) {

        logger.debug("Adding definitionDimension to batch: defDimsId: {}, defId: {}, dimId: {}",
                     defDimsId.toHexString(), defId, dimId);
        stagedDefinitionDimensionsBatch.add().bind("id", defDimsId.getSha1Hash())
            .bind("definition_id", defId.getSha1Hash())
            .bind("dimension_set_id", dimId.getSha1Hash());

        definitionDimensionsIdSet.add(defDimsId);
      }

    } else {

      definitionDimensionCacheHitMeter.mark();

    }
  }

  @Override
  public void flush(String id) {
    try {
      long startTime = System.currentTimeMillis();
      Timer.Context context = flushTimer.time();
      executeBatches();
      writeRowsFromTempStagingTablesToPermTables();
      handle.commit();
      handle.begin();
      long endTime = System.currentTimeMillis();
      context.stop();
      logger.debug("Writing measurements, definitions, and dimensions to database took "
          + (endTime - startTime) / 1000 + " seconds");
      updateIdCaches();
    } catch (Exception e) {
      logger.error("Failed to write measurements, definitions, or dimensions to database", e);
      if (handle.isInTransaction()) {
        handle.rollback();
      }
      clearTempCaches();
      handle.begin();
    }
  }

  private void executeBatches() {

    metricsBatch.execute();
    stagedDefinitionsBatch.execute();
    stagedDimensionsBatch.execute();
    stagedDefinitionDimensionsBatch.execute();
  }

  private void updateIdCaches() {
    for (Sha1HashId defId : definitionIdSet) {
      definitionsIdCache.put(defId, defId);
    }

    for (Sha1HashId dimId : dimensionIdSet) {
      dimensionsIdCache.put(dimId, dimId);
    }

    for (Sha1HashId defDimsId : definitionDimensionsIdSet) {
      definitionDimensionsIdCache.put(defDimsId, defDimsId);
    }

    clearTempCaches();
  }

  private void writeRowsFromTempStagingTablesToPermTables() {
    handle.execute(definitionsTempStagingTableInsertStmt);
    handle.execute("truncate table " + definitionsTempStagingTableName);
    handle.execute(dimensionsTempStagingTableInsertStmt);
    handle.execute("truncate table " + dimensionsTempStagingTableName);
    handle.execute(definitionDimensionsTempStagingTableInsertStmt);
    handle.execute("truncate table " + definitionDimensionsTempStagingTableName);
  }

  private void clearTempCaches() {
    definitionIdSet.clear();
    dimensionIdSet.clear();
    definitionDimensionsIdSet.clear();
  }

  private Map<String, String> prepDimensions(Map<String, String> dimMap) {

    Map<String, String> newDimMap = new TreeMap<>();

    if (dimMap != null) {
      for (String dimName : dimMap.keySet()) {
        if (dimName != null && !dimName.isEmpty()) {
          String dimValue = dimMap.get(dimName);
          if (dimValue != null && !dimValue.isEmpty()) {
            newDimMap.put(trunc(dimName, MAX_COLUMN_LENGTH), trunc(dimValue, MAX_COLUMN_LENGTH));
            dimensionCounter.inc();
          }
        }
      }
    }
    return newDimMap;
  }

  private String trunc(String s, int l) {

    if (s == null) {
      return "";
    } else if (s.length() <= l) {
      return s;
    } else {
      String r = s.substring(0, l);
      logger.warn("Input string exceeded max column length. Truncating input string {} to {} chars",
                  s, l);
      logger.warn("Resulting string {}", r);
      return r;
    }
  }
}
