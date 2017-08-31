/*
 * (C) Copyright 2014-2016 Hewlett Packard Enterprise Development LP
 *
 * (C) Copyright 2017 SUSE LLC.
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

import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import javax.inject.Inject;

import io.dropwizard.setup.Environment;
import monasca.common.model.metric.Metric;
import monasca.common.model.metric.MetricEnvelope;
import monasca.persister.configuration.PersisterConfig;
import monasca.persister.repository.Repo;
import monasca.persister.repository.RepoException;
import monasca.persister.repository.Sha1HashId;

public class VerticaMetricRepo extends VerticaRepo implements Repo<MetricEnvelope> {

  private static final Logger logger = LoggerFactory.getLogger(VerticaMetricRepo.class);

  public static final int MAX_COLUMN_LENGTH = 255;

  public static final int MAX_VALUE_META_LENGTH = 2048;

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

  private int measurementCnt = 0;

  private final ObjectMapper objectMapper = new ObjectMapper();

  private static final String SQL_INSERT_INTO_METRICS =
      "insert into MonMetrics.measurements (definition_dimensions_id, time_stamp, value, value_meta) "
      + "values (:definition_dimension_id, :time_stamp, :value, :value_meta)";

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

  private final Timer commitTimer;

  public final Meter measurementMeter;
  public final Meter definitionCacheMissMeter;
  public final Meter dimensionCacheMissMeter;
  public final Meter definitionDimensionCacheMissMeter;
  public final Meter definitionCacheHitMeter;
  public final Meter dimensionCacheHitMeter;
  public final Meter definitionDimensionCacheHitMeter;

  @Inject
  public VerticaMetricRepo(
      DBI dbi,
      PersisterConfig configuration,
      Environment environment) throws NoSuchAlgorithmException, SQLException {

    super(dbi);

    logger.debug("Instantiating " + this.getClass().getName());

    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT-0"));

    this.environment = environment;

    this.commitTimer =
        this.environment.metrics().timer(this.getClass().getName() + "." + "commit-timer");

    this.measurementMeter =
        this.environment.metrics()
            .meter(this.getClass().getName() + "." + "measurement-meter");

    this.definitionCacheMissMeter =
        this.environment.metrics()
            .meter(this.getClass().getName() + "." + "definition-cache-miss-meter");

    this.dimensionCacheMissMeter =
        this.environment.metrics()
            .meter(this.getClass().getName() + "." + "dimension-cache-miss-meter");

    this.definitionDimensionCacheMissMeter =
        this.environment.metrics()
            .meter(this.getClass().getName() + "." + "definition-dimension-cache-miss-meter");

    this.definitionCacheHitMeter =
        this.environment.metrics()
            .meter(this.getClass().getName() + "." + "definition-cache-hit-meter");

    this.dimensionCacheHitMeter =
        this.environment.metrics()
            .meter(this.getClass().getName() + "." + "dimension-cache-hit-meter");

    this.definitionDimensionCacheHitMeter =
        this.environment.metrics()
            .meter(this.getClass().getName() + "." + "definition-dimension-cache-hit-meter");

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
        "merge into MonMetrics.Definitions tgt"
        + " using " + this.definitionsTempStagingTableName + " src"
        + " on src.id = tgt.id"
        + " when not matched then insert values(src.id, src.name, src.tenant_id, src.region)";

    logger.debug("definitions insert stmt: " + definitionsTempStagingTableInsertStmt);

    this.dimensionsTempStagingTableInsertStmt =
        "merge into MonMetrics.Dimensions tgt "
        + " using " + this.dimensionsTempStagingTableName + " src"
        + " on src.dimension_set_id = tgt.dimension_set_id"
        + " and src.name = tgt.name"
        + " and src.value = tgt.value"
        + " when not matched then insert values(src.dimension_set_id, src.name, src.value)";

    logger.debug("dimensions insert stmt: " + definitionsTempStagingTableInsertStmt);

    this.definitionDimensionsTempStagingTableInsertStmt =
        "merge into MonMetrics.definitionDimensions tgt"
        + " using " + this.definitionDimensionsTempStagingTableName + " src"
        + " on src.id = tgt.id"
        + " when not matched then insert values(src.id, src.definition_id, src.dimension_set_id)";

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

    logger.debug(this.getClass().getName() + " is fully instantiated");
  }

  @Override
  public void addToBatch(MetricEnvelope metricEnvelope, String id) {

    Metric metric = metricEnvelope.metric;
    Map<String, Object> metaMap = metricEnvelope.meta;

    String tenantId = getMeta(TENANT_ID, metric, metaMap, id);

    String region = getMeta(REGION, metric, metaMap, id);

    // Add the definition to the batch.
    StringBuilder definitionIdStringToHash =
        new StringBuilder(trunc(metric.getName(), MAX_COLUMN_LENGTH, id));

    definitionIdStringToHash.append(trunc(tenantId, MAX_COLUMN_LENGTH, id));

    definitionIdStringToHash.append(trunc(region, MAX_COLUMN_LENGTH, id));

    byte[] definitionIdSha1Hash = DigestUtils.sha(definitionIdStringToHash.toString());

    Sha1HashId definitionSha1HashId = new Sha1HashId((definitionIdSha1Hash));

    addDefinitionToBatch(definitionSha1HashId, trunc(metric.getName(), MAX_COLUMN_LENGTH, id),
                         trunc(tenantId, MAX_COLUMN_LENGTH, id),
                         trunc(region, MAX_COLUMN_LENGTH, id), id);

    // Calculate dimensions sha1 hash id.
    StringBuilder dimensionIdStringToHash = new StringBuilder();

    Map<String, String> preppedDimMap = prepDimensions(metric.getDimensions(), id);

    for (Map.Entry<String, String> entry : preppedDimMap.entrySet()) {

      dimensionIdStringToHash.append(entry.getKey());

      dimensionIdStringToHash.append(entry.getValue());
    }

    byte[] dimensionIdSha1Hash = DigestUtils.sha(dimensionIdStringToHash.toString());

    Sha1HashId dimensionsSha1HashId = new Sha1HashId(dimensionIdSha1Hash);

    // Add the dimension name/values to the batch.
    addDimensionsToBatch(dimensionsSha1HashId, preppedDimMap, id);

    // Add the definition dimensions to the batch.
    StringBuilder definitionDimensionsIdStringToHash =
        new StringBuilder(definitionSha1HashId.toHexString());

    definitionDimensionsIdStringToHash.append(dimensionsSha1HashId.toHexString());

    byte[] definitionDimensionsIdSha1Hash =
        DigestUtils.sha(definitionDimensionsIdStringToHash.toString());

    Sha1HashId definitionDimensionsSha1HashId = new Sha1HashId(definitionDimensionsIdSha1Hash);

    addDefinitionDimensionToBatch(definitionDimensionsSha1HashId, definitionSha1HashId,
                                  dimensionsSha1HashId, id);

    // Add the measurement to the batch.
    String timeStamp = simpleDateFormat.format(new Date(metric.getTimestamp()));

    double value = metric.getValue();

    addMetricToBatch(definitionDimensionsSha1HashId, timeStamp, value, metric.getValueMeta(), id);

  }

  private String getMeta(String name, Metric metric, Map<String, Object> meta, String id) {

    if (meta.containsKey(name)) {

      return (String) meta.get(name);

    } else {

      logger.warn(
          "[{}]: failed to find {} in message envelope meta data. metric message may be malformed. "
          + "setting {} to empty string.", id, name);

      logger.warn("[{}]: metric: {}", id, metric.toString());

      logger.warn("[{}]: meta: {}", id, meta.toString());

      return "";
    }
  }

  public void addMetricToBatch(Sha1HashId defDimsId, String timeStamp, double value,
                               Map<String, String> valueMeta, String id) {

    String valueMetaString = getValueMetaString(valueMeta, id);

    logger.debug("[{}]: adding metric to batch: defDimsId: {}, time: {}, value: {}, value meta {}",
                 id, defDimsId.toHexString(), timeStamp, value, valueMetaString);

    metricsBatch.add()
        .bind("definition_dimension_id", defDimsId.getSha1Hash())
        .bind("time_stamp", timeStamp)
        .bind("value", value)
        .bind("value_meta", valueMetaString);

    this.measurementCnt++;

    measurementMeter.mark();
  }

  private String getValueMetaString(Map<String, String> valueMeta, String id) {

    String valueMetaString = "";

    if (valueMeta != null && !valueMeta.isEmpty()) {

      try {

        valueMetaString = this.objectMapper.writeValueAsString(valueMeta);
        if (valueMetaString.length() > MAX_VALUE_META_LENGTH) {
          logger
              .error("[{}]: Value meta length {} longer than maximum {}, dropping value meta",
                     id, valueMetaString.length(), MAX_VALUE_META_LENGTH);
          return "";
        }

      } catch (JsonProcessingException e) {

        logger
            .error("[{}]: Failed to serialize value meta {}, dropping value meta from measurement",
                   id, valueMeta);
      }
    }

    return valueMetaString;
  }

  private void addDefinitionToBatch(Sha1HashId defId, String name, String tenantId, String region, String id) {

    if (definitionsIdCache.getIfPresent(defId) == null) {

      definitionCacheMissMeter.mark();

      if (!definitionIdSet.contains(defId)) {

        logger.debug("[{}]: adding definition to batch: defId: {}, name: {}, tenantId: {}, region: {}",
                     id, defId.toHexString(), name, tenantId, region);

        stagedDefinitionsBatch.add()
            .bind("id", defId.getSha1Hash())
            .bind("name", name)
            .bind("tenant_id", tenantId)
            .bind("region", region);

        definitionIdSet.add(defId);

      }

    } else {

      definitionCacheHitMeter.mark();

    }
  }

  private void addDimensionsToBatch(Sha1HashId dimSetId, Map<String, String> dimMap, String id) {

    if (dimensionsIdCache.getIfPresent(dimSetId) == null) {

      dimensionCacheMissMeter.mark();

      if (!dimensionIdSet.contains(dimSetId)) {

        for (Map.Entry<String, String> entry : dimMap.entrySet()) {

          String name = entry.getKey();
          String value = entry.getValue();

          logger.debug(
              "[{}]: adding dimension to batch: dimSetId: {}, name: {}, value: {}",
              id, dimSetId.toHexString(), name, value);

          stagedDimensionsBatch.add()
              .bind("dimension_set_id", dimSetId.getSha1Hash())
              .bind("name", name)
              .bind("value", value);
        }

        dimensionIdSet.add(dimSetId);
      }

    } else {

      dimensionCacheHitMeter.mark();

    }
  }

  private void addDefinitionDimensionToBatch(Sha1HashId defDimsId, Sha1HashId defId,
                                            Sha1HashId dimId, String id) {

    if (definitionDimensionsIdCache.getIfPresent(defDimsId) == null) {

      definitionDimensionCacheMissMeter.mark();

      if (!definitionDimensionsIdSet.contains(defDimsId)) {

        logger.debug("[{}]: adding definitionDimension to batch: defDimsId: {}, defId: {}, dimId: {}",
                     id, defDimsId.toHexString(), defId, dimId);

        stagedDefinitionDimensionsBatch.add()
            .bind("id", defDimsId.getSha1Hash())
            .bind("definition_id", defId.getSha1Hash())
            .bind("dimension_set_id", dimId.getSha1Hash());

        definitionDimensionsIdSet.add(defDimsId);
      }

    } else {

      definitionDimensionCacheHitMeter.mark();

    }
  }

  @Override
  public int flush(String id) throws RepoException {

    try {

      Stopwatch swOuter = Stopwatch.createStarted();

      Timer.Context context = commitTimer.time();

      executeBatches(id);

      writeRowsFromTempStagingTablesToPermTables(id);

      Stopwatch swInner = Stopwatch.createStarted();

      handle.commit();
      swInner.stop();

      logger.debug("[{}]: committing transaction took: {}", id, swInner);

      swInner.reset().start();
      handle.begin();
      swInner.stop();

      logger.debug("[{}]: beginning new transaction took: {}", id, swInner);

      context.stop();

      swOuter.stop();

      logger.debug("[{}]: total time for writing measurements, definitions, and dimensions to vertica took {}",
                   id, swOuter);

      updateIdCaches(id);

      int commitCnt = this.measurementCnt;

      this.measurementCnt = 0;

      return commitCnt;

    } catch (Exception e) {

      logger.error("[{}]: failed to write measurements, definitions, and dimensions to vertica", id,
                   e);

      throw new RepoException("failed to commit batch to vertica", e);

    }
  }

  private void executeBatches(String id) {

    Stopwatch sw = Stopwatch.createStarted();

    metricsBatch.execute();

    stagedDefinitionsBatch.execute();

    stagedDimensionsBatch.execute();

    stagedDefinitionDimensionsBatch.execute();

    sw.stop();

    logger.debug("[{}]: executing batches took {}: ", id, sw);

  }

  private void updateIdCaches(String id) {

    Stopwatch sw = Stopwatch.createStarted();

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

    sw.stop();

    logger.debug("[{}]: clearing temp caches took: {}", id, sw);

  }

  private void writeRowsFromTempStagingTablesToPermTables(String id) {

    Stopwatch sw = Stopwatch.createStarted();

    handle.execute(definitionsTempStagingTableInsertStmt);
    handle.execute("truncate table " + definitionsTempStagingTableName);
    sw.stop();

    logger.debug("[{}]: flushing definitions temp staging table took: {}", id, sw);

    sw.reset().start();
    handle.execute(dimensionsTempStagingTableInsertStmt);
    handle.execute("truncate table " + dimensionsTempStagingTableName);
    sw.stop();

    logger.debug("[{}]: flushing dimensions temp staging table took: {}", id, sw);

    sw.reset().start();
    handle.execute(definitionDimensionsTempStagingTableInsertStmt);
    handle.execute("truncate table " + definitionDimensionsTempStagingTableName);
    sw.stop();

    logger.debug("[{}]: flushing definition dimensions temp staging table took: {}", id, sw);
  }

  private void clearTempCaches() {

    definitionIdSet.clear();
    dimensionIdSet.clear();
    definitionDimensionsIdSet.clear();

  }

  private Map<String, String> prepDimensions(Map<String, String> dimMap, String id) {

    Map<String, String> newDimMap = new TreeMap<>();

    if (dimMap != null) {

      for (String dimName : dimMap.keySet()) {

        if (dimName != null && !dimName.isEmpty()) {

          String dimValue = dimMap.get(dimName);

          if (dimValue != null && !dimValue.isEmpty()) {

            newDimMap.put(trunc(dimName, MAX_COLUMN_LENGTH, id),
                          trunc(dimValue, MAX_COLUMN_LENGTH, id));

          }
        }
      }
    }

    return newDimMap;

  }

  private String trunc(String s, int l, String id) {

    if (s == null) {

      return "";

    } else if (s.length() <= l) {

      return s;

    } else {

      String r = s.substring(0, l);

      logger.warn( "[{}]: input string exceeded max column length. truncating input string {} to {} chars",
                   id, s, l);

      logger.warn("[{}]: resulting string {}", id, r);

      return r;
    }
  }
}
