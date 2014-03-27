package com.hpcloud.mon.persister.repository;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.PreparedBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class VerticaMetricRepository extends VerticaRepository {

    private static final Logger logger = LoggerFactory.getLogger(VerticaMetricRepository.class);

    private final MonPersisterConfiguration configuration;

    private final Cache<byte[], byte[]> defIdCache;
    private final Set<byte[]> defIdSet = new HashSet<>();

    private static final String SQL_INSERT_INTO_METRICS =
            "insert into MonMetrics.metrics (metric_definition_id, time_stamp, value) values (:metric_definition_id, :time_stamp, :value)";

    private static final String defs = "(" +
            "   metric_definition_id BINARY(20) NOT NULL," +
            "   name VARCHAR NOT NULL," +
            "   tenant_id VARCHAR(14) NOT NULL," +
            "   region VARCHAR" +
            ")";

    private static final String dims = "(" +
            "    metric_definition_id BINARY(20)," +
            "    name VARCHAR NOT NULL," +
            "    value VARCHAR NOT NULL" +
            ")";

    private PreparedBatch metricsBatch;

    private PreparedBatch stagedDefinitionsBatch;
    private PreparedBatch stagedDimensionsBatch;

    private final String sDefs;
    private final String sDims;

    private final String dsDefs;
    private final String dsDims;

    private final Timer commitTimer = Metrics.newTimer(this.getClass(), "commits-timer");
    private final Timer flushTimer = Metrics.newTimer(this.getClass(), "staging-tables-flushed-timer");

    @Inject
    public VerticaMetricRepository(DBI dbi, MonPersisterConfiguration configuration) throws NoSuchAlgorithmException, SQLException {
        super(dbi);
        logger.debug("Instantiating: " + this);

        this.configuration = configuration;

        defIdCache = CacheBuilder.newBuilder()
                .maximumSize(configuration.getVerticaMetricRepositoryConfiguration().getMaxCacheSize()).build();

        logger.info("Building temp staging tables...");

        this.sDefs = this.toString().replaceAll("\\.", "_").replaceAll("\\@", "_") + "_staged_definitions";
        logger.debug("temp staging definitions table: " + sDefs);

        this.sDims = this.toString().replaceAll("\\.", "_").replaceAll("\\@", "_") + "_staged_dimensions";
        logger.debug("temp staging dimensions table: " + sDims);

        this.dsDefs = "insert into  MonMetrics.Definitions select distinct * from " + sDefs + " where metric_definition_id not in (select metric_definition_id from MonMetrics.Definitions)";
        logger.debug("insert stmt: " + dsDefs);

        this.dsDims = "insert into MonMetrics.Dimensions select distinct * from " + sDims + " where metric_definition_id not in (select metric_definition_id from MonMetrics.Dimensions)";
        logger.debug("insert stmt: " + dsDefs);

        handle.execute("drop table if exists " + sDefs + " cascade");
        handle.execute("drop table if exists " + sDims + " cascade");

        handle.execute("create local temp table " + sDefs + " " + defs + " on commit preserve rows");
        handle.execute("create local temp table " + sDims + " " + dims + " on commit preserve rows");

        handle.getConnection().setAutoCommit(false);
        metricsBatch = handle.prepareBatch(SQL_INSERT_INTO_METRICS);
        stagedDefinitionsBatch = handle.prepareBatch("insert into " + sDefs + " values (:metric_definition_id, :name, :tenant_id, :region)");
        stagedDimensionsBatch = handle.prepareBatch("insert into " + sDims + " values (:metric_definition_id, :name, :value)");
        handle.begin();
    }

    public void addToBatchMetrics(byte[] defId, String timeStamp, double value) {
        metricsBatch.add().bind(0, defId).bind(1, timeStamp).bind(2, value);
    }

    public void addToBatchStagingDefinitions(byte[] defId, String name, String tenantId, String region) {
        if (defIdCache.getIfPresent(defId) == null) {
            stagedDefinitionsBatch.add().bind(0, defId).bind(1, name).bind(2, tenantId).bind(3, region);
            defIdSet.add(defId);
        }
    }

    public void addToBatchStagingDimensions(byte[] defId, String name, String value) {
        if (defIdCache.getIfPresent(defId) == null) {
            stagedDimensionsBatch.add().bind(0, defId)
                    .bind(1, name)
                    .bind(2, value);
            defIdSet.add(defId);
        }
    }

    public void flush() {
        commitBatch();
        long startTime = System.currentTimeMillis();
        TimerContext context = flushTimer.time();
        handle.execute(dsDefs);
        handle.execute("truncate table " + sDefs);
        handle.execute(dsDims);
        handle.execute("truncate table " + sDims);
        handle.commit();
        handle.begin();
        context.stop();
        long endTime = System.currentTimeMillis();
        logger.debug("Flushing staging tables took " + (endTime - startTime) / 1000 + " seconds");

    }

    private void commitBatch() {
        long startTime = System.currentTimeMillis();
        TimerContext context = commitTimer.time();
        metricsBatch.execute();
        stagedDefinitionsBatch.execute();
        stagedDimensionsBatch.execute();
        handle.commit();
        updateDefIdCache();
        handle.begin();
        context.stop();
        long endTime = System.currentTimeMillis();
        logger.debug("Committing batch took " + (endTime - startTime) / 1000 + " seconds");
    }

    private void updateDefIdCache() {
        for (byte[] defId : defIdSet) {
            defIdCache.put(defId, defId);
        }
        defIdSet.clear();
    }
}
