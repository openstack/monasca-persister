package com.hpcloud.repository;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.PreparedBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

public class VerticaMetricRepository extends VerticaRepository {

    private static final Logger logger = LoggerFactory.getLogger(VerticaMetricRepository.class);

    private static final String SQL_INSERT_INTO_METRICS =
            "insert into MonMetrics.metrics (metric_definition_id, time_stamp, value) values (:metric_definition_id, :time_stamp, :value)";
    private static final String SQL_INSERT_INTO_STAGING_DEFINITIONS =
            "insert into MonMetrics.stagedDefinitions values (:metric_definition_id, :name, :tenant_id," +
                    ":region)";
    private static final String SQL_INSERT_INTO_STAGING_DIMENSIONS =
            "insert into MonMetrics.stagedDimensions values (:metric_definition_id, :name, :value)";

    private static final String METRIC_DEF_ID_COL_NAME = "metric_definition_id";
    private static final String TIME_STAMP_COL_NAME = "time_stamp";
    private static final String VALUE_COL_NAME = "value";
    private static final String NAME_COL_NAME = "name";
    private static final String TENANT_ID_COL_NAME = "tenant_id";
    private static final String REGION_COL_NAME = "region";

    private PreparedBatch metricsBatch;

    private PreparedBatch stagedDefinitionsBatch;
    private PreparedBatch stagedDimensionsBatch;

    @Inject
    public VerticaMetricRepository(DBI dbi) throws NoSuchAlgorithmException, SQLException {
        super(dbi);
        handle.getConnection().setAutoCommit(false);
        metricsBatch = handle.prepareBatch(SQL_INSERT_INTO_METRICS);
        stagedDefinitionsBatch = handle.prepareBatch(SQL_INSERT_INTO_STAGING_DEFINITIONS);
        stagedDimensionsBatch = handle.prepareBatch(SQL_INSERT_INTO_STAGING_DIMENSIONS);
        handle.begin();
    }

    public void addToBatchMetrics(byte[] defId, String timeStamp, double value) {
        metricsBatch.add().bind(0, defId).bind(1, timeStamp).bind(2, value);
    }

    public void addToBatchStagingDefinitions(byte[] defId, String name, String tenantId, String region) {
        stagedDefinitionsBatch.add().bind(0, defId).bind(1, name).bind(2, tenantId).bind(3, region);
    }

    public void addToBatchStagingDimensions(byte[] defId, String name, String value) {
        stagedDimensionsBatch.add().bind(0, defId)
                .bind(1, name)
                .bind(2, value);
    }

    public void commitBatch() {
        metricsBatch.execute();
        stagedDefinitionsBatch.execute();
        stagedDimensionsBatch.execute();
        handle.commit();
        handle.begin();
    }
}
