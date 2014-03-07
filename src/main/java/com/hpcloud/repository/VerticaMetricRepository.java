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

    @Inject
    public VerticaMetricRepository(DBI dbi) throws NoSuchAlgorithmException, SQLException {
        super(dbi);
        logger.debug("Instantiating: " + this);

        String sDefs = this.toString().replaceAll(".", "_").replaceAll("@", "_") + "staged_definitions";
        String sDims = this.toString().replaceAll(".", "_").replaceAll("@", "_") + "staged_dimensions";
        handle.execute("drop table if exists" + sDefs + " cascade");
        handle.execute("drop table if exists" + sDims + " cascade");

        handle.execute("create local temp table " + sDefs + " " + defs + " on commit preserve rows");
        handle.execute("create local temp table " + sDims + " " + dims + " on commit preserve rows");

        handle.getConnection().setAutoCommit(false);
        metricsBatch = handle.prepareBatch(SQL_INSERT_INTO_METRICS);
        stagedDefinitionsBatch = handle.prepareBatch("insert into " + sDefs + " values (:metric_definition_id, :name, :tenant_id, ");
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
