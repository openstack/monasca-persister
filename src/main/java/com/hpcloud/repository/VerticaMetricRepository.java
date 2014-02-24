package com.hpcloud.repository;

import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.security.NoSuchAlgorithmException;

public class VerticaMetricRepository extends VerticaRepository {

    private static final Logger logger = LoggerFactory.getLogger(VerticaMetricRepository.class);

    private static final String SQL_INSERT_INTO_METRICS =
            "INSERT INTO test.metric VALUES (:metric)";

    private static final String METRIC_COLUMN_NAME = "metric";

    @Inject
    public VerticaMetricRepository(DBI dbi) throws NoSuchAlgorithmException {
        super(dbi);
        batch = handle.prepareBatch(SQL_INSERT_INTO_METRICS);
    }

    public void addToBatch(String aString) {
        batch.add().bind(METRIC_COLUMN_NAME, aString);
    }

    public void commitBatch() {
        batch.execute();
    }
}
