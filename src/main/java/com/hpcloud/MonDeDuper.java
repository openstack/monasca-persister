package com.hpcloud;

import com.google.inject.Inject;
import com.hpcloud.configuration.MonPersisterConfiguration;
import com.yammer.dropwizard.lifecycle.Managed;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class MonDeDuper implements Managed {

    private static Logger logger = LoggerFactory.getLogger(MonDeDuper.class);

    private final MonPersisterConfiguration configuration;
    private final DBI dbi;
    private final DeDuperRunnable deDuperRunnable;
    private Thread deduperThread;

    @Inject
    public MonDeDuper(MonPersisterConfiguration configuration,
                      DBI dbi) {
        this.configuration = configuration;
        this.dbi = dbi;
        this.deDuperRunnable = new DeDuperRunnable(configuration, dbi);

    }

    @Override
    public void start() throws Exception {

        Thread deduperThread = new Thread(deDuperRunnable);
        deduperThread.start();
    }

    @Override
    public void stop() throws Exception {
    }

    private static class DeDuperRunnable implements Runnable {

        private static Logger logger = LoggerFactory.getLogger(DeDuperRunnable.class);

        private final MonPersisterConfiguration configuration;
        private final DBI dbi;
        private final Handle handle;

        private static final String DEDUPE_STAGING_DEFS =
                "insert into MonMetrics.Definitions select distinct * from MonMetrics.StagedDefinitions where metric_definition_id not in (select metric_definition_id from MonMetrics.Definitions)";

        private static final String DEDEUP_STAGING_DIMS =
                "insert into MonMetrics.Dimensions select distinct * from MonMetrics.StagedDimensions where metric_definition_id not in (select metric_definition_id from MonMetrics.Dimensions)";

        private static final String TRUNCATE_STAGING_DEFS =
                "truncate table monmetrics.stageddefinitions";

        private static final String TRUNCATE_STAGING_DIMS =
                "truncate table monmetrics.stageddimensions";

        private DeDuperRunnable(MonPersisterConfiguration configuration, DBI dbi) {
            this.configuration = configuration;
            this.dbi = dbi;
            this.handle = this.dbi.open();
            this.handle.execute("SET TIME ZONE TO 'UTC'");
            try {
                this.handle.getConnection().setAutoCommit(false);
            } catch (SQLException e) {
                logger.error("Failed to set autocommit to false", e);
                System.exit(-1);
            }

        }

        @Override
        public void run() {
            int seconds = configuration.getMonDeDuperConfiguration().getDedupeRunFrequencySeconds();
            for (; ; ) {
                try {
                    Thread.sleep(seconds * 1000);
                    handle.begin();
                    logger.debug("Waited " + seconds + " seconds");

                    logger.debug("Executing: " + DEDUPE_STAGING_DEFS);
                    handle.execute(DEDUPE_STAGING_DEFS);

                    logger.debug("Executing: " + TRUNCATE_STAGING_DEFS);
                    handle.execute(TRUNCATE_STAGING_DEFS);

                    handle.commit();

                    handle.begin();
                    logger.debug("Executing: " + DEDEUP_STAGING_DIMS);
                    handle.execute(DEDEUP_STAGING_DIMS);

                    logger.debug("Executing: " + TRUNCATE_STAGING_DIMS);
                    handle.execute(TRUNCATE_STAGING_DIMS);
                    handle.commit();

                } catch (InterruptedException e) {
                    logger.warn("Failed to wait for " + seconds + " between deduping", e);
                }

            }

        }
    }
}
