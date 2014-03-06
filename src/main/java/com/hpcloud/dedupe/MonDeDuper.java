package com.hpcloud.dedupe;

import com.google.inject.Inject;
import com.hpcloud.configuration.MonPersisterConfiguration;
import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
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
        private final Timer dedupeTimer = Metrics.newTimer(this.getClass(), "dedupe-execution-timer");

        private static final String DEDUPE_STAGING_DEFS =
                "insert into MonMetrics.Definitions select distinct * from MonMetrics.StagedDefinitions where metric_definition_id not in (select metric_definition_id from MonMetrics.Definitions)";

        private static final String DEDEUP_STAGING_DIMS =
                "insert into MonMetrics.Dimensions select distinct * from MonMetrics.StagedDimensions where metric_definition_id not in (select metric_definition_id from MonMetrics.Dimensions)";

        private static final String DELETE_STAGING_DEFS =
                "delete from monmetrics.stageddefinitions where metric_definition_id in (select metric_definition_id from MonMetrics.Definitions)";

        private static final String PURGE_STAGING_DEFS =
                "select purge_table('monmetrics.stageddefinitions')";

        private static final String DELETE_STAGING_DIMS =
                "delete from monmetrics.stageddimensions where metric_definition_id in (select metric_definition_id from MonMetrics.Dimensions)";

        private static final String PURGE_STAGING_DIMS =
                "select purge_table('monmetrics.stageddimensions')";

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
            long startTime;
            long endTime;
            for (; ; ) {
                try {
                    Thread.sleep(seconds * 1000);
                    logger.debug("Waking up after sleeping " + seconds + " seconds, yawn...");

                    TimerContext context = dedupeTimer.time();

                    handle.begin();

                    startTime = System.currentTimeMillis();
                    logger.debug("Executing: " + DEDUPE_STAGING_DEFS);
                    handle.execute(DEDUPE_STAGING_DEFS);
                    logger.debug("Executing: " + DELETE_STAGING_DEFS);
                    handle.execute(DELETE_STAGING_DEFS);
                    logger.debug("Executing: " + PURGE_STAGING_DEFS);
                    handle.execute(PURGE_STAGING_DEFS);
                    handle.commit();

                    endTime = System.currentTimeMillis();
                    logger.debug("Deduping metric defintitions took " + (endTime - startTime) / 1000 + " seconds");

                    handle.begin();

                    startTime = System.currentTimeMillis();
                    logger.debug("Executing: " + DEDEUP_STAGING_DIMS);
                    handle.execute(DEDEUP_STAGING_DIMS);
                    logger.debug("Executing: " + DELETE_STAGING_DIMS);
                    handle.execute(DELETE_STAGING_DIMS);
                    logger.debug("Executing: " + PURGE_STAGING_DIMS);
                    handle.execute(PURGE_STAGING_DIMS);
                    handle.commit();
                    endTime = System.currentTimeMillis();
                    logger.debug("Deduping metric dimensions took " + (endTime - startTime) / 1000 + " seconds");

                    context.stop();

                } catch (InterruptedException e) {
                    logger.warn("Failed to wait for " + seconds + " between deduping", e);
                }

            }

        }
    }
}
