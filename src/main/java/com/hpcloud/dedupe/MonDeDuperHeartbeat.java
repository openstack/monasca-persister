package com.hpcloud.dedupe;

import com.google.inject.Inject;
import com.hpcloud.configuration.MonPersisterConfiguration;
import com.hpcloud.disruptor.event.MetricMessageEvent;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonDeDuperHeartbeat implements Managed {

    private static Logger logger = LoggerFactory.getLogger(MonDeDuperHeartbeat.class);

    private final MonPersisterConfiguration configuration;
    private final Disruptor disruptor;
    private final DeDuperRunnable deDuperRunnable;

    @Inject
    public MonDeDuperHeartbeat(MonPersisterConfiguration configuration,
                               Disruptor disruptor) {
        this.configuration = configuration;
        this.disruptor = disruptor;
        this.deDuperRunnable = new DeDuperRunnable(configuration, disruptor);

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
        private final Disruptor disruptor;

        private DeDuperRunnable(MonPersisterConfiguration configuration, Disruptor disruptor) {
            this.configuration = configuration;
            this.disruptor = disruptor;
        }

        @Override
        public void run() {
            int seconds = configuration.getMonDeDuperConfiguration().getDedupeRunFrequencySeconds();
            for (; ; ) {
                try {
                    Thread.sleep(seconds * 1000);
                    logger.debug("Waking up after sleeping " + seconds + " seconds, yawn...");

                    // Send heartbeat
                    logger.debug("Sending dedupe heartbeat message");
                    disruptor.publishEvent(new EventTranslator<MetricMessageEvent>() {

                        @Override
                        public void translateTo(MetricMessageEvent event, long sequence) {
                            event.setMetricEnvelope(null);

                        }
                    });

                } catch (Exception e) {
                    logger.error("Failed to send dedupe heartbeat", e);
                }

            }

        }
    }
}
