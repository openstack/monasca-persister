package com.hpcloud.mon.persister.dedupe;

import com.google.inject.Inject;
import com.hpcloud.mon.persister.disruptor.event.MetricMessageEvent;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonPersisterDeduperHeartbeat implements Managed {

    private static Logger logger = LoggerFactory.getLogger(MonPersisterDeduperHeartbeat.class);

    private final Disruptor disruptor;
    private final DeduperRunnable deduperRunnable;

    @Inject
    public MonPersisterDeduperHeartbeat(Disruptor disruptor) {
        this.disruptor = disruptor;
        this.deduperRunnable = new DeduperRunnable(disruptor);

    }

    @Override
    public void start() throws Exception {

        Thread deduperThread = new Thread(deduperRunnable);
        deduperThread.start();
    }

    @Override
    public void stop() throws Exception {
    }

    private static class DeduperRunnable implements Runnable {

        private static Logger logger = LoggerFactory.getLogger(DeduperRunnable.class);

        private final Disruptor disruptor;

        private DeduperRunnable(Disruptor disruptor) {
            this.disruptor = disruptor;
        }

        @Override
        public void run() {
            for (; ; ) {
                try {
                    // Send a heartbeat every second.
                    Thread.sleep(1000);
                    logger.debug("Waking up after sleeping 1 seconds, yawn...");

                    // Send heartbeat
                    logger.debug("Sending heartbeat message");
                    disruptor.publishEvent(new EventTranslator<MetricMessageEvent>() {

                        @Override
                        public void translateTo(MetricMessageEvent event, long sequence) {
                            event.setMetricEnvelope(null);

                        }
                    });

                } catch (Exception e) {
                    logger.error("Failed to send heartbeat", e);
                }

            }

        }
    }
}
