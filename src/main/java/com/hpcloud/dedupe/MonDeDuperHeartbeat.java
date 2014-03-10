package com.hpcloud.dedupe;

import com.google.inject.Inject;
import com.hpcloud.disruptor.event.MetricMessageEvent;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonDeDuperHeartbeat implements Managed {

    private static Logger logger = LoggerFactory.getLogger(MonDeDuperHeartbeat.class);

    private final Disruptor disruptor;
    private final DeDuperRunnable deDuperRunnable;

    @Inject
    public MonDeDuperHeartbeat(Disruptor disruptor) {
        this.disruptor = disruptor;
        this.deDuperRunnable = new DeDuperRunnable(disruptor);

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

        private final Disruptor disruptor;

        private DeDuperRunnable(Disruptor disruptor) {
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
