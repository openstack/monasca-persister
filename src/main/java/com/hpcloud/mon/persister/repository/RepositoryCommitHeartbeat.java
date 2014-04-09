package com.hpcloud.mon.persister.repository;

import com.google.inject.Inject;
import com.hpcloud.mon.persister.disruptor.AlarmStateHistoryDisruptor;
import com.hpcloud.mon.persister.disruptor.MetricDisruptor;
import com.hpcloud.mon.persister.disruptor.event.AlarmStateTransitionedMessageEvent;
import com.hpcloud.mon.persister.disruptor.event.MetricMessageEvent;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepositoryCommitHeartbeat implements Managed {

    private static Logger logger = LoggerFactory.getLogger(RepositoryCommitHeartbeat.class);

    private final MetricDisruptor metricDisruptor;
    private final AlarmStateHistoryDisruptor alarmHistoryDisruptor;
    private final HeartbeatRunnable deduperRunnable;

    @Inject
    public RepositoryCommitHeartbeat(MetricDisruptor metricDisruptor, AlarmStateHistoryDisruptor alarmHistoryDisruptor) {
        this.metricDisruptor = metricDisruptor;
        this.alarmHistoryDisruptor = alarmHistoryDisruptor;
        this.deduperRunnable = new HeartbeatRunnable(metricDisruptor, alarmHistoryDisruptor);
    }

    @Override
    public void start() throws Exception {

        Thread heartbeatThread = new Thread(deduperRunnable);
        heartbeatThread.start();
    }

    @Override
    public void stop() throws Exception {
    }

    private static class HeartbeatRunnable implements Runnable {

        private static final Logger logger = LoggerFactory.getLogger(HeartbeatRunnable.class);
        private final Disruptor metricDisruptor;
        private final Disruptor alarmHistoryDisruptor;

        private HeartbeatRunnable(Disruptor metricDisruptor, Disruptor alarmHistoryDisruptor) {
            this.metricDisruptor = metricDisruptor;
            this.alarmHistoryDisruptor = alarmHistoryDisruptor;
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
                    metricDisruptor.publishEvent(new EventTranslator<MetricMessageEvent>() {

                        @Override
                        public void translateTo(MetricMessageEvent event, long sequence) {
                            event.setEnvelope(null);
                        }
                    });
                    alarmHistoryDisruptor.publishEvent(new EventTranslator<AlarmStateTransitionedMessageEvent>() {

                        @Override
                        public void translateTo(AlarmStateTransitionedMessageEvent event, long sequence) {
                            event.setMessage(null);
                        }
                    });

                } catch (Exception e) {
                    logger.error("Failed to send heartbeat", e);
                }

            }

        }
    }
}
