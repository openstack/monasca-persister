package com.hpcloud.mon.persister.consumer;

import com.google.inject.Inject;
import com.hpcloud.mon.persister.disruptor.AlarmStateHistoryDisruptor;
import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlarmStateTransitionsConsumer implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(AlarmStateTransitionsConsumer.class);
    private final KafkaAlarmStateTransitionConsumer consumer;
    private final AlarmStateHistoryDisruptor disruptor;

    @Inject
    public AlarmStateTransitionsConsumer(KafkaAlarmStateTransitionConsumer kafkaConsumer, AlarmStateHistoryDisruptor disruptor) {
        this.consumer = kafkaConsumer;
        this.disruptor = disruptor;
    }

    @Override
    public void start() throws Exception {
        logger.debug("start");
        consumer.run();
    }

    @Override
    public void stop() throws Exception {
        logger.debug("stop");
        consumer.stop();
        disruptor.shutdown();
    }
}
