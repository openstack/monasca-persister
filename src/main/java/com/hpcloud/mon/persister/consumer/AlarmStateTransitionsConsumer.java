package com.hpcloud.mon.persister.consumer;

import com.google.inject.Inject;
import com.hpcloud.mon.persister.disruptor.AlarmStateHistoryDisruptor;

public class AlarmStateTransitionsConsumer extends Consumer {

    @Inject
    public AlarmStateTransitionsConsumer(KafkaAlarmStateTransitionConsumer kafkaConsumer, AlarmStateHistoryDisruptor disruptor) {
        super(kafkaConsumer, disruptor);
    }
}
