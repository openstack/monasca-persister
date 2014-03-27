package com.hpcloud.mon.persister.consumer;

import kafka.consumer.KafkaStream;

public interface KafkaAlarmStateTransitionConsumerRunnableBasicFactory {
    KafkaAlarmStateTransitionConsumerRunnableBasic create(KafkaStream stream, int threadNumber);
}
