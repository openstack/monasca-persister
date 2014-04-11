package com.hpcloud.mon.persister.consumer;

import kafka.consumer.KafkaStream;

public interface KafkaMetricsConsumerRunnableBasicFactory {
    KafkaMetricsConsumerRunnableBasic create(KafkaStream stream, int threadNumber);
}
