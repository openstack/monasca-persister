package com.hpcloud.consumer;

import kafka.consumer.KafkaStream;

public interface KafkaConsumerRunnableBasicFactory {
    KafkaConsumerRunnableBasic create(KafkaStream stream, int threadNumber);

}
