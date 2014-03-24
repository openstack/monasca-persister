package com.hpcloud.mon.persister.consumer;

import kafka.consumer.KafkaStream;

public interface KafkaConsumerRunnableBasicFactory {
    KafkaConsumerRunnableBasic create(KafkaStream stream, int threadNumber);

}
