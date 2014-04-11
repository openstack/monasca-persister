package com.hpcloud.mon.persister.consumer;

import com.google.inject.Inject;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import kafka.consumer.KafkaStream;

public class KafkaMetricsConsumer extends KafkaConsumer {

    @Inject
    private KafkaMetricsConsumerRunnableBasicFactory factory;

    @Inject
    public KafkaMetricsConsumer(MonPersisterConfiguration configuration) {
        super(configuration);
    }

    @Override
    protected Runnable createRunnable(KafkaStream stream, int threadNumber) {
        return factory.create(stream, threadNumber);
    }

    @Override
    protected String getStreamName() {
        return "metrics";
    }
}
