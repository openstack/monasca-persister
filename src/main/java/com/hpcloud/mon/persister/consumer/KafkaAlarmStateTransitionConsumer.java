package com.hpcloud.mon.persister.consumer;

import com.google.inject.Inject;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import kafka.consumer.KafkaStream;

public class KafkaAlarmStateTransitionConsumer extends KafkaConsumer {

    @Inject
    private KafkaAlarmStateTransitionConsumerRunnableBasicFactory factory;

    @Inject
    public KafkaAlarmStateTransitionConsumer(MonPersisterConfiguration configuration) {
        super(configuration);
    }

    @Override
    protected Runnable createRunnable(KafkaStream stream, int threadNumber) {
        return factory.create(stream, threadNumber);
    }

    @Override
    protected String getStreamName() {
        return "alarm-state-transitions";
    }
}
