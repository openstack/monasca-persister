package com.hpcloud.mon.persister.consumer;

import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;

import javax.inject.Inject;
import javax.inject.Provider;

public class KafkaStreamsProvider implements Provider<KafkaStreams> {

    private final MonPersisterConfiguration configuration;

    @Inject
    public KafkaStreamsProvider(MonPersisterConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public KafkaStreams get() {
        return new KafkaStreams(configuration);
    }
}