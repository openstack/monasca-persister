package com.hpcloud;

import com.google.inject.AbstractModule;

public class MonPersisterModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(KafkaConsumer.class);
    }
}
