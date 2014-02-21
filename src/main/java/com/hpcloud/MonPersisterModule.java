package com.hpcloud;

import com.google.inject.AbstractModule;
import com.lmax.disruptor.dsl.Disruptor;

public class MonPersisterModule extends AbstractModule {

    private final MonPersisterConfiguration configuration;
    private final Disruptor disruptor;

    public MonPersisterModule(MonPersisterConfiguration configuration, Disruptor<StringEvent> disruptor) {
        this.configuration = configuration;
        this.disruptor = disruptor;
    }

    @Override
    protected void configure() {
        bind(MonPersisterConfiguration.class).toInstance(configuration);
        bind(Disruptor.class).toInstance(disruptor);
        bind(MonConsumer.class);

    }
}
