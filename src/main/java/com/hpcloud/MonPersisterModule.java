package com.hpcloud;

import com.google.inject.AbstractModule;
import com.yammer.dropwizard.config.Environment;

public class MonPersisterModule extends AbstractModule {

    private final MonPersisterConfiguration configuration;
    private final Environment environment;

    public MonPersisterModule(MonPersisterConfiguration configuration, Environment environment) {
        this.configuration = configuration;
        this.environment = environment;
    }

    @Override
    protected void configure() {
        bind(MonPersisterConfiguration.class).toInstance(configuration);
        bind(Environment.class).toInstance(environment);
        bind(MonConsumer.class);

    }
}
