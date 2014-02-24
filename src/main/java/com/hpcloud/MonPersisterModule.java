package com.hpcloud;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.hpcloud.configuration.MonPersisterConfiguration;
import com.hpcloud.consumer.KafkaConsumerRunnableBasic;
import com.hpcloud.consumer.KafkaConsumerRunnableBasicFactory;
import com.hpcloud.consumer.MonConsumer;
import com.hpcloud.disruptor.DisruptorProvider;
import com.hpcloud.event.StringEventHandler;
import com.hpcloud.event.StringEventHandlerFactory;
import com.lmax.disruptor.dsl.Disruptor;
import com.yammer.dropwizard.config.Environment;
import org.skife.jdbi.v2.DBI;

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

        install(new FactoryModuleBuilder()
                .implement(StringEventHandler.class, StringEventHandler.class)
                .build(StringEventHandlerFactory.class));

        install(new FactoryModuleBuilder()
                .implement(KafkaConsumerRunnableBasic.class, KafkaConsumerRunnableBasic.class)
                .build(KafkaConsumerRunnableBasicFactory.class));

        bind(Disruptor.class)
                .toProvider(DisruptorProvider.class).in(Scopes.SINGLETON);

        bind(DBI.class).toProvider(DBIProvider.class).in(Scopes.SINGLETON);

        bind(MonConsumer.class);

    }
}
