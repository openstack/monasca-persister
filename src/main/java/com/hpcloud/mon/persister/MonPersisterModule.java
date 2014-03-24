package com.hpcloud.mon.persister;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.consumer.KafkaConsumerRunnableBasic;
import com.hpcloud.mon.persister.consumer.KafkaConsumerRunnableBasicFactory;
import com.hpcloud.mon.persister.consumer.MonPersisterConsumer;
import com.hpcloud.mon.persister.dbi.DBIProvider;
import com.hpcloud.mon.persister.dedupe.MonPersisterDeduperHeartbeat;
import com.hpcloud.mon.persister.disruptor.DisruptorExceptionHandler;
import com.hpcloud.mon.persister.disruptor.DisruptorProvider;
import com.hpcloud.mon.persister.disruptor.event.MetricMessageEventHandler;
import com.hpcloud.mon.persister.disruptor.event.MetricMessageEventHandlerFactory;
import com.lmax.disruptor.ExceptionHandler;
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
                .implement(MetricMessageEventHandler.class, MetricMessageEventHandler.class)
                .build(MetricMessageEventHandlerFactory.class));

        install(new FactoryModuleBuilder()
                .implement(KafkaConsumerRunnableBasic.class, KafkaConsumerRunnableBasic.class)
                .build(KafkaConsumerRunnableBasicFactory.class));

        bind(ObjectMapper.class);

        bind(ExceptionHandler.class).to(DisruptorExceptionHandler.class);

        bind(Disruptor.class)
                .toProvider(DisruptorProvider.class).in(Scopes.SINGLETON);

        bind(DBI.class).toProvider(DBIProvider.class).in(Scopes.SINGLETON);

        bind(MonPersisterConsumer.class);
        bind(MonPersisterDeduperHeartbeat.class);

    }
}
