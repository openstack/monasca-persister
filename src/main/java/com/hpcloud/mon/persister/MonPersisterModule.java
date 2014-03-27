package com.hpcloud.mon.persister;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.consumer.*;
import com.hpcloud.mon.persister.dbi.DBIProvider;
import com.hpcloud.mon.persister.disruptor.*;
import com.hpcloud.mon.persister.disruptor.event.AlarmStateTransitionMessageEventHandler;
import com.hpcloud.mon.persister.disruptor.event.AlarmStateTransitionMessageEventHandlerFactory;
import com.hpcloud.mon.persister.disruptor.event.MetricMessageEventHandler;
import com.hpcloud.mon.persister.disruptor.event.MetricMessageEventHandlerFactory;
import com.hpcloud.mon.persister.repository.RepositoryCommitHeartbeat;
import com.lmax.disruptor.ExceptionHandler;
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
                .implement(AlarmStateTransitionMessageEventHandler.class, AlarmStateTransitionMessageEventHandler.class)
                .build(AlarmStateTransitionMessageEventHandlerFactory.class));

        install(new FactoryModuleBuilder()
                .implement(KafkaMetricsConsumerRunnableBasic.class, KafkaMetricsConsumerRunnableBasic.class)
                .build(KafkaMetricsConsumerRunnableBasicFactory.class));

        install(new FactoryModuleBuilder()
                .implement(KafkaAlarmStateTransitionConsumerRunnableBasic.class, KafkaAlarmStateTransitionConsumerRunnableBasic.class)
                .build(KafkaAlarmStateTransitionConsumerRunnableBasicFactory.class));

        bind(ExceptionHandler.class).to(DisruptorExceptionHandler.class);

        bind(MetricDisruptor.class)
                .toProvider(MetricDisruptorProvider.class).in(Scopes.SINGLETON);

        bind(AlarmStateHistoryDisruptor.class)
                .toProvider(AlarmHistoryDisruptorProvider.class).in(Scopes.SINGLETON);

        bind(DBI.class).toProvider(DBIProvider.class).in(Scopes.SINGLETON);
        bind(KafkaStreams.class).toProvider(KafkaStreamsProvider.class).in(Scopes.SINGLETON);
        bind(MetricsConsumer.class);
        bind(AlarmStateTransitionsConsumer.class);
        bind(RepositoryCommitHeartbeat.class);
    }
}
