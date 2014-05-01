/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hpcloud.mon.persister;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.consumer.*;
import com.hpcloud.mon.persister.dbi.DBIProvider;
import com.hpcloud.mon.persister.disruptor.*;
import com.hpcloud.mon.persister.disruptor.event.*;
import com.hpcloud.mon.persister.disruptor.event.MetricHandler;
import com.hpcloud.mon.persister.disruptor.event.MetricHandlerFactory;
import com.hpcloud.mon.persister.repository.RepositoryCommitHeartbeat;
import com.lmax.disruptor.ExceptionHandler;
import io.dropwizard.setup.Environment;
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
                .implement(MetricHandler.class, MetricHandler.class)
                .build(MetricHandlerFactory.class));

        install(new FactoryModuleBuilder()
                .implement(AlarmStateTransitionedEventHandler.class, AlarmStateTransitionedEventHandler.class)
                .build(AlarmStateTransitionedEventHandlerFactory.class));

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
