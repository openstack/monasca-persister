/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hpcloud.mon.persister;

import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.consumer.AlarmStateTransitionConsumer;
import com.hpcloud.mon.persister.consumer.AlarmStateTransitionConsumerFactory;
import com.hpcloud.mon.persister.consumer.KafkaAlarmStateTransitionConsumer;
import com.hpcloud.mon.persister.consumer.KafkaAlarmStateTransitionConsumerFactory;
import com.hpcloud.mon.persister.consumer.KafkaAlarmStateTransitionConsumerRunnableBasic;
import com.hpcloud.mon.persister.consumer.KafkaAlarmStateTransitionConsumerRunnableBasicFactory;
import com.hpcloud.mon.persister.consumer.KafkaChannel;
import com.hpcloud.mon.persister.consumer.KafkaChannelFactory;
import com.hpcloud.mon.persister.consumer.KafkaMetricsConsumer;
import com.hpcloud.mon.persister.consumer.KafkaMetricsConsumerFactory;
import com.hpcloud.mon.persister.consumer.KafkaMetricsConsumerRunnableBasic;
import com.hpcloud.mon.persister.consumer.KafkaMetricsConsumerRunnableBasicFactory;
import com.hpcloud.mon.persister.consumer.MetricsConsumer;
import com.hpcloud.mon.persister.consumer.MetricsConsumerFactory;
import com.hpcloud.mon.persister.dbi.DBIProvider;
import com.hpcloud.mon.persister.pipeline.AlarmStateTransitionPipeline;
import com.hpcloud.mon.persister.pipeline.AlarmStateTransitionPipelineFactory;
import com.hpcloud.mon.persister.pipeline.MetricPipeline;
import com.hpcloud.mon.persister.pipeline.MetricPipelineFactory;
import com.hpcloud.mon.persister.pipeline.event.AlarmStateTransitionedEventHandler;
import com.hpcloud.mon.persister.pipeline.event.AlarmStateTransitionedEventHandlerFactory;
import com.hpcloud.mon.persister.pipeline.event.MetricHandler;
import com.hpcloud.mon.persister.pipeline.event.MetricHandlerFactory;
import com.hpcloud.mon.persister.repository.AlarmRepository;
import com.hpcloud.mon.persister.repository.InfluxDBAlarmRepository;
import com.hpcloud.mon.persister.repository.InfluxDBMetricRepository;
import com.hpcloud.mon.persister.repository.MetricRepository;
import com.hpcloud.mon.persister.repository.VerticaAlarmRepository;
import com.hpcloud.mon.persister.repository.VerticaMetricRepository;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;

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

    install(new FactoryModuleBuilder().implement(MetricHandler.class, MetricHandler.class).build(
        MetricHandlerFactory.class));

    install(new FactoryModuleBuilder().implement(AlarmStateTransitionedEventHandler.class,
        AlarmStateTransitionedEventHandler.class).build(
        AlarmStateTransitionedEventHandlerFactory.class));

    install(new FactoryModuleBuilder().implement(KafkaMetricsConsumerRunnableBasic.class,
        KafkaMetricsConsumerRunnableBasic.class).build(
        KafkaMetricsConsumerRunnableBasicFactory.class));

    install(new FactoryModuleBuilder().implement(
        KafkaAlarmStateTransitionConsumerRunnableBasic.class,
        KafkaAlarmStateTransitionConsumerRunnableBasic.class).build(
        KafkaAlarmStateTransitionConsumerRunnableBasicFactory.class));

    install(new FactoryModuleBuilder().implement(
        KafkaMetricsConsumer.class,
        KafkaMetricsConsumer.class).build(
            KafkaMetricsConsumerFactory.class));

    install(new FactoryModuleBuilder().implement(
        MetricPipeline.class,
        MetricPipeline.class).build(
            MetricPipelineFactory.class));

    install(new FactoryModuleBuilder().implement(
        AlarmStateTransitionPipeline.class,
        AlarmStateTransitionPipeline.class).build(
            AlarmStateTransitionPipelineFactory.class));

    install(new FactoryModuleBuilder().implement(
        AlarmStateTransitionConsumer.class,
        AlarmStateTransitionConsumer.class).build(
            AlarmStateTransitionConsumerFactory.class));

    install(new FactoryModuleBuilder().implement(
        KafkaAlarmStateTransitionConsumer.class,
        KafkaAlarmStateTransitionConsumer.class).build(
            KafkaAlarmStateTransitionConsumerFactory.class));

    install(new FactoryModuleBuilder().implement(
        MetricsConsumer.class,
        MetricsConsumer.class).build(
            MetricsConsumerFactory.class));

    install(new FactoryModuleBuilder().implement(KafkaChannel.class, KafkaChannel.class).build(
        KafkaChannelFactory.class));

    if (configuration.getDatabaseConfiguration().getDatabaseType().equals("vertica")) {
      bind(DBI.class).toProvider(DBIProvider.class).in(Scopes.SINGLETON);
      bind(MetricRepository.class).to(VerticaMetricRepository.class);
      bind(AlarmRepository.class).to(VerticaAlarmRepository.class);
    } else if (configuration.getDatabaseConfiguration().getDatabaseType().equals("influxdb")) {
      bind(MetricRepository.class).to(InfluxDBMetricRepository.class);
      bind(AlarmRepository.class).to(InfluxDBAlarmRepository.class);
    } else {
      System.out.println("Unknown database type encountered: "
          + configuration.getDatabaseConfiguration().getDatabaseType());
      System.out.println("Supported databases are 'vertica' and 'influxdb'");
      System.out.println("Check your config file.");
      System.exit(1);
    }
  }
}
