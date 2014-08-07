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

package monasca.persister;

import monasca.persister.configuration.MonPersisterConfiguration;
import monasca.persister.consumer.AlarmStateTransitionConsumer;
import monasca.persister.consumer.AlarmStateTransitionConsumerFactory;
import monasca.persister.consumer.KafkaAlarmStateTransitionConsumer;
import monasca.persister.consumer.KafkaAlarmStateTransitionConsumerFactory;
import monasca.persister.consumer.KafkaAlarmStateTransitionConsumerRunnableBasic;
import monasca.persister.consumer.KafkaAlarmStateTransitionConsumerRunnableBasicFactory;
import monasca.persister.consumer.KafkaChannel;
import monasca.persister.consumer.KafkaChannelFactory;
import monasca.persister.consumer.KafkaMetricsConsumer;
import monasca.persister.consumer.KafkaMetricsConsumerFactory;
import monasca.persister.consumer.KafkaMetricsConsumerRunnableBasic;
import monasca.persister.consumer.KafkaMetricsConsumerRunnableBasicFactory;
import monasca.persister.consumer.MetricsConsumer;
import monasca.persister.consumer.MetricsConsumerFactory;
import monasca.persister.dbi.DBIProvider;
import monasca.persister.pipeline.AlarmStateTransitionPipeline;
import monasca.persister.pipeline.AlarmStateTransitionPipelineFactory;
import monasca.persister.pipeline.MetricPipeline;
import monasca.persister.pipeline.MetricPipelineFactory;
import monasca.persister.pipeline.event.AlarmStateTransitionedEventHandler;
import monasca.persister.pipeline.event.AlarmStateTransitionedEventHandlerFactory;
import monasca.persister.pipeline.event.MetricHandler;
import monasca.persister.pipeline.event.MetricHandlerFactory;
import monasca.persister.repository.AlarmRepository;
import monasca.persister.repository.InfluxDBAlarmRepository;
import monasca.persister.repository.InfluxDBMetricRepository;
import monasca.persister.repository.MetricRepository;
import monasca.persister.repository.VerticaAlarmRepository;
import monasca.persister.repository.VerticaMetricRepository;

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
