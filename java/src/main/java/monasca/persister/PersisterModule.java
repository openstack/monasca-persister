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

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;

import org.skife.jdbi.v2.DBI;

import javax.inject.Singleton;

import io.dropwizard.setup.Environment;
import monasca.common.model.event.AlarmStateTransitionedEvent;
import monasca.common.model.metric.MetricEnvelope;
import monasca.persister.configuration.PersisterConfig;
import monasca.persister.consumer.Consumer;
import monasca.persister.consumer.ConsumerFactory;
import monasca.persister.consumer.KafkaChannel;
import monasca.persister.consumer.KafkaChannelFactory;
import monasca.persister.consumer.KafkaConsumerRunnableBasic;
import monasca.persister.consumer.KafkaConsumerRunnableBasicFactory;
import monasca.persister.consumer.alarmstate.KafkaAlarmStateTransitionConsumer;
import monasca.persister.consumer.alarmstate.KafkaAlarmStateTransitionConsumerFactory;
import monasca.persister.consumer.metric.KafkaMetricsConsumer;
import monasca.persister.consumer.metric.KafkaMetricsConsumerFactory;
import monasca.persister.dbi.DBIProvider;
import monasca.persister.pipeline.ManagedPipelineFactory;
import monasca.persister.pipeline.ManagedPipeline;
import monasca.persister.pipeline.event.AlarmStateTransitionedEventHandler;
import monasca.persister.pipeline.event.AlarmStateTransitionedEventHandlerFactory;
import monasca.persister.pipeline.event.MetricHandler;
import monasca.persister.pipeline.event.MetricHandlerFactory;
import monasca.persister.repository.AlarmRepo;
import monasca.persister.repository.MetricRepo;
import monasca.persister.repository.influxdb.InfluxV8AlarmRepo;
import monasca.persister.repository.influxdb.InfluxV8MetricRepo;
import monasca.persister.repository.influxdb.InfluxV8RepoWriter;
import monasca.persister.repository.influxdb.InfluxV9AlarmRepo;
import monasca.persister.repository.influxdb.InfluxV9MetricRepo;
import monasca.persister.repository.influxdb.InfluxV9RepoWriter;
import monasca.persister.repository.vertica.VerticaAlarmRepo;
import monasca.persister.repository.vertica.VerticaMetricRepo;

public class PersisterModule extends AbstractModule {

  private static final String VERTICA = "vertica";
  private static final String INFLUXDB = "influxdb";
  private static final String INFLUXDB_V8 = "v8";
  private static final String INFLUXDB_V9 = "v9";

  private final PersisterConfig config;
  private final Environment env;

  public PersisterModule(PersisterConfig config, Environment env) {
    this.config = config;
    this.env = env;
  }

  @Override
  protected void configure() {

    bind(PersisterConfig.class).toInstance(config);
    bind(Environment.class).toInstance(env);

    install(
        new FactoryModuleBuilder().implement(
            new TypeLiteral<MetricHandler<MetricEnvelope[]>>() {},
            new TypeLiteral<MetricHandler<MetricEnvelope[]>>() {})
            .build(new TypeLiteral<MetricHandlerFactory<MetricEnvelope[]>>() {}));

    install(
        new FactoryModuleBuilder().implement(
        new TypeLiteral<AlarmStateTransitionedEventHandler<AlarmStateTransitionedEvent>>() {},
        new TypeLiteral<AlarmStateTransitionedEventHandler<AlarmStateTransitionedEvent>>() {})
            .build(new TypeLiteral<AlarmStateTransitionedEventHandlerFactory<AlarmStateTransitionedEvent>>() {}));

    install(
        new FactoryModuleBuilder().implement(
            new TypeLiteral<KafkaConsumerRunnableBasic<MetricEnvelope[]>>() {},
            new TypeLiteral<KafkaConsumerRunnableBasic<MetricEnvelope[]>>() {})
            .build(new TypeLiteral<KafkaConsumerRunnableBasicFactory<MetricEnvelope[]>>() {}));

    install(
        new FactoryModuleBuilder().implement(
        new TypeLiteral<KafkaConsumerRunnableBasic<AlarmStateTransitionedEvent>>() {},
        new TypeLiteral<KafkaConsumerRunnableBasic<AlarmStateTransitionedEvent>>() {})
            .build(new TypeLiteral<KafkaConsumerRunnableBasicFactory<AlarmStateTransitionedEvent>>() {}));

    install(
        new FactoryModuleBuilder().implement(
            new TypeLiteral<KafkaMetricsConsumer<MetricEnvelope[]>>() {},
            new TypeLiteral<KafkaMetricsConsumer<MetricEnvelope[]>>() {})
            .build(new TypeLiteral<KafkaMetricsConsumerFactory<MetricEnvelope[]>>() {}));

    install(
        new FactoryModuleBuilder().implement(
            new TypeLiteral<ManagedPipeline<MetricEnvelope[]>>() {},
            new TypeLiteral<ManagedPipeline<MetricEnvelope[]>>() {})
            .build(new TypeLiteral<ManagedPipelineFactory<MetricEnvelope[]>>() {}));

    install(
        new FactoryModuleBuilder().implement(
        new TypeLiteral<ManagedPipeline<AlarmStateTransitionedEvent>>() {},
        new TypeLiteral<ManagedPipeline<AlarmStateTransitionedEvent>>() {})
            .build(new TypeLiteral<ManagedPipelineFactory<AlarmStateTransitionedEvent>>() {}));

    install(
        new FactoryModuleBuilder().implement(
        new TypeLiteral<Consumer<AlarmStateTransitionedEvent>>() {},
        new TypeLiteral<Consumer<AlarmStateTransitionedEvent>>() {})
            .build(new TypeLiteral<ConsumerFactory<AlarmStateTransitionedEvent>>() {}));

    install(
        new FactoryModuleBuilder().implement(
        new TypeLiteral<KafkaAlarmStateTransitionConsumer<AlarmStateTransitionedEvent>>() {},
        new TypeLiteral<KafkaAlarmStateTransitionConsumer<AlarmStateTransitionedEvent>>() {})
            .build(new TypeLiteral<KafkaAlarmStateTransitionConsumerFactory<AlarmStateTransitionedEvent>>() {}));

    install(
        new FactoryModuleBuilder().implement(
            new TypeLiteral<Consumer<MetricEnvelope[]>>() {},
            new TypeLiteral<Consumer<MetricEnvelope[]>>() {})
            .build(new TypeLiteral<ConsumerFactory<MetricEnvelope[]>>() {}));

    install(
        new FactoryModuleBuilder().implement(
            KafkaChannel.class, KafkaChannel.class).build(KafkaChannelFactory.class));

    if (config.getDatabaseConfiguration().getDatabaseType().equalsIgnoreCase(VERTICA)) {

      bind(DBI.class).toProvider(DBIProvider.class).in(Scopes.SINGLETON);
      bind(MetricRepo.class).to(VerticaMetricRepo.class);
      bind(AlarmRepo.class).to(VerticaAlarmRepo.class);

    } else if (config.getDatabaseConfiguration().getDatabaseType().equalsIgnoreCase(INFLUXDB)) {

      // Check for null to not break existing configs. If no version, default to V8.
      if (config.getInfluxDBConfiguration().getVersion() == null || config
          .getInfluxDBConfiguration().getVersion().equalsIgnoreCase(INFLUXDB_V8)) {

        bind(InfluxV8RepoWriter.class);
        bind(MetricRepo.class).to(InfluxV8MetricRepo.class);
        bind(AlarmRepo.class).to(InfluxV8AlarmRepo.class);

      } else if (config.getInfluxDBConfiguration().getVersion().equalsIgnoreCase(INFLUXDB_V9)) {

        bind(InfluxV9RepoWriter.class).in(Singleton.class);
        bind(MetricRepo.class).to(InfluxV9MetricRepo.class);
        bind(AlarmRepo.class).to(InfluxV9AlarmRepo.class);

      } else {

        System.err.println(
            "Found unknown Influxdb version: " + config.getInfluxDBConfiguration().getVersion());
        System.err.println("Supported Influxdb versions are 'v8' and 'v9'");
        System.err.println("Check your config file");
        System.exit(1);

      }

    } else {

      System.err.println(
          "Found unknown database type: " + config.getDatabaseConfiguration().getDatabaseType());
      System.err.println("Supported databases are 'vertica' and 'influxdb'");
      System.err.println("Check your config file.");
      System.exit(1);

    }
  }
}
