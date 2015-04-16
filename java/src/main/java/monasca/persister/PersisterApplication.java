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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import monasca.common.model.event.AlarmStateTransitionedEvent;
import monasca.common.model.metric.MetricEnvelope;
import monasca.persister.configuration.PersisterConfig;
import monasca.persister.consumer.Consumer;
import monasca.persister.consumer.ConsumerFactory;
import monasca.persister.consumer.KafkaChannel;
import monasca.persister.consumer.KafkaChannelFactory;
import monasca.persister.consumer.alarmstate.KafkaAlarmStateTransitionConsumer;
import monasca.persister.consumer.alarmstate.KafkaAlarmStateTransitionConsumerFactory;
import monasca.persister.consumer.metric.KafkaMetricsConsumer;
import monasca.persister.consumer.metric.KafkaMetricsConsumerFactory;
import monasca.persister.healthcheck.SimpleHealthCheck;
import monasca.persister.pipeline.ManagedPipeline;
import monasca.persister.pipeline.ManagedPipelineFactory;
import monasca.persister.pipeline.event.AlarmStateTransitionedEventHandlerFactory;
import monasca.persister.pipeline.event.MetricHandlerFactory;
import monasca.persister.resource.Resource;

public class PersisterApplication extends Application<PersisterConfig> {
  private static final Logger logger = LoggerFactory.getLogger(PersisterApplication.class);

  public static void main(String[] args) throws Exception {
    /*
     * This should allow command line options to show the current version
     * java -jar monasca-persister.jar --version
     * java -jar monasca-persister.jar -version
     * java -jar monasca-persister.jar version
     * Really anything with the word version in it will show the
     * version as long as there is only one argument
     * */
    if (args.length == 1 && args[0].toLowerCase().contains("version")) {
      showVersion();
      System.exit(0);
    }

    new PersisterApplication().run(args);
  }

  private static void showVersion() {
    Package pkg;
    pkg = Package.getPackage("monasca.persister");

    System.out.println("-------- Version Information --------");
    System.out.println(pkg.getImplementationVersion());
  }

  @Override
  public void initialize(Bootstrap<PersisterConfig> bootstrap) {
  }

  @Override
  public String getName() {
    return "monasca-persister";
  }

  @Override
  public void run(PersisterConfig configuration, Environment environment)
      throws Exception {

    Injector injector = Guice.createInjector(new PersisterModule(configuration, environment));

    // Sample resource.
    environment.jersey().register(new Resource());

    // Sample health check.
    environment.healthChecks().register("test-health-check", new SimpleHealthCheck());

    final KafkaChannelFactory kafkaChannelFactory = injector.getInstance(KafkaChannelFactory.class);

    final ConsumerFactory<MetricEnvelope[]> metricsConsumerFactory =
        injector.getInstance(Key.get(new TypeLiteral<ConsumerFactory<MetricEnvelope[]>>() {
        }));

    // Metrics
    final KafkaMetricsConsumerFactory<MetricEnvelope[]> kafkaMetricsConsumerFactory =
        injector.getInstance(Key.get(new TypeLiteral<KafkaMetricsConsumerFactory<MetricEnvelope[]>>(){}));

    for (int i = 0; i < configuration.getMetricConfiguration().getNumThreads(); i++) {

      final KafkaChannel kafkaChannel =
          kafkaChannelFactory.create(configuration, configuration.getMetricConfiguration(), i);

      final ManagedPipeline<MetricEnvelope[]> metricPipeline = getMetricPipeline(
          configuration, i, injector);

      final KafkaMetricsConsumer<MetricEnvelope[]> kafkaMetricsConsumer =
          kafkaMetricsConsumerFactory.create(MetricEnvelope[].class, kafkaChannel, i, metricPipeline);

      Consumer<MetricEnvelope[]> metricsConsumer =
          metricsConsumerFactory.create(kafkaMetricsConsumer, metricPipeline);

      environment.lifecycle().manage(metricsConsumer);
    }

    // AlarmStateTransitions
    final ConsumerFactory<AlarmStateTransitionedEvent>
        alarmStateTransitionsConsumerFactory = injector.getInstance(Key.get(new TypeLiteral
        <ConsumerFactory<AlarmStateTransitionedEvent>>(){}));

    final KafkaAlarmStateTransitionConsumerFactory<AlarmStateTransitionedEvent>
        kafkaAlarmStateTransitionConsumerFactory =
        injector.getInstance(Key.get(new TypeLiteral<KafkaAlarmStateTransitionConsumerFactory<AlarmStateTransitionedEvent
                >>() {}));

    for (int i = 0; i < configuration.getAlarmHistoryConfiguration().getNumThreads(); i++) {

      final KafkaChannel kafkaChannel =
          kafkaChannelFactory
              .create(configuration, configuration.getAlarmHistoryConfiguration(), i);

      final ManagedPipeline<AlarmStateTransitionedEvent> pipeline =
          getAlarmStateHistoryPipeline(configuration, i, injector);

      final KafkaAlarmStateTransitionConsumer<AlarmStateTransitionedEvent> kafkaAlarmStateTransitionConsumer =
          kafkaAlarmStateTransitionConsumerFactory.create(AlarmStateTransitionedEvent.class, kafkaChannel, i, pipeline);

      Consumer<AlarmStateTransitionedEvent> alarmStateTransitionConsumer =
          alarmStateTransitionsConsumerFactory.create(kafkaAlarmStateTransitionConsumer, pipeline);

      environment.lifecycle().manage(alarmStateTransitionConsumer);
    }
  }

  private ManagedPipeline<MetricEnvelope[]> getMetricPipeline(PersisterConfig configuration, int threadNum,
      Injector injector) {

    logger.debug("Creating metric pipeline...");

    final int batchSize = configuration.getMetricConfiguration().getBatchSize();
    logger.debug("Batch size for metric pipeline [" + batchSize + "]");

    MetricHandlerFactory<MetricEnvelope[]> metricEventHandlerFactory =
        injector.getInstance(Key.get(new TypeLiteral<MetricHandlerFactory<MetricEnvelope[]>>(){}));

    ManagedPipelineFactory<MetricEnvelope[]>
        managedPipelineFactory = injector.getInstance(Key.get(new TypeLiteral
        <ManagedPipelineFactory<MetricEnvelope[]>>(){}));

    final ManagedPipeline<MetricEnvelope[]> pipeline =
        managedPipelineFactory.create(metricEventHandlerFactory.create(
            configuration.getMetricConfiguration(), threadNum, batchSize));

    logger.debug("Instance of metric pipeline fully created");

    return pipeline;
  }

  public ManagedPipeline<AlarmStateTransitionedEvent> getAlarmStateHistoryPipeline(
      PersisterConfig configuration, int threadNum, Injector injector) {

    logger.debug("Creating alarm state history pipeline...");

    int batchSize = configuration.getAlarmHistoryConfiguration().getBatchSize();
    logger.debug("Batch size for each AlarmStateHistoryPipeline [" + batchSize + "]");

    AlarmStateTransitionedEventHandlerFactory<AlarmStateTransitionedEvent> alarmHistoryEventHandlerFactory =
        injector.getInstance(Key.get(new TypeLiteral<AlarmStateTransitionedEventHandlerFactory
            <AlarmStateTransitionedEvent>>(){}));

    ManagedPipelineFactory<AlarmStateTransitionedEvent> alarmStateTransitionPipelineFactory =
        injector.getInstance(new Key<ManagedPipelineFactory<AlarmStateTransitionedEvent>>(){});

    ManagedPipeline<AlarmStateTransitionedEvent> pipeline =
        alarmStateTransitionPipelineFactory.create(alarmHistoryEventHandlerFactory.create(
            configuration.getAlarmHistoryConfiguration(), threadNum, batchSize));

    logger.debug("Instance of alarm state history pipeline fully created");

    return pipeline;
  }
}
