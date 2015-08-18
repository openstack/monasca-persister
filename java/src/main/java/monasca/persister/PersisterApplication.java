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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import monasca.common.model.event.AlarmStateTransitionedEvent;
import monasca.common.model.metric.MetricEnvelope;
import monasca.persister.configuration.PersisterConfig;
import monasca.persister.consumer.ManagedConsumer;
import monasca.persister.consumer.ManagedConsumerFactory;
import monasca.persister.consumer.KafkaChannel;
import monasca.persister.consumer.KafkaChannelFactory;
import monasca.persister.consumer.KafkaConsumer;
import monasca.persister.consumer.KafkaConsumerFactory;
import monasca.persister.consumer.KafkaConsumerRunnableBasic;
import monasca.persister.consumer.KafkaConsumerRunnableBasicFactory;
import monasca.persister.healthcheck.SimpleHealthCheck;
import monasca.persister.pipeline.ManagedPipeline;
import monasca.persister.pipeline.ManagedPipelineFactory;
import monasca.persister.pipeline.event.AlarmStateTransitionHandlerFactory;
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

    final ManagedConsumerFactory<MetricEnvelope[]> metricManagedConsumerFactory =
        injector.getInstance(Key.get(new TypeLiteral<ManagedConsumerFactory<MetricEnvelope[]>>() {}));

    // Metrics
    final KafkaConsumerFactory<MetricEnvelope[]> kafkaMetricConsumerFactory =
        injector.getInstance(Key.get(new TypeLiteral<KafkaConsumerFactory<MetricEnvelope[]>>(){}));

    final KafkaConsumerRunnableBasicFactory<MetricEnvelope[]> kafkaMetricConsumerRunnableBasicFactory =
        injector.getInstance(
            Key.get(new TypeLiteral<KafkaConsumerRunnableBasicFactory<MetricEnvelope[]>>() {
            }));

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .build();

    int totalNumberOfThreads = configuration.getMetricConfiguration().getNumThreads()
                               + configuration.getAlarmHistoryConfiguration().getNumThreads();

    ExecutorService executorService = Executors.newFixedThreadPool(totalNumberOfThreads, threadFactory);

    for (int i = 0; i < configuration.getMetricConfiguration().getNumThreads(); i++) {

      String threadId = "metric-" + String.valueOf(i);

      final KafkaChannel kafkaMetricChannel =
          kafkaChannelFactory.create(configuration.getMetricConfiguration(), threadId);

      final ManagedPipeline<MetricEnvelope[]> managedMetricPipeline =
          getMetricPipeline(configuration, threadId, injector);

      KafkaConsumerRunnableBasic<MetricEnvelope[]> kafkaMetricConsumerRunnableBasic =
          kafkaMetricConsumerRunnableBasicFactory.create(managedMetricPipeline, kafkaMetricChannel, threadId);

      final KafkaConsumer<MetricEnvelope[]> kafkaMetricConsumer =
          kafkaMetricConsumerFactory.create(kafkaMetricConsumerRunnableBasic, threadId, executorService);

      ManagedConsumer<MetricEnvelope[]> managedMetricConsumer =
          metricManagedConsumerFactory.create(kafkaMetricConsumer, threadId);

      environment.lifecycle().manage(managedMetricConsumer);
    }

    // AlarmStateTransitions
    final ManagedConsumerFactory<AlarmStateTransitionedEvent>
        alarmStateTransitionsManagedConsumerFactory = injector.getInstance(Key.get(new TypeLiteral
        <ManagedConsumerFactory<AlarmStateTransitionedEvent>>(){}));

    final KafkaConsumerFactory<AlarmStateTransitionedEvent>
        kafkaAlarmStateTransitionConsumerFactory =
        injector.getInstance(Key.get(new TypeLiteral<KafkaConsumerFactory<AlarmStateTransitionedEvent>>() { }));

    final KafkaConsumerRunnableBasicFactory<AlarmStateTransitionedEvent> kafkaAlarmStateTransitionConsumerRunnableBasicFactory =
        injector.getInstance(Key.get(new TypeLiteral<KafkaConsumerRunnableBasicFactory
            <AlarmStateTransitionedEvent>>(){}))      ;

    for (int i = 0; i < configuration.getAlarmHistoryConfiguration().getNumThreads(); i++) {

      String threadId = "alarm-state-transition-" + String.valueOf(i);

      final KafkaChannel kafkaAlarmStateTransitionChannel =
          kafkaChannelFactory
              .create(configuration.getAlarmHistoryConfiguration(), threadId);

      final ManagedPipeline<AlarmStateTransitionedEvent> managedAlarmStateTransitionPipeline =
          getAlarmStateHistoryPipeline(configuration, threadId, injector);

      KafkaConsumerRunnableBasic<AlarmStateTransitionedEvent> kafkaAlarmStateTransitionConsumerRunnableBasic =
          kafkaAlarmStateTransitionConsumerRunnableBasicFactory.create(managedAlarmStateTransitionPipeline, kafkaAlarmStateTransitionChannel, threadId);

      final KafkaConsumer<AlarmStateTransitionedEvent> kafkaAlarmStateTransitionConsumer =
          kafkaAlarmStateTransitionConsumerFactory.create(kafkaAlarmStateTransitionConsumerRunnableBasic, threadId,
                                                          executorService);

      ManagedConsumer<AlarmStateTransitionedEvent> managedAlarmStateTransitionConsumer =
          alarmStateTransitionsManagedConsumerFactory.create(kafkaAlarmStateTransitionConsumer, threadId);

      environment.lifecycle().manage(managedAlarmStateTransitionConsumer);
    }
  }

  private ManagedPipeline<MetricEnvelope[]> getMetricPipeline(
      PersisterConfig configuration,
      String threadId,
      Injector injector) {

    logger.debug("Creating metric pipeline [{}]...", threadId);

    final int batchSize = configuration.getMetricConfiguration().getBatchSize();
    logger.debug("Batch size for metric pipeline [{}]", batchSize);

    MetricHandlerFactory metricEventHandlerFactory =
        injector.getInstance(MetricHandlerFactory.class);

    ManagedPipelineFactory<MetricEnvelope[]>
        managedPipelineFactory = injector.getInstance(Key.get(new TypeLiteral
        <ManagedPipelineFactory<MetricEnvelope[]>>(){}));

    final ManagedPipeline<MetricEnvelope[]> pipeline =
        managedPipelineFactory.create(metricEventHandlerFactory.create(
            configuration.getMetricConfiguration(), threadId, batchSize), threadId);

    logger.debug("Instance of metric pipeline [{}] fully created", threadId);

    return pipeline;
  }

  public ManagedPipeline<AlarmStateTransitionedEvent> getAlarmStateHistoryPipeline(
      PersisterConfig configuration,
      String threadId,
      Injector injector) {

    logger.debug("Creating alarm state history pipeline [{}]...", threadId);

    int batchSize = configuration.getAlarmHistoryConfiguration().getBatchSize();
    logger.debug("Batch size for each AlarmStateHistoryPipeline [{}]", batchSize);

    AlarmStateTransitionHandlerFactory alarmHistoryEventHandlerFactory =
        injector.getInstance(AlarmStateTransitionHandlerFactory.class);

    ManagedPipelineFactory<AlarmStateTransitionedEvent> alarmStateTransitionPipelineFactory =
        injector.getInstance(new Key<ManagedPipelineFactory<AlarmStateTransitionedEvent>>(){});

    ManagedPipeline<AlarmStateTransitionedEvent> pipeline =
        alarmStateTransitionPipelineFactory.create(alarmHistoryEventHandlerFactory.create(
            configuration.getAlarmHistoryConfiguration(), threadId, batchSize), threadId);

    logger.debug("Instance of alarm state history pipeline [{}] fully created", threadId);

    return pipeline;
  }
}
