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
import com.hpcloud.mon.persister.consumer.KafkaChannel;
import com.hpcloud.mon.persister.consumer.KafkaChannelFactory;
import com.hpcloud.mon.persister.consumer.KafkaMetricsConsumer;
import com.hpcloud.mon.persister.consumer.KafkaMetricsConsumerFactory;
import com.hpcloud.mon.persister.consumer.MetricsConsumer;
import com.hpcloud.mon.persister.consumer.MetricsConsumerFactory;
import com.hpcloud.mon.persister.healthcheck.SimpleHealthCheck;
import com.hpcloud.mon.persister.pipeline.AlarmStateTransitionPipeline;
import com.hpcloud.mon.persister.pipeline.AlarmStateTransitionPipelineFactory;
import com.hpcloud.mon.persister.pipeline.MetricPipeline;
import com.hpcloud.mon.persister.pipeline.MetricPipelineFactory;
import com.hpcloud.mon.persister.pipeline.event.AlarmStateTransitionedEventHandlerFactory;
import com.hpcloud.mon.persister.pipeline.event.MetricHandlerFactory;
import com.hpcloud.mon.persister.resource.Resource;

import com.google.inject.Guice;
import com.google.inject.Injector;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonPersisterApplication extends Application<MonPersisterConfiguration> {
  private static final Logger logger = LoggerFactory.getLogger(MonPersisterApplication.class);

  public static void main(String[] args) throws Exception {
    new MonPersisterApplication().run(args);
  }

  @Override
  public void initialize(Bootstrap<MonPersisterConfiguration> bootstrap) {
  }

  @Override
  public String getName() {
    return "mon-persister";
  }

  @Override
  public void run(MonPersisterConfiguration configuration, Environment environment)
      throws Exception {

    Injector injector = Guice.createInjector(new MonPersisterModule(configuration, environment));

    // Sample resource.
    environment.jersey().register(new Resource());

    // Sample health check.
    environment.healthChecks().register("test-health-check", new SimpleHealthCheck());

    final KafkaChannelFactory kafkaChannelFactory = injector.getInstance(KafkaChannelFactory.class);
    final MetricsConsumerFactory metricsConsumerFactory =
        injector.getInstance(MetricsConsumerFactory.class);
    final KafkaMetricsConsumerFactory kafkaMetricsConsumerFactory =
        injector.getInstance(KafkaMetricsConsumerFactory.class);
    for (int i = 0; i < configuration.getMetricConfiguration().getNumThreads(); i++) {
      final KafkaChannel kafkaChannel =
          kafkaChannelFactory.create(configuration, configuration.getMetricConfiguration(), i);
      final MetricPipeline metricPipeline = getMetricPipeline(configuration, i, injector);
      final KafkaMetricsConsumer kafkaMetricsConsumer =
          kafkaMetricsConsumerFactory.create(kafkaChannel, i, metricPipeline);
      MetricsConsumer metricsConsumer =
          metricsConsumerFactory.create(kafkaMetricsConsumer, metricPipeline);
      environment.lifecycle().manage(metricsConsumer);
    }

    final AlarmStateTransitionConsumerFactory alarmStateTransitionsConsumerFactory =
        injector.getInstance(AlarmStateTransitionConsumerFactory.class);
    final KafkaAlarmStateTransitionConsumerFactory kafkaAlarmStateTransitionConsumerFactory =
        injector.getInstance(KafkaAlarmStateTransitionConsumerFactory.class);
    for (int i = 0; i < configuration.getAlarmHistoryConfiguration().getNumThreads(); i++) {
      final KafkaChannel kafkaChannel =
          kafkaChannelFactory
              .create(configuration, configuration.getAlarmHistoryConfiguration(), i);
      final AlarmStateTransitionPipeline pipeline =
          getAlarmStateHistoryPipeline(configuration, i, injector);
      final KafkaAlarmStateTransitionConsumer kafkaAlarmStateTransitionConsumer =
          kafkaAlarmStateTransitionConsumerFactory.create(kafkaChannel, i, pipeline);
      AlarmStateTransitionConsumer alarmStateTransitionConsumer =
          alarmStateTransitionsConsumerFactory.create(kafkaAlarmStateTransitionConsumer, pipeline);
      environment.lifecycle().manage(alarmStateTransitionConsumer);
    }
  }

  private MetricPipeline getMetricPipeline(MonPersisterConfiguration configuration, int threadNum,
      Injector injector) {

    logger.debug("Creating metric pipeline...");

    final int batchSize = configuration.getMetricConfiguration().getBatchSize();
    logger.debug("Batch size for metric pipeline [" + batchSize + "]");

    MetricHandlerFactory metricEventHandlerFactory =
        injector.getInstance(MetricHandlerFactory.class);
    MetricPipelineFactory metricPipelineFactory = injector.getInstance(MetricPipelineFactory.class);
    final MetricPipeline pipeline =
        metricPipelineFactory.create(metricEventHandlerFactory.create(
            configuration.getMetricConfiguration(), threadNum, batchSize));

    logger.debug("Instance of metric pipeline fully created");

    return pipeline;
  }

  public AlarmStateTransitionPipeline getAlarmStateHistoryPipeline(
      MonPersisterConfiguration configuration, int threadNum, Injector injector) {

    logger.debug("Creating alarm state history pipeline...");

    int batchSize = configuration.getAlarmHistoryConfiguration().getBatchSize();
    logger.debug("Batch size for each AlarmStateHistoryPipeline [" + batchSize + "]");
    AlarmStateTransitionedEventHandlerFactory alarmHistoryEventHandlerFactory =
        injector.getInstance(AlarmStateTransitionedEventHandlerFactory.class);

    AlarmStateTransitionPipelineFactory alarmStateTransitionPipelineFactory =
        injector.getInstance(AlarmStateTransitionPipelineFactory.class);

    AlarmStateTransitionPipeline pipeline =
        alarmStateTransitionPipelineFactory.create(alarmHistoryEventHandlerFactory.create(
            configuration.getAlarmHistoryConfiguration(), threadNum, batchSize));

    logger.debug("Instance of alarm state history pipeline fully created");

    return pipeline;
  }
}
