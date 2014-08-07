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

package monasca.persister.consumer;

import com.hpcloud.mon.common.model.metric.MetricEnvelope;
import monasca.persister.pipeline.MetricPipeline;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

public class KafkaMetricsConsumer extends KafkaConsumer<MetricEnvelope[]> {

  @Inject
  private KafkaMetricsConsumerRunnableBasicFactory factory;

  private final MetricPipeline pipeline;

  @Inject
  public KafkaMetricsConsumer(@Assisted KafkaChannel kafkaChannel, @Assisted int threadNum,
      @Assisted MetricPipeline pipeline) {
    super(kafkaChannel, threadNum);
    this.pipeline = pipeline;
  }

  @Override
  protected KafkaConsumerRunnableBasic<MetricEnvelope[]> createRunnable(KafkaChannel kafkaChannel,
      int threadNumber) {
    return factory.create(pipeline, kafkaChannel, threadNumber);
  }
}
