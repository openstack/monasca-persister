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

package monasca.persister.consumer.metric;

import monasca.common.model.metric.MetricEnvelope;
import monasca.persister.consumer.KafkaChannel;
import monasca.persister.consumer.KafkaConsumerRunnableBasic;
import monasca.persister.pipeline.MetricPipeline;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMetricsConsumerRunnableBasic extends
                                               KafkaConsumerRunnableBasic<MetricEnvelope[]> {

  private static final Logger logger = LoggerFactory
      .getLogger(KafkaMetricsConsumerRunnableBasic.class);
  private final ObjectMapper objectMapper;

  @Inject
  public KafkaMetricsConsumerRunnableBasic(@Assisted MetricPipeline pipeline,
      @Assisted KafkaChannel kafkaChannel, @Assisted int threadNumber) {
    super(kafkaChannel, pipeline, threadNumber);
    this.objectMapper = new ObjectMapper();
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    objectMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
    objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
  }

  @Override
  protected void publishHeartbeat() {
    publishEvent(null);
  }

  @Override
  protected void handleMessage(String message) {
    try {
      final MetricEnvelope[] envelopes = objectMapper.readValue(message, MetricEnvelope[].class);

      for (final MetricEnvelope envelope : envelopes) {
        logger.debug("{}", envelope);
      }

      publishEvent(envelopes);
    } catch (Exception e) {
      logger.error("Failed to deserialize JSON message and place on pipeline queue: " + message,
          e);
    }
  }
}
