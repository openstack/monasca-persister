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

import monasca.persister.consumer.KafkaChannel;
import monasca.persister.consumer.KafkaConsumer;
import monasca.persister.consumer.KafkaConsumerRunnableBasic;
import monasca.persister.consumer.KafkaConsumerRunnableBasicFactory;
import monasca.persister.pipeline.ManagedPipeline;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

public class KafkaMetricsConsumer<T> extends KafkaConsumer<T> {

  @Inject
  private KafkaConsumerRunnableBasicFactory<T> factory;

  private final ManagedPipeline<T> pipeline;

  private final Class<T> clazz;

  @Inject
  public KafkaMetricsConsumer(
      @Assisted Class<T> clazz,
      @Assisted KafkaChannel kafkaChannel,
      @Assisted int threadNum,
      @Assisted ManagedPipeline<T> pipeline) {

    super(kafkaChannel, threadNum);

    this.pipeline = pipeline;
    this.clazz = clazz;
  }

  @Override
  protected KafkaConsumerRunnableBasic<T> createRunnable(
      KafkaChannel kafkaChannel,
      int threadNumber) {

    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    objectMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
    objectMapper.setPropertyNamingStrategy(
        PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    return factory.create(objectMapper, clazz, pipeline, kafkaChannel, threadNumber);
  }
}
