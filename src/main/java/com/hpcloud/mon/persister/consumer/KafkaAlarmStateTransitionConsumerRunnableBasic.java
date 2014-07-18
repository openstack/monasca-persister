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

package com.hpcloud.mon.persister.consumer;

import com.hpcloud.mon.common.event.AlarmStateTransitionedEvent;
import com.hpcloud.mon.persister.pipeline.AlarmStateTransitionPipeline;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAlarmStateTransitionConsumerRunnableBasic extends
    KafkaConsumerRunnableBasic<AlarmStateTransitionedEvent> {

  private static final Logger logger = LoggerFactory
      .getLogger(KafkaAlarmStateTransitionConsumerRunnableBasic.class);

  private final ObjectMapper objectMapper;

  @Inject
  public KafkaAlarmStateTransitionConsumerRunnableBasic(@Assisted AlarmStateTransitionPipeline pipeline,
      @Assisted KafkaChannel kafkaChannel, @Assisted int threadNumber) {
    super(kafkaChannel, pipeline, threadNumber);
    this.objectMapper = new ObjectMapper();
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    objectMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
    objectMapper.enable(DeserializationFeature.UNWRAP_ROOT_VALUE);
  }

  @Override
  protected void publishHeartbeat() {
    publishEvent(null);
  }

  @Override
  protected void handleMessage(String message) {
    try {
      final AlarmStateTransitionedEvent event =
          objectMapper.readValue(message, AlarmStateTransitionedEvent.class);

      logger.debug(event.toString());

      publishEvent(event);
    } catch (Exception e) {
      logger.error("Failed to deserialize JSON message and send to handler: " + message, e);
    }
  }
}
