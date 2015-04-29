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

package monasca.persister.pipeline.event;

import monasca.common.model.event.AlarmStateTransitionedEvent;
import monasca.persister.configuration.PipelineConfig;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import com.codahale.metrics.Counter;
import com.fasterxml.jackson.databind.DeserializationFeature;

import io.dropwizard.setup.Environment;
import monasca.persister.repository.Repo;
import monasca.persister.repository.RepoException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AlarmStateTransitionHandler extends
    FlushableHandler<AlarmStateTransitionedEvent> {

  private static final Logger logger =
      LoggerFactory.getLogger(AlarmStateTransitionHandler.class);

  private final Repo<AlarmStateTransitionedEvent> alarmRepo;

  private final Counter alarmStateTransitionCounter;

  @Inject
  public AlarmStateTransitionHandler(Repo<AlarmStateTransitionedEvent> alarmRepo,
                                     Environment environment,
                                     @Assisted PipelineConfig configuration,
                                     @Assisted("threadId") String threadId,
                                     @Assisted("batchSize") int batchSize) {

    super(configuration, environment, threadId, batchSize);

    this.alarmRepo = alarmRepo;

    this.alarmStateTransitionCounter =
        environment.metrics()
            .counter(this.handlerName + "." + "alarm-state-transitions-added-to-batch-counter");

  }

  @Override
  protected int process(String msg) {

    AlarmStateTransitionedEvent alarmStateTransitionedEvent;

    try {

      alarmStateTransitionedEvent =
          this.objectMapper.readValue(msg, AlarmStateTransitionedEvent.class);

    } catch (IOException e) {

      logger.error("[{}]: failed to deserialize message {}", this.threadId, msg, e);

      return 0;
    }

    logger.debug("[{}]: [{}:{}] {}",
                 this.threadId,
                 this.getBatchCount(),
                 this.getMsgCount(),
                 alarmStateTransitionedEvent);

    this.alarmRepo.addToBatch(alarmStateTransitionedEvent, this.threadId);

    this.alarmStateTransitionCounter.inc();

    return 1;
  }

  @Override
  protected void initObjectMapper() {

    this.objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    this.objectMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

    this.objectMapper.enable(DeserializationFeature.UNWRAP_ROOT_VALUE);

  }

  @Override
  protected int flushRepository() throws RepoException {

    return this.alarmRepo.flush(this.threadId);

  }
}
