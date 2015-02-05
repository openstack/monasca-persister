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
import monasca.persister.repository.AlarmRepo;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import io.dropwizard.setup.Environment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlarmStateTransitionedEventHandler extends
    FlushableHandler<AlarmStateTransitionedEvent> {

  private static final Logger logger = LoggerFactory
      .getLogger(AlarmStateTransitionedEventHandler.class);

  private final AlarmRepo repository;
  private final int ordinal;

  @Inject
  public AlarmStateTransitionedEventHandler(AlarmRepo repository,
      @Assisted PipelineConfig configuration, Environment environment,
      @Assisted("ordinal") int ordinal,
      @Assisted("batchSize") int batchSize) {
    super(configuration, environment, ordinal, batchSize,
        AlarmStateTransitionedEventHandler.class.getName());
    this.repository = repository;
    this.ordinal = ordinal;
  }

  @Override
  protected int process(AlarmStateTransitionedEvent event) throws Exception {
    logger.debug("Ordinal:  Event: {}", this.ordinal, event);

    repository.addToBatch(event);
    return 1;
  }

  @Override
  protected void flushRepository() {
    repository.flush();
  }
}
