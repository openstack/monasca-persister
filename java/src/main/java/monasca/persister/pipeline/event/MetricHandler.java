/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 * 
 * Copyright (c) 2017 SUSE LLC
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

import monasca.common.model.metric.MetricEnvelope;
import monasca.persister.configuration.PipelineConfig;
import monasca.persister.repository.Repo;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import com.codahale.metrics.Counter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.dropwizard.setup.Environment;
import monasca.persister.repository.RepoException;

public class MetricHandler extends FlushableHandler<MetricEnvelope[]> {

  private static final Logger logger = LoggerFactory.getLogger(MetricHandler.class);

  private final Repo<MetricEnvelope> metricRepo;

  private final Counter metricCounter;

  @Inject
  public MetricHandler(Repo<MetricEnvelope> metricRepo, Environment environment,
      @Assisted PipelineConfig configuration, @Assisted("threadId") String threadId,
      @Assisted("batchSize") int batchSize) {

    super(configuration, environment, threadId, batchSize);

    this.metricRepo = metricRepo;

    this.metricCounter = environment.metrics()
        .counter(this.handlerName + "." + "metrics-added-to-batch-counter");

  }

  @Override
  public int process(String msg) {

    MetricEnvelope[] metricEnvelopesArry;

    try {

      metricEnvelopesArry = this.objectMapper.readValue(msg, MetricEnvelope[].class);

    } catch (IOException e) {

      logger.error("[{}]: failed to deserialize message {}", this.threadId, msg, e);

      return 0;
    }

    for (final MetricEnvelope metricEnvelope : metricEnvelopesArry) {

      processEnvelope(metricEnvelope);

    }

    return metricEnvelopesArry.length;
  }

  private void processEnvelope(MetricEnvelope metricEnvelope) {
    if (logger.isDebugEnabled()) {
      logger.debug("[{}]: [{}:{}] {}", this.threadId, this.getBatchCount(), this.getMsgCount(),
          metricEnvelope);
    }

    this.metricRepo.addToBatch(metricEnvelope, this.threadId);

    this.metricCounter.inc();

  }

  @Override
  protected void initObjectMapper() {

    this.objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    this.objectMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

    this.objectMapper
        .setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

  }

  @Override
  public int flushRepository() throws RepoException {

    return this.metricRepo.flush(this.threadId);
  }

}
