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

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import io.dropwizard.lifecycle.Managed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedConsumer<T> implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(ManagedConsumer.class);

  private final KafkaConsumer<T> consumer;
  private final String threadId;

  @Inject
  public ManagedConsumer(
      @Assisted KafkaConsumer<T> kafkaConsumer,
      @Assisted String threadId) {

    this.consumer = kafkaConsumer;
    this.threadId = threadId;

  }

  @Override
  public void start() throws Exception {

    logger.debug("[{}]: start", this.threadId);

    this.consumer.start();
  }

  @Override
  public void stop() throws Exception {

    logger.debug("[{}]: stop", this.threadId);

    this.consumer.stop();
  }
}
