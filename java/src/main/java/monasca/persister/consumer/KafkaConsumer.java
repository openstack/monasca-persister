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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaConsumer<T> {

  private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

  private static final int WAIT_TIME = 10;

  private ExecutorService executorService;

  private final KafkaConsumerRunnableBasic<T> kafkaConsumerRunnableBasic;
  private final String threadId;

  @Inject
  public KafkaConsumer(
      @Assisted KafkaConsumerRunnableBasic<T> kafkaConsumerRunnableBasic,
      @Assisted String threadId) {

    this.kafkaConsumerRunnableBasic = kafkaConsumerRunnableBasic;
    this.threadId = threadId;

  }

  public void start() {

    logger.info("[{}]: start", this.threadId);

    executorService = Executors.newFixedThreadPool(1);

    executorService.submit(kafkaConsumerRunnableBasic);

  }

  public void stop() {

    logger.info("[{}]: stop", this.threadId);

    kafkaConsumerRunnableBasic.stop();

    if (executorService != null) {

      logger.info("[{}]: shutting down executor service", this.threadId);

      executorService.shutdown();

      try {

        logger.info("[{}]: awaiting termination...", this.threadId);

        if (!executorService.awaitTermination(WAIT_TIME, TimeUnit.SECONDS)) {

          logger.warn("[{}]: did not shut down in {} seconds", this.threadId, WAIT_TIME);

        }

        logger.info("[{}]: terminated", this.threadId);

      } catch (InterruptedException e) {

        logger.info("[{}]: awaitTermination interrupted", this.threadId, e);

      }
    }
  }
}
