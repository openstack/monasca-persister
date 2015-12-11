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

import monasca.persister.pipeline.ManagedPipeline;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

import kafka.consumer.ConsumerIterator;
import monasca.persister.repository.RepoException;

public class KafkaConsumerRunnableBasic<T> implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerRunnableBasic.class);

  private final KafkaChannel kafkaChannel;
  private final String threadId;
  private final ManagedPipeline<T> pipeline;
  private volatile boolean stop = false;

  private ExecutorService executorService;

  @Inject
  public KafkaConsumerRunnableBasic(
      @Assisted KafkaChannel kafkaChannel,
      @Assisted ManagedPipeline<T> pipeline,
      @Assisted String threadId) {

    this.kafkaChannel = kafkaChannel;
    this.pipeline = pipeline;
    this.threadId = threadId;
  }

  public KafkaConsumerRunnableBasic<T> setExecutorService(ExecutorService executorService) {

    this.executorService = executorService;

    return this;

  }

  protected void publishHeartbeat() throws RepoException {

    publishEvent(null);

  }

  private void markRead() {

    logger.debug("[{}]: marking read", this.threadId);

    this.kafkaChannel.markRead();

  }

  public void stop() {

    logger.info("[{}]: stop", this.threadId);

    this.stop = true;

  }

  public void run() {

    logger.info("[{}]: run", this.threadId);

    final ConsumerIterator<byte[], byte[]> it = kafkaChannel.getKafkaStream().iterator();

    logger.debug("[{}]: KafkaChannel has stream iterator", this.threadId);

    while (!this.stop) {

      try {

        try {

          if (isInterrupted()) {

            logger.debug("[{}]: is interrupted", this.threadId);
            break;

          }

          if (it.hasNext()) {

            if (isInterrupted()) {

              logger.debug("[{}]: is interrupted", this.threadId);
              break;

            }

            if (this.stop) {

              logger.debug("[{}]: is stopped", this.threadId);
              break;

            }

            final String msg = new String(it.next().message());

            logger.debug("[{}]: {}", this.threadId, msg);

            publishEvent(msg);

          }

        } catch (kafka.consumer.ConsumerTimeoutException cte) {

          if (isInterrupted()) {

            logger.debug("[{}]: is interrupted", this.threadId);
            break;

          }

          if (this.stop) {

            logger.debug("[{}]: is stopped", this.threadId);
            break;

          }

          publishHeartbeat();

        }

      } catch (Throwable e) {

        logger.error(
            "[{}]: caught fatal exception while publishing msg. Shutting entire persister down "
            + "now!", this.threadId, e);

          logger.error("[{}]: calling shutdown on executor service", this.threadId);
          this.executorService.shutdownNow();

          logger.error("[{}]: shutting down system. calling system.exit(1)", this.threadId);
          System.exit(1);

        }

    }

    logger.info("[{}]: calling stop on kafka channel", this.threadId);

    this.kafkaChannel.stop();

    logger.debug("[{}]: exiting main run loop", this.threadId);

  }

  protected void publishEvent(final String msg) throws RepoException {

    if (pipeline.publishEvent(msg)) {

      markRead();

    }

  }

  private boolean isInterrupted() {

    if (Thread.currentThread().interrupted()) {

      logger.debug("[{}]: is interrupted. breaking out of run loop", this.threadId);

      return true;

    } else {

      return false;

    }
  }
}
