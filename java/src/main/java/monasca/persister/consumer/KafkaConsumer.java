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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class KafkaConsumer<T> {

  private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

  private static final int WAIT_TIME = 10;

  private ExecutorService executorService;
  private final KafkaChannel kafkaChannel;
  private final int threadNum;
  private KafkaConsumerRunnableBasic<T> kafkaConsumerRunnableBasic;

  public KafkaConsumer(KafkaChannel kafkaChannel, int threadNum) {
    this.kafkaChannel = kafkaChannel;
    this.threadNum = threadNum;
  }

  protected abstract KafkaConsumerRunnableBasic<T> createRunnable(
      KafkaChannel kafkaChannel,
      int threadNumber);

  public void start() {
    executorService = Executors.newFixedThreadPool(1);
    kafkaConsumerRunnableBasic = createRunnable(kafkaChannel, this.threadNum);
    executorService.submit(kafkaConsumerRunnableBasic);
  }

  public void stop() {
    kafkaConsumerRunnableBasic.stop();
    if (executorService != null) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(WAIT_TIME, TimeUnit.SECONDS)) {
          logger.warn("Did not shut down in {} seconds", WAIT_TIME);
        }
      } catch (InterruptedException e) {
        logger.info("awaitTermination interrupted", e);
      }
    }
  }
}
