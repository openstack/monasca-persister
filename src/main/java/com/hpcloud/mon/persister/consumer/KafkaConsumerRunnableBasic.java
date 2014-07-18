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

import com.hpcloud.mon.persister.pipeline.ManagedPipeline;

import kafka.consumer.ConsumerIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KafkaConsumerRunnableBasic<T> implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerRunnableBasic.class);
  private final KafkaChannel kafkaChannel;
  private final int threadNumber;
  private final ManagedPipeline<T> pipeline;
  private volatile boolean stop = false;

  public KafkaConsumerRunnableBasic(KafkaChannel kafkaChannel,
      ManagedPipeline<T> pipeline,
      int threadNumber) {
    this.kafkaChannel = kafkaChannel;
    this.pipeline = pipeline;
    this.threadNumber = threadNumber;
  }

  abstract protected void publishHeartbeat();

  abstract protected void handleMessage(String message);

  protected void markRead() {
    this.kafkaChannel.markRead();
  }

  public void stop() {
    this.stop = true;
  }

  public void run() {
    final ConsumerIterator<byte[], byte[]> it = kafkaChannel.getKafkaStream().iterator();
    logger.debug("KafkaChannel {} has stream", this.threadNumber);
    while (!this.stop) {
      try {
        if (it.hasNext()) {
          final String s = new String(it.next().message());

          logger.debug("Thread {}: {}", threadNumber, s);

          handleMessage(s);
        }
      } catch (kafka.consumer.ConsumerTimeoutException cte) {
        publishHeartbeat();
        continue;
      }
    }
    logger.debug("Shutting down Thread: {}", threadNumber);
    this.kafkaChannel.stop();
  }

  protected void publishEvent(final T event) {
    if (pipeline.publishEvent(event)) {
      markRead();
    }
  }
}
