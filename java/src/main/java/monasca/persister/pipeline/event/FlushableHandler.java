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

import monasca.persister.configuration.PipelineConfig;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.dropwizard.setup.Environment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FlushableHandler<T> {

  private static final Logger logger = LoggerFactory.getLogger(FlushableHandler.class);

  private final int batchSize;

  private long flushTimeMillis = System.currentTimeMillis();
  private final long millisBetweenFlushes;
  private final int secondsBetweenFlushes;
  private int msgCount = 0;
  private long batchCount = 0;

  private final Meter processedMeter;
  private final Meter commitMeter;
  private final Timer commitTimer;

  protected final String threadId;

  protected ObjectMapper objectMapper = new ObjectMapper();

  protected final String handlerName;

  protected FlushableHandler(
      PipelineConfig configuration,
      Environment environment,
      String threadId,
      int batchSize) {

    this.threadId = threadId;

    this.handlerName =
        String.format(
            "%s[%s]",
            this.getClass().getName(),
            threadId);

    this.processedMeter =
        environment.metrics()
            .meter(handlerName + "." + "events-processed-processedMeter");

    this.commitMeter =
        environment.metrics().meter(handlerName + "." + "commits-executed-processedMeter");

    this.commitTimer =
        environment.metrics().timer(handlerName + "." + "total-commit-and-flush-timer");

    this.secondsBetweenFlushes = configuration.getMaxBatchTime();

    this.millisBetweenFlushes = secondsBetweenFlushes * 1000;

    this.batchSize = batchSize;

    initObjectMapper();

  }

  protected abstract void initObjectMapper();

  protected abstract void flushRepository();

  protected abstract int process(String msg) throws Exception;

  public boolean onEvent(final String msg) throws Exception {

    if (msg == null) {

      logger.debug("[{}]: got heartbeat message, flush every {} seconds.", this.threadId,
          this.secondsBetweenFlushes);

      if (this.flushTimeMillis < System.currentTimeMillis()) {

        logger.debug("[{}]: {} millis past flush time. flushing to repository now.",
            this.threadId, (System.currentTimeMillis() - this.flushTimeMillis));

        flush();

        return true;

      } else {

        logger.debug("[{}]: {} millis to next flush time. no need to flush at this time.",
            this.threadId,  this.flushTimeMillis - System.currentTimeMillis());

        return false;

      }
    }

    this.processedMeter.mark();

    this.msgCount += process(msg);

    if (this.msgCount >= this.batchSize) {

      logger.debug("[{}]: batch sized {} attained", this.threadId, this.batchSize);

      flush();

      return true;

    } else {

      return false;

    }
  }

  public void flush() {

    logger.debug("[{}]: flush", this.threadId);

    if (this.msgCount == 0) {

      logger.debug("[{}]: nothing to flush", this.threadId);
    }

    Timer.Context context = this.commitTimer.time();

    flushRepository();

    context.stop();

    this.commitMeter.mark();

    this.flushTimeMillis = System.currentTimeMillis() + this.millisBetweenFlushes;

    logger.debug("[{}]: flushed {} msg", this.threadId, this.msgCount);

    this.msgCount = 0;
    this.batchCount++;

  }

  public long getBatchCount() {

    return this.batchCount;

  }

  public int getMsgCount() {

    return this.msgCount;
  }
}
