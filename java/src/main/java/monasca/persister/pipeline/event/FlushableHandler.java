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
import monasca.persister.repository.RepoException;

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
  private final Meter flushMeter;
  private final Timer flushTimer;

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
        environment.metrics().meter(handlerName + "." + "events-processed-meter");

    this.flushMeter =
        environment.metrics().meter(handlerName + "." + "flush-meter");

    this.flushTimer =
        environment.metrics().timer(handlerName + "." + "flush-timer");

    this.secondsBetweenFlushes = configuration.getMaxBatchTime();

    this.millisBetweenFlushes = secondsBetweenFlushes * 1000;

    this.batchSize = batchSize;

    initObjectMapper();

  }

  protected abstract void initObjectMapper();

  protected abstract int flushRepository() throws RepoException;

  protected abstract int process(String msg);

  public boolean onEvent(final String msg) throws RepoException {

    if (msg == null) {

      if (isFlushTime()) {

        int msgFlushCnt = flush();

        return msgFlushCnt > 0 ? true : false;

      } else {

         return false;

      }
    }

    this.msgCount += process(msg);

    this.processedMeter.mark();

    if (isBatchSize()) {

      int msgFlushCnt = flush();

      return msgFlushCnt > 0 ? true : false;

    } else {

      return false;

    }
  }

  private boolean isBatchSize() {

    logger.debug("[{}]: checking batch size", this.threadId);

    if (this.msgCount >= this.batchSize) {

      logger.debug("[{}]: batch sized {} attained", this.threadId, this.batchSize);

      return true;

    } else {

      logger.debug("[{}]: batch size now at {}, batch size {} not attained",
                   this.threadId,
                   this.msgCount,
                   this.batchSize);

      return false;

    }
  }

  private boolean isFlushTime() {

    logger.debug("[{}]: got heartbeat message, checking flush time. flush every {} seconds.",
                 this.threadId,
                 this.secondsBetweenFlushes);

    long now = System.currentTimeMillis();

    if (this.flushTimeMillis <= now ) {

      logger.debug(
          "[{}]: {} ms past flush time. flushing to repository now.",
          this.threadId,
          now - this.flushTimeMillis);

      return true;

    } else {

      logger.debug(
          "[{}]: {} ms to next flush time. no need to flush at this time.",
          this.threadId,
          this.flushTimeMillis - now);

      return false;

    }
  }

  public int flush() throws RepoException {

    logger.debug("[{}]: flushing", this.threadId);

    Timer.Context context = this.flushTimer.time();

    int msgFlushCnt = flushRepository();

    context.stop();

    this.flushMeter.mark();

    this.flushTimeMillis = System.currentTimeMillis() + this.millisBetweenFlushes;

    logger.debug("[{}]: flushed {} msg", this.threadId, msgFlushCnt);

    this.msgCount = 0;

    this.batchCount++;

    return msgFlushCnt;

  }

  protected long getBatchCount() {

    return this.batchCount;

  }

  protected int getMsgCount() {

    return this.msgCount;
  }
}
