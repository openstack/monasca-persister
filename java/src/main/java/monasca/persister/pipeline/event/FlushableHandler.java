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

import io.dropwizard.setup.Environment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FlushableHandler<T> {

  private static final Logger logger = LoggerFactory.getLogger(FlushableHandler.class);

  private final int ordinal;
  private final int batchSize;
  private final String handlerName;

  private long millisSinceLastFlush = System.currentTimeMillis();
  private final long millisBetweenFlushes;
  private final int secondsBetweenFlushes;
  private int eventCount = 0;

  private final Environment environment;

  private final Meter processedMeter;
  private final Meter commitMeter;
  private final Timer commitTimer;

  protected FlushableHandler(
      PipelineConfig configuration,
      Environment environment,
      int ordinal,
      int batchSize,
      String baseName) {

    this.handlerName = String.format("%s[%d]", baseName, ordinal);
    this.environment = environment;
    this.processedMeter =
        this.environment.metrics()
            .meter(handlerName + "." + "events-processed-processedMeter");
    this.commitMeter =
        this.environment.metrics().meter(handlerName + "." + "commits-executed-processedMeter");
    this.commitTimer =
        this.environment.metrics().timer(handlerName + "." + "total-commit-and-flush-timer");

    this.secondsBetweenFlushes = configuration.getMaxBatchTime();
    this.millisBetweenFlushes = secondsBetweenFlushes * 1000;

    this.ordinal = ordinal;
    this.batchSize = batchSize;
  }

  protected abstract void flushRepository();

  protected abstract int process(T metricEvent) throws Exception;

  public boolean onEvent(final T event) throws Exception {

    if (event == null) {

      long delta = millisSinceLastFlush + millisBetweenFlushes;
      logger.debug("{} received heartbeat message, flush every {} seconds.", this.handlerName,
          this.secondsBetweenFlushes);

      if (delta < System.currentTimeMillis()) {

        logger.debug("{}: {} seconds since last flush. Flushing to repository now.",
            this.handlerName, delta);

        flush();

        return true;

      } else {

        logger.debug("{}: {} seconds since last flush. No need to flush at this time.",
            this.handlerName, delta);
        return false;

      }
    }

    processedMeter.mark();

    eventCount += process(event);

    if (eventCount >= batchSize) {
      flush();
      return true;
    } else {
      return false;
    }
  }

  public void flush() {
    if (eventCount == 0) {
      logger.debug("{}: Nothing to flush", this.handlerName);
    }
    Timer.Context context = commitTimer.time();
    flushRepository();
    context.stop();
    commitMeter.mark();
    millisSinceLastFlush = System.currentTimeMillis();
    logger.debug("{}: Flushed {} events", this.handlerName, this.eventCount);
    eventCount = 0;
  }
}
