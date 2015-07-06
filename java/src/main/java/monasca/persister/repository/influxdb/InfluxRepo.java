/*
 * Copyright (c) 2015 Hewlett-Packard Development Company, L.P.
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
package monasca.persister.repository.influxdb;

import com.codahale.metrics.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.setup.Environment;
import monasca.persister.repository.Repo;
import monasca.persister.repository.RepoException;

public abstract class InfluxRepo<T> implements Repo<T> {

  private static final Logger logger = LoggerFactory.getLogger(InfluxRepo.class);

  protected final com.codahale.metrics.Timer flushTimer;

  public InfluxRepo (final Environment env) {

    this.flushTimer =
        env.metrics().timer(this.getClass().getName() + "." + "flush-timer");

  }

  @Override
  public int flush(String id) throws RepoException {

    if (isBufferEmpty()) {

      logger.debug("[{}]: no msg to be written to influxdb", id);

      logger.debug("[{}]: returning from flush without flushing", id);

      return 0;

    } else {

      return writeToRepo(id);

    }
  }

  private int writeToRepo(String id) throws RepoException {

    try {

      final Timer.Context context = flushTimer.time();

      final long startTime = System.currentTimeMillis();

      int msgWriteCnt = write(id);

      final long endTime = System.currentTimeMillis();

      context.stop();

      logger.debug("[{}]: writing to influxdb took {} ms", id, endTime - startTime);

      clearBuffers();

      return msgWriteCnt;

    } catch (Exception e) {

      logger.error("[{}]: failed to write to influxdb", id, e);

      throw e;

    }
  }

  protected abstract boolean isBufferEmpty();

  protected abstract int write(String id) throws RepoException;

  protected abstract void clearBuffers();
}
