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

package monasca.persister.pipeline;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import monasca.persister.pipeline.event.FlushableHandler;
import monasca.persister.repository.RepoException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedPipeline<T> {

  private static final Logger logger = LoggerFactory.getLogger(ManagedPipeline.class);

  private final FlushableHandler<T> handler;
  private final String threadId;

  @Inject
  public ManagedPipeline(
      @Assisted FlushableHandler<T> handler,
      @Assisted String threadId) {

    this.handler = handler;
    this.threadId = threadId;

  }

  public boolean shutdown() throws RepoException {

    logger.info("[{}]: shutdown", this.threadId);

    try {

      int msgFlushCnt = handler.flush();

      return msgFlushCnt > 0 ? true : false;

    } catch (RepoException e) {

      logger.error("[{}}: failed to flush repo on shutdown", this.threadId, e);
      logger.error(
          "[{}]: pipeline broken. repo unavailable. check that database is running. shutting pipeline down now!",
          this.threadId);

      throw e;

    }
  }


  public boolean publishEvent(String msg) throws RepoException {

      try {

        return this.handler.onEvent(msg);

      } catch (RepoException e) {

        logger.error("[{}]: failed to handle msg: {}", this.threadId, msg, e);
        logger.error("[{}]: pipeline broken. repo unavailable. check that database is running. shutting pipeline down now!", this.threadId);

        throw e;

      }
  }
}
