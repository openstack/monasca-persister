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

package com.hpcloud.mon.persister.pipeline;

import com.hpcloud.mon.persister.pipeline.event.FlushableHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedPipeline<T> {
  private static final Logger logger = LoggerFactory.getLogger(ManagedPipeline.class);

  private final FlushableHandler<T> eventHandler;

  public ManagedPipeline(FlushableHandler<T> eventHandler) {
    this.eventHandler = eventHandler;
  }

  public void shutdown() {
    eventHandler.flush();
  }

  public boolean publishEvent(T holder) {
      try {
        return this.eventHandler.onEvent(holder);
      } catch (Exception e) {
        logger.error("Failed to handle event", e);
        return false;
      }
  }
}
