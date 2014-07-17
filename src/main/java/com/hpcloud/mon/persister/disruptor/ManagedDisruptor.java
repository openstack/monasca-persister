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

package com.hpcloud.mon.persister.disruptor;

import com.hpcloud.mon.persister.disruptor.event.FlushableHandler;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.Executor;

public class ManagedDisruptor<T> extends Disruptor<T> {
  private FlushableHandler[] handlers = new FlushableHandler[0];

  public ManagedDisruptor(EventFactory<T> eventFactory, int ringBufferSize, Executor executor) {
    super(eventFactory, ringBufferSize, executor);
  }

  public ManagedDisruptor(final EventFactory<T> eventFactory, int ringBufferSize,
      Executor executor, ProducerType producerType, WaitStrategy waitStrategy) {
    super(eventFactory, ringBufferSize, executor, producerType, waitStrategy);
  }

  @Override
  public void shutdown() {
    for (FlushableHandler handler : handlers) {
      handler.flush();
    }
    super.shutdown();
  }

  public void setHandlers(FlushableHandler[] handlers) {
    this.handlers = handlers;
  }
}
