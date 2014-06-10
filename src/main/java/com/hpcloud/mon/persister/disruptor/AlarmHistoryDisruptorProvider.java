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

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.disruptor.event.AlarmStateTransitionedEventFactory;
import com.hpcloud.mon.persister.disruptor.event.AlarmStateTransitionedEventHandlerFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class AlarmHistoryDisruptorProvider implements Provider<AlarmStateHistoryDisruptor> {

    private static final Logger logger = LoggerFactory.getLogger(AlarmHistoryDisruptorProvider.class);

    private final MonPersisterConfiguration configuration;
    private final AlarmStateTransitionedEventHandlerFactory eventHandlerFactory;
    private final ExceptionHandler exceptionHandler;
    private final AlarmStateHistoryDisruptor instance;

    @Inject
    public AlarmHistoryDisruptorProvider(MonPersisterConfiguration configuration,
                                         AlarmStateTransitionedEventHandlerFactory eventHandlerFactory,
                                         ExceptionHandler exceptionHandler) {
        this.configuration = configuration;
        this.eventHandlerFactory = eventHandlerFactory;
        this.exceptionHandler = exceptionHandler;
        this.instance = createInstance();
    }

    private AlarmStateHistoryDisruptor createInstance() {

        logger.debug("Creating disruptor...");

        Executor executor = Executors.newCachedThreadPool();
        AlarmStateTransitionedEventFactory eventFactory = new AlarmStateTransitionedEventFactory();

        int bufferSize = configuration.getDisruptorConfiguration().getBufferSize();
        logger.debug("Buffer size for instance of disruptor [" + bufferSize + "]");

        AlarmStateHistoryDisruptor disruptor = new AlarmStateHistoryDisruptor(eventFactory, bufferSize, executor);
        disruptor.handleExceptionsWith(exceptionHandler);

        int batchSize = configuration.getOutputProcessorConfiguration().getBatchSize();
        logger.debug("Batch size for each output processor [" + batchSize + "]");

        int numOutputProcessors = configuration.getDisruptorConfiguration().getNumProcessors();
        logger.debug("Number of output processors [" + numOutputProcessors + "]");

        EventHandler[] eventHandlers = new EventHandler[numOutputProcessors];

        for (int i = 0; i < numOutputProcessors; ++i) {
            eventHandlers[i] = eventHandlerFactory.create(i, numOutputProcessors, batchSize);
        }

        disruptor.handleEventsWith(eventHandlers);
        disruptor.start();

        logger.debug("Instance of disruptor successfully started");
        logger.debug("Instance of disruptor fully created");

        return disruptor;
    }

    public AlarmStateHistoryDisruptor get() {
        return instance;
    }
}
