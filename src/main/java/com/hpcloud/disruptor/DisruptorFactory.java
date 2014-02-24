package com.hpcloud.disruptor;

import com.google.inject.Inject;
import com.hpcloud.configuration.MonPersisterConfiguration;
import com.hpcloud.event.StringEvent;
import com.hpcloud.event.StringEventFactory;
import com.hpcloud.event.StringEventHandler;
import com.hpcloud.event.StringEventHandlerFactory;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class DisruptorFactory {

    private MonPersisterConfiguration configuration;
    private StringEventHandlerFactory stringEventHandlerFactory;
    private Disruptor instance;

    @Inject
    public DisruptorFactory(MonPersisterConfiguration configuration,
                            StringEventHandlerFactory stringEventHandlerFactory) {
        this.configuration = configuration;
        this.stringEventHandlerFactory = stringEventHandlerFactory;
    }

    public synchronized Disruptor<StringEvent> create() {
        if (instance == null) {

            Executor executor = Executors.newCachedThreadPool();
            StringEventFactory stringEventFactory = new StringEventFactory();

            int buffersize = configuration.getDisruptorConfiguration().getBufferSize();
            Disruptor<StringEvent> disruptor = new Disruptor(stringEventFactory, buffersize, executor);

            int batchSize = configuration.getVerticaOutputProcessorConfiguration().getBatchSize();
            int numOutputProcessors = configuration.getDisruptorConfiguration().getNumProcessors();
            EventHandlerGroup<StringEvent> handlerGroup = null;
            for (int i = 0; i < numOutputProcessors; ++i) {

                StringEventHandler stringEventHandler = stringEventHandlerFactory.create(i, numOutputProcessors, batchSize);

                if (handlerGroup == null) {
                    handlerGroup = disruptor.handleEventsWith(stringEventHandler);
                } else {
                    handlerGroup.then(stringEventHandler);
                }

            }
            disruptor.start();
            instance = disruptor;
        }
        return instance;
    }
}
