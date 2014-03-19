package com.hpcloud.disruptor;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.hpcloud.configuration.MonPersisterConfiguration;
import com.hpcloud.disruptor.event.MetricMessageEvent;
import com.hpcloud.disruptor.event.MetricMessageEventFactory;
import com.hpcloud.disruptor.event.MetricMessageEventHandlerFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class DisruptorProvider implements Provider<Disruptor> {

    private static Logger logger = LoggerFactory.getLogger(DisruptorProvider.class);

    private final MonPersisterConfiguration configuration;
    private final MetricMessageEventHandlerFactory metricMessageEventHandlerFactory;
    private final ExceptionHandler exceptionHandler;
    private final Disruptor instance;

    @Inject
    public DisruptorProvider(MonPersisterConfiguration configuration,
                             MetricMessageEventHandlerFactory metricMessageEventHandlerFactory,
                             ExceptionHandler exceptionHandler) {
        this.configuration = configuration;
        this.metricMessageEventHandlerFactory = metricMessageEventHandlerFactory;
        this.exceptionHandler = exceptionHandler;
        this.instance = createInstance();
    }

    private Disruptor createInstance() {

        logger.debug("Creating disruptor...");

        Executor executor = Executors.newCachedThreadPool();
        MetricMessageEventFactory metricMessageEventFactory = new MetricMessageEventFactory();

        int bufferSize = configuration.getDisruptorConfiguration().getBufferSize();
        logger.debug("Buffer size for instance of disruptor [" + bufferSize + "]");

        Disruptor<MetricMessageEvent> disruptor = new Disruptor(metricMessageEventFactory, bufferSize, executor);
        disruptor.handleExceptionsWith(exceptionHandler);

        int batchSize = configuration.getVerticaOutputProcessorConfiguration().getBatchSize();
        logger.debug("Batch size for each output processor [" + batchSize + "]");

        int numOutputProcessors = configuration.getDisruptorConfiguration().getNumProcessors();
        logger.debug("Number of output processors [" + numOutputProcessors + "]");

        EventHandler[] eventHandlers = new EventHandler[numOutputProcessors];

        for (int i = 0; i < numOutputProcessors; ++i) {
            eventHandlers[i] = metricMessageEventHandlerFactory.create(i, numOutputProcessors, batchSize);
        }

        disruptor.handleEventsWith(eventHandlers);
        disruptor.start();

        logger.debug("Instance of disruptor successfully started");
        logger.debug("Instance of disruptor fully created");

        return disruptor;
    }

    public Disruptor<MetricMessageEvent> get() {
        return instance;
    }
}
