package com.hpcloud.mon.persister.disruptor;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.disruptor.event.MetricMessageEventFactory;
import com.hpcloud.mon.persister.disruptor.event.MetricMessageEventHandlerFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MetricDisruptorProvider implements Provider<MetricDisruptor> {

    private static Logger logger = LoggerFactory.getLogger(MetricDisruptorProvider.class);

    private final MonPersisterConfiguration configuration;
    private final MetricMessageEventHandlerFactory metricMessageEventHandlerFactory;
    private final ExceptionHandler exceptionHandler;
    private final MetricDisruptor instance;

    @Inject
    public MetricDisruptorProvider(MonPersisterConfiguration configuration,
                                   MetricMessageEventHandlerFactory metricMessageEventHandlerFactory,
                                   ExceptionHandler exceptionHandler) {

        this.configuration = configuration;
        this.metricMessageEventHandlerFactory = metricMessageEventHandlerFactory;
        this.exceptionHandler = exceptionHandler;
        this.instance = createInstance();
    }

    private MetricDisruptor createInstance() {

        logger.debug("Creating disruptor...");

        Executor executor = Executors.newCachedThreadPool();
        MetricMessageEventFactory metricMessageEventFactory = new MetricMessageEventFactory();

        int bufferSize = configuration.getDisruptorConfiguration().getBufferSize();
        logger.debug("Buffer size for instance of disruptor [" + bufferSize + "]");

        MetricDisruptor disruptor = new MetricDisruptor(metricMessageEventFactory, bufferSize, executor);
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

    public MetricDisruptor get() {
        return instance;
    }
}
