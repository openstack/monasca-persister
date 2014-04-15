package com.hpcloud.mon.persister.disruptor;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.disruptor.event.MetricFactory;
import com.hpcloud.mon.persister.disruptor.event.MetricHandlerFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MetricDisruptorProvider implements Provider<MetricDisruptor> {

    private static final Logger logger = LoggerFactory.getLogger(MetricDisruptorProvider.class);

    private final MonPersisterConfiguration configuration;
    private final MetricHandlerFactory eventHandlerFactory;
    private final ExceptionHandler exceptionHandler;
    private final MetricDisruptor instance;

    @Inject
    public MetricDisruptorProvider(MonPersisterConfiguration configuration,
                                   MetricHandlerFactory eventHandlerFactory,
                                   ExceptionHandler exceptionHandler) {

        this.configuration = configuration;
        this.eventHandlerFactory = eventHandlerFactory;
        this.exceptionHandler = exceptionHandler;
        this.instance = createInstance();
    }

    private MetricDisruptor createInstance() {

        logger.debug("Creating disruptor...");

        Executor executor = Executors.newCachedThreadPool();
        MetricFactory eventFactory = new MetricFactory();

        int bufferSize = configuration.getDisruptorConfiguration().getBufferSize();
        logger.debug("Buffer size for instance of disruptor [" + bufferSize + "]");

        MetricDisruptor disruptor = new MetricDisruptor(eventFactory, bufferSize, executor);
        disruptor.handleExceptionsWith(exceptionHandler);

        int batchSize = configuration.getVerticaOutputProcessorConfiguration().getBatchSize();
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

    public MetricDisruptor get() {
        return instance;
    }
}
