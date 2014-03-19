package com.hpcloud.disruptor;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisruptorExceptionHandler implements ExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(DisruptorExceptionHandler.class);

    @Override
    public void handleEventException(Throwable ex, long sequence, Object event) {

        logger.error("Disruptor encountered an exception during normal operation", ex);
        throw new RuntimeException(ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {

        logger.error("Disruptor encountered an exception during startup", ex);
        throw new RuntimeException(ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {

        logger.error("Disruptor encountered an exception during shutdown", ex);
        throw new RuntimeException(ex);
    }
}
