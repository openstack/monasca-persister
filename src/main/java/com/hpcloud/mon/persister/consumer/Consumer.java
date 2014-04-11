package com.hpcloud.mon.persister.consumer;

import com.google.inject.Inject;
import com.lmax.disruptor.dsl.Disruptor;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final KafkaConsumer consumer;
    private final Disruptor disruptor;

    @Inject
    public Consumer(KafkaConsumer kafkaConsumer, Disruptor disruptor) {
        this.consumer = kafkaConsumer;
        this.disruptor = disruptor;
    }

    @Override
    public void start() throws Exception {
        logger.debug("start");
        consumer.run();
    }

    @Override
    public void stop() throws Exception {
        logger.debug("stop");
        consumer.stop();
        disruptor.shutdown();
    }
}
