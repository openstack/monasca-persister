package com.hpcloud.consumer;

import com.google.inject.Inject;
import com.lmax.disruptor.dsl.Disruptor;
import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonConsumer implements Managed {

    private static Logger logger = LoggerFactory.getLogger(MonConsumer.class);

    private KafkaConsumer kafkaConsumer;
    private Disruptor disruptor;

    @Inject
    public MonConsumer(KafkaConsumer kafkaConsumer, Disruptor disruptor) {
        this.kafkaConsumer = kafkaConsumer;
        this.disruptor = disruptor;
    }

    @Override
    public void start() throws Exception {
        logger.debug("start");
        kafkaConsumer.run();
    }

    @Override
    public void stop() throws Exception {
        logger.debug("stop");
        kafkaConsumer.stop();
        disruptor.shutdown();

    }
}
