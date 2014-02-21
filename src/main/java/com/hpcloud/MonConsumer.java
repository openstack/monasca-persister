package com.hpcloud;

import com.google.inject.Inject;
import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonConsumer implements Managed {

    private static Logger logger = LoggerFactory.getLogger(MonConsumer.class);

    KafkaConsumer kafkaConsumer;

    @Inject
    public MonConsumer(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
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
    }
}
