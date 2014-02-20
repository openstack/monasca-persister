package com.hpcloud;

import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumer implements Managed {

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @Override
    public void start() throws Exception {
        logger.debug("start");
    }

    @Override
    public void stop() throws Exception {
        logger.debug("stop");
    }
}
