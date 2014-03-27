package com.hpcloud.mon.persister.consumer;

import com.google.inject.Inject;
import com.hpcloud.mon.persister.disruptor.MetricDisruptor;
import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsConsumer implements Managed {

    private static Logger logger = LoggerFactory.getLogger(MetricsConsumer.class);
    private final KafkaMetricsConsumer consumer;
    private final MetricDisruptor disruptor;

    @Inject
    public MetricsConsumer(KafkaMetricsConsumer kafkaConsumer, MetricDisruptor disruptor) {
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
