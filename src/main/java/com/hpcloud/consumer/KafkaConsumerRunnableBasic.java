package com.hpcloud.consumer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.hpcloud.event.MetricMessageEvent;
import com.hpcloud.message.MetricMessage;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KafkaConsumerRunnableBasic implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumerRunnableBasic.class);

    private KafkaStream stream;
    private int threadNumber;
    private Disruptor disruptor;
    private ObjectMapper objectMapper;

    @Inject
    public KafkaConsumerRunnableBasic(Disruptor disruptor,
                                      ObjectMapper objectMapper,
                                      @Assisted KafkaStream stream,
                                      @Assisted int threadNumber) {
        this.stream = stream;
        this.threadNumber = threadNumber;
        this.disruptor = disruptor;
        this.objectMapper = objectMapper;
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
    }

    @SuppressWarnings("unchecked")
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {

            final String s = new String(it.next().message());

            logger.debug("Thread " + threadNumber + ": " + s);

            try {
                final MetricMessage[] metricMessages = objectMapper.readValue(s, MetricMessage[].class);

                for (final MetricMessage metricMessage : metricMessages) {
                    logger.debug(metricMessage.toString());

                    disruptor.publishEvent(new EventTranslator<MetricMessageEvent>() {
                        @Override
                        public void translateTo(MetricMessageEvent event, long sequence) {
                            event.setMetricMessage(metricMessage);

                        }

                    });
                }

            } catch (IOException e) {

                logger.error("Failed to deserialize JSON message: " + s, e);
            }

        }

        logger.debug("Shutting down Thread: " + threadNumber);

    }
}
