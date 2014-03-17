package com.hpcloud.consumer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.hpcloud.disruptor.event.MetricMessageEvent;
import com.hpcloud.message.MetricEnvelope;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                final MetricEnvelope[] metricEnvelopes = objectMapper.readValue(s, MetricEnvelope[].class);

                for (final MetricEnvelope metricEnvelope : metricEnvelopes) {

                    logger.debug(metricEnvelope.toString());

                    disruptor.publishEvent(new EventTranslator<MetricMessageEvent>() {
                        @Override
                        public void translateTo(MetricMessageEvent event, long sequence) {
                            event.setMetricEnvelope(metricEnvelope);

                        }

                    });
                }

            } catch (Exception e) {
                logger.error("Failed to deserialize JSON message and place on disruptor queue: " + s, e);
            }

        }

        logger.debug("Shutting down Thread: " + threadNumber);

    }
}
