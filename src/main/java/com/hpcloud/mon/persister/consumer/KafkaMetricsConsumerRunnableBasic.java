package com.hpcloud.mon.persister.consumer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.hpcloud.mon.persister.disruptor.MetricDisruptor;
import com.hpcloud.mon.persister.disruptor.event.MetricMessageEvent;
import com.hpcloud.mon.persister.message.MetricEnvelope;
import com.lmax.disruptor.EventTranslator;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMetricsConsumerRunnableBasic implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMetricsConsumerRunnableBasic.class);
    private final KafkaStream stream;
    private final int threadNumber;
    private final MetricDisruptor disruptor;
    private final ObjectMapper objectMapper;

    @Inject
    public KafkaMetricsConsumerRunnableBasic(MetricDisruptor disruptor,
                                             @Assisted KafkaStream stream,
                                             @Assisted int threadNumber) {
        this.stream = stream;
        this.threadNumber = threadNumber;
        this.disruptor = disruptor;
        this.objectMapper = new ObjectMapper();
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
                final MetricEnvelope[] envelopes = objectMapper.readValue(s, MetricEnvelope[].class);

                for (final MetricEnvelope envelope : envelopes) {

                    logger.debug(envelope.toString());

                    disruptor.publishEvent(new EventTranslator<MetricMessageEvent>() {
                        @Override
                        public void translateTo(MetricMessageEvent event, long sequence) {
                            event.setEnvelope(envelope);
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
