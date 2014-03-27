package com.hpcloud.mon.persister.consumer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.hpcloud.mon.persister.disruptor.AlarmStateHistoryDisruptor;
import com.hpcloud.mon.persister.disruptor.event.AlarmStateTransitionMessageEvent;
import com.hpcloud.mon.persister.message.AlarmStateTransitionMessage;
import com.lmax.disruptor.EventTranslator;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAlarmStateTransitionConsumerRunnableBasic implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(KafkaAlarmStateTransitionConsumerRunnableBasic.class);

    private final KafkaStream stream;
    private int threadNumber;
    private final AlarmStateHistoryDisruptor disruptor;
    private final ObjectMapper objectMapper;

    @Inject
    public KafkaAlarmStateTransitionConsumerRunnableBasic(AlarmStateHistoryDisruptor disruptor,
                                                          @Assisted KafkaStream stream,
                                                          @Assisted int threadNumber) {
        this.stream = stream;
        this.threadNumber = threadNumber;
        this.disruptor = disruptor;
        this.objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        objectMapper.enable(DeserializationFeature.UNWRAP_ROOT_VALUE);
    }

    @SuppressWarnings("unchecked")
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {

            final String s = new String(it.next().message());

            logger.debug("Thread " + threadNumber + ": " + s);

            try {
                final AlarmStateTransitionMessage message = objectMapper.readValue(s, AlarmStateTransitionMessage.class);

                logger.debug(message.toString());

                disruptor.publishEvent(new EventTranslator<AlarmStateTransitionMessageEvent>() {
                    @Override
                    public void translateTo(AlarmStateTransitionMessageEvent event, long sequence) {
                        event.setMessage(message);
                    }
                });
            } catch (Exception e) {
                logger.error("Failed to deserialize JSON message and place on disruptor queue: " + s, e);
            }
        }
        logger.debug("Shutting down Thread: " + threadNumber);
    }
}
