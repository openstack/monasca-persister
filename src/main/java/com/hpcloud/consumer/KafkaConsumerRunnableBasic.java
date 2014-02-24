package com.hpcloud.consumer;

import com.hpcloud.event.StringEvent;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class KafkaConsumerRunnableBasic implements Runnable {
    private KafkaStream stream;
    private int threadNumber;
    private Disruptor disruptor;

    public KafkaConsumerRunnableBasic(KafkaStream stream, int threadNumber, Disruptor disruptor) {
        this.stream = stream;
        this.threadNumber = threadNumber;
        this.disruptor = disruptor;
    }

    @SuppressWarnings("unchecked")
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            final String s = new String(it.next().message());
            System.out.println("Thread " + threadNumber + ": " + s);

            disruptor.publishEvent(new EventTranslator<StringEvent>() {
                @Override
                public void translateTo(StringEvent event, long sequence) {
                    event.set(s);

                }
            });
        }

        System.out.println("Shutting down Thread: " + threadNumber);

    }
}
