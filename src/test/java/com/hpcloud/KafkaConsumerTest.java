package com.hpcloud;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaConsumerTest {

    KafkaConsumer kafkaConsumer;

    @Before
    public void before() {
        kafkaConsumer = new KafkaConsumer();
    }

    @Test
    public void testKafkaConsumerStart() {
        try {
            kafkaConsumer.start();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    @Test
    public void testKafkaConsumerStop() {
        try {
            kafkaConsumer.stop();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @After
    public void after() {
        System.out.println("after");
    }
}
