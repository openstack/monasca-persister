package com.hpcloud;

import com.hpcloud.consumer.KafkaConsumer;
import com.hpcloud.consumer.MonConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MonConsumerTest {

    @Mock
    private KafkaConsumer kafkaConsumer;

    @InjectMocks
    private MonConsumer monConsumer;

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testKafkaConsumerStart() {
        try {
            monConsumer.start();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    @Test
    public void testKafkaConsumerStop() {
        try {
            monConsumer.stop();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @After
    public void after() {
        System.out.println("after");
    }
}
