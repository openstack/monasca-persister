package com.hpcloud.mon.persister;

import com.hpcloud.mon.persister.consumer.KafkaMetricsConsumer;
import com.hpcloud.mon.persister.consumer.MetricsConsumer;
import com.lmax.disruptor.dsl.Disruptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MonPersisterConsumerTest {

    @Mock
    private KafkaMetricsConsumer kafkaConsumer;

    @Mock
    private Disruptor disruptor;

    @InjectMocks
    private MetricsConsumer monConsumer;

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
