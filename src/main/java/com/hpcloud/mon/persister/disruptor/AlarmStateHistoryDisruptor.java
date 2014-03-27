package com.hpcloud.mon.persister.disruptor;

import com.hpcloud.mon.persister.disruptor.event.AlarmStateTransitionMessageEvent;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.Executor;

public class AlarmStateHistoryDisruptor extends Disruptor<AlarmStateTransitionMessageEvent> {
    public AlarmStateHistoryDisruptor(EventFactory eventFactory, int ringBufferSize, Executor executor) {
        super(eventFactory, ringBufferSize, executor);
    }

    public AlarmStateHistoryDisruptor(final EventFactory eventFactory,
                                      int ringBufferSize,
                                      Executor executor,
                                      ProducerType producerType,
                                      WaitStrategy waitStrategy) {
        super(eventFactory, ringBufferSize, executor, producerType, waitStrategy);
    }
}
