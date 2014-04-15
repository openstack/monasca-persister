package com.hpcloud.mon.persister.disruptor.event;

import com.lmax.disruptor.EventFactory;

public class AlarmStateTransitionedEventFactory implements EventFactory<AlarmStateTransitionedEventHolder> {

    public static final AlarmStateTransitionedEventFactory INSTANCE = new AlarmStateTransitionedEventFactory();

    @Override
    public AlarmStateTransitionedEventHolder newInstance() {
        return new AlarmStateTransitionedEventHolder();
    }
}
