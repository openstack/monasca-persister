package com.hpcloud.mon.persister.disruptor.event;

import com.lmax.disruptor.EventFactory;

public class AlarmStateTransitionedMessageEventFactory implements EventFactory<AlarmStateTransitionedMessageEvent> {

    public static final AlarmStateTransitionedMessageEventFactory INSTANCE = new AlarmStateTransitionedMessageEventFactory();

    @Override
    public AlarmStateTransitionedMessageEvent newInstance() {
        return new AlarmStateTransitionedMessageEvent();
    }
}
