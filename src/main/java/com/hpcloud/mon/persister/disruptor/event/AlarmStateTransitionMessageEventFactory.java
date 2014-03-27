package com.hpcloud.mon.persister.disruptor.event;

import com.lmax.disruptor.EventFactory;

public class AlarmStateTransitionMessageEventFactory implements EventFactory<AlarmStateTransitionMessageEvent> {

    public static final AlarmStateTransitionMessageEventFactory INSTANCE = new AlarmStateTransitionMessageEventFactory();

    @Override
    public AlarmStateTransitionMessageEvent newInstance() {
        return new AlarmStateTransitionMessageEvent();
    }
}
