package com.hpcloud.mon.persister.disruptor.event;

import com.hpcloud.mon.common.event.AlarmStateTransitionedEvent;

public class AlarmStateTransitionedEventHolder
{
    AlarmStateTransitionedEvent event;

    public AlarmStateTransitionedEvent getEvent() {
        return event;
    }

    public void setEvent(AlarmStateTransitionedEvent event) {
        this.event = event;
    }
}
