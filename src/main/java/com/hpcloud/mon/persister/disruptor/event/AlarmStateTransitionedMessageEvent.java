package com.hpcloud.mon.persister.disruptor.event;

import com.hpcloud.mon.common.event.AlarmStateTransitionedEvent;

public class AlarmStateTransitionedMessageEvent
{
    private AlarmStateTransitionedEvent message;

    public AlarmStateTransitionedEvent getMessage() {
        return message;
    }

    public void setMessage(AlarmStateTransitionedEvent message) {
        this.message = message;
    }
}
