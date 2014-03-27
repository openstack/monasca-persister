package com.hpcloud.mon.persister.disruptor.event;

import com.hpcloud.mon.persister.message.AlarmStateTransitionMessage;

public class AlarmStateTransitionMessageEvent
{
    private AlarmStateTransitionMessage message;

    public AlarmStateTransitionMessage getMessage() {
        return message;
    }

    public void setMessage(AlarmStateTransitionMessage message) {
        this.message = message;
    }
}
