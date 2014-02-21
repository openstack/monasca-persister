package com.hpcloud;

import com.lmax.disruptor.EventHandler;

public class StringEventHandler implements EventHandler<StringEvent> {

    @Override
    public void onEvent(StringEvent stringEvent, long l, boolean b) throws Exception {
        System.out.println("Event: " + stringEvent.getValue());
    }
}
