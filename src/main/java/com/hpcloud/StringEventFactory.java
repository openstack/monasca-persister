package com.hpcloud;

import com.lmax.disruptor.EventFactory;

public class StringEventFactory implements EventFactory<StringEvent> {

    public static final StringEventFactory INSTANCE = new StringEventFactory();

    @Override
    public StringEvent newInstance() {
        return new StringEvent();
    }
}
